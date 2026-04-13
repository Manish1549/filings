"""
ASX (Australian Securities Exchange) Router — Production Level
==============================================================
API: https://asx.api.markitdigital.com/asx-research/1.0
PDF: https://cdn-api.markitdigital.com/apiman-gateway/ASX/asx-research/1.0/file/{docKey}

Key endpoints (confirmed from ASX website DevTools):
  /search/predictive               — typeahead company search → returns entityXId
  /companies/{symbol}/announcements   — company-specific filings (by ticker)
  /markets/announcements              — market-wide feed; filter by entityXids for company
  /companies                          — company search / list

Search → Filter flow:
  1. GET /search/predictive?searchText=TLM  → pick result → grab entityXId
  2. GET /markets/announcements?entityXids=<id>  → filings for that company

Filtering params (confirmed from DevTools):
  page                0-based page number
  itemsPerPage        items per page (max 100)
  dateRangeType       "custom" | "allDates"
  startDate           YYYY-MM-DD  (used when dateRangeType=custom)
  endDate             YYYY-MM-DD
  headingId           numeric announcement-type ID  (0 = all)
  priceSensitiveOnly  true | false
  excludeCanceledDocs true | false
  entityXids          numeric entity ID(s) — use instead of company name
  summaryCountsDate   YYYY-MM-DD  (market endpoint — sets the "today" for counts)
  includeFacets       true | false  (get per-type counts in the response)

TOTAL FILINGS:  ASX has millions of filings going back to ~1995.
HEADING IDs:    Numeric IDs for announcement types — call /api/asx/categories
                with a date range to get live counts, or use the built-in table.
"""

import asyncio
import logging
from datetime import date, datetime, timezone, timedelta
from typing import Optional

try:
    from zoneinfo import ZoneInfo
    _AEST = ZoneInfo("Australia/Sydney")
except Exception:
    # Fallback: fixed UTC+10 (no DST) — close enough for date display
    _AEST = timezone(timedelta(hours=10))

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter()
logger = logging.getLogger(__name__)

ASX_API  = "https://asx.api.markitdigital.com/asx-research/1.0"
CDN_BASE = "https://cdn-api.markitdigital.com/apiman-gateway/ASX/asx-research/1.0"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.asx.com.au",
    "Referer": "https://www.asx.com.au/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

# Known ASX announcement-type heading IDs.
HEADING_IDS: dict[int, str] = {
    0:  "All Announcements",
    1:  "Chairman's Address",
    2:  "Change in Substantial Holding",
    3:  "Director's Interest",
    5:  "Quarterly Activities Report",
    6:  "Half Yearly Report",
    7:  "Annual Report",
    10: "Other",
    12: "Appendix 4C",
    13: "Appendix 3B",
    14: "Appendix 4E",
    15: "Appendix 3Y",
    17: "Prospectus",
    18: "Offer Document",
    19: "Trading Halt",
    21: "Results of Meeting",
    24: "Market Update",
    25: "Preliminary Final Report",
    27: "Appendix 4D",
    30: "Notice of Meeting",
    31: "Notice of AGM",
    38: "Appendix 5B",
    39: "Quarterly Cash Flow Report",
    40: "Periodic Report",
    43: "Letter to Shareholders",
    57: "Target's Statement",
    66: "Mineral Resources",
    70: "Annual Report to Shareholders",
}


# ── Pydantic models ───────────────────────────────────────────────────────────

class ASXSearchResult(BaseModel):
    """A single result from the predictive search endpoint."""
    entity_xid: Optional[int] = None   # xidEntity from API — use this in /market?entity_xid=
    xid: Optional[str] = None          # xid (security-level ID, different from entity)
    symbol: Optional[str] = None
    company_name: Optional[str] = None
    industry_group: Optional[str] = None
    market_cap: Optional[int] = None
    price_last: Optional[float] = None


class ASXFiling(BaseModel):
    doc_key: str
    symbol: str
    company_name: Optional[str] = None
    headline: str
    date: str
    file_size: Optional[str] = None
    announcement_types: list[str] = []
    heading_id: Optional[int] = None
    is_price_sensitive: bool = False
    is_cancelled: bool = False
    pdf_url: str


class ASXResponse(BaseModel):
    total: int
    total_pages: int
    page: int
    page_size: int
    has_more: bool
    filings: list[ASXFiling]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_asx_date(raw) -> str:
    """Convert any ASX date value to YYYY-MM-DD string."""
    if not raw:
        return ""
    # Unix timestamp in milliseconds — convert using AEST so the date matches
    # what Australia sees (ASX is Sydney-based; UTC timestamp at midnight AEST
    # would show as "previous day" if we used UTC).
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(raw / 1000, tz=_AEST).strftime("%Y-%m-%d")
        except Exception:
            return ""
    s = str(raw)
    # ISO 8601: "2024-01-15T00:00:00.000Z" or "2024-01-15T00:00:00.000+11:00"
    if len(s) >= 10 and s[4] == "-":
        return s[:10]
    return s[:10]


def parse_asx_filing(item: dict, symbol: str) -> ASXFiling:
    doc_key = item.get("documentKey", "")
    companies = item.get("companies", [])
    company_name = companies[0].get("name", symbol) if companies else symbol
    symbol_display = companies[0].get("symbolDisplay", symbol) if companies else symbol

    # Date can live under "date", "broadcastDate", or "tradingDate" depending on endpoint
    raw_date = (
        item.get("date")
        or item.get("broadcastDate")
        or item.get("tradingDate")
        or ""
    )

    return ASXFiling(
        doc_key=doc_key,
        symbol=symbol_display.upper(),
        company_name=company_name,
        headline=item.get("headline", ""),
        date=_parse_asx_date(raw_date),
        file_size=item.get("fileSize"),
        announcement_types=item.get("announcementTypes", []),
        heading_id=item.get("headingId"),
        is_price_sensitive=item.get("isPriceSensitive", False),
        is_cancelled=item.get("isCancelled", False),
        pdf_url=f"{CDN_BASE}/file/{doc_key}",
    )


def _date_params(from_date: Optional[str], to_date: Optional[str]) -> dict:
    """Build ASX date-range query params. Both None → allDates."""
    if not from_date and not to_date:
        return {"dateRangeType": "allDates"}
    today = date.today().isoformat()
    return {
        "dateRangeType": "custom",
        "startDate": from_date or "1995-01-01",
        "endDate": to_date or today,
    }


def _extract_page_info(data: dict) -> tuple[list, int, int]:
    """Return (items, total_count, page_count) from an ASX API response."""
    inner = data.get("data", {}) or {}
    items = inner.get("announcementsTimeline") or inner.get("items") or []
    total_count = int(inner.get("totalCount", len(items)))
    page_count  = int(inner.get("pageCount",  max(1, -(-total_count // max(len(items), 1)))))
    return items, total_count, page_count


async def _asx_get(url: str, params) -> dict:
    """GET to ASX Markit API with 3-attempt retry on connectivity errors."""
    last_exc = None
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        for attempt in range(3):
            try:
                r = await client.get(url, headers=HEADERS, params=params)
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as e:
                raise HTTPException(
                    status_code=e.response.status_code,
                    detail=f"ASX API error {e.response.status_code}: {e.response.text[:300]}",
                )
            except httpx.ConnectError as e:
                last_exc = e
                logger.warning("ASX connect error (attempt %d/3): %s", attempt + 1, e)
                await asyncio.sleep(2 ** attempt)
            except httpx.RequestError as e:
                raise HTTPException(status_code=503, detail=f"ASX API unreachable: {e}")
    raise HTTPException(status_code=503, detail=f"ASX API unreachable after retries: {last_exc}")


# ── NEW: Predictive Search ────────────────────────────────────────────────────

@router.get("/search", summary="Predictive company search — returns entityXId for use in /market filters")
async def search_asx_companies(
    q: str = Query(..., min_length=1, description="Search text — company name or ticker (e.g. 'TLM', 'BHP', 'Talisman')"),
    max_results: int = Query(10, ge=1, le=50, description="Max suggestions to return"),
):
    """
    Typeahead / predictive search for ASX-listed companies.

    Returns company suggestions including their `entity_xid` which you should
    pass as `entity_xid` to `/api/asx/market` or `/api/asx/market/all` to
    filter announcements for that specific company.

    **Correct search → filings flow:**
    1. `GET /api/asx/search?q=TLM` → pick your company, note `entity_xid`
    2. `GET /api/asx/market?entity_xid=2041721278` → filings for that company

    Examples:
    - `/api/asx/search?q=TLM`
    - `/api/asx/search?q=BHP`
    - `/api/asx/search?q=common%20wealth%20bank`
    """
    url = f"{ASX_API}/search/predictive"
    params = {"searchText": q}

    data = await _asx_get(url, params)

    # The predictive endpoint returns a list directly or nested under data/results
    raw_results: list = []
    if isinstance(data, list):
        raw_results = data
    elif isinstance(data, dict):
        raw_results = (
            data.get("data")
            or data.get("results")
            or data.get("companies")
            or []
        )
        if isinstance(raw_results, dict):
            # Sometimes nested one more level
            raw_results = raw_results.get("companies") or raw_results.get("items") or []

    raw_results = raw_results[:max_results]

    results: list[ASXSearchResult] = []
    for item in raw_results:
        results.append(ASXSearchResult(
            entity_xid=item.get("xidEntity"),   # confirmed field name from DevTools
            xid=item.get("xid"),
            symbol=item.get("symbol"),
            company_name=item.get("displayName"),
            industry_group=item.get("industryGroup"),
            market_cap=item.get("marketCap"),
            price_last=item.get("priceLast"),
        ))

    return {
        "query":   q,
        "count":   len(results),
        "results": results,
        "hint":    "Use entity_xid with /api/asx/market?entity_xid=<id> to filter filings",
    }


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/filings/{symbol}", response_model=ASXResponse, summary="Get filings for an ASX symbol (ticker-based)")
async def get_asx_filings(
    symbol: str,
    page: int = Query(0, ge=0, description="0-based page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page (max 100)"),
    from_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD. Omit for all time"),
    to_date: Optional[str] = Query(None, description="End date YYYY-MM-DD. Default: today"),
    heading_id: Optional[int] = Query(None, description="Announcement type ID — see /api/asx/categories"),
    price_sensitive: Optional[bool] = Query(None, description="True = price-sensitive filings only"),
    exclude_cancelled: bool = Query(True, description="Exclude cancelled documents"),
):
    """
    Fetch filings for an ASX-listed company by ticker symbol.
    Uses the company-specific endpoint (not the market feed).

    Prefer `/api/asx/market?entity_xid=<id>` when you need the same
    filtering as the ASX website (search first, then filter by entityXId).

    Examples:
    - `/api/asx/filings/BHP`
    - `/api/asx/filings/CBA?page_size=50&from_date=2024-01-01`
    - `/api/asx/filings/BHP?heading_id=7` — Annual Reports only
    - `/api/asx/filings/BHP?price_sensitive=true&from_date=2024-01-01`
    """
    url = f"{ASX_API}/companies/{symbol.lower()}/announcements"
    params: dict = {
        "page": page,
        "itemsPerPage": page_size,
        "excludeCanceledDocs": str(exclude_cancelled).lower(),
        **_date_params(from_date, to_date),
    }
    if heading_id is not None:
        params["headingId"] = heading_id
    if price_sensitive is not None:
        params["priceSensitiveOnly"] = str(price_sensitive).lower()

    data = await _asx_get(url, params)
    items, total_count, page_count = _extract_page_info(data)

    return ASXResponse(
        total=total_count,
        total_pages=page_count,
        page=page,
        page_size=page_size,
        has_more=(page + 1) < page_count,
        filings=[parse_asx_filing(item, symbol) for item in items],
    )


@router.get("/filings/{symbol}/all", summary="Fetch ALL filings for an ASX symbol (auto-paginates)")
async def get_asx_filings_all(
    symbol: str,
    from_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD. Omit for all time"),
    to_date: Optional[str] = Query(None, description="End date YYYY-MM-DD. Default: today"),
    heading_id: Optional[int] = Query(None, description="Announcement type heading ID"),
    price_sensitive: Optional[bool] = Query(None),
    exclude_cancelled: bool = Query(True),
    max_items: int = Query(1000, ge=1, le=10000, description="Safety cap — maximum items to return"),
):
    """
    Auto-paginates through every page for a company symbol and returns all filings
    up to `max_items` (default 1000, max 10 000).

    Examples:
    - `/api/asx/filings/BHP/all` — all BHP filings, capped at 1000
    - `/api/asx/filings/CBA/all?max_items=5000&from_date=2020-01-01`
    - `/api/asx/filings/BHP/all?heading_id=7` — all BHP Annual Reports ever filed
    """
    url = f"{ASX_API}/companies/{symbol.lower()}/announcements"
    base_params: dict = {
        "itemsPerPage": 100,
        "excludeCanceledDocs": str(exclude_cancelled).lower(),
        **_date_params(from_date, to_date),
    }
    if heading_id is not None:
        base_params["headingId"] = heading_id
    if price_sensitive is not None:
        base_params["priceSensitiveOnly"] = str(price_sensitive).lower()

    all_filings: list[ASXFiling] = []
    page = 0
    total_on_server = 0

    while len(all_filings) < max_items:
        data = await _asx_get(url, {**base_params, "page": page})
        items, total_count, page_count = _extract_page_info(data)
        total_on_server = total_count

        all_filings.extend(parse_asx_filing(item, symbol) for item in items)

        if page + 1 >= page_count or not items:
            break
        page += 1

    all_filings = all_filings[:max_items]

    return {
        "symbol":          symbol.upper(),
        "total_on_server": total_on_server,
        "returned":        len(all_filings),
        "capped_at":       max_items,
        "filings":         all_filings,
    }


@router.get("/market", summary="Market-wide ASX announcements — filter by entityXId for a specific company")
async def get_asx_market(
    page: int = Query(0, ge=0, description="0-based page number"),
    page_size: int = Query(25, ge=1, le=100, description="Items per page (max 100)"),
    from_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD. Omit for today's feed"),
    to_date: Optional[str] = Query(None, description="End date YYYY-MM-DD. Default: today"),
    entity_xid: Optional[int] = Query(None, description="Company entityXId from /api/asx/search"),
    announcement_types: list[str] = Query(default=[], description="e.g. 'annual report', 'half-yearly report', 'quarterly cash flow report'"),
    industries: list[str] = Query(default=[], description="e.g. 'energy', 'materials', 'software & services'"),
    asset_types: list[str] = Query(default=[], description="e.g. 'equity', 'etf', 'mfund', 'debt', 'other'"),
    price_sensitive: Optional[bool] = Query(None, description="True = price-sensitive only"),
    exclude_cancelled: bool = Query(True),
    include_facets: bool = Query(False, description="Include per-type counts in response (adds latency)"),
):
    """
    Fetch market-wide ASX announcements with full server-side filtering.

    **To filter by company:** search first with `/api/asx/search?q=TLM` to get the
    `entity_xid`, then pass it here. This matches exactly what the ASX website does.

    Filter params match the ASX website DevTools exactly:
    - `announcementTypes[]=annual report` — one or more announcement types
    - `industries[]=energy` — one or more industries
    - `assetTypes[]=equity` — one or more asset types
    - `priceSensitiveOnly=true` — price-sensitive only
    """
    url = f"{ASX_API}/markets/announcements"
    today = date.today().isoformat()
    # Build as list of tuples so array params encode as key[]=v1&key[]=v2 (not percent-encoded)
    params: list = [
        ("page",                page),
        ("itemsPerPage",        page_size),
        ("summaryCountsDate",   today),
        ("excludeCanceledDocs", str(exclude_cancelled).lower()),
        ("includeFacets",       str(include_facets).lower()),
    ]
    for k, v in _date_params(from_date, to_date).items():
        params.append((k, v))
    if entity_xid is not None:
        params.append(("entityXids", entity_xid))
    for t in announcement_types:
        params.append(("announcementTypes[]", t))
    for ind in industries:
        params.append(("industries[]", ind))
    for a in asset_types:
        params.append(("assetTypes[]", a))
    if price_sensitive is not None:
        params.append(("priceSensitiveOnly", str(price_sensitive).lower()))

    data  = await _asx_get(url, params)
    inner = data.get("data", {}) or {}
    items = inner.get("items") or []

    # Market endpoint uses "count" (not "totalCount") and has no "pageCount"
    total_count = int(inner.get("count") or inner.get("totalCount") or len(items))
    page_count  = max(1, -(-total_count // page_size))  # ceiling division

    filings = []
    for item in items:
        companies = item.get("companies", [])
        sym = companies[0].get("symbolDisplay", "UNKNOWN") if companies else "UNKNOWN"
        filings.append(parse_asx_filing(item, sym))

    result: dict = {
        "total":       total_count,
        "total_pages": page_count,
        "page":        page,
        "page_size":   page_size,
        "has_more":    (page + 1) < page_count,
        "filings":     filings,
    }

    if include_facets:
        # Facets live inside summaryCounts on the market endpoint
        summary = inner.get("summaryCounts") or {}
        heading_facets = (
            inner.get("facets", {}).get("headingId")
            or inner.get("facets", {}).get("announcementType")
            or summary.get("headingId")
            or []
        )
        if heading_facets:
            result["facets"] = {
                "heading_types": [
                    {"id": f.get("id"), "name": f.get("name"), "count": f.get("count")}
                    for f in heading_facets
                ]
            }

    return result


@router.get("/market/all", summary="Fetch ALL market announcements (auto-paginates — use with a narrow date range)")
async def get_asx_market_all(
    from_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="End date YYYY-MM-DD. Default: today"),
    entity_xid: Optional[int] = Query(None, description="Company entityXId from /api/asx/search"),
    announcement_types: list[str] = Query(default=[], description="e.g. 'annual report', 'half-yearly report'"),
    industries: list[str] = Query(default=[], description="e.g. 'energy', 'materials'"),
    asset_types: list[str] = Query(default=[], description="e.g. 'equity', 'etf', 'debt'"),
    price_sensitive: Optional[bool] = Query(None),
    exclude_cancelled: bool = Query(True),
    max_items: int = Query(500, ge=1, le=10000, description="Safety cap on total items returned"),
):
    """
    Auto-paginates market-wide announcements and returns all matching filings.
    Use with a narrow date range or `entity_xid` to avoid enormous result sets.
    """
    url   = f"{ASX_API}/markets/announcements"
    today = date.today().isoformat()
    # Fixed params (page added per iteration)
    base_params: list = [
        ("itemsPerPage",        100),
        ("summaryCountsDate",   today),
        ("excludeCanceledDocs", str(exclude_cancelled).lower()),
        ("includeFacets",       "false"),
    ]
    for k, v in _date_params(from_date, to_date).items():
        base_params.append((k, v))
    if entity_xid is not None:
        base_params.append(("entityXids", entity_xid))
    for t in announcement_types:
        base_params.append(("announcementTypes[]", t))
    for ind in industries:
        base_params.append(("industries[]", ind))
    for a in asset_types:
        base_params.append(("assetTypes[]", a))
    if price_sensitive is not None:
        base_params.append(("priceSensitiveOnly", str(price_sensitive).lower()))

    all_filings: list[ASXFiling] = []
    page = 0
    total_on_server = 0

    while len(all_filings) < max_items:
        data  = await _asx_get(url, base_params + [("page", page)])
        inner = data.get("data", {}) or {}
        items = inner.get("items") or []
        total_on_server = int(inner.get("count") or inner.get("totalCount") or len(items))
        page_count      = max(1, -(-total_on_server // 100))

        for item in items:
            companies = item.get("companies", [])
            sym = companies[0].get("symbolDisplay", "UNKNOWN") if companies else "UNKNOWN"
            all_filings.append(parse_asx_filing(item, sym))

        if page + 1 >= page_count or not items:
            break
        page += 1

    all_filings = all_filings[:max_items]

    return {
        "total_on_server": total_on_server,
        "returned":        len(all_filings),
        "capped_at":       max_items,
        "filings":         all_filings,
    }


@router.get("/companies", summary="Search / list ASX-listed companies")
async def get_asx_companies(
    q: Optional[str] = Query(None, description="Filter by company name or symbol"),
    page: int = Query(0, ge=0),
    page_size: int = Query(100, ge=1, le=200, description="Items per page (max 200)"),
):
    """
    Search ASX companies by name or symbol.

    Note: For typeahead/autocomplete use `/api/asx/search` which returns
    `entity_xid` values needed for the market endpoint.

    Examples:
    - `/api/asx/companies` — first 100 companies (alphabetical)
    - `/api/asx/companies?q=BHP`
    - `/api/asx/companies?q=mine&page_size=50`
    """
    url = f"{ASX_API}/companies"
    params: dict = {
        "page":                 page,
        "itemsPerPage":         page_size,
        "includeFilterOptions": "false",
        "pricingDate":          date.today().isoformat(),
    }
    if q:
        params["name"] = q

    data  = await _asx_get(url, params)
    inner = data.get("data", {}) or {}
    items = inner.get("companies") or inner.get("items") or []
    total = int(inner.get("totalCount", len(items)))

    return {
        "total":     total,
        "page":      page,
        "page_size": page_size,
        "companies": items,
    }


@router.get("/categories", summary="ASX announcement types — built-in list or live facet counts")
async def get_asx_categories(
    from_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD for live counts"),
    to_date: Optional[str] = Query(None, description="End date YYYY-MM-DD for live counts"),
):
    """
    Returns ASX announcement-type categories and their heading IDs.

    Without dates: returns the built-in table of known heading IDs.
    With dates: fetches live facets from the ASX market API — shows
    actual counts for each type in the specified period.

    Examples:
    - `/api/asx/categories` — full built-in list
    - `/api/asx/categories?from_date=2024-01-01&to_date=2024-12-31` — 2024 counts
    - `/api/asx/categories?from_date=2026-04-01` — this month's counts
    """
    if not from_date and not to_date:
        return {
            "source":     "built-in",
            "total":      len(HEADING_IDS),
            "categories": [
                {"heading_id": hid, "name": name}
                for hid, name in HEADING_IDS.items()
            ],
        }

    try:
        url   = f"{ASX_API}/markets/announcements"
        today = date.today().isoformat()
        params = {
            "page":              0,
            "itemsPerPage":      1,
            "summaryCountsDate": today,
            "includeFacets":     "true",
            **_date_params(from_date, to_date),
        }
        data  = await _asx_get(url, params)
        inner = data.get("data", {}) or {}
        facets = inner.get("facets", {})
        heading_facets = (
            facets.get("headingId")
            or facets.get("announcementType")
            or []
        )

        if heading_facets:
            return {
                "source":  "live",
                "period":  {"from": from_date, "to": to_date},
                "total":   len(heading_facets),
                "categories": [
                    {
                        "heading_id": f.get("id"),
                        "name":       f.get("name"),
                        "count":      f.get("count"),
                    }
                    for f in heading_facets
                ],
            }
    except Exception as e:
        logger.warning("Live ASX facet fetch failed: %s", e)

    return {
        "source":  "built-in",
        "note":    "Live facet fetch failed — using built-in category list",
        "total":   len(HEADING_IDS),
        "categories": [
            {"heading_id": hid, "name": name}
            for hid, name in HEADING_IDS.items()
        ],
    }


@router.get("/download/{doc_key}", summary="Stream PDF for an ASX filing")
async def download_asx_pdf(doc_key: str):
    """
    Stream the PDF filing directly from ASX CDN.
    `doc_key` is the `documentKey` value from any filing in the response.

    Example: `/api/asx/download/00527652`
    """
    pdf_url = f"{CDN_BASE}/file/{doc_key}"
    hdrs = {**HEADERS, "Accept": "application/pdf,*/*"}

    async def _stream():
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            async with client.stream("GET", pdf_url, headers=hdrs) as r:
                try:
                    r.raise_for_status()
                except httpx.HTTPStatusError as e:
                    raise HTTPException(
                        status_code=e.response.status_code,
                        detail="PDF download failed",
                    )
                async for chunk in r.aiter_bytes(8192):
                    yield chunk

    return StreamingResponse(
        _stream(),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{doc_key}.pdf"'},
    )


@router.get("/debug/market-raw", summary="DEBUG — raw ASX API response for entityXids filter")
async def debug_market_raw(
    entity_xid: int = Query(..., description="entityXId to test, e.g. 204172127 for TLM"),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
):
    """
    Returns the RAW response from the ASX market API when filtered by entityXids.
    Use this to verify:
      1. Whether totalCount changes (= filter is working)
      2. What exact keys are in data/items (= parsing is correct)
      3. Whether items belong to the searched company

    Open in browser: /api/asx/debug/market-raw?entity_xid=204172127
    """
    url   = f"{ASX_API}/markets/announcements"
    today = date.today().isoformat()
    params = {
        "page":              0,
        "itemsPerPage":      5,
        "summaryCountsDate": today,
        "excludeCanceledDocs": "true",
        "includeFacets":     "false",
        "entityXids":        entity_xid,
        **_date_params(from_date, to_date),
    }
    logger.info("DEBUG market-raw params: %s", params)
    data  = await _asx_get(url, params)
    inner = data.get("data", {}) or {}
    items = inner.get("items") or []

    # Show first item's keys so we can verify field names
    first_keys = list(items[0].keys()) if items else []
    companies_in_result = [
        item.get("companies", [{}])[0].get("symbolDisplay", "?")
        for item in items
    ]

    return {
        "params_sent":          params,
        "total_count":          inner.get("totalCount"),
        "page_count":           inner.get("pageCount"),
        "items_returned":       len(items),
        "companies_in_result":  companies_in_result,
        "first_item_keys":      first_keys,
        "raw_data_keys":        list(inner.keys()),
        "first_item_raw":       items[0] if items else None,
    }