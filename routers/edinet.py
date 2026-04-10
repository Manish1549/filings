"""
EDINET (Japan FSA) Router
==========================
Official API — free, requires Subscription-Key (register at disclosure2.edinet-fsa.go.jp)

TO ADD YOUR KEY: set env var EDINET_API_KEY

API base: https://api.edinet-fsa.go.jp/api/v2

Key constraint: DATE-BASED ONLY — no company search endpoint.
  Every query is GET /documents.json?date=YYYY-MM-DD
  We filter by company client-side using a local cache (edinet_companies.json)

Rate limit: 3 seconds between requests (enforced here)

Confirmed fields from API:
  docID, edinetCode, secCode, filerName, docTypeCode, docDescription,
  periodStart, periodEnd, submitDateTime, xbrlFlag, pdfFlag,
  attachDocFlag, englishDocFlag, csvFlag

Doc types we care about:
  120 = Annual securities report (有価証券報告書)
  130 = Amended annual report
  140 = Semi-annual report (半期報告書)
  160 = Quarterly report (四半期報告書)
  180 = Current report (臨時報告書)
  010 = Registration statement
  B10 = Large shareholding report (大量保有報告書)
"""

import asyncio
import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter()
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
# Set via environment variable EDINET_API_KEY (required — no hardcoded fallback)
EDINET_API_KEY = os.environ.get("EDINET_API_KEY", "")

EDINET_BASE    = "https://api.edinet-fsa.go.jp/api/v2"
RATE_LIMIT_SEC = 3.1   # FSA enforces 3s between requests

# Local company cache — grows automatically as we fetch dates
COMPANY_CACHE_FILE = Path(__file__).parent.parent / "edinet_companies.json"

HEADERS = {
    "User-Agent": "GlobalFilings research@globalfilings.com",
    "Accept": "application/json",
}

# Known document type codes → readable labels
DOC_TYPE_MAP = {
    "010": "Registration statement",
    "020": "Amended registration",
    "030": "Prospectus",
    "040": "Amended prospectus",
    "100": "Securities notification",
    "110": "Amended securities notification",
    "120": "Annual securities report",
    "130": "Amended annual report",
    "135": "Confirmation letter",
    "140": "Semi-annual report",
    "150": "Amended semi-annual report",
    "160": "Quarterly report",
    "170": "Amended quarterly report",
    "180": "Current report (extraordinary)",
    "190": "Amended current report",
    "200": "Tender offer notification",
    "210": "Amended tender offer",
    "300": "Parent company statement",
    "310": "Amended parent company statement",
    "400": "Share buyback report",
    "410": "Amended share buyback report",
    "B10": "Large shareholding report",
    "B20": "Amended large shareholding report",
    "F10": "Fund securities notification",
}

DOC_CATEGORY_MAP = {
    "120": "Annual Report",
    "130": "Annual Report",
    "140": "Semi-Annual Report",
    "150": "Semi-Annual Report",
    "160": "Quarterly Report",
    "170": "Quarterly Report",
    "180": "Current Report",
    "190": "Current Report",
    "B10": "Large Shareholding",
    "B20": "Large Shareholding",
    "200": "Tender Offer",
    "210": "Tender Offer",
    "400": "Share Buyback",
}


# ── Company cache ─────────────────────────────────────────────────────────────
def load_company_cache() -> dict:
    """Load edinetCode → {name, secCode} map from local JSON file."""
    if COMPANY_CACHE_FILE.exists():
        try:
            return json.loads(COMPANY_CACHE_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("Failed to load EDINET company cache from %s: %s", COMPANY_CACHE_FILE, e)
    return {}


def save_company_cache(cache: dict):
    """Save updated company cache to disk."""
    try:
        COMPANY_CACHE_FILE.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning("Failed to save EDINET company cache to %s: %s", COMPANY_CACHE_FILE, e)


def update_cache_from_results(items: list) -> dict:
    """Add any new companies found in API results to local cache."""
    cache = load_company_cache()
    changed = False
    for item in items:
        code = item.get("edinetCode", "")
        if code and code not in cache:
            cache[code] = {
                "name": item.get("filerName", ""),
                "sec_code": item.get("secCode", ""),
            }
            changed = True
    if changed:
        save_company_cache(cache)
    return cache


# ── Pydantic models ───────────────────────────────────────────────────────────
class EDINETFiling(BaseModel):
    doc_id: str
    edinet_code: Optional[str] = None
    sec_code: Optional[str] = None
    filer_name: Optional[str] = None
    doc_type_code: Optional[str] = None
    doc_type_label: Optional[str] = None
    category: Optional[str] = None
    doc_description: Optional[str] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    submit_datetime: Optional[str] = None
    has_xbrl: bool = False
    has_pdf: bool = False
    has_english: bool = False
    has_csv: bool = False
    pdf_url: Optional[str] = None
    viewer_url: Optional[str] = None


class EDINETResponse(BaseModel):
    date: str
    total: int
    filings: list[EDINETFiling]
    note: Optional[str] = None


class EDINETCompany(BaseModel):
    edinet_code: str
    name: str
    sec_code: Optional[str] = None


# ── Helpers ───────────────────────────────────────────────────────────────────
def has_key() -> bool:
    return bool(EDINET_API_KEY) and EDINET_API_KEY != "YOUR_EDINET_KEY_HERE"


def require_key():
    if not has_key():
        raise HTTPException(
            status_code=503,
            detail={
                "error": "EDINET API key not configured",
                "how_to_fix": "Register free at https://disclosure2.edinet-fsa.go.jp then set the EDINET_API_KEY environment variable",
            },
        )


def parse_filing(item: dict) -> EDINETFiling:
    doc_id   = item.get("docID", "")
    doc_type = item.get("docTypeCode", "") or ""

    def flag(key): return item.get(key) == "1"

    return EDINETFiling(
        doc_id=doc_id,
        edinet_code=item.get("edinetCode"),
        sec_code=item.get("secCode") or None,
        filer_name=item.get("filerName"),
        doc_type_code=doc_type,
        doc_type_label=DOC_TYPE_MAP.get(doc_type, doc_type or "Unknown"),
        category=DOC_CATEGORY_MAP.get(doc_type, "Other"),
        doc_description=item.get("docDescription"),
        period_start=item.get("periodStart"),
        period_end=item.get("periodEnd"),
        submit_datetime=item.get("submitDateTime") or "",
        has_xbrl=flag("xbrlFlag"),
        has_pdf=flag("pdfFlag"),
        has_english=flag("englishDocFlag"),
        has_csv=flag("csvFlag"),
        pdf_url=(
            f"{EDINET_BASE}/documents/{doc_id}?type=4&Subscription-Key={EDINET_API_KEY}"
            if doc_id and has_key() and flag("pdfFlag") else None
        ),
        viewer_url=(
            f"https://disclosure2.edinet-fsa.go.jp/WZEK0040.aspx?S{doc_id}"
            if doc_id else None
        ),
    )


async def fetch_date(target_date: str) -> list:
    """
    Fetch all filings for a single date from EDINET.
    type=2 returns full document metadata (type=1 is list only).
    """
    require_key()
    params = {"date": target_date, "type": "2", "Subscription-Key": EDINET_API_KEY}
    last_exc = None
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        for attempt in range(3):
            try:
                r = await client.get(f"{EDINET_BASE}/documents.json", headers=HEADERS, params=params)
                r.raise_for_status()
                return r.json().get("results", [])
            except httpx.HTTPStatusError as e:
                raise HTTPException(
                    status_code=e.response.status_code,
                    detail={
                        "error": f"EDINET returned {e.response.status_code}",
                        "date": target_date,
                        "body": e.response.text[:300],
                    },
                )
            except httpx.ConnectError as e:
                last_exc = e
                logger.warning("EDINET connection error for %s (attempt %d/3): %s", target_date, attempt + 1, e)
                await asyncio.sleep(2 ** attempt)
            except httpx.RequestError as e:
                raise HTTPException(status_code=503, detail=f"EDINET unreachable: {str(e)}")
    raise HTTPException(status_code=503, detail=f"EDINET unreachable after retries: {str(last_exc)}")


# ── Endpoints ─────────────────────────────────────────────────────────────────
@router.get("/filings", response_model=EDINETResponse, summary="Get EDINET filings by date")
async def get_edinet_filings(
    date_str: Optional[str] = Query(None, alias="date", description="Filing date YYYY-MM-DD. Defaults to today."),
    doc_type: Optional[str] = Query(None, description="Filter by doc type code: 120 (annual), 160 (quarterly), 180 (current), B10 (shareholding)"),
    company: Optional[str] = Query(None, description="Filter by company name (partial match, case-insensitive)"),
    sec_code: Optional[str] = Query(None, description="Filter by securities code (e.g. 7203 for Toyota)"),
    has_english: bool = Query(False, description="Only return filings with English version"),
    has_pdf: bool = Query(False, description="Only return filings with PDF"),
):
    """
    Fetch all EDINET filings for a given date.

    EDINET only supports date-based queries — this returns ALL filings
    for that day, then filters client-side by company/type/etc.

    Examples:
    - `/api/edinet/filings?date=2026-03-30`
    - `/api/edinet/filings?date=2026-03-30&doc_type=120`
    - `/api/edinet/filings?date=2026-03-30&company=Toyota`
    - `/api/edinet/filings?date=2026-03-30&sec_code=7203`
    - `/api/edinet/filings?date=2026-03-30&has_english=true`
    """
    target = date_str or date.today().isoformat()

    try:
        d = date.fromisoformat(target)
        if d.weekday() >= 5:
            return EDINETResponse(
                date=target, total=0, filings=[],
                note=f"{target} is a {'Saturday' if d.weekday()==5 else 'Sunday'} — EDINET has no filings on weekends. Try a weekday.",
            )
    except ValueError:
        pass

    items = await fetch_date(target)
    update_cache_from_results(items)
    filings = [parse_filing(i) for i in items]

    if doc_type:
        filings = [f for f in filings if f.doc_type_code == doc_type]
    if company:
        q = company.lower()
        filings = [f for f in filings if f.filer_name and q in f.filer_name.lower()]
    if sec_code:
        filings = [f for f in filings if f.sec_code == sec_code]
    if has_english:
        filings = [f for f in filings if f.has_english]
    if has_pdf:
        filings = [f for f in filings if f.has_pdf]

    return EDINETResponse(date=target, total=len(filings), filings=filings)


@router.get("/filings/range", response_model=list[EDINETResponse], summary="Get EDINET filings for a date range")
async def get_edinet_range(
    from_date: str = Query(..., description="Start date YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="End date YYYY-MM-DD. Defaults to today."),
    doc_type: Optional[str] = Query(None, description="Filter by doc type code"),
    company: Optional[str] = Query(None, description="Filter by company name"),
    sec_code: Optional[str] = Query(None, description="Filter by securities code"),
    has_english: bool = Query(False),
    has_pdf: bool = Query(False),
    max_days: int = Query(7, ge=1, le=30, description="Max days to fetch (each day = 1 API call with 3s gap)"),
):
    """
    Fetch EDINET filings across a date range.

    WARNING: Each date = 1 API call with 3s enforced gap.
    max_days=7 takes ~21 seconds minimum.
    max_days=30 takes ~90 seconds.

    Examples:
    - `/api/edinet/filings/range?from_date=2026-03-24&to_date=2026-03-30&doc_type=120`
    - `/api/edinet/filings/range?from_date=2026-03-24&company=Toyota&max_days=7`
    """
    require_key()

    start = date.fromisoformat(from_date)
    end   = date.fromisoformat(to_date) if to_date else date.today()

    if (end - start).days + 1 > max_days:
        end = start + timedelta(days=max_days - 1)

    results = []
    current = start

    while current <= end:
        if current.weekday() < 5:
            try:
                items = await fetch_date(current.isoformat())
                update_cache_from_results(items)
                filings = [parse_filing(i) for i in items]

                if doc_type:
                    filings = [f for f in filings if f.doc_type_code == doc_type]
                if company:
                    q = company.lower()
                    filings = [f for f in filings if f.filer_name and q in f.filer_name.lower()]
                if sec_code:
                    filings = [f for f in filings if f.sec_code == sec_code]
                if has_english:
                    filings = [f for f in filings if f.has_english]
                if has_pdf:
                    filings = [f for f in filings if f.has_pdf]

                results.append(EDINETResponse(
                    date=current.isoformat(), total=len(filings), filings=filings,
                ))
            except HTTPException as e:
                logger.warning("EDINET fetch failed for %s: %s", current.isoformat(), e.detail)

            if current < end:
                await asyncio.sleep(RATE_LIMIT_SEC)

        current += timedelta(days=1)

    return results


@router.get("/document/{doc_id}", summary="Download EDINET document")
async def download_edinet_doc(
    doc_id: str,
    type: int = Query(
        2,
        description=(
            "1 = metadata only (JSON), "
            "2 = main document ZIP (XBRL + PDF), "
            "4 = PDF only (if available), "
            "5 = English doc (if available)"
        ),
    ),
):
    """
    Download a filing document from EDINET.

    doc_id comes from the `doc_id` field in /filings response.

    type options:
    - 1 = document list (JSON)
    - 2 = main document ZIP (XBRL + PDF inside)
    - 4 = PDF only
    - 5 = English version (if available)

    Examples:
    - `/api/edinet/document/S100ABCD?type=4` — PDF only
    - `/api/edinet/document/S100ABCD?type=2` — full ZIP with XBRL
    """
    require_key()
    url = f"{EDINET_BASE}/documents/{doc_id}"
    params = {"type": type, "Subscription-Key": EDINET_API_KEY}

    # type=1 returns JSON metadata — fetch fully, don't stream
    if type == 1:
        try:
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                r = await client.get(url, headers=HEADERS, params=params)
                r.raise_for_status()
                return r.json()
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail="EDINET metadata fetch failed")
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Download failed: {str(e)}")

    # Binary types — stream
    filename = f"{doc_id}_type{type}"
    if type == 2:
        filename += ".zip"
        media_type = "application/zip"
    else:
        filename += ".pdf"
        media_type = "application/pdf"

    async def _stream():
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            async with client.stream("GET", url, headers=HEADERS, params=params) as r:
                try:
                    r.raise_for_status()
                except httpx.HTTPStatusError as e:
                    raise HTTPException(status_code=e.response.status_code, detail="EDINET download failed")
                async for chunk in r.aiter_bytes(8192):
                    yield chunk

    return StreamingResponse(
        _stream(),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/companies", summary="List cached EDINET companies")
async def get_edinet_companies(
    q: Optional[str] = Query(None, description="Search by company name"),
    sec_code: Optional[str] = Query(None, description="Filter by securities code"),
):
    """
    Return companies from local cache (edinet_companies.json).
    Cache builds automatically as you query /filings.
    Initially empty — grows with each date you fetch.

    Examples:
    - `/api/edinet/companies?q=Toyota`
    - `/api/edinet/companies?sec_code=7203`
    """
    cache = load_company_cache()
    companies = [
        EDINETCompany(edinet_code=k, name=v["name"], sec_code=v.get("sec_code"))
        for k, v in cache.items()
    ]

    if q:
        q_lower = q.lower()
        companies = [c for c in companies if q_lower in c.name.lower()]
    if sec_code:
        companies = [c for c in companies if c.sec_code == sec_code]

    return {"total": len(companies), "companies": companies[:300]}


@router.get("/doc-types", summary="List EDINET document type codes")
async def get_doc_types():
    """Return all known EDINET document type codes with labels."""
    return {
        "doc_types": [
            {"code": k, "label": v, "category": DOC_CATEGORY_MAP.get(k, "Other")}
            for k, v in DOC_TYPE_MAP.items()
        ]
    }


@router.get("/raw", summary="Raw EDINET response for debugging")
async def raw_edinet(
    date_str: Optional[str] = Query(None, alias="date"),
    type_num: int = Query(2, alias="type"),
):
    """Returns the raw EDINET API response — use this to debug response structure."""
    require_key()
    target = date_str or date.today().isoformat()
    params = {"date": target, "type": str(type_num), "Subscription-Key": EDINET_API_KEY}
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(f"{EDINET_BASE}/documents.json", headers=HEADERS, params=params)
            raw = r.json()
            results_raw = raw.get("results", "KEY_NOT_FOUND")
            return {
                "http_status": r.status_code,
                "url_called": str(r.url),
                "top_level_keys": list(raw.keys()) if isinstance(raw, dict) else "not a dict",
                "results_type": type(results_raw).__name__,
                "results_count": len(results_raw) if isinstance(results_raw, list) else "N/A",
                "metadata": raw.get("metadata", {}),
                "first_item": results_raw[0] if isinstance(results_raw, list) and results_raw else "EMPTY",
            }
    except Exception as e:
        logger.warning("EDINET raw endpoint error: %s", e)
        return {"error": str(e)}


@router.get("/status", summary="Check EDINET API key status")
async def get_edinet_status():
    """Check if EDINET API key is configured and working."""
    if not has_key():
        return {
            "configured": False,
            "message": "No API key set. Register at https://disclosure2.edinet-fsa.go.jp",
            "how_to_set": "Set the EDINET_API_KEY environment variable",
        }

    params = {"date": date.today().isoformat(), "type": "1", "Subscription-Key": EDINET_API_KEY}
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            r = await client.get(f"{EDINET_BASE}/documents.json", headers=HEADERS, params=params)
            if r.status_code == 200:
                return {
                    "configured": True,
                    "key_works": True,
                    "test_date": date.today().isoformat(),
                    "filings_today": len(r.json().get("results", [])),
                }
            return {
                "configured": True,
                "key_works": False,
                "status_code": r.status_code,
                "message": r.text[:200],
            }
    except Exception as e:
        return {"configured": True, "key_works": False, "error": str(e)}
