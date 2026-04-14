"""
SGX (Singapore Exchange) Router — Production Level
====================================================
All endpoints confirmed from DevTools screenshots + appconfig.json

BASE APIs:
  Market feed: https://api.sgx.com/announcements/v1.1/         (no company filter)
  Company:     https://api.sgx.com/announcements/v1.1/company  (value= + exactsearch=true)
  Count:       https://api.sgx.com/announcements/v1.1/count
  Companylist: https://api.sgx.com/announcements/v1.1/companylist
  Securitylist:https://api.sgx.com/announcements/v1.1/securitylist
  Categories:  https://api2.sgx.com/content-api?queryId=...
  PDFs:        https://links.sgx.com/1.0.0/corporate-announcements/{id}/{hash}

CONFIRMED PARAMS (DevTools):
  periodstart  YYYYMMDD_HHMMSS  (e.g. 20060410_160000 for full history)
  periodend    YYYYMMDD_HHMMSS
  cat          comma-separated category codes  (e.g. ANNC,FINSTMT)
  sub          sub-category code
  pagestart    0-based page number
  pagesize     items per page (max 100 on SGX side)

COMPANY ENDPOINT PARAMS (confirmed from DevTools):
  value        company name (uppercased)  — replaces "company" param
  exactsearch  "true"                     — exact match on company name
  Default periodstart for company queries = SGX_EARLIEST (full history since 2006)

COUNT ENDPOINT (DevTools):
  GET /v1.1/count?periodstart=...&periodend=...
  Response: { "meta": {..., "totalItems": 1}, "data": 816772 }
  → "data" is the raw integer count

FULL HISTORY: SGX data starts from ~2006-04-10 (periodstart=20060410_160000)
  Full corpus = 816,772 filings (as of Apr 2026)

AUTH:
  Token fetched from CMS API (api2.sgx.com/content-api), ROT13-decoded.
  Falls back to dynamic discovery (homepage → appconfig.js → CMS_VERSION).
  Token cached per singleton, invalidated on 401.

WEBSITE CATEGORY GROUPS (4 groups matching the UI):
  Announcements            (ANNC + sub-codes)
  Corporate Action         (DIV, ACQDIS, RIGHTS, etc.)
  Product Announcements    (NEWLISTING, DELISTINGS, etc.)
  Trading Status           (SUSP, RESUME, etc.)
"""

import asyncio
import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional
from urllib.parse import quote

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Constants ─────────────────────────────────────────────────────────────────
SGX_HOME     = "https://www.sgx.com"
SGX_API_BASE = "https://api.sgx.com/announcements/v1.1"
SGX_LINKS    = "https://links.sgx.com/1.0.0/corporate-announcements"

CMS_API_URL    = "https://api2.sgx.com/content-api"
CMS_QUERY_NAME = "we_chat_qr_validator"
CMS_VERSION    = "70f75ec90c030bab34d750ee55d74b016f70d4b6"

# Full SGX history starts here (confirmed from DevTools)
SGX_EARLIEST = "20060410_160000"

APPCONFIG_RE   = re.compile(r'appconfig\.js\?v=([\w]+)')
CMS_VERSION_RE = re.compile(r'"CMS_VERSION"\s*:\s*"([a-f0-9]+)"')

BASE_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.sgx.com",
    "Referer": "https://www.sgx.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}

# Website category groups — mirrors the 4 dropdown groups on sgx.com
CATEGORY_GROUPS = {
    "Announcements": [
        {"code": "ANNC",    "label": "All Announcements"},
        {"code": "ANNC01",  "label": "Amendment to Articles"},
        {"code": "ANNC02",  "label": "Regulatory Actions"},
        {"code": "ANNC03",  "label": "Announcement of Appointment"},
        {"code": "ANNC04",  "label": "Changes in Interested Person Transactions"},
        {"code": "ANNC05",  "label": "Changes in Interested Person Transactions (Circular)"},
        {"code": "ANNC06",  "label": "Changes in Substantial Shareholdings"},
        {"code": "ANNC07",  "label": "Changes to Board Committees"},
        {"code": "ANNC08",  "label": "Changes to Share Capital"},
        {"code": "ANNC09",  "label": "Cessation of Director/Key Personnel"},
        {"code": "ANNC10",  "label": "Code of Corporate Governance"},
        {"code": "ANNC11",  "label": "Interested Person Transactions"},
        {"code": "ANNC12",  "label": "Interested Person Transactions (Circular)"},
        {"code": "ANNC13",  "label": "Dealings in Securities by Directors"},
        {"code": "ANNC14",  "label": "Letter to Shareholders"},
        {"code": "ANNC15",  "label": "Material Transactions"},
        {"code": "ANNC16",  "label": "Material Transactions (Circular)"},
        {"code": "ANNC17",  "label": "Miscellaneous"},
        {"code": "ANNC18",  "label": "Notices & Circulars"},
        {"code": "ANNC19",  "label": "Notice of Shareholders Meeting"},
        {"code": "ANNC20",  "label": "Overseas Regulatory Announcement"},
        {"code": "ANNC21",  "label": "Overseas Regulatory Announcement (Price Sensitive)"},
        {"code": "ANNC22",  "label": "Profit Guidance / Warning"},
        {"code": "ANNC23",  "label": "Placements"},
        {"code": "ANNC24",  "label": "Prospectus / Offer Information Statement"},
        {"code": "ANNC25",  "label": "Response to SGX Queries"},
        {"code": "ANNC26",  "label": "Shareholder Rights Plan"},
        {"code": "ANNC27",  "label": "Share Buy Back - Immediate Announcement"},
        {"code": "ANNC28",  "label": "Share Buy Back - Daily Announcement"},
        {"code": "ANNC29",  "label": "Takeover"},
        {"code": "ANNC30",  "label": "Voluntary Delisting"},
        {"code": "FINSTMT", "label": "Financial Statements & Related"},
        {"code": "OTHERS",  "label": "Others"},
    ],
    "Corporate Action": [
        {"code": "ACQDIS",  "label": "Acquisitions & Disposals"},
        {"code": "DIV",     "label": "Dividends"},
        {"code": "RIGHTS",  "label": "Rights Issue"},
        {"code": "BONUS",   "label": "Bonus Issue"},
        {"code": "SUBDIV",  "label": "Subdivision of Shares"},
        {"code": "CONSOLID","label": "Consolidation of Shares"},
        {"code": "CAPRED",  "label": "Capital Reduction"},
        {"code": "SCHARE",  "label": "Scheme of Arrangement"},
        {"code": "TENDER",  "label": "Tender / Exit Offer"},
        {"code": "WARRANT", "label": "Warrants"},
    ],
    "Product Announcements & Listings": [
        {"code": "NEWLISTING",  "label": "New Listing"},
        {"code": "ADDLISTING",  "label": "Additional Listing"},
        {"code": "DELIST",      "label": "Delisting"},
        {"code": "NAMECHANGE",  "label": "Name Change"},
        {"code": "LOTSIZE",     "label": "Change in Lot Size"},
        {"code": "TRANSFER",    "label": "Transfer of Listing"},
        {"code": "DUALLISTING", "label": "Dual Listing"},
    ],
    "Trading Status": [
        {"code": "SUSP",    "label": "Suspension"},
        {"code": "RESUME",  "label": "Resumption of Trading"},
        {"code": "TRADING", "label": "Trading Halt / Lift"},
    ],
}


# ── Auth helpers ──────────────────────────────────────────────────────────────
def rot13(s: str) -> str:
    return "".join(
        chr((ord(c) - 97 + 13) % 26 + 97) if "a" <= c <= "z" else
        chr((ord(c) - 65 + 13) % 26 + 65) if "A" <= c <= "Z" else c
        for c in s
    )


def _cms_token_url(version: str) -> str:
    return f"{CMS_API_URL}/?queryId={version}:{CMS_QUERY_NAME}"


def _cms_taxonomy_url(version: str) -> str:
    vars_enc = '%7B%22vid%22%3A%22company_announcements_categories%22%2C%22lang%22%3A%22EN%22%7D'
    return f"{CMS_API_URL}?queryId={version}%3Ataxonomy_terms&variables={vars_enc}"


# ── SGX client ────────────────────────────────────────────────────────────────
class SGXClient:
    """
    Manages SGX auth token lifecycle.
    Module-level singleton — token persists across all requests.
    """

    def __init__(self):
        self._token: str | None = None
        self._cms_version: str = CMS_VERSION

    async def _fetch_token_for_version(self, client: httpx.AsyncClient, version: str) -> str | None:
        url = _cms_token_url(version)
        logger.info("Fetching SGX token from CMS (version=%s...)", version[:8])
        res = await client.get(url, headers=BASE_HEADERS)
        if res.status_code != 200:
            logger.warning("CMS returned %d for token fetch", res.status_code)
            return None
        qr = res.json().get("data", {}).get("qrValidator")
        if not qr:
            logger.warning("qrValidator not found in CMS response")
            return None
        return rot13(qr)

    async def _discover_cms_version(self, client: httpx.AsyncClient) -> str:
        logger.info("Discovering CMS version dynamically from SGX frontend...")
        res = await client.get(SGX_HOME, headers=BASE_HEADERS)
        res.raise_for_status()

        match = APPCONFIG_RE.search(res.text)
        if not match:
            raise RuntimeError("Could not find appconfig.js URL in SGX homepage")

        config_url = f"{SGX_HOME}/config/appconfig.js?v={match.group(1)}"
        logger.info("Found appconfig URL: %s", config_url)

        res = await client.get(config_url, headers=BASE_HEADERS)
        res.raise_for_status()

        match = CMS_VERSION_RE.search(res.text)
        if not match:
            raise RuntimeError("Could not find CMS_VERSION in appconfig.js")

        version = match.group(1)
        logger.info("Discovered CMS_VERSION: %s...", version[:8])
        return version

    async def _get_token(self, client: httpx.AsyncClient, *, allow_rediscover: bool = True) -> str:
        if self._token:
            return self._token

        token = await self._fetch_token_for_version(client, self._cms_version)

        if not token and allow_rediscover:
            logger.warning("Hardcoded CMS version failed — falling back to dynamic discovery")
            try:
                new_version = await self._discover_cms_version(client)
                self._cms_version = new_version
                token = await self._fetch_token_for_version(client, new_version)
            except Exception as e:
                logger.warning("Dynamic CMS version discovery failed: %s", e)

        if not token:
            raise HTTPException(
                status_code=503,
                detail="Failed to obtain SGX auth token — CMS API unavailable",
            )

        self._token = token
        return self._token

    def invalidate_token(self):
        logger.info("Invalidating cached SGX token")
        self._token = None

    async def get(self, url: str, params: dict = None) -> dict:
        """Authenticated GET with 401 → token invalidation + retry."""
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            token = await self._get_token(client)
            headers = {**BASE_HEADERS, "authorizationtoken": token}

            res = await client.get(url, headers=headers, params=params)

            if res.status_code in (401, 403):
                logger.warning("%d from SGX API — invalidating token and retrying with rediscovery", res.status_code)
                self.invalidate_token()
                # Force CMS version rediscovery on 403 (token may be permanently revoked)
                if res.status_code == 403:
                    try:
                        self._cms_version = await self._discover_cms_version(client)
                    except Exception as e:
                        logger.warning("CMS rediscovery failed: %s", e)
                token = await self._get_token(client)
                headers["authorizationtoken"] = token
                res = await client.get(url, headers=headers, params=params)

            if res.status_code >= 400:
                raise HTTPException(
                    status_code=res.status_code,
                    detail={
                        "error": f"SGX API {res.status_code}",
                        "url_called": str(res.url),
                        "body": res.text[:400],
                    },
                )
            return res.json()

    async def get_unauthenticated(self, url: str, params: dict = None) -> dict:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            res = await client.get(url, headers=BASE_HEADERS, params=params)
            if res.status_code >= 400:
                raise HTTPException(
                    status_code=res.status_code,
                    detail={"error": f"SGX CMS {res.status_code}", "body": res.text[:400]},
                )
            return res.json()


_sgx = SGXClient()

# ── Company cache (loaded once, used for typeahead) ────────────────────────────
_company_cache: list[dict] = []
_company_cache_loaded: bool = False

async def _ensure_company_cache() -> None:
    """Fetch /companylist once and keep it in memory for fast typeahead filtering."""
    global _company_cache, _company_cache_loaded
    if _company_cache_loaded:
        return
    try:
        data = await _sgx.get(SGX_API_BASE + "/companylist")
        companies = data.get("data", []) or (data if isinstance(data, list) else [])
        _company_cache = companies
        _company_cache_loaded = True
        logger.info("SGX company cache loaded: %d companies", len(companies))
    except Exception as e:
        logger.warning("SGX company cache load failed: %s", e)


# ── Date helpers ──────────────────────────────────────────────────────────────
def to_sgx_dt(d: Optional[str], end_of_day: bool = False) -> str:
    """
    Convert date string → SGX format YYYYMMDD_HHMMSS.
    Accepts YYYY-MM-DD or YYYYMMDD. Raises HTTP 422 on invalid input.
    Pass d=None for defaults:
      start → 30 days ago 00:00:00
      end   → today 23:59:59
    """
    if not d:
        today = date.today()
        if end_of_day:
            return today.strftime("%Y%m%d") + "_235959"
        return (today - timedelta(days=30)).strftime("%Y%m%d") + "_000000"

    clean = d.replace("-", "")
    if len(clean) != 8 or not clean.isdigit():
        raise HTTPException(
            status_code=422,
            detail=f"Invalid date {d!r} — expected YYYY-MM-DD or YYYYMMDD",
        )
    try:
        datetime.strptime(clean, "%Y%m%d")
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid date {d!r} — date does not exist",
        )
    return clean + ("_235959" if end_of_day else "_000000")


def _build_params(
    from_date: Optional[str],
    to_date: Optional[str],
    cat: Optional[str],
    sub: Optional[str],
    company: Optional[str],
    page: int,
    page_size: int,
) -> dict:
    """Build params for the market-wide /v1.1/ endpoint (no company filter)."""
    params: dict = {
        "periodstart": to_sgx_dt(from_date, end_of_day=False),
        "periodend":   to_sgx_dt(to_date,   end_of_day=True),
        "pagestart":   page,
        "pagesize":    page_size,
    }
    if cat:
        params["cat"] = cat
    if sub:
        params["sub"] = sub
    if company:
        params["company"] = company.upper()
    return params


def _company_url(
    from_date: Optional[str],
    to_date: Optional[str],
    company: str,
    cat: Optional[str],
    sub: Optional[str],
    page: int,
    page_size: int,
) -> str:
    """
    Build the full URL for the /v1.1/company endpoint with %20-encoded spaces.

    SGX requires RFC 3986 percent-encoding (%20) for spaces in the value param,
    not form-encoding (+). We build the query string manually with urllib.parse.quote
    so spaces in company names are never encoded as +.
    """
    # quote() uses %20 for spaces; quote_plus() would use + (wrong for SGX)
    def qv(v: str) -> str:
        return quote(str(v), safe="")

    pairs = [
        f"periodstart={qv(to_sgx_dt(from_date, end_of_day=False) if from_date else SGX_EARLIEST)}",
        f"periodend={qv(to_sgx_dt(to_date, end_of_day=True))}",
        f"value={qv(company.upper())}",
        "exactsearch=true",
        f"pagestart={page}",
        f"pagesize={page_size}",
    ]
    if cat:
        pairs.append(f"cat={qv(cat)}")
    if sub:
        pairs.append(f"sub={qv(sub)}")
    return f"{SGX_API_BASE}/company?{'&'.join(pairs)}"


# ── Pydantic models ───────────────────────────────────────────────────────────
class SGXIssuer(BaseModel):
    isin_code: Optional[str] = None
    stock_code: Optional[str] = None
    security_name: Optional[str] = None
    issuer_name: Optional[str] = None
    ibm_code: Optional[str] = None


class SGXFiling(BaseModel):
    ref_id: Optional[str] = None
    id: Optional[str] = None
    title: str
    category_name: Optional[str] = None
    cat: Optional[str] = None
    sub: Optional[str] = None
    issuer_name: Optional[str] = None
    security_name: Optional[str] = None
    submitted_by: Optional[str] = None
    announcer_name: Optional[str] = None
    submission_date: Optional[str] = None
    broadcast_date_time: Optional[str] = None
    issuers: list[SGXIssuer] = []
    url: Optional[str] = None
    product_category: Optional[str] = None
    is_price_sensitive: Optional[bool] = None


class SGXMeta(BaseModel):
    code: Optional[str] = None
    message: Optional[str] = None
    total_pages: Optional[int] = None
    total_items: Optional[int] = None


class SGXResponse(BaseModel):
    meta: SGXMeta
    total: int
    page: int
    page_size: int
    has_more: bool
    filings: list[SGXFiling]


class SGXCategory(BaseModel):
    id: str
    name: str
    code: Optional[str] = None
    parent_code: Optional[str] = None
    order: Optional[str] = None


# ── Parsers ───────────────────────────────────────────────────────────────────
def parse_filing(item: dict) -> SGXFiling:
    issuers = [
        SGXIssuer(
            isin_code=i.get("isin_code"),
            stock_code=i.get("stock_code"),
            security_name=i.get("security_name"),
            issuer_name=i.get("issuer_name"),
            ibm_code=i.get("ibm_code"),
        )
        for i in (item.get("issuers") or [])
    ]

    sub_dt   = item.get("submission_date_time")
    sub_date = item.get("submission_date", "")
    if isinstance(sub_dt, (int, float)) and sub_dt:
        try:
            sub_date = datetime.fromtimestamp(sub_dt / 1000).strftime("%Y-%m-%d %H:%M")
        except Exception:
            pass

    broadcast_dt  = item.get("broadcast_date_time")
    broadcast_str = None
    if isinstance(broadcast_dt, (int, float)) and broadcast_dt:
        try:
            broadcast_str = datetime.fromtimestamp(broadcast_dt / 1000).strftime("%Y-%m-%d %H:%M")
        except Exception:
            pass

    return SGXFiling(
        ref_id=item.get("ref_id"),
        id=item.get("id"),
        title=item.get("title") or "Untitled",
        category_name=item.get("category_name"),
        cat=item.get("cat"),
        sub=item.get("sub"),
        issuer_name=item.get("issuer_name"),
        security_name=item.get("security_name"),
        submitted_by=item.get("submitted_by"),
        announcer_name=item.get("announcer_name"),
        submission_date=sub_date,
        broadcast_date_time=broadcast_str,
        issuers=issuers,
        url=item.get("url"),
        product_category=item.get("product_category"),
        is_price_sensitive=item.get("is_price_sensitive"),
    )


def _make_response(data: dict, page: int, page_size: int) -> SGXResponse:
    meta_raw = data.get("meta", {})
    items    = data.get("data", []) or []

    # SGX API always returns totalItems = number of items in THIS page (not real total).
    # Infer has_more from whether we received a full page.
    has_more    = len(items) >= page_size
    # Running total: at least what we've fetched so far; show +1 placeholder when more exist
    items_so_far = page * page_size + len(items)
    total_items  = items_so_far + (1 if has_more else 0)   # "+1 so UI knows list continues"

    total_pages = page + (2 if has_more else 1)

    return SGXResponse(
        meta=SGXMeta(
            code=str(meta_raw.get("code", "")),
            message=meta_raw.get("message"),
            total_pages=total_pages,
            total_items=total_items,
        ),
        total=total_items,
        page=page,
        page_size=page_size,
        has_more=has_more,
        filings=[parse_filing(i) for i in items],
    )


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/count", summary="Count SGX announcements for a query (no items returned)")
async def count_sgx(
    from_date: Optional[str] = Query(None, description="Start date YYYYMMDD or YYYY-MM-DD. Omit for full history since 2006"),
    to_date: Optional[str] = Query(None, description="End date YYYYMMDD or YYYY-MM-DD. Default: today"),
    cat: Optional[str] = Query(None, description="Category code(s) comma-separated"),
    sub: Optional[str] = Query(None, description="Sub-category code"),
    company: Optional[str] = Query(None, description="Company/issuer name"),
):
    """
    Fast count of total announcements matching the filters — no items returned.
    Uses the dedicated /count endpoint on SGX API (confirmed from DevTools).

    The full SGX corpus (2006 → today) is ~816,772 filings.

    Examples:
    - `/api/sgx/count` — total all time
    - `/api/sgx/count?from_date=20260101` — YTD count
    - `/api/sgx/count?cat=FINSTMT` — total financial statements
    - `/api/sgx/count?company=DBS` — total DBS announcements
    """
    params: dict = {
        "periodstart": from_date and to_sgx_dt(from_date) or SGX_EARLIEST,
        "periodend":   to_sgx_dt(to_date, end_of_day=True),
    }
    if cat:
        params["cat"] = cat
    if sub:
        params["sub"] = sub
    if company:
        params["company"] = company.upper()

    data = await _sgx.get(SGX_API_BASE + "/count", params)
    # SGX returns total count directly in "data" field (confirmed from DevTools)
    total = data.get("data") or data.get("meta", {}).get("totalItems", 0)
    return {
        "total": int(total),
        "period_start": params["periodstart"],
        "period_end":   params["periodend"],
        "filters": {k: v for k, v in {"cat": cat, "sub": sub, "company": company}.items() if v},
    }


@router.get("/filings", response_model=SGXResponse, summary="List SGX announcements with full pagination")
async def get_sgx_filings(
    from_date: Optional[str] = Query(None, description="Start date YYYYMMDD or YYYY-MM-DD. Default: 30 days ago"),
    to_date: Optional[str] = Query(None, description="End date YYYYMMDD or YYYY-MM-DD. Default: today"),
    cat: Optional[str] = Query(None, description="Category code(s) comma-separated e.g. ANNC or ANNC23,FINSTMT"),
    sub: Optional[str] = Query(None, description="Sub-category code e.g. ANNC01"),
    company: Optional[str] = Query(None, description="Company/issuer name filter (uppercased automatically)"),
    page: int = Query(0, ge=0, description="Page number (0-based). Use meta.total_pages to know how many pages exist"),
    page_size: int = Query(20, ge=1, le=100, description="Results per page (max 100)"),
):
    """
    Fetch SGX announcements with server-side filtering and full pagination info.

    The `meta.total_pages` and `has_more` fields tell you exactly how many
    pages exist — iterate them to get all matching filings.

    Examples:
    - `/api/sgx/filings` — last 30 days, page 0
    - `/api/sgx/filings?from_date=20060410&page=0&page_size=100` — full history, page 0
    - `/api/sgx/filings?cat=FINSTMT&page=2` — financial statements, page 2
    - `/api/sgx/filings?company=DBS&cat=ANNC&page_size=100`
    - `/api/sgx/filings?cat=ANNC23,FINSTMT&from_date=20260101`
    """
    # SGX website uses /company?value=NAME&exactsearch=true when filtering by company
    if company:
        data = await _sgx.get(_company_url(from_date, to_date, company, cat, sub, page, page_size))
    else:
        params = _build_params(from_date, to_date, cat, sub, None, page, page_size)
        data   = await _sgx.get(SGX_API_BASE + "/", params)
    return _make_response(data, page, page_size)


@router.get("/filings/all", summary="Fetch ALL SGX announcements for a query (auto-paginates)")
async def get_sgx_filings_all(
    from_date: Optional[str] = Query(None, description="Start date YYYYMMDD or YYYY-MM-DD. Default: 30 days ago"),
    to_date: Optional[str] = Query(None, description="End date YYYYMMDD or YYYY-MM-DD. Default: today"),
    cat: Optional[str] = Query(None, description="Category code(s) comma-separated"),
    sub: Optional[str] = Query(None, description="Sub-category code"),
    company: Optional[str] = Query(None, description="Company/issuer name filter"),
    max_items: int = Query(1000, ge=1, le=10000, description="Safety cap — maximum total items to return"),
):
    """
    Auto-paginates through ALL pages and returns every filing matching the filters,
    up to `max_items` (default 1000, max 10000).

    Use `/count` first to know the total before deciding on max_items.

    Examples:
    - `/api/sgx/filings/all?company=DBS&cat=FINSTMT` — all DBS financial statements
    - `/api/sgx/filings/all?from_date=20260101&cat=DIV&max_items=5000` — all dividends YTD
    """
    page_size   = 100   # maximum per page on SGX
    all_filings = []
    page        = 0

    use_company_endpoint = bool(company)

    while len(all_filings) < max_items:
        if use_company_endpoint:
            data = await _sgx.get(_company_url(from_date, to_date, company, cat, sub, page, page_size))
        else:
            params = _build_params(from_date, to_date, cat, sub, None, page, page_size)
            data   = await _sgx.get(SGX_API_BASE + "/", params)

        meta_raw    = data.get("meta", {})
        items       = data.get("data", []) or []
        total_pages = int(meta_raw.get("totalPages", 1))
        total_items = int(meta_raw.get("totalItems", 0))

        all_filings.extend([parse_filing(i) for i in items])

        if page + 1 >= total_pages or not items:
            break

        page += 1

    # Trim to max_items
    all_filings = all_filings[:max_items]

    return {
        "total_on_server": int(data.get("meta", {}).get("totalItems", len(all_filings))),
        "returned": len(all_filings),
        "capped_at": max_items,
        "filings": all_filings,
    }


@router.get("/search", response_model=SGXResponse, summary="Search SGX by company name")
async def search_sgx(
    q: str = Query(..., min_length=1, description="Company name or stock code"),
    from_date: Optional[str] = Query(None, description="Start date YYYYMMDD or YYYY-MM-DD. Default: 30 days ago"),
    to_date: Optional[str] = Query(None, description="End date YYYYMMDD or YYYY-MM-DD. Default: today"),
    cat: Optional[str] = Query(None, description="Category code filter"),
    sub: Optional[str] = Query(None, description="Sub-category code"),
    page: int = Query(0, ge=0),
    page_size: int = Query(20, ge=1, le=100),
):
    """
    Search SGX announcements by company name.

    Examples:
    - `/api/sgx/search?q=DBS`
    - `/api/sgx/search?q=CAPITALAND&cat=FINSTMT`
    - `/api/sgx/search?q=DBS&from_date=20260101&cat=DIV`
    """
    data = await _sgx.get(_company_url(from_date, to_date, q, cat, sub, page, page_size))
    return _make_response(data, page, page_size)


@router.get("/categories", summary="List SGX announcement categories (live + grouped)")
async def get_sgx_categories(
    grouped: bool = Query(True, description="If true, return categories grouped by the 4 website sections"),
):
    """
    Returns SGX categories. By default returns the same 4-group structure
    as the sgx.com Company Announcements filter UI:
      - Announcements
      - Corporate Action
      - Product Announcements & Listings
      - Trading Status

    Set grouped=false to return a flat list fetched live from the CMS taxonomy API.
    """
    if grouped:
        return {
            "grouped": True,
            "groups": [
                {"group": name, "categories": cats}
                for name, cats in CATEGORY_GROUPS.items()
            ],
        }

    # Flat live list from CMS
    try:
        taxonomy_url = _cms_taxonomy_url(_sgx._cms_version)
        data = await _sgx.get_unauthenticated(taxonomy_url)
        results = (
            data.get("data", {})
                .get("taxonomyTermByVocabulary", {})
                .get("results", [])
        )
        categories = [
            SGXCategory(
                id=str(r.get("data", r).get("id", "")),
                name=r.get("data", r).get("name", ""),
                code=r.get("data", r).get("code"),
                parent_code=r.get("data", r).get("parentCode"),
                order=str(r.get("data", r).get("order", "")),
            )
            for r in results
        ]
        return {"grouped": False, "total": len(categories), "categories": categories}
    except Exception as e:
        logger.warning("Live taxonomy fetch failed, returning grouped fallback: %s", e)
        return {
            "grouped": True,
            "note": "Live CMS taxonomy unavailable — using built-in category list",
            "groups": [
                {"group": name, "categories": cats}
                for name, cats in CATEGORY_GROUPS.items()
            ],
        }


@router.get("/suggest", summary="Typeahead company search — returns matching SGX companies from cache")
async def suggest_sgx_companies(
    q: str = Query(..., min_length=1, description="Company name or stock code prefix"),
    limit: int = Query(10, ge=1, le=50),
):
    """
    Fast typeahead suggest for SGX companies. Loads the full companylist once into
    memory, then filters locally — no SGX API call per keystroke.

    Use the returned ``issuer_name`` as the ``company`` param in /filings so that
    the /company?value=NAME&exactsearch=true query matches exactly.

    Examples:
    - `/api/sgx/suggest?q=DBS`
    - `/api/sgx/suggest?q=sing`
    - `/api/sgx/suggest?q=D05`
    """
    await _ensure_company_cache()
    ql = q.lower()
    results = [
        c for c in _company_cache
        if ql in str(c.get("issuer_name",   "")).lower()
        or ql in str(c.get("security_name", "")).lower()
        or str(c.get("stock_code", "")).lower().startswith(ql)
    ][:limit]
    return {"query": q, "count": len(results), "results": results}


@router.get("/companies", summary="List SGX listed companies")
async def get_sgx_companies(
    q: Optional[str] = Query(None, description="Filter by name or stock code"),
):
    """
    Fetch all SGX listed companies from /companylist.
    Use issuer_name / stock_code as the `company` param in /filings.
    """
    data = await _sgx.get(SGX_API_BASE + "/companylist")
    companies = data.get("data", []) or (data if isinstance(data, list) else [])

    if q:
        q_lower = q.lower()
        companies = [
            c for c in companies
            if q_lower in str(c.get("issuer_name", "")).lower()
            or q_lower in str(c.get("stock_code", "")).lower()
            or q_lower in str(c.get("security_name", "")).lower()
        ]

    return {"total": len(companies), "companies": companies}


@router.get("/securities", summary="List SGX securities")
async def get_sgx_securities(
    q: Optional[str] = Query(None, description="Filter by name or ISIN"),
):
    """
    Fetch all SGX securities from /securitylist.
    """
    data = await _sgx.get(SGX_API_BASE + "/securitylist")
    securities = data.get("data", []) or (data if isinstance(data, list) else [])

    if q:
        q_lower = q.lower()
        securities = [
            s for s in securities
            if q_lower in str(s.get("security_name", "")).lower()
            or q_lower in str(s.get("isin_code", "")).lower()
            or q_lower in str(s.get("stock_code", "")).lower()
        ]

    return {"total": len(securities), "securities": securities[:500]}


@router.get("/pdf/{filing_id:path}", summary="Redirect to SGX announcement PDF")
async def get_sgx_pdf(filing_id: str):
    """Redirect to SGX PDF. Pass the path from the `url` field in /filings."""
    return RedirectResponse(url=f"{SGX_LINKS}/{filing_id}")


@router.get("/debug", summary="Debug — token status and connectivity test")
async def debug_sgx():
    """Shows token/CMS version cache state and tests all SGX endpoints."""
    results: dict = {
        "token": {
            "cached": _sgx._token is not None,
            "cms_version": _sgx._cms_version[:8] + "...",
        }
    }

    # Count (full history)
    try:
        data = await _sgx.get(
            SGX_API_BASE + "/count",
            {"periodstart": SGX_EARLIEST, "periodend": to_sgx_dt(None, end_of_day=True)},
        )
        results["count_all_time"] = data.get("data") or data.get("meta", {}).get("totalItems")
    except Exception as e:
        results["count_all_time"] = {"error": str(e)}

    # Filings (last 7 days, 3 items)
    try:
        from_7d = (date.today() - timedelta(days=7)).strftime("%Y%m%d")
        data = await _sgx.get(
            SGX_API_BASE + "/",
            _build_params(from_7d, None, None, None, None, 0, 3),
        )
        meta = data.get("meta", {})
        results["filings_last_7d"] = {
            "status": "ok",
            "total_items": meta.get("totalItems"),
            "total_pages": meta.get("totalPages"),
            "items_returned": len(data.get("data", [])),
        }
    except Exception as e:
        results["filings_last_7d"] = {"error": str(e)}

    # Companylist
    try:
        data = await _sgx.get(SGX_API_BASE + "/companylist")
        results["companylist"] = {"status": "ok", "companies": len(data.get("data", []))}
    except Exception as e:
        results["companylist"] = {"error": str(e)}

    return results
