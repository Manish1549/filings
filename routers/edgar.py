"""
EDGAR (US SEC) Router
======================
Official free API — no API key required.
Source: https://data.sec.gov

Confirmed response fields from DevTools (Image 3):
  id, url, type, date, category, regId (CIK), accessionNumber,
  headlines, attachments, periodGroup, language, description,
  source, ticker, companyId, countryId, companyName

API Endpoints used:
  Company search:     https://efts.sec.gov/LATEST/search-index?q="{ticker}"&dateRange=custom
  Submissions:        https://data.sec.gov/submissions/CIK{cik10}.json
  Filing index:       https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&...
  Full text search:   https://efts.sec.gov/LATEST/search-index?q=...&forms=10-K,10-Q
  XBRL facts:         https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json
  File download:      https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{file}

Rate limit: 10 req/sec. Must set User-Agent header with name + email.
"""

import asyncio
import logging
from datetime import date, timedelta
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter()
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
SUBMISSIONS_BASE = "https://data.sec.gov/submissions"
EFTS_BASE        = "https://efts.sec.gov/LATEST/search-index"
EDGAR_BASE       = "https://www.sec.gov"
XBRL_BASE        = "https://data.sec.gov/api/xbrl"
COMPANY_TICKERS  = "https://www.sec.gov/files/company_tickers.json"

# REQUIRED by SEC — must include name + contact info
HEADERS = {
    "User-Agent": "GlobalFilings research@globalfilings.com",
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate",
}

# Common form types
FORM_TYPES = {
    "10-K":   "Annual report",
    "10-Q":   "Quarterly report",
    "8-K":    "Current report (material events)",
    "4":      "Insider transactions",
    "S-1":    "IPO registration",
    "13F":    "Institutional holdings (>$100M)",
    "DEF14A": "Proxy statement",
    "SC 13G": "Beneficial ownership >5%",
    "SC 13D": "Beneficial ownership >5% (active)",
    "20-F":   "Annual report (foreign private issuer)",
    "6-K":    "Current report (foreign private issuer)",
    "424B4":  "Prospectus",
    "S-3":    "Shelf registration",
}


# ── Pydantic models ───────────────────────────────────────────────────────────
class EDGARFiling(BaseModel):
    accession_number: str
    form_type: str
    filing_date: str
    report_date: Optional[str] = None
    company_name: Optional[str] = None
    cik: Optional[str] = None
    ticker: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    document_url: Optional[str] = None
    index_url: Optional[str] = None
    is_xbrl: bool = False
    is_inline_xbrl: bool = False


class EDGARCompany(BaseModel):
    cik: str
    name: str
    ticker: Optional[str] = None
    sic: Optional[str] = None
    sic_description: Optional[str] = None
    state: Optional[str] = None
    fiscal_year_end: Optional[str] = None
    entity_type: Optional[str] = None


class EDGARResponse(BaseModel):
    company: Optional[EDGARCompany] = None
    total: int
    page: int
    page_size: int
    filings: list[EDGARFiling]


# ── Helpers ───────────────────────────────────────────────────────────────────
def pad_cik(cik: str) -> str:
    """Pad CIK to 10 digits as required by SEC API."""
    return str(cik).strip().lstrip("0").zfill(10)


def fmt_accession(raw: str) -> str:
    """Format accession number: 0001234567-24-000001"""
    clean = raw.replace("-", "")
    if len(clean) == 18:
        return f"{clean[:10]}-{clean[10:12]}-{clean[12:]}"
    return raw


def accession_to_path(accession: str) -> str:
    """Convert accession number to URL path segment."""
    return accession.replace("-", "")


def build_index_url(cik: str, accession: str) -> str:
    cik_clean = str(int(cik))
    acc_path  = accession_to_path(accession)
    return f"{EDGAR_BASE}/Archives/edgar/data/{cik_clean}/{acc_path}/{accession}-index.htm"


def build_doc_url(cik: str, accession: str, filename: str) -> str:
    cik_clean = str(int(cik))
    acc_path  = accession_to_path(accession)
    return f"{EDGAR_BASE}/Archives/edgar/data/{cik_clean}/{acc_path}/{filename}"


def categorize_form(form_type: str) -> str:
    """Map form type to a readable category."""
    form = form_type.upper().strip()
    if form in ("10-K", "10-KSB", "10-KT", "20-F", "40-F"):
        return "Annual Report"
    if form in ("10-Q", "10-QSB"):
        return "Quarterly Report"
    if form.startswith("8-K"):
        return "Current Report"
    if form in ("4", "4/A"):
        return "Insider Transaction"
    if form in ("13F", "13F-HR", "13F-NT"):
        return "Institutional Holdings"
    if form in ("S-1", "S-1/A", "S-11", "424B4", "424B3"):
        return "Registration / Prospectus"
    if form in ("SC 13G", "SC 13G/A", "SC 13D", "SC 13D/A"):
        return "Beneficial Ownership"
    if form in ("DEF 14A", "DEF14A", "DEFA14A"):
        return "Proxy Statement"
    if form.startswith("6-K"):
        return "Foreign Current Report"
    return "Other"


async def edgar_get(url: str, params: dict = None) -> dict:
    """Async GET to SEC EDGAR API with up to 2 retries on connection errors."""
    last_exc = None
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        for attempt in range(3):
            try:
                r = await client.get(url, headers=HEADERS, params=params)
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as e:
                raise HTTPException(
                    status_code=e.response.status_code,
                    detail={
                        "error": f"EDGAR API {e.response.status_code}",
                        "url": str(e.request.url),
                        "body": e.response.text[:400],
                    },
                )
            except httpx.ConnectError as e:
                last_exc = e
                logger.warning("EDGAR connection error (attempt %d/3): %s", attempt + 1, e)
                await asyncio.sleep(2 ** attempt)
            except httpx.RequestError as e:
                raise HTTPException(status_code=503, detail=f"EDGAR unreachable: {str(e)}")
    raise HTTPException(status_code=503, detail=f"EDGAR unreachable after retries: {str(last_exc)}")


async def resolve_cik(ticker_or_cik: str) -> tuple[str, str]:
    """
    Resolve ticker symbol or CIK to (cik_padded, company_name).
    Uses the SEC company tickers JSON file.
    """
    raw = ticker_or_cik.strip()

    if raw.isdigit():
        return pad_cik(raw), ""

    # Search by ticker in company tickers file
    try:
        data = await edgar_get(COMPANY_TICKERS)
        for entry in data.values():
            if entry.get("ticker", "").upper() == raw.upper():
                return pad_cik(str(entry["cik_str"])), entry.get("title", "")
    except Exception as e:
        logger.warning("EDGAR ticker lookup failed for %r: %s", raw, e)

    # Fallback: EFTS full-text search
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            r = await client.get(
                "https://efts.sec.gov/LATEST/search-index",
                headers=HEADERS,
                params={"q": f'"{raw}"', "dateRange": "custom", "startdt": "2020-01-01"},
            )
            hits = r.json().get("hits", {}).get("hits", [])
            if hits:
                src = hits[0].get("_source", {})
                cik = src.get("entity_id") or src.get("file_num", "")
                if cik:
                    return pad_cik(cik), src.get("display_names", [raw])[0]
    except Exception as e:
        logger.warning("EDGAR EFTS fallback lookup failed for %r: %s", raw, e)

    raise HTTPException(
        status_code=404,
        detail=f"Could not resolve '{raw}' to a CIK. Try passing the numeric CIK directly.",
    )


def parse_submission_filings(
    cik: str,
    company_name: str,
    ticker: str,
    recent: dict,
    form_filter: Optional[str],
    start_idx: int,
    page_size: int,
) -> list[EDGARFiling]:
    """Parse the 'recent' block from submissions JSON into EDGARFiling objects."""
    accessions   = recent.get("accessionNumber", [])
    forms        = recent.get("form", [])
    dates        = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])
    descriptions = recent.get("primaryDocument", [])
    xbrl_flags   = recent.get("isXBRL", [])
    ixbrl_flags  = recent.get("isInlineXBRL", [])

    filings = []
    count = 0

    for i, (acc, form, d) in enumerate(zip(accessions, forms, dates)):
        if form_filter and form.upper() != form_filter.upper():
            continue
        count += 1
        if count <= start_idx:
            continue
        if len(filings) >= page_size:
            break

        acc_fmt   = fmt_accession(acc)
        doc_file  = descriptions[i] if i < len(descriptions) else ""
        report_dt = report_dates[i] if i < len(report_dates) else None
        is_xbrl   = bool(xbrl_flags[i]) if i < len(xbrl_flags) else False
        is_ixbrl  = bool(ixbrl_flags[i]) if i < len(ixbrl_flags) else False

        filings.append(EDGARFiling(
            accession_number=acc_fmt,
            form_type=form,
            filing_date=d,
            report_date=report_dt or None,
            company_name=company_name,
            cik=cik.lstrip("0"),
            ticker=ticker,
            category=categorize_form(form),
            description=FORM_TYPES.get(form.upper(), ""),
            document_url=build_doc_url(cik, acc_fmt, doc_file) if doc_file else None,
            index_url=build_index_url(cik, acc_fmt),
            is_xbrl=is_xbrl,
            is_inline_xbrl=is_ixbrl,
        ))

    return filings


# ── Company cache for typeahead ────────────────────────────────────────────────
_edgar_companies: list[dict] = []
_edgar_companies_loaded: bool = False

async def _ensure_edgar_companies() -> None:
    """Load company_tickers.json once and cache for suggest endpoint."""
    global _edgar_companies, _edgar_companies_loaded
    if _edgar_companies_loaded:
        return
    try:
        data = await edgar_get(COMPANY_TICKERS)
        _edgar_companies = [
            {"ticker": v.get("ticker", ""), "cik": str(v.get("cik_str", "")), "title": v.get("title", "")}
            for v in data.values()
        ]
        _edgar_companies_loaded = True
        logger.info("EDGAR company cache loaded: %d companies", len(_edgar_companies))
    except Exception as e:
        logger.warning("EDGAR company cache load failed: %s", e)


@router.get("/suggest", summary="Typeahead company search for EDGAR")
async def suggest_edgar_companies(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
):
    """Fast typeahead: returns companies matching name or ticker from SEC company list."""
    await _ensure_edgar_companies()
    ql = q.lower()
    # Prefer ticker prefix matches, then name contains
    ticker_hits = [c for c in _edgar_companies if c["ticker"].lower().startswith(ql)]
    name_hits   = [c for c in _edgar_companies if ql in c["title"].lower() and c not in ticker_hits]
    results = (ticker_hits + name_hits)[:limit]
    return {"query": q, "count": len(results), "results": results}


# ── Endpoints ─────────────────────────────────────────────────────────────────
@router.get(
    "/filings/{ticker_or_cik}",
    response_model=EDGARResponse,
    summary="Get EDGAR filings by ticker or CIK",
)
async def get_edgar_filings(
    ticker_or_cik: str,
    form_type: Optional[str] = Query(
        None,
        description="Filter by form type: 10-K, 10-Q, 8-K, 4, S-1, 13F, DEF14A, etc."
    ),
    page: int = Query(0, ge=0, description="Page number (0-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Results per page"),
):
    """
    Fetch EDGAR filings for a company by ticker or CIK.

    Examples:
    - `/api/edgar/filings/AAPL`
    - `/api/edgar/filings/TSLA?form_type=10-K`
    - `/api/edgar/filings/0000320193` (Apple's CIK)
    - `/api/edgar/filings/MSFT?form_type=8-K&page=0&page_size=10`
    """
    cik, resolved_name = await resolve_cik(ticker_or_cik)
    data = await edgar_get(f"{SUBMISSIONS_BASE}/CIK{cik}.json")

    name   = data.get("name", resolved_name)
    ticker = data.get("tickers", [ticker_or_cik])[0] if data.get("tickers") else ticker_or_cik
    company = EDGARCompany(
        cik=cik.lstrip("0"),
        name=name,
        ticker=ticker.upper() if ticker else None,
        sic=data.get("sic"),
        sic_description=data.get("sicDescription"),
        state=data.get("stateOfIncorporation"),
        fiscal_year_end=data.get("fiscalYearEnd"),
        entity_type=data.get("entityType"),
    )

    recent = data.get("filings", {}).get("recent", {})
    start  = page * page_size

    filings = parse_submission_filings(cik, name, ticker, recent, form_type, start, page_size)

    all_forms = recent.get("form", [])
    total = (
        sum(1 for f in all_forms if f.upper() == form_type.upper())
        if form_type else len(all_forms)
    )

    return EDGARResponse(company=company, total=total, page=page, page_size=page_size, filings=filings)


@router.get("/search", summary="Full-text search across all EDGAR filings")
async def search_edgar(
    q: str = Query(..., min_length=1, description="Company name, ticker, or keyword"),
    form_type: Optional[str] = Query(None, description="Filter: 10-K, 10-Q, 8-K, 4, S-1 etc."),
    from_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    page: int = Query(0, ge=0),
    page_size: int = Query(20, ge=1, le=40),
):
    """
    Full-text search across 18M+ EDGAR filings using EFTS.

    Examples:
    - `/api/edgar/search?q=Tesla&form_type=10-K`
    - `/api/edgar/search?q=AAPL&from_date=2025-01-01`
    - `/api/edgar/search?q=artificial+intelligence&form_type=8-K`
    """
    params = {
        "q": f'"{q}"',
        "dateRange": "custom",
        "startdt": from_date or (date.today() - timedelta(days=365)).isoformat(),
        "enddt": to_date or date.today().isoformat(),
        "from": page * page_size,
        "size": page_size,
    }
    if form_type:
        params["forms"] = form_type

    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            r = await client.get(EFTS_BASE, headers=HEADERS, params=params)
            r.raise_for_status()
            data = r.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text[:300])
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=str(e))

    hits  = data.get("hits", {})
    items = hits.get("hits", [])
    total = hits.get("total", {}).get("value", len(items))

    filings = []
    for item in items:
        src  = item.get("_source", {})
        acc  = src.get("file_num") or src.get("period_of_report", "")
        form = src.get("form_type", "")
        filings.append(EDGARFiling(
            accession_number=acc,
            form_type=form,
            filing_date=src.get("file_date", ""),
            report_date=src.get("period_of_report"),
            company_name=", ".join(src.get("display_names", [])),
            cik=src.get("entity_id", ""),
            category=categorize_form(form),
            description=src.get("description", ""),
            document_url=src.get("file_href"),
            index_url=src.get("file_href"),
        ))

    return {"total": total, "page": page, "page_size": page_size, "filings": filings}


@router.get("/company/{ticker_or_cik}", summary="Get company info from EDGAR")
async def get_edgar_company(ticker_or_cik: str):
    """
    Get company metadata — name, CIK, SIC, state, fiscal year end, tickers.

    Examples:
    - `/api/edgar/company/AAPL`
    - `/api/edgar/company/0000320193`
    """
    cik, _ = await resolve_cik(ticker_or_cik)
    data = await edgar_get(f"{SUBMISSIONS_BASE}/CIK{cik}.json")

    return EDGARCompany(
        cik=cik.lstrip("0"),
        name=data.get("name", ""),
        ticker=(data.get("tickers") or [None])[0],
        sic=data.get("sic"),
        sic_description=data.get("sicDescription"),
        state=data.get("stateOfIncorporation"),
        fiscal_year_end=data.get("fiscalYearEnd"),
        entity_type=data.get("entityType"),
    )


@router.get("/form-types", summary="List common EDGAR form types")
async def get_form_types():
    """Return known EDGAR form types with descriptions."""
    return {
        "form_types": [
            {"form": k, "description": v, "category": categorize_form(k)}
            for k, v in FORM_TYPES.items()
        ]
    }


@router.get(
    "/download/{cik}/{accession}/{filename:path}",
    summary="Download a filing document from EDGAR",
)
async def download_edgar_filing(cik: str, accession: str, filename: str):
    """
    Stream a filing document directly from SEC EDGAR archives.

    Example:
    - `/api/edgar/download/320193/0000320193-24-000123/aapl-20240930.htm`
    """
    cik_clean = str(int(pad_cik(cik)))
    acc_path  = accession_to_path(accession)
    url = f"{EDGAR_BASE}/Archives/edgar/data/{cik_clean}/{acc_path}/{filename}"
    media_type = "application/pdf" if filename.endswith(".pdf") else "text/html"

    async def _stream():
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            async with client.stream("GET", url, headers=HEADERS) as r:
                try:
                    r.raise_for_status()
                except httpx.HTTPStatusError as e:
                    raise HTTPException(status_code=e.response.status_code, detail="Download failed")
                async for chunk in r.aiter_bytes(8192):
                    yield chunk

    return StreamingResponse(
        _stream(),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
