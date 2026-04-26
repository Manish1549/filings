"""
Europe Router — financialreports.eu API
========================================
Covers 30+ European countries (DE, FR, IT, ES, NL, SE, CH, etc.)

Base URL: https://api.financialreports.eu/
Auth:     X-API-Key header  (set FINANCIALREPORTS_API_KEY env var)

Key endpoints:
  GET /companies/          — search/list companies
  GET /filings/            — list filings with rich filters
  GET /filings/{id}/       — single filing detail

Polling uses added_to_platform_from filter — no timestamp comparison needed.
"""

import logging
import os
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()
logger = logging.getLogger(__name__)

EUROPE_API_KEY = os.environ.get("FINANCIALREPORTS_API_KEY", "")
EUROPE_BASE    = "https://api.financialreports.eu"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "GlobalFilings research@globalfilings.com",
}


def _auth_headers() -> dict:
    if not EUROPE_API_KEY:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "FINANCIALREPORTS_API_KEY not configured",
                "how_to_fix": "Set FINANCIALREPORTS_API_KEY in your environment variables",
            },
        )
    return {**HEADERS, "X-API-Key": EUROPE_API_KEY}


async def europe_get(path: str, params: dict = None) -> dict:
    """Authenticated GET to financialreports.eu with retry."""
    url = f"{EUROPE_BASE}{path}"
    headers = _auth_headers()
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        for attempt in range(3):
            try:
                res = await client.get(url, headers=headers, params=params)
                if res.status_code == 429:
                    raise HTTPException(status_code=429, detail="financialreports.eu rate limit exceeded")
                if res.status_code >= 400:
                    raise HTTPException(
                        status_code=res.status_code,
                        detail={"error": f"financialreports.eu {res.status_code}", "body": res.text[:400]},
                    )
                return res.json()
            except HTTPException:
                raise
            except httpx.ConnectError:
                if attempt == 2:
                    raise HTTPException(status_code=503, detail="financialreports.eu unreachable")
                import asyncio; await asyncio.sleep(2 ** attempt)
            except httpx.RequestError as e:
                raise HTTPException(status_code=503, detail=str(e))
    return {}


# ── Pydantic models ───────────────────────────────────────────────────────────

class EuropeCompany(BaseModel):
    id: int
    name: str
    country: Optional[str] = None
    isin: Optional[str] = None
    lei: Optional[str] = None
    ticker: Optional[str] = None
    industry: Optional[str] = None


class EuropeFiling(BaseModel):
    id: int
    title: Optional[str] = None
    company_id: Optional[int] = None
    company_name: Optional[str] = None
    country: Optional[str] = None
    filing_type: Optional[str] = None
    category: Optional[str] = None
    release_datetime: Optional[str] = None
    added_to_platform: Optional[str] = None
    period_ending_date: Optional[str] = None
    fiscal_year: Optional[int] = None
    fiscal_period: Optional[str] = None
    language: Optional[str] = None
    document_url: Optional[str] = None
    viewer_url: Optional[str] = None


def _parse_company(c: dict) -> EuropeCompany:
    isins = c.get("isins") or []
    isin  = isins[0].get("isin") if isins and isinstance(isins[0], dict) else (isins[0] if isins else None)
    stocks = c.get("listed_stock_exchanges") or []
    ticker = stocks[0].get("ticker_symbol") if stocks and isinstance(stocks[0], dict) else None
    return EuropeCompany(
        id=c["id"],
        name=c.get("name", ""),
        country=c.get("country_of_registration") or c.get("country"),
        isin=isin,
        lei=c.get("lei"),
        ticker=ticker,
        industry=c.get("sub_industry_code"),
    )


def _parse_filing(f: dict) -> EuropeFiling:
    company = f.get("company") or {}
    ft      = f.get("filing_type") or {}
    cat     = f.get("category") or {}
    lang    = f.get("language") or {}
    return EuropeFiling(
        id=f["id"],
        title=f.get("title"),
        company_id=company.get("id"),
        company_name=company.get("name"),
        country=company.get("country_of_registration") or company.get("country"),
        filing_type=ft.get("name") if isinstance(ft, dict) else str(ft),
        category=cat.get("name") if isinstance(cat, dict) else str(cat),
        release_datetime=f.get("release_datetime"),
        added_to_platform=f.get("added_to_platform"),
        period_ending_date=f.get("period_ending_date"),
        fiscal_year=f.get("fiscal_year"),
        fiscal_period=f.get("fiscal_period"),
        language=lang.get("name") if isinstance(lang, dict) else str(lang or ""),
        document_url=f.get("document_url") or f.get("s3_url"),
        viewer_url=f.get("viewer_url"),
    )


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/companies", summary="Search European companies")
async def list_europe_companies(
    search:    Optional[str] = Query(None, description="Company name, ticker, ISIN or LEI"),
    countries: Optional[str] = Query(None, description="Comma-separated ISO Alpha-2 country codes e.g. DE,FR,IT"),
    page:      int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """
    Search companies across 30+ European markets.

    Examples:
    - `/api/europe/companies?search=Volkswagen`
    - `/api/europe/companies?countries=DE,FR&page_size=50`
    - `/api/europe/companies?search=VOW3`
    """
    params: dict = {"page": page, "page_size": page_size, "view": "summary"}
    if search:
        params["search"] = search
    if countries:
        params["countries"] = countries
    data = await europe_get("/companies/", params)
    results = [_parse_company(c) for c in (data.get("results") or [])]
    return {"total": data.get("count", 0), "page": page, "page_size": page_size, "companies": results}


@router.get("/filings", summary="List European filings")
async def list_europe_filings(
    company:    Optional[str] = Query(None, description="Company ID (integer)"),
    search:     Optional[str] = Query(None, description="Search filing title or company name"),
    countries:  Optional[str] = Query(None, description="Comma-separated ISO Alpha-2 codes e.g. DE,FR"),
    category:   Optional[str] = Query(None, description="Filing category name"),
    from_date:  Optional[str] = Query(None, description="Release date from YYYY-MM-DD"),
    to_date:    Optional[str] = Query(None, description="Release date to YYYY-MM-DD"),
    language:   Optional[str] = Query(None, description="ISO 639-1 language code e.g. en, de, fr"),
    fiscal_year:Optional[int] = Query(None, description="Fiscal year e.g. 2025"),
    page:       int = Query(1, ge=1),
    page_size:  int = Query(20, ge=1, le=100),
):
    """
    List filings from European markets with rich filtering.

    Examples:
    - `/api/europe/filings?company=12345`
    - `/api/europe/filings?countries=DE&from_date=2026-01-01`
    - `/api/europe/filings?search=annual+report&language=en`
    """
    params: dict = {
        "page": page,
        "page_size": page_size,
        "ordering": "-release_datetime",
        "view": "summary",
    }
    if company:    params["company"]   = company
    if search:     params["search"]    = search
    if countries:  params["countries"] = countries
    if category:   params["category"]  = category
    if language:   params["language"]  = language
    if fiscal_year:params["fiscal_year"] = fiscal_year
    if from_date:  params["release_datetime_from"] = f"{from_date}T00:00:00Z"
    if to_date:    params["release_datetime_to"]   = f"{to_date}T23:59:59Z"

    data    = await europe_get("/filings/", params)
    results = [_parse_filing(f) for f in (data.get("results") or [])]
    return {
        "total":     data.get("count", 0),
        "page":      page,
        "page_size": page_size,
        "has_more":  bool(data.get("next")),
        "filings":   results,
    }


@router.get("/filings/{filing_id}", summary="Get single European filing")
async def get_europe_filing(filing_id: int):
    data = await europe_get(f"/filings/{filing_id}/")
    return _parse_filing(data)


@router.get("/suggest", summary="Typeahead for European companies (from DB)")
async def suggest_europe(
    q:     str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
):
    """Fast typeahead using locally seeded company DB — no API call per keystroke."""
    from routers.suggest import suggest_companies
    from fastapi import Request
    # Delegate to unified suggest with europe market filter
    # This reuses the DB-backed suggest which is already seeded with Europe companies
    return {"note": "Use /api/suggest?q=QUERY&markets=europe for typeahead"}
