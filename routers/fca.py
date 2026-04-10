"""
FCA NSM (UK Financial Conduct Authority — National Storage Mechanism) Router
=============================================================================
Confirmed working from Gemini test script — NO auth headers needed.

API:  POST https://api.data.fca.org.uk/search?index=fca-nsm-searchdata
Auth: None — standard headers only

Exact payload structure (from DevTools payload screenshot):
{
  "from": 0,
  "size": 50,
  "sortorder": "desc",
  "criteriaObj": {
    "criteria": [
      {"name": "latest_flag",       "value": "Y"},
      {"name": "document_content",  "value": ["Apple", "exact_match"]},
      {"name": "company_lei",       "value": ["CompanyName", "LEI", "disclose_org", "related_org"]},
      {"name": "category",          "value": "some_category"},
      {"name": "source",            "value": "NSM"}
    ],
    "dateCriteria": [
      {"name": "submitted_date",    "value": {"from": "2026-01-01T00:00:00Z", "to": "2026-03-30T23:59:59Z"}},
      {"name": "publication_date",  "value": {"from": null, "to": "2026-03-30T20:10:00Z"}}
    ]
  }
}

Response: standard Elasticsearch hits format
  hits.total.value = total count
  hits.hits[]._source = filing fields:
    submitted_date, document_date, publication_date,
    company, lei, headline, type_code, type, source,
    download_link, disclosure_id, classifications,
    classifications_code, latest_flag, tag_esef

Document URL: https://data.fca.org.uk/artefacts/ + download_link
"""

import asyncio
import logging
from datetime import date, timedelta
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

router = APIRouter()
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
FCA_SEARCH_URL = "https://api.data.fca.org.uk/search?index=fca-nsm-searchdata"
FCA_ARTEFACTS  = "https://data.fca.org.uk/artefacts/"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://data.fca.org.uk",
    "referer": "https://data.fca.org.uk/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}

FCA_CATEGORIES = [
    {"code": "Annual Financial Report",    "label": "Annual Financial Report"},
    {"code": "Half Yearly Financial Report","label": "Half Yearly Report"},
    {"code": "Preliminary Results",        "label": "Preliminary Results"},
    {"code": "1st Quarter Results",        "label": "1st Quarter Results"},
    {"code": "3rd Quarter Results",        "label": "3rd Quarter Results"},
    {"code": "Director/PDMR Shareholding", "label": "Director/PDMR Shareholding"},
    {"code": "Holding(s) in Company",      "label": "Major Holdings"},
    {"code": "Total Voting Rights",        "label": "Total Voting Rights"},
    {"code": "Prospectus",                 "label": "Prospectus"},
    {"code": "Inside Information",         "label": "Inside Information"},
    {"code": "Mergers & Acquisitions",     "label": "Mergers & Acquisitions"},
    {"code": "Non Regulatory",             "label": "Non Regulatory"},
]


# ── Pydantic models ───────────────────────────────────────────────────────────
class FCAFiling(BaseModel):
    id: Optional[str] = None
    disclosure_id: Optional[str] = None
    company: Optional[str] = None
    lei: Optional[str] = None
    headline: Optional[str] = None
    category: Optional[str] = None
    type_code: Optional[str] = None
    source: Optional[str] = None
    submitted_date: Optional[str] = None
    document_date: Optional[str] = None
    publication_date: Optional[str] = None
    classifications: Optional[str] = None
    classifications_code: Optional[str] = None
    download_link: Optional[str] = None
    document_url: Optional[str] = None
    tag_esef: Optional[str] = None
    latest_flag: Optional[str] = None


class FCAResponse(BaseModel):
    total: int
    page: int
    page_size: int
    filings: list[FCAFiling]


# ── Payload builder ───────────────────────────────────────────────────────────
def build_payload(
    company: Optional[str] = None,
    lei: Optional[str] = None,
    document_text: Optional[str] = None,
    category: Optional[str] = None,
    source: Optional[str] = None,
    filing_date_from: Optional[str] = None,
    filing_date_to: Optional[str] = None,
    pub_date_from: Optional[str] = None,
    pub_date_to: Optional[str] = None,
    page: int = 0,
    page_size: int = 20,
    sort_order: str = "desc",
) -> dict:
    """Build exact FCA NSM payload structure confirmed from DevTools + Gemini script."""
    criteria = []
    date_criteria = []

    criteria.append({"name": "latest_flag", "value": "Y"})

    if document_text:
        criteria.append({"name": "document_content", "value": [document_text, "exact_match"]})

    if company or lei:
        criteria.append({
            "name": "company_lei",
            "value": [company or "", lei or "", "disclose_org", "related_org"],
        })

    if category:
        criteria.append({"name": "category", "value": category})

    if source:
        criteria.append({"name": "source", "value": source})

    if filing_date_from or filing_date_to:
        date_criteria.append({
            "name": "submitted_date",
            "value": {
                "from": f"{filing_date_from}T00:00:00Z" if filing_date_from else None,
                "to":   f"{filing_date_to}T23:59:59Z"   if filing_date_to   else None,
            },
        })

    if pub_date_from or pub_date_to:
        date_criteria.append({
            "name": "publication_date",
            "value": {
                "from": f"{pub_date_from}T00:00:00Z" if pub_date_from else None,
                "to":   f"{pub_date_to}T23:59:59Z"   if pub_date_to   else None,
            },
        })

    return {
        "from": page * page_size,
        "size": page_size,
        "sortorder": sort_order,
        "criteriaObj": {"criteria": criteria, "dateCriteria": date_criteria},
    }


# ── Core request ──────────────────────────────────────────────────────────────
async def fca_post(payload: dict) -> dict:
    """Async POST to FCA NSM with up to 2 retries on connection errors."""
    last_exc = None
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        for attempt in range(3):
            try:
                r = await client.post(FCA_SEARCH_URL, headers=HEADERS, json=payload)
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as e:
                raise HTTPException(
                    status_code=e.response.status_code,
                    detail={"error": f"FCA API {e.response.status_code}", "body": e.response.text[:400]},
                )
            except httpx.ConnectError as e:
                last_exc = e
                logger.warning("FCA connection error (attempt %d/3): %s", attempt + 1, e)
                await asyncio.sleep(2 ** attempt)
            except httpx.RequestError as e:
                raise HTTPException(status_code=503, detail=f"FCA unreachable: {str(e)}")
    raise HTTPException(status_code=503, detail=f"FCA unreachable after retries: {str(last_exc)}")


def parse_results(data: dict) -> tuple[int, list[FCAFiling]]:
    """Parse Elasticsearch response into filings list."""
    if "hits" in data and "hits" in data["hits"]:
        raw_hits  = data["hits"]["hits"]
        total_obj = data["hits"].get("total", {})
        total = total_obj.get("value", len(raw_hits)) if isinstance(total_obj, dict) else int(total_obj or 0)
    else:
        raw_hits = data if isinstance(data, list) else data.get("data", [])
        total = len(raw_hits)

    filings = []
    for hit in raw_hits:
        src = hit.get("_source", hit)
        dl  = src.get("download_link", "")
        filings.append(FCAFiling(
            id=hit.get("_id"),
            disclosure_id=src.get("disclosure_id") or src.get("seq_id"),
            company=src.get("company"),
            lei=src.get("lei"),
            headline=src.get("headline") or src.get("type"),
            category=src.get("type") or src.get("classifications"),
            type_code=src.get("type_code"),
            source=src.get("source"),
            submitted_date=(src.get("submitted_date") or "")[:16],
            document_date=(src.get("document_date") or "")[:10],
            publication_date=(src.get("publication_date") or "")[:16],
            classifications=src.get("classifications"),
            classifications_code=src.get("classifications_code"),
            download_link=dl,
            document_url=(FCA_ARTEFACTS + dl) if dl else None,
            tag_esef=src.get("tag_esef"),
            latest_flag=src.get("latest_flag"),
        ))

    return total, filings


# ── Endpoints ─────────────────────────────────────────────────────────────────
@router.get("/filings", response_model=FCAResponse, summary="Search FCA NSM filings")
async def get_fca_filings(
    company: Optional[str] = Query(None, description="Company name e.g. 'BARCLAYS', 'CARNIVAL PLC'"),
    lei: Optional[str] = Query(None, description="Legal Entity Identifier e.g. '4DR1VPDQMHD3N3QW8W95'"),
    category: Optional[str] = Query(None, description="Filing category e.g. 'Annual Financial Report'"),
    source: Optional[str] = Query(None, description="Source: NSM, RNS, or ESEF"),
    from_date: Optional[str] = Query(None, description="Filing date from YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="Filing date to YYYY-MM-DD"),
    text: Optional[str] = Query(None, description="Free text search across document content"),
    page: int = Query(0, ge=0),
    page_size: int = Query(20, ge=1, le=50),
):
    """
    Search FCA NSM filings using exact payload structure from DevTools.

    Examples:
    - `/api/fca/filings?company=BARCLAYS`
    - `/api/fca/filings?company=CARNIVAL PLC&category=1st Quarter Results`
    - `/api/fca/filings?from_date=2026-03-01&to_date=2026-03-30`
    - `/api/fca/filings?lei=4DR1VPDQMHD3N3QW8W95`
    - `/api/fca/filings?source=RNS&from_date=2026-03-01`
    """
    payload = build_payload(
        company=company, lei=lei, document_text=text, category=category,
        source=source, filing_date_from=from_date, filing_date_to=to_date,
        page=page, page_size=page_size,
    )
    data = await fca_post(payload)
    total, filings = parse_results(data)
    return FCAResponse(total=total, page=page, page_size=page_size, filings=filings)


@router.get("/search", response_model=FCAResponse, summary="Full text search across FCA NSM")
async def search_fca(
    q: str = Query(..., min_length=1, description="Search term — company, headline, keyword"),
    category: Optional[str] = Query(None),
    days: int = Query(90, ge=1, le=730, description="Look back N days"),
    page: int = Query(0, ge=0),
    page_size: int = Query(20, ge=1, le=50),
):
    """
    Full text search across FCA NSM.

    Examples:
    - `/api/fca/search?q=Barclays`
    - `/api/fca/search?q=quarterly results&days=30`
    """
    today   = date.today()
    from_dt = (today - timedelta(days=days)).isoformat()
    to_dt   = today.isoformat()

    payload = build_payload(
        company=q, category=category,
        filing_date_from=from_dt, filing_date_to=to_dt,
        page=page, page_size=page_size,
    )
    data = await fca_post(payload)
    total, filings = parse_results(data)
    return FCAResponse(total=total, page=page, page_size=page_size, filings=filings)


@router.get("/categories", summary="List FCA NSM filing categories")
async def get_fca_categories():
    """Return known FCA NSM category values for the `category` filter."""
    return {"categories": FCA_CATEGORIES, "sources": ["NSM", "RNS", "ESEF"]}


@router.get("/document/{path:path}", summary="Redirect to FCA NSM document")
async def get_fca_document(path: str):
    """
    Redirect to FCA NSM document.
    Pass the `download_link` value from /filings response.

    Example:
    - `/api/fca/document/NSM/RNS/0310df44-cbcf-4975-9cbe-efbcf6eaf797.html`
    """
    return RedirectResponse(url=FCA_ARTEFACTS + path)


@router.get("/debug", summary="Debug — test FCA NSM connectivity")
async def debug_fca():
    """Test FCA API with a simple search for Barclays."""
    payload = build_payload(
        company="BARCLAYS",
        filing_date_from=(date.today() - timedelta(days=30)).isoformat(),
        filing_date_to=date.today().isoformat(),
        page=0,
        page_size=3,
    )
    try:
        data = await fca_post(payload)
        total, filings = parse_results(data)
        return {
            "total_found": total,
            "returned": len(filings),
            "payload_sent": payload,
            "first_filing": filings[0].model_dump() if filings else None,
        }
    except Exception as e:
        return {"error": str(e)}
