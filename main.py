"""
Global Filings API
==================
FastAPI backend for SGX, ASX, EDGAR, EDINET filings.

Run:
    uvicorn main:app --reload --port 8000
"""

import asyncio
import logging

from dotenv import load_dotenv
load_dotenv()  # must run before routers import so EDINET_API_KEY is set at module load

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import uvicorn

from routers import sgx, asx, edgar, edinet, fca
from routers import suggest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Seed company DB in background — server starts immediately, seed runs async
    async def _seed():
        try:
            from db.seed import seed_all
            await seed_all()
        except Exception as e:
            logger.warning("Company DB seed failed: %s", e)

    asyncio.create_task(_seed())
    yield


app = FastAPI(
    title="Global Filings API",
    description="Regulatory filings from SGX, ASX, EDGAR, EDINET",
    version="2.0.0",
    lifespan=lifespan,
)

templates = Jinja2Templates(directory="templates")

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(sgx.router,     prefix="/api/sgx",     tags=["SGX"])
app.include_router(asx.router,     prefix="/api/asx",     tags=["ASX"])
app.include_router(edgar.router,   prefix="/api/edgar",   tags=["EDGAR"])
app.include_router(edinet.router,  prefix="/api/edinet",  tags=["EDINET"])
app.include_router(fca.router,     prefix="/api/fca",     tags=["FCA"])
app.include_router(suggest.router, prefix="/api/suggest", tags=["Suggest"])


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(request=request, name="home.html", context={})


@app.get("/company", response_class=HTMLResponse)
async def company(request: Request):
    return templates.TemplateResponse(request=request, name="company.html", context={})


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="dashboard.html", context={})


@app.get("/health")
async def health():
    from routers.suggest import suggest_status
    db = suggest_status()
    return {"status": "ok", "markets": ["SGX", "ASX", "EDGAR", "EDINET", "FCA"],
            "company_db": db}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
