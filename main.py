"""
Global Filings API
==================
FastAPI backend for SGX, ASX, EDGAR, EDINET filings.

Run:
    uvicorn main:app --reload --port 8000
"""

import asyncio
import logging
import os
import secrets

from dotenv import load_dotenv
load_dotenv()  # must run before routers import so EDINET_API_KEY is set at module load

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
import uvicorn

from routers import sgx, asx, edgar, edinet, fca
from routers import suggest
from routers import auth

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

# Session middleware — SECRET_KEY must be set in env (Railway env vars)
app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ.get("SECRET_KEY", secrets.token_hex(32)),
    https_only=False,
)

templates = Jinja2Templates(directory="templates")

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(auth.router,    tags=["Auth"])
app.include_router(sgx.router,     prefix="/api/sgx",     tags=["SGX"])
app.include_router(asx.router,     prefix="/api/asx",     tags=["ASX"])
app.include_router(edgar.router,   prefix="/api/edgar",   tags=["EDGAR"])
app.include_router(edinet.router,  prefix="/api/edinet",  tags=["EDINET"])
app.include_router(fca.router,     prefix="/api/fca",     tags=["FCA"])
app.include_router(suggest.router, prefix="/api/suggest", tags=["Suggest"])


async def _user(request: Request) -> dict | None:
    from routers.auth import get_user
    return await get_user(request)


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    user = await _user(request)
    logger.info("HOME: session_keys=%s user=%s", list(request.session.keys()), bool(user))
    if not user:
        return RedirectResponse("/login")
    return templates.TemplateResponse(request=request, name="home.html", context={"user": user})


@app.get("/company", response_class=HTMLResponse)
async def company(request: Request):
    user = await _user(request)
    if not user:
        return RedirectResponse("/login")
    return templates.TemplateResponse(request=request, name="company.html", context={"user": user})


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    user = await _user(request)
    if not user:
        return RedirectResponse("/login")
    return templates.TemplateResponse(request=request, name="dashboard.html", context={"user": user})


@app.get("/health")
async def health():
    from routers.suggest import suggest_status
    db = suggest_status()
    return {"status": "ok", "markets": ["SGX", "ASX", "EDGAR", "EDINET", "FCA"],
            "company_db": db}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
