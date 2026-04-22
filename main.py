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
load_dotenv()

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
import uvicorn

from routers import sgx, asx, edgar, edinet, fca
from routers import suggest, auth, watchlist

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from db import create_pool, init_schema
    from db.seed import seed_all

    pool = await create_pool()
    await init_schema(pool)
    app.state.pool = pool
    logger.info("DB pool ready")

    async def _seed():
        try:
            await seed_all(pool)
        except Exception as e:
            logger.warning("Company DB seed failed: %s", e)

    asyncio.create_task(_seed())

    from alerts.sgx_poller import start_poller, stop_poller
    start_poller(pool)

    yield

    stop_poller()
    await pool.close()


app = FastAPI(
    title="Global Filings API",
    description="Regulatory filings from SGX, ASX, EDGAR, EDINET",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ.get("SECRET_KEY", secrets.token_hex(32)),
    https_only=False,
)

templates = Jinja2Templates(directory="templates")

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(auth.router,      tags=["Auth"])
app.include_router(sgx.router,       prefix="/api/sgx",       tags=["SGX"])
app.include_router(asx.router,       prefix="/api/asx",       tags=["ASX"])
app.include_router(edgar.router,     prefix="/api/edgar",     tags=["EDGAR"])
app.include_router(edinet.router,    prefix="/api/edinet",    tags=["EDINET"])
app.include_router(fca.router,       prefix="/api/fca",       tags=["FCA"])
app.include_router(suggest.router,   prefix="/api/suggest",   tags=["Suggest"])
app.include_router(watchlist.router, prefix="/api/watchlist", tags=["Watchlist"])


async def _user(request: Request) -> dict | None:
    return await auth.get_user(request)


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    user = await _user(request)
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
async def health(request: Request):
    from routers.suggest import suggest_status
    db = await suggest_status(request.app.state.pool)
    return {"status": "ok", "markets": ["SGX", "ASX", "EDGAR", "EDINET", "FCA"],
            "company_db": db}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
