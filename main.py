"""
Global Filings API
==================
FastAPI backend for SGX (Singapore) and ASX (Australia) filings.

Run:
    uvicorn main:app --reload --port 8000

Install:
    pip install fastapi uvicorn requests jinja2 python-multipart aiofiles
"""

import logging

from dotenv import load_dotenv
load_dotenv()  # must run before routers import so EDINET_API_KEY is set at module load

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import uvicorn

from routers import sgx, asx, edgar, edinet, fca

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(
    title="Global Filings API",
    description="Fetch regulatory filings from SGX (Singapore) and ASX (Australia)",
    version="1.0.0",
    lifespan=lifespan,
)

templates = Jinja2Templates(directory="templates")

# Include routers
app.include_router(sgx.router, prefix="/api/sgx", tags=["SGX - Singapore"])
app.include_router(asx.router,   prefix="/api/asx",   tags=["ASX - Australia"])
app.include_router(edgar.router,  prefix="/api/edgar",  tags=["EDGAR - US SEC"])
app.include_router(edinet.router, prefix="/api/edinet", tags=["EDINET - Japan FSA"])
app.include_router(fca.router,    prefix="/api/fca",    tags=["FCA NSM - UK"])


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={}
    )


@app.get("/health")
async def health():
    return {"status": "ok", "markets": ["SGX", "ASX", "EDGAR", "EDINET", "FCA"]}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)