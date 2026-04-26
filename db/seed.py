"""
Company database seed — populates Supabase PostgreSQL from all exchange sources.

Run standalone:  python db/seed.py
Called at startup — skips if DB is fresh (< 7 days old).

Sources:
  EDGAR  — SEC company_tickers.json      (~10 K US companies)
  EDINET — edinet-tools bundled CSV      (~30 K JP companies)
  SGX    — SGX /companylist + /securitylist (~700 SG companies)
  ASX    — ASX company directory CSV     (~2 K AU companies)
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import asyncpg

_root = Path(__file__).parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "GlobalFilings research@globalfilings.com",
    "Accept":     "application/json",
}

_COLUMNS = [
    "name", "name_alt", "ticker", "exchange", "exchange_label",
    "sector", "lookup_key", "lookup_type", "symbol", "updated_at",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.utcnow().isoformat()


async def _bulk_insert(pool: asyncpg.Pool, rows: list):
    if not rows:
        return
    async with pool.acquire() as conn:
        await conn.copy_records_to_table("companies", records=rows, columns=_COLUMNS)


async def _is_stale(pool: asyncpg.Pool) -> bool:
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM companies")
        if count == 0:
            return True
        # If any expected exchange has 0 companies, seed is incomplete
        by_exchange = await conn.fetch(
            "SELECT exchange, COUNT(*) AS n FROM companies GROUP BY exchange"
        )
        seeded = {r["exchange"] for r in by_exchange if r["n"] > 0}
        required = {"edgar", "edinet", "sgx", "asx"}
        if os.environ.get("FINANCIALREPORTS_API_KEY"):
            required.add("europe")
        if not required.issubset(seeded):
            return True
        oldest = await conn.fetchval("SELECT MIN(updated_at) FROM companies")
    if not oldest:
        return True
    try:
        age = datetime.utcnow() - datetime.fromisoformat(str(oldest))
        return age > timedelta(days=7)
    except Exception:
        return True


# ── Per-exchange seeders ───────────────────────────────────────────────────────

async def seed_edgar(pool: asyncpg.Pool, client: httpx.AsyncClient) -> int:
    r = await client.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS)
    r.raise_for_status()
    rows = []
    for entry in r.json().values():
        cik    = str(entry.get("cik_str", "")).zfill(10)
        ticker = (entry.get("ticker") or "").upper()
        title  = entry.get("title") or ""
        if not title:
            continue
        rows.append((title, None, ticker or None, "edgar", "US", None,
                     cik, "edgar_cik", ticker or None, _now()))
    await _bulk_insert(pool, rows)
    return len(rows)


async def seed_edinet(pool: asyncpg.Pool) -> int:
    from edinet_tools.entity import _get_classifier
    classifier = await asyncio.to_thread(_get_classifier)
    entities   = classifier._edinet_entities
    rows = []
    for edinet_code, raw in entities.items():
        name_en = (raw.get("name_en") or "").strip()
        name_jp = (raw.get("name_jp") or "").strip()
        ticker  = str(raw.get("ticker") or "").strip() or None
        sector  = raw.get("industry") or None
        name    = name_en or name_jp
        if not name:
            continue
        rows.append((
            name, name_jp if name_en else None, ticker,
            "edinet", "TSE", sector,
            edinet_code, "edinet_code", ticker, _now(),
        ))
    await _bulk_insert(pool, rows)
    return len(rows)


async def seed_sgx(pool: asyncpg.Pool) -> int:
    from routers.sgx import _sgx, SGX_API_BASE

    sec_data   = await _sgx.get(SGX_API_BASE + "/securitylist")
    securities = sec_data.get("data", []) if isinstance(sec_data, dict) else []

    ticker_map: dict[str, str] = {}
    for s in (securities if isinstance(securities, list) else []):
        if isinstance(s, dict):
            issuer = (s.get("issuer_name") or "").upper()
            code   = s.get("stock_code") or s.get("nc") or None
            if issuer and code:
                ticker_map[issuer] = code

    co_data   = await _sgx.get(SGX_API_BASE + "/companylist")
    companies = co_data.get("data", []) if isinstance(co_data, dict) else co_data

    rows = []
    for c in (companies if isinstance(companies, list) else []):
        if isinstance(c, str):
            name   = c.strip()
            ticker = ticker_map.get(name.upper())
        elif isinstance(c, dict):
            name   = (c.get("issuer_name") or c.get("name") or "").strip()
            ticker = c.get("stock_code") or ticker_map.get(name.upper())
        else:
            continue
        if not name:
            continue
        rows.append((name, None, ticker, "sgx", "SGX", None,
                     name.upper(), "sgx_name", ticker, _now()))
    await _bulk_insert(pool, rows)
    return len(rows)


async def seed_asx(pool: asyncpg.Pool, client: httpx.AsyncClient) -> int:
    asx_headers = {**HEADERS, "Origin": "https://www.asx.com.au", "Referer": "https://www.asx.com.au/"}
    try:
        r = await client.get(
            "https://asx.api.markitdigital.com/asx-research/1.0/companies/directory/file",
            headers=asx_headers,
        )
        r.raise_for_status()
        return await _parse_asx_csv(pool, r.text)
    except Exception:
        pass
    try:
        r = await client.get(
            "https://asx.api.markitdigital.com/asx-research/1.0/companies/directory",
            headers=asx_headers,
        )
        r.raise_for_status()
        data      = r.json()
        companies = (data.get("data") or {})
        if isinstance(companies, dict):
            companies = companies.get("listedSecurities") or []
        rows = []
        for c in (companies if isinstance(companies, list) else []):
            name   = c.get("displayName") or c.get("companyName") or ""
            ticker = (c.get("ticker") or c.get("symbol") or "").upper()
            xid    = str(c.get("entityXid") or c.get("xid") or "")
            sector = c.get("industrySector") or c.get("sector") or None
            if not name or not xid:
                continue
            rows.append((name, None, ticker or None, "asx", "ASX", sector,
                         xid, "asx_xid", ticker or None, _now()))
        await _bulk_insert(pool, rows)
        return len(rows)
    except Exception as e:
        logger.warning("ASX seed failed: %s", e)
        return 0


async def _parse_asx_csv(pool: asyncpg.Pool, text: str) -> int:
    import csv, io
    rows = []
    for row in csv.DictReader(io.StringIO(text)):
        name   = (row.get("Company name") or row.get("CompanyName") or "").strip()
        ticker = (row.get("ASX code") or row.get("ASXCode") or "").strip().upper()
        sector = row.get("GICS industry group") or row.get("Sector") or None
        if not name or not ticker:
            continue
        rows.append((name, None, ticker, "asx", "ASX", sector,
                     ticker, "asx_ticker", ticker, _now()))
    await _bulk_insert(pool, rows)
    return len(rows)


async def seed_europe(pool: asyncpg.Pool, client: httpx.AsyncClient) -> int:
    api_key = os.environ.get("FINANCIALREPORTS_API_KEY", "")
    if not api_key:
        logger.warning("FINANCIALREPORTS_API_KEY not set — skipping Europe seed")
        return 0

    headers = {
        "X-API-Key": api_key,
        "Accept": "application/json",
        "User-Agent": "GlobalFilings research@globalfilings.com",
    }

    rows = []
    page = 1

    while True:
        try:
            res = await client.get(
                "https://api.financialreports.eu/companies/",
                headers=headers,
                params={"page": page, "page_size": 100, "view": "summary"},
                timeout=30,
            )
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            logger.warning("Europe seed page %d failed: %s", page, e)
            break

        results = data.get("results") or []
        if not results:
            break

        for c in results:
            name    = (c.get("name") or "").strip()
            company_id = str(c.get("id", ""))
            if not name or not company_id:
                continue
            isins   = c.get("isins") or []
            isin    = isins[0].get("isin") if isins and isinstance(isins[0], dict) else None
            stocks  = c.get("listed_stock_exchanges") or []
            ticker  = stocks[0].get("ticker_symbol") if stocks and isinstance(stocks[0], dict) else None
            country = c.get("country_of_registration") or c.get("country") or "EU"
            rows.append((
                name, None, ticker or None,
                "europe", country, c.get("sub_industry_code") or None,
                company_id, "europe_id", ticker or None, _now(),
            ))

        if not data.get("next"):
            break
        page += 1
        import asyncio; await asyncio.sleep(0.3)  # be polite to rate limits

    await _bulk_insert(pool, rows)
    return len(rows)


# ── Main seed function ────────────────────────────────────────────────────────

async def seed_all(pool: asyncpg.Pool, force: bool = False) -> dict:
    if not force and not await _is_stale(pool):
        count = await pool.fetchval("SELECT COUNT(*) FROM companies")
        logger.info("Company DB up-to-date (%d companies). Skipping seed.", count)
        return {}

    logger.info("Seeding company database%s…", " (forced)" if force else "")

    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE companies RESTART IDENTITY")

    counts: dict[str, int] = {}
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for label, coro in [
            ("edgar",  seed_edgar(pool, client)),
            ("edinet", seed_edinet(pool)),
            ("sgx",    seed_sgx(pool)),
            ("asx",    seed_asx(pool, client)),
            ("europe", seed_europe(pool, client)),
        ]:
            try:
                counts[label] = await coro
                logger.info("%-8s %d companies", label.upper(), counts[label])
            except Exception as e:
                logger.warning("%-8s seed failed: %s", label.upper(), e)
                counts[label] = 0

    total = await pool.fetchval("SELECT COUNT(*) FROM companies")
    logger.info("Seed complete — %d total companies in DB", total)
    return counts


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

    async def _main():
        from db import create_pool, init_schema
        pool = await create_pool()
        await init_schema(pool)
        await seed_all(pool, force=True)
        await pool.close()

    asyncio.run(_main())
