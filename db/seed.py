"""
Company database seed — populates Supabase PostgreSQL from all exchange sources.

Run standalone:  python db/seed.py
Called at startup if DB is empty or older than 7 days.

Sources:
  EDGAR  — SEC company_tickers.json      (~10 K US companies)
  EDINET — edinet-tools bundled CSV      (~30 K JP companies)
  SGX    — SGX /companylist + /securitylist (~700 SG companies)
  ASX    — ASX company directory CSV     (~2 K AU companies)
"""

import asyncio
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import psycopg2
from psycopg2.extras import execute_batch

_root = Path(__file__).parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

logger = logging.getLogger(__name__)

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS companies (
        id             SERIAL PRIMARY KEY,
        name           TEXT NOT NULL,
        name_alt       TEXT,
        ticker         TEXT,
        exchange       TEXT NOT NULL,
        exchange_label TEXT,
        sector         TEXT,
        lookup_key     TEXT NOT NULL,
        lookup_type    TEXT NOT NULL,
        symbol         TEXT,
        updated_at     TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_exchange ON companies(exchange)",
    "CREATE INDEX IF NOT EXISTS idx_ticker   ON companies(ticker)",
    """
    CREATE INDEX IF NOT EXISTS idx_search_gin ON companies USING GIN(
        to_tsvector('simple',
            COALESCE(name, '') || ' ' ||
            COALESCE(name_alt, '') || ' ' ||
            COALESCE(ticker, '')
        )
    )
    """,
]

HEADERS = {
    "User-Agent": "GlobalFilings research@globalfilings.com",
    "Accept":     "application/json",
}


# ── Per-exchange seeders ───────────────────────────────────────────────────────

async def seed_edgar(conn, client: httpx.AsyncClient) -> int:
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
    await asyncio.to_thread(_bulk_insert, conn, rows)
    return len(rows)


async def seed_edinet(conn) -> int:
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
            name,
            name_jp if name_en else None,
            ticker,
            "edinet", "TSE",
            sector,
            edinet_code, "edinet_code",
            ticker,
            _now(),
        ))
    await asyncio.to_thread(_bulk_insert, conn, rows)
    return len(rows)


async def seed_sgx(conn) -> int:
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
    await asyncio.to_thread(_bulk_insert, conn, rows)
    return len(rows)


async def seed_asx(conn, client: httpx.AsyncClient) -> int:
    asx_headers = {
        **HEADERS,
        "Origin":  "https://www.asx.com.au",
        "Referer": "https://www.asx.com.au/",
    }
    try:
        r = await client.get(
            "https://asx.api.markitdigital.com/asx-research/1.0/companies/directory/file",
            headers=asx_headers,
        )
        r.raise_for_status()
        count = await asyncio.to_thread(_parse_asx_csv, conn, r.text)
        return count
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
        await asyncio.to_thread(_bulk_insert, conn, rows)
        return len(rows)
    except Exception as e:
        logger.warning("ASX seed failed: %s", e)
        return 0


def _parse_asx_csv(conn, text: str) -> int:
    import csv, io
    rows = []
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        name   = (row.get("Company name") or row.get("CompanyName") or
                  row.get("company_name") or "").strip()
        ticker = (row.get("ASX code") or row.get("ASXCode") or
                  row.get("ticker") or "").strip().upper()
        sector = (row.get("GICS industry group") or row.get("Sector") or
                  row.get("sector") or None)
        if not name or not ticker:
            continue
        rows.append((name, None, ticker, "asx", "ASX", sector,
                     ticker, "asx_ticker", ticker, _now()))
    _bulk_insert(conn, rows)
    return len(rows)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.utcnow().isoformat()


def _bulk_insert(conn, rows: list):
    if not rows:
        return
    with conn.cursor() as cur:
        execute_batch(
            cur,
            """INSERT INTO companies
               (name, name_alt, ticker, exchange, exchange_label, sector,
                lookup_key, lookup_type, symbol, updated_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            rows,
            page_size=500,
        )
    conn.commit()


def _is_stale(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM companies")
        count = cur.fetchone()[0]
        if count == 0:
            return True
        cur.execute("SELECT MIN(updated_at) FROM companies")
        oldest = cur.fetchone()[0]
    if not oldest:
        return True
    try:
        age = datetime.utcnow() - datetime.fromisoformat(oldest)
        return age > timedelta(days=7)
    except Exception:
        return True


def _apply_schema(conn):
    with conn.cursor() as cur:
        for stmt in SCHEMA_STATEMENTS:
            cur.execute(stmt)
    conn.commit()


# ── Main seed function ────────────────────────────────────────────────────────

async def seed_all(force: bool = False) -> dict:
    from db import get_conn
    conn = await asyncio.to_thread(get_conn)

    await asyncio.to_thread(_apply_schema, conn)

    if not force and not await asyncio.to_thread(_is_stale, conn):
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM companies")
            count = cur.fetchone()[0]
        logger.info("Company DB up-to-date (%d companies). Skipping seed.", count)
        conn.close()
        return {}

    logger.info("Seeding company database%s…", " (forced)" if force else "")

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE companies RESTART IDENTITY")
    conn.commit()

    counts: dict[str, int] = {}
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for label, coro in [
            ("edgar",  seed_edgar(conn, client)),
            ("edinet", seed_edinet(conn)),
            ("sgx",    seed_sgx(conn)),
            ("asx",    seed_asx(conn, client)),
        ]:
            try:
                counts[label] = await coro
                logger.info("%-8s %d companies", label.upper(), counts[label])
            except Exception as e:
                logger.warning("%-8s seed failed: %s", label.upper(), e)
                counts[label] = 0

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM companies")
        total = cur.fetchone()[0]
    logger.info("Seed complete — %d total companies in DB", total)
    conn.close()
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s — %(message)s")
    asyncio.run(seed_all(force=True))
