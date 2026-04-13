"""
Company database seed — populates SQLite FTS5 from all exchange sources.

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
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import httpx

# Allow importing routers when running as standalone script
_root = Path(__file__).parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

DB_PATH = Path(__file__).parent / "companies.db"

logger = logging.getLogger(__name__)

# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
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
);

CREATE VIRTUAL TABLE IF NOT EXISTS companies_fts USING fts5(
    name, name_alt, ticker,
    content='companies',
    content_rowid='id'
);

CREATE INDEX IF NOT EXISTS idx_exchange ON companies(exchange);
CREATE INDEX IF NOT EXISTS idx_ticker   ON companies(ticker);
"""

HEADERS = {
    "User-Agent": "GlobalFilings research@globalfilings.com",
    "Accept":     "application/json",
}


# ── Per-exchange seeders ───────────────────────────────────────────────────────

async def seed_edgar(conn: sqlite3.Connection, client: httpx.AsyncClient) -> int:
    """~10 K US companies from SEC company_tickers.json."""
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
    _bulk_insert(conn, rows)
    return len(rows)


async def seed_edinet(conn: sqlite3.Connection) -> int:
    """~30 K JP companies from edinet-tools bundled CSV (no API key needed)."""
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
            name_jp if name_en else None,  # alt = JP when EN is primary
            ticker,
            "edinet", "TSE",
            sector,
            edinet_code, "edinet_code",
            ticker,
            _now(),
        ))
    _bulk_insert(conn, rows)
    return len(rows)


async def seed_sgx(conn: sqlite3.Connection) -> int:
    """~700 SG companies from authenticated SGX API."""
    from routers.sgx import _sgx, SGX_API_BASE

    # Security list has ticker codes; company list has canonical names
    sec_data = await _sgx.get(SGX_API_BASE + "/securitylist")
    securities = sec_data.get("data", []) if isinstance(sec_data, dict) else []

    # Map: UPPERCASED issuer name → stock code
    ticker_map: dict[str, str] = {}
    for s in (securities if isinstance(securities, list) else []):
        if isinstance(s, dict):
            issuer = (s.get("issuer_name") or "").upper()
            code   = s.get("stock_code") or s.get("nc") or None
            if issuer and code:
                ticker_map[issuer] = code

    co_data  = await _sgx.get(SGX_API_BASE + "/companylist")
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
    _bulk_insert(conn, rows)
    return len(rows)


async def seed_asx(conn: sqlite3.Connection, client: httpx.AsyncClient) -> int:
    """~2 K AU companies from ASX company directory."""
    asx_headers = {
        **HEADERS,
        "Origin":  "https://www.asx.com.au",
        "Referer": "https://www.asx.com.au/",
    }
    # Try CSV file endpoint first (most complete)
    try:
        r = await client.get(
            "https://asx.api.markitdigital.com/asx-research/1.0/companies/directory/file",
            headers=asx_headers,
        )
        r.raise_for_status()
        return _parse_asx_csv(conn, r.text)
    except Exception:
        pass

    # Fallback: JSON directory
    try:
        r = await client.get(
            "https://asx.api.markitdigital.com/asx-research/1.0/companies/directory",
            headers=asx_headers,
        )
        r.raise_for_status()
        data = r.json()
        companies = (data.get("data") or {})
        if isinstance(companies, dict):
            companies = companies.get("listedSecurities") or []
        rows = []
        for c in (companies if isinstance(companies, list) else []):
            name      = c.get("displayName") or c.get("companyName") or ""
            ticker    = (c.get("ticker") or c.get("symbol") or "").upper()
            xid       = str(c.get("entityXid") or c.get("xid") or "")
            sector    = c.get("industrySector") or c.get("sector") or None
            if not name or not xid:
                continue
            rows.append((name, None, ticker or None, "asx", "ASX", sector,
                         xid, "asx_xid", ticker or None, _now()))
        _bulk_insert(conn, rows)
        return len(rows)
    except Exception as e:
        logger.warning("ASX seed failed: %s", e)
        return 0


def _parse_asx_csv(conn: sqlite3.Connection, text: str) -> int:
    """Parse ASX company directory CSV into companies table."""
    import csv, io
    rows = []
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        # Column names vary — try common ones
        name   = (row.get("Company name") or row.get("CompanyName") or
                  row.get("company_name") or "").strip()
        ticker = (row.get("ASX code") or row.get("ASXCode") or
                  row.get("ticker") or "").strip().upper()
        sector = (row.get("GICS industry group") or row.get("Sector") or
                  row.get("sector") or None)
        if not name or not ticker:
            continue
        # ASX: lookup by ticker symbol (entityXid not in CSV — use ticker)
        rows.append((name, None, ticker, "asx", "ASX", sector,
                     ticker, "asx_ticker", ticker, _now()))
    _bulk_insert(conn, rows)
    return len(rows)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.utcnow().isoformat()


def _bulk_insert(conn: sqlite3.Connection, rows: list):
    conn.executemany(
        """INSERT INTO companies
           (name,name_alt,ticker,exchange,exchange_label,sector,
            lookup_key,lookup_type,symbol,updated_at)
           VALUES(?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()


def _rebuild_fts(conn: sqlite3.Connection):
    conn.execute("INSERT INTO companies_fts(companies_fts) VALUES('rebuild')")
    conn.commit()


def _is_stale(conn: sqlite3.Connection) -> bool:
    """True if DB is empty or older than 7 days."""
    count = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    if count == 0:
        return True
    oldest = conn.execute("SELECT MIN(updated_at) FROM companies").fetchone()[0]
    if not oldest:
        return True
    try:
        age = datetime.utcnow() - datetime.fromisoformat(oldest)
        return age > timedelta(days=7)
    except Exception:
        return True


# ── Main seed function ────────────────────────────────────────────────────────

async def seed_all(force: bool = False) -> dict:
    """
    Seed all exchanges. Safe to call at startup — skips if DB is fresh.
    Returns dict with per-exchange counts.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    conn.commit()

    if not force and not _is_stale(conn):
        count = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        logger.info("Company DB up-to-date (%d companies). Skipping seed.", count)
        conn.close()
        return {}

    logger.info("Seeding company database%s…", " (forced)" if force else "")

    # Clear old data
    conn.execute("DELETE FROM companies")
    conn.execute("DELETE FROM companies_fts")
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

    _rebuild_fts(conn)
    total = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    logger.info("Seed complete — %d total companies in DB", total)
    conn.close()
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s — %(message)s")
    asyncio.run(seed_all(force=True))
