"""
Unified company suggest — single FTS5-powered endpoint for all exchanges.

GET /api/suggest?q=toyo          → top 10 across all markets
GET /api/suggest?q=633           → ticker prefix match (TSE.6330, 6331 …)
GET /api/suggest?q=DBS&markets=sgx,edgar
GET /api/suggest/status          → DB stats
"""

import sqlite3
import logging
from fastapi import APIRouter, Query
from db import DB_PATH

router = APIRouter()
logger = logging.getLogger(__name__)

# Exchange display config used by the frontend
EXCHANGE_META = {
    "edgar":  {"label": "US",  "flag": "🇺🇸"},
    "edinet": {"label": "JP",  "flag": "🇯🇵"},
    "sgx":    {"label": "SG",  "flag": "🇸🇬"},
    "asx":    {"label": "AU",  "flag": "🇦🇺"},
}


@router.get("")
def suggest_companies(
    q:       str = Query(..., min_length=1, description="Search query — name or ticker prefix"),
    limit:   int = Query(10, ge=1, le=50),
    markets: str = Query("all", description="Comma-separated exchanges to search, or 'all'"),
):
    """
    Full-text + ticker-prefix search across all exchanges.

    Ranking (lower = better):
      0 — exact ticker match      (e.g. "DBS" → DBS Group)
      1 — ticker starts with q    (e.g. "633" → TSE.6330, 6331 …)
      2 — name starts with q      (e.g. "toyo" → Toyo Engineering …)
      3 — name/alt contains q     (e.g. "motor" → Toyota Motor …)
    """
    if not DB_PATH.exists():
        return {"query": q, "count": 0, "results": [],
                "error": "Database not seeded yet — restart server or run: python db/seed.py"}

    ql      = q.strip().lower()
    # Build FTS5 prefix query: each word gets a * suffix
    words   = ql.split()
    fts_q   = " ".join(w.replace('"', '""') + "*" for w in words)

    # Market filter clause
    market_clause  = ""
    market_params: dict = {}
    if markets != "all":
        mlist = [m.strip() for m in markets.split(",") if m.strip()]
        if mlist:
            clauses = " OR ".join(f"c.exchange = :m{i}" for i in range(len(mlist)))
            market_clause = f"AND ({clauses})"
            market_params = {f"m{i}": m for i, m in enumerate(mlist)}

    sql = f"""
        SELECT
            c.id, c.name, c.name_alt, c.ticker,
            c.exchange, c.exchange_label, c.sector,
            c.lookup_key, c.lookup_type, c.symbol,
            CASE
                WHEN LOWER(c.ticker) = :ql                 THEN 0
                WHEN LOWER(c.ticker) LIKE :ql_pfx          THEN 1
                WHEN LOWER(c.name)   LIKE :ql_pfx          THEN 2
                ELSE                                             3
            END AS priority
        FROM companies_fts f
        JOIN companies c ON c.id = f.rowid
        WHERE companies_fts MATCH :fts_q
        {market_clause}
        ORDER BY priority, rank
        LIMIT :lim
    """
    params = {"fts_q": fts_q, "ql": ql, "ql_pfx": ql + "%",
              "lim": limit, **market_params}

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows    = conn.execute(sql, params).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            d.pop("priority", None)
            # Attach display meta
            meta = EXCHANGE_META.get(d["exchange"], {})
            d["exchange_flag"]  = meta.get("flag", "")
            d["exchange_label"] = d.get("exchange_label") or meta.get("label", d["exchange"].upper())
            results.append(d)
        conn.close()
        return {"query": q, "count": len(results), "results": results}
    except Exception as e:
        logger.exception("Suggest error for q=%r", q)
        return {"query": q, "count": 0, "results": [], "error": str(e)}


@router.get("/status")
def suggest_status():
    """DB stats — total companies per exchange and last seed time."""
    if not DB_PATH.exists():
        return {"seeded": False}
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        total  = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        by_exch = conn.execute(
            "SELECT exchange, COUNT(*) as n, MAX(updated_at) as last_updated "
            "FROM companies GROUP BY exchange"
        ).fetchall()
        conn.close()
        return {
            "seeded": total > 0,
            "total":  total,
            "by_exchange": [dict(r) for r in by_exch],
        }
    except Exception as e:
        return {"seeded": False, "error": str(e)}
