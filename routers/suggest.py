"""
Unified company suggest — PostgreSQL tsvector-powered endpoint for all exchanges.

GET /api/suggest?q=toyo          → top 10 across all markets
GET /api/suggest?q=633           → ticker prefix match
GET /api/suggest?q=DBS&markets=sgx,edgar
GET /api/suggest/status          → DB stats
"""

import re
import logging
from fastapi import APIRouter, Query
from db import get_conn

router = APIRouter()
logger = logging.getLogger(__name__)

EXCHANGE_META = {
    "edgar":  {"label": "US",  "flag": "🇺🇸"},
    "edinet": {"label": "JP",  "flag": "🇯🇵"},
    "sgx":    {"label": "SG",  "flag": "🇸🇬"},
    "asx":    {"label": "AU",  "flag": "🇦🇺"},
}


def _build_tsquery(q: str) -> str | None:
    words = q.strip().lower().split()
    clean = [re.sub(r"[^\w]", "", w) for w in words]
    valid = [w for w in clean if w]
    if not valid:
        return None
    return " & ".join(f"{w}:*" for w in valid)


@router.get("")
def suggest_companies(
    q:       str = Query(..., min_length=1),
    limit:   int = Query(10, ge=1, le=50),
    markets: str = Query("all"),
):
    ql      = q.strip().lower()
    ql_pfx  = ql + "%"
    tsquery = _build_tsquery(ql)

    market_clause = ""
    market_vals: list = []
    if markets != "all":
        mlist = [m.strip() for m in markets.split(",") if m.strip()]
        if mlist:
            placeholders = ", ".join("%s" for _ in mlist)
            market_clause = f"AND exchange IN ({placeholders})"
            market_vals   = mlist

    fts_vec = (
        "to_tsvector('simple', COALESCE(name,'') || ' ' || "
        "COALESCE(name_alt,'') || ' ' || COALESCE(ticker,''))"
    )

    if tsquery:
        where = f"({fts_vec} @@ to_tsquery('simple', %s) OR LOWER(ticker) LIKE %s OR LOWER(name) LIKE %s)"
        where_vals = [tsquery, ql_pfx, ql_pfx]
    else:
        where = "(LOWER(ticker) LIKE %s OR LOWER(name) LIKE %s)"
        where_vals = [ql_pfx, ql_pfx]

    sql = f"""
        SELECT id, name, name_alt, ticker, exchange, exchange_label, sector,
               lookup_key, lookup_type, symbol,
               CASE
                   WHEN LOWER(ticker) = %s          THEN 0
                   WHEN LOWER(ticker) LIKE %s        THEN 1
                   WHEN LOWER(name)   LIKE %s        THEN 2
                   ELSE                                   3
               END AS priority
        FROM companies
        WHERE {where}
        {market_clause}
        ORDER BY priority
        LIMIT %s
    """
    params = [ql, ql_pfx, ql_pfx] + where_vals + market_vals + [limit]

    try:
        conn = get_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        conn.close()

        results = []
        for r in rows:
            r.pop("priority", None)
            meta = EXCHANGE_META.get(r["exchange"], {})
            r["exchange_flag"]  = meta.get("flag", "")
            r["exchange_label"] = r.get("exchange_label") or meta.get("label", r["exchange"].upper())
            results.append(r)

        return {"query": q, "count": len(results), "results": results}
    except Exception as e:
        logger.exception("Suggest error for q=%r", q)
        return {"query": q, "count": 0, "results": [], "error": str(e)}


@router.get("/status")
def suggest_status():
    try:
        conn = get_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM companies")
            total = cur.fetchone()[0]
            cur.execute(
                "SELECT exchange, COUNT(*) as n, MAX(updated_at) as last_updated "
                "FROM companies GROUP BY exchange"
            )
            cols     = [d[0] for d in cur.description]
            by_exch  = [dict(zip(cols, row)) for row in cur.fetchall()]
        conn.close()
        return {"seeded": total > 0, "total": total, "by_exchange": by_exch}
    except Exception as e:
        return {"seeded": False, "error": str(e)}
