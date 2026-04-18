"""
Unified company suggest — asyncpg + tsvector search across all exchanges.

GET /api/suggest?q=toyo
GET /api/suggest?q=DBS&markets=sgx,edgar
GET /api/suggest/status
"""

import re
import logging
from fastapi import APIRouter, Query, Request

router = APIRouter()
logger = logging.getLogger(__name__)

EXCHANGE_META = {
    "edgar":  {"label": "US",  "flag": "🇺🇸"},
    "edinet": {"label": "JP",  "flag": "🇯🇵"},
    "sgx":    {"label": "SG",  "flag": "🇸🇬"},
    "asx":    {"label": "AU",  "flag": "🇦🇺"},
}

_FTS_VEC = (
    "to_tsvector('simple', COALESCE(name,'') || ' ' || "
    "COALESCE(name_alt,'') || ' ' || COALESCE(ticker,''))"
)


def _build_tsquery(q: str) -> str | None:
    words = q.strip().lower().split()
    clean = [re.sub(r"[^\w]", "", w) for w in words]
    valid = [w for w in clean if w]
    if not valid:
        return None
    return " & ".join(f"{w}:*" for w in valid)


@router.get("")
async def suggest_companies(
    request: Request,
    q:       str = Query(..., min_length=1),
    limit:   int = Query(10, ge=1, le=50),
    markets: str = Query("all"),
):
    pool    = request.app.state.pool
    ql      = q.strip().lower()
    ql_pfx  = ql + "%"
    tsquery = _build_tsquery(ql)
    mlist   = None if markets == "all" else [m.strip() for m in markets.split(",") if m.strip()]

    # Fixed param positions:
    # $1=ql  $2=ql_pfx  $3=tsquery|None  $4=mlist|None  $5=limit
    sql = f"""
        SELECT id, name, name_alt, ticker, exchange, exchange_label, sector,
               lookup_key, lookup_type, symbol,
               CASE
                   WHEN LOWER(ticker) = $1        THEN 0
                   WHEN LOWER(ticker) LIKE $2      THEN 1
                   WHEN LOWER(name)   LIKE $2      THEN 2
                   ELSE                                 3
               END AS priority
        FROM companies
        WHERE (
            ($3::text IS NOT NULL AND {_FTS_VEC} @@ to_tsquery('simple', $3))
            OR LOWER(ticker) LIKE $2
            OR LOWER(name)   LIKE $2
        )
        AND ($4::text[] IS NULL OR exchange = ANY($4))
        ORDER BY priority
        LIMIT $5
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, ql, ql_pfx, tsquery, mlist, limit)
    except Exception:
        # tsquery syntax error — fall back to ILIKE only
        sql_fallback = f"""
            SELECT id, name, name_alt, ticker, exchange, exchange_label, sector,
                   lookup_key, lookup_type, symbol,
                   CASE
                       WHEN LOWER(ticker) = $1   THEN 0
                       WHEN LOWER(ticker) LIKE $2 THEN 1
                       WHEN LOWER(name)   LIKE $2 THEN 2
                       ELSE                            3
                   END AS priority
            FROM companies
            WHERE (LOWER(ticker) LIKE $2 OR LOWER(name) LIKE $2)
            AND ($3::text[] IS NULL OR exchange = ANY($3))
            ORDER BY priority LIMIT $4
        """
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(sql_fallback, ql, ql_pfx, mlist, limit)
        except Exception as e:
            logger.exception("Suggest error for q=%r", q)
            return {"query": q, "count": 0, "results": [], "error": str(e)}

    results = []
    for r in rows:
        d = dict(r)
        d.pop("priority", None)
        meta = EXCHANGE_META.get(d["exchange"], {})
        d["exchange_flag"]  = meta.get("flag", "")
        d["exchange_label"] = d.get("exchange_label") or meta.get("label", d["exchange"].upper())
        results.append(d)

    return {"query": q, "count": len(results), "results": results}


async def suggest_status(pool) -> dict:
    try:
        async with pool.acquire() as conn:
            total   = await conn.fetchval("SELECT COUNT(*) FROM companies")
            by_exch = await conn.fetch(
                "SELECT exchange, COUNT(*) AS n, MAX(updated_at) AS last_updated "
                "FROM companies GROUP BY exchange"
            )
        return {
            "seeded":      total > 0,
            "total":       total,
            "by_exchange": [dict(r) for r in by_exch],
        }
    except Exception as e:
        return {"seeded": False, "error": str(e)}
