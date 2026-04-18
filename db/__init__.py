import os
import asyncpg

DATABASE_URL = os.environ.get("DATABASE_URL", "")

SCHEMA = """
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
);
CREATE INDEX IF NOT EXISTS idx_exchange   ON companies(exchange);
CREATE INDEX IF NOT EXISTS idx_ticker     ON companies(ticker);
CREATE INDEX IF NOT EXISTS idx_search_gin ON companies USING GIN(
    to_tsvector('simple',
        COALESCE(name,'') || ' ' || COALESCE(name_alt,'') || ' ' || COALESCE(ticker,'')
    )
);
CREATE TABLE IF NOT EXISTS watchlist (
    id         SERIAL PRIMARY KEY,
    user_sub   TEXT NOT NULL,
    exchange   TEXT NOT NULL,
    lookup_key TEXT NOT NULL,
    name       TEXT NOT NULL,
    ticker     TEXT,
    added_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_sub, exchange, lookup_key)
);
CREATE INDEX IF NOT EXISTS idx_watchlist_user ON watchlist(user_sub);
"""


async def create_pool() -> asyncpg.Pool:
    url = DATABASE_URL
    if not url:
        raise RuntimeError("DATABASE_URL environment variable not set")
    if "?" in url:
        url = url.split("?")[0]
    return await asyncpg.create_pool(url, ssl="require", min_size=2, max_size=10)


async def init_schema(pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA)
