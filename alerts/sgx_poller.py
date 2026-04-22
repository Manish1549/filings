"""
SGX filing poller — runs every 60s, detects new filings, sends email alerts.

Redis key per company:
    sgx:last_ts:{lookup_key}  →  broadcast_date_time in ms (int as string)

On first run (key missing): store current time, send no alerts.
Subsequent runs: only alert on filings newer than stored timestamp.
"""

import asyncio
import logging
import os
import time

import httpx
import redis.asyncio as aioredis
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from alerts.emailer import send_filing_alert

logger = logging.getLogger(__name__)

SGX_API_BASE  = "https://api.sgx.com/announcements/v1.1"
SGX_LINKS     = "https://links.sgx.com/1.0.0/corporate-announcements"
POLL_INTERVAL = int(os.environ.get("SGX_POLL_INTERVAL", 60))  # seconds

BASE_HEADERS = {
    "Accept": "*/*",
    "Origin": "https://www.sgx.com",
    "Referer": "https://www.sgx.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}

_redis: aioredis.Redis | None = None
_scheduler: AsyncIOScheduler | None = None


def _redis_key(lookup_key: str) -> str:
    return f"sgx:last_ts:{lookup_key}"


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        _redis = aioredis.from_url(url, decode_responses=True)
    return _redis


async def _get_sgx_token() -> str | None:
    """Reuse token logic from the SGX router singleton."""
    try:
        from routers.sgx import _sgx
        async with httpx.AsyncClient(timeout=15) as client:
            return await _sgx._get_token(client)
    except Exception as e:
        logger.warning("Could not get SGX token for poller: %s", e)
        return None


async def _fetch_latest_filings(company_name: str, since_ms: int, token: str) -> list[dict]:
    """
    Fetch the last 10 filings for a company from SGX.
    Returns only those with broadcast_date_time > since_ms.
    """
    from urllib.parse import quote
    from datetime import datetime, timezone

    now_str = datetime.now(timezone.utc).strftime("%Y%m%d_235959")
    # Use full history start so we catch anything filed today
    since_dt = datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc)
    since_str = since_dt.strftime("%Y%m%d_000000")

    url = (
        f"{SGX_API_BASE}/company"
        f"?periodstart={since_str}&periodend={now_str}"
        f"&value={quote(company_name.upper(), safe='')}&exactsearch=true"
        f"&pagestart=0&pagesize=10"
    )

    headers = {**BASE_HEADERS, "authorizationtoken": token}
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            res = await client.get(url, headers=headers)
            if res.status_code == 401:
                from routers.sgx import _sgx
                _sgx.invalidate_token()
                return []
            if res.status_code >= 400:
                logger.warning("SGX API %d for %s", res.status_code, company_name)
                return []
            data = res.json()
    except Exception as e:
        logger.warning("SGX fetch error for %s: %s", company_name, e)
        return []

    items = data.get("data", []) or []
    new_filings = []
    for item in items:
        bdt = item.get("broadcast_date_time")
        if isinstance(bdt, (int, float)) and int(bdt) > since_ms:
            from routers.sgx import parse_filing
            f = parse_filing(item)
            new_filings.append({
                "title":              f.title,
                "category_name":      f.category_name,
                "cat":                f.cat,
                "submission_date":    f.submission_date,
                "broadcast_date_time": f.broadcast_date_time,
                "url":                f.url,
                "broadcast_ms":       int(bdt),
            })

    return new_filings


async def _get_watched_sgx_companies(pool) -> list[dict]:
    """Return unique SGX companies across all watchlists."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT DISTINCT lookup_key, name, ticker
               FROM watchlist WHERE exchange = 'sgx'"""
        )
    return [dict(r) for r in rows]


async def _get_users_for_company(pool, lookup_key: str) -> list[dict]:
    """Return all users watching a specific SGX company."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT w.user_sub, u.email
               FROM watchlist w
               JOIN users u ON u.sub = w.user_sub
               WHERE w.exchange = 'sgx' AND w.lookup_key = $1""",
            lookup_key,
        )
    return [dict(r) for r in rows]


async def _poll(pool) -> None:
    """Single poll cycle — called every POLL_INTERVAL seconds."""
    companies = await _get_watched_sgx_companies(pool)
    if not companies:
        return

    token = await _get_sgx_token()
    if not token:
        logger.warning("SGX poller: no auth token, skipping cycle")
        return

    r       = await get_redis()
    now_ms  = int(time.time() * 1000)

    for company in companies:
        lookup_key   = company["lookup_key"]
        company_name = company["name"]
        ticker       = company.get("ticker")
        key          = _redis_key(lookup_key)

        last_ts_str = await r.get(key)

        if last_ts_str is None:
            # First time seeing this company — seed Redis, don't alert
            await r.set(key, str(now_ms))
            logger.info("SGX poller: seeded %s at %d", lookup_key, now_ms)
            continue

        since_ms    = int(last_ts_str)
        new_filings = await _fetch_latest_filings(company_name, since_ms, token)

        if not new_filings:
            continue

        logger.info("SGX poller: %d new filing(s) for %s", len(new_filings), company_name)

        newest_ms = max(f["broadcast_ms"] for f in new_filings)

        # Send emails first — update Redis only after all succeed
        # If email fails, Redis stays at old timestamp so next cycle retries
        users = await _get_users_for_company(pool, lookup_key)
        all_sent = True
        loop = asyncio.get_event_loop()
        for user in users:
            email = user.get("email")
            if not email:
                continue
            sent = await loop.run_in_executor(
                None,
                send_filing_alert,
                email, company_name, ticker, new_filings,
            )
            if not sent:
                all_sent = False
                logger.error("SGX poller: email failed for %s → %s", company_name, email)

        if all_sent:
            await r.set(key, str(newest_ms))
        else:
            logger.warning("SGX poller: skipping Redis update for %s — will retry next cycle", company_name)

        # Small delay between companies to be polite to SGX API
        await asyncio.sleep(1)


def start_poller(pool) -> None:
    global _scheduler
    _scheduler = AsyncIOScheduler()
    _scheduler.add_job(
        _poll,
        "interval",
        seconds=POLL_INTERVAL,
        args=[pool],
        id="sgx_poller",
        max_instances=1,          # never overlap if a cycle runs long
        misfire_grace_time=30,
    )
    _scheduler.start()
    logger.info("SGX poller started — interval=%ds", POLL_INTERVAL)


def stop_poller() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("SGX poller stopped")
