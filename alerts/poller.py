"""
Unified filing poller — SGX, ASX, EDGAR, EDINET, FCA.
Runs every 60s via APScheduler.

Redis key format: {exchange}:last_ts:{lookup_key}
Value format (per exchange):
  SGX    → broadcast_date_time in ms as string  e.g. "1745000000000"
  ASX    → broadcastDate in ms as string        e.g. "1745000000000"
  EDGAR  → filing date string                   e.g. "2026-04-22"
  EDINET → submitDateTime string                e.g. "2026-04-22 09:15"
  FCA    → submitted_date ISO string            e.g. "2026-04-22T09:15:00Z"
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

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", 60))

_redis: aioredis.Redis | None = None
_scheduler: AsyncIOScheduler | None = None

ASX_API      = "https://asx.api.markitdigital.com/asx-research/1.0"
ASX_CDN      = "https://cdn-api.markitdigital.com/apiman-gateway/ASX/asx-research/1.0"
ASX_HEADERS  = {
    "Accept": "application/json",
    "Origin": "https://www.asx.com.au",
    "Referer": "https://www.asx.com.au/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

EDGAR_BASE    = "https://data.sec.gov/submissions"
EDGAR_HEADERS = {
    "User-Agent": "GlobalFilings research@globalfilings.com",
    "Accept": "application/json",
}

EDINET_BASE    = "https://api.edinet-fsa.go.jp/api/v2"
EDINET_API_KEY = os.environ.get("EDINET_API_KEY", "")
EDINET_HEADERS = {
    "User-Agent": "GlobalFilings research@globalfilings.com",
    "Accept": "application/json",
}

FCA_SEARCH_URL = "https://api.data.fca.org.uk/search?index=fca-nsm-searchdata"
FCA_ARTEFACTS  = "https://data.fca.org.uk/artefacts/"
FCA_HEADERS    = {
    "accept": "application/json",
    "content-type": "application/json",
    "origin": "https://data.fca.org.uk",
    "referer": "https://data.fca.org.uk/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


# ── Redis ──────────────────────────────────────────────────────────────────────

async def _get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        _redis = aioredis.from_url(url, decode_responses=True)
    return _redis


def _rkey(exchange: str, lookup_key: str) -> str:
    return f"{exchange}:last_ts:{lookup_key}"


# ── DB helpers ─────────────────────────────────────────────────────────────────

async def _get_watched(pool, exchange: str) -> list[dict]:
    """Unique companies for an exchange across all watchlists."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT lookup_key, name, ticker FROM watchlist WHERE exchange = $1",
            exchange,
        )
    return [dict(r) for r in rows]


async def _get_users(pool, exchange: str, lookup_key: str) -> list[str]:
    """Return emails of all users watching a company."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT u.email FROM watchlist w
               JOIN users u ON u.sub = w.user_sub
               WHERE w.exchange = $1 AND w.lookup_key = $2""",
            exchange, lookup_key,
        )
    return [r["email"] for r in rows if r["email"]]


async def _notify(pool, exchange: str, lookup_key: str,
                  company_name: str, ticker: str | None, new_filings: list[dict]) -> bool:
    """Send email to all watchers. Returns True if all succeeded."""
    users = await _get_users(pool, exchange, lookup_key)
    if not users:
        return True
    loop = asyncio.get_event_loop()
    all_ok = True
    for email in users:
        sent = await loop.run_in_executor(
            None, send_filing_alert, email, company_name, ticker, new_filings,
        )
        if not sent:
            all_ok = False
            logger.error("Email failed: %s → %s (%s)", exchange, email, company_name)
    return all_ok


# ── SGX ───────────────────────────────────────────────────────────────────────

async def _poll_sgx(pool, r: aioredis.Redis, companies: list[dict]) -> None:
    from urllib.parse import quote
    from datetime import datetime, timezone

    try:
        from routers.sgx import _sgx, SGX_API_BASE, SGX_LINKS, parse_filing
    except Exception as e:
        logger.warning("SGX imports failed: %s", e)
        return

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            token = await _sgx._get_token(client)
    except Exception as e:
        logger.warning("SGX token failed: %s", e)
        return

    now_ms  = int(time.time() * 1000)
    now_str = datetime.now(timezone.utc).strftime("%Y%m%d_235959")

    for company in companies:
        lookup_key   = company["lookup_key"]
        company_name = company["name"]
        ticker       = company.get("ticker")
        key          = _rkey("sgx", lookup_key)

        last_ts_str = await r.get(key)
        if last_ts_str is None:
            await r.set(key, str(now_ms))
            continue

        since_ms  = int(last_ts_str)
        since_dt  = datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc)
        since_str = since_dt.strftime("%Y%m%d_000000")

        url = (
            f"{SGX_API_BASE}/company"
            f"?periodstart={since_str}&periodend={now_str}"
            f"&value={quote(lookup_key, safe='')}&exactsearch=true"
            f"&pagestart=0&pagesize=10"
        )
        try:
            headers = {"authorizationtoken": token, "Accept": "*/*",
                       "Origin": "https://www.sgx.com", "Referer": "https://www.sgx.com/",
                       "User-Agent": "Mozilla/5.0"}
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                res = await client.get(url, headers=headers)
                if res.status_code == 401:
                    _sgx.invalidate_token()
                    continue
                if res.status_code >= 400:
                    continue
                data = res.json()
        except Exception as e:
            logger.warning("SGX fetch error for %s: %s", company_name, e)
            continue

        new_filings = []
        newest_ms   = since_ms
        for item in (data.get("data", []) or []):
            bdt = item.get("broadcast_date_time")
            if isinstance(bdt, (int, float)) and int(bdt) > since_ms:
                f = parse_filing(item)
                new_filings.append({
                    "title":              f.title,
                    "category_name":      f.category_name or f.cat or "",
                    "submission_date":    f.broadcast_date_time or f.submission_date,
                    "url":                f"{SGX_LINKS}/{f.url}" if f.url else "",
                    "_ts":                int(bdt),
                })
                newest_ms = max(newest_ms, int(bdt))

        if not new_filings:
            continue

        logger.info("SGX: %d new filing(s) for %s", len(new_filings), company_name)
        ok = await _notify(pool, "sgx", lookup_key, company_name, ticker, new_filings)
        if ok:
            await r.set(key, str(newest_ms))

        await asyncio.sleep(1)


# ── ASX ───────────────────────────────────────────────────────────────────────

async def _poll_asx(pool, r: aioredis.Redis, companies: list[dict]) -> None:
    from datetime import datetime, timezone, timedelta

    now_ms   = int(time.time() * 1000)
    today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for company in companies:
        lookup_key   = company["lookup_key"]
        company_name = company["name"]
        ticker       = company.get("ticker") or lookup_key
        key          = _rkey("asx", lookup_key)

        last_ts_str = await r.get(key)
        if last_ts_str is None:
            await r.set(key, str(now_ms))
            continue

        since_ms   = int(last_ts_str)
        since_date = datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc)
        # Use one day before to ensure we don't miss filings near midnight
        start_date = (since_date - timedelta(days=1)).strftime("%Y-%m-%d")

        # lookup_type asx_xid uses market endpoint, asx_ticker uses company endpoint
        try:
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                # Try as ticker first (most common from CSV seed)
                url    = f"{ASX_API}/companies/{lookup_key.lower()}/announcements"
                params = {
                    "page": 0, "itemsPerPage": 10,
                    "excludeCanceledDocs": "true",
                    "dateRangeType": "custom",
                    "startDate": start_date, "endDate": today,
                }
                res = await client.get(url, headers=ASX_HEADERS, params=params)
                if res.status_code == 404:
                    # Fallback: treat lookup_key as entityXid
                    url    = f"{ASX_API}/markets/announcements"
                    params = {
                        "page": 0, "itemsPerPage": 10,
                        "excludeCanceledDocs": "true",
                        "entityXids": lookup_key,
                        "dateRangeType": "custom",
                        "startDate": start_date, "endDate": today,
                    }
                    res = await client.get(url, headers=ASX_HEADERS, params=params)
                if res.status_code >= 400:
                    continue
                data = res.json()
        except Exception as e:
            logger.warning("ASX fetch error for %s: %s", company_name, e)
            continue

        inner = (data.get("data") or {})
        items = inner.get("announcementsTimeline") or inner.get("items") or []

        new_filings = []
        newest_ms   = since_ms
        for item in items:
            raw_date = item.get("date") or item.get("broadcastDate") or item.get("tradingDate")
            if not isinstance(raw_date, (int, float)):
                continue
            bdt = int(raw_date)
            if bdt > since_ms:
                doc_key = item.get("documentKey", "")
                new_filings.append({
                    "title":           item.get("headline", "Untitled"),
                    "category_name":   ", ".join(item.get("announcementTypes", [])) or "Announcement",
                    "submission_date": datetime.fromtimestamp(bdt / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                    "url":             f"{ASX_CDN}/file/{doc_key}" if doc_key else "",
                    "_ts":             bdt,
                })
                newest_ms = max(newest_ms, bdt)

        if not new_filings:
            continue

        logger.info("ASX: %d new filing(s) for %s", len(new_filings), company_name)
        ok = await _notify(pool, "asx", lookup_key, company_name, ticker, new_filings)
        if ok:
            await r.set(key, str(newest_ms))

        await asyncio.sleep(0.5)


# ── EDGAR ──────────────────────────────────────────────────────────────────────

async def _poll_edgar(pool, r: aioredis.Redis, companies: list[dict]) -> None:
    from datetime import date

    today = date.today().isoformat()

    for company in companies:
        lookup_key   = company["lookup_key"]     # CIK padded to 10 digits
        company_name = company["name"]
        ticker       = company.get("ticker")
        key          = _rkey("edgar", lookup_key)

        last_date = await r.get(key)
        if last_date is None:
            await r.set(key, today)
            continue

        try:
            url = f"{EDGAR_BASE}/CIK{lookup_key}.json"
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                res = await client.get(url, headers=EDGAR_HEADERS)
                if res.status_code >= 400:
                    continue
                data = res.json()
        except Exception as e:
            logger.warning("EDGAR fetch error for %s: %s", company_name, e)
            continue

        recent    = data.get("filings", {}).get("recent", {})
        accessions = recent.get("accessionNumber", [])
        forms      = recent.get("form", [])
        dates      = recent.get("filingDate", [])
        docs       = recent.get("primaryDocument", [])
        cik_raw    = data.get("cik", lookup_key.lstrip("0"))

        new_filings = []
        newest_date = last_date

        for i, (acc, form, d) in enumerate(zip(accessions, forms, dates)):
            if d <= last_date:
                break  # sorted newest-first, stop when we hit old ones
            doc_file  = docs[i] if i < len(docs) else ""
            acc_clean = acc.replace("-", "")
            doc_url   = (
                f"https://www.sec.gov/Archives/edgar/data/{cik_raw}/{acc_clean}/{doc_file}"
                if doc_file else f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_raw}"
            )
            new_filings.append({
                "title":           f"{form} — {data.get('name', company_name)}",
                "category_name":   form,
                "submission_date": d,
                "url":             doc_url,
                "_ts":             d,
            })
            if d > newest_date:
                newest_date = d

        if not new_filings:
            continue

        logger.info("EDGAR: %d new filing(s) for %s", len(new_filings), company_name)
        ok = await _notify(pool, "edgar", lookup_key, company_name, ticker, new_filings)
        if ok:
            await r.set(key, newest_date)

        await asyncio.sleep(0.15)  # SEC rate limit: 10 req/sec


# ── EDINET ─────────────────────────────────────────────────────────────────────

async def _poll_edinet(pool, r: aioredis.Redis, companies: list[dict]) -> None:
    """
    EDINET is date-based: fetch today's filings ONCE and filter all watched companies.
    This respects the 3s rate limit and avoids per-company API calls.
    """
    from datetime import date, datetime

    if not EDINET_API_KEY:
        return

    today    = date.today().isoformat()
    # Map edinetCode → company info for quick lookup
    watched  = {c["lookup_key"]: c for c in companies}
    if not watched:
        return

    try:
        params = {"date": today, "type": "2", "Subscription-Key": EDINET_API_KEY}
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            res = await client.get(f"{EDINET_BASE}/documents.json",
                                   headers=EDINET_HEADERS, params=params)
            if res.status_code >= 400:
                return
            items = res.json().get("results", [])
    except Exception as e:
        logger.warning("EDINET fetch error: %s", e)
        return

    # Group items by edinetCode
    by_code: dict[str, list] = {}
    for item in items:
        code = item.get("edinetCode", "")
        if code in watched:
            by_code.setdefault(code, []).append(item)

    for edinet_code, filings_raw in by_code.items():
        company      = watched[edinet_code]
        company_name = company["name"]
        ticker       = company.get("ticker")
        key          = _rkey("edinet", edinet_code)

        last_dt = await r.get(key)
        if last_dt is None:
            # Seed with current datetime, alert on nothing today
            await r.set(key, datetime.utcnow().strftime("%Y-%m-%d %H:%M"))
            continue

        new_filings  = []
        newest_dt    = last_dt

        for item in filings_raw:
            submit_dt = (item.get("submitDateTime") or "")[:16]  # "YYYY-MM-DD HH:MM"
            if not submit_dt or submit_dt <= last_dt:
                continue
            doc_id = item.get("docID", "")
            new_filings.append({
                "title":           item.get("docDescription") or item.get("filerName", "Filing"),
                "category_name":   item.get("docTypeCode", ""),
                "submission_date": submit_dt,
                "url":             (
                    f"https://disclosure2.edinet-fsa.go.jp/WZEK0040.aspx?S{doc_id}"
                    if doc_id else ""
                ),
                "_ts":             submit_dt,
            })
            if submit_dt > newest_dt:
                newest_dt = submit_dt

        if not new_filings:
            continue

        logger.info("EDINET: %d new filing(s) for %s", len(new_filings), company_name)
        ok = await _notify(pool, "edinet", edinet_code, company_name, ticker, new_filings)
        if ok:
            await r.set(key, newest_dt)


# ── FCA ────────────────────────────────────────────────────────────────────────

async def _poll_fca(pool, r: aioredis.Redis, companies: list[dict]) -> None:
    from datetime import datetime, timezone, timedelta

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for company in companies:
        lookup_key   = company["lookup_key"]
        company_name = company["name"]
        ticker       = company.get("ticker")
        key          = _rkey("fca", lookup_key)

        last_dt = await r.get(key)
        if last_dt is None:
            await r.set(key, now_iso)
            continue

        # Parse last_dt back to date string for FCA query
        try:
            last_date = last_dt[:10]  # "YYYY-MM-DD"
        except Exception:
            last_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

        payload = {
            "from": 0,
            "size": 10,
            "sortorder": "desc",
            "criteriaObj": {
                "criteria": [
                    {"name": "latest_flag", "value": "Y"},
                    {"name": "company_lei",
                     "value": [company_name, "", "disclose_org", "related_org"]},
                    {"name": "source", "value": "NSM"},
                ],
                "dateCriteria": [
                    {"name": "submitted_date",
                     "value": {"from": f"{last_date}T00:00:00Z", "to": None}},
                ],
            },
        }
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                res = await client.post(FCA_SEARCH_URL, headers=FCA_HEADERS, json=payload)
                if res.status_code >= 400:
                    continue
                hits = res.json().get("hits", {}).get("hits", [])
        except Exception as e:
            logger.warning("FCA fetch error for %s: %s", company_name, e)
            continue

        new_filings = []
        newest_dt   = last_dt

        for hit in hits:
            src      = hit.get("_source", {})
            sub_dt   = src.get("submitted_date", "")
            if not sub_dt or sub_dt <= last_dt:
                continue
            dl = src.get("download_link", "")
            new_filings.append({
                "title":           src.get("headline", "Filing"),
                "category_name":   src.get("type", src.get("category", "")),
                "submission_date": sub_dt[:16].replace("T", " "),
                "url":             f"{FCA_ARTEFACTS}{dl}" if dl else "",
                "_ts":             sub_dt,
            })
            if sub_dt > newest_dt:
                newest_dt = sub_dt

        if not new_filings:
            continue

        logger.info("FCA: %d new filing(s) for %s", len(new_filings), company_name)
        ok = await _notify(pool, "fca", lookup_key, company_name, ticker, new_filings)
        if ok:
            await r.set(key, newest_dt)

        await asyncio.sleep(0.5)


# ── Main poll cycle ────────────────────────────────────────────────────────────

async def _poll(pool) -> None:
    r = await _get_redis()

    # Fetch all watched companies per exchange in parallel DB queries
    sgx_cos, asx_cos, edgar_cos, edinet_cos, fca_cos = await asyncio.gather(
        _get_watched(pool, "sgx"),
        _get_watched(pool, "asx"),
        _get_watched(pool, "edgar"),
        _get_watched(pool, "edinet"),
        _get_watched(pool, "fca"),
    )

    total = sum(len(x) for x in [sgx_cos, asx_cos, edgar_cos, edinet_cos, fca_cos])
    if total == 0:
        return

    logger.debug(
        "Poll cycle: SGX=%d ASX=%d EDGAR=%d EDINET=%d FCA=%d",
        len(sgx_cos), len(asx_cos), len(edgar_cos), len(edinet_cos), len(fca_cos),
    )

    # Run all exchange pollers — sequential to avoid hammering APIs simultaneously
    if sgx_cos:
        await _poll_sgx(pool, r, sgx_cos)
    if asx_cos:
        await _poll_asx(pool, r, asx_cos)
    if edgar_cos:
        await _poll_edgar(pool, r, edgar_cos)
    if edinet_cos:
        await _poll_edinet(pool, r, edinet_cos)
    if fca_cos:
        await _poll_fca(pool, r, fca_cos)


# ── Scheduler ─────────────────────────────────────────────────────────────────

def start_poller(pool) -> None:
    global _scheduler
    _scheduler = AsyncIOScheduler()
    _scheduler.add_job(
        _poll,
        "interval",
        seconds=POLL_INTERVAL,
        args=[pool],
        id="filing_poller",
        max_instances=1,
        misfire_grace_time=30,
    )
    _scheduler.start()
    logger.info("Filing poller started — interval=%ds (SGX, ASX, EDGAR, EDINET, FCA)", POLL_INTERVAL)


def stop_poller() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Filing poller stopped")
