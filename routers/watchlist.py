"""
Watchlist — per-user saved companies.

GET    /api/watchlist          → list user's watchlist
POST   /api/watchlist          → add company  { exchange, lookup_key, name, ticker? }
DELETE /api/watchlist/{id}     → remove item
"""

import logging
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from routers.auth import get_user

router = APIRouter()
logger = logging.getLogger(__name__)


def _auth_error():
    return JSONResponse({"error": "Unauthorized"}, status_code=401)


@router.get("")
async def get_watchlist(request: Request):
    user = await get_user(request)
    if not user:
        return _auth_error()
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, exchange, lookup_key, name, ticker,
                      added_at AT TIME ZONE 'UTC' AS added_at
               FROM watchlist WHERE user_sub = $1 ORDER BY added_at DESC""",
            user["sub"],
        )
    return [dict(r) for r in rows]


@router.post("")
async def add_to_watchlist(request: Request):
    user = await get_user(request)
    if not user:
        return _auth_error()
    try:
        body = await request.json()
        exchange   = body["exchange"]
        lookup_key = body["lookup_key"]
        name       = body["name"]
        ticker     = body.get("ticker")
    except Exception:
        return JSONResponse({"error": "Invalid request body"}, status_code=400)

    async with request.app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO watchlist (user_sub, exchange, lookup_key, name, ticker)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (user_sub, exchange, lookup_key) DO NOTHING
               RETURNING id, exchange, lookup_key, name, ticker,
                         added_at AT TIME ZONE 'UTC' AS added_at""",
            user["sub"], exchange, lookup_key, name, ticker,
        )
    if row is None:
        return JSONResponse({"error": "Already in watchlist"}, status_code=409)
    return dict(row)


@router.delete("/{item_id}")
async def remove_from_watchlist(request: Request, item_id: int):
    user = await get_user(request)
    if not user:
        return _auth_error()
    async with request.app.state.pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM watchlist WHERE id = $1 AND user_sub = $2",
            item_id, user["sub"],
        )
    if result == "DELETE 0":
        return JSONResponse({"error": "Not found"}, status_code=404)
    return {"deleted": item_id}
