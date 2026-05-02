"""
User settings — Claude API key for AI-powered email summaries.

GET  /api/user/settings  → {"has_claude_key": bool}
PUT  /api/user/settings  → body {"anthropic_api_key": "sk-ant-..."} — saves or clears
"""

import logging
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from routers.auth import get_user

router = APIRouter()
logger = logging.getLogger(__name__)


def _unauth():
    return JSONResponse({"error": "Unauthorized"}, status_code=401)


@router.get("/settings")
async def get_settings(request: Request):
    user = await get_user(request)
    if not user:
        return _unauth()
    async with request.app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT anthropic_api_key FROM users WHERE sub = $1", user["sub"]
        )
    has_key = bool(row and row["anthropic_api_key"])
    return {"has_claude_key": has_key}


@router.put("/settings")
async def save_settings(request: Request):
    user = await get_user(request)
    if not user:
        return _unauth()
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    api_key = (body.get("anthropic_api_key") or "").strip()
    # Basic sanity check — Anthropic keys start with "sk-ant-"
    if api_key and not api_key.startswith("sk-ant-"):
        return JSONResponse({"error": "Invalid API key format"}, status_code=400)

    async with request.app.state.pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET anthropic_api_key = $1 WHERE sub = $2",
            api_key or None,
            user["sub"],
        )
    return {"saved": True, "has_claude_key": bool(api_key)}
