"""
Auth0 authentication router.

Routes:
  GET /login      → redirect to Auth0 Universal Login
  GET /callback   → exchange code for tokens, store user in session
  GET /logout     → clear session + redirect to Auth0 logout
  GET /me         → JSON: current user info (or 401)
"""

import os
import logging
from urllib.parse import urlencode, quote_plus

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

router = APIRouter()
logger = logging.getLogger(__name__)

AUTH0_DOMAIN        = os.environ.get("AUTH0_DOMAIN", "")
AUTH0_CLIENT_ID     = os.environ.get("AUTH0_CLIENT_ID", "")
AUTH0_CLIENT_SECRET = os.environ.get("AUTH0_CLIENT_SECRET", "")
AUTH0_CALLBACK_URL  = os.environ.get("AUTH0_CALLBACK_URL", "http://localhost:8000/callback")


@router.get("/login")
async def login(request: Request):
    params = urlencode({
        "response_type": "code",
        "client_id":     AUTH0_CLIENT_ID,
        "redirect_uri":  AUTH0_CALLBACK_URL,
        "scope":         "openid profile email",
        "audience":      f"https://{AUTH0_DOMAIN}/userinfo",
    })
    return RedirectResponse(f"https://{AUTH0_DOMAIN}/authorize?{params}")


@router.get("/callback")
async def callback(request: Request):
    code = request.query_params.get("code")
    if not code:
        return RedirectResponse("/login")

    async with httpx.AsyncClient() as client:
        token_res = await client.post(
            f"https://{AUTH0_DOMAIN}/oauth/token",
            json={
                "grant_type":    "authorization_code",
                "client_id":     AUTH0_CLIENT_ID,
                "client_secret": AUTH0_CLIENT_SECRET,
                "code":          code,
                "redirect_uri":  AUTH0_CALLBACK_URL,
            },
        )
        if token_res.status_code != 200:
            logger.error("Token exchange failed: %s", token_res.text)
            return RedirectResponse("/login")

        tokens = token_res.json()
        access_token = tokens.get("access_token")

        userinfo_res = await client.get(
            f"https://{AUTH0_DOMAIN}/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        user = userinfo_res.json()

    request.session["user"] = {
        "sub":     user.get("sub", ""),
        "name":    user.get("name", ""),
        "email":   user.get("email", ""),
        "picture": user.get("picture", ""),
    }
    return RedirectResponse("/")


@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    params = urlencode({
        "returnTo":  str(request.base_url),
        "client_id": AUTH0_CLIENT_ID,
    }, quote_via=quote_plus)
    return RedirectResponse(f"https://{AUTH0_DOMAIN}/v2/logout?{params}")


@router.get("/me")
async def me(request: Request):
    user = request.session.get("user")
    if not user:
        from fastapi.responses import JSONResponse
        return JSONResponse({"authenticated": False}, status_code=401)
    return {"authenticated": True, "user": user}
