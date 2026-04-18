"""
Auth0 authentication — official auth0-server-python SDK.

Adapts the SDK's AbstractDataStore to use Starlette's SessionMiddleware
as the session backing store (instead of Flask's after_this_request cookie pattern).

Routes:
  GET /login      → redirect to Auth0 Universal Login
  GET /callback   → exchange code, store session
  GET /logout     → clear session + Auth0 logout
  GET /me         → JSON: current user or 401
"""

import os
import logging
from typing import Any, Optional

from auth0_server_python.auth_server.server_client import ServerClient
from auth0_server_python.auth_types import (
    LogoutOptions,
    StartInteractiveLoginOptions,
    StateData,
    TransactionData,
)
from auth0_server_python.store.abstract import AbstractDataStore
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse

router = APIRouter()
logger = logging.getLogger(__name__)

AUTH0_SECRET        = os.environ.get("AUTH0_SECRET", "")
AUTH0_DOMAIN        = os.environ.get("AUTH0_DOMAIN", "")
AUTH0_CLIENT_ID     = os.environ.get("AUTH0_CLIENT_ID", "")
AUTH0_CLIENT_SECRET = os.environ.get("AUTH0_CLIENT_SECRET", "")
APP_BASE_URL        = os.environ.get("APP_BASE_URL", "http://localhost:8000")


class StarletteSessionStore(AbstractDataStore):
    """
    Auth0 SDK store backed by Starlette's SessionMiddleware.

    SDK passes store_options={"request": request} to every method.
    We store the encrypted payload in request.session[key] so the
    SessionMiddleware cookie handles persistence automatically.
    """

    def __init__(self, secret: str, session_key: str, model):
        super().__init__({"secret": secret})
        self.session_key = session_key
        self.model = model

    def _req(self, options: Optional[dict]) -> Optional[Request]:
        return (options or {}).get("request")

    async def set(
        self,
        identifier: str,
        state: Any,
        remove_if_expires: bool = False,
        options: Optional[dict] = None,
    ) -> None:
        req = self._req(options)
        if req is None:
            return
        data = state.model_dump() if hasattr(state, "model_dump") else state
        req.session[self.session_key] = self.encrypt(identifier, data)

    async def get(self, identifier: str, options: Optional[dict] = None) -> Any:
        req = self._req(options)
        if req is None:
            return None
        encrypted = req.session.get(self.session_key)
        if not encrypted:
            return None
        try:
            return self.model.model_validate(self.decrypt(identifier, encrypted))
        except Exception:
            return None

    async def delete(self, identifier: str, options: Optional[dict] = None) -> None:
        req = self._req(options)
        if req:
            req.session.pop(self.session_key, None)


def make_client() -> ServerClient:
    """Create a fresh ServerClient per request (stores are stateless wrappers)."""
    return ServerClient(
        domain=AUTH0_DOMAIN,
        client_id=AUTH0_CLIENT_ID,
        client_secret=AUTH0_CLIENT_SECRET,
        redirect_uri=APP_BASE_URL + "/callback",
        secret=AUTH0_SECRET,
        authorization_params={"scope": "openid profile email"},
        state_store=StarletteSessionStore(AUTH0_SECRET, "_a0_session", StateData),
        transaction_store=StarletteSessionStore(AUTH0_SECRET, "_a0_tx", TransactionData),
    )


async def get_user(request: Request) -> Optional[dict]:
    """Return the logged-in user dict, or None if unauthenticated."""
    try:
        return await make_client().get_user(store_options={"request": request})
    except Exception:
        return None


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/login")
async def login(request: Request):
    url = await make_client().start_interactive_login(
        options=StartInteractiveLoginOptions(
            authorization_params=dict(request.query_params),
        ),
        store_options={"request": request},
    )
    return RedirectResponse(url)


@router.get("/callback")
async def callback(request: Request):
    try:
        await make_client().complete_interactive_login(
            url=str(request.url),
            store_options={"request": request},
        )
        return RedirectResponse("/")
    except Exception:
        logger.exception("Auth0 callback error")
        return RedirectResponse("/login")


@router.get("/logout")
async def logout(request: Request):
    url = await make_client().logout(
        options=LogoutOptions(return_to=APP_BASE_URL),
        store_options={"request": request},
    )
    return RedirectResponse(url)


@router.get("/me")
async def me(request: Request):
    user = await get_user(request)
    if not user:
        return JSONResponse({"authenticated": False}, status_code=401)
    return {"authenticated": True, "user": user}
