"""Clinic portal authentication (login / session)."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Header
from pydantic import BaseModel, Field

from auth.portal import (
    authenticate,
    create_session_token,
    load_portal_users,
    verify_session_token,
    parse_bearer_token,
)

router = APIRouter(prefix="/auth", tags=["portal-auth"])


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=128)
    password: str = Field(..., min_length=1, max_length=256)


class LoginResponse(BaseModel):
    token: str
    username: str
    display_name: str
    role: str
    expires_in: int


class SessionResponse(BaseModel):
    username: str
    display_name: str
    role: str


@router.post("/login", response_model=LoginResponse)
def portal_login(body: LoginRequest) -> LoginResponse:
    user = authenticate(body.username, body.password)
    token = create_session_token(user)
    from auth.portal import SESSION_TTL_SECONDS

    return LoginResponse(
        token=token,
        username=user.username,
        display_name=user.display_name,
        role=user.role,
        expires_in=SESSION_TTL_SECONDS,
    )


@router.get("/me", response_model=SessionResponse)
def portal_me(authorization: Optional[str] = Header(None)) -> SessionResponse:
    token = parse_bearer_token(authorization)
    session = verify_session_token(token)
    return SessionResponse(
        username=session.username,
        display_name=session.display_name,
        role=session.role,
    )


@router.get("/configured")
def portal_configured() -> dict:
    """Whether portal login is available (no secrets exposed)."""
    users = load_portal_users()
    return {"portal_login_enabled": bool(users)}
