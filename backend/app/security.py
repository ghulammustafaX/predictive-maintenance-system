"""
Module 8 — Authentication & RBAC
Sub-modules: Password Security, Token Refresh & Expiry (token creation
side; verification lives in rbac.py).
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone

import jwt
from passlib.context import CryptContext

from app.config import settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

PASSWORD_RULES_DESCRIPTION = (
    f"Password must be at least {settings.PASSWORD_MIN_LENGTH} characters long and "
    "include at least one uppercase letter, one lowercase letter, one digit, "
    "and one special character."
)


class WeakPasswordError(ValueError):
    pass


def validate_password_strength(password: str) -> None:
    """Sub-module: Password Security — enforces a minimum-strength rule
    at registration (and at password change/reset)."""
    if len(password) < settings.PASSWORD_MIN_LENGTH:
        raise WeakPasswordError(
            f"Password must be at least {settings.PASSWORD_MIN_LENGTH} characters long."
        )
    if not re.search(r"[A-Z]", password):
        raise WeakPasswordError("Password must include at least one uppercase letter.")
    if not re.search(r"[a-z]", password):
        raise WeakPasswordError("Password must include at least one lowercase letter.")
    if not re.search(r"\d", password):
        raise WeakPasswordError("Password must include at least one digit.")
    if not re.search(r"[^A-Za-z0-9]", password):
        raise WeakPasswordError("Password must include at least one special character.")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def _create_token(subject: str, role: str, expires_delta: timedelta, token_type: str) -> tuple[str, str]:
    """Returns (encoded_token, jti) — the jti is what gets recorded on
    logout/revocation."""
    jti = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    payload = {
        "sub": subject,
        "role": role,
        "type": token_type,
        "jti": jti,
        "iat": now,
        "exp": now + expires_delta,
    }
    token = jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)
    return token, jti


def create_access_token(user_id: str, role: str) -> tuple[str, str]:
    return _create_token(
        user_id, role, timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES), "access"
    )


def create_refresh_token(user_id: str, role: str) -> tuple[str, str]:
    return _create_token(
        user_id, role, timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS), "refresh"
    )


def decode_token(token: str) -> dict:
    """Raises jwt.PyJWTError (or a subclass) if invalid/expired —
    callers are expected to catch and translate to HTTP 401."""
    return jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])


def generate_opaque_token() -> str:
    """For email verification / password reset links — not a JWT, just
    a high-entropy random string, since these are single-use and looked
    up directly in the DB rather than self-verifying."""
    return uuid.uuid4().hex + uuid.uuid4().hex
