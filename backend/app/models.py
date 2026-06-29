"""
Module 8 — Authentication & RBAC
Sub-modules: Role Definitions, Audit Logging (data layer for these +
User Registration & Login + Token Refresh & Expiry).
"""

from __future__ import annotations

import enum
import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


def _now() -> datetime:
    # Deliberately naive UTC (no tzinfo) — SQLite silently drops tzinfo
    # on read-back even if you write an aware datetime in, which causes
    # "can't compare offset-naive and offset-aware datetimes" errors the
    # moment you compare a freshly-created aware datetime against a
    # value loaded back from the DB. Postgres preserves tzinfo fine, but
    # being consistently naive-UTC everywhere works correctly on BOTH,
    # which matters since dev uses SQLite and prod uses Postgres.
    return datetime.utcnow()


def _uuid() -> str:
    return str(uuid.uuid4())


class UserRole(str, enum.Enum):
    """Three roles, matching the scope doc exactly."""
    ADMIN = "admin"
    MAINTENANCE_ENGINEER = "maintenance_engineer"
    VIEWER = "viewer"


class AuthProvider(str, enum.Enum):
    """Tracks how the account was created and what login methods are available.

    - LOCAL : registered via email/password only; no Google link yet.
    - GOOGLE: registered via Google only; no local password set yet.
    - BOTH  : started as one provider then linked the other — can log in either way.
    """
    LOCAL  = "local"
    GOOGLE = "google"
    BOTH   = "both"


class User(Base):
    __tablename__ = "users"

    id:              Mapped[str]            = mapped_column(String, primary_key=True, default=_uuid)
    email:           Mapped[str]            = mapped_column(String, unique=True, index=True, nullable=False)
    full_name:       Mapped[str]            = mapped_column(String, nullable=False)

    # Nullable — Google-only users have no local password.
    # We store None here instead of a random hash so auth logic can
    # reliably distinguish "has no password" from "has a password".
    hashed_password: Mapped[str | None]     = mapped_column(String, nullable=True)

    # Nullable — local-only users have no Google ID.
    google_id:       Mapped[str | None]     = mapped_column(String, unique=True, index=True, nullable=True)

    # Tracks which login methods are available on this account.
    auth_provider:   Mapped[AuthProvider]   = mapped_column(
        Enum(AuthProvider), default=AuthProvider.LOCAL, nullable=False
    )

    role:            Mapped[UserRole]       = mapped_column(Enum(UserRole), default=UserRole.VIEWER, nullable=False)
    is_active:       Mapped[bool]           = mapped_column(Boolean, default=True)
    is_verified:     Mapped[bool]           = mapped_column(Boolean, default=False)
    created_at:      Mapped[datetime]       = mapped_column(DateTime(), default=_now)


class EmailVerificationToken(Base):
    __tablename__ = "email_verification_tokens"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    token: Mapped[str] = mapped_column(String, unique=True, index=True, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(), nullable=False)
    used: Mapped[bool] = mapped_column(Boolean, default=False)

    user: Mapped["User"] = relationship()


class PasswordResetToken(Base):
    __tablename__ = "password_reset_tokens"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    token: Mapped[str] = mapped_column(String, unique=True, index=True, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(), nullable=False)
    used: Mapped[bool] = mapped_column(Boolean, default=False)

    user: Mapped["User"] = relationship()


class RevokedToken(Base):
    """Supports real logout for stateless JWTs: a revoked access/refresh
    token's unique jti is recorded here and rejected on every subsequent
    request until it would have expired anyway."""
    __tablename__ = "revoked_tokens"

    jti: Mapped[str] = mapped_column(String, primary_key=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(), nullable=False)


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    user_id: Mapped[str | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    event: Mapped[str] = mapped_column(String, nullable=False)
    detail: Mapped[str | None] = mapped_column(String, nullable=True)
    ip_address: Mapped[str | None] = mapped_column(String, nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(), default=_now)
