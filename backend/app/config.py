"""
Module 8 — Authentication & RBAC
Centralized settings, loaded from environment variables (.env), with
sane local-dev defaults so the API still boots without every var set.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


def _env(key: str, default: str | None = None) -> str | None:
    return os.getenv(key, default)


class Settings:
    # --- Database ---
    DATABASE_URL: str = _env(
        "DATABASE_URL",
        "postgresql+psycopg2://pms_user:pms_password@postgres:5432/pms_auth",
    )

    # --- JWT ---
    JWT_SECRET: str = _env("JWT_SECRET", "CHANGE_ME_IN_PRODUCTION")
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(_env("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
    REFRESH_TOKEN_EXPIRE_DAYS: int = int(_env("REFRESH_TOKEN_EXPIRE_DAYS", "7"))

    # --- Password policy ---
    PASSWORD_MIN_LENGTH: int = int(_env("PASSWORD_MIN_LENGTH", "8"))

    # --- SMTP (registration confirmation + forgot-password emails) ---
    SMTP_HOST: str | None = _env("SMTP_HOST")
    SMTP_PORT: int = int(_env("SMTP_PORT", "587"))
    SMTP_USER: str | None = _env("SMTP_USER")
    SMTP_PASSWORD: str | None = _env("SMTP_PASSWORD")
    SMTP_FROM: str = _env("SMTP_FROM", "no-reply@predictive-maintenance.local")

    # --- Frontend (for building email links) ---
    FRONTEND_URL: str = _env("FRONTEND_URL", "http://localhost:3000")

    # --- Token TTLs for email-based flows ---
    EMAIL_VERIFICATION_TTL_HOURS: int = 24
    PASSWORD_RESET_TTL_MINUTES: int = 30

    # --- Google OAuth ---
    GOOGLE_CLIENT_ID: str | None = _env("GOOGLE_CLIENT_ID")


settings = Settings()

