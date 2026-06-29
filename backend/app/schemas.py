"""
Module 8 — Authentication & RBAC
Pydantic request/response schemas.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, EmailStr, Field

from app.models import AuthProvider, UserRole


class UserRegister(BaseModel):
    email: EmailStr
    full_name: str = Field(min_length=1, max_length=120)
    password: str
    role: UserRole = UserRole.VIEWER  # admin accounts should be promoted manually, not self-assigned at signup


class UserOut(BaseModel):
    id: str
    email: EmailStr
    full_name: str
    role: UserRole
    auth_provider: AuthProvider
    is_active: bool
    is_verified: bool
    created_at: datetime

    class Config:
        from_attributes = True


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class RefreshRequest(BaseModel):
    refresh_token: str


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


class SetPasswordRequest(BaseModel):
    """Used by Google-only users who want to add a local password."""
    new_password: str


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str


class MessageResponse(BaseModel):
    message: str


class GoogleLoginRequest(BaseModel):
    id_token: str


class AuthConfigResponse(BaseModel):
    google_client_id: str | None

