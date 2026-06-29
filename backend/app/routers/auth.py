"""
Module 8 — Authentication & RBAC
Sub-modules: User Registration & Login, Token Refresh & Expiry, Audit
Logging, registration-confirmation, Forgot Password, Google Sign-In,
and Account Linking.

Account-linking rules
─────────────────────
Scenario A — Google-only user tries password login
    The user signed up with Google and has no local password (hashed_password
    is None).  We detect this and return a friendly 401 that tells them to use
    Google or set a password in their profile settings, rather than a generic
    "wrong password" message.

Scenario B — Local user signs in with Google
    The user already has a local account and clicks "Sign in with Google" with
    the same email.  We automatically link the Google ID to their existing
    record (auth_provider → BOTH) instead of creating a duplicate row.

set-password endpoint
─────────────────────
POST /api/auth/set-password  (authenticated)
    Allows a Google-only user to add a local password so they can log in
    either way going forward.  auth_provider is promoted to BOTH.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

import jwt
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.email_utils import send_password_reset_email, send_verification_email
from app.models import (
    AuditLog, AuthProvider, EmailVerificationToken,
    PasswordResetToken, RevokedToken, User,
)
from app.rbac import get_current_user, oauth2_scheme
from app.schemas import (
    AuthConfigResponse, ChangePasswordRequest, ForgotPasswordRequest,
    GoogleLoginRequest, LoginRequest, MessageResponse,
    RefreshRequest, ResetPasswordRequest, SetPasswordRequest,
    TokenPair, UserOut, UserRegister,
)
from app.security import (
    WeakPasswordError, create_access_token, create_refresh_token, decode_token,
    generate_opaque_token, hash_password, validate_password_strength, verify_password,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _log_event(
    db: Session,
    event: str,
    user_id: str | None,
    request: Request,
    detail: str | None = None,
) -> None:
    db.add(AuditLog(
        user_id=user_id,
        event=event,
        detail=detail,
        ip_address=request.client.host if request.client else None,
    ))
    db.commit()


def _issue_tokens(user: User) -> TokenPair:
    access_token,  _ = create_access_token(user.id, user.role.value)
    refresh_token, _ = create_refresh_token(user.id, user.role.value)
    return TokenPair(access_token=access_token, refresh_token=refresh_token)


# ---------------------------------------------------------------------------
# Public configuration
# ---------------------------------------------------------------------------

@router.get("/config", response_model=AuthConfigResponse)
def get_auth_config():
    """Returns public auth configuration so the frontend doesn't need to
    hardcode values like the Google Client ID."""
    return AuthConfigResponse(google_client_id=settings.GOOGLE_CLIENT_ID)


# ---------------------------------------------------------------------------
# Google Sign-In  (handles registration, login, and account linking)
# ---------------------------------------------------------------------------

@router.post("/google", response_model=TokenPair)
def google_login(
    payload: GoogleLoginRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    """Verify a Google ID token, then handle one of three outcomes:

    1. No account for this email → auto-register as verified viewer
       (auth_provider = GOOGLE, hashed_password = None).

    2. Existing LOCAL account with the same email (Scenario B) →
       link the Google ID to that account (auth_provider → BOTH)
       and log them in without creating a duplicate.

    3. Existing GOOGLE or BOTH account → normal login.
    """
    if not settings.GOOGLE_CLIENT_ID:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Google Sign-In is not configured on this server.",
        )

    # ── Verify the ID token with Google ──────────────────────────────
    try:
        from google.oauth2 import id_token as google_id_token
        from google.auth.transport import requests as google_requests

        idinfo = google_id_token.verify_oauth2_token(
            payload.id_token,
            google_requests.Request(),
            settings.GOOGLE_CLIENT_ID,
        )
    except ValueError as exc:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid Google token: {exc}",
        )

    email: str     = idinfo.get("email", "").lower().strip()
    google_sub: str = idinfo.get("sub", "")          # stable Google user ID
    full_name: str  = idinfo.get("name") or email.split("@")[0]

    if not email or not google_sub:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Google token did not include a valid email or subject.",
        )

    user = db.query(User).filter(User.email == email).first()

    if user is None:
        # ── New user — auto-register ──────────────────────────────────
        user = User(
            email=email,
            full_name=full_name,
            hashed_password=None,           # no local password
            google_id=google_sub,
            auth_provider=AuthProvider.GOOGLE,
            is_verified=True,               # Google already verified the email
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        _log_event(db, "google_register", user.id, request, detail=email)

    elif user.auth_provider == AuthProvider.LOCAL:
        # ── Scenario B: existing local account — link Google ─────────
        user.google_id      = google_sub
        user.auth_provider  = AuthProvider.BOTH
        user.is_verified    = True          # retroactively verify via Google
        db.commit()
        _log_event(db, "google_account_linked", user.id, request, detail=email)

    else:
        # ── Existing Google / BOTH account — normal login ─────────────
        if not user.is_active:
            raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Account is disabled.")

        # Keep google_id current in case the user revoked and re-authorised
        if user.google_id != google_sub:
            user.google_id = google_sub
            db.commit()

        _log_event(db, "google_login", user.id, request, detail=email)

    if not user.is_active:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Account is disabled.")

    return _issue_tokens(user)


# ---------------------------------------------------------------------------
# Registration + email confirmation
# ---------------------------------------------------------------------------

@router.post("/register", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def register(
    payload: UserRegister,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    if db.query(User).filter(User.email == payload.email).first():
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail="An account with this email already exists.",
        )

    try:
        validate_password_strength(payload.password)
    except WeakPasswordError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))

    user = User(
        email=payload.email,
        full_name=payload.full_name,
        hashed_password=hash_password(payload.password),
        auth_provider=AuthProvider.LOCAL,
        role=payload.role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    token = generate_opaque_token()
    db.add(EmailVerificationToken(
        user_id=user.id,
        token=token,
        expires_at=datetime.utcnow() + timedelta(hours=settings.EMAIL_VERIFICATION_TTL_HOURS),
    ))
    db.commit()

    background_tasks.add_task(send_verification_email, user.email, user.full_name, token)
    _log_event(db, "register", user.id, request)
    return user


@router.get("/verify-email", response_model=MessageResponse)
def verify_email(token: str, db: Session = Depends(get_db)):
    record = (
        db.query(EmailVerificationToken)
        .filter(EmailVerificationToken.token == token)
        .first()
    )
    if not record or record.used:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Invalid or already-used verification link.",
        )
    if record.expires_at < datetime.utcnow():
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Verification link has expired.",
        )

    user = db.get(User, record.user_id)
    user.is_verified = True
    record.used = True
    db.commit()
    return {"message": "Email verified successfully. You can now log in."}


# ---------------------------------------------------------------------------
# Login / Refresh / Logout
# ---------------------------------------------------------------------------

@router.post("/login", response_model=TokenPair)
def login(payload: LoginRequest, request: Request, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == payload.email).first()

    # ── Scenario A: Google-only account tries password login ──────────
    # Check this before the generic "wrong password" path so we can give
    # a useful message instead of a confusing one.
    if user and user.auth_provider == AuthProvider.GOOGLE:
        _log_event(
            db, "login_failed", user.id, request,
            detail="password attempt on google-only account",
        )
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail=(
                "This email is linked to a Google account. "
                "Please sign in with Google, or set a local password "
                "in your profile settings first."
            ),
        )

    # ── Normal credential check ───────────────────────────────────────
    if not user or not user.hashed_password or not verify_password(payload.password, user.hashed_password):
        _log_event(db, "login_failed", user.id if user else None, request, detail="bad credentials")
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Incorrect email or password.")

    if not user.is_active:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Account is disabled.")

    _log_event(db, "login_success", user.id, request)
    return _issue_tokens(user)


@router.post("/refresh", response_model=TokenPair)
def refresh(payload: RefreshRequest, db: Session = Depends(get_db)):
    try:
        decoded = decode_token(payload.refresh_token)
    except jwt.PyJWTError:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token.",
        )

    if decoded.get("type") != "refresh":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Not a refresh token.")
    if decoded.get("jti") and db.get(RevokedToken, decoded["jti"]):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Refresh token has been revoked.")

    user = db.get(User, decoded.get("sub"))
    if not user or not user.is_active:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="User no longer active.")

    return _issue_tokens(user)


@router.post("/logout", response_model=MessageResponse)
def logout(
    request: Request,
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    decoded = decode_token(token)
    db.add(RevokedToken(
        jti=decoded["jti"],
        expires_at=datetime.utcfromtimestamp(decoded["exp"]),
    ))
    _log_event(db, "logout", user.id, request)
    db.commit()
    return {"message": "Logged out successfully."}


@router.get("/me", response_model=UserOut)
def me(user: User = Depends(get_current_user)):
    return user


# ---------------------------------------------------------------------------
# Password management (authenticated)
# ---------------------------------------------------------------------------

@router.post("/set-password", response_model=MessageResponse)
def set_password(
    payload: SetPasswordRequest,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Allow a Google-only user to create a local password.

    Once set, auth_provider is promoted to BOTH so the user can log in
    with either Google or their new password.

    Also available to LOCAL/BOTH users who want to change their password
    without needing to supply the old one (use change-password for that).
    Note: for security, change-password requires the current password.
    This endpoint is specifically for accounts that have no password yet.
    """
    if user.auth_provider == AuthProvider.LOCAL and user.hashed_password:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail=(
                "Your account already has a local password. "
                "Use the change-password endpoint to update it."
            ),
        )

    try:
        validate_password_strength(payload.new_password)
    except WeakPasswordError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))

    user.hashed_password = hash_password(payload.new_password)

    # Promote provider so both login paths now work
    if user.auth_provider == AuthProvider.GOOGLE:
        user.auth_provider = AuthProvider.BOTH

    db.commit()
    _log_event(db, "set_password", user.id, request)
    return {"message": "Password set successfully. You can now sign in with email and password too."}


@router.post("/change-password", response_model=MessageResponse)
def change_password(
    payload: ChangePasswordRequest,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Update an existing local password. Requires the current password."""
    if user.auth_provider == AuthProvider.GOOGLE or not user.hashed_password:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail=(
                "Your account does not have a local password yet. "
                "Use the set-password endpoint to create one first."
            ),
        )

    if not verify_password(payload.current_password, user.hashed_password):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Current password is incorrect.")

    try:
        validate_password_strength(payload.new_password)
    except WeakPasswordError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))

    user.hashed_password = hash_password(payload.new_password)
    db.commit()
    _log_event(db, "change_password", user.id, request)
    return {"message": "Password updated successfully."}


# ---------------------------------------------------------------------------
# Forgot + Reset password (unauthenticated)
# ---------------------------------------------------------------------------

@router.post("/forgot-password", response_model=MessageResponse)
def forgot_password(
    payload: ForgotPasswordRequest,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.email == payload.email).first()
    # Always return the same message — confirming/denying account existence
    # here is an account-enumeration vulnerability.
    generic_message = {"message": "If that email is registered, a reset link has been sent."}

    if not user:
        return generic_message

    # Google-only accounts have no password to reset — silently skip
    # (don't leak that the account exists or that it's Google-only)
    if user.auth_provider == AuthProvider.GOOGLE:
        return generic_message

    token = generate_opaque_token()
    db.add(PasswordResetToken(
        user_id=user.id,
        token=token,
        expires_at=datetime.utcnow() + timedelta(minutes=settings.PASSWORD_RESET_TTL_MINUTES),
    ))
    db.commit()
    background_tasks.add_task(send_password_reset_email, user.email, user.full_name, token)
    _log_event(db, "forgot_password_requested", user.id, request)
    return generic_message


@router.post("/reset-password", response_model=MessageResponse)
def reset_password(
    payload: ResetPasswordRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    record = (
        db.query(PasswordResetToken)
        .filter(PasswordResetToken.token == payload.token)
        .first()
    )
    if not record or record.used:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Invalid or already-used reset link.",
        )
    if record.expires_at < datetime.utcnow():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Reset link has expired.")

    try:
        validate_password_strength(payload.new_password)
    except WeakPasswordError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))

    user = db.get(User, record.user_id)
    user.hashed_password = hash_password(payload.new_password)

    # If this was a Google-only account that used forgot-password somehow,
    # promote to BOTH so the new password is usable.
    if user.auth_provider == AuthProvider.GOOGLE:
        user.auth_provider = AuthProvider.BOTH

    record.used = True
    db.commit()
    _log_event(db, "password_reset", user.id, request)
    return {"message": "Password has been reset. You can now log in."}
