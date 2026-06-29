"""
Module 8 — Authentication & RBAC
Sub-module: Route-Level Authorization

FastAPI dependencies for protecting routes: `get_current_user` decodes
and validates the bearer token (including checking it hasn't been
revoked via logout), and `require_role(...)` further restricts a route
to specific roles. Other modules' routers (dashboard, scheduling,
alerting, etc.) import `require_role` to guard their own endpoints —
this is the reusable RBAC layer the rest of the system sits behind.
"""

from __future__ import annotations

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import RevokedToken, User, UserRole
from app.security import decode_token

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=True)


def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = decode_token(token)
    except jwt.PyJWTError:
        raise credentials_exception

    if payload.get("type") != "access":
        raise credentials_exception

    jti = payload.get("jti")
    if jti and db.get(RevokedToken, jti):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has been revoked")

    user_id = payload.get("sub")
    user = db.get(User, user_id)
    if user is None or not user.is_active:
        raise credentials_exception
    return user


def require_role(*allowed_roles: UserRole):
    """Usage in another module's router:

        from app.rbac import require_role
        from app.models import UserRole

        @router.post("/work-orders")
        def create_work_order(user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER))):
            ...
    """
    def dependency(user: User = Depends(get_current_user)) -> User:
        if user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires one of roles: {[r.value for r in allowed_roles]}",
            )
        return user
    return dependency
