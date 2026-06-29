# Module 8 — Authentication & RBAC: API Reference

Base path: `/api/auth` (mounted in `backend/main.py` alongside the existing
InfluxDB dashboard endpoints).

All endpoints are tested end-to-end in `backend/test_flow.py` and
`backend/test_verify_reset.py` (run with `PYTHONPATH=. python3 test_flow.py`
against a local SQLite DB — no Docker needed for a quick sanity check).

## Endpoints

| Method | Path | Auth required? | Purpose |
|---|---|---|---|
| POST | `/api/auth/register` | No | Create account. Enforces password policy. Sends a verification email (or logs the link to console if SMTP isn't configured). |
| GET | `/api/auth/verify-email?token=...` | No | Confirms the email link. One-time use, expires after 24h. |
| POST | `/api/auth/login` | No | Returns `access_token` + `refresh_token`. |
| POST | `/api/auth/refresh` | No (needs refresh token) | Exchanges a refresh token for a new access+refresh pair. |
| POST | `/api/auth/logout` | Yes | Revokes the current access token (real revocation, via a `jti` blacklist — not just a no-op). |
| GET | `/api/auth/me` | Yes | Returns the logged-in user's profile (id, email, role, is_verified, etc). Use this to drive "Role-Aware UI" in the dashboard. |
| POST | `/api/auth/change-password` | Yes | Requires current password; enforces the same password policy on the new one. |
| POST | `/api/auth/forgot-password` | No | Always returns the same generic message whether or not the email exists (prevents account enumeration). Sends a reset link. |
| POST | `/api/auth/reset-password` | No (needs reset token) | One-time use, expires after 30 minutes. |

## Password policy
Enforced at registration, change-password, AND reset-password (all three —
this was one of the panel's specific complaints, so it's deliberately not
just at signup):
- At least 8 characters (configurable via `PASSWORD_MIN_LENGTH`)
- At least one uppercase letter
- At least one lowercase letter
- At least one digit
- At least one special character

## Roles
Three roles, matching the scope doc exactly: `admin`, `maintenance_engineer`, `viewer`.
New registrations default to `viewer` — promote to a higher role manually
via direct DB access or build an admin-only "promote user" endpoint later
(not needed for the defense demo, but worth a sentence in your docs as a
known follow-up).

## Protecting OTHER modules' routes with RBAC
Any other router (dashboard, scheduling, alerting, etc.) can reuse this
same RBAC layer:

```python
from fastapi import APIRouter, Depends
from app.rbac import require_role
from app.models import User, UserRole

router = APIRouter()

@router.post("/work-orders")
def create_work_order(user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER))):
    ...
```

## Demo script for the panel (suggested order — NOT starting with code)
1. Open the (eventual) frontend register page → fill in a weak password → show the rejection message live.
2. Register with a strong password → show the success state.
3. Open `docker compose logs backend` (or the real inbox, if SMTP is configured) → show the verification link → click it → show "verified" confirmation.
4. Log in → land on dashboard → show `/api/auth/me` reflecting the logged-in user's role.
5. Change password from inside the app → log out → log back in with the NEW password (proves the change took effect, not just a fake success message).
6. Log out → click "Forgot password" → show the reset email/link arriving → reset → log in with the new password.
7. Only if a panelist asks "how does this work" — THEN show code.

## Known follow-ups (mention proactively in the defense, don't wait to be asked)
- No admin "promote user to admin" endpoint yet — first admin must be set directly in the DB. Fine for a defense demo; flag it as a fast follow-up.
- No Alembic migrations yet — using `Base.metadata.create_all()` at startup. Acceptable for an FYP timeline; mention Alembic as the production-grade next step.
- Rate limiting on login/forgot-password isn't implemented yet (brute-force/spam protection) — reasonable to flag as future work rather than build under this time pressure.
