# Module 8 Frontend — Integration into `frontend/dashboard/index.html`

**Confirmed folder structure:**
```
frontend/
├── authentication/     ← all auth pages (login, register, etc.) — Module 8
└── dashboard/
    └── index.html       ← your existing live-telemetry dashboard — Module 11
```

These are **6 small, additive edits** to your existing, working
`frontend/dashboard/index.html` — nothing about your live-telemetry code
changes. Each one is copy-paste, in order.

## 1. Add the shared CSS (in `<head>`, right after your existing `<style>` block closes)
```html
<link rel="stylesheet" href="../authentication/auth-shared.css">
```

## 2. Add an account strip placeholder in the topbar
Find this in your `<header class="topbar">`:
```html
<button class="cta" id="refreshButton">Refresh live data</button>
```
Add a div right before it:
```html
<div id="pms-account-strip" style="display:flex; align-items:center;"></div>
<button class="cta" id="refreshButton">Refresh live data</button>
```

## 3. Tell auth-shared.js where things live FROM index.html's perspective
Since `index.html` is in a different folder than the auth pages, add this
**BEFORE** the `<script src="...auth-shared.js">` tag (next step) so the
shared script knows the correct relative paths from here:
```html
<script>
  window.PMS_LOGIN_PATH = '../authentication/login.html';
  window.PMS_CHANGE_PASSWORD_PATH = '../authentication/change-password.html';
  window.PMS_PROFILE_PATH = '../authentication/profile.html';
</script>
```

## 4. Load the shared auth script
```html
<script src="../authentication/auth-shared.js"></script>
```

## 5. Guard the page — add this as the FIRST line inside your existing `<script>` block
```javascript
Auth.requireAuth();    // redirects to login.html if nobody's signed in
renderAccountStrip();  // fills in the "Signed in as ... | Change password | Logout" strip
```

## 6. (Optional but recommended) Use `Auth.authedFetch` instead of plain `fetch` for the live-data call
Find this line in your existing script:
```javascript
const response = await fetch(`${API_BASE}/api/dashboard/live`, { cache: 'no-store' });
```
Change it to:
```javascript
const response = await Auth.authedFetch('/api/dashboard/live', { cache: 'no-store' });
```
Optional for the defense — the page works without this too, since
`/api/dashboard/live` isn't behind RBAC yet (see note below).

## 7. (Optional, but a nice demo moment) Actually protect `/api/dashboard/live` with RBAC
Right now it's open to anyone with a valid token. If you want to show
the panel an actual *403 Forbidden* for the wrong role, add this to
`backend/main.py`:
```python
from app.rbac import require_role
from app.models import User, UserRole

@app.get("/api/dashboard/live")
def live_dashboard(user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER, UserRole.VIEWER))):
    ...
```
(All three roles can view — this just proves the route is actually
gated, which is exactly the "Route-Level Authorization" sub-module the
panel will be looking for.)

---

## Running it locally to test
Since the two folders are siblings under `frontend/`, serve from the
REPO ROOT (one level above `frontend/`), not from inside either folder
— otherwise the `../authentication/...` relative paths won't resolve:
```bash
cd predictive-maintenance-system   # repo root
python -m http.server 5500
```
Then open:
- `http://localhost:5500/frontend/authentication/register.html`
- `http://localhost:5500/frontend/dashboard/index.html` (after logging in)

## Important: the `/api/dashboard/live` 500 error you'll see locally
That endpoint queries InfluxDB. If you're only running `uvicorn main:app`
standalone (no Docker), there's no InfluxDB container for it to reach,
so you'll see `500 Internal Server Error` on that ONE endpoint — this
is expected and unrelated to auth. Everything under `/api/auth/...`
works fine without Docker. The live-data endpoint will work correctly
once you run the full stack with `docker compose up --build`.

## Note: clean up duplicate dashboard copies
Your server logs showed BOTH `/dashboard/index.html` (200) and
`/frontend/dashboard/index.html` (200) responding — meaning there are
two copies of this file sitting in two different locations on disk.
Keep only `frontend/dashboard/index.html` as the canonical one and
delete (or never create) a top-level `dashboard/index.html`, so there's
no risk of editing the wrong copy later and wondering why your changes
"don't show up."
