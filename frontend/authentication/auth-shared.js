/**
 * Module 8 — Authentication & RBAC
 * Shared frontend helper: token storage, API calls, route guards.
 * Included by every auth page AND by index.html (the main dashboard)
 * so the dashboard itself becomes "Role-Aware" / protected.
 */
(function() {
  const savedTheme = localStorage.getItem('pms_theme') || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  document.documentElement.setAttribute('data-theme', savedTheme);
  
  // Favicon automatically switches based on browser's prefers-color-scheme
  // No manual switching needed - handled by media attribute in HTML
})();

const API_BASE = 'http://localhost:8000';

// These two pages live in DIFFERENT folders (frontend/authentication/
// vs frontend/dashboard/), so the relative path between them depends on
// which folder is including this script. Defaults below assume this
// script is loaded from frontend/authentication/ (the normal case for
// login, register, etc). When frontend/dashboard/index.html includes
// this same script, it sets window.PMS_LOGIN_PATH BEFORE the
// <script src="..."> tag to override the default — see
// INDEX_HTML_INTEGRATION.md.
const LOGIN_PATH = window.PMS_LOGIN_PATH || 'login.html';
const DASHBOARD_PATH = window.PMS_DASHBOARD_PATH || '../dashboard/index.html';
const CHANGE_PASSWORD_PATH = window.PMS_CHANGE_PASSWORD_PATH || 'change-password.html';
const PROFILE_PATH = window.PMS_PROFILE_PATH || 'profile.html';

const Auth = {
  saveTokens(accessToken, refreshToken) {
    localStorage.setItem('pms_access_token', accessToken);
    localStorage.setItem('pms_refresh_token', refreshToken);
  },

  getAccessToken() {
    return localStorage.getItem('pms_access_token');
  },

  getRefreshToken() {
    return localStorage.getItem('pms_refresh_token');
  },

  clearTokens() {
    localStorage.removeItem('pms_access_token');
    localStorage.removeItem('pms_refresh_token');
  },

  isLoggedIn() {
    return !!this.getAccessToken();
  },

  /** Redirect to login if there's no token at all. Pages that need to
   * be protected (the main dashboard, change-password) call this
   * immediately on load. Does NOT validate the token server-side —
   * that happens naturally on the first authenticated fetch, which
   * will redirect via handleUnauthorized() below if the token's
   * actually expired/revoked. */
  requireAuth() {
    if (!this.isLoggedIn()) {
      window.location.href = LOGIN_PATH;
    }
  },

  /** Wraps fetch with the Authorization header, and auto-retries once
   * via refresh token on a 401 before giving up and bouncing to login.
   * This is what makes "Token Refresh & Expiry" actually transparent
   * to the rest of the dashboard instead of every page reinventing it. */
  async authedFetch(path, options = {}) {
    const doFetch = (token) => fetch(`${API_BASE}${path}`, {
      ...options,
      headers: {
        ...(options.headers || {}),
        Authorization: `Bearer ${token}`,
      },
    });

    let response = await doFetch(this.getAccessToken());

    if (response.status === 401 && this.getRefreshToken()) {
      const refreshed = await this.tryRefresh();
      if (refreshed) {
        response = await doFetch(this.getAccessToken());
      }
    }

    if (response.status === 401) {
      this.clearTokens();
      window.location.href = LOGIN_PATH;
      throw new Error('Session expired');
    }

    return response;
  },

  async tryRefresh() {
    try {
      const response = await fetch(`${API_BASE}/api/auth/refresh`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ refresh_token: this.getRefreshToken() }),
      });
      if (!response.ok) return false;
      const data = await response.json();
      this.saveTokens(data.access_token, data.refresh_token);
      return true;
    } catch {
      return false;
    }
  },

  async fetchMe() {
    const response = await this.authedFetch('/api/auth/me');
    if (!response.ok) return null;
    return response.json();
  },

  async logout() {
    try {
      await this.authedFetch('/api/auth/logout', { method: 'POST' });
    } catch {
      // Already-expired tokens, network errors etc. — clear locally regardless.
    }
    this.clearTokens();
    window.location.href = LOGIN_PATH;
  },
};

/** Renders a small account menu — avatar initials, name/role, a link to
 * the profile settings page, and a real button-styled sign-out — into
 * any element with id="pms-account-strip". Call this from index.html
 * once the page has loaded, to make the dashboard Role-Aware without
 * having to hand-edit its whole topbar markup. */
async function renderAccountStrip(elementId = 'pms-account-strip') {
  const el = document.getElementById(elementId);
  if (!el) return;
  const user = await Auth.fetchMe();
  if (!user) return;

  const initials = user.full_name
    .split(' ')
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0].toUpperCase())
    .join('');

  el.innerHTML = `
    <div class="account-menu">
      <div class="account-avatar" aria-hidden="true">${initials}</div>
      <span class="account-chip">
        <strong>${user.full_name}</strong>
        <small>${user.role.replace('_', ' ')}</small>
      </span>
      <a class="account-link" href="${PROFILE_PATH}">Profile</a>
      <a class="account-link" href="${CHANGE_PASSWORD_PATH}">Change password</a>
      <button class="account-signout-btn" id="pms-logout-btn" type="button">Sign out</button>
    </div>
  `;
  document.getElementById('pms-logout-btn').addEventListener('click', () => {
    Auth.logout();
  });
}

// 3. Setup form decorators
function setupFormPolish() {
  const fields = document.querySelectorAll('.field input');
  if (!fields.length) return;

  fields.forEach(input => {
    // Avoid double processing
    if (input.parentNode.classList.contains('input-container')) return;

    const container = document.createElement('div');
    container.className = 'input-container';
    input.parentNode.insertBefore(container, input);
    container.appendChild(input);

    // Password toggle (eye toggle icon)
    if (input.type === 'password' || input.id === 'password' || input.id === 'currentPassword' || input.id === 'newPassword') {
      container.classList.add('has-toggle');
      const toggleBtn = document.createElement('button');
      toggleBtn.type = 'button';
      toggleBtn.className = 'password-toggle-btn';
      toggleBtn.setAttribute('aria-label', 'Toggle password visibility');
      toggleBtn.innerHTML = `
        <svg viewBox="0 0 24 24">
          <path class="eye-open" d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
          <circle cx="12" cy="12" r="3" />
          <line class="eye-slash" x1="1" y1="1" x2="23" y2="23" style="display: none; stroke: currentColor; stroke-width: 2;" />
        </svg>
      `;
      container.appendChild(toggleBtn);

      toggleBtn.addEventListener('click', () => {
        const isPassword = input.type === 'password';
        input.type = isPassword ? 'text' : 'password';
        const slash = toggleBtn.querySelector('.eye-slash');
        slash.style.display = isPassword ? 'block' : 'none';
      });

      // Register page strength meter
      if (window.location.pathname.includes('register.html') && input.id === 'password') {
        const meterContainer = document.createElement('div');
        meterContainer.className = 'strength-meter-container';
        meterContainer.innerHTML = `
          <div class="strength-meter-bar">
            <div class="strength-meter-fill" id="strengthFill"></div>
          </div>
          <span class="strength-meter-text" id="strengthText">Password Strength: Empty</span>
        `;
        container.parentNode.appendChild(meterContainer);

        input.addEventListener('input', () => {
          const val = input.value;
          let score = 0;
          if (val.length >= 8) score++;
          if (/[A-Z]/.test(val)) score++;
          if (/[a-z]/.test(val)) score++;
          if (/\d/.test(val)) score++;
          if (/[^A-Za-z0-9]/.test(val)) score++;

          const fill = document.getElementById('strengthFill');
          const txt = document.getElementById('strengthText');
          if (!val) {
            fill.style.width = '0%';
            fill.style.backgroundColor = 'var(--error)';
            txt.textContent = 'Password Strength: Empty';
          } else if (score <= 2) {
            fill.style.width = '33%';
            fill.style.backgroundColor = 'var(--error)';
            txt.textContent = 'Password Strength: Weak';
          } else if (score <= 4) {
            fill.style.width = '66%';
            fill.style.backgroundColor = 'var(--status-warn)';
            txt.textContent = 'Password Strength: Medium';
          } else {
            fill.style.width = '100%';
            fill.style.backgroundColor = 'var(--success)';
            txt.textContent = 'Password Strength: Strong';
          }
        });
      }
    }
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', setupFormPolish);
} else {
  setupFormPolish();
}
