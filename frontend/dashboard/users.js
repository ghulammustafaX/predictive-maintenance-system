/**
 * Module 8 — User Management
 * Admin-only page: list users, change roles, view role-change audit log.
 *
 * Security model:
 *   - Backend enforces require_role(ADMIN) on both GET /api/auth/users
 *     and PATCH /api/auth/users/{id}/role.
 *   - This page shows an access-denied banner for any non-admin role;
 *     it does NOT expose user data before the API confirms admin access.
 *   - Role changes go server-side; a stale JWT cannot bypass DB checks.
 */

const ROLES = ['viewer', 'floor_operator', 'maintenance_engineer', 'admin'];
const ROLE_LABELS = {
    admin: 'Admin',
    maintenance_engineer: 'Maintenance Engineer',
    floor_operator: 'Floor Operator',
    viewer: 'Viewer',
};
const ROLE_COLORS = {
    admin:                { bg: 'rgba(200,0,0,0.10)',   fg: '#a00' },
    maintenance_engineer: { bg: 'rgba(0,120,180,0.10)', fg: '#066' },
    floor_operator:       { bg: 'rgba(140,80,180,0.10)',fg: '#636' },
    viewer:               { bg: 'rgba(100,100,100,0.10)',fg: '#555' },
};

let currentUser = null;

// ---------- helpers ----------------------------------------------------------

function roleBadge(role) {
    const c = ROLE_COLORS[role] || ROLE_COLORS.viewer;
    return `<span style="padding:2px 10px;border-radius:12px;font-size:0.76rem;font-weight:600;` +
           `background:${c.bg};color:${c.fg};">${ROLE_LABELS[role] || role}</span>`;
}

function statusDot(active, verified) {
    if (!active)   return '<span style="color:#c00;font-size:0.8rem;">● Disabled</span>';
    if (!verified) return '<span style="color:#880;font-size:0.8rem;">● Unverified</span>';
    return '<span style="color:#060;font-size:0.8rem;">● Active</span>';
}

function formatTs(iso) {
    if (!iso) return '--';
    try { return new Date(iso).toLocaleString('en-GB', { year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit' }); }
    catch { return iso; }
}

function toast(msg, ok = true) {
    const el = document.getElementById('toastMsg');
    el.textContent = msg;
    el.style.display = 'block';
    el.style.background = ok ? 'rgba(0,140,0,0.10)' : 'rgba(200,0,0,0.10)';
    el.style.color = ok ? '#060' : '#c00';
    el.style.border = `1px solid ${ok ? '#080' : '#c44'}`;
    clearTimeout(el._t);
    el._t = setTimeout(() => { el.style.display = 'none'; }, 4000);
}

// ---------- sidebar nav (injected so Admin link only shows for admins) --------

function buildNav(role) {
    const isAdmin = role === 'admin';
    const adminLink = isAdmin
        ? `<a href="users.html" class="nav-item active" aria-current="page">
               <svg class="nav-icon" viewBox="0 0 24 24" aria-hidden="true">
                   <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/>
                   <circle cx="9" cy="7" r="4"/>
                   <path d="M23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/>
               </svg>
               <span>Users</span>
           </a>`
        : '';

    document.getElementById('sidebarNav').innerHTML = `
        <a href="index.html" class="nav-item">
            <svg class="nav-icon" viewBox="0 0 24 24" aria-hidden="true">
                <rect x="3" y="3" width="7" height="9" rx="1"/>
                <rect x="14" y="3" width="7" height="5" rx="1"/>
                <rect x="14" y="12" width="7" height="9" rx="1"/>
                <rect x="3" y="16" width="7" height="5" rx="1"/>
            </svg>
            <span>Dashboard</span>
        </a>
        <a href="alerts.html" class="nav-item">
            <svg class="nav-icon" viewBox="0 0 24 24" aria-hidden="true">
                <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/>
                <path d="M13.73 21a2 2 0 0 1-3.46 0"/>
            </svg>
            <span>Alerts</span>
        </a>
        <a href="anomalies.html" class="nav-item">
            <svg class="nav-icon" viewBox="0 0 24 24" aria-hidden="true">
                <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>
            </svg>
            <span>Anomalies</span>
        </a>
        <a href="#" class="nav-item disabled" aria-disabled="true">
            <svg class="nav-icon" viewBox="0 0 24 24" aria-hidden="true">
                <line x1="18" y1="20" x2="18" y2="10"/>
                <line x1="12" y1="20" x2="12" y2="4"/>
                <line x1="6"  y1="20" x2="6"  y2="14"/>
            </svg>
            <span>Analytics</span>
        </a>
        ${adminLink}
    `;
}

// ---------- sidebar account strip --------------------------------------------

function buildAccountStrip(user) {
    const initials = (user.full_name || '?').split(' ').filter(Boolean)
        .slice(0, 2).map(p => p[0].toUpperCase()).join('');
    const el = document.getElementById('pms-account-strip');
    if (!el) return;
    el.innerHTML = `
        <div style="display:flex;align-items:center;gap:8px;padding:4px 0;">
            <div style="width:32px;height:32px;border-radius:50%;background:var(--accent,#2563eb);
                        color:#fff;display:flex;align-items:center;justify-content:center;
                        font-size:0.8rem;font-weight:600;flex-shrink:0;">${initials}</div>
            <div style="text-align:left;line-height:1.3;min-width:0;">
                <strong style="display:block;font-size:0.82rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${user.full_name}</strong>
                <small style="display:block;font-size:0.72rem;opacity:0.7;">${(user.role || '').replace(/_/g,' ')}</small>
            </div>
        </div>`;
}

// ---------- load users -------------------------------------------------------

async function loadUsers() {
    const tbody = document.getElementById('usersBody');
    try {
        const resp = await Auth.authedFetch('/api/auth/users');
        if (resp.status === 403) {
            document.getElementById('accessDenied').style.display = '';
            document.getElementById('usersSection').style.display = 'none';
            tbody.innerHTML = '';
            return;
        }
        if (!resp.ok) throw new Error('HTTP ' + resp.status);

        const users = await resp.json();
        document.getElementById('userCount').textContent = users.length + ' users';
        document.getElementById('usersSection').style.display = '';
        document.getElementById('accessDenied').style.display = 'none';

        tbody.innerHTML = users.map(u => {
            const isSelf = currentUser && u.id === currentUser.id;
            const roleSelect = isSelf
                ? `<em class="soft" style="font-size:0.8rem;">Cannot change own role</em>`
                : `<select data-uid="${u.id}" data-current="${u.role}" class="role-select"
                           style="padding:4px 8px;border-radius:6px;border:1px solid var(--border,#ddd);
                                  background:var(--surface,#fff);color:inherit;font-size:0.82rem;">
                       ${ROLES.map(r => `<option value="${r}"${r === u.role ? ' selected' : ''}>${ROLE_LABELS[r]}</option>`).join('')}
                   </select>`;
            const saveBtn = isSelf ? '' :
                `<button class="save-role-btn" data-uid="${u.id}"
                         style="padding:4px 12px;border-radius:6px;border:1px solid var(--accent,#2563eb);
                                background:transparent;color:var(--accent,#2563eb);cursor:pointer;font-size:0.8rem;">
                     Save
                 </button>`;

            return `<tr>
                <td style="font-weight:500;">${u.full_name}</td>
                <td style="font-size:0.82rem;">${u.email}</td>
                <td id="rolebadge-${u.id}">${roleBadge(u.role)}</td>
                <td>${statusDot(u.is_active, u.is_verified)}</td>
                <td style="font-size:0.8rem;">${(u.auth_provider || '').replace(/_/g,' ')}</td>
                <td style="font-size:0.8rem;">${formatTs(u.created_at)}</td>
                <td>${roleSelect}</td>
                <td>${saveBtn}</td>
            </tr>`;
        }).join('');

        // wire save buttons
        tbody.querySelectorAll('.save-role-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const uid = btn.dataset.uid;
                const sel = tbody.querySelector(`.role-select[data-uid="${uid}"]`);
                if (sel) saveRole(uid, sel.value, sel.dataset.current);
            });
        });

    } catch (e) {
        tbody.innerHTML = `<tr><td colspan="8" class="soft" style="text-align:center;padding:28px;">Error loading users: ${e.message}</td></tr>`;
    }
}

// ---------- save role --------------------------------------------------------

async function saveRole(userId, newRole, oldRole) {
    if (newRole === oldRole) { toast('Role unchanged — nothing to save.', true); return; }

    const confirmMsg = `Change role from "${ROLE_LABELS[oldRole] || oldRole}" to "${ROLE_LABELS[newRole] || newRole}"?`;
    if (!confirm(confirmMsg)) return;

    try {
        const resp = await Auth.authedFetch(`/api/auth/users/${userId}/role`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ role: newRole }),
        });

        if (resp.status === 400) {
            const err = await resp.json();
            toast(err.detail || 'Role change rejected.', false);
            return;
        }
        if (resp.status === 403) { toast('Administrator role required.', false); return; }
        if (!resp.ok) { toast('Save failed (HTTP ' + resp.status + ').', false); return; }

        const updated = await resp.json();
        // Update the badge cell immediately
        const badgeCell = document.getElementById('rolebadge-' + userId);
        if (badgeCell) badgeCell.innerHTML = roleBadge(updated.role);
        // Update the select's data-current attribute
        const sel = document.querySelector(`.role-select[data-uid="${userId}"]`);
        if (sel) sel.dataset.current = updated.role;

        toast(`Role updated to "${ROLE_LABELS[updated.role] || updated.role}". Audit log recorded.`, true);
        loadAuditLog();
    } catch (e) {
        toast('Network error: ' + e.message, false);
    }
}

// ---------- audit log --------------------------------------------------------

async function loadAuditLog() {
    const tbody = document.getElementById('auditBody');
    try {
        // The audit_logs table is queried via a simple admin endpoint
        // We use GET /api/auth/users (admin-only) as proof, then fetch audit separately.
        // For now render from the backend audit_logs via a dedicated route if exists,
        // otherwise show a note that it is stored in the database.
        const resp = await Auth.authedFetch('/api/auth/audit?event=role_changed&limit=20');
        if (resp.status === 404) {
            tbody.innerHTML = `<tr><td colspan="4" class="soft" style="text-align:center;padding:16px;">
                Audit records stored in audit_logs table (DB-level). Role changes are logged on every PATCH.</td></tr>`;
            return;
        }
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        const logs = await resp.json();
        if (!logs.length) {
            tbody.innerHTML = `<tr><td colspan="4" class="soft" style="text-align:center;padding:16px;">No role changes recorded yet.</td></tr>`;
            return;
        }
        tbody.innerHTML = logs.map(l => `<tr>
            <td style="font-size:0.8rem;">${formatTs(l.timestamp)}</td>
            <td style="font-size:0.82rem;">${l.actor_email || l.user_id || '--'}</td>
            <td style="font-size:0.82rem;">${l.event}</td>
            <td style="font-size:0.82rem;">${l.detail || '--'}</td>
        </tr>`).join('');
    } catch {
        tbody.innerHTML = `<tr><td colspan="4" class="soft" style="text-align:center;padding:16px;">
            Audit records stored in audit_logs table. Role changes are logged on every PATCH.</td></tr>`;
    }
}

// ---------- bootstrap --------------------------------------------------------

document.addEventListener('DOMContentLoaded', async () => {
    try {
        currentUser = await Auth.fetchMe();
    } catch {
        window.location.href = window.PMS_LOGIN_PATH || '../authentication/login.html';
        return;
    }

    buildNav(currentUser.role);
    buildAccountStrip(currentUser);

    if (currentUser.role !== 'admin') {
        document.getElementById('accessDenied').style.display = '';
        document.getElementById('usersSection').style.display = 'none';
        // still show the page — backend double-checks anyway
    } else {
        await loadUsers();
        await loadAuditLog();
    }

    document.getElementById('refreshUsersBtn').addEventListener('click', () => {
        loadUsers();
        loadAuditLog();
    });
    document.getElementById('refreshAuditBtn').addEventListener('click', loadAuditLog);
});
