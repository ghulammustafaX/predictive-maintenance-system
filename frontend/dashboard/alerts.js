/**
 * Module 9 — Alert Center frontend logic.
 * Fetches persisted alerts from /api/alerts and renders the table.
 * Role-aware action buttons: acknowledge (engineer/admin), resolve (admin).
 */

let currentUser = null;
let currentPage = 0;
const PAGE_SIZE = 25;

const SEVERITY_COLORS = {
    critical: { bg: 'rgba(200,0,0,0.12)', fg: '#c00', border: '#e44' },
    high:     { bg: 'rgba(230,120,0,0.12)', fg: '#c60', border: '#e80' },
    medium:   { bg: 'rgba(200,160,0,0.12)', fg: '#880', border: '#cc0' },
    low:      { bg: 'rgba(100,150,200,0.12)', fg: '#369', border: '#6af' },
    warning:  { bg: 'rgba(200,160,0,0.12)', fg: '#880', border: '#cc0' },
};

const STATUS_COLORS = {
    open:         { bg: 'rgba(200,0,0,0.08)',  fg: '#c00' },
    acknowledged: { bg: 'rgba(200,160,0,0.08)', fg: '#880' },
    resolved:     { bg: 'rgba(0,150,0,0.08)',   fg: '#060' },
};

function severityBadge(sev) {
    const c = SEVERITY_COLORS[sev] || SEVERITY_COLORS.low;
    return `<span style="padding:3px 10px;border-radius:12px;font-size:0.78rem;font-weight:600;background:${c.bg};color:${c.fg};border:1px solid ${c.border};">${sev.toUpperCase()}</span>`;
}

function statusBadge(status) {
    const c = STATUS_COLORS[status] || STATUS_COLORS.open;
    return `<span style="padding:3px 10px;border-radius:12px;font-size:0.78rem;font-weight:600;background:${c.bg};color:${c.fg};">${status}</span>`;
}

function formatTime(iso) {
    if (!iso) return '--';
    try {
        const d = new Date(iso);
        return d.toLocaleString('en-GB', { year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
    } catch { return iso; }
}

function canAcknowledge() {
    return currentUser && (currentUser.role === 'admin' || currentUser.role === 'maintenance_engineer');
}

function canResolve() {
    return currentUser && currentUser.role === 'admin';
}

function canViewAlerts() {
    return currentUser && ['admin', 'maintenance_engineer', 'floor_operator', 'viewer'].includes(currentUser.role);
}

function isWriteDenied() {
    return currentUser && ['floor_operator', 'viewer'].includes(currentUser.role);
}

async function loadCurrentUser() {
    try {
        currentUser = await Auth.fetchMe();
    } catch (e) {
        console.warn('Could not load user:', e);
    }
    if (currentUser) {
        const el = document.getElementById('pms-account-strip');
        if (el) {
            const initials = (currentUser.full_name || '?').split(' ').filter(Boolean).slice(0, 2).map(p => p[0].toUpperCase()).join('');
            el.innerHTML = `
                <div class="profile-trigger" style="display:flex;align-items:center;gap:8px;cursor:pointer;">
                    <div class="account-avatar" style="width:32px;height:32px;border-radius:50%;background:var(--accent,#2563eb);color:#fff;display:flex;align-items:center;justify-content:center;font-size:0.8rem;font-weight:600;">${initials}</div>
                    <div style="text-align:left;line-height:1.3;">
                        <strong style="display:block;font-size:0.85rem;">${currentUser.full_name}</strong>
                        <small style="display:block;font-size:0.72rem;opacity:0.7;">${currentUser.role.replace('_', ' ')}</small>
                    </div>
                </div>
            `;
        }
        if (currentUser.role === 'admin') {
            const navUsers = document.getElementById('navUsers');
            if (navUsers) navUsers.style.display = '';
        }
    }
}

async function loadAlerts() {
    const tbody = document.getElementById('alertsBody');
    const errBox = document.getElementById('alertsError');
    errBox.style.display = 'none';

    const status = document.getElementById('statusFilter').value;
    const severity = document.getElementById('severityFilter').value;
    let path = `/api/alerts?limit=${PAGE_SIZE}&offset=${currentPage * PAGE_SIZE}`;
    if (status) path += `&status=${status}`;
    if (severity) path += `&severity=${severity}`;

    try {
        const resp = await Auth.authedFetch(path);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();

        renderAlerts(data);
        updateSummary(data.alerts || []);
    } catch (e) {
        errBox.textContent = `Failed to load alerts: ${e.message}`;
        errBox.style.display = 'block';
        tbody.innerHTML = '<tr><td colspan="9" class="soft" style="text-align:center;padding:28px;">Error loading alerts.</td></tr>';
    }
}

async function loadSummary() {
    try {
        const [open, ack, resolved, critical] = await Promise.all([
            Auth.authedFetch('/api/alerts?status=open&limit=1'),
            Auth.authedFetch('/api/alerts?status=acknowledged&limit=1'),
            Auth.authedFetch('/api/alerts?status=resolved&limit=1'),
            Auth.authedFetch('/api/alerts?status=open&severity=critical&limit=1'),
        ]);
        const openData = await open.json();
        const ackData = await ack.json();
        const resData = await resolved.json();
        const critData = await critical.json();

        document.getElementById('openCount').textContent = openData.total;
        document.getElementById('ackCount').textContent = ackData.total;
        document.getElementById('resolvedCount').textContent = resData.total;
        document.getElementById('criticalCount').textContent = critData.total;
    } catch (e) {
        console.warn('Summary load failed:', e);
    }
}

function renderAlerts(data) {
    const tbody = document.getElementById('alertsBody');
    const alerts = data.alerts || [];

    if (!alerts.length) {
        tbody.innerHTML = '<tr><td colspan="9" class="soft" style="text-align:center;padding:28px;">No alerts found.</td></tr>';
    } else {
        tbody.innerHTML = alerts.map(a => {
            const engineId = `${a.dataset_id}-${String(a.unit_id).padStart(3, '0')}`;
            const btnStyle = 'display:block;width:100%;padding:4px 10px;border-radius:6px;cursor:pointer;font-size:0.78rem;font-weight:500;text-align:center;white-space:nowrap;';
            let actionBtns = '';
            if (a.status === 'open' && canAcknowledge()) {
                actionBtns += `<button class="alert-ack-btn" data-id="${a.id}" style="${btnStyle}border:1px solid var(--accent,#2563eb);background:transparent;color:var(--accent,#2563eb);">Acknowledge</button>`;
            }
            if (a.status !== 'resolved' && canResolve()) {
                actionBtns += `<button class="alert-res-btn" data-id="${a.id}" style="${btnStyle}border:1px solid #2a7a2a;background:transparent;color:#2a7a2a;${actionBtns ? 'margin-top:4px;' : ''}">Resolve</button>`;
            }
            const actions = actionBtns
                ? `<div style="display:flex;flex-direction:column;gap:4px;min-width:90px;">${actionBtns}</div>`
                : '<span class="soft" style="font-size:0.78rem;white-space:nowrap;">No actions available</span>';

            return `
                <tr>
                    <td>${severityBadge(a.severity)}</td>
                    <td style="font-size:0.82rem;">${a.alert_type === 'rul_low' ? 'Low RUL' : 'Anomaly'}</td>
                    <td style="font-weight:600;">${engineId}</td>
                    <td style="font-size:0.82rem;max-width:120px;overflow:hidden;text-overflow:ellipsis;" title="${a.run_id}">${a.run_id || '--'}</td>
                    <td>${a.cycle ?? '--'}</td>
                    <td style="font-size:0.82rem;max-width:320px;">${a.message}</td>
                    <td>${statusBadge(a.status)}</td>
                    <td style="font-size:0.8rem;">${formatTime(a.created_at)}</td>
                    <td style="vertical-align:middle;">${actions}</td>
                </tr>`;
        }).join('');

        // Wire up action buttons
        tbody.querySelectorAll('.alert-ack-btn').forEach(btn => {
            btn.addEventListener('click', () => acknowledgeAlert(btn.dataset.id));
        });
        tbody.querySelectorAll('.alert-res-btn').forEach(btn => {
            btn.addEventListener('click', () => resolveAlert(btn.dataset.id));
        });
    }

    // Pagination
    const total = data.total || 0;
    document.getElementById('alertsPageInfo').textContent =
        `${total} total — page ${currentPage + 1} of ${Math.max(1, Math.ceil(total / PAGE_SIZE))}`;
    document.getElementById('prevPageBtn').disabled = currentPage === 0;
    document.getElementById('prevPageBtn').style.opacity = currentPage === 0 ? '0.4' : '1';
    document.getElementById('nextPageBtn').disabled = (currentPage + 1) * PAGE_SIZE >= total;
    document.getElementById('nextPageBtn').style.opacity = (currentPage + 1) * PAGE_SIZE >= total ? '0.4' : '1';
}

function updateSummary() {
    // Summary is loaded separately via loadSummary()
}

async function acknowledgeAlert(alertId) {
    try {
        const resp = await Auth.authedFetch(`/api/alerts/${alertId}/acknowledge`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ note: 'Acknowledged via alert center' }),
        });
        if (resp.status === 403) {
            alert('You do not have permission to acknowledge alerts (Viewer role is read-only).');
            return;
        }
        if (resp.status === 409) {
            alert('Alert is no longer in "open" status.');
        }
        if (resp.ok) {
            loadAlerts();
            loadSummary();
        }
    } catch (e) {
        alert(`Acknowledge failed: ${e.message}`);
    }
}

async function resolveAlert(alertId) {
    if (!confirm('Mark this alert as resolved?')) return;
    try {
        const resp = await Auth.authedFetch(`/api/alerts/${alertId}/resolve`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ note: 'Resolved via alert center' }),
        });
        if (resp.status === 403) {
            alert('Only Admins can resolve alerts.');
            return;
        }
        if (resp.ok) {
            loadAlerts();
            loadSummary();
        }
    } catch (e) {
        alert(`Resolve failed: ${e.message}`);
    }
}

document.addEventListener('DOMContentLoaded', async () => {
    await loadCurrentUser();
    await loadSummary();
    await loadAlerts();

    document.getElementById('refreshAlertsBtn').addEventListener('click', () => {
        loadAlerts();
        loadSummary();
    });
    document.getElementById('statusFilter').addEventListener('change', () => { currentPage = 0; loadAlerts(); });
    document.getElementById('severityFilter').addEventListener('change', () => { currentPage = 0; loadAlerts(); });
    document.getElementById('prevPageBtn').addEventListener('click', () => { if (currentPage > 0) { currentPage--; loadAlerts(); } });
    document.getElementById('nextPageBtn').addEventListener('click', () => { currentPage++; loadAlerts(); });
});
