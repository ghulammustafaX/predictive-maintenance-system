/**
 * Module 7 — Anomaly Detection frontend logic.
 * Lists persisted anomaly events and triggers on-demand detection.
 */

let currentUser = null;

const SEVERITY_COLORS = {
    critical: { bg: 'rgba(200,0,0,0.12)', fg: '#c00', border: '#e44' },
    high:     { bg: 'rgba(230,120,0,0.12)', fg: '#c60', border: '#e80' },
    medium:   { bg: 'rgba(200,160,0,0.12)', fg: '#880', border: '#cc0' },
    low:      { bg: 'rgba(100,150,200,0.12)', fg: '#369', border: '#6af' },
};

function severityBadge(sev) {
    const c = SEVERITY_COLORS[sev] || SEVERITY_COLORS.low;
    return `<span style="padding:3px 10px;border-radius:12px;font-size:0.78rem;font-weight:600;background:${c.bg};color:${c.fg};border:1px solid ${c.border};">${sev.toUpperCase()}</span>`;
}

function formatTime(iso) {
    if (!iso) return '--';
    try {
        const d = new Date(iso);
        return d.toLocaleString('en-GB', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch { return iso; }
}

function canTriggerDetection() {
    return currentUser && (currentUser.role === 'admin' || currentUser.role === 'maintenance_engineer');
}

function canCreateAlertFromAnomaly() {
    return currentUser && (currentUser.role === 'admin' || currentUser.role === 'maintenance_engineer');
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
        if (!canTriggerDetection()) {
            document.getElementById('runDetectionBtn').disabled = true;
            document.getElementById('runDetectionBtn').style.opacity = '0.4';
            document.getElementById('anomHint').textContent = 'Detection trigger requires Maintenance Engineer or Admin role. Viewers can browse anomaly history.';
        }
    }
}

async function loadAnomalies(datasetId, unitId) {
    const tbody = document.getElementById('anomalyBody');
    const errBox = document.getElementById('anomError');
    errBox.style.display = 'none';

    try {
        const resp = await Auth.authedFetch(`/api/anomalies?dataset_id=${datasetId}&unit_id=${unitId}&limit=200`);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        renderAnomalies(data.anomalies || []);
        updateAnomalySummary(data.anomalies || []);
    } catch (e) {
        errBox.textContent = `Failed to load anomalies: ${e.message}`;
        errBox.style.display = 'block';
        tbody.innerHTML = '<tr><td colspan="10" class="soft" style="text-align:center;padding:28px;">Error loading anomalies.</td></tr>';
    }
}

function updateAnomalySummary(anomalies) {
    document.getElementById('anomTotal').textContent = anomalies.length;
    document.getElementById('anomCritical').textContent = anomalies.filter(a => a.severity === 'critical').length;
    document.getElementById('anomHigh').textContent = anomalies.filter(a => a.severity === 'high').length;
    document.getElementById('anomZscore').textContent = anomalies.filter(a => a.detection_method === 'zscore').length;
}

function renderAnomalies(anomalies) {
    const tbody = document.getElementById('anomalyBody');
    if (!anomalies.length) {
        tbody.innerHTML = '<tr><td colspan="10" class="soft" style="text-align:center;padding:28px;">No anomaly events found. Run detection to evaluate telemetry.</td></tr>';
        return;
    }

    tbody.innerHTML = anomalies.map(a => {
        const engineId = `${a.dataset_id}-${String(a.unit_id).padStart(3, '0')}`;
        const canCreateAlert = canCreateAlertFromAnomaly();
        const alertBtn = canCreateAlert
            ? `<button class="anom-alert-btn" data-id="${a.event_id}" style="padding:3px 10px;border-radius:6px;border:1px solid var(--accent,#2563eb);background:transparent;color:var(--accent,#2563eb);cursor:pointer;font-size:0.78rem;">Create Alert</button>`
            : '<span class="soft" style="font-size:0.78rem;">--</span>';

        return `
            <tr>
                <td>${severityBadge(a.severity)}</td>
                <td style="font-weight:600;">${a.sensor}</td>
                <td style="font-size:0.82rem;">${a.detection_method}</td>
                <td>${engineId}</td>
                <td style="font-size:0.78rem;max-width:100px;overflow:hidden;text-overflow:ellipsis;" title="${a.run_id}">${a.run_id || '--'}</td>
                <td>${a.cycle}</td>
                <td>${a.observed_value.toFixed(4)}</td>
                <td>${a.score.toFixed(4)}</td>
                <td style="font-size:0.8rem;">${formatTime(a.timestamp)}</td>
                <td>${alertBtn}</td>
            </tr>`;
    }).join('');

    tbody.querySelectorAll('.anom-alert-btn').forEach(btn => {
        btn.addEventListener('click', () => createAlertFromAnomaly(btn.dataset.id));
    });
}

async function runDetection() {
    if (!canTriggerDetection()) {
        alert('You do not have permission to run detection. Requires Maintenance Engineer or Admin role.');
        return;
    }

    const datasetId = document.getElementById('anomDataset').value;
    const unitId = document.getElementById('anomUnit').value;
    const runId = document.getElementById('anomRun').value.trim();

    if (!runId) {
        alert('Please enter a Run ID.');
        return;
    }

    const btn = document.getElementById('runDetectionBtn');
    const originalText = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<span>Running...</span>';

    try {
        const resp = await Auth.authedFetch(
            `/api/anomalies/detect?dataset_id=${datasetId}&unit_id=${unitId}&run_id=${encodeURIComponent(runId)}&lookback_hours=48`,
            { method: 'POST' }
        );
        if (!resp.ok) {
            const errBody = await resp.text();
            throw new Error(`HTTP ${resp.status}: ${errBody}`);
        }
        const data = await resp.json();
        alert(`Detection complete.\nNew events: ${data.new_events}\nTotal detected: ${data.total_detected}\n${data.message}`);
        // Reload the anomaly list
        loadAnomalies(datasetId, unitId);
    } catch (e) {
        alert(`Detection failed: ${e.message}`);
    } finally {
        btn.disabled = false;
        btn.innerHTML = originalText;
    }
}

async function createAlertFromAnomaly(eventId) {
    try {
        const resp = await Auth.authedFetch(`/api/alerts/from-anomaly/${eventId}`, { method: 'POST' });
        if (resp.status === 403) {
            alert('You do not have permission to create alerts from anomalies.');
            return;
        }
        const data = await resp.json();
        if (data.triggered) {
            alert(`Alert created: ${data.alert.message}`);
        } else {
            alert(data.message);
        }
    } catch (e) {
        alert(`Failed to create alert: ${e.message}`);
    }
}

document.addEventListener('DOMContentLoaded', async () => {
    await loadCurrentUser();
    // Load default anomaly list
    const datasetId = document.getElementById('anomDataset').value;
    const unitId = document.getElementById('anomUnit').value;
    await loadAnomalies(datasetId, unitId);

    document.getElementById('runDetectionBtn').addEventListener('click', runDetection);
});
