/**
 * PMS Live Operations Dashboard Script
 * Fetches sensor telemetry from FastAPI and updates UI metrics.
 */

// Authenticate on load
Auth.requireAuth();

// Render account information in the sidebar footer
async function renderSidebarProfile() {
    const el = document.getElementById('pms-account-strip');
    if (!el) return;
    if (!Auth.isLoggedIn()) return;
    let user;
    try {
        user = await Auth.fetchMe();
    } catch (error) {
        console.warn('[Dashboard] Account profile unavailable:', error.message);
        return;
    }
    if (!user) return;

    const initials = user.full_name
        .split(' ')
        .filter(Boolean)
        .slice(0, 2)
        .map((part) => part[0].toUpperCase())
        .join('');

    const changePasswordPath = window.PMS_CHANGE_PASSWORD_PATH || '../authentication/change-password.html';

    el.innerHTML = `
        <div class="profile-trigger" id="profileTrigger">
            <div class="account-avatar" aria-hidden="true">${initials}</div>
            <div class="account-chip">
                <strong>${user.full_name}</strong>
                <small>${user.role.replace('_', ' ')}</small>
            </div>
            <!-- Chevron up to indicate popping up -->
            <svg class="chevron-icon" viewBox="0 0 24 24">
                <polyline points="18 15 12 9 6 15"></polyline>
            </svg>
        </div>
        
        <div class="profile-popover" id="profilePopover">
            <div class="popover-header">
                <div class="large-avatar" aria-hidden="true">${initials}</div>
                <div class="user-details">
                    <h4>${user.full_name}</h4>
                    <p class="email">${user.email}</p>
                    <span class="role-badge">${user.role.replace('_', ' ')}</span>
                </div>
            </div>
            <div class="popover-divider"></div>
            <div class="popover-menu">
                <a class="popover-item disabled" href="#" onclick="event.preventDefault();">
                    <svg class="item-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"></path>
                        <circle cx="12" cy="7" r="4"></circle>
                    </svg>
                    <span>Edit Profile (Disabled)</span>
                </a>
                <a class="popover-item" href="${changePasswordPath}">
                    <svg class="item-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <rect x="3" y="11" width="18" height="11" rx="2" ry="2"></rect>
                        <path d="M7 11V7a5 5 0 0 1 10 0v4"></path>
                    </svg>
                    <span>Change Password</span>
                </a>
                <button class="popover-item logout-btn" id="popoverLogoutBtn" type="button">
                    <svg class="item-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"></path>
                        <polyline points="16 17 21 12 16 7"></polyline>
                        <line x1="21" y1="12" x2="9" y2="12"></line>
                    </svg>
                    <span>Sign Out</span>
                </button>
            </div>
        </div>
    `;

    const trigger = document.getElementById('profileTrigger');
    const popover = document.getElementById('profilePopover');
    const logoutBtn = document.getElementById('popoverLogoutBtn');

    trigger.addEventListener('click', (e) => {
        e.stopPropagation();
        popover.classList.toggle('active');
    });

    popover.addEventListener('click', (e) => {
        e.stopPropagation();
    });

    document.addEventListener('click', () => {
        popover.classList.remove('active');
    });

    logoutBtn.addEventListener('click', () => {
        Auth.logout();
    });

    // Reveal the Users nav link for admins only
    if (user.role === 'admin') {
        const navUsers = document.getElementById('navUsers');
        if (navUsers) navUsers.style.display = '';
    }
}

renderSidebarProfile();

const POLL_MS = 5000;
const REQUEST_TIMEOUT_MS = 8000;
const LIVE_MAX_AGE_SECONDS = 60;
const STALE_MAX_AGE_SECONDS = 24 * 60 * 60;

const state = {
    limit: 8,
    values: [],
    lastRecordCount: null,
    refreshCooldownActive: false,
    lastSuccessTime: null,
    tableState: null,
    fleet: [],
    selectedEngine: null,
    loading: false,
    hasRenderedData: false,
    activeRequestController: null,
    pollTimer: null,
    refreshTimer: null,
    pageExiting: false,
    selectionVersion: 0,
    pendingReload: false,
    latestTelemetryRun: null,
    selectedRunId: null,
    systemHealth: 'unknown'
};

const elements = {
    apiStatus: document.getElementById('apiStatus'),
    lastUpdate: document.getElementById('lastUpdate'),
    streamCount: document.getElementById('streamCount'),
    unitCount: document.getElementById('unitCount'),
    latestCycle: document.getElementById('latestCycle'),
    freshnessMetric: document.getElementById('freshnessMetric'),
    snapshotLabel: document.getElementById('snapshotLabel'),
    freshnessText: document.getElementById('freshnessText'),
    unitText: document.getElementById('unitText'),
    freshnessBar: document.getElementById('freshnessBar'),
    unitBar: document.getElementById('unitBar'),
    windowLabel: document.getElementById('windowLabel'),
    dataTable: document.getElementById('dataTable'),
    sensorMatrix: document.getElementById('sensorMatrix'),
    errorBox: document.getElementById('errorBox'),
    streamBadge: document.getElementById('streamBadge'),
    tableStatus: document.getElementById('tableStatus'),
    statusNote: document.getElementById('statusNote'),
    ingestionRate: document.getElementById('ingestionRate'),
    liveNoteDot: document.getElementById('liveNoteDot'),
    chartNoDataMessage: document.getElementById('chartNoDataMessage'),
    pipelineTrack: document.getElementById('pipelineTrack'),
    ingestionChartCanvas: document.getElementById('ingestionChart'),
    engineSelect: document.getElementById('engineSelect'),
    runSelect: document.getElementById('runSelect'),
    fleetSelectionStatus: document.getElementById('fleetSelectionStatus'),
    rulCycles: document.getElementById('rulCycles'),
    rulStatus: document.getElementById('rulStatus'),
    rulDetail: document.getElementById('rulDetail'),
    rulTimestamp: document.getElementById('rulTimestamp'),
    rulModelPairing: document.getElementById('rulModelPairing')
};

// Log missing elements for debugging
console.log('[Dashboard] Initializing elements...');
const missingElements = [];
for (const [key, value] of Object.entries(elements)) {
    if (value === null) {
        missingElements.push(key);
    }
}
if (missingElements.length > 0) {
    console.warn('[Dashboard] Missing elements:', missingElements);
} else {
    console.log('[Dashboard] All elements found ✓');
}

const trackedSensors = ['sensor_1', 'sensor_2', 'sensor_3', 'sensor_9', 'sensor_14'];

const nf = new Intl.NumberFormat('en-US');
const df = new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
});

function valueOrDash(value, digits = 2) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
        return '--';
    }
    const number = Number(value);
    if (Math.abs(number) >= 1000 || Number.isInteger(number)) {
        return nf.format(number);
    }
    return number.toFixed(digits);
}

function safeInt(value) {
    const number = Number(value);
    return Number.isFinite(number) ? Math.round(number) : null;
}

function formatTimestamp(value) {
    if (!value) {
        return '--';
    }
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? '--' : df.format(date);
}

function sourceTimeMillis(value) {
    if (!value) return null;
    const milliseconds = new Date(value).getTime();
    return Number.isFinite(milliseconds) ? milliseconds : null;
}

function sourceAgeSeconds(value) {
    const milliseconds = sourceTimeMillis(value);
    if (milliseconds === null) return null;
    const offsetSeconds = (Date.now() - milliseconds) / 1000;
    // Return null for future timestamps to avoid showing misleading "1s ago"
    return offsetSeconds < 0 ? null : offsetSeconds;
}

function sourceOffsetSeconds(value) {
    const milliseconds = sourceTimeMillis(value);
    return milliseconds === null ? null : (Date.now() - milliseconds) / 1000;
}

function isFutureTimestamp(value) {
    const offsetSeconds = sourceOffsetSeconds(value);
    return offsetSeconds !== null && offsetSeconds < 0;
}

function formatAge(seconds) {
    if (!Number.isFinite(seconds)) {
        return '--';
    }
    if (seconds < 60) return `${Math.max(1, Math.round(seconds))}s ago`;
    if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
    return `${Math.round(seconds / 3600)}h ago`;
}

function sensorPercent(value, min = 0, max = 9000) {
    const number = Number(value);
    if (!Number.isFinite(number)) return 0;
    const pct = ((number - min) / (max - min)) * 100;
    return Math.max(8, Math.min(100, pct));
}

// Render dynamic elements
function renderTable(rows, isOnline = true, emptyMessage = '') {
    if (!rows.length) {
        if (emptyMessage) {
            state.tableState = 'empty';
            elements.dataTable.innerHTML = `<tr><td colspan="6" class="soft" style="text-align:center; padding: 28px;">${emptyMessage}</td></tr>`;
            return;
        }
        if (!isOnline) {
            if (state.tableState === 'offline') return;
            state.tableState = 'offline';
            elements.dataTable.innerHTML = `
                <tr class="no-data-illustration-row">
                    <td colspan="6" style="text-align: center; padding: 48px 24px; vertical-align: middle;">
                        <div class="no-data-wrapper" style="display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 14px;">
                            <!-- Premium 3D Isometric Empty Box SVG -->
                            <svg viewBox="0 0 64 64" width="56" height="56" stroke="currentColor" fill="none" stroke-width="1.8" style="color: var(--accent); opacity: 0.75; display: block; margin: 0 auto;">
                                <path d="M32 8 L54 18 L32 28 L10 18 Z" />
                                <path d="M10 18 L10 44 L32 54 L32 28" />
                                <path d="M54 18 L54 44 L32 54" />
                                <path d="M21 23 L43 33" stroke-linecap="round" opacity="0.5" />
                                <path d="M21 28 L43 38" stroke-linecap="round" opacity="0.5" />
                            </svg>
                            <div style="font-weight: 700; color: var(--text); font-size: 1.05rem; margin-top: 4px;">Waiting for live data...</div>
                            <p style="color: var(--muted); font-size: 0.85rem; max-width: 320px; margin: 0 auto; line-height: 1.5; font-family: var(--font-body);">
                                The backend connection is currently in an <strong>offline</strong> state. Start the Kafka telemetry simulator stream to view live sensor points.
                            </p>
                        </div>
                    </td>
                </tr>
            `;
            return;
        }

        if (state.tableState === 'skeletons') return;
        state.tableState = 'skeletons';
        elements.dataTable.innerHTML = Array(5).fill(0).map(() => `
            <tr class="skeleton-row">
                <td><div class="skeleton-text" style="width: 75%"></div></td>
                <td><div class="skeleton-text" style="width: 40%"></div></td>
                <td><div class="skeleton-text" style="width: 50%"></div></td>
                <td><div class="skeleton-text" style="width: 60%"></div></td>
                <td><div class="skeleton-text" style="width: 65%"></div></td>
                <td><div class="skeleton-text" style="width: 55%"></div></td>
            </tr>
        `).join('');
        return;
    }

    state.tableState = 'data';
    elements.dataTable.innerHTML = rows.map((row) => `
        <tr>
            <td>${formatTimestamp(row.timestamp)}</td>
            <td>${state.selectedEngine || '--'}</td>
            <td>${valueOrDash(row.cycle, 0)}</td>
            <td>${valueOrDash(row.sensor_1)}</td>
            <td>${valueOrDash(row.sensor_2)}</td>
            <td>${valueOrDash(row.sensor_14)}</td>
        </tr>
    `).join('');
}

function renderSensors(row) {
    if (!row) {
        return;
    }

    const sensorCards = [...trackedSensors, 'time_cycles'];
    const cards = elements.sensorMatrix.querySelectorAll('.sensor-box');

    sensorCards.forEach((key, index) => {
        const box = cards[index];
        if (!box) return;
        const reading = box.querySelector('.reading');
        const bar = box.querySelector('.bar span');
        const label = key === 'time_cycles' ? 'Cycle' : key.replace('sensor_', 'Sensor ');
        const value = key === 'time_cycles' ? row.cycle : row[key];
        reading.textContent = valueOrDash(value, 2);
        bar.style.width = `${key === 'time_cycles' ? sensorPercent(value, 0, 300) : sensorPercent(value)}%`;
        box.querySelector('.name').textContent = label;
    });
}

function renderEmptySensors() {
    const cards = elements.sensorMatrix.querySelectorAll('.sensor-box');
    cards.forEach((box) => {
        const reading = box.querySelector('.reading');
        const bar = box.querySelector('.bar span');
        reading.textContent = '--';
        bar.style.width = '0%';
    });
}

// ── Chart.js: Real-Time Ingestion Frequency ─────────────────────────────────
let ingestionChart = null;
const CHART_MAX_POINTS = 30;
const chartLabels = [];
const chartData = [];

function getChartColors(isDark) {
    return isDark
        ? {
            line:  '#e0e0e0',                       /* bright white-grey line */
            fill:  'rgba(224, 224, 224, 0.08)',      /* very subtle white fill */
            grid:  'rgba(255, 255, 255, 0.10)',      /* visible grid lines */
            tick:  '#b0b0b0',                        /* readable tick labels */
            point: '#ffffff'                         /* hover point */
          }
        : {
            line:  '#8B2020',
            fill:  'rgba(139, 32, 32, 0.07)',
            grid:  'rgba(0,0,0,0.05)',
            tick:  '#5E5E5E',
            point: '#8B2020'
          };
}

function initIngestionChart() {
    const canvas = document.getElementById('ingestionChart');
    if (!canvas || !window.Chart) return;
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    const colors = getChartColors(isDark);

    ingestionChart = new Chart(canvas, {
        type: 'line',
        data: {
            labels: chartLabels,
            datasets: [{
                label: 'Loaded telemetry points',
                data: chartData,
                borderColor: colors.line,
                backgroundColor: colors.fill,
                borderWidth: 2,
                pointRadius: 0,
                pointHoverRadius: 4,
                pointHoverBackgroundColor: colors.point,
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 400, easing: 'easeInOutQuart' },
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: isDark ? 'rgba(36,36,36,0.95)' : 'rgba(255,255,255,0.95)',
                    titleColor: isDark ? '#F4F4F4' : '#191919',
                    bodyColor: isDark ? '#B6B6B6' : '#5E5E5E',
                    borderColor: isDark ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.08)',
                    borderWidth: 1,
                    padding: 10,
                    cornerRadius: 8,
                    callbacks: {
                        title: (items) => items[0]?.label || '',
                        label: (item) => `  ${item.formattedValue} loaded points`
                    }
                }
            },
            scales: {
                x: {
                    grid: { color: colors.grid, drawBorder: false },
                    ticks: { color: colors.tick, font: { size: 10 }, maxTicksLimit: 6, maxRotation: 0 }
                },
                y: {
                    grid: { color: colors.grid, drawBorder: false },
                    ticks: { color: colors.tick, font: { size: 10 }, precision: 0 },
                    beginAtZero: false
                }
            }
        }
    });
}

function updateIngestionChart(loadedPointCount, isOnline) {
    if (!ingestionChart) initIngestionChart();
    if (!ingestionChart) return;

    const now = new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });

    if (!isOnline) {
        // Don't push new points when offline — leave chart frozen
        return;
    }

    chartLabels.push(now);
    chartData.push(loadedPointCount ?? 0);

    // Keep rolling 30-point window
    if (chartLabels.length > CHART_MAX_POINTS) {
        chartLabels.shift();
        chartData.shift();
    }

    // Re-apply theme colors in case user toggled theme
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    const colors = getChartColors(isDark);
    ingestionChart.data.datasets[0].borderColor = colors.line;
    ingestionChart.data.datasets[0].backgroundColor = colors.fill;
    ingestionChart.options.scales.x.grid.color = colors.grid;
    ingestionChart.options.scales.y.grid.color = colors.grid;
    ingestionChart.options.scales.x.ticks.color = colors.tick;
    ingestionChart.options.scales.y.ticks.color = colors.tick;

    ingestionChart.update('active');
}

// Re-apply chart colors immediately when theme toggles (no new data needed)
function applyChartTheme() {
    if (!ingestionChart) return;
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    const colors = getChartColors(isDark);
    ingestionChart.data.datasets[0].borderColor = colors.line;
    ingestionChart.data.datasets[0].backgroundColor = colors.fill;
    ingestionChart.data.datasets[0].pointHoverBackgroundColor = colors.point;
    ingestionChart.options.scales.x.grid.color = colors.grid;
    ingestionChart.options.scales.y.grid.color = colors.grid;
    ingestionChart.options.scales.x.ticks.color = colors.tick;
    ingestionChart.options.scales.y.ticks.color = colors.tick;
    ingestionChart.options.plugins.tooltip.backgroundColor = isDark ? 'rgba(36,36,36,0.95)' : 'rgba(255,255,255,0.95)';
    ingestionChart.options.plugins.tooltip.titleColor  = isDark ? '#f0f0f0' : '#191919';
    ingestionChart.options.plugins.tooltip.bodyColor   = isDark ? '#b0b0b0' : '#5E5E5E';
    ingestionChart.options.plugins.tooltip.borderColor = isDark ? 'rgba(255,255,255,0.10)' : 'rgba(0,0,0,0.08)';
    ingestionChart.update('none'); // 'none' = instant, no animation
}

function animateSparkline(isOnline) {
    // Legacy stub — real chart handled by updateIngestionChart
}

function setOnlineState(status, message) {
    const normalizedStatus = status === true ? 'live' : status === false ? 'offline' : status;
    const isLive = normalizedStatus === 'live';
    const isStale = normalizedStatus === 'stale';
    const isFuture = normalizedStatus === 'future';
    const isOffline = normalizedStatus === 'offline';
    const label = normalizedStatus.charAt(0).toUpperCase() + normalizedStatus.slice(1);

    // Dynamic styling for streamBadge status-chip
    elements.streamBadge.className = isLive ? 'status-chip' : 'status-chip warning';
    elements.streamBadge.textContent = isLive ? 'Live' : isStale ? 'Stale' : isFuture ? 'Future' : 'Offline';

    // Dynamic styling for tableStatus status-chip
    elements.tableStatus.className = isLive ? 'status-chip' : 'status-chip warning';
    elements.tableStatus.innerHTML = isLive
        ? '<span class="pulse" id="tableStatusPulse"></span> Live'
        : `<span class="pulse warning" id="tableStatusPulse"></span> ${label}`;

    // Keep the last known telemetry visible when the API is stale or unreachable.
    if (isLive || isStale || isFuture) {
        elements.statusNote.textContent = message;
    } else {
        if (state.lastSuccessTime) {
            elements.statusNote.innerHTML = `${message}<br><small style="color: var(--muted); font-size: 0.8rem; margin-top: 4px; display: block;">Last successful sync: ${df.format(state.lastSuccessTime)}</small>`;
        } else {
            elements.statusNote.textContent = message;
        }
    }

    if (elements.liveNoteDot) {
        elements.liveNoteDot.className = isLive ? 'note-indicator-dot status online' : 'note-indicator-dot status';
    }

    // Toggle active class on pipelineTrack (triggers shimmer animation)
    if (elements.pipelineTrack) {
        elements.pipelineTrack.classList.toggle('active', isLive);
    }

    // Toggle zero-data watermark message in chart
    if (elements.chartNoDataMessage) {
        elements.chartNoDataMessage.style.display = state.hasRenderedData ? 'none' : 'flex';
    }
    if (elements.ingestionChartCanvas) {
        elements.ingestionChartCanvas.style.opacity = state.hasRenderedData ? '1' : '0.35';
    }

    // Toggle warning class on all pulse indicator dots on the page when offline
    const pulses = document.querySelectorAll('.pulse');
    pulses.forEach(pulse => {
        if (isLive) {
            pulse.classList.remove('warning');
            pulse.classList.remove('offline');
        } else {
            pulse.classList.add('warning');
        }
    });

    animateSparkline(isLive);

    // Only show skeletons before the first successful data render. Do not erase
    // last-known values during a transient timeout or a stale-data response.
    if (isOffline && !state.hasRenderedData) {
        elements.streamCount.innerHTML = '<div class="skeleton-text" style="width: 50%; height: 1.15rem; margin: 4px 0;"></div>';
        elements.unitCount.innerHTML = '<div class="skeleton-text" style="width: 40%; height: 1.15rem; margin: 4px 0;"></div>';
        elements.latestCycle.innerHTML = '<div class="skeleton-text" style="width: 45%; height: 1.15rem; margin: 4px 0;"></div>';
        elements.freshnessMetric.innerHTML = '<div class="skeleton-text" style="width: 55%; height: 1.15rem; margin: 4px 0;"></div>';
        elements.lastUpdate.innerHTML = '<div class="skeleton-text" style="width: 75%; height: 1.25rem;"></div>';
        if (elements.ingestionRate) {
            elements.ingestionRate.textContent = '0.0 rec/s';
        }

        elements.freshnessText.innerHTML = '<div class="skeleton-text" style="width: 30px; height: 1rem;"></div>';
        elements.unitText.innerHTML = '<div class="skeleton-text" style="width: 30px; height: 1rem;"></div>';
        elements.freshnessBar.style.width = '0%';
        elements.unitBar.style.width = '0%';
        elements.snapshotLabel.textContent = 'Waiting for data';

        // Clear table and show skeleton or offline illustration
        renderTable([], false);

        // Clear sensor boxes and show skeletons
        const cards = elements.sensorMatrix.querySelectorAll('.sensor-box');
        cards.forEach(box => {
            const reading = box.querySelector('.reading');
            const bar = box.querySelector('.bar span');
            reading.innerHTML = '<div class="skeleton-text" style="width: 50%"></div>';
            bar.style.width = '0%';
        });
    }
}

function setSystemHealth(status, message) {
    state.systemHealth = status;
    const healthy = status === 'healthy';
    const unavailable = status === 'unavailable';
    elements.apiStatus.textContent = healthy
        ? 'System healthy'
        : unavailable
            ? 'System unavailable'
            : 'System degraded';
    elements.apiStatus.style.color = healthy ? 'var(--status-ok)' : 'var(--status-warn)';
    if (message) {
        elements.apiStatus.title = message;
    }
}

function setFleetSelectionStatus(message, kind = '') {
    elements.fleetSelectionStatus.textContent = message;
    elements.fleetSelectionStatus.className = `fleet-selection-status ${kind}`.trim();
}

function setRulStatus(message, kind = 'warning') {
    elements.rulStatus.textContent = message;
    elements.rulStatus.className = kind === 'live' ? 'status-chip' : 'status-chip warning';
}

function clearRul(detail = 'Loading the latest stored prediction…') {
    state.latestTelemetryRun = null;
    elements.rulCycles.innerHTML = '-- <span>cycles</span>';
    elements.rulDetail.textContent = detail;
    elements.rulTimestamp.textContent = '--';
    elements.rulModelPairing.textContent = 'Experimental pairing unavailable';
    setRulStatus(detail.startsWith('Loading') ? 'Loading' : 'Awaiting data');
}

function selectEngine(engineId) {
    if (engineId === state.selectedEngine) return;
    state.selectedEngine = engineId;
    state.selectedRunId = null;
    state.selectionVersion += 1;
    clearRul();
}

function renderRunOptions(points) {
    if (!elements.runSelect) return;
    const runs = [...new Set(points.map((point) => point.run_id).filter(Boolean))];
    console.log(`[RunSelector] Found ${runs.length} unique runs from ${points.length} points:`, runs);
    if (state.selectedRunId && !runs.includes(state.selectedRunId)) {
        state.selectedRunId = null;
    }
    if (!state.selectedRunId) {
        state.selectedRunId = runs[0] || null;
    }
    elements.runSelect.replaceChildren();
    if (!runs.length) {
        console.log('[RunSelector] No runs available, setting disabled state');
        elements.runSelect.add(new Option('No runs in range', ''));
        elements.runSelect.disabled = true;
        return;
    }
    console.log(`[RunSelector] Populating dropdown with ${runs.length} runs, selected: ${state.selectedRunId}`);
    runs.forEach((runId) => {
        const option = new Option(runId, runId);
        option.selected = runId === state.selectedRunId;
        elements.runSelect.add(option);
    });
    elements.runSelect.disabled = false;
    console.log('[RunSelector] Dropdown populated and enabled');
}

function setRunSelectorState(label, disabled = true) {
    if (!elements.runSelect) return;
    elements.runSelect.replaceChildren(new Option(label, ''));
    elements.runSelect.disabled = disabled;
}

function renderRul(payload) {
    const prediction = payload?.prediction || payload?.points?.slice(-1)[0];
    if (!prediction) {
        clearRul('No stored RUL prediction is available for this engine.');
        setRulStatus('No prediction');
        return;
    }

    const ageSeconds = sourceAgeSeconds(prediction.timestamp);
    const future = isFutureTimestamp(prediction.timestamp);
    const isStale = ageSeconds === null || ageSeconds > STALE_MAX_AGE_SECONDS;
    const runMismatch = Boolean(state.selectedRunId && prediction.run_id !== state.selectedRunId);
    elements.rulCycles.innerHTML = `${valueOrDash(prediction.rul_cycles)} <span>cycles</span>`;
    elements.rulTimestamp.textContent = formatTimestamp(prediction.timestamp);
    elements.rulModelPairing.textContent = `${prediction.model_version} / ${prediction.preprocessing_artifact_version} · experimental`;
    elements.rulDetail.textContent = future
        ? `Future timestamp detected (${formatTimestamp(prediction.timestamp)}). Verify source clock; run ${prediction.run_id}.`
        : runMismatch
        ? `Run mismatch: telemetry ${state.latestTelemetryRun}; RUL ${prediction.run_id}.`
        : isStale
            ? `Prediction is stale${ageSeconds === null ? '' : ` (${formatAge(ageSeconds)})`}. Latest run: ${prediction.run_id}.`
            : `Prediction for cycle ${valueOrDash(prediction.cycle, 0)} · ${formatAge(ageSeconds)} · run ${prediction.run_id}.`;
    setRulStatus(future ? 'Future timestamp' : runMismatch ? 'Run mismatch' : isStale ? 'Stale prediction' : 'Experimental', future || runMismatch || isStale ? 'warning' : 'live');
}

function showRulError(message) {
    elements.rulCycles.innerHTML = '-- <span>cycles</span>';
    elements.rulDetail.textContent = message;
    elements.rulTimestamp.textContent = '--';
    elements.rulModelPairing.textContent = 'Experimental pairing unavailable';
    setRulStatus('Prediction unavailable');
}

function statusLabel(status) {
    if (status === 'healthy' || status === 'live') return 'Live';
    if (status === 'warning' || status === 'stale') return 'Stale';
    if (status === 'future') return 'Future timestamp';
    return 'Offline';
}

function statusClass(status) {
    if (status === 'healthy' || status === 'live') return 'healthy';
    if (status === 'warning' || status === 'stale') return 'warning';
    if (status === 'future') return 'warning';
    return 'offline';
}

function renderFleet(fleet) {
    state.fleet = fleet;
    const selectedStillExists = fleet.some((engine) => engine.engine_id === state.selectedEngine);
    if (!selectedStillExists) selectEngine(fleet[0]?.engine_id || null);

    elements.engineSelect.replaceChildren();
    if (!fleet.length) {
        const option = new Option('No engines with recent telemetry', '');
        elements.engineSelect.add(option);
        elements.engineSelect.disabled = true;
        setFleetSelectionStatus('No fleet telemetry available', 'warning');
        clearRul('No engine with recent telemetry is available for prediction lookup.');
        return;
    }

    fleet.forEach((engine) => {
        const option = new Option(`${engine.engine_id} · ${engine.status}`, engine.engine_id);
        option.selected = engine.engine_id === state.selectedEngine;
        elements.engineSelect.add(option);
    });
    elements.engineSelect.disabled = false;
    const selected = fleet.find((engine) => engine.engine_id === state.selectedEngine);
    const selectedStatus = selected?.status || 'offline';
    setFleetSelectionStatus(statusLabel(selectedStatus), statusClass(selectedStatus));
}

function renderTelemetry(payload, selectedFleetEngine) {
    const points = (Array.isArray(payload.points) ? payload.points : []).slice().sort((left, right) => {
        const leftTime = sourceTimeMillis(left.timestamp);
        const rightTime = sourceTimeMillis(right.timestamp);
        if (leftTime === null && rightTime === null) return 0;
        if (leftTime === null) return 1;
        if (rightTime === null) return -1;
        return leftTime - rightTime;
    });
    state.values = points;
    const latest = points.slice().reverse().find((point) => sourceTimeMillis(point.timestamp) !== null) || null;
    state.latestTelemetryRun = latest?.run_id || null;
    renderRunOptions(points);
    // Fleet and telemetry timestamps are both source timestamps. Never replace a
    // missing source timestamp with the polling time.
    const ageSeconds = sourceAgeSeconds(latest?.timestamp ?? selectedFleetEngine?.last_seen_at);

    state.lastSuccessTime = new Date();
    state.hasRenderedData = state.hasRenderedData || points.length > 0;
    const future = isFutureTimestamp(latest?.timestamp);
    const engineStatus = future ? 'future' : selectedFleetEngine?.status || 'offline';
    const normalizedEngineStatus = engineStatus === 'healthy'
        ? 'live'
        : engineStatus === 'warning'
            ? 'stale'
            : ['live', 'stale', 'offline', 'future'].includes(engineStatus)
                ? engineStatus
                : 'offline';
    setOnlineState(
        normalizedEngineStatus,
        points.length
            ? future
                ? `Future timestamp for ${state.selectedEngine}; telemetry is not considered current.`
                : `Last point from ${state.selectedEngine} at cycle ${valueOrDash(latest.cycle, 0)}.`
            : `No telemetry points were returned for ${state.selectedEngine} in the bounded time range.`
    );
    elements.streamCount.textContent = nf.format(points.length);
    elements.unitCount.textContent = nf.format(state.fleet.length);
    elements.latestCycle.textContent = latest?.cycle == null ? '--' : nf.format(safeInt(latest.cycle) ?? 0);
    elements.freshnessMetric.textContent = ageSeconds == null ? '--' : formatAge(ageSeconds);
    elements.snapshotLabel.textContent = latest ? `${state.selectedEngine} • Cycle ${valueOrDash(latest.cycle, 0)}` : `${state.selectedEngine} • No points in range`;
    elements.lastUpdate.textContent = latest ? formatTimestamp(latest.timestamp) : '--';
    elements.freshnessText.textContent = ageSeconds == null ? '--' : formatAge(ageSeconds);
    elements.unitText.textContent = state.selectedEngine || '--';
    elements.windowLabel.textContent = 'Last hour · bounded telemetry query';
    elements.freshnessBar.style.width = `${ageSeconds == null ? 0 : Math.max(12, Math.min(100, 100 - Math.min(100, ageSeconds)))}%`;
    elements.unitBar.style.width = `${Math.min(100, state.fleet.length * 8)}%`;

    renderTable(points.slice().reverse(), true, `No telemetry points were returned for ${state.selectedEngine} in the last hour.`);
    if (latest) renderSensors(latest); else renderEmptySensors();
    updateIngestionChart(points.length, true);
    elements.errorBox.style.display = 'none';
}

function showError(error) {
    elements.errorBox.textContent = error;
    elements.errorBox.style.display = 'block';
    if (state.systemHealth !== 'healthy') {
        setSystemHealth('unavailable', 'The dashboard API could not be reached.');
    } else {
        setSystemHealth('degraded', 'The dashboard API returned an error while infrastructure remains reachable.');
    }
    // Don't reset run selector state if telemetry already populated it
    if (!state.values || state.values.length === 0) {
        setRunSelectorState('Runs unavailable', true);
    }
}

async function loadDashboard() {
    if (state.pageExiting) return;
    if (state.loading) {
        state.pendingReload = true;
        return;
    }
    state.loading = true;
    const controller = new AbortController();
    state.activeRequestController = controller;
    // Fleet + telemetry get their own generous timeout (30 s).
    // Health runs fire-and-forget on a separate signal so a slow health
    // response never cancels the fleet request.
    const FLEET_TIMEOUT_MS = 30000;
    const timeout = setTimeout(() => controller.abort('request-timeout'), FLEET_TIMEOUT_MS);
    const requestOptions = { cache: 'no-store', signal: controller.signal };
    try {
        // Only show "Loading runs..." on first load, not on every poll cycle
        console.log(`[Dashboard] Poll cycle ${state.hasRenderedData ? '(subsequent)' : '(initial)'}, engine: ${state.selectedEngine}`);
        if (!state.hasRenderedData) {
            setRunSelectorState('Loading runs...', true);
        }
        // Health check is fire-and-forget on its own signal — it must NOT
        // share the fleet controller, otherwise a slow health response burns
        // down the timer and cancels the fleet request mid-flight.
        const healthController = new AbortController();
        const healthTimeout = setTimeout(() => healthController.abort(), 10000);
        fetch(`${API_BASE}/api/health`, { cache: 'no-store', signal: healthController.signal })
            .then((r) => {
                clearTimeout(healthTimeout);
                if (r.ok) setSystemHealth('healthy', 'Backend and InfluxDB health check passed.');
                else setSystemHealth('degraded', `Backend health check returned ${r.status}.`);
            })
            .catch(() => { clearTimeout(healthTimeout); /* silent — fleet response will surface any real outage */ });
        const fleetStartedAt = performance.now();
        const fleetResponse = await Auth.authedFetch('/api/fleet/status', requestOptions);
        console.debug(`[Dashboard] fleet/status ${fleetResponse.status} in ${Math.round(performance.now() - fleetStartedAt)}ms`);
        if (!fleetResponse.ok) throw new Error(`Fleet API returned ${fleetResponse.status}`);
        const fleetPayload = await fleetResponse.json();
        const fleet = Array.isArray(fleetPayload.fleet) ? fleetPayload.fleet : [];
        renderFleet(fleet);
        if (!state.selectedEngine) {
            renderTelemetry({ points: [] }, null);
            return;
        }

        const selectedEngine = state.selectedEngine;
        const selectionVersion = state.selectionVersion;
        const channels = ['time_cycles', ...trackedSensors].join(',');
        const from = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
        const to = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
        const telemetryUrl = `/api/engines/${encodeURIComponent(selectedEngine)}/telemetry?channels=${encodeURIComponent(channels)}&points=8&from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`;
        const rulUrl = `/api/engines/${encodeURIComponent(selectedEngine)}/rul/latest`;
        const telemetryStartedAt = performance.now();
        const telemetryResult = await Promise.allSettled([
            Auth.authedFetch(telemetryUrl, requestOptions),
        ]);
        if (selectionVersion !== state.selectionVersion || selectedEngine !== state.selectedEngine) {
            state.pendingReload = true;
            return;
        }
        if (telemetryResult[0].status === 'rejected') throw telemetryResult[0].reason;
        const telemetryResponse = telemetryResult[0].value;
        console.debug(`[Dashboard] engine telemetry ${telemetryResponse.status} in ${Math.round(performance.now() - telemetryStartedAt)}ms`);
        if (!telemetryResponse.ok) throw new Error(`Telemetry API returned ${telemetryResponse.status}`);
        const telemetryPayload = await telemetryResponse.json();
        renderTelemetry(telemetryPayload, fleet.find((engine) => engine.engine_id === selectedEngine));

        const runId = state.selectedRunId;
        const rulHistoryUrl = runId
            ? `/api/engines/${encodeURIComponent(selectedEngine)}/rul/history?points=200&run_id=${encodeURIComponent(runId)}&from=${encodeURIComponent(from)}&to=${encodeURIComponent(new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString())}`
            : rulUrl;
        const rulResult = await Promise.allSettled([
            Auth.authedFetch(rulHistoryUrl, requestOptions),
        ]);
        if (rulResult[0].status === 'rejected') {
            console.warn('[Dashboard] RUL request rejected:', rulResult[0].reason);
            showRulError('Could not load the latest RUL prediction.');
        } else if (!rulResult[0].value.ok) {
            console.warn('[Dashboard] RUL request failed:', rulResult[0].value.status);
            showRulError(`RUL prediction request failed (${rulResult[0].value.status}).`);
        } else {
            const rulPayload = await rulResult[0].value.json();
            if (selectionVersion === state.selectionVersion && selectedEngine === state.selectedEngine) renderRul(rulPayload);
        }
    } catch (error) {
        if (state.pageExiting) return;
        if (error?.name === 'AbortError') {
            showError('Dashboard request was canceled or timed out.');
            setFleetSelectionStatus('Dashboard request timed out', 'error');
            return;
        }
        console.error('[Dashboard] Load failed:', error);
        showError(`Live dashboard update failed: ${error.message}`);
        setFleetSelectionStatus('Fleet connection failed', 'error');
    } finally {
        clearTimeout(timeout);
        if (state.activeRequestController === controller) state.activeRequestController = null;
        state.loading = false;
        if (state.pendingReload && !state.pageExiting) {
            state.pendingReload = false;
            void loadDashboard();
        }
    }
}

elements.engineSelect.addEventListener('change', () => {
    selectEngine(elements.engineSelect.value || null);
    void loadDashboard();
});

elements.runSelect?.addEventListener('change', () => {
    state.selectedRunId = elements.runSelect.value || null;
    void loadDashboard();
});

const refreshButton = document.getElementById('refreshButton');
if (refreshButton) {
    refreshButton.addEventListener('click', async () => {
        if (state.refreshCooldownActive) return;

        state.refreshCooldownActive = true;
        refreshButton.disabled = true;
        const originalHTML = refreshButton.innerHTML;

        // Show Loading state with spinner
        refreshButton.innerHTML = `
            <svg class="refresh-icon spinning" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" style="animation: spin 1s infinite linear;">
                <path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"></path>
            </svg>
            <span>Loading...</span>
        `;

        // Perform dashboard load
        await loadDashboard();

        // Start 5 second cooldown
        let secondsLeft = 5;
        refreshButton.innerHTML = `
            <svg class="refresh-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
                <path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"></path>
            </svg>
            <span>Ready in ${secondsLeft}s</span>
        `;

        if (state.refreshTimer) clearInterval(state.refreshTimer);
        state.refreshTimer = setInterval(() => {
            secondsLeft--;
            if (secondsLeft <= 0) {
                clearInterval(state.refreshTimer);
                state.refreshTimer = null;
                refreshButton.innerHTML = originalHTML;
                refreshButton.disabled = false;
                state.refreshCooldownActive = false;
            } else {
                refreshButton.innerHTML = `
                    <svg class="refresh-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
                        <path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"></path>
                    </svg>
                    <span>Ready in ${secondsLeft}s</span>
                `;
            }
        }, 1000);
    });
}

const primaryRefresh = document.getElementById('primaryRefresh');
if (primaryRefresh) {
    primaryRefresh.addEventListener('click', loadDashboard);
}

loadDashboard();
state.pollTimer = setInterval(() => {
    void loadDashboard();
}, POLL_MS);

window.addEventListener('pagehide', () => {
    state.pageExiting = true;
    if (state.pollTimer) clearInterval(state.pollTimer);
    if (state.refreshTimer) clearInterval(state.refreshTimer);
    if (state.activeRequestController) state.activeRequestController.abort('page-exit');
});

// Swap scan-beam gradient between light (red) and dark (white)
function applyScanBeamTheme(theme) {
    const beam = document.querySelector('.scan-beam');
    if (!beam) return;
    beam.setAttribute('fill', theme === 'dark' ? 'url(#scanGradDark)' : 'url(#scanGradLight)');
}
// Apply on page load based on saved theme
applyScanBeamTheme(document.documentElement.getAttribute('data-theme') || 'light');

// Theme-aware asset switching (sidebar logo only)
// Favicon automatically switches based on browser's prefers-color-scheme via media attribute
function updateThemeAssets(theme) {
    // Wordmark logo doesn't need theme switching
    // If using logo-primary-* variants, uncomment below:
    // const sidebarLogo = document.getElementById('sidebarLogo');
    // if (sidebarLogo) {
    //     sidebarLogo.src = theme === 'dark'
    //         ? '../assets/brand/logo-primary-dark.png'
    //         : '../assets/brand/logo-primary-light.png';
    // }
}

// Setup Theme Toggle Button
function setupThemeToggle() {
    const btn = document.getElementById('themeToggleBtn');
    if (!btn) return;
    const sunIcon = btn.querySelector('.theme-icon-sun');
    const moonIcon = btn.querySelector('.theme-icon-moon');
    
    function updateIcons(theme) {
        if (theme === 'dark') {
            sunIcon.style.display = 'block';
            moonIcon.style.display = 'none';
        } else {
            sunIcon.style.display = 'none';
            moonIcon.style.display = 'block';
        }
    }
    
    const currentTheme = document.documentElement.getAttribute('data-theme') || 'light';
    updateIcons(currentTheme);
    updateThemeAssets(currentTheme); // Apply initial theme assets
    
    btn.addEventListener('click', () => {
        const nextTheme = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
        document.documentElement.setAttribute('data-theme', nextTheme);
        localStorage.setItem('pms_theme', nextTheme);
        updateIcons(nextTheme);
        applyChartTheme(); // instantly re-color chart on theme switch
        applyScanBeamTheme(nextTheme);
        updateThemeAssets(nextTheme); // Update favicon and logo
    });
}
setupThemeToggle();

// Sidebar collapse functionality
const sidebar = document.querySelector('.sidebar');
const collapseBtn = document.getElementById('collapseBtn');

if (localStorage.getItem('pms_sidebar_collapsed') === 'true') {
    sidebar.classList.add('collapsed');
}

collapseBtn.addEventListener('click', () => {
    sidebar.classList.toggle('collapsed');
    localStorage.setItem('pms_sidebar_collapsed', sidebar.classList.contains('collapsed'));
});

// Auto-collapse sidebar on smaller screens (less than 1024px wide)
function handleSidebarResponsive() {
    if (window.innerWidth < 1024) {
        sidebar.classList.add('collapsed');
    }
}
window.addEventListener('resize', handleSidebarResponsive);
handleSidebarResponsive();
