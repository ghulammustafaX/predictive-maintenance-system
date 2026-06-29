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
    const user = await Auth.fetchMe();
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
}

renderSidebarProfile();

const POLL_MS = 2000; // Refreshed to 2 seconds for a faster "live" experience

const state = {
    limit: 8,
    values: [],
    lastRecordCount: null,
    refreshCooldownActive: false,
    lastSuccessTime: null,
    tableState: null
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
    ingestionChartCanvas: document.getElementById('ingestionChart')
};

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
function renderTable(rows, isOnline = true) {
    if (!rows.length) {
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
            <td>${valueOrDash(row.unit_id, 0)}</td>
            <td>${valueOrDash(row.time_cycles, 0)}</td>
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
        const value = key === 'time_cycles' ? row.time_cycles : row[key];
        reading.textContent = valueOrDash(value, 2);
        bar.style.width = `${key === 'time_cycles' ? sensorPercent(value, 0, 300) : sensorPercent(value)}%`;
        box.querySelector('.name').textContent = label;
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
                label: 'InfluxDB Ingests',
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
                        label: (item) => `  ${item.formattedValue} records`
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

function updateIngestionChart(recordCount, isOnline) {
    if (!ingestionChart) initIngestionChart();
    if (!ingestionChart) return;

    const now = new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });

    if (!isOnline) {
        // Don't push new points when offline — leave chart frozen
        return;
    }

    chartLabels.push(now);
    chartData.push(recordCount ?? 0);

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

function setOnlineState(isOnline, message) {
    elements.apiStatus.textContent = isOnline ? 'Live' : 'Offline';
    elements.apiStatus.style.color = isOnline ? 'var(--status-ok)' : 'var(--status-warn)';

    // Dynamic styling for streamBadge status-chip
    elements.streamBadge.className = isOnline ? 'status-chip' : 'status-chip warning';
    elements.streamBadge.textContent = isOnline ? 'Streaming' : 'Disconnected';

    // Dynamic styling for tableStatus status-chip
    elements.tableStatus.className = isOnline ? 'status-chip' : 'status-chip warning';
    elements.tableStatus.innerHTML = isOnline
        ? '<span class="pulse" id="tableStatusPulse"></span> Live'
        : '<span class="pulse warning" id="tableStatusPulse"></span> Offline';

    // Show last successful connection time when status is OFFLINE
    if (isOnline) {
        elements.statusNote.textContent = message;
    } else {
        if (state.lastSuccessTime) {
            elements.statusNote.innerHTML = `Backend API could not be reached.<br><small style="color: var(--muted); font-size: 0.8rem; margin-top: 4px; display: block;">Last sync: ${df.format(state.lastSuccessTime)}</small>`;
        } else {
            elements.statusNote.textContent = 'Backend API could not be reached.';
        }
    }

    if (elements.liveNoteDot) {
        elements.liveNoteDot.className = isOnline ? 'note-indicator-dot status online' : 'note-indicator-dot status';
    }

    // Toggle active class on pipelineTrack (triggers shimmer animation)
    if (elements.pipelineTrack) {
        elements.pipelineTrack.classList.toggle('active', isOnline);
    }

    // Toggle zero-data watermark message in chart
    if (elements.chartNoDataMessage) {
        elements.chartNoDataMessage.style.display = isOnline ? 'none' : 'flex';
    }
    if (elements.ingestionChartCanvas) {
        elements.ingestionChartCanvas.style.opacity = isOnline ? '1' : '0.35';
    }

    // Toggle warning class on all pulse indicator dots on the page when offline
    const pulses = document.querySelectorAll('.pulse');
    pulses.forEach(pulse => {
        if (isOnline) {
            pulse.classList.remove('warning');
            pulse.classList.remove('offline');
        } else {
            pulse.classList.add('warning');
        }
    });

    animateSparkline(isOnline);

    // If offline, populate fields with skeleton loading indicators to make it feel polished
    if (!isOnline) {
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
        renderTable([], isOnline);

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

function updateDashboard(payload) {
    const rows = Array.isArray(payload.latest_records) ? payload.latest_records : [];
    state.values = rows;

    const metrics = payload.metrics || {};
    const latest = rows[0] || null;

    // We got data, so mark it online
    state.lastSuccessTime = new Date();
    setOnlineState(true, 'System operating normally.');

    elements.streamCount.textContent = metrics.record_count ? nf.format(metrics.record_count) : '--';
    elements.unitCount.textContent = metrics.unique_units ? nf.format(metrics.unique_units) : '--';
    elements.latestCycle.textContent = latest ? nf.format(safeInt(latest.time_cycles) ?? 0) : '--';
    elements.freshnessMetric.textContent = payload.latest_age_seconds !== null && payload.latest_age_seconds !== undefined
        ? formatAge(payload.latest_age_seconds)
        : '--';

    elements.snapshotLabel.textContent = latest
        ? `Unit ${valueOrDash(latest.unit_id, 0)} • Cycle ${valueOrDash(latest.time_cycles, 0)}`
        : 'Waiting for data';

    elements.lastUpdate.textContent = payload.latest_timestamp ? formatTimestamp(payload.latest_timestamp) : '--';
    elements.freshnessText.textContent = payload.latest_age_seconds !== null && payload.latest_age_seconds !== undefined
        ? formatAge(payload.latest_age_seconds)
        : '--';
    elements.unitText.textContent = latest
        ? `Unit ${valueOrDash(latest.unit_id, 0)}`
        : '--';
    elements.windowLabel.textContent = payload.window_label || 'Last 24 hours';

    elements.freshnessBar.style.width = `${Math.max(12, Math.min(100, 100 - Math.min(100, payload.latest_age_seconds || 0)))}%`;
    elements.unitBar.style.width = `${metrics.unit_bar_percent || 0}%`;

    // Dynamic ingestion rate calculation
    if (state.lastRecordCount && metrics.record_count) {
        const diff = metrics.record_count - state.lastRecordCount;
        const rate = Math.max(0, diff / (POLL_MS / 1000));
        elements.ingestionRate.textContent = rate > 0 ? `${rate.toFixed(1)} rec/s` : '1.0 rec/s';
    } else {
        elements.ingestionRate.textContent = '1.0 rec/s';
    }
    state.lastRecordCount = metrics.record_count || null;

    renderTable(rows);
    renderSensors(latest);

    // Feed the real-time Chart.js ingestion chart
    updateIngestionChart(metrics.record_count ?? null, true);

    setOnlineState(true, `Fetched ${rows.length} live rows from InfluxDB.`);

    if (latest) {
        const values = trackedSensors.map((key) => latest[key]).filter((value) => Number.isFinite(Number(value)));
        const avg = values.length
            ? values.reduce((sum, value) => sum + Number(value), 0) / values.length
            : 0;
        elements.statusNote.textContent = `Last point from unit ${valueOrDash(latest.unit_id, 0)} at cycle ${valueOrDash(latest.time_cycles, 0)}. Sensor average ${avg.toFixed(2)}.`;
    }

    elements.errorBox.style.display = 'none';
}

function showError(error) {
    elements.errorBox.textContent = error;
    elements.errorBox.style.display = 'block';
    setOnlineState(false, 'Backend API could not be reached.');
}

async function loadDashboard() {
    try {
        const response = await Auth.authedFetch('/api/dashboard/live', { cache: 'no-store' });
        if (!response.ok) {
            throw new Error(`Dashboard API returned ${response.status}`);
        }

        const payload = await response.json();
        updateDashboard(payload);
    } catch (error) {
        showError(`Live dashboard update failed: ${error.message}`);
    }
}

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

        const interval = setInterval(() => {
            secondsLeft--;
            if (secondsLeft <= 0) {
                clearInterval(interval);
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
setInterval(loadDashboard, POLL_MS);

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

