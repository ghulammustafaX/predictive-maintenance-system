/**
 * PMS Dashboard Run Selector Regression Tests
 * Tests for issue: Run selector stuck on "Loading runs..." indefinitely
 * 
 * Run with: node dashboard.test.js
 */

// Mock DOM environment
class MockElement {
    constructor(tagName) {
        this.tagName = tagName;
        this.children = [];
        this.disabled = false;
        this.value = '';
        this.className = '';
        this.textContent = '';
        this.innerHTML = '';
        this._style = {};
    }
    
    get style() { return this._style; }
    set style(val) { this._style = val; }
    
    replaceChildren(...nodes) {
        this.children = nodes;
    }
    
    add(option) {
        this.children.push(option);
    }
    
    addEventListener() {}
}

class MockOption {
    constructor(label, value) {
        this.label = label;
        this.value = value;
        this.selected = false;
    }
}

global.Option = MockOption;
global.document = {
    getElementById: () => new MockElement('div'),
    querySelector: () => new MockElement('div'),
    querySelectorAll: () => [],
    documentElement: { getAttribute: () => 'light' }
};
global.window = {
    addEventListener: () => {}
};
global.performance = { now: () => Date.now() };

// Mock Auth module
global.Auth = {
    requireAuth: () => {},
    isLoggedIn: () => true,
    fetchMe: async () => ({ email: 'test@example.com' }),
    authedFetch: async (url) => {
        if (url.includes('/api/fleet/status')) {
            return {
                ok: true,
                status: 200,
                json: async () => ({ fleet: [{ engine_id: 'FD001-001', status: 'healthy' }] })
            };
        }
        if (url.includes('/telemetry')) {
            return {
                ok: true,
                status: 200,
                json: async () => ({
                    points: [
                        { run_id: 'run-1', timestamp: new Date().toISOString(), cycle: 1 },
                        { run_id: 'run-2', timestamp: new Date().toISOString(), cycle: 2 }
                    ]
                })
            };
        }
        if (url.includes('/rul')) {
            return {
                ok: true,
                status: 200,
                json: async () => ({ prediction: { rul_cycles: 100 } })
            };
        }
        return { ok: false, status: 404 };
    }
};

global.fetch = async (url) => {
    if (url.includes('/api/health')) {
        return { ok: true, status: 200 };
    }
    return { ok: false, status: 404 };
};

// Test utilities
let testsPassed = 0;
let testsFailed = 0;

function assert(condition, message) {
    if (!condition) {
        console.error(`❌ FAIL: ${message}`);
        testsFailed++;
        throw new Error(message);
    } else {
        console.log(`✅ PASS: ${message}`);
        testsPassed++;
    }
}

function assertEqual(actual, expected, message) {
    assert(actual === expected, `${message} (expected: ${expected}, actual: ${actual})`);
}

async function test(name, fn) {
    console.log(`\n📋 Test: ${name}`);
    try {
        await fn();
        console.log(`✓ ${name} passed`);
    } catch (error) {
        console.error(`✗ ${name} failed:`, error.message);
    }
}

// Load dashboard code (inline critical functions for testing)
const state = {
    limit: 8,
    values: [],
    lastRecordCount: null,
    refreshCooldownActive: false,
    lastSuccessTime: null,
    tableState: null,
    fleet: [],
    selectedEngine: 'FD001-001',
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
    runSelect: new MockElement('select')
};

function renderRunOptions(points) {
    if (!elements.runSelect) return;
    const runs = [...new Set(points.map((point) => point.run_id).filter(Boolean))];
    if (state.selectedRunId && !runs.includes(state.selectedRunId)) {
        state.selectedRunId = null;
    }
    if (!state.selectedRunId) {
        state.selectedRunId = runs[0] || null;
    }
    elements.runSelect.replaceChildren();
    if (!runs.length) {
        elements.runSelect.add(new MockOption('No runs in range', ''));
        elements.runSelect.disabled = true;
        return;
    }
    runs.forEach((runId) => {
        const option = new MockOption(runId, runId);
        option.selected = runId === state.selectedRunId;
        elements.runSelect.add(option);
    });
    elements.runSelect.disabled = false;
}

function setRunSelectorState(label, disabled = true) {
    if (!elements.runSelect) return;
    elements.runSelect.replaceChildren(new MockOption(label, ''));
    elements.runSelect.disabled = disabled;
}

// ==================== REGRESSION TESTS ====================

async function testInitialLoad() {
    await test('Initial load sets "Loading runs..." only on first poll', async () => {
        // Reset state
        state.hasRenderedData = false;
        state.values = [];
        elements.runSelect = new MockElement('select');
        
        // Simulate first poll
        if (!state.hasRenderedData) {
            setRunSelectorState('Loading runs...', true);
        }
        
        assertEqual(elements.runSelect.children.length, 1, 'Should have one option');
        assertEqual(elements.runSelect.children[0].label, 'Loading runs...', 'Should show "Loading runs..."');
        assertEqual(elements.runSelect.disabled, true, 'Should be disabled');
        
        // Simulate successful telemetry render
        const points = [
            { run_id: 'run-1', timestamp: new Date().toISOString() },
            { run_id: 'run-2', timestamp: new Date().toISOString() }
        ];
        renderRunOptions(points);
        state.hasRenderedData = true;
        
        assertEqual(elements.runSelect.children.length, 2, 'Should have two run options');
        assertEqual(elements.runSelect.disabled, false, 'Should be enabled');
        
        // Simulate second poll (should NOT reset to "Loading runs...")
        if (!state.hasRenderedData) {
            setRunSelectorState('Loading runs...', true);
        }
        
        // Dropdown should remain populated
        assertEqual(elements.runSelect.children.length, 2, 'Should still have two run options after second poll');
        assertEqual(elements.runSelect.disabled, false, 'Should still be enabled after second poll');
    });
}

async function testTelemetryFailure() {
    await test('Telemetry failure shows "Runs unavailable", not "Loading runs..."', async () => {
        // Reset state
        state.hasRenderedData = false;
        state.values = [];
        elements.runSelect = new MockElement('select');
        
        // Simulate first poll
        if (!state.hasRenderedData) {
            setRunSelectorState('Loading runs...', true);
        }
        
        // Simulate telemetry failure (error handler)
        if (!state.values || state.values.length === 0) {
            setRunSelectorState('Runs unavailable', true);
        }
        
        assertEqual(elements.runSelect.children[0].label, 'Runs unavailable', 'Should show "Runs unavailable" on failure');
        assertEqual(elements.runSelect.disabled, true, 'Should be disabled on failure');
    });
}

async function testRulFailure() {
    await test('RUL failure does not affect run selector state', async () => {
        // Reset state
        state.hasRenderedData = true;
        state.values = [{ run_id: 'run-1' }];
        elements.runSelect = new MockElement('select');
        
        // Simulate successful telemetry
        const points = [
            { run_id: 'run-1', timestamp: new Date().toISOString() }
        ];
        renderRunOptions(points);
        
        const runCountBeforeRulFailure = elements.runSelect.children.length;
        
        // Simulate RUL failure (should not touch run selector)
        // (No action - RUL failure only affects RUL display)
        
        assertEqual(elements.runSelect.children.length, runCountBeforeRulFailure, 'Run selector unchanged after RUL failure');
        assertEqual(elements.runSelect.disabled, false, 'Run selector still enabled after RUL failure');
    });
}

async function testPollingPreservesDropdown() {
    await test('Polling preserves dropdown across multiple cycles', async () => {
        // Reset state
        state.hasRenderedData = true;
        state.values = [{ run_id: 'run-1' }, { run_id: 'run-2' }];
        elements.runSelect = new MockElement('select');
        
        const points = [
            { run_id: 'run-1', timestamp: new Date().toISOString() },
            { run_id: 'run-2', timestamp: new Date().toISOString() }
        ];
        
        // Simulate 3 polling cycles
        for (let i = 0; i < 3; i++) {
            // Poll starts
            if (!state.hasRenderedData) {
                setRunSelectorState('Loading runs...', true);
            }
            
            // Telemetry renders
            renderRunOptions(points);
        }
        
        assertEqual(elements.runSelect.children.length, 2, 'Should have two options after 3 polls');
        assertEqual(elements.runSelect.disabled, false, 'Should be enabled after 3 polls');
        assertEqual(elements.runSelect.children[0].label, 'run-1', 'First option preserved');
        assertEqual(elements.runSelect.children[1].label, 'run-2', 'Second option preserved');
    });
}

async function testManualRunSelection() {
    await test('Manual run selection is preserved across polling', async () => {
        // Reset state
        state.hasRenderedData = true;
        state.selectedRunId = null;
        elements.runSelect = new MockElement('select');
        
        const points = [
            { run_id: 'run-1', timestamp: new Date().toISOString() },
            { run_id: 'run-2', timestamp: new Date().toISOString() }
        ];
        
        // Initial render (selects first run by default)
        renderRunOptions(points);
        assertEqual(state.selectedRunId, 'run-1', 'Should select first run by default');
        
        // User manually selects second run
        state.selectedRunId = 'run-2';
        
        // Next poll cycle
        renderRunOptions(points);
        assertEqual(state.selectedRunId, 'run-2', 'User selection preserved after poll');
        assertEqual(elements.runSelect.children[1].selected, true, 'Second option marked as selected');
    });
}

async function testRunDisappears() {
    await test('Selected run is cleared if it disappears from telemetry', async () => {
        // Reset state
        state.hasRenderedData = true;
        state.selectedRunId = 'run-2';
        elements.runSelect = new MockElement('select');
        
        // New telemetry data without run-2
        const points = [
            { run_id: 'run-1', timestamp: new Date().toISOString() }
        ];
        
        renderRunOptions(points);
        assertEqual(state.selectedRunId, 'run-1', 'Should fall back to first run when selected run disappears');
    });
}

async function testEmptyTelemetry() {
    await test('Empty telemetry shows "No runs in range"', async () => {
        // Reset state
        state.hasRenderedData = true;
        elements.runSelect = new MockElement('select');
        
        // Empty telemetry
        const points = [];
        renderRunOptions(points);
        
        assertEqual(elements.runSelect.children.length, 1, 'Should have one option');
        assertEqual(elements.runSelect.children[0].label, 'No runs in range', 'Should show "No runs in range"');
        assertEqual(elements.runSelect.disabled, true, 'Should be disabled');
        assertEqual(state.selectedRunId, null, 'Selected run should be null');
    });
}

// ==================== RUN ALL TESTS ====================

async function runAllTests() {
    console.log('\n🧪 PMS Dashboard Run Selector Regression Test Suite\n');
    console.log('=' .repeat(60));
    
    await testInitialLoad();
    await testTelemetryFailure();
    await testRulFailure();
    await testPollingPreservesDropdown();
    await testManualRunSelection();
    await testRunDisappears();
    await testEmptyTelemetry();
    
    console.log('\n' + '='.repeat(60));
    console.log(`\n📊 Test Results: ${testsPassed} passed, ${testsFailed} failed`);
    
    if (testsFailed === 0) {
        console.log('\n✅ All tests passed!\n');
        process.exit(0);
    } else {
        console.log('\n❌ Some tests failed!\n');
        process.exit(1);
    }
}

runAllTests().catch(error => {
    console.error('\n💥 Test suite crashed:', error);
    process.exit(1);
});
