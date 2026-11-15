const assert = require('node:assert/strict');
const fs = require('node:fs');
const test = require('node:test');

const dashboardSource = fs.readFileSync(
  require('node:path').join(__dirname, '..', 'frontend', 'dashboard', 'dashboard.js'),
  'utf8',
);

test('dashboard reads the settled telemetry result from the Promise.allSettled array', async () => {
  const telemetryResult = await Promise.allSettled([Promise.resolve({ status: 200 })]);

  assert.equal(telemetryResult[0].status, 'fulfilled');
  assert.match(dashboardSource, /telemetryResult\[0\]\.status/);
  assert.doesNotMatch(dashboardSource, /telemetryResult\.status/);
});
