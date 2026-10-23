import test from 'node:test';
import assert from 'node:assert/strict';
import { nativeHttpFixture } from './support/native-capability-http.mjs';

test('actual Root feedback readiness covers its seven registered contracts and MCP independently of Notes', async t => {
  const f = await nativeHttpFixture(t, { enabled: false });
  const status = await f.http('/api/capabilities/v1/status');
  assert.equal(status.status, 200); assert.equal(status.headers['cache-control'], 'no-store');
  assert.deepEqual(status.body, { notesCreateEnabled: false, audience: f.origin, appFeedbackEnabled: true, mcpAudience: f.origin + '/mcp' });
  const installed = f.app.locals.capabilitiesService.external.contracts.filter(item => item.capabilityId.startsWith('apps.feedback.'));
  assert.equal(installed.length, 7); assert.ok(f.app.locals.capabilitiesMcp);
  assert.deepEqual(Object.keys(status.body).sort(), ['appFeedbackEnabled', 'audience', 'mcpAudience', 'notesCreateEnabled']);
  await f.app.locals.closeServices();
  assert.equal(f.app.locals.capabilitiesApiStatus().appFeedbackEnabled, false, 'closing host cannot advertise a live feedback transport');
});

test('a real host without the feedback bridge keeps Notes issuance and refuses feedback availability', async t => {
  const f = await nativeHttpFixture(t, { universalAppsEnabled: false });
  assert.deepEqual((await f.http('/api/capabilities/v1/status')).body,
    { notesCreateEnabled: true, audience: f.origin, appFeedbackEnabled: false, mcpAudience: null });
});
