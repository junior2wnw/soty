import test from 'node:test';
import assert from 'node:assert/strict';
import { createScopedGatewayFixture, sourceAvailable } from './support/scoped-gateway-fixture.mjs';

test('actual admitted selected app HTTP permits RAM audio previews without widening frame, form or microphone policy',
  { skip: !sourceAvailable && 'Explicit packaged Source required' }, async t => {
    const f = await createScopedGatewayFixture({ t }); await f.launch();
    const response = await f.wire.request(f.embedded + '/embed');
    assert.equal(response.status, 200);
    const policies = [response.headers['content-security-policy']].flat();
    assert.ok(policies.some(value => /(?:^|;\s*)media-src 'self' blob: data:(?:;|$)/u.test(value)));
    const root = policies[0];
    assert.match(root, /(?:^|;\s*)frame-src 'none'(?:;|$)/u);
    assert.match(root, /(?:^|;\s*)form-action 'self'(?:;|$)/u);
    assert.match(root, /(?:^|;\s*)connect-src 'self'(?:;|$)/u);
    assert.match(root, /(?:^|;\s*)script-src 'self' 'unsafe-inline'(?:;|$)/u);
    assert.doesNotMatch(root, /script-src[^;]*(?:data:|blob:|\*)/u);
    assert.match(response.headers['permissions-policy'], /(?:^|,\s*)microphone=\(\)(?:,|$)/u);
    assert.match(root, /sandbox allow-scripts allow-forms allow-same-origin allow-downloads allow-popups allow-popups-to-escape-sandbox/u);
  });
