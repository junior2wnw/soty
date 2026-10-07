import test from 'node:test';
import assert from 'node:assert/strict';
import { createSourceNativeAuthorityPort, createNativeAuthorityRuntime } from '../server/native-authority.mjs';

const binding = { issuer: 'https://issuer.test/human-identity', subject: 'opaque-verified-subject', resource: { kind: 'soty.resource.v1', nativeId: 'one', incarnationId: 'incarnation' } };
test('constructor brand and exact proof wrappers reject JSON/missing/default permissions', async () => {
  assert.throws(() => createNativeAuthorityRuntime({ capture() {}, withCurrent() {} }));
  let live = true;
  const runtime = createNativeAuthorityRuntime(createSourceNativeAuthorityPort({ capture: async () => ({ nativeId: 'actual-native-one' }),
    withCurrent(_native, scope, callback) { assert.equal(scope.subject, binding.subject); assert.equal(live, true); return callback(); } }));
  const proof = await runtime.capture(binding);
  assert.throws(() => runtime.withCurrent(JSON.parse(JSON.stringify(proof)), () => true));
  await assert.rejects(runtime.call(proof, 'read', {}), error => error.code === 'source_app_capability_disabled');
  live = false; assert.throws(() => runtime.withCurrent(proof, () => true));
});
test('late/double/swallowed/reentrant and thenable final Native callbacks fail closed', async () => {
  for (const mode of ['late', 'double', 'swallowed', 'thenable']) {
    let callback;
    const runtime = createNativeAuthorityRuntime(createSourceNativeAuthorityPort({ capture: async () => ({}),
      withCurrent(_native, _binding, apply) { callback = apply; if (mode === 'late') return true;
        const result = apply(); if (mode === 'double') apply(); if (mode === 'swallowed') { try { apply(); } catch {} }
        return mode === 'thenable' ? Promise.resolve(result) : result; } }));
    await assert.rejects(runtime.capture(binding)); assert.throws(() => callback());
  }
});
test('Native revoke during read/feedback await never returns private result', async () => {
  let live = true, entered, release;
  const reached = new Promise(done => { entered = done; }), gate = new Promise(done => { release = done; });
  const runtime = createNativeAuthorityRuntime(createSourceNativeAuthorityPort({ capture: async () => ({}),
    withCurrent(_native, _binding, apply) { assert.equal(live, true); return apply(); },
    read: async () => { entered(); await gate; return { text: 'private fixture only' }; } }));
  const proof = await runtime.capture(binding), pending = runtime.call(proof, 'read', {}); await reached; live = false; release();
  await assert.rejects(pending);
});
