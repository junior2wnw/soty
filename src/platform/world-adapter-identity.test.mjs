import * as connectionIntents from './connection-intent.mjs';
import * as nativeVoiceDescriptor from './native-voice-descriptor.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';

test('actual world adapter keeps each controller and late contact action bound to its mounted account', async () => {
  const source = await readFile(new URL('./world-adapter.ts', import.meta.url), 'utf8');
  const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  const module = { exports: {} }, mounts = [], calls = [];
  let accountId = 'account-A', observer, bootstraps = 0, resets = 0;
  let status = { notesCreateEnabled: false, audience: 'https://shell.example', appFeedbackEnabled: true, mcpAudience: 'https://shell.example/mcp' };
  const ports = {
    './notifications-client.mjs': { createNotificationsController() { return { async refresh() {}, dispose() {} }; } },
    './native-voice-descriptor.mjs': nativeVoiceDescriptor,
    './connection-intent.mjs': connectionIntents,
    './connection-hub': { async ensureConnectionAccount(_host, options) { const state = await options.client.getLocalState(); return { state, intent: null }; } },
    '../core/connect-client': {
      accountClient: {
        async getLocalState() { return { accountId, deviceId: accountId ? 'device-' + accountId : null, label: 'Local profile' }; },
        async bootstrap() { bootstraps++; },
        async extension(operation, args, context) { calls.push(JSON.parse(JSON.stringify({ operation, args, context }))); return {}; },
      },
      observeAccount(callback) { observer = callback; return () => {}; },
    },
    '../world/app': { mountWorldApp(_root, options) {
      const mounted = { options, destroyed: false, destroy() { this.destroyed = true; }, async refresh() {} };
      mounts.push(mounted); return mounted;
    } },
    './local-apps': { createAppActions() { return { connectDevice() {}, agentCreate() {}, resetAccount() { resets++; }, destroy() {} }; } },
    './assistant': { mountAssistant() {} },
    './call-identity': { bindCallIdentity() { return () => {}; } },
    '../sync': { createHumanCallPorts() { return { subscribe: () => () => {}, snapshot: () => ({ connection: 'idle', peers: 0, playbackBlocked: false, turnConfigured: false }) }; } },
    '../../modules/personal-agent/call/lifecycle.mjs': { createCallLifecycle() { return { snapshot: () => ({ state: 'idle', microphone: 'off', scope: null, code: null }), dispose() {} }; } },
  };
  vm.runInNewContext(compiled, { module, exports: module.exports, require: name => ports[name], URL, AbortSignal, AbortController,
    fetch: async () => ({ ok: true, json: async () => status }),
    window: { location: { href: 'https://shell.example/#mine', origin: 'https://shell.example' }, addEventListener() {}, removeEventListener() {} },
  });
  await module.exports.startWorld({ isConnected: true });
  const first = mounts[0];
  assert.deepEqual(JSON.parse(JSON.stringify(await first.options.accessAvailability())), status);
  status = { notesCreateEnabled: true, audience: 'https://shell.example' };
  assert.deepEqual(JSON.parse(JSON.stringify(await first.options.accessAvailability())), { ...status, appFeedbackEnabled: false, mcpAudience: null });
  accountId = 'account-B'; observer({ accountId, deviceId: accountId ? 'device-' + accountId : null });
  const second = mounts[1];
  await first.options.api.request('world.community.list', {});
  await second.options.api.request('world.profile.get');
  await first.options.requestContact({ profileId: 'contact-id' });
  assert.equal(first.destroyed, true); assert.equal(resets, 1); assert.equal(bootstraps, 0);
  assert.deepEqual(calls.map(value => value.context.expectedAccountId), ['account-A', 'account-B', 'account-A']);
  assert.deepEqual(calls[0].args, {}, 'client admission context does not alter strict product arguments');
  await first.options.api.request('apps.feedback.context', { appId: 'app-' + 'a'.repeat(32) });
  assert.deepEqual(calls.at(-1).args, { appId: 'app-' + 'a'.repeat(32) });
  assert.equal(calls.at(-1).context.expectedAccountId, 'account-A', 'native feedback uses the same original account admission fence');
  accountId = null; observer({ accountId, deviceId: accountId ? 'device-' + accountId : null });
  await assert.rejects(mounts[2].options.api.request('world.profile.get'), { code: 'NO_LOCAL_PROFILE' });
  assert.equal(calls.length, 4, 'a controller without identity cannot enqueue unchecked operations');
});
