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
  const ports = {
    '../core/connect-client': {
      accountClient: {
        async getLocalState() { return { accountId, label: 'Local profile' }; },
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
  };
  vm.runInNewContext(compiled, { module, exports: module.exports, require: name => ports[name], URL,
    window: { location: { href: 'https://shell.example/#mine' }, addEventListener() {} },
  });
  await module.exports.startWorld({});
  const first = mounts[0];
  accountId = 'account-B'; observer({ accountId });
  const second = mounts[1];
  await first.options.api.request('world.community.list', {});
  await second.options.api.request('world.profile.get');
  await first.options.requestContact({ profileId: 'contact-id' });
  assert.equal(first.destroyed, true); assert.equal(resets, 1); assert.equal(bootstraps, 0);
  assert.deepEqual(calls.map(value => value.context.expectedAccountId), ['account-A', 'account-B', 'account-A']);
  assert.deepEqual(calls[0].args, {}, 'client admission context does not alter strict product arguments');
  accountId = null; observer({ accountId });
  await assert.rejects(mounts[2].options.api.request('world.profile.get'), { code: 'NO_LOCAL_PROFILE' });
  assert.equal(calls.length, 3, 'a controller without identity cannot enqueue unchecked operations');
});
