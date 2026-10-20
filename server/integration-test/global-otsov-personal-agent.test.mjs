import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { randomBytes, randomUUID } from 'node:crypto';
import { join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createOrdinaryInstalledFixture } from '../../modules/source-app/test/support/ordinary-installed.mjs';
import * as sdk from '../../modules/source-app/server/index.mjs';
import { createSelectedOtsovSourceApplications } from '../otsov-source.js';
import { captureExternalApplications } from '../external-applications.js';
import { GLM_MODEL } from '../../modules/personal-agent/provider/validation.mjs';

// Explicit integration gate: the maintained Native consumer is supplied by its
// own checkout, never copied into Root. All identities/databases/provider replies
// below are synthetic. No installed config, credential or Live endpoint is read.
assert.ok(process.env.SOTY_OTSOV_NATIVE_FIXTURE_ROOT, 'Provide the maintained OTsov Native checkout for this integration gate');
const nativeRoot = resolve(process.env.SOTY_OTSOV_NATIVE_FIXTURE_ROOT);
const nativeImport = relative => import(pathToFileURL(join(nativeRoot, relative)).href);
const { createOtsovSourceStore } = await nativeImport('server/soty-source-store.mjs');
const { createOtsovSotyIntegration } = await nativeImport('server/soty-integration.mjs');
const { ProjectStore } = await nativeImport('server/storage.mjs');
const { PERMISSIONS } = await nativeImport('server/contracts.mjs');
const { createDefaultDocument } = await nativeImport('src/visual/index.ts');
const payload = { name: 'Synthetic counter without UI navigation', files: [{ path: 'index.html', content: '<!doctype html><button id="n">0</button><script>n.onclick=()=>n.textContent=+n.textContent+1</script>' }] };

async function fixture(t, { editSources = false } = {}) {
  let entries, target, phase = 'create', boundary, rounds = 0, invokeCount = 0, hideReceipt = false, lastInvocation, forcedReference, exactSourceInput;
  const credentials = new Map(), receipts = new Map(), requests = [], tenant = 'synthetic_headless_otsov', keyRef = {};
  const diagnostics = [];
  const f = await createOrdinaryInstalledFixture(t, { workspace: true, sourceProfile: sdk.STANDARD_ROOT_WORKSPACE_SOURCE,
    connectorScript: fileURLToPath(new URL('../../scripts/soty-connector.mjs', import.meta.url)),
    externalApplications(realms) {
      target = realms[0]; const rows = realms.flatMap(realm => createSelectedOtsovSourceApplications({ appId: realm.appId,
        target: realm.profile.target, resource: realm.profile.resource, sourceProfile: realm.profile.sourceProfile }));
      entries = captureExternalApplications(rows); return rows;
    },
    async nativeFactory({ realm, reader, initialize, directory }) {
      const dataDir = join(directory, realm.realmId + '-native'), legacy = new ProjectStore({ dataDir });
      if (initialize) {
        const owner = { actorId: 'synthetic-native-owner', projectId: '*', name: 'Synthetic Native owner', kind: 'human',
          permissions: [...PERMISSIONS], branches: ['*'], allowedPaths: ['**'], nodeIds: ['*'], writeProperties: ['*'] };
        for (let n = 0; n < 13; n++) legacy.createProject(owner, { name: 'Kept Native project ' + n,
          files: { 'index.html': '<!doctype html><title>Kept</title>' }, visualDocument: createDefaultDocument() });
      }
      const source = createOtsovSourceStore({ sdk, databasePath: realm.databasePath, key: realm.cipherKey,
        keyId: 'synthetic-' + realm.realmId, initialize });
      const integration = createOtsovSotyIntegration({ sdk, profile: realm.profile, transportKey: realm.key,
        connectorPort: realm.options.connectorPort, sourceStore: source, workspaceDataDir: join(directory, realm.realmId + '-users'),
        legacyWorkspace: { accountId: reader.account.accountId, dataDir }, legacyStore: legacy });
      const server = createServer((req, res) => {
        const end = res.end.bind(res); res.end = (...args) => {
          try { const value = JSON.parse(String(args[0])); const code = value.error?.code;
            diagnostics.push({ route: req.url.split('?')[0], status: res.statusCode,
              code: typeof code === 'string' && /^(?:source_|app_|context_|OTSOV_|authentication_)[A-Za-z0-9_]{0,96}$/.test(code) ? code : null }); } catch {}
          if (req.url === '/api/embed/receipt' && hideReceipt && res.statusCode === 200
            && Number(legacy.db.prepare('SELECT count(*) AS n FROM projects').get().n) > 13) { res.destroy(); return res; }
          return end(...args);
        };
        if (req.url === '/api/embed/invoke') invokeCount++;
        void integration.handleRequest(req, res).then(handled => { if (!handled) res.writeHead(404).end(); });
      });
      return { store: legacy, source, integration, server,
        listen: () => new Promise(done => server.listen(realm.port, '127.0.0.1', done)),
        async close() { server.closeAllConnections(); if (server.listening) await new Promise(done => server.close(done)); integration.close(); source.close(); legacy.close(); } };
    },
    personalAgent() { return { enabled: true, historyEnabled: false, sourceReferences: entries.map(row => row.contract),
      sourceCredentialFor: ({ reference }) => credentials.get(reference.capabilityId), gateway: {
        gatewayOrigin: 'https://synthetic-headless.invalid', billingTenant: tenant, keyRef, authorizeActor: () => true,
        resolveCredential: async () => ({ keyRef, tenantId: tenant, bearer: 'synthetic-no-provider' }),
        readReceipt: async ({ requestId }) => ({ tenantId: tenant, request: receipts.get(requestId) }),
        fetchImpl: async (_url, init) => {
          const request = JSON.parse(init.body); requests.push(request); rounds++;
          let name, args;
          if (request.messages.at(-1).role !== 'tool') {
            name = 'source_catalog'; args = exactSourceInput ? { query: target.appId + ' ' + exactSourceInput.operation }
              : editSources ? { query: target.appId } : {};
          }
          else {
            const last = JSON.parse(request.messages.at(-1).content), evidence = last.evidence;
            if (Array.isArray(evidence?.[0]?.data?.items)) {
              const create = entries.find(row => row.appId === target.appId && row.catalog.capabilityId.endsWith(':otsov.project.create'));
              if (!exactSourceInput) assert.equal(evidence[0].data.items.some(item => item.reference.capabilityId === create.contract.capabilityId), true);
              await boundary?.();
              name = phase === 'receipt' ? 'source_receipt_1' : phase === 'list' ? 'source_read_1' : 'source_write_1';
              const reference = phase === 'list' ? entries.find(row => row.appId === target.appId && row.catalog.capabilityId.endsWith(':otsov.projects.list')).contract : create.contract;
              args = { reference: forcedReference ?? reference, input: phase === 'list' ? { operation: 'otsov.projects.list', representation: 'compact' }
                : { operation: 'otsov.project.create', payload }, ...(phase === 'receipt' ? { invocationId: lastInvocation } : {}) };
              if (exactSourceInput) {
                const exact = entries.find(row => row.appId === target.appId
                  && row.catalog.capabilityId.endsWith(':' + exactSourceInput.operation));
                assert.ok(exact, 'The exact installed source capability exists');
                name = exact.readonly ? 'source_read_1' : 'source_write_1';
                args = { reference: exact.contract, input: structuredClone(exactSourceInput) };
              }
              if (phase === 'prepared_create') {
                const item = evidence[0].data.items.find(item => item.reference.capabilityId === create.contract.capabilityId);
                assert.ok(item.guidance?.length);
                name = 'apps_guidance_get'; args = { reference: item.reference, guidance: item.guidance[0].reference };
              }
            } else if (phase === 'prepared_create' && last.applicationGuidance) {
              assert.ok(last.applicationGuidance[0].content.includes('OTsov'));
              name = 'source_read_1';
              args = { reference: entries.find(row => row.appId === target.appId && row.catalog.capabilityId.endsWith(':otsov.projects.list')).contract,
                input: { operation: 'otsov.projects.list', representation: 'compact' } };
            } else if (phase === 'prepared_create' && request.messages.at(-2).tool_calls[0].function.name === 'source_read_1') {
              name = 'source_write_1';
              args = { reference: entries.find(row => row.appId === target.appId && row.catalog.capabilityId.endsWith(':otsov.project.create')).contract,
                input: { operation: 'otsov.project.create', payload } };
            } else {
              lastInvocation = evidence[0].data.invocationId ?? lastInvocation;
              name = 'finish_answer'; args = { answer: 'Synthetic Native result verified', selectedEvidenceIds: evidence.map(row => row.id), claims: [] };
            }
          }
          const id = randomUUID(); receipts.set(id, { id, status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL,
            input_tokens: 3, output_tokens: 2, charge_urub: '0' });
          return Response.json({ id: 'synthetic_headless', model: GLM_MODEL, choices: [{ index: 0, finish_reason: 'tool_calls', message: {
            role: 'assistant', tool_calls: [{ id: 'synthetic_' + rounds, type: 'function', function: { name, arguments: JSON.stringify(args) } }] } }],
            usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } }, { headers: { 'x-soty-request-id': id } });
        } } }; } });
  const issued = new Map();
  const apps = f.root().locals.appsService, originalOpen = apps.openScopedSourceContext;
  apps.openScopedSourceContext = async (...args) => { try { return await originalOpen(...args); }
    catch(error) { diagnostics.push({ route: 'host-only-open', code: /^[a-z_]{1,96}$/.test(error.code ?? '') ? error.code : 'unclassified' }); throw error; } };
  for (const kind of ['current_context', 'otsov.projects.list', 'otsov.project.create',
    ...(editSources ? ['otsov.project.context', 'otsov.project.operations'] : [])]) for (const realm of f.realms) {
    const rows = entries.filter(row => row.appId === realm.appId && row.catalog.capabilityId.endsWith(':' + kind));
    const principal = (await f.reader.client.extension('access.principals.create', { expectedAccountId: f.reader.account.accountId, label: 'Synthetic ' + kind })).principal;
    const grant = (await f.reader.client.extension('access.grants.issue', { expectedAccountId: f.reader.account.accountId, principalId: principal.id,
      capabilities: rows.map(row => ({ capabilityId: row.contract.capabilityId, version: 1 })), resources: [...new Set(rows.flatMap(row => row.catalog.resources))],
      effects: rows[0].catalog.effects, recipients: [...new Set(rows.flatMap(row => row.catalog.recipients))], expiresAt: Date.now() + 600000,
      budget: { unit: 'invocations', limit: 30 } })).grant;
    const credential = await f.reader.client.extension('access.credentials.issue', { expectedAccountId: f.reader.account.accountId, grantId: grant.id, audience: f.origin });
    for (const row of rows) credentials.set(row.contract.capabilityId, { token: credential.token, audience: f.origin });
    issued.set(kind + ':' + realm.appId, grant); if (realm.index === 0) issued.set(kind, grant);
  }
  return { ...f, target, entries, requests, issued, credentials, invokeCount: () => invokeCount,
    setTarget: realm => { target = realm; }, setPhase: next => { phase = next; exactSourceInput = undefined; },
    setSourceInput: input => { exactSourceInput = structuredClone(input); phase = 'exact_source'; }, afterDiscovery: callback => { boundary = callback; },
    hideReceipt: value => { hideReceipt = value; }, invocation: () => lastInvocation,
    forceReference: reference => { forcedReference = reference; },
    async expireCredential(realm, kind = 'current_context') {
      const row = entries.find(value => value.appId === realm.appId && value.catalog.capabilityId.endsWith(':' + kind));
      const principal = (await f.reader.client.extension('access.principals.create', { expectedAccountId: f.reader.account.accountId,
        label: 'Synthetic expiring ' + kind })).principal;
      const grant = (await f.reader.client.extension('access.grants.issue', { expectedAccountId: f.reader.account.accountId, principalId: principal.id,
        capabilities: [{ capabilityId: row.contract.capabilityId, version: row.contract.version }], resources: row.catalog.resources,
        effects: row.catalog.effects, recipients: row.catalog.recipients, expiresAt: Date.now() + 400,
        budget: { unit: 'invocations', limit: 30 } })).grant;
      const issued = await f.reader.client.extension('access.credentials.issue', { expectedAccountId: f.reader.account.accountId,
        grantId: grant.id, audience: f.origin });
      credentials.set(row.contract.capabilityId, { token: issued.token, audience: f.origin });
      await new Promise(done => setTimeout(done, Math.max(0, grant.expiresAt - Date.now()) + 20));
      assert.ok(Date.now() >= grant.expiresAt);
      assert.throws(() => f.root().locals.capabilitiesService.authenticateCredential(credentials.get(row.contract.capabilityId)));
    },
    count: realm => Number(realm.instance.store.db.prepare('SELECT count(*) AS n FROM projects').get().n),
    turn: async (suffix, selection, caller = f.reader) => { try { return await caller.client.extension('agent.turn', { requestId: 'headless_otsov_' + suffix,
      prompt: 'Create one synthetic ordinary-code application through my permitted OTsov', ...(selection ? { selection } : {}) }); }
      catch(error) { console.log(JSON.stringify({ syntheticFixtureDiagnostics: diagnostics, nativeInvokeCount: invokeCount })); throw error; } } };
}

test('FIELD creates through the existing OTsov Native consumer/Root/SDK without any UI launch or handle in model data', { timeout: 120000 }, async t => {
  const f = await fixture(t), result = await f.turn('field');
  assert.equal(result.evidence.at(-1).data.completionVerified, true);
  assert.equal(f.count(f.realms[0]), 14); assert.equal(f.count(f.realms[1]), 13); assert.equal(f.invokeCount(), 1);
  assert.equal(f.realms.some(realm => realm.lastLaunch), false);
  assert.equal(JSON.stringify(f.requests).includes('synthetic-no-provider'), false);
  assert.equal(JSON.stringify(f.requests).includes('contextRef'), false);
  const discovery = JSON.parse(f.requests[1].messages.at(-1).content);
  assert.equal(Object.hasOwn(discovery, 'applicationGuidance'), false);
  for (const item of discovery.evidence[0].data.items) {
    const original = f.entries.find(entry => entry.contract.capabilityId === item.reference.capabilityId);
    assert.deepEqual(item.reference, original.contract);
    if (item.inputSchema) assert.deepEqual(item.inputSchema, original.catalog.inputSchema.properties.input);
    for (const guidance of item.guidance ?? []) assert.equal(Object.hasOwn(guidance, 'content'), false);
  }
  assert.equal(f.requests[1].tools.some(tool => tool.function.name === 'apps_guidance_get'), true);
});

test('one signed turn discovers OTsov, reads its skill and projects, then creates one application', { timeout: 120000 }, async t => {
  const f = await fixture(t); f.setPhase('prepared_create');
  const result = await f.turn('skill_list_create');
  assert.equal(result.status, 'complete'); assert.equal(result.evidence.at(-1).data.completionVerified, true);
  assert.equal(f.requests.length, 5); assert.equal(f.invokeCount(), 1);
  assert.equal(f.count(f.realms[0]), 14); assert.equal(f.count(f.realms[1]), 13);
  assert.equal(f.realms.some(realm => realm.lastLaunch), false);
  assert.deepEqual(f.requests.slice(1).map(request => request.messages.at(-2).tool_calls[0].function.name),
    ['source_catalog', 'apps_guidance_get', 'source_read_1', 'source_write_1']);
  assert.deepEqual(f.requests.at(-1).tools.map(tool => tool.function.name), ['finish_answer']);
});

test('an expired grant in the owned visible app omits its UI tools but does not block a separately valid OTsov Source', { timeout: 120000 }, async t => {
  const f = await fixture(t), shown = f.realms[1];
  const launch = await f.reader.client.extension('apps.launch', { appId: shown.appId });
  const boot = await f.wire(shown.embedded + '/_soty/session', { body: { ticket: new URL(launch.launchUrl).hash.slice(1) } });
  assert.equal(boot.status, 200);
  const ready = await f.wire(shown.embedded + '/api/embed/session-continue', { headers: { 'x-soty-frame': boot.value.frame },
    body: { requestId: 'expired-visible-continue-001' } }); assert.equal(ready.status, 200);
  const selection = { appId: shown.appId, handle: launch.scopedCloseHandle };
  await f.expireCredential(shown); const result = await f.turn('expired_visible', selection);
  assert.equal(result.evidence.at(-1).data.completionVerified, true);
  assert.equal(f.requests[0].tools.some(tool => tool.function.name === 'current_context'), false);
  assert.equal(f.count(f.realms[0]), 14); assert.equal(f.count(shown), 13); assert.equal(f.invokeCount(), 1);
  await assert.rejects(f.turn('bad_visible', { ...selection, handle: 'A'.repeat(43) })); assert.equal(f.invokeCount(), 1);
});

test('a different visible Native frame remains current while OTsov executes in its isolated default instance', { timeout: 120000 }, async t => {
  const f = await fixture(t), shown = f.realms[1];
  const launch = await f.reader.client.extension('apps.launch', { appId: shown.appId }); shown.lastLaunch = launch;
  const boot = await f.wire(shown.embedded + '/_soty/session', { body: { ticket: new URL(launch.launchUrl).hash.slice(1) } });
  assert.equal(boot.status, 200);
  const headers = { 'x-soty-frame': boot.value.frame };
  const ready = await f.wire(shown.embedded + '/api/embed/session-continue', { headers, body: { requestId: 'visible-native-continue-001' } });
  assert.equal(ready.status, 200, ready.value?.error?.code); const handle = shown.lastLaunch.scopedCloseHandle;
  const visible = await f.wire(shown.embedded + '/api/embed/query', { headers: { 'x-soty-frame': boot.value.frame },
    body: { requestId: 'visible-native-context-001', input: { operation: 'soty.current_context' } } });
  assert.equal(visible.status, 200, visible.value?.error?.code);
  const before = await f.reader.client.extension('apps.scoped.context', { appId: shown.appId, handle });
  await f.turn('other_visible', { appId: shown.appId, handle });
  const after = await f.reader.client.extension('apps.scoped.context', { appId: shown.appId, handle });
  assert.equal(JSON.stringify(before) === JSON.stringify(after), true);
  const stillVisible = await f.wire(shown.embedded + '/api/embed/query', { headers: { 'x-soty-frame': boot.value.frame },
    body: { requestId: 'visible-native-context-002', input: { operation: 'soty.current_context' } } });
  assert.equal(stillVisible.status, 200, stillVisible.value?.error?.code);
  assert.deepEqual(stillVisible.value.data, visible.value.data);
  assert.equal(f.count(shown), 13); assert.equal(f.count(f.realms[0]), 14);
  assert.equal(f.realms[0].lastLaunch, undefined);
});

test('unknown receipt recovery after another backend target retains the original input/context and never sends invoke twice', { timeout: 120000 }, async t => {
  const f = await fixture(t); f.hideReceipt(true); const unknown = await f.turn('unknown');
  assert.equal(unknown.evidence.at(-1).data.completionVerified, false); assert.equal(f.count(f.realms[0]), 14); assert.equal(f.invokeCount(), 1);
  const id = f.invocation(); assert.equal(typeof id, 'string');
  f.hideReceipt(false); f.setTarget(f.realms[1]); f.setPhase('list'); await f.turn('other_backend');
  f.setTarget(f.realms[0]); f.setPhase('receipt'); const result = await f.turn('original_receipt');
  assert.equal(result.evidence.at(-1).data.invocationId, id); assert.equal(result.evidence.at(-1).data.completionVerified, true);
  assert.equal(f.invokeCount(), 1); assert.equal(f.count(f.realms[0]), 14); assert.equal(f.count(f.realms[1]), 13);
});

test('foreign actor and revoked Source grant deny before Native create', { timeout: 120000 }, async t => {
  const f = await fixture(t); await assert.rejects(f.turn('foreign', undefined, f.foreign));
  assert.equal(f.invokeCount(), 0);
  f.afterDiscovery(async () => { await f.reader.client.extension('access.grants.revoke', { expectedAccountId: f.reader.account.accountId,
    grantId: f.issued.get('otsov.project.create').id }); });
  await assert.rejects(f.turn('revoked')); assert.equal(f.invokeCount(), 0); assert.equal(f.count(f.realms[0]), 13);
});

test('a nonmatching installed reference and an expired target grant never admit a Native create', { timeout: 120000 }, async t => {
  const f = await fixture(t), row = f.entries.find(value => value.appId === f.realms[0].appId
    && value.catalog.capabilityId.endsWith(':otsov.project.create'));
  f.forceReference({ ...row.contract, digest: '0'.repeat(64) }); await assert.rejects(f.turn('wrong_ref'));
  assert.equal(f.invokeCount(), 0); assert.equal(f.count(f.realms[0]), 13);
  f.forceReference(undefined); await f.expireCredential(f.realms[0], 'otsov.project.create'); await assert.rejects(f.turn('expired_target'));
  assert.equal(f.invokeCount(), 0); assert.equal(f.count(f.realms[0]), 13);
});


// The existing fixture keeps real Root/Source/Native storage and original CAS;
// only the provider replies and explicit test identities/grants are synthetic.
// These turns intentionally stay headless: a persisted source is not a UI preview.
test('signed OTsov context and CAS put update the same existing project and independently read back its new revision', { timeout: 120000 }, async t => {
  const f = await fixture(t, { editSources: true }), created = await f.turn('cas_create');
  const projectId = created.evidence.at(-1).data.receipt.artifacts.find(item => item.type === 'otsov-project').id;
  const context = { operation: 'otsov.project.context', projectId, paths: ['index.html'] };
  f.setSourceInput(context); const beforeTurn = await f.turn('cas_read');
  const before = beforeTurn.evidence.at(-1).data;
  assert.equal(before.projectId, projectId); assert.equal(before.file.text, payload.files[0].content);
  assert.equal(before.file.truncated, false); assert.ok(before.file.version >= 1);
  assert.equal(Object.hasOwn(before, 'actor'), false); assert.equal(Object.hasOwn(before, 'tokens'), false);
  const content = '<!doctype html><title>Saved through original OTsov CAS</title><h1>Updated</h1>';
  const updateInput = { operation: 'otsov.project.operations', projectId,
    payload: { fileOps: [{ kind: 'put', path: 'index.html', expectedVersion: before.file.version, content }] } };
  f.setSourceInput(updateInput); const updated = await f.turn('cas_update');
  const proof = updated.evidence.at(-1).data;
  assert.equal(proof.completionVerified, true); assert.equal(proof.effects[0].kind, 'updated');
  assert.equal(proof.receipt.artifacts[0].id, projectId); assert.equal(proof.receipt.artifacts[0].revision, before.revision + 1);
  assert.equal(f.count(f.realms[0]), 14); assert.equal(f.count(f.realms[1]), 13);
  f.setSourceInput(context); const readback = (await f.turn('cas_readback')).evidence.at(-1).data;
  assert.equal(readback.file.text, content); assert.equal(readback.file.version, before.file.version + 1);
  assert.equal(readback.revision, before.revision + 1); assert.notEqual(readback.contentHash, before.contentHash);
  assert.notEqual(readback.file.hash, before.file.hash); assert.equal(readback.projectId, projectId);
  assert.equal(f.invokeCount(), 2); assert.equal(f.realms.some(realm => realm.lastLaunch), false);

  // This scenario already consumed ten current-context invocations (3+2+3+2).
  // Respect the real existing 10/min per-principal admission policy. Never
  // rewrite its clock/ledger or replace a principal to make the CAS test pass.
  const ownerHistory = await f.reader.client.extension('access.invocations.list', {
    expectedAccountId: f.reader.account.accountId, limit: 50 });
  assert.equal(ownerHistory.nextCursor, null, 'This bounded owned fixture history is complete');
  const contextCapability = f.entries.find(row => row.appId === f.target.appId
    && row.catalog.capabilityId.endsWith(':current_context')).contract.capabilityId;
  const contexts = ownerHistory.invocations.filter(row => row.capabilityId === contextCapability);
  assert.equal(contexts.length, 10, 'Create/read/update/readback must have used the real ten context probes');
  assert.ok(contexts.every(row => row.status === 'succeeded' && row.effectState === 'none'));
  const allowAfter = Math.max(...contexts.map(row => row.createdAt)) + 60050;
  console.log(JSON.stringify({ syntheticCasRateWindow: { currentContextInvocations: contexts.length,
    nativeInvokeCount: f.invokeCount(), waitMs: Math.max(0, allowAfter - Date.now()), productionLimitChanged: false } }));
  while (Date.now() < allowAfter) await new Promise(done => setTimeout(done, Math.min(1000, allowAfter - Date.now())));

  // A new request with an obsolete captured file version must not overwrite it.
  f.setSourceInput({ ...updateInput, payload: { fileOps: [{ ...updateInput.payload.fileOps[0], content: 'MUST_NOT_REPLACE' }] } });
  const stale = await f.turn('cas_stale').then(value => ({ value }), error => ({ error }));
  if (stale.value) assert.notEqual(stale.value.evidence.at(-1).data.completionVerified, true);
  assert.equal(f.invokeCount(), 3, 'The stale put must actually reach Native; a pre-admission rate denial is not a CAS regression');
  f.setSourceInput(context); const afterConflict = (await f.turn('cas_after_conflict')).evidence.at(-1).data;
  assert.deepEqual(afterConflict, readback, 'Native rejects stale CAS without changing bytes, revision or file version');

  // Installed availability does not renew the explicitly issued update grant.
  await f.reader.client.extension('access.grants.revoke', { expectedAccountId: f.reader.account.accountId,
    grantId: f.issued.get('otsov.project.operations').id });
  const invokes = f.invokeCount();
  f.setSourceInput({ ...updateInput, payload: { fileOps: [{ ...updateInput.payload.fileOps[0], expectedVersion: readback.file.version,
    content: 'MUST_NOT_APPLY_AFTER_REVOCATION' }] } });
  const revoked = await f.turn('cas_revoked').then(value => ({ value }), error => ({ error }));
  if (revoked.value) assert.notEqual(revoked.value.evidence.at(-1).data.completionVerified, true);
  assert.equal(f.invokeCount(), invokes, 'A revoked signed update grant stops before the Native mutation');
  f.setSourceInput(context); const afterRevocation = (await f.turn('cas_after_revocation')).evidence.at(-1).data;
  assert.deepEqual(afterRevocation, readback); assert.equal(f.realms.some(realm => realm.lastLaunch), false);
});
