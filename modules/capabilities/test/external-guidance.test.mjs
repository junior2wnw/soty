import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createCapabilitiesService } from '../server/index.mjs';
import { createCatalog } from '../server/catalog.mjs';
import { EXTERNAL_ADAPTER_PROFILE } from '../server/external-adapters.mjs';
import { captureExternalGuidance } from '../server/external-guidance.mjs';
import { createExternalCapabilityOperations } from '../../../server/external-capabilities.js';
import { buildCapabilitiesOpenApi } from '../../../server/capabilities-openapi.js';

const CAP = {
  capabilityId: 'planner:createItem', version: 1, appId: 'planner', title: 'Создать задачу', description: 'В одном разрешённом пространстве.',
  visibility: 'private', executionEnabled: true,
  inputSchema: { type: 'object', properties: { title: { type: 'string', maxLength: 160 } }, required: ['title'], additionalProperties: false },
  outputSchema: { type: 'object', properties: { id: { type: 'string', maxLength: 160 } }, required: ['id'], additionalProperties: false },
  resources: ['planner:workspace-a'], effects: ['create'], recipients: ['planner:workspace-a'],
  executionBinding: { kind: 'registered', handler: 'planner:createItem', version: 1,
    binding: { id: 'planner:workspace-a', version: 1, digest: 'a'.repeat(64) } },
};
const REF = Object.freeze({ capabilityId: CAP.capabilityId, version: CAP.version, digest: createCatalog([CAP]).get(CAP.capabilityId, CAP.version).digest });
const OWNER = Object.freeze({ accountId: 'guidance_owner', deviceId: 'guidance_device' });
const AUDIENCE = 'https://soty.test';
const GUIDE = Object.freeze({ contract: REF, kind: 'skill', language: 'ru', title: 'Работа с задачами',
  summary: 'Когда требуется задача в выбранном пространстве.', content: 'Проверьте выбранное пространство. После потерянного ответа используйте прежний ключ запроса.\nНе меняйте получателя без разрешения человека.' });

function environment(t, { guidance = [GUIDE] } = {}) {
  const parent = realpathSync(tmpdir()), directory = realpathSync(mkdtempSync(join(parent, 'soty-guidance-'))), file = join(directory, 'capabilities.sqlite');
  let allowed = true, active = true, sourceChecks = 0;
  const adapter = { profile: EXTERNAL_ADAPTER_PROFILE, withAuthority(_request, callback) {
    sourceChecks++; if (!allowed) throw Object.assign(new Error('denied'), { code: 'external_resource_denied' }); return callback();
  }, execute() { throw new Error('guidance_must_never_execute'); }, readProof() { throw new Error('guidance_must_never_read_effect'); } };
  const open = () => createCapabilitiesService({ databasePath: file, projectId: 'guidance-test', actorActive: actor => active
    && actor.accountId === OWNER.accountId && actor.deviceId === OWNER.deviceId, catalog: [CAP], documentation: [],
  externalAdapters: [{ contract: REF, adapter }], externalGuidance: guidance });
  let service = open();
  const call = (op, args = {}) => service.execute({ op: `access.${op}`, actor: OWNER, args: { expectedAccountId: OWNER.accountId, ...args } });
  const principal = call('principals.create', { label: 'Агент' }).principal;
  const grant = call('grants.issue', { principalId: principal.id, capabilities: [{ capabilityId: REF.capabilityId, version: REF.version }],
    resources: CAP.resources, effects: CAP.effects, recipients: CAP.recipients, expiresAt: Date.now() + 600000,
    allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: 3 } }).grant;
  const issued = call('credentials.issue', { grantId: grant.id, audience: AUDIENCE });
  const actor = () => service.authenticateCredential({ token: issued.token, audience: AUDIENCE });
  t.after(() => { service.close(); assert.equal(dirname(realpathSync(directory)), parent); assert.match(basename(directory), /^soty-guidance-/u);
    rmSync(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }); });
  return { get service() { return service; }, actor, call, grant,
    revokeSource() { allowed = false; }, revokeDevice() { active = false; }, checks: () => sourceChecks,
    reopen() { service.close(); service = open(); },
    ledgerCount() { const db = new DatabaseSync(file, { readOnly: true }); try { return db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n; } finally { db.close(); } },
  };
}

test('real grant and Source authority disclose exact static guidance, keep pins across restart and never invoke an action', t => {
  const f = environment(t), request = { actor: f.actor(), reference: REF };
  const before = f.service.externalGuidance.list(request); assert.equal(before.items.length, 1); assert.equal(before.authority, 'application-data');
  assert.equal(JSON.stringify(before).includes(GUIDE.content), false);
  const content = f.service.externalGuidance.get({ ...request, guidance: before.items[0].reference }); assert.equal(content.content, GUIDE.content);
  assert.equal(content.contract.digest, REF.digest); assert.equal(f.ledgerCount(), 0); assert.ok(f.checks() >= 4);
  f.reopen(); const after = f.service.externalGuidance.list({ actor: f.actor(), reference: REF }); assert.deepEqual(after, before);
  assert.equal(f.service.external.getContract({ actor: f.actor(), reference: REF }).skills.length, 0, 'old catalog wire stays unchanged');
});

test('Source, device and Root grant revocation each close guidance under actual current authority', t => {
  for (const scope of ['source', 'device', 'grant']) {
    const f = environment(t), actor = f.actor(), index = f.service.externalGuidance.list({ actor, reference: REF });
    if (scope === 'source') f.revokeSource(); else if (scope === 'device') f.revokeDevice(); else f.call('grants.revoke', { grantId: f.grant.id });
    assert.throws(() => f.service.externalGuidance.list({ actor, reference: REF }));
    assert.throws(() => f.service.externalGuidance.get({ actor, reference: REF, guidance: index.items[0].reference }));
    assert.equal(f.ledgerCount(), 0);
  }
});

test('wrong capability, old content pin, extra fields and getter cannot select or overwrite guidance', t => {
  const f = environment(t), actor = f.actor(), index = f.service.externalGuidance.list({ actor, reference: REF }), guidance = index.items[0].reference;
  assert.throws(() => f.service.externalGuidance.get({ actor, reference: { ...REF, digest: 'f'.repeat(64) }, guidance }));
  assert.throws(() => f.service.externalGuidance.get({ actor, reference: REF, guidance: { ...guidance, digest: 'f'.repeat(64) } }), /external_guidance_not_found/u);
  assert.throws(() => f.service.externalGuidance.get({ actor, reference: REF, guidance: { ...guidance, url: 'https://elsewhere.test' } }));
  let getter = false;
  const raw = { ...GUIDE }; Object.defineProperty(raw, 'content', { enumerable: true, get() { getter = true; return GUIDE.content; } });
  assert.throws(() => captureExternalGuidance([raw])); assert.equal(getter, false);
  assert.throws(() => captureExternalGuidance([{ ...GUIDE, content: 'x'.repeat(32769) }]));
  assert.throws(() => captureExternalGuidance([{ ...GUIDE, url: 'https://elsewhere.test' }]));
  assert.throws(() => captureExternalGuidance([{ ...GUIDE, content: 'Bearer ' + 'x'.repeat(32) }]), /secret_not_allowed/u);
  assert.throws(() => captureExternalGuidance([GUIDE, GUIDE]), /external_guidance_invalid/u);
});

test('installed typed tools validate outputs, recheck revocation and expose guidance only in the configured OpenAPI surface', async t => {
  const f = environment(t), operations = createExternalCapabilityOperations({ service: f.service, origin: AUDIENCE });
  assert.equal(operations.tools.length, 7); assert.equal(operations.check('apps_guidance_list', { reference: REF }), true);
  assert.equal(operations.check('apps_guidance_list', { reference: REF, actor: OWNER }), false);
  const args = { reference: REF }, value = await operations.call({ actor: f.actor(), name: 'apps_guidance_list', args });
  const content = await operations.call({ actor: f.actor(), name: 'apps_guidance_get', args: { reference: REF, guidance: value.items[0].reference } });
  assert.equal(content.content, GUIDE.content);
  f.revokeSource(); assert.throws(() => operations.recheck({ actor: f.actor(), name: 'apps_guidance_list', args, value }));
  const defaultDoc = buildCapabilitiesOpenApi({ externalConfigured: true, mcpConfigured: true });
  assert.equal(defaultDoc['x-soty-mcp'].tools.length, 9); assert.equal(defaultDoc.paths['/api/capabilities/v1/app-actions/guidance-list'], undefined);
  const configured = buildCapabilitiesOpenApi({ externalConfigured: true, mcpConfigured: true, guidanceConfigured: true });
  assert.equal(configured['x-soty-mcp'].tools.length, 11); assert.ok(configured.paths['/api/capabilities/v1/app-actions/guidance-get'].post);
  assert.throws(() => buildCapabilitiesOpenApi({ guidanceConfigured: true }));
});

test('no installed guidance leaves the original tool set and does not invent instructions', t => {
  const f = environment(t, { guidance: [] }); assert.equal(f.service.externalGuidance, null);
  const operations = createExternalCapabilityOperations({ service: f.service, origin: AUDIENCE }); assert.equal(operations.tools.length, 5);
});
