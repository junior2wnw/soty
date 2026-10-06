import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { createCatalog } from '../../capabilities/server/catalog.mjs';
import { normalizeManifest } from '../../apps/server/protocol.mjs';
import { createDescriptor, createAuthorDraft, materializeAuthorDraft } from '../sdk.mjs';
import { authorFixtureConfiguration, runAuthorExample } from '../examples/author.mjs';
import { fixtureConfiguration, pin, runFixture } from '../examples/fixture.mjs';
import { validateDescriptor, capabilityDigest, parseContractJson, canonicalContractJson, contractDigest, createAdmissionHost,
  upgradeAdmissionHost, closeAdmissionHost, beginAdmission, replayAdmission, planAdmission, createFeedbackReceipt, confirmFeedback, holdFeedback, LIMITS } from '../index.mjs';

const clone = value => structuredClone(value);
const throws = (fn, code) => assert.throws(fn, error => error.code === code);
function fixture() {
  const { descriptor, hostConfig } = fixtureConfiguration();
  return { descriptor, hostConfig, host: createAdmissionHost(hostConfig) };
}
const receiptInput = { installationId: 'fixture.inbox', receiptDigest: '7'.repeat(64) };
function temporary(fn) {
  const path = mkdtempSync(join(tmpdir(), 'soty-u1-test-'));
  try { return fn(path); }
  finally {
    assert.equal(dirname(resolve(path)), resolve(tmpdir()));
    assert.match(basename(path), /^soty-u1-test-/);
    rmSync(path, { recursive: true, force: true });
  }
}
const repo = fileURLToPath(new URL('../../../', import.meta.url));
const cli = fileURLToPath(new URL('../cli.mjs', import.meta.url));

test('conformance fixture traverses held/readiness and replay without unlocking agent/local or production', () => {
  assert.deepEqual(runFixture(), { prototype: true, providerCalls: 0, executedHandlers: 0,
    states: ['pending-feedback', 'feedback-held', 'fixture-ready'], replaySameObject: true,
    conflict: 'admission_intent_conflict', gates: { ui: 'fixture-ready', agent: 'not-admitted', local: 'not-admitted' }, productionAdmission: false });
});
test('descriptor is a detached immutable data snapshot and UI-only app is valid', () => {
  const { descriptor } = fixture(); const input = clone(descriptor), accepted = validateDescriptor(input);
  input.app.title = 'After validation';
  assert.equal(accepted.app.title, descriptor.app.title);
  assert.throws(() => { accepted.capabilities[0].effects.push('delete'); }, TypeError);
  input.capabilities = []; assert.equal(validateDescriptor(input).capabilities.length, 0);
});
test('literal UUID/hex IDs are preserved and UI-only namespaces use the same typed prefix grammar', () => {
  const { descriptor, hostConfig } = fixture(); const d = clone(descriptor), h = clone(hostConfig);
  d.app.id = h.context.scope.appId = '01a10c1a-d9df-7e13-ab12-dbe75e313eff';
  h.context.scope.tenantId = '0123456789abcdef'; h.context.ownerId = '0123456789abcdef';
  h.context.scope.environmentId = '01-environment'; d.capabilities = []; h.bindings = [];
  const plan = planAdmission(createAdmissionHost(h), d, '01-request');
  assert.equal(plan.scope.appId, d.app.id); assert.equal(plan.ownerId, h.context.ownerId);
  for (const bad of ['0123', 'name/path', 'name:part', 'a'.repeat(65)]) {
    const changed = clone(d); changed.app.namespace = bad;
    throws(() => validateDescriptor(changed), 'invalid_namespace');
  }
});
test('SDK and validator reject accessor data without running the getter', () => {
  const { descriptor } = fixture(); let calls = 0;
  const input = clone(descriptor); Object.defineProperty(input.app, 'title', { enumerable: true, get() { calls++; return 'unsafe'; } });
  throws(() => validateDescriptor(input), 'invalid_json'); throws(() => createDescriptor(input), 'invalid_json'); assert.equal(calls, 0);
});
test('host registry copies configuration and cannot change between plan and readiness', () => {
  const { descriptor, hostConfig, host } = fixture(); const pending = beginAdmission(host, descriptor, 'fixture.request');
  hostConfig.context.ownerId = 'foreign.owner'; hostConfig.profiles[0].digest = '0'.repeat(64);
  const ready = confirmFeedback(host, pending, createFeedbackReceipt(host, pending, receiptInput));
  assert.equal(ready.plan.ownerId, 'fixture.owner'); assert.equal(ready.plan.feedbackIntent.captureProfile.digest, 'd'.repeat(64));
});
test('capability semantic pin survives prose/docs changes but changes for effects/schema/recipients', () => {
  const { descriptor } = fixture(); const cap = clone(descriptor.capabilities[0]);
  cap.title = 'New prose'; cap.description = 'Metadata only'; assert.equal(capabilityDigest(cap), descriptor.capabilities[0].digest);
  for (const mutate of [
    c => { c.effects = ['delete']; },
    c => { c.inputSchema.properties.title.maxLength = 20; },
    c => { c.recipients[0].digest = '9'.repeat(64); },
  ]) { const changed = clone(cap); mutate(changed); assert.notEqual(capabilityDigest(changed), cap.digest); }
});
test('raw semantic corruption and rehashed escalation under a reviewed binding are both rejected', () => {
  const { descriptor, host } = fixture(); const modified = clone(descriptor); modified.capabilities[0].effects = ['delete'];
  throws(() => validateDescriptor(modified), 'capability_digest_mismatch');
  delete modified.capabilities[0].digest;
  const rehashed = createDescriptor(modified);
  throws(() => planAdmission(host, rehashed, 'fixture.request'), 'binding_contract_mismatch');
});
test('closed schemas reject authority, credential, endpoint and executable fields at every relevant level', () => {
  const { descriptor } = fixture();
  for (const [where, key] of [
    ['', 'ownerProof'], ['', 'token'], ['app', 'tenantId'], ['app', 'command'],
    ['feedback', 'ready'], ['feedback', 'endpoint'], ['reviews', 'publisherProof'],
  ]) { const d = clone(descriptor); (where ? d[where] : d)[key] = true; throws(() => validateDescriptor(d), 'closed_fields'); }
  const d = clone(descriptor); d.capabilities[0].binding.command = 'echo NO'; throws(() => validateDescriptor(d), 'closed_fields');
});
test('future wire/schema/profile revisions cannot be silently interpreted as known versions', () => {
  const { descriptor, host } = fixture(); const d = clone(descriptor); d.schema = 'soty.app-agent.v2';
  throws(() => validateDescriptor(d), 'unsupported_schema');
  d.schema = descriptor.schema; d.capabilities[0].binding.version = 2; throws(() => planAdmission(host, d, 'fixture.request'), 'binding_missing');
  d.capabilities[0].binding.version = 0; throws(() => validateDescriptor(d), 'unsupported_version');
  const j = clone(descriptor); j.capabilities[0].inputSchema.$schema = 'https://json-schema.org/draft/future/schema';
  throws(() => validateDescriptor(j), 'schema_unsupported');
});
test('host opaque handle cannot be supplied as JSON or inferred from a request Host header', () => {
  const { descriptor, hostConfig } = fixture();
  throws(() => planAdmission(hostConfig, descriptor, 'fixture.request'), 'host_context_required');
  throws(() => planAdmission({ Host: 'fixture.registry' }, descriptor, 'fixture.request'), 'host_context_required');
});
test('foreign app, tenant and environment bindings do not satisfy an otherwise valid descriptor', () => {
  const { descriptor, hostConfig } = fixture();
  const app = clone(hostConfig); app.context.scope.appId = 'foreign-app';
  throws(() => planAdmission(createAdmissionHost(app), descriptor, 'fixture.request'), 'app_scope_mismatch');
  for (const field of ['tenantId', 'environmentId']) {
    const foreign = clone(hostConfig); foreign.bindings[0].scope = { ...foreign.bindings[0].scope, [field]: 'foreign' };
    throws(() => planAdmission(createAdmissionHost(foreign), descriptor, 'fixture.request'), 'binding_scope_mismatch');
  }
});
test('source promotion is a host-reviewed pin and never silently alters the admitted source', () => {
  const { descriptor, host } = fixture(); const d = clone(descriptor); d.app.source.revision++;
  throws(() => planAdmission(host, d, 'fixture.request'), 'source_authority_mismatch');
  const mismatch = clone(descriptor); mismatch.capabilities[0].binding.digest = '0'.repeat(64);
  throws(() => planAdmission(host, mismatch, 'fixture.request'), 'binding_missing');
});
test('a missing binding or missing skill/docs pin fails closed before admission', () => {
  const { descriptor, hostConfig } = fixture();
  for (const [list, code] of [['bindings', 'binding_missing'], ['skills', 'skills_pin_missing'], ['docs', 'docs_pin_missing']]) {
    const h = clone(hostConfig); h[list] = []; throws(() => planAdmission(createAdmissionHost(h), descriptor, 'fixture.request'), code);
  }
});
test('feedback privacy and declared ready status cannot be spoofed', () => {
  const { descriptor, host } = fixture();
  const d = clone(descriptor); d.feedback.submitAudience = 'public'; throws(() => validateDescriptor(d), 'audience_privacy');
  const pending = beginAdmission(host, descriptor, 'fixture.request');
  throws(() => confirmFeedback(host, pending, { ...receiptInput, ready: true }), 'feedback_receipt_required');
  throws(() => confirmFeedback(host, clone(pending), receiptInput), 'admission_state_required');
});
test('receipt is scoped to exact intent, app, tenant, environment and current owner authority', () => {
  const { descriptor, hostConfig, host } = fixture(); const pending = beginAdmission(host, descriptor, 'fixture.request');
  const receipt = createFeedbackReceipt(host, pending, receiptInput);
  for (const [section, field] of [['scope', 'tenantId'], ['scope', 'appId'], ['scope', 'environmentId'], ['context', 'ownerId']]) {
    const h = clone(hostConfig);
    if (section === 'scope') h.context.scope[field] = 'foreign'; else h.context[field] = 'foreign';
    throws(() => confirmFeedback(createAdmissionHost(h), pending, receipt), 'authority_changed');
  }
  const other = beginAdmission(host, descriptor, 'another.request');
  throws(() => confirmFeedback(host, other, receipt), 'feedback_receipt_required');
  throws(() => confirmFeedback(host, pending, clone(receipt)), 'feedback_receipt_required');
});
test('provider/capture/retention/auth registry changes invalidate old intent before using any receipt', () => {
  const { descriptor, hostConfig, host } = fixture(); const pending = beginAdmission(host, descriptor, 'fixture.request');
  const receipt = createFeedbackReceipt(host, pending, receiptInput);
  for (const mutate of [
    h => { h.providers[0].digest = '5'.repeat(64); },
    h => { h.profiles[0].digest = '5'.repeat(64); },
    h => { h.profiles[1].digest = '5'.repeat(64); },
    h => { h.context.auth = { mode: 'linked-existing', profile: pin('fixture:auth/linked', '5') }; },
  ]) { const h = clone(hostConfig); mutate(h); throws(() => confirmFeedback(createAdmissionHost(h), pending, receipt), 'authority_changed'); }
});
test('replay preserves the exact state; changed intent or a replacement receipt conflicts', () => {
  const { descriptor, host } = fixture(); const pending = beginAdmission(host, descriptor, 'fixture.request');
  const receipt = createFeedbackReceipt(host, pending, receiptInput), ready = confirmFeedback(host, pending, receipt);
  assert.equal(replayAdmission(host, ready, descriptor, 'fixture.request'), ready);
  assert.equal(confirmFeedback(host, ready, receipt), ready);
  const d = clone(descriptor); d.app.title = 'New display name';
  throws(() => replayAdmission(host, ready, d, 'fixture.request'), 'admission_intent_conflict');
  throws(() => replayAdmission(host, ready, descriptor, 'different.request'), 'admission_intent_conflict');
  const different = createFeedbackReceipt(host, ready, { ...receiptInput, installationId: 'different.inbox' });
  throws(() => confirmFeedback(host, ready, different), 'feedback_receipt_conflict');
  throws(() => holdFeedback(host, ready), 'admission_transition_invalid');
});
test('new source request preserves inbox provisioning identity without widening capability semantics', () => {
  const { descriptor, hostConfig, host } = fixture(); const before = planAdmission(host, descriptor, 'before.request');
  const next = clone(hostConfig); next.context.authorityRevision++; next.context.source.revision++;
  next.bindings = [{ ...next.bindings[0], version: 2, source: next.context.source }];
  const d = clone(descriptor); d.app.source = next.context.source; d.capabilities[0].binding.version = 2;
  const after = planAdmission(upgradeAdmissionHost(host, next), d, 'after.request');
  assert.equal(before.feedbackIntent.provisioningKey, after.feedbackIntent.provisioningKey);
  assert.equal(d.capabilities[0].digest, descriptor.capabilities[0].digest);
  assert.equal(after.gates.agent, 'not-admitted');
});
test('registry upgrades reject same-version pin rewrites and same capability version with changed effects', () => {
  const { descriptor, hostConfig, host } = fixture(); const next = clone(hostConfig); next.context.authorityRevision++;
  next.providers[0].digest = '6'.repeat(64); throws(() => upgradeAdmissionHost(host, next), 'immutable_pin_conflict');
  const semantic = clone(hostConfig); semantic.context.authorityRevision++;
  const cap = clone(descriptor.capabilities[0]); cap.effects = ['delete'];
  semantic.bindings = [{ ...semantic.bindings[0], id: 'fixture.board:binding/other', capability: { ...semantic.bindings[0].capability, digest: capabilityDigest(cap) } }];
  throws(() => upgradeAdmissionHost(host, semantic), 'immutable_capability_conflict');
});
test('successful authority upgrade retires the old host even when a stale callback retains it', () => {
  const { descriptor, hostConfig, host } = fixture();
  const pending = beginAdmission(host, descriptor, 'fixture.request'), receipt = createFeedbackReceipt(host, pending, receiptInput);
  const next = clone(hostConfig); next.context.authorityRevision++; next.context.ownerId = 'new.owner';
  const upgraded = upgradeAdmissionHost(host, next);
  throws(() => confirmFeedback(host, pending, receipt), 'authority_changed');
  throws(() => beginAdmission(host, descriptor, 'another.request'), 'authority_changed');
  throws(() => replayAdmission(host, pending, descriptor, 'fixture.request'), 'authority_changed');
  throws(() => confirmFeedback(upgraded, pending, receipt), 'authority_changed');
  assert.equal(beginAdmission(upgraded, descriptor, 'new.request').plan.ownerId, 'new.owner');
});
test('failed immutable registry upgrade does not retire the current host', () => {
  const { descriptor, hostConfig, host } = fixture(); const next = clone(hostConfig);
  next.context.authorityRevision++; next.providers[0].digest = '6'.repeat(64);
  throws(() => upgradeAdmissionHost(host, next), 'immutable_pin_conflict');
  assert.equal(beginAdmission(host, descriptor, 'fixture.request').status, 'pending-feedback');
});
test('revoked registry pin cannot be rewritten by remove-then-readd across generations', () => {
  const { hostConfig, host } = fixture();
  const removed = clone(hostConfig); removed.context.authorityRevision++; removed.providers = [];
  const next = upgradeAdmissionHost(host, removed);
  const readded = clone(hostConfig); readded.context.authorityRevision += 2; readded.providers[0].digest = '5'.repeat(64);
  throws(() => upgradeAdmissionHost(next, readded), 'immutable_pin_conflict');
});
test('competing ready callbacks cannot fork a request; lost-ACK replay returns the original head', () => {
  const { descriptor, host } = fixture(); const pending = beginAdmission(host, descriptor, 'fixture.request');
  const a = createFeedbackReceipt(host, pending, receiptInput);
  const b = createFeedbackReceipt(host, pending, { ...receiptInput, installationId: 'second.inbox' });
  const ready = confirmFeedback(host, pending, a);
  throws(() => confirmFeedback(host, pending, b), 'feedback_receipt_conflict');
  assert.equal(confirmFeedback(host, pending, a), ready);
  assert.equal(replayAdmission(host, pending, descriptor, 'fixture.request'), ready);
  assert.equal(beginAdmission(host, descriptor, 'fixture.request'), ready);
  assert.equal(ready.feedbackReceipt.installationId, 'fixture.inbox');
  const changed = clone(descriptor); changed.app.title = 'Changed';
  throws(() => beginAdmission(host, changed, 'fixture.request'), 'admission_intent_conflict');
});
test('hold advances the CAS head; an older receipt cannot release it without current reconciliation', () => {
  const { descriptor, host } = fixture(); const pending = beginAdmission(host, descriptor, 'fixture.request');
  const old = createFeedbackReceipt(host, pending, receiptInput), held = holdFeedback(host, pending);
  throws(() => confirmFeedback(host, pending, old), 'admission_stale');
  throws(() => createFeedbackReceipt(host, pending, receiptInput), 'admission_stale');
  assert.equal(replayAdmission(host, pending, descriptor, 'fixture.request'), held);
  const current = createFeedbackReceipt(host, held, receiptInput);
  assert.equal(confirmFeedback(host, held, current).status, 'fixture-ready');
});
test('public review read needs no publisher placement proof, but requires a public subject projection', () => {
  const { descriptor, hostConfig, host } = fixture(); assert.equal(hostConfig.placements.length, 0);
  assert.equal(planAdmission(host, descriptor, 'fixture.request').reviews.mode, 'public-read');
  const h = clone(hostConfig); h.publicSubjects = []; throws(() => planAdmission(createAdmissionHost(h), descriptor, 'fixture.request'), 'review_not_public');
  const managedField = clone(descriptor); managedField.reviews.rights = ['collect']; throws(() => validateDescriptor(managedField), 'closed_fields');
});
test('managed review rights are separate from public display and cannot borrow another tenant grant', () => {
  const { descriptor, hostConfig } = fixture(); const d = clone(descriptor), h = clone(hostConfig);
  const placement = { ...pin('fixture:placement/board', '9'), subjectId: 'fixture:subject/private', rights: ['display'] };
  d.reviews = { mode: 'managed', provider: descriptor.reviews.provider, placements: [placement] };
  throws(() => planAdmission(createAdmissionHost(h), d, 'fixture.request'), 'review_binding_missing');
  h.placements = [{ ...clone(placement), scope: clone(h.context.scope), provider: clone(d.reviews.provider) }];
  assert.equal(planAdmission(createAdmissionHost(h), d, 'fixture.request').reviews.mode, 'managed');
  d.reviews.placements[0].rights.push('collect');
  throws(() => planAdmission(createAdmissionHost(h), d, 'fixture.request'), 'review_scope_mismatch');
  d.reviews.placements[0].rights = ['display']; h.placements[0].scope = { ...h.context.scope, tenantId: 'foreign' };
  throws(() => planAdmission(createAdmissionHost(h), d, 'fixture.request'), 'review_scope_mismatch');
});
test('managed placement is pinned to its exact reviewed provider, not any approved review provider', () => {
  const { descriptor, hostConfig } = fixture(); const d = clone(descriptor), h = clone(hostConfig);
  const a = clone(descriptor.reviews.provider), b = pin('fixture:reviews-second', '6');
  const placement = { ...pin('fixture:placement/board', '9'), subjectId: 'fixture:subject/private', rights: ['display', 'collect'] };
  h.providers.push({ ...b, kind: 'reviews', publicRead: true });
  h.placements = [{ ...clone(placement), scope: clone(h.context.scope), provider: a }];
  d.reviews = { mode: 'managed', provider: a, placements: [placement] };
  const host = createAdmissionHost(h);
  assert.equal(planAdmission(host, d, 'provider-a.request').reviews.provider.id, a.id);
  d.reviews.provider = b;
  throws(() => planAdmission(host, d, 'provider-b.request'), 'review_provider_mismatch');
  const missing = clone(h); delete missing.placements[0].provider;
  throws(() => createAdmissionHost(missing), 'closed_fields');
  const wrongKind = clone(h); wrongKind.placements[0].provider = clone(descriptor.feedback.provider);
  throws(() => createAdmissionHost(wrongKind), 'review_provider_missing');
  const pinMismatch = clone(h); pinMismatch.placements[0].provider.digest = '0'.repeat(64);
  throws(() => createAdmissionHost(pinMismatch), 'review_provider_missing');
});
test('human auth declaration is pinned metadata and not a proof that shared login works', () => {
  const { descriptor, hostConfig } = fixture(); const d = clone(descriptor), h = clone(hostConfig);
  const profile = pin('fixture:auth/shared', '6'); d.app.auth = h.context.auth = { mode: 'shared-soty', profile };
  throws(() => planAdmission(createAdmissionHost(h), d, 'fixture.request'), 'auth_profile_missing');
  h.profiles.push({ ...profile, kind: 'auth' });
  assert.equal(planAdmission(createAdmissionHost(h), d, 'fixture.request').productionAdmission, false);
});
test('bounded parser rejects duplicate and escaped duplicate keys, unsafe JSON and nesting', () => {
  for (const input of ['{"schema":1,"schema":2}', '{"schema":1,"sc\\u0068ema":2}']) throws(() => parseContractJson(input), 'duplicate_key');
  for (const input of ['9007199254740992', '-0']) throws(() => parseContractJson(input), 'unsafe_number');
  for (const input of ['1.0', '1e3', '{"constructor":0}', '{"x":1,}', '[1,]']) throws(() => parseContractJson(input), 'invalid_json');
  throws(() => parseContractJson(Buffer.from([0xff])), 'invalid_utf8');
  throws(() => parseContractJson(' '.repeat(LIMITS.bytes + 1)), 'input_limit');
  throws(() => parseContractJson('['.repeat(20) + '0' + ']'.repeat(20)), 'input_limit');
  throws(() => parseContractJson('"' + '\\ud800' + '"'), 'invalid_string');
});
test('canonical data profile is explicit and rejects secrets without reflecting their value', () => {
  assert.equal(canonicalContractJson({ z: '✨', a: 1 }), '{"a":1,"z":"✨"}');
  throws(() => parseContractJson('{"x":"Bearer SENSITIVE_SENTINEL"}'), 'secret_not_allowed');
  const { descriptor } = fixture(); const d = clone(descriptor); d.capabilities[0].inputSchema.$ref = 'https://unsafe.invalid/schema';
  throws(() => createDescriptor(d), 'closed_fields');
});
test('independent Python author creates files, computes digests and traverses actual Node CLI conformance', () => temporary(path => {
  const script = fileURLToPath(new URL('../examples/descriptor.py', import.meta.url));
  const result = spawnSync('python', [script, path, process.execPath], { encoding: 'utf8', windowsHide: true, timeout: 10000 });
  assert.equal(result.status, 0, result.stderr); const summary = JSON.parse(result.stdout);
  const descriptor = validateDescriptor(parseContractJson(readFileSync(join(path, '.soty', 'agent.json'))));
  assert.equal(summary.descriptorDigest, contractDigest(descriptor));
  assert.equal(summary.capabilityDigest, descriptor.capabilities[0].digest);
  assert.deepEqual(descriptor, fixture().descriptor);
  assert.equal(JSON.parse(readFileSync(join(path, 'admission-proposal.json'), 'utf8')).plan.productionAdmission, false);
}));
test('CLI failures disclose only safe error codes, not the input or paths', () => temporary(path => {
  const file = join(path, 'invalid.json'); writeFileSync(file, '{"endpoint":"SENSITIVE_SENTINEL","schema":"future"}');
  const result = spawnSync(process.execPath, [cli, 'validate', file], { encoding: 'utf8', windowsHide: true });
  assert.equal(result.status, 1); assert.equal(result.stdout, '');
  const error = JSON.parse(result.stderr);
  assert.equal(error.ok, false); assert.equal(error.error, 'closed_fields'); assert.equal(error.stage, 'validate');
  assert.equal(typeof error.message, 'string'); assert.equal(typeof error.hint, 'string');
  assert(!result.stderr.includes('SENSITIVE_SENTINEL')); assert(!result.stderr.includes(path));
}));
test('title-only draft materializes a private UI-only app entirely from one explicit trusted profile', () => {
  const draft = createAuthorDraft({ title: 'Простой проект' }), config = authorFixtureConfiguration();
  const host = createAdmissionHost(config), descriptor = materializeAuthorDraft(host, draft);
  assert.deepEqual(draft, { schema: 'soty.app-author-draft.v1', title: 'Простой проект' });
  assert.deepEqual(descriptor.app, { id: config.context.scope.appId, namespace: config.context.namespace,
    title: draft.title, visibility: config.context.visibility, source: config.context.source, auth: config.context.auth });
  assert.deepEqual(descriptor.feedback, config.authorProfile.feedback);
  assert.equal(descriptor.feedback.submitAudience, 'members');
  assert.equal(descriptor.app.visibility, 'private');
  assert.deepEqual(descriptor.capabilities, []); assert.deepEqual(descriptor.skills, []); assert.deepEqual(descriptor.docs, []);
  assert.deepEqual(descriptor.reviews, { mode: 'disabled' });
  const plan = planAdmission(host, descriptor, 'author.request');
  assert.deepEqual(plan.gates, { ui: 'pending-feedback', agent: 'not-admitted', local: 'not-admitted' });
  assert.equal(plan.productionAdmission, false);
});
test('author cannot choose host authority, provider, visibility, capabilities or readiness', () => {
  for (const field of ['scope', 'appId', 'tenantId', 'source', 'auth', 'visibility', 'feedback', 'profile', 'ownerId', 'ready', 'capabilities', 'effects']) {
    throws(() => createAuthorDraft({ title: 'Simple', [field]: 'SENTINEL' }), 'closed_fields');
  }
  throws(() => createAuthorDraft({ schema: 'soty.app-author-draft.v2', title: 'Simple' }), 'unsupported_schema');
  throws(() => createAuthorDraft({ title: '  ' }), 'invalid_title');
});
test('author and advanced descriptor reject whitespace-only titles without trimming nonempty metadata', () => {
  const { descriptor } = fixture();
  for (const title of ['', ' ', '\t\n']) {
    throws(() => createAuthorDraft({ title }), 'invalid_title');
    const advanced = clone(descriptor); advanced.app.title = title;
    throws(() => createDescriptor(advanced), 'invalid_title');
    throws(() => validateDescriptor(advanced), 'invalid_title');
  }
  const title = '  Мой проект  ';
  assert.equal(createAuthorDraft({ title }).title, title);
  const advanced = clone(descriptor); advanced.app.title = title;
  assert.equal(createDescriptor(advanced).app.title, title);
});
test('materialization never chooses a first provider or accepts an unregistered default pin', () => {
  const { hostConfig } = fixture(); const draft = createAuthorDraft({ title: 'Simple' });
  throws(() => materializeAuthorDraft(createAdmissionHost(hostConfig), draft), 'author_profile_required');
  const config = authorFixtureConfiguration(); config.providers.reverse();
  assert.deepEqual(materializeAuthorDraft(createAdmissionHost(config), draft).feedback.provider, config.authorProfile.feedback.provider);
  const wrong = clone(config); wrong.authorProfile.feedback.provider = clone(wrong.providers[0]);
  delete wrong.authorProfile.feedback.provider.kind; delete wrong.authorProfile.feedback.provider.publicRead;
  throws(() => createAdmissionHost(wrong), 'feedback_provider_missing');
  const missing = clone(config); missing.authorProfile.feedback.captureProfile.digest = '0'.repeat(64);
  throws(() => createAdmissionHost(missing), 'feedback_profile_missing');
  const widened = clone(config); widened.authorProfile.feedback.submitAudience = 'public';
  throws(() => createAdmissionHost(widened), 'audience_privacy');
});
test('named author profile cannot be rewritten at the same version or via remove and re-add', () => {
  const config = authorFixtureConfiguration(), host = createAdmissionHost(config);
  const changed = clone(config); changed.context.authorityRevision++;
  // Re-use a valid profile of the same kind to isolate immutable history, not shape rejection.
  changed.authorProfile.feedback.retentionProfile = { ...config.authorProfile.feedback.retentionProfile, version: 2 };
  changed.profiles.push({ ...changed.authorProfile.feedback.retentionProfile, kind: 'retention' });
  throws(() => upgradeAdmissionHost(host, changed), 'immutable_pin_conflict');
  const removed = clone(config); removed.context.authorityRevision++; delete removed.authorProfile;
  const next = upgradeAdmissionHost(host, removed);
  changed.context.authorityRevision++;
  throws(() => upgradeAdmissionHost(next, changed), 'immutable_pin_conflict');
  throws(() => materializeAuthorDraft(host, { title: 'Simple' }), 'authority_changed');
  throws(() => materializeAuthorDraft(next, { title: 'Simple' }), 'author_profile_required');
});
test('changed author profile generation retires old receipts without enabling advanced bindings', () => {
  const config = authorFixtureConfiguration(), host = createAdmissionHost(config);
  const descriptor = materializeAuthorDraft(host, { title: 'Simple' }), pending = beginAdmission(host, descriptor, 'old.request');
  const receipt = createFeedbackReceipt(host, pending, receiptInput);
  const next = clone(config); next.context.authorityRevision++; next.authorProfile.version++;
  next.authorProfile.digest = '6'.repeat(64);
  const upgraded = upgradeAdmissionHost(host, next);
  throws(() => confirmFeedback(host, pending, receipt), 'authority_changed');
  const materialized = materializeAuthorDraft(upgraded, { title: 'Simple' });
  assert.equal(materialized.capabilities.length, 0);
  const advanced = clone(fixture().descriptor); advanced.capabilities[0].effects = ['delete']; delete advanced.capabilities[0].digest;
  throws(() => planAdmission(upgraded, createDescriptor(advanced), 'advanced.request'), 'binding_contract_mismatch');
});
test('SDK fills absent schema/digest only and rejects explicit incompatible metadata', () => {
  const { descriptor } = fixture(); const missing = clone(descriptor); delete missing.schema; delete missing.capabilities[0].digest;
  assert.deepEqual(createDescriptor(missing), descriptor);
  const future = clone(descriptor); future.schema = 'soty.app-agent.v99';
  throws(() => createDescriptor(future), 'unsupported_schema');
  const stale = clone(descriptor); stale.capabilities[0].effects = ['delete'];
  throws(() => createDescriptor(stale), 'capability_digest_mismatch');
  const nullSchema = clone(descriptor); nullSchema.schema = null;
  throws(() => createDescriptor(nullSchema), 'unsupported_schema');
  const nullCaps = clone(descriptor); nullCaps.capabilities = null;
  throws(() => createDescriptor(nullCaps), 'invalid_descriptor');
});
test('bounded admission heads reject new requests but preserve replay, conflicts and receipts at capacity', () => {
  const { descriptor, host } = fixture();
  const first = beginAdmission(host, descriptor, 'capacity.0');
  const receipt = createFeedbackReceipt(host, first, receiptInput), ready = confirmFeedback(host, first, receipt);
  for (let i = 1; i < LIMITS.admissionRequests; i++) beginAdmission(host, descriptor, 'capacity.' + i);
  throws(() => beginAdmission(host, descriptor, 'overflow.request'), 'admission_request_limit');
  assert.equal(beginAdmission(host, descriptor, 'capacity.0'), ready);
  assert.equal(replayAdmission(host, first, descriptor, 'capacity.0'), ready);
  assert.equal(confirmFeedback(host, first, receipt), ready);
  const changed = clone(descriptor); changed.app.title = 'Different';
  throws(() => beginAdmission(host, changed, 'capacity.0'), 'admission_intent_conflict');
  assert.equal(planAdmission(host, descriptor, 'plan-only.request').productionAdmission, false);
});
test('fixture disposal releases host maps, rejects callbacks and cannot reset a living request history', () => {
  const { descriptor, host } = fixture(); const pending = beginAdmission(host, descriptor, 'close.request');
  const receipt = createFeedbackReceipt(host, pending, receiptInput);
  assert.equal(closeAdmissionHost(host), true); assert.equal(closeAdmissionHost(host), false);
  throws(() => confirmFeedback(host, pending, receipt), 'host_closed');
  throws(() => beginAdmission(host, descriptor, 'close.request'), 'host_closed');
  throws(() => planAdmission(host, descriptor, 'close.request'), 'host_closed');
  throws(() => closeAdmissionHost({}), 'host_context_required');
});
test('compiled registry remains immutable and bounded with near-limit public subject pins', () => {
  const { descriptor, hostConfig } = fixture(); const config = clone(hostConfig);
  for (let i = 1; i < LIMITS.registryEntries; i++) config.publicSubjects.push({
    ...pin('fixture:subject/n' + i, '8'), provider: clone(descriptor.reviews.provider)
  });
  const host = createAdmissionHost(config);
  config.publicSubjects[0].provider.digest = '0'.repeat(64);
  const before = planAdmission(host, descriptor, 'registry.request');
  const after = planAdmission(host, descriptor, 'registry.request');
  assert.equal(before.authorityDigest, after.authorityDigest);
  assert.equal(before.intentDigest, after.intentDigest);
  assert(Buffer.byteLength(JSON.stringify(before)) < LIMITS.bytes);
  const overflow = clone(hostConfig);
  overflow.publicSubjects = Array.from({ length: LIMITS.registryEntries + 1 }, (_, i) => ({
    ...pin('fixture:subject/n' + i, '8'), provider: clone(descriptor.reviews.provider)
  }));
  throws(() => createAdmissionHost(overflow), 'input_limit');
});
test('simple example and CLI onboarding work without any author-supplied pins or execution', () => temporary(path => {
  const folder = join(path, 'author-example');
  const summary = runAuthorExample(folder);
  assert.equal(summary.capabilities, 0); assert.equal(summary.providerCalls, 0); assert.equal(summary.executedHandlers, 0);
  const help = spawnSync(process.execPath, [cli, '--help'], { encoding: 'utf8', windowsHide: true });
  assert.equal(help.status, 0); assert.equal(JSON.parse(help.stdout).productionAdmission, false);
  const draft = spawnSync(process.execPath, [cli, 'draft', '--title', 'Мой проект'], { encoding: 'utf8', windowsHide: true });
  assert.equal(draft.status, 0); assert.deepEqual(JSON.parse(draft.stdout), { schema: 'soty.app-author-draft.v1', title: 'Мой проект' });
  const output = spawnSync(process.execPath, [cli, 'materialize', join(folder, '.soty', 'author.json'), '--fixture-host', join(folder, 'fixture-host.json')], { encoding: 'utf8', windowsHide: true });
  assert.equal(output.status, 0, output.stderr);
  const result = JSON.parse(output.stdout);
  assert.equal(result.trustedInput, 'local-fixture-only'); assert.equal(result.productionAdmission, false);
  assert.equal(result.descriptor.capabilities.length, 0);
}));
test('CLI diagnostic codes/stages/hints remain useful without reflecting filenames or payload', () => temporary(path => {
  const bad = join(path, 'SENTINEL-invalid.json'); writeFileSync(bad, '{"title":"SAFE","extra":"SENTINEL"}');
  const malformed = join(path, 'SENTINEL-malformed.json'); writeFileSync(malformed, '{"SENTINEL":');
  const future = join(path, 'SENTINEL-future.json'); writeFileSync(future, JSON.stringify({ ...fixture().descriptor, schema: 'future' }));
  for (const [args, code, stage] of [
    [['validate', join(path, 'SENTINEL-missing.json')], 'input_not_found', 'read'],
    [['validate', malformed], 'invalid_json', 'parse'],
    [['validate', future], 'unsupported_schema', 'validate'],
    [['materialize', bad, '--fixture-host', bad], 'closed_fields', 'validate'],
    [['wrong', 'SENTINEL'], 'cli_arguments', 'arguments']
  ]) {
    const result = spawnSync(process.execPath, [cli, ...args], { encoding: 'utf8', windowsHide: true });
    assert.equal(result.status, 1); assert.equal(result.stdout, '');
    const safe = JSON.parse(result.stderr); assert.equal(safe.error, code); assert.equal(safe.stage, stage);
    assert(safe.message && safe.hint); assert(!result.stderr.includes('SENTINEL')); assert(!result.stderr.includes(path));
  }
}));
test('legacy app, Notes semantic wire and canonical Identity manifest remain unchanged', () => {
  assert.deepEqual(normalizeManifest({ schema: 'soty.local-app.v1', name: 'Legacy', port: 3000, entryPath: '/' }), { schema: 'soty.local-app.v1', name: 'Legacy', port: 3000, entryPath: '/' });
  assert.throws(() => normalizeManifest({ ...fixture().descriptor, schema: 'soty.local-app.v1' }));
  assert.equal(createCatalog().get('notes.createDraft', 1).digest, '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204');
  assert.equal(createHash('sha256').update(readFileSync(join(repo, 'contracts/identity/v1/data/contracts/identity/v1/conformance-manifest.json'))).digest('hex'), '5570120a7c5c1262f457625ef646c3652b1d6de323edb88926a8fa97d67ba786');
});
