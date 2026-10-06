import test from 'node:test';
import assert from 'node:assert/strict';
import { createCatalog } from '../server/catalog.mjs';
import { createTrustedAdapterRegistry, createNativeNotesAdapter, NOTES_CREATE_DRAFT_CONTRACT } from '../server/adapters.mjs';
import { connectedFixture, INPUT, code } from './support/native-connected.mjs';

function trustedPorts() {
  return { marker: undefined, readiness() { return { ready: true }; }, admit(args) { return args; },
    get(args) { return args; }, beginAttempt(args) { return args; }, execute(args) { return args; },
    reconcile(args) { return args; }, reconcilePage(args) { return args; }, verifyContext(value) { return value; } };
}
function entry() { const adapter = trustedPorts(); delete adapter.marker; return { contract: { ...NOTES_CREATE_DRAFT_CONTRACT }, adapter }; }

test('registry preserves exact legacy contract and denies unknown ID, version, digest and executable JSON', () => {
  const item = entry(), registry = createTrustedAdapterRegistry([item]);
  assert.deepEqual(registry.listContracts(), [NOTES_CREATE_DRAFT_CONTRACT]);
  assert.equal(createCatalog().get('notes.createDraft', 1).digest, NOTES_CREATE_DRAFT_CONTRACT.digest);
  for (const changed of [{ capabilityId: 'notes.other' }, { version: 2 }, { digest: '0'.repeat(64) }]) {
    assert.throws(() => registry.get({ ...NOTES_CREATE_DRAFT_CONTRACT, ...changed }), code('adapter_not_registered'));
  }
  assert.throws(() => createTrustedAdapterRegistry([{ contract: NOTES_CREATE_DRAFT_CONTRACT, adapter: { command: 'do not execute' } }]), code('adapter_ports_invalid'));
  assert.throws(() => createTrustedAdapterRegistry([item, { ...entry(), contract: { ...NOTES_CREATE_DRAFT_CONTRACT, digest: '0'.repeat(64) } }]), code('adapter_version_conflict'));
});

test('registry captures trusted functions and contract fields without invoking accessors', () => {
  const item = entry(); let calls = 0;
  const registry = createTrustedAdapterRegistry([item]), accepted = registry.get(NOTES_CREATE_DRAFT_CONTRACT);
  item.adapter.execute = () => { throw Error('replaced'); }; item.contract.digest = '0'.repeat(64);
  const actor = Object.freeze({ synthetic: true }); assert.equal(accepted.adapter.execute(actor), actor);
  assert.throws(() => { accepted.contract.version = 2; }, TypeError);
  const accessor = entry(); Object.defineProperty(accessor.adapter, 'execute', { enumerable: true, get() { calls++; return () => {}; } });
  assert.throws(() => createTrustedAdapterRegistry([accessor]), code('adapter_ports_invalid')); assert.equal(calls, 0);
  assert.throws(() => createNativeNotesAdapter(undefined), code('adapter_ports_invalid'));
});

test('actual registered Notes adapter preserves one effect, exact retry, proof and budget', async t => {
  const f = await connectedFixture(t), caller = await f.issue({ budget: 1 });
  const registered = f.caps.adapters.get(NOTES_CREATE_DRAFT_CONTRACT);
  assert.equal(f.caps.nativeNotes, registered.adapter);
  assert.equal(registered.adapter.readiness().ready, true);
  const first = registered.adapter.admit({ actor: caller.actor, input: INPUT, idempotencyKey: 'adapter-real-create' });
  registered.adapter.beginAttempt({ invocationId: first.invocation.invocationId });
  assert.equal(registered.adapter.execute({ invocationId: first.invocation.invocationId }).outcome, 'committed');
  const repeated = registered.adapter.admit({ actor: caller.actor, input: INPUT, idempotencyKey: 'adapter-real-create' });
  assert.equal(repeated.reused, true); assert.equal(repeated.invocation.invocationId, first.invocation.invocationId);
  assert.equal(registered.adapter.reconcile({ invocationId: first.invocation.invocationId }).outcome, 'committed');
  assert.equal(f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM notes').get().n), 1);
  assert.deepEqual(f.sql('caps', db => ({ ...db.prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() })), { reserved_amount: 0, spent_amount: 1 });
});

test('registered Notes adapter retains revocation checks before effect dispatch', async t => {
  const f = await connectedFixture(t), caller = await f.issue(), adapter = f.caps.adapters.get(NOTES_CREATE_DRAFT_CONTRACT).adapter;
  const admitted = adapter.admit({ actor: caller.actor, input: INPUT, idempotencyKey: 'adapter-revoke' });
  await f.access('grants.revoke', { grantId: caller.grant.id });
  assert.throws(() => adapter.beginAttempt({ invocationId: admitted.invocation.invocationId }), code('access_denied'));
  assert.equal(f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM notes').get().n), 0);
});
