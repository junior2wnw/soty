import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join, resolve, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { openMemoryPartition, MemoryError } from '../index.mjs';

export const personal = (accountId = 'alice', projectId = null) => ({ issuer: 'connect:soty', accountId,
  audienceKind: 'personal', audienceId: accountId, projectId });
export const community = (accountId = 'alice') => ({ issuer: 'connect:soty', accountId,
  audienceKind: 'community', audienceId: 'family', projectId: null });
export const record = (id = 'record_a', text = 'synthetic lighthouse private knowledge', changes = {}) => ({
  id, type: 'fact', text, source: 'synthetic:fixture', freshUntil: 50000, retainUntil: 100000, ...changes });
export const error = code => value => value instanceof MemoryError && value.code === code && value.message === code;
export const tick = () => new Promise(resolveTick => setImmediate(resolveTick));
export function deferred() {
  let resolvePromise, rejectPromise;
  const promise = new Promise((resolve, reject) => { resolvePromise = resolve; rejectPromise = reject; });
  return { promise, resolve: resolvePromise, reject: rejectPromise };
}
export function fixture(t, { embedding, limits, disk = false, scope = personal() } = {}) {
  const contexts = new WeakMap(), floors = new Map(), admissions = [], advances = [], stores = [];
  const base = resolve(tmpdir()), directory = mkdtempSync(join(base, 'soty-personal-memory-'));
  let clock = 1000, onVerify = null, onAdvance = null;
  function context(bound = scope, changes = {}) {
    const token = Object.freeze({});
    contexts.set(token, { scope: JSON.stringify(bound), leaseId: 'lease_a', epoch: 1, expiresAt: 200000, active: true, ...changes });
    return token;
  }
  const owner = context();
  const verifyContext = request => {
    admissions.push(request.operation); onVerify?.(request);
    const state = contexts.get(request.context);
    if (!state?.active || state.scope !== JSON.stringify(request.scope)) return false;
    return { leaseId: state.leaseId, epoch: state.epoch, expiresAt: state.expiresAt };
  };
  const readRestoreFloor = ({ partitionId }) => floors.get(partitionId) ?? 0;
  const advanceRestoreFloor = request => {
    advances.push(request);
    assert.equal(readRestoreFloor(request), request.expectedFloor);
    const next = request.expectedFloor + 1; floors.set(request.partitionId, next);
    onAdvance?.(request); return next;
  };
  const options = { scope, context: owner, verifyContext, readRestoreFloor, advanceRestoreFloor, clock: () => clock, embedding, limits };
  const databasePath = disk ? join(directory, 'memory.sqlite') : ':memory:';
  function open(overrides = {}) {
    const store = openMemoryPartition({ databasePath, ...options, ...overrides }); stores.push(store); return store;
  }
  const store = open();
  t.after(() => {
    stores.forEach(value => value.close());
    assert.equal(dirname(resolve(directory)), base); assert.ok(directory.startsWith(join(base, 'soty-personal-memory-')));
    rmSync(directory, { recursive: true, force: true });
  });
  return { store, owner, context, state: token => contexts.get(token), floors, admissions, advances, open,
    directory, databasePath, options, setClock: value => { clock = value; },
    onVerify: callback => { onVerify = callback; }, onAdvance: callback => { onAdvance = callback; } };
}
