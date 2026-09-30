import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createNotesService } from '../../../notes/server/index.mjs';
import { createCapabilitiesService, BUILTIN_CAPABILITIES } from '../../server/index.mjs';

export const PROJECT = 'native-effect-test';
export const OWNER = Object.freeze({ accountId: 'account_native_owner', deviceId: 'device_native_owner' });
export const AUDIENCE = 'https://native-effect.test/api';
export const code = expected => error => error?.code === expected;

/** Real Notes/Caps SQLite and ACL. The B2a host fence is explicitly a sync stub;
 * separate B2b/host acceptance must prove the actual Connect writer fence. */
export function nativeFixture(t, initial = {}) {
  const parent = realpathSync(tmpdir()), directory = mkdtempSync(path.join(parent, 'soty-native-effect-'));
  const notesFile = path.join(directory, 'notes.sqlite'), capsFile = path.join(directory, 'caps.sqlite');
  let time = 1000, active = true, notes, caps, native, options = initial;
  const external = [];
  const captured = [];
  function open() {
    notes = createNotesService({ databasePath: notesFile, projectId: PROJECT, clock: () => time,
      allowNativeMigration: options.notesMigration ?? true, limits: options.notesLimits,
      verifyNativeContext(context, mode) {
        captured.push({ context, mode });
        options.onVerify?.(context, mode);
        return native.verifyContext(context, mode);
      } });
    const port = Object.freeze({ ...notes.native, ...options.port });
    caps = createCapabilitiesService({ databasePath: capsFile, projectId: PROJECT, clock: () => time,
      actorActive: actor => active && actor.accountId === OWNER.accountId && actor.deviceId === OWNER.deviceId,
      allowNativeMigration: options.capsMigration ?? true,
      catalog: BUILTIN_CAPABILITIES.map(entry => ({ ...entry, executionEnabled: options.enabled ?? true })),
      limits: { invocations: options.invocationLimits ?? {}, nativeNotes: options.nativeLimits ?? {} },
      nativeNotes: { notes: port, withAuthorityFence: options.fence ?? (action => action()) } });
    native = caps.nativeNotes;
  }
  open();
  t.after(() => {
    for (const db of external) db.close();
    caps.close(); notes.close();
    const actual = realpathSync(directory);
    assert.equal(path.dirname(actual), parent); assert.match(path.basename(actual), /^soty-native-effect-/u);
    rmSync(actual, { recursive: true });
  });
  const call = (op, args) => caps.execute({ op: `access.${op}`, actor: OWNER, args: { expectedAccountId: OWNER.accountId, ...args } });
  function issue({ expiresAt = 100000, budget = 20, credentialExpiresAt = expiresAt } = {}) {
    const principal = call('principals.create', { label: 'Native test' }).principal;
    const grant = call('grants.issue', { principalId: principal.id, capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }],
      resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'], expiresAt,
      allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: budget } }).grant;
    const credential = call('credentials.issue', { grantId: grant.id, audience: AUDIENCE, expiresAt: credentialExpiresAt });
    return { principal, grant, credential, actor: caps.authenticateCredential({ token: credential.token, audience: AUDIENCE }) };
  }
  function sql(file) { const db = new DatabaseSync(file); external.push(db); return db; }
  return { get notes() { return notes; }, get caps() { return caps; }, get native() { return native; }, notesFile, capsFile,
    captured, issue, call, sql, time: value => { if (value !== undefined) time = value; return time; }, revokeHost: () => { active = false; },
    reopen(next = {}) { caps.close(); notes.close(); options = { ...options, ...next }; open(); },
    actor: token => caps.authenticateCredential({ token, audience: AUDIENCE }),
    note(op, args) { return notes.execute({ op: `notes.${op}`, actor: OWNER, args: { expectedAccountId: OWNER.accountId, ...args } }); },
    create(actor, input = { title: 'Native 😀', body: 'Original text' }, key = 'native_request_0001') {
      const { invocation } = native.admit({ actor, idempotencyKey: key, input });
      native.beginAttempt({ invocationId: invocation.invocationId });
      return native.execute({ invocationId: invocation.invocationId });
    },
  };
}
