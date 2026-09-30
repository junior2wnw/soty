import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import path from 'node:path';
import { connectedFixture, INPUT, code, identity, ORIGIN } from './support/native-connected.mjs';

test('lowered native principal/account/global/rate/ledger bounds serialize admission while exact replay bypasses new quotas', async t => {
  const f = await connectedFixture(t), first = await f.issue(), sibling = await f.issue();
  const otherOwner = identity('Other account');
  const otherAccount = await f.call('bootstrap', { label: otherOwner.label, encryptionPublicJwk: otherOwner.encryptionPublicJwk }, otherOwner);
  const foreign = await f.issue({ signer: otherOwner, accountId: otherAccount.accountId });
  f.time(2000);
  const key = 'quota_first_key', invocationId = f.native.admit({ actor: first.actor, idempotencyKey: key, input: INPUT }).invocation.invocationId;
  for (const [limits, who, expected] of [
    [{ nonterminalPerPrincipal: 1 }, first, 'native_admission_limit'],
    [{ nonterminalPerAccount: 1 }, sibling, 'native_admission_limit'],
    [{ nonterminalTotal: 1 }, foreign, 'native_admission_limit'],
  ]) {
    f.restart({ nativeLimits: limits });
    assert.throws(() => f.native.admit({ actor: f.actor(who.credential.token), idempotencyKey: 'quota_new_key', input: INPUT }), code(expected));
    assert.equal(f.native.admit({ actor: f.actor(first.credential.token), idempotencyKey: key, input: INPUT }).reused, true);
  }
  f.caps.invocations.requestCancel({ actor: f.actor(first.credential.token), invocationId });
  assert.equal(f.native.reconcile({ invocationId }).outcome, 'not_applied');
  f.time(1500); // The admitted timestamp is in the future; a rollback must not erase the rate window.
  for (const [limits, who] of [[{ admissionsPerPrincipal: 1 }, first], [{ admissionsPerAccount: 1 }, sibling]]) {
    f.restart({ nativeLimits: limits });
    assert.throws(() => f.native.admit({ actor: f.actor(who.credential.token), idempotencyKey: 'rate_new_key', input: INPUT }), code('native_rate_limit'));
  }
  f.time(62001);
  const second = f.native.admit({ actor: f.actor(first.credential.token), idempotencyKey: 'after_rate_window_key', input: INPUT });
  assert.equal(second.reused, false);
  for (const [limits, who] of [[{ identitiesPerAccount: 2 }, sibling], [{ identitiesTotal: 2 }, foreign]]) {
    f.restart({ nativeLimits: limits });
    assert.throws(() => f.native.admit({ actor: f.actor(who.credential.token), idempotencyKey: 'ledger_new_key', input: INPUT }), code('native_ledger_limit'));
  }
  assert.equal(f.sql('caps', db => db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n), 2);
  assert.equal(f.sql('caps', db => db.prepare('SELECT count(*) AS n FROM cap_budget_reservations').get().n), 2);
  assert.equal(f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM notes').get().n), 0);
});

test('actual admission SQL uses bounded IDs and indexed scopes; recovery respects a two-item keyset without executing', async t => {
  const f = await connectedFixture(t, { nativeLimits: { recoveryPageSize: 2 } }), caller = await f.issue();
  const observed = [], prepare = DatabaseSync.prototype.prepare;
  const ids = [];
  try {
    DatabaseSync.prototype.prepare = function(sql) {
      const statement = prepare.call(this, sql);
      if (/^SELECT id FROM cap_invocations/u.test(sql)) {
        const iterate = statement.iterate.bind(statement);
        statement.iterate = (...args) => { observed.push({ sql, args }); return iterate(...args); };
      }
      return statement;
    };
    for (let i = 0; i < 3; i++) {
      f.time(1000 + i);
      ids.push(f.native.admit({ actor: caller.actor, input: INPUT, idempotencyKey: `bounded_page_${i}` }).invocation.invocationId);
    }
  } finally { DatabaseSync.prototype.prepare = prepare; }
  assert.ok(observed.length >= 7);
  for (const { sql, args } of observed) {
    assert.match(sql, / LIMIT \?$/u); assert.ok(args.at(-1) <= 100000);
    const details = f.sql('caps', db => db.prepare('EXPLAIN QUERY PLAN ' + sql).all(...args).map(row => row.detail).join('\n'));
    assert.match(details, /USING (?:COVERING )?INDEX/u, sql);
  }
  f.caps.invocations.requestCancel({ actor: caller.actor, invocationId: ids[0] });
  const first = f.native.reconcilePage();
  assert.deepEqual(first.items, [{ invocationId: ids[0], outcome: 'not_applied' }, { invocationId: ids[1], outcome: 'retryable' }]);
  assert.ok(first.nextCursor && first.nextCursor.length <= 512);
  const second = f.native.reconcilePage({ cursor: first.nextCursor });
  assert.deepEqual(second, { items: [{ invocationId: ids[2], outcome: 'retryable' }], nextCursor: null });
  assert.equal(f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM notes').get().n), 0);
  assert.throws(() => f.native.reconcilePage({ limit: 1000 }), code('invalid_input'));
});

test('actual Connect, Caps and Notes competing write locks retain the intent and restore each acquisition timeout', async t => {
  for (const name of ['connect','caps','notes']) {
    const f = await connectedFixture(t), caller = await f.issue();
    const invocationId = f.native.admit({ actor: caller.actor, input: INPUT, idempotencyKey: 'busy_native_key' }).invocation.invocationId;
    f.native.beginAttempt({ invocationId });
    const blocker = new DatabaseSync(path.join(f.directory, `${name}.sqlite`)); blocker.exec('BEGIN IMMEDIATE');
    const captures = new Map(), exec = DatabaseSync.prototype.exec;
    const started = performance.now();
    try {
      DatabaseSync.prototype.exec = function(sql) {
        if (sql === 'PRAGMA busy_timeout=100') captures.set(this, this.prepare('PRAGMA busy_timeout').get().timeout);
        return exec.call(this, sql);
      };
      assert.throws(() => f.native.execute({ invocationId }), code(name === 'connect' ? 'connect_authority_busy' : 'native_storage_busy'));
    } finally { DatabaseSync.prototype.exec = exec; blocker.exec('ROLLBACK'); blocker.close(); }
    assert.ok(performance.now() - started < 2000); assert.ok(captures.size >= 1);
    for (const [db, before] of captures) assert.equal(db.prepare('PRAGMA busy_timeout').get().timeout, before);
    assert.equal(f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM notes').get().n), 0);
    assert.equal(f.sql('caps', db => db.prepare('SELECT completed_at FROM cap_invocations').get().completed_at), null);
    assert.equal(f.native.reconcile({ invocationId }).outcome, 'retryable');
    assert.equal(f.native.execute({ invocationId }).outcome, 'committed');
  }
});

test('real delegated ancestry distinguishes a sibling epoch change from an ancestor or original-credential revoke', async t => {
  for (const reason of ['sibling','ancestor','credential']) {
    const f = await connectedFixture(t), root = await f.issue({ allowDelegation: true, maxDepth: 2 });
    const principal = (await f.access('principals.create', { label: 'Delegated external caller' })).principal;
    const scope = { parentGrantId: root.grant.id, principalId: principal.id,
      capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }], resources: ['notes:new'], effects: ['create'],
      recipients: ['soty:notes'], expiresAt: 100000, allowDelegation: false, maxDepth: 0 };
    const grant = (await f.access('grants.derive', scope)).grant;
    const caller = await f.issue({ principalId: principal.id, grantId: grant.id });
    const replacement = await f.access('credentials.issue', { grantId: grant.id, audience: ORIGIN });
    const invocationId = f.native.admit({ actor: caller.actor, input: INPUT, idempotencyKey: 'ancestry_native_key' }).invocation.invocationId;
    f.native.beginAttempt({ invocationId });
    if (reason === 'sibling') {
      const sibling = (await f.access('grants.derive', scope)).grant;
      await f.access('grants.revoke', { grantId: sibling.id });
    } else if (reason === 'ancestor') await f.access('grants.revoke', { grantId: root.grant.id });
    else await f.access('credentials.revoke', { credentialId: caller.credential.credential.id });
    assert.equal(f.native.execute({ invocationId }).outcome, reason === 'sibling' ? 'committed' : 'not_applied');
    if (reason === 'credential') assert.equal(f.native.get({ actor: f.actor(replacement.token), invocationId }).invocation.status, 'failed');
    assert.equal(f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM notes').get().n), Number(reason === 'sibling'));
  }
});
