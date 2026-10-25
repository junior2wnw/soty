import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, basename, resolve } from 'node:path';
import * as authority from '../server/native-authority.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../server/standard-profile.mjs';
import { digest } from '../server/wire.mjs';

const binding = () => ({ identity: { issuer: 'https://issuer.invalid', subject: 'person-a' },
  rootPrincipal: { accountId: 'account-a', deviceId: 'device-a' }, humanPrincipal: { subject: 'person-a' },
  resource: { selection: { kind: 'soty.workspace.v1', nativeId: 'namespace', incarnationId: 'one' } },
  semanticDigest: digest('approved-profile'), sessionIdHash: digest('session-a'), rootReferenceDigest: digest('root-a'), rootExpiresAt: 10000 });

function fixture(t) {
  const parent = resolve(tmpdir()), directory = mkdtempSync(join(parent, 'source-receipt-recovery-'));
  const file = join(directory, 'native.sqlite'); let db, runtime, live = true, now = 1000;
  const options = { receiptTransform: value => value, resultTransform: value => value,
    beforeRecovery: null, afterRecovery: null, receiptKey: Buffer.alloc(32, 7), skipRecovery: false, cloneCommitContext: false,
    receiptPadding: 0, currentTitle: null, currentRole: 'owner' };
  const captured = new WeakMap(); let savedFinal, savedRecovery, attemptedReceiptBytes;
  function current(proof, selected) {
    const held = captured.get(proof);
    assert.ok(live && held && held.binding === digest(selected), 'Native current actor/resource');
    const row = db.prepare('SELECT * FROM workspaces WHERE id=?').get(selected.resource.selection.nativeId);
    const member = db.prepare('SELECT * FROM memberships WHERE actor=? AND workspace=?').get(selected.identity.subject, row.id);
    assert.ok(member?.active && row.generation === held.generation, 'Native ACL/generation');
    return { row, member };
  }
  function transaction(action) {
    db.exec('BEGIN IMMEDIATE');
    try { const value = action(); db.exec('COMMIT'); return value; }
    catch (error) { db.exec('ROLLBACK'); throw error; }
  }
  function boot() {
    db = new DatabaseSync(file);
    db.exec(`CREATE TABLE IF NOT EXISTS workspaces(id TEXT PRIMARY KEY, incarnation TEXT, generation INTEGER, revision INTEGER);
      CREATE TABLE IF NOT EXISTS memberships(actor TEXT, workspace TEXT, active INTEGER, PRIMARY KEY(actor,workspace));
      CREATE TABLE IF NOT EXISTS items(id TEXT PRIMARY KEY, workspace TEXT, title TEXT);
      CREATE TABLE IF NOT EXISTS receipts(actor TEXT,workspace TEXT,request_id TEXT,input_digest TEXT,result_json TEXT,binding_json TEXT,
        PRIMARY KEY(actor,workspace,request_id));
      INSERT OR IGNORE INTO workspaces VALUES('namespace','original-incarnation',1,1),('other-space','other-incarnation',1,1);
      INSERT OR IGNORE INTO memberships VALUES('person-a','namespace',1),('person-b','namespace',1),('person-a','other-space',1);`);
    const port = authority.createSourceNativeAuthorityPort({
      capture(selected) {
        const proof = Object.freeze({});
        captured.set(proof, { binding: digest(selected), generation: db.prepare('SELECT generation FROM workspaces WHERE id=?').get(selected.resource.selection.nativeId).generation });
        current(proof, selected); return proof;
      },
      withCurrent(proof, selected, action) { current(proof, selected); return action(); },
      currentContext(proof, selected) {
        const { row } = current(proof, selected);
        return { key: digest([row.id, row.incarnation]), generation: row.generation, expiresAt: 10000,
          title: options.currentTitle ?? row.id, role: options.currentRole, revision: row.revision };
      },
      read() { throw new Error('unexpected domain read'); },
      execute(proof, selected, args, final) {
        assert.equal(Object.hasOwn(args, 'contextRef'), false); savedFinal = final;
        return transaction(() => final.commit(() => {
          current(proof, selected);
          db.prepare('INSERT INTO items VALUES(?,?,?)').run(args.requestId, selected.resource.selection.nativeId, args.input.title);
          db.prepare('UPDATE workspaces SET revision=revision+1 WHERE id=?').run(selected.resource.selection.nativeId);
          const receipt = { requestId: args.requestId, inputDigest: digest(args.input), outcome: 'committed',
            receipt: { id: args.requestId, revision: 1 }, contextAfter: final.contextAfter(),
            workspaceReceipt: { schema: 'soty.workspace-receipt.v1', effects: [{ kind: 'created', resourceType: 'item', resourceId: args.requestId, revision: 1 }],
              receipt: { verificationMethod: 'domain_read', artifacts: [{ type: 'item', id: args.requestId, revision: 1 }] } } };
          if(options.receiptPadding)receipt.receipt.padding='x'.repeat(options.receiptPadding);
          attemptedReceiptBytes=Buffer.byteLength(JSON.stringify(receipt));
          const receiptBinding = final.receiptBinding?.(options.cloneCommitContext ? JSON.parse(JSON.stringify(receipt)) : receipt);
          db.prepare('INSERT INTO receipts VALUES(?,?,?,?,?,?)').run(selected.identity.subject, selected.resource.selection.nativeId,
            args.requestId, receipt.inputDigest, JSON.stringify(receipt), receiptBinding ? JSON.stringify(receiptBinding) : null);
          return receipt;
        }));
      },
      async readProof(proof, selected, args, recovery) {
        assert.equal(Object.hasOwn(args, 'contextRef'), false); savedRecovery = recovery;
        current(proof, selected);
        const row = db.prepare('SELECT * FROM receipts WHERE actor=? AND workspace=? AND request_id=?').get('person-a', 'namespace', 'receipt-restart-0001');
        const original = options.receiptTransform({ receipt: JSON.parse(row.result_json), binding: row.binding_json ? JSON.parse(row.binding_json) : null });
        await options.beforeRecovery?.();
        const result = transaction(() => { current(proof, selected);
          return !options.skipRecovery && original.binding && recovery?.recover ? recovery.recover(original.receipt, original.binding) : original.receipt; });
        await options.afterRecovery?.(); return options.resultTransform(result);
      },
    });
    runtime = authority.createNativeAuthorityRuntime(port, { sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE, clock: () => now, receiptKey: options.receiptKey });
  }
  boot();
  t.after(() => { runtime.close(); db.close(); assert.equal(dirname(directory), parent); assert.match(basename(directory), /^source-receipt-recovery-/u); rmSync(directory, { recursive: true, force: true }); });
  return { options, binding,
    async session(selected = binding()) {
      const proof = await runtime.capture(selected);
      const current = await runtime.call(proof, 'read', { requestId: 'receipt-current-0001', input: { operation: 'soty.current_context' } });
      return { proof, contextRef: current.context.ref, selected };
    },
    call(session, operation, args) { return runtime.call(session.proof, operation, { contextRef: session.contextRef, ...args }); },
    restart() { runtime.close(); db.close(); boot(); },
    db: () => db, final: () => savedFinal, recovery: () => savedRecovery, attemptedReceiptBytes: () => attemptedReceiptBytes,
    revoke: () => { live = false; }, advance: value => { now += value; }, close: () => runtime.close(),
  };
}

const intent = { requestId: 'receipt-restart-0001', input: { operation: 'items.create', title: 'Durable original' } };
const readIntent = () => ({ requestId: intent.requestId, input: { inputDigest: digest(intent.input) } });

test('committed Native receipt recovers after cold Source restart and a fresh Root/session ref without applying again', async t => {
  const f = fixture(t), first = await f.session();
  await f.call(first, 'execute', intent);
  const original = f.db().prepare('SELECT result_json FROM receipts').get().result_json;
  f.restart();
  const fresh = await f.session({ ...binding(), rootReferenceDigest: digest('new-root'), sessionIdHash: digest('new-session') });
  assert.notEqual(fresh.contextRef, first.contextRef);
  assert.equal(f.db().prepare('SELECT count(*) n FROM items').get().n, 1);
  assert.equal(f.db().prepare('SELECT result_json FROM receipts').get().result_json, original);
  const recovered = await f.call(fresh, 'readProof', readIntent());
  const { recovery, ...originalProjection } = recovered;
  assert.deepEqual(originalProjection, JSON.parse(original));
  assert.ok(recovery, 'fresh authority is a separate SDK recovery envelope');
  assert.equal(f.db().prepare('SELECT count(*) n FROM items').get().n, 1);
  assert.equal(f.db().prepare('SELECT result_json FROM receipts').get().result_json, original);
});

for (const mode of ['actor', 'root-device', 'resource', 'profile', 'native-incarnation', 'requestId', 'input', 'old-ref', 'wrong-key'])
  test('fresh receipt recovery rejects '+mode+' substitution without a second effect', async t => {
    const f = fixture(t), first = await f.session(); await f.call(first, 'execute', intent);
    const original = f.db().prepare('SELECT result_json FROM receipts').get().result_json;
    if (mode === 'wrong-key') f.options.receiptKey = Buffer.alloc(32, 9);
    f.restart();
    if (mode === 'native-incarnation') f.db().prepare("UPDATE workspaces SET incarnation='recreated' WHERE id='namespace'").run();
    let selected = { ...binding(), rootReferenceDigest: digest('fresh-root'), sessionIdHash: digest('fresh-session') };
    if (mode === 'actor') selected = { ...selected, identity: { ...selected.identity, subject: 'person-b' }, humanPrincipal: { subject: 'person-b' } };
    if (mode === 'root-device') selected = { ...selected, rootPrincipal: { ...selected.rootPrincipal, deviceId: 'foreign-device' } };
    if (mode === 'resource') selected = { ...selected, resource: { selection: { ...selected.resource.selection, nativeId: 'other-space' } } };
    if (mode === 'profile') selected = { ...selected, semanticDigest: digest('other-approved-profile') };
    const fresh = await f.session(selected), read = readIntent();
    if (mode === 'requestId') read.requestId = 'receipt-foreign-0002';
    if (mode === 'input') read.input.inputDigest = digest({ ...intent.input, title: 'Different intent' });
    if (mode === 'old-ref') read.contextRef = first.contextRef;
    await assert.rejects(f.call(fresh, 'readProof', read));
    assert.equal(f.db().prepare('SELECT count(*) n FROM items').get().n, 1);
    assert.equal(f.db().prepare('SELECT result_json FROM receipts').get().result_json, original);
  });

for (const mode of ['receipt', 'binding', 'old-mutationRef', 'binding-ref', 'binding-scope', 'binding-extra'])
  test('sealed original receipt rejects '+mode+' corruption', async t => {
    const f = fixture(t), first = await f.session(); await f.call(first, 'execute', intent); f.restart();
    const fresh = await f.session();
    f.options.receiptTransform = value => {
      if (mode === 'receipt') value.receipt.receipt.id = 'foreign-artifact';
      if (mode === 'binding') value.binding.mac = '0'.repeat(64);
      if (mode === 'old-mutationRef') value.receipt.contextAfter.mutationRef = '0'.repeat(64);
      if (mode === 'binding-ref') value.binding.contextRef = fresh.contextRef;
      if (mode === 'binding-scope') value.binding.scopeDigest = '0'.repeat(64);
      if (mode === 'binding-extra') value.binding.actor = 'person-a';
      return value;
    };
    await assert.rejects(f.call(fresh, 'readProof', readIntent()));
    assert.equal(f.db().prepare('SELECT count(*) n FROM items').get().n, 1);
  });

for (const phase of ['beforeRecovery', 'afterRecovery']) for (const mode of ['epoch', 'revoke', 'expiry', 'close'])
  test('fresh recovery suppresses its result after '+mode+' at '+phase, async t => {
    const f = fixture(t), first = await f.session(); await f.call(first, 'execute', intent); f.restart();
    const fresh = await f.session();
    f.options[phase] = () => {
      if (mode === 'epoch') f.db().prepare("UPDATE workspaces SET generation=generation+2 WHERE id='namespace'").run();
      if (mode === 'revoke') f.db().prepare("UPDATE memberships SET active=0 WHERE actor='person-a'").run();
      if (mode === 'expiry') f.advance(11000);
      if (mode === 'close') f.close();
    };
    await assert.rejects(f.call(fresh, 'readProof', readIntent()));
    assert.equal(f.db().prepare('SELECT count(*) n FROM items').get().n, 1);
  });

test('a historical receipt and fresh read metadata remain separate after a later Native revision', async t => {
  const f = fixture(t), first = await f.session(); await f.call(first, 'execute', intent);
  const original = f.db().prepare('SELECT result_json FROM receipts').get().result_json;
  f.restart(); f.db().prepare("UPDATE workspaces SET revision=revision+3 WHERE id='namespace'").run();
  const recovered = await f.call(await f.session(), 'readProof', readIntent());
  assert.equal(recovered.contextAfter.revision, 2);
  assert.equal(recovered.recovery.contextAfter.revision, 5);
  assert.equal(f.db().prepare('SELECT result_json FROM receipts').get().result_json, original);
});

test('old receipt without a binding keeps exact same-ref behavior and does not get reconstructed after restart', async t => {
  const f = fixture(t), first = await f.session(); await f.call(first, 'execute', intent);
  f.db().prepare('UPDATE receipts SET binding_json=NULL').run();
  const original = JSON.parse(f.db().prepare('SELECT result_json FROM receipts').get().result_json);
  assert.deepEqual(await f.call(first, 'readProof', readIntent()), original);
  f.restart(); await assert.rejects(f.call(await f.session(), 'readProof', readIntent()), { code: 'source_workspace_receipt_denied' });
  assert.equal(f.db().prepare('SELECT count(*) n FROM items').get().n, 1);
});

test('only the original branded commit context can seal a durable binding', async t => {
  const f = fixture(t); f.options.cloneCommitContext = true;
  await assert.rejects(f.call(await f.session(), 'execute', intent));
  assert.equal(f.db().prepare('SELECT count(*) n FROM items').get().n, 0);
  assert.equal(f.db().prepare('SELECT count(*) n FROM receipts').get().n, 0);
});

for (const mode of ['cloned-result', 'json-envelope']) test('JSON cannot replace the scoped readProof capability: '+mode, async t => {
  const f = fixture(t), first = await f.session(); await f.call(first, 'execute', intent); f.restart();
  const fresh = await f.session();
  if (mode === 'cloned-result') f.options.resultTransform = value => JSON.parse(JSON.stringify(value));
  else {
    f.options.skipRecovery = true;
    f.options.receiptTransform = value => { value.receipt.recovery = { schema: 'soty.workspace-receipt-recovery.v1', contextRef: fresh.contextRef,
      receiptDigest: digest(value.receipt), contextAfter: value.receipt.contextAfter }; return value; };
  }
  await assert.rejects(f.call(fresh, 'readProof', readIntent()), { code: 'source_workspace_recovery_invalid' });
});

test('commit/recovery capabilities expire on method return and do not survive JSON serialization', async t => {
  const f = fixture(t), first = await f.session(); await f.call(first, 'execute', intent);
  const row = f.db().prepare('SELECT * FROM receipts').get(), original = JSON.parse(row.result_json), sidecar = JSON.parse(row.binding_json);
  assert.throws(() => f.final().receiptBinding(original), { code: 'source_app_commit_invalid' });
  await f.call(first, 'readProof', readIntent());
  assert.equal(authority.isSourceNativeReceiptRecoveryPort(f.recovery()), true);
  assert.equal(authority.isSourceNativeReceiptRecoveryPort(JSON.parse(JSON.stringify(f.recovery()))), false);
  assert.throws(() => f.recovery().recover(original, sidecar), { code: 'source_workspace_recovery_unavailable' });
});

test('a near-limit original reserves its future recovery envelope without widening the 64KiB protocol', async t => {
  async function atMaximumMetadata(f) {
    f.restart();f.options.currentTitle='\uffff'.repeat(240);f.options.currentRole='editor';
    f.db().prepare("UPDATE workspaces SET generation=?,revision=? WHERE id='namespace'").run(Number.MAX_SAFE_INTEGER,Number.MAX_SAFE_INTEGER);
    return f.call(await f.session(),'readProof',readIntent());
  }
  const probe=fixture(t);probe.options.receiptPadding=60000;
  await probe.call(await probe.session(),'execute',intent);
  const maximum=await atMaximumMetadata(probe), wireBytes=Buffer.byteLength(JSON.stringify({ok:true,data:maximum}));
  const boundaryPadding=60000+65536-wireBytes;
  const allowed=fixture(t);allowed.options.receiptPadding=boundaryPadding;
  await allowed.call(await allowed.session(),'execute',intent);
  const recovered=await atMaximumMetadata(allowed);
  assert.equal(Buffer.byteLength(JSON.stringify({ok:true,data:recovered})),65536,'the full HTTP envelope fits at the exact byte boundary');
  assert.equal(allowed.db().prepare('SELECT count(*) n FROM items').get().n,1);
  const oversized=fixture(t);oversized.options.receiptPadding=boundaryPadding+1;
  await assert.rejects(oversized.call(await oversized.session(),'execute',intent),{code:'source_app_effect_unknown'});
  assert.ok(oversized.attemptedReceiptBytes()<65536,'the original alone fits; the maximum recovery reserve is the limiting factor');
  assert.equal(oversized.db().prepare('SELECT count(*) n FROM items').get().n,0,'Native transaction rolls back the staged effect');
  assert.equal(oversized.db().prepare('SELECT count(*) n FROM receipts').get().n,0);
});
