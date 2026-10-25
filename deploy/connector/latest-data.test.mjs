import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, mkdir, readFile, writeFile, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createConnectorStore } from '../../server/connector-store.js';
import { probe } from '../connect/maintenance-probe.mjs';
import { checkedFullEncryptedBackup } from './rollout.mjs';
import { productionMaintenance } from './runtime.mjs';
import { SafeError } from './docker-api.mjs';
import { fixture, args, modelProxies } from './test-support/rollout-engine.mjs';

const modernArgs = { ...args, transaction: 'f'.repeat(32), rollbackMode: 'latest-data' };
// An injected operator receipt exercises the ordering contract. Genuine GPG,
// Docker logging and decrypt/restore remain separate Linux/exact-image gates.
const backupReceipt = transaction => ({ ok: true, encrypted: true, operationId: transaction,
  sourceCheckpointSha256: '1'.repeat(64), volume: { sha256: '2'.repeat(64), bytes: 100, entries: 2, fileBytes: 10, inventorySha256: '3'.repeat(64) },
  configuration: { sha256: '4'.repeat(64), bytes: 100, entries: 2, fileBytes: 10, inventorySha256: '5'.repeat(64) } });
function originalLifecycle(f){
  const original=f.map.get(args.originalId);original.RestartCount=0;
  Object.assign(original.State,{Pid:1234,StartedAt:'2026-10-25T00:00:00.000000000Z',FinishedAt:'0001-01-01T00:00:00Z',ExitCode:0,OOMKilled:false,Paused:false,Restarting:false});
  const stop=f.engine.stop,start=f.engine.start;
  f.engine.stop=async id=>{await stop(id);if(id===args.originalId)Object.assign(f.map.get(id).State,{Pid:0,FinishedAt:'2026-10-25T00:00:01.000000000Z'});};
  f.engine.start=async id=>{await start(id);if(id===args.originalId)Object.assign(f.map.get(id).State,{Pid:2345,StartedAt:'2026-10-25T00:00:02.000000000Z'});};
}

async function setup(t) {
  const dataDir = await mkdtemp(path.join(await realpath(tmpdir()), 'soty-latest-data-'));
  t.after(async () => { assert.equal(path.dirname(dataDir), await realpath(tmpdir()));
    assert.match(path.basename(dataDir), /^soty-latest-data-/u); await rm(dataDir, { recursive: true, force: true, maxRetries: 3 }); });
  const request = { id: '1'.repeat(64), fingerprint: '2'.repeat(64), jobId: 'retained-accepted-request' };
  await writeFile(path.join(dataDir, 'connector-store.json'), JSON.stringify({ schema: 'soty.connector-store.v2',
    connectors: [], accessGrants: [], jobs: [], requests: [request] }));
  const store = createConnectorStore(dataDir); await store.ready; await store.close();
  const bridgeDir = path.join(dataDir, 'account-ai'); await mkdir(bridgeDir);
  const bridge = path.join(bridgeDir, 'bridge.sqlite'), database = path.join(dataDir, 'connector-store.sqlite');
  const db = new DatabaseSync(bridge);
  db.exec('CREATE TABLE callback_nonces (issuer TEXT NOT NULL,kid TEXT NOT NULL,jti TEXT NOT NULL,expires_at INTEGER NOT NULL,PRIMARY KEY(issuer,kid,jti)); CREATE INDEX callback_nonce_expiry ON callback_nonces(expires_at);');
  db.prepare('INSERT INTO callback_nonces VALUES (?,?,?,?)').run('fixture-issuer', 'fixture-kid', 'before-candidate', 1); db.close();
  const f = fixture(); originalLifecycle(f);
  f.run.maintenance = async verb => { assert.notEqual(verb, 'rollback', 'latest-data must never call legacy rollback');
    f.events.push('modern:' + verb); return probe({ dataDir, verb, transaction: modernArgs.transaction,
      ...(process.platform === 'win32' ? { sync: async () => {} } : {}) }); };
  let backups = 0;
  f.run.fullEncryptedBackup = async input => {
    assert.equal(input.containerId, args.originalId); assert.equal(input.originalImage, args.originalImage);
    assert.equal(input.transaction, modernArgs.transaction); assert.equal(input.storageProbeImage, args.storageProbeImage);
    assert.equal(f.map.get(args.originalId).State.Running, false);
    assert.deepEqual(f.map.get(args.originalId).HostConfig.RestartPolicy, { Name: 'no', MaximumRetryCount: 0 });
    assert.equal(f.map.get(f.run.candidate.Id).State.Running, false);
    assert.equal(f.events.includes('modern:enter'), false); backups++; f.events.push('full-encrypted-backup');
    return backupReceipt(input.transaction);
  };
  const ready = f.run.ready;
  f.run.ready = async (kind, run) => {
    if (kind === 'candidate' && f.map.get(args.originalId).State.Running) {
      f.events.push('ready:restored-original');
      return { ok: true, storageReady: true, maintenance: true, schema: 'soty.connector-storage-ready.v1', modelProxies };
    }
    return ready(kind, run);
  };
  return { ...f, dataDir, database, bridge, request, backups: () => backups };
}

test('latest-data retains real SQLite accepted request identities and newer AI data after a failed candidate', async t => {
  const f = await setup(t), legacy = await readFile(path.join(f.dataDir, 'connector-store.json'));
  await f.run.prepare(modernArgs); const ready = f.run.ready;
  f.run.ready = async (kind, run) => {
    if (kind === 'candidate' && f.map.get(run.candidate.Id).State.Running) {
      const db = new DatabaseSync(f.bridge);
      db.prepare('INSERT INTO callback_nonces VALUES (?,?,?,?)').run('fixture-issuer', 'fixture-kid', 'accepted-by-candidate', 2); db.close();
      throw new SafeError('candidate_test_failure');
    }
    return ready(kind, run);
  };
  await assert.rejects(f.run.promote(), /candidate_test_failure/);
  assert.equal(f.run.state.phase, 'restored'); assert.equal(f.backups(), 1);
  assert.equal(f.map.get(args.originalId).State.Running, true); assert.equal(f.map.get(f.run.candidate.Id).State.Running, false);
  const connector = new DatabaseSync(f.database, { readOnly: true });
  assert.deepEqual(JSON.parse(connector.prepare('SELECT value FROM requests WHERE id=?').get(f.request.id).value), f.request);
  connector.close(); const bridge = new DatabaseSync(f.bridge, { readOnly: true });
  assert.deepEqual(bridge.prepare('SELECT jti FROM callback_nonces ORDER BY expires_at').all().map(row => row.jti), ['before-candidate', 'accepted-by-candidate']); bridge.close();
  assert.deepEqual(await readFile(path.join(f.dataDir, 'connector-store.json')), legacy);
  assert.equal((await probe({ dataDir: f.dataDir, transaction: modernArgs.transaction })).maintenance, false);
  assert.equal(f.events.includes('helper:rollback'), false); assert.equal(f.run.state.rollbackMode, 'latest-data');
  assert.ok(f.events.indexOf('full-encrypted-backup') < f.events.indexOf('modern:enter'));
  assert.ok(f.events.indexOf('modern:enter') < f.events.indexOf('start-candidate'));
  assert.doesNotMatch(JSON.stringify(f.records), /synthetic-sensitive|TOKEN=/u);
});

test('latest-data successful promotion keeps the original fenced and its complete backup receipt', async t => {
  const f = await setup(t); await f.run.prepare(modernArgs); await f.run.promote();
  assert.equal(f.run.state.phase, 'committed'); assert.equal(f.backups(), 1);
  assert.equal(f.map.get(args.originalId).State.Running, false);
  assert.deepEqual(f.map.get(args.originalId).HostConfig.RestartPolicy, { Name: 'no', MaximumRetryCount: 0 });
  assert.deepEqual(f.run.state.fullEncryptedBackup, backupReceipt(modernArgs.transaction));
});

test('foreign maintenance is never cleared before stop or during latest-data recovery', async t => {
  for (const timing of ['preflight', 'recovery']) {
    const f = await setup(t); await f.run.prepare(modernArgs);
    const marker = path.join(f.dataDir, 'connector-maintenance.json');
    const foreign = JSON.stringify({ schema: 'soty.connector-maintenance.v1', connectTransaction: '0'.repeat(32) });
    if (timing === 'preflight') await writeFile(marker, foreign);
    else { const ready = f.run.ready; f.run.ready = async (kind, run) => {
      if (kind === 'candidate' && f.map.get(run.candidate.Id).State.Running) { await writeFile(marker, foreign); throw new SafeError('candidate_test_failure'); }
      return ready(kind, run);
    }; }
    await assert.rejects(f.run.promote()); assert.equal(await readFile(marker, 'utf8'), foreign);
    assert.equal(f.events.includes('modern:leave'), false);
    assert.equal(f.run.state.phase, timing === 'preflight' ? 'aborted' : 'recovery_required');
    assert.equal(f.map.get(args.originalId).State.Running, timing === 'preflight');
  }
});

test('missing or failed full backup prevents candidate start and safely preserves the original', async t => {
  for (const failure of ['missing-hook', 'failed', 'wrong-shape', 'wrong-transaction']) {
    const f = await setup(t); await f.run.prepare(modernArgs);
    if (failure === 'missing-hook') f.run.fullEncryptedBackup = undefined;
    else f.run.fullEncryptedBackup = async () => {
      if (failure === 'failed') throw new SafeError('backup_failed');
      return failure === 'wrong-shape' ? { ok: true, encrypted: true, sha256: '1'.repeat(64) } : backupReceipt('0'.repeat(32));
    };
    await assert.rejects(f.run.promote());
    assert.equal(f.events.includes('start-candidate'), false); assert.equal(f.map.get(args.originalId).State.Running, true);
    assert.equal(f.run.state.phase, failure === 'missing-hook' ? 'prepared' : 'restored');
  }
});

test('unresolved backup, candidate START or recovery STOP stays fenced without replay or original START', async t => {
  for (const operation of ['backup', 'start', 'stop']) {
    const f = await setup(t); await f.run.prepare(modernArgs); let calls = 0;
    if (operation === 'backup') f.run.fullEncryptedBackup = async () => { calls++; throw new SafeError('full_encrypted_backup_unresolved'); };
    else if (operation === 'start') { const start = f.engine.start; f.engine.start = async id => { if (id === f.run.candidate.Id) { calls++; throw new SafeError('engine_response_ambiguous'); } return start(id); }; }
    else { const ready = f.run.ready, stop = f.engine.stop;
      f.run.ready = async (kind, run) => { if (kind === 'candidate' && f.map.get(run.candidate.Id).State.Running) throw new SafeError('candidate_test_failure'); return ready(kind, run); };
      f.engine.stop = async id => { if (id === f.run.candidate.Id) { calls++; throw new SafeError('engine_response_ambiguous'); } return stop(id); };
    }
    await assert.rejects(f.run.promote());
    assert.equal(calls, 1); assert.equal(f.run.state.phase, 'recovery_required');
    assert.equal(f.map.get(args.originalId).State.Running, false); assert.equal(f.events.includes('start-old'), false);
  }
});

test('original configuration drift during full backup cannot start either application', async t => {
  const f = await setup(t); await f.run.prepare(modernArgs);
  f.run.fullEncryptedBackup = async () => {
    f.map.get(args.originalId).Config.Env.push('UNREVIEWED=1'); return backupReceipt(modernArgs.transaction);
  };
  await assert.rejects(f.run.promote(), /recovery_required/);
  assert.equal(f.run.state.phase, 'recovery_required'); assert.equal(f.events.includes('start-old'), false);
  assert.equal(f.events.includes('start-candidate'), false);
});

test('backup callback exceptions cannot put private messages or error codes in the journal', async t => {
  const f = await setup(t); await f.run.prepare(modernArgs);
  f.run.fullEncryptedBackup = async () => { throw Object.assign(new Error('synthetic-private-backup-message'), { code: 'synthetic-private-backup-code' }); };
  await assert.rejects(f.run.promote(), /full_encrypted_backup_failed/);
  assert.equal(f.run.state.phase, 'restored'); assert.doesNotMatch(JSON.stringify(f.records), /synthetic-private-backup/u);
});

test('dirty or unstable original STOP is fenced before backup, enter and either application START',async t=>{
  for(const failure of ['exit','oom','restarting','paused','pid','finished','started','restart-count']){
    const f=await setup(t);await f.run.prepare(modernArgs);const stop=f.engine.stop;
    f.engine.stop=async id=>{await stop(id);if(id!==args.originalId)return;const old=f.map.get(id);
      if(failure==='exit')old.State.ExitCode=137;
      if(failure==='oom')old.State.OOMKilled=true;
      if(failure==='restarting')old.State.Restarting=true;
      if(failure==='paused')old.State.Paused=true;
      if(failure==='pid')old.State.Pid=1234;
      if(failure==='finished')old.State.FinishedAt='0001-01-01T00:00:00Z';
      if(failure==='started')old.State.StartedAt='2026-10-24T00:00:00.000000000Z';
      if(failure==='restart-count')old.RestartCount++;
    };
    await assert.rejects(f.run.promote());assert.equal(f.run.state.phase,'recovery_required');assert.equal(f.backups(),0);
    assert.equal(f.events.includes('modern:enter'),false);assert.equal(f.events.includes('start-old'),false);assert.equal(f.events.includes('start-candidate'),false);
    assert.deepEqual(f.map.get(args.originalId).HostConfig.RestartPolicy,{Name:'no',MaximumRetryCount:0});
  }
});

test('original restart before STOP or lifecycle drift during backup cannot be accepted as quiescence',async t=>{
  const before=await setup(t);await before.run.prepare(modernArgs);before.map.get(args.originalId).RestartCount++;
  await assert.rejects(before.run.promote(),/original_lifecycle_changed/);assert.equal(before.run.state.phase,'aborted');
  assert.equal(before.events.some(event=>event.startsWith('stop:')),false);
  const after=await setup(t);await after.run.prepare(modernArgs);const backup=after.run.fullEncryptedBackup;
  after.run.fullEncryptedBackup=async input=>{const result=await backup(input);after.map.get(args.originalId).State.FinishedAt='2026-10-25T00:00:03.000000000Z';return result;};
  await assert.rejects(after.run.promote(),/original_lifecycle_changed/);assert.equal(after.run.state.phase,'recovery_required');
  assert.equal(after.events.includes('start-old'),false);assert.equal(after.events.includes('start-candidate'),false);
});

test('latest-data refuses queued preservation, legacy recovery, short transactions and prepared mode drift', async t => {
  for (const change of [{ preserveQueuedSha256: '0'.repeat(64) }, { legacyRecovery: true }, { transaction: args.transaction }, { rollbackMode: 'arbitrary' }]) {
    const f = await setup(t); await assert.rejects(f.run.prepare({ ...modernArgs, ...change }));
    assert.equal(f.events.some(event => event.startsWith('create:')), false);
  }
  const f = await setup(t); await f.run.prepare(modernArgs); delete f.run.state.rollbackMode;
  await assert.rejects(f.run.promote(), /prepared_rollback_mode_mismatch/); assert.equal(f.events.some(event => event.startsWith('stop:')), false);
});

test('active jobs, an incompatible latest-data reader and an ambiguous leave refuse automatic restoration', async t => {
  for (const failure of ['job', 'reader', 'leave']) {
    const f = await setup(t); await f.run.prepare(modernArgs);
    if (failure === 'leave') { const maintenance = f.run.maintenance; f.run.maintenance = async verb => {
      if (verb === 'leave') { await maintenance(verb); throw new SafeError('maintenance_helper_unresolved'); } return maintenance(verb);
    }; }
    else { const ready = f.run.ready; f.run.ready = async (kind, run) => {
      if (kind === 'candidate' && f.map.get(run.candidate.Id).State.Running) {
        if (failure === 'job') { const db = new DatabaseSync(f.database); db.prepare('INSERT INTO jobs VALUES (?,?,?)').run('new-job', '{"status":"queued"}', 'fixture'); db.close(); }
        else { const image = f.engine.image; f.engine.image = async id => { const value = await image(id);
          if (id === args.originalImage) { const readers = JSON.parse(value.Config.Labels['io.soty.storage.readers']); readers.readers.world = [6]; value.Config.Labels['io.soty.storage.readers'] = JSON.stringify(readers); } return value; };
          f.run.storageProbe = async () => ({ ok: true, schema: 'soty.storage-format.v7', rooms: 'empty', apps: 'empty', notes: 'empty', capabilities: 'empty',
            appRegistration: 'empty', feedback: 'empty', humanIdentity: 'empty', connect: 'empty', world: 7 }); }
        throw new SafeError('candidate_test_failure');
      }
      return ready(kind, run);
    }; }
    await assert.rejects(f.run.promote()); assert.equal(f.run.state.phase, 'recovery_required');
    assert.equal(f.map.get(args.originalId).State.Running, false); assert.equal(f.events.includes('start-old'), false);
    assert.equal(f.events.includes('helper:rollback'), false);
  }
});

test('production latest-data helper uses pinned host probe and refuses the legacy rollback verb', async () => {
  const f = fixture(); originalLifecycle(f); await f.run.prepare(modernArgs);
  f.engine.helperOutput = async () => ({ ok: true, schema: 'soty.connect.maintenance.v1', count: 0, maintenance: false, owned: false });
  let body; const create = f.engine.create; f.engine.create = async (name, config) => { body = structuredClone(config); return create(name, config); };
  const maintenance = productionMaintenance(f.engine, { maxPolls: 2, sleep: async () => {} }); await maintenance('status', f.run);
  assert.deepEqual(body.Cmd, ['--input-type=module', '-e', await readFile(new URL('../connect/maintenance-probe.mjs', import.meta.url), 'utf8'), 'status', modernArgs.transaction]);
  assert.equal(body.Env.at(-1), 'SOTY_CONNECT_PROBE=1'); assert.equal(body.HostConfig.NetworkMode, 'none'); assert.equal(body.HostConfig.ReadonlyRootfs, true);
  assert.doesNotMatch(body.Cmd.join(' '), /server\/connector-maintenance\.js/u);
  const events = f.events.length; await assert.rejects(maintenance('rollback', f.run), /latest_data_rollback_forbidden/); assert.equal(f.events.length, events);
  assert.throws(() => checkedFullEncryptedBackup({ ...backupReceipt(modernArgs.transaction), rawInspect: {} }, modernArgs.transaction), /full_encrypted_backup_invalid/);
});
