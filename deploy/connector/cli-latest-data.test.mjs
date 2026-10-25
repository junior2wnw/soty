import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile, realpath, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { runConnectorCli } from './cli.mjs';
import { dockerHttpFixture } from './test-support/docker-http.mjs';
import { args } from './test-support/rollout-engine.mjs';

test('CLI binds latest-data to both prepared journal and supervised receipt before any STOP', async t => {
  const root = await mkdtemp(path.join(await realpath(tmpdir()), 'soty-universal-policy-')), engine = await dockerHttpFixture(root);
  t.after(async () => { await engine.close(); assert.equal(path.dirname(root), await realpath(tmpdir()));
    assert.match(path.basename(root), /^soty-universal-policy-/u); await rm(root, { recursive: true, force: true, maxRetries: 3 }); });
  const journal = path.join(root, 'journal.json'), approval = path.join(root, 'approval.json');
  const original=engine.map.get(args.originalId);original.RestartCount=0;
  Object.assign(original.State,{Pid:1234,StartedAt:'2026-10-25T00:00:00.000000000Z',FinishedAt:'0001-01-01T00:00:00Z',ExitCode:0,OOMKilled:false,Paused:false,Restarting:false});
  const flags = ['--journal', journal, '--original-id', args.originalId, '--original-image', args.originalImage,
    '--candidate-image', args.candidateImage, '--storage-probe-image', args.storageProbeImage, '--revision', args.revision,
    '--transaction', 'f'.repeat(32), '--health-origin', engine.healthOrigin, '--docker-socket', engine.socket];
  const prepared = await runConnectorCli(['prepare', ...flags, '--rollback-mode', 'latest-data']);
  assert.equal(prepared.ok, true, prepared.code); assert.equal(prepared.rollbackMode, 'latest-data');
  for (const failure of ['approval-omitted', 'prepared-omitted', 'flag-omitted']) {
    const approved = { ...prepared, approved: true, noPendingConnectorJobsObserved: true, fullStoppedWriterBackupRequired: true };
    const prior = { ...prepared };
    if (failure === 'approval-omitted') delete approved.rollbackMode;
    if (failure === 'prepared-omitted') delete prior.rollbackMode;
    await writeFile(journal, JSON.stringify(prior)); await writeFile(approval, JSON.stringify(approved));
    const result = await runConnectorCli(['promote', ...flags, '--reviewed-receipt', approval,
      ...(failure === 'flag-omitted' ? [] : ['--rollback-mode', 'latest-data'])]);
    assert.equal(result.code, 'supervised_rollback_mode_mismatch');
    assert.equal(engine.events.some(event => event.startsWith('stop:')), false);
  }
  await writeFile(journal, JSON.stringify(prepared));
  const approved={...prepared,approved:true,noPendingConnectorJobsObserved:true,fullStoppedWriterBackupRequired:true};
  for(const failure of ['jobs','backup','lifecycle']){
    const changed=structuredClone(approved);
    if(failure==='jobs')delete changed.noPendingConnectorJobsObserved;
    if(failure==='backup')delete changed.fullStoppedWriterBackupRequired;
    if(failure==='lifecycle')changed.originalLifecycle.restartCount++;
    changed.legacyNoPendingWritesObserved=true;
    await writeFile(approval,JSON.stringify(changed));
    const result=await runConnectorCli(['promote',...flags,'--reviewed-receipt',approval,'--rollback-mode','latest-data']);
    assert.equal(result.code,failure==='lifecycle'?'supervised_original_lifecycle_mismatch':'supervised_latest_data_prerequisites_missing');
    assert.equal(engine.events.some(event=>event.startsWith('stop:')),false);
  }
  await writeFile(approval,JSON.stringify(approved));
  const missingBackup = await runConnectorCli(['promote', ...flags, '--reviewed-receipt', approval, '--rollback-mode', 'latest-data']);
  assert.equal(missingBackup.code, 'full_encrypted_backup_required'); assert.equal(engine.events.some(event => event.startsWith('stop:')), false);
  assert.equal(JSON.parse(await readFile(journal)).phase, 'prepared');
});
