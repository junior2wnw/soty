import { constants } from 'node:fs';
import { lstat, open, mkdir, unlink, realpath } from 'node:fs/promises';
import path from 'node:path';
import { SafeError } from '../connector/docker-api.mjs';
import { createConfig, preservationHash, hash } from '../connector/rollout.mjs';
import { storageReaders, reconcileStorageProbe } from '../connector/storage-guard.mjs';

export const FIRST_TRANSITION = 'first_transition';
const HASH = /^[a-f0-9]{64}$/, IMAGE = /^sha256:[a-f0-9]{64}$/, REV = /^[a-f0-9]{40}$/;
const TX = /^[a-f0-9]{32}$/, LABEL = 'io.soty.connector-rollout';
const NO_RESTART = { Name: 'no', MaximumRetryCount: 0 };
const requireThat = (value, code) => { if (!value) throw new SafeError(code); };
const keys = (value, names) => value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).sort().join(',') === names.split(',').sort().join(',');
const same = (a, b) => hash(a) === hash(b);
const exactProfile = value => keys(value, 'image,buildRevision,configurationSha256')
  && IMAGE.test(value.image) && REV.test(value.buildRevision) && HASH.test(value.configurationSha256);

// Private operator inputs only. No commands, environment, key bytes or config
// values are accepted here. A missing restore receipt is a paused cold phase.
export async function readFirstTransitionFile(file, { optional = false } = {}) {
  requireThat(typeof file === 'string' && path.isAbsolute(file) && !file.includes('\0'), 'first_transition_path_invalid');
  let before;
  try { before = await lstat(file); }
  catch (error) { if (optional && error.code === 'ENOENT') return null; throw new SafeError('first_transition_receipt_unavailable'); }
  requireThat(before.isFile() && !before.isSymbolicLink() && before.size > 0 && before.size <= 65536, 'first_transition_file_invalid');
  const handle = await open(file, constants.O_RDONLY | (constants.O_NOFOLLOW || 0));
  try {
    const opened = await handle.stat(), bytes = Buffer.alloc(65537); let length = 0;
    requireThat(opened.dev === before.dev && opened.ino === before.ino && opened.isFile(), 'first_transition_file_changed');
    while (length < bytes.length) { const result = await handle.read(bytes, length, bytes.length - length, null); if (!result.bytesRead) break; length += result.bytesRead; }
    const after = await handle.stat();
    requireThat(length === opened.size && length <= 65536 && after.size === opened.size && after.mtimeMs === opened.mtimeMs, 'first_transition_file_changed');
    try { return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(0, length))); }
    catch { throw new SafeError('first_transition_json_invalid'); }
  } finally { await handle.close(); }
}

export function checkedFirstTransition(request) {
  requireThat(keys(request, 'schema,id,outgoing,candidate,recovery,handoff,admissionReceiptFile,restoreReceiptFile')
    && request.schema === 'soty.connect.first-transition.v1' && TX.test(request.id)
    && keys(request.outgoing, 'id,image,startedAt,configurationSha256') && HASH.test(request.outgoing.id)
    && IMAGE.test(request.outgoing.image) && HASH.test(request.outgoing.configurationSha256)
    && typeof request.outgoing.startedAt === 'string' && /^\d{4}-\d{2}-\d{2}T[0-9:.]+Z$/.test(request.outgoing.startedAt)
    && exactProfile(request.candidate) && exactProfile(request.recovery)
    && keys(request.handoff, 'sourceRoot,revision,tree') && REV.test(request.handoff.revision) && HASH.test(request.handoff.tree)
    && typeof request.handoff.sourceRoot === 'string' && path.isAbsolute(request.handoff.sourceRoot) && !request.handoff.sourceRoot.includes('\0')
    && request.candidate.image !== request.outgoing.image && request.recovery.image !== request.outgoing.image,
  'first_transition_request_invalid');
  for (const key of ['admissionReceiptFile', 'restoreReceiptFile']) requireThat(typeof request[key] === 'string'
    && path.isAbsolute(request[key]) && !request[key].includes('\0'), 'first_transition_path_invalid');
  return structuredClone(request);
}

export function checkedCompleteBackup(receipt, transaction) {
  const witness = receipt?.sourceWitness;
  requireThat(receipt?.encrypted === true && receipt.complete === true && HASH.test(receipt.sha256 || '')
    && HASH.test(receipt.manifestSha256 || '') && typeof receipt.receiptPath === 'string' && path.isAbsolute(receipt.receiptPath)
    && keys(witness, 'generationId,checkpointSha256,inventorySha256') && witness.generationId === transaction.id
    && witness.checkpointSha256 === transaction.coldCheckpointSha256 && HASH.test(witness.inventorySha256 || ''),
  'first_transition_complete_backup_required');
  return { encrypted: true, complete: true, receiptPath: receipt.receiptPath, sha256: receipt.sha256,
    manifestSha256: receipt.manifestSha256, sourceWitness: structuredClone(witness) };
}

function checkedAdmission(value, request) {
  requireThat(keys(value, 'schema,transactionId,outgoing,candidate,recovery,handoff,admissionClosed,imageGatePassed')
    && value.schema === 'soty.connect.first-transition.admission.v1' && value.transactionId === request.id
    && same(value.outgoing, request.outgoing) && same(value.candidate, request.candidate) && same(value.recovery, request.recovery) && same(value.handoff, request.handoff)
    && value.admissionClosed === true && value.imageGatePassed === true, 'first_transition_admission_required');
  return structuredClone(value);
}

export function checkedFirstRestore(value, transaction) {
  const r = transaction.request, b = checkedCompleteBackup(transaction.backup, transaction);
  requireThat(keys(value, 'schema,transactionId,archiveSha256,manifestSha256,sourceWitness,candidate,recovery,handoff,restored,applicationReady,admissionClosed,sourceStillCold')
    && value.schema === 'soty.connect.first-transition.restore.v1' && value.transactionId === transaction.id
    && value.archiveSha256 === b.sha256 && value.manifestSha256 === b.manifestSha256 && same(value.sourceWitness, b.sourceWitness)
    && same(value.candidate, r.candidate) && same(value.recovery, r.recovery) && same(value.handoff, r.handoff) && value.restored === true
    && value.applicationReady === true && value.admissionClosed === true && value.sourceStillCold === true,
  'first_transition_restore_required');
  return structuredClone(value);
}

// This barrier also applies to direct calls and reconciliation, not just the
// forward phase adapter. O never gets reader permission through this mode.
export function assertFirstTransitionAction(state, kind, id, value) {
  const t = state?.transaction;
  if (t?.mode !== FIRST_TRANSITION || id !== t.oldId) return;
  requireThat(kind === 'stop' || kind === 'rename' || (kind === 'restartPolicy' && same(value, NO_RESTART)), 'first_transition_outgoing_stop_only');
}

async function moduleState(host, allowedTarget = host.target) {
  const file = path.join(host.moduleStateDir, 'state.json');
  const value = await readFirstTransitionFile(file, { optional: true });
  if (value) requireThat(value.format === 1 && typeof value.target === 'string' && path.resolve(value.target) === path.resolve(allowedTarget)
    && Number.isSafeInteger(value.lastSequence) && value.lastSequence >= 0 && !value.pending, 'first_transition_module_pending');
  return { value, sha256: hash(value) };
}

async function oldRuntime(host) {
  const t = host.state.transaction, current = await host.original();
  requireThat(current.State.StartedAt === t.oldStartedAt, 'first_transition_outgoing_restarted');
  requireThat(current.Name === '/' + host.config.runtimeName || current.Name === '/' + t.previousName, 'first_transition_outgoing_name_changed');
  return current;
}

function configFor(host, ports, original, role) {
  const t = host.state.transaction, entry = t.request[role];
  // Build revision is the actual immutable application's revision, independent
  // of the later host-tools checkout. No image gets a rewritten label.
  const body = ports.candidateConfig(original, entry.image, t.id, { ...host.config, revision: entry.buildRevision }, t.nextTree);
  body.HostConfig.RestartPolicy = structuredClone(NO_RESTART);
  for (const name of ['SOTY_NATIVE_NOTES_ENABLED', 'SOTY_OAUTH_ENABLED']) {
    const values = (body.Env || []).filter(value => value.startsWith(name + '='));
    requireThat(values.length <= 1 && (!values.length || values[0] === name + '=0' || values[0] === name + '='), 'first_transition_admissions_not_off');
  }
  requireThat(preservationHash(body) === entry.configurationSha256, 'first_transition_configuration_changed');
  return body;
}

function validateRuntime(host, runtime, role) {
  const t = host.state.transaction, entry = t.request[role];
  const checked = structuredClone(runtime);
  if (t.restartEnabled === role && same(runtime.HostConfig.RestartPolicy, t.restartPolicy)) checked.HostConfig.RestartPolicy = structuredClone(NO_RESTART);
  requireThat(HASH.test(runtime?.Id || '') && runtime.Image === entry.image && runtime.Config?.Labels?.[LABEL] === t.id
    && runtime.Config.Labels[LABEL + '.original'] === t.oldId && runtime.Config.Labels['io.soty.connect.tree'] === t.nextTree
    && runtime.Config.Labels['org.opencontainers.image.revision'] === entry.buildRevision
    && same(checked.HostConfig.RestartPolicy, NO_RESTART)
    && preservationHash(createConfig(checked, entry.image, t.id, entry.buildRevision)) === entry.configurationSha256,
  'first_transition_runtime_changed');
}

async function candidate(host, ports, role) {
  let t = host.state.transaction;
  const name = `soty-connect-${role}-${t.id}`;
  const record = t.containers[role];
  if (!record) {
    const body = configFor(host, ports, await oldRuntime(host), role);
    await host.note(t.phase, { containers: { ...t.containers, [role]: { name, id: null } } });
    try { await host.engine.create(name, body); } catch { /* Resolve this intent once; never replay CREATE. */ }
  }
  t = host.state.transaction;
  const runtime = await host.poll(t.containers[role].id || name, c => c.Name === '/' + name || c.Name === '/' + host.config.runtimeName);
  validateRuntime(host, runtime, role);
  if (!t.containers[role].id) {
    requireThat(!runtime.State.Running && runtime.State.Status === 'created', 'first_transition_create_unresolved');
    await host.note(t.phase, { containers: { ...t.containers, [role]: { name, id: runtime.Id } } });
  }
  return runtime;
}

async function guardImages(host, request, tree) {
  for (const role of ['candidate', 'recovery']) {
    const entry = request[role], image = await host.engine.image(entry.image);
    requireThat(image.Id === entry.image && image.Config?.Labels?.['org.opencontainers.image.revision'] === entry.buildRevision
      && image.Config.Labels['io.soty.connect.tree'] === tree, 'first_transition_image_changed');
    storageReaders(image);
  }
  requireThat(IMAGE.test(host.config.storageProbeImage || '') && (await host.engine.image(host.config.storageProbeImage)).Id === host.config.storageProbeImage, 'storage_probe_image_required');
}

async function nextSource(host, request, ports) {
  for (const other of [host.config.stateDir, host.config.releaseDirectory]) {
    const relative = path.relative(request.handoff.sourceRoot, other);
    requireThat(relative === '..' || relative.startsWith('..' + path.sep) || path.isAbsolute(relative), 'first_transition_source_overlaps_control');
  }
  const fromState = path.relative(host.config.stateDir, request.handoff.sourceRoot);
  requireThat(fromState === '..' || fromState.startsWith('..' + path.sep) || path.isAbsolute(fromState), 'first_transition_source_overlaps_control');
  let cursor = request.handoff.sourceRoot;
  while (true) { requireThat(!(await lstat(cursor)).isSymbolicLink(), 'first_transition_source_symlink'); const parent = path.dirname(cursor); if (cursor === parent) break; cursor = parent; }
  requireThat(await realpath(request.handoff.sourceRoot) === path.resolve(request.handoff.sourceRoot), 'first_transition_source_alias');
  await host.checkSource({ ...host.config, sourceRoot: request.handoff.sourceRoot, revision: request.handoff.revision });
  const module = await ports.moduleTree(path.join(request.handoff.sourceRoot, 'modules', 'connect'));
  requireThat(module.tree === request.handoff.tree, 'first_transition_next_tree_changed'); return module;
}

async function handoff(host, ports) {
  let t = host.state.transaction;
  await nextSource(host, t.request, ports);
  const role = t.selected, entry = t.request[role], c = await candidate(host, ports, role);
  requireThat(c.State.Running && c.Name === '/' + host.config.runtimeName && !(await oldRuntime(host)).State.Running, 'first_transition_handoff_runtime_changed');
  await host.guardStart(c.Id, { running: true });
  const imageEntry = { image: entry.image, revision: entry.buildRevision, version: t.version, hasConnect: true };
  host.compareHealth(await host.ready({ entry: imageEntry, maintenance: false, idle: true }));
  const moduleFile = path.join(host.moduleStateDir, 'state.json'), nextTarget = path.join(t.request.handoff.sourceRoot, 'modules', 'connect');
  if (t.phase !== 'handoff') {
    const checkpoint = await moduleState(host);
    requireThat(checkpoint.sha256 === t.moduleStateSha256, 'first_transition_module_changed');
    const next = checkpoint.value ? { ...checkpoint.value, target: nextTarget } : null;
    await host.note('handoff', { handoff: { previousModuleState: checkpoint.value, nextModuleState: next,
      nextModuleStateSha256: hash(next), predecessor: { sourceRoot: host.state.sourceRoot, revision: host.state.revision, images: host.state.images, active: host.state.active } } });
  }
  t = host.state.transaction;
  const observed = await readFirstTransitionFile(moduleFile, { optional: true });
  requireThat(hash(observed) === t.moduleStateSha256 || hash(observed) === t.handoff.nextModuleStateSha256, 'first_transition_module_changed');
  if (hash(observed) !== t.handoff.nextModuleStateSha256) await host.write(moduleFile, t.handoff.nextModuleState);
  requireThat((await moduleState(host, nextTarget)).sha256 === t.handoff.nextModuleStateSha256, 'first_transition_module_handoff_failed');
  // A module-target write interrupted before host publication is resumed only
  // from this durable intent. Sequence and releaseHash are never changed.
  host.state.firstTransition = { id: t.id, request: t.request, selected: role, predecessor: t.handoff.predecessor,
    previousModuleState: t.handoff.previousModuleState, moduleStateSha256: t.moduleStateSha256,
    backup: t.backup, restoreEvidence: t.restoreEvidence };
  host.state.images = { [t.nextTree]: imageEntry };
  host.state.active = { tree: t.nextTree, image: entry.image, containerId: c.Id };
  host.state.sourceRoot = t.request.handoff.sourceRoot; host.state.revision = t.request.handoff.revision;
  host.state.lastTransaction = { id: t.id, mode: FIRST_TRANSITION, outcome: 'baseline_serving', backup: t.backup };
  host.state.transaction = null; await host.save();
  return { ok: true, status: 'baseline_serving', transactionId: t.id, recovery: role === 'recovery' };
}

async function runLocked(host, request, recover, ports) {
  if (host.state.transaction?.phase === 'handoff') {
    requireThat(host.state.transaction.mode === FIRST_TRANSITION && same(host.state.transaction.request, request) && !recover, 'first_transition_handoff_pending');
    await reconcileStorageProbe(host.storageContext()); await host.reconcileOperation();
    return handoff(host, ports);
  }
  const currentModule = await ports.moduleTree(host.target), checkpoint = await moduleState(host), nextModule = await nextSource(host, request, ports);
  if (!host.state.transaction) {
    if (host.state.firstTransition?.id === request.id) {
      requireThat(same(host.state.firstTransition.request, request) && !recover, 'first_transition_request_changed');
      await host.verifyActive(); return { ok: true, status: 'current', transactionId: request.id };
    }
    requireThat(!host.state.firstTransition && !recover, 'first_transition_already_used');
    const active = host.state.active, old = await host.engine.inspect(request.outgoing.id);
    requireThat(active.containerId === old.Id && active.image === old.Image && active.tree === currentModule.tree
      && old.Image === request.outgoing.image && old.State.Running && old.State.StartedAt === request.outgoing.startedAt
      && old.Name === '/' + host.config.runtimeName, 'first_transition_outgoing_changed');
    requireThat(ports.originalPreservationHash(old, old.Image, request.id) === request.outgoing.configurationSha256, 'first_transition_configuration_changed');
    await guardImages(host, request, nextModule.tree);
    const admission = checkedAdmission(await readFirstTransitionFile(request.admissionReceiptFile), request);
    const health = await host.ready({ entry: await host.imageEntry(active.tree), maintenance: false, idle: true });
    const status = await host.probe('status', old);
    requireThat(status.count === 0 && !status.maintenance, 'first_transition_not_quiescent');
    const tx = { mode: FIRST_TRANSITION, id: request.id, phase: 'prepared', request, admission,
      oldId: old.Id, oldImage: old.Image, oldTree: active.tree, oldStartedAt: old.State.StartedAt,
      oldConfigHash: request.outgoing.configurationSha256, restartPolicy: old.HostConfig.RestartPolicy,
      previousName: `${host.config.runtimeName}-previous-${request.id}`, nextTree: nextModule.tree, version: nextModule.version,
      moduleStateSha256: checkpoint.sha256, modelsHash: health.modelsHash, policyHash: health.policyHash,
      containers: {}, selected: 'candidate', operation: null, helper: null };
    tx.coldCheckpointSha256 = hash({ id: tx.id, oldId: tx.oldId, oldImage: tx.oldImage, oldConfigHash: tx.oldConfigHash,
      oldStartedAt: tx.oldStartedAt, tree: tx.oldTree, moduleStateSha256: tx.moduleStateSha256, candidate: request.candidate, recovery: request.recovery, handoff: request.handoff });
    host.state.transaction = tx;
    // The previous controller only accepts v1. Fence that entrypoint before it
    // could misinterpret this transaction and restore the historical image.
    host.state.schema = 'soty.connect.host.v2';
    // Validate both requested config hashes before the first Docker mutation.
    configFor(host, ports, old, 'candidate'); configFor(host, ports, old, 'recovery');
    await host.save();
  }
  let t = host.state.transaction;
  requireThat(t.mode === FIRST_TRANSITION && same(t.request, request) && t.oldTree === currentModule.tree
    && t.moduleStateSha256 === checkpoint.sha256 && ['prepared', 'outgoing_stopped', 'backing_up', 'backed_up', 'restore_verified', 'starting', 'serving_verified'].includes(t.phase), 'first_transition_request_changed');
  await guardImages(host, request, t.nextTree);
  await reconcileStorageProbe(host.storageContext());
  await host.reconcileOperation(); if (host.state.transaction.helper) await host.reconcileHelper();
  t = host.state.transaction;
  let old = await oldRuntime(host);
  if (t.phase === 'prepared') {
    // Prepare while O's observed network configuration is still available;
    // stopped C/R keep restart=no and never share live ports or write data.
    await candidate(host, ports, 'candidate'); await candidate(host, ports, 'recovery');
    if (!same(old.HostConfig.RestartPolicy, NO_RESTART)) await host.action('restartPolicy', old.Id, NO_RESTART, c => same(c.HostConfig.RestartPolicy, NO_RESTART));
    old = await oldRuntime(host); await host.ensureStopped(old);
    await host.note('outgoing_stopped');
  }
  old = await oldRuntime(host);
  requireThat(!old.State.Running && same(old.HostConfig.RestartPolicy, NO_RESTART), 'first_transition_outgoing_not_stopped');
  t = host.state.transaction;
  if (t.phase === 'outgoing_stopped') await host.backup(t.oldId);
  t = host.state.transaction;
  requireThat(t.phase !== 'backing_up', 'first_transition_backup_unresolved');
  const backup = checkedCompleteBackup(t.backup, t);
  if (!t.restoreEvidence) {
    const value = host.verifyFirstTransition
      ? await host.verifyFirstTransition({ transactionId: t.id, backup: structuredClone(backup), candidate: request.candidate, recovery: request.recovery, handoff: request.handoff })
      : await readFirstTransitionFile(request.restoreReceiptFile, { optional: true });
    if (value === null) return { ok: true, status: 'awaiting_restore_evidence', transactionId: t.id };
    await host.note('restore_verified', { restoreEvidence: checkedFirstRestore(value, t) });
  } else checkedFirstRestore(t.restoreEvidence, t);
  t = host.state.transaction;
  // Recovery is explicit. It stops C and uses the same current data volume;
  // this module has no data restore operation and can never replay B over it.
  if (recover && t.selected !== 'recovery') {
    if (t.containers.candidate) {
      const c = await candidate(host, ports, 'candidate');
      if (!same(c.HostConfig.RestartPolicy, NO_RESTART)) await host.action('restartPolicy', c.Id, NO_RESTART, x => same(x.HostConfig.RestartPolicy, NO_RESTART));
      if (c.State.Running) { const status = await host.probe('status', c); requireThat(status.count === 0 && (!status.maintenance || status.owned), 'first_transition_candidate_busy'); await host.ensureStopped(c); }
      if (c.Name === '/' + host.config.runtimeName) await host.action('rename', c.Id, t.containers.candidate.name, x => x.Name === '/' + t.containers.candidate.name);
    }
    await host.note('restore_verified', { selected: 'recovery' });
  }
  t = host.state.transaction;
  const role = t.selected, entry = request[role]; let c = await candidate(host, ports, role);
  if (!c.State.Running) {
    requireThat(!t.startAttempts?.[role], 'first_transition_start_not_replayed');
    await host.guardStart(c.Id);
    const status = await host.probe('status', c);
    requireThat(status.count === 0 && (!status.maintenance || status.owned), 'first_transition_candidate_busy');
    if (!status.maintenance) { const entered = await host.probe('enter', c); requireThat(entered.maintenance && entered.owned && entered.count === 0, 'first_transition_maintenance_failed'); }
    old = await oldRuntime(host);
    if (old.Name === '/' + host.config.runtimeName) await host.action('rename', old.Id, t.previousName, x => x.Name === '/' + t.previousName);
    c = await candidate(host, ports, role);
    if (c.Name !== '/' + host.config.runtimeName) await host.action('rename', c.Id, host.config.runtimeName, x => x.Name === '/' + host.config.runtimeName);
    await host.note('starting', { startAttempts: { ...t.startAttempts, [role]: true } });
    await host.action('start', c.Id, undefined, x => x.State.Running);
  }
  c = await candidate(host, ports, role); requireThat(c.State.Running && !(await oldRuntime(host)).State.Running, 'first_transition_runtime_not_running');
  const status = await host.probe('status', c);
  requireThat(status.count === 0 && (!status.maintenance || status.owned), 'first_transition_candidate_busy');
  const imageEntry = { image: entry.image, revision: entry.buildRevision, version: t.version, hasConnect: true };
  host.compareHealth(await host.ready({ entry: imageEntry, maintenance: status.maintenance, idle: true }));
  await host.guardStart(c.Id, { running: true });
  if (status.maintenance) { const cleared = await host.probe('leave', c); requireThat(!cleared.maintenance && cleared.count === 0, 'first_transition_marker_uncleared'); }
  await host.note('serving_verified', { restartEnabled: role });
  if (!same(c.HostConfig.RestartPolicy, t.restartPolicy)) await host.action('restartPolicy', c.Id, t.restartPolicy, x => same(x.HostConfig.RestartPolicy, t.restartPolicy));
  return handoff(host, ports);
}

/** Host lock is already held. Reuse the updater's existing lock/sequence domain. */
export async function runFirstTransition(host, input, { recover = false, ...ports } = {}) {
  const request = checkedFirstTransition(input);
  await mkdir(host.moduleStateDir, { recursive: true, mode: 0o700 });
  requireThat(!(await lstat(host.moduleStateDir)).isSymbolicLink() && await realpath(host.moduleStateDir) === path.resolve(host.moduleStateDir), 'first_transition_module_path_invalid');
  const file = path.join(host.moduleStateDir, 'update.lock'); let lock, identity;
  try { lock = await open(file, 'wx', 0o600); identity = await lock.stat(); }
  catch (error) { throw new SafeError(error.code === 'EEXIST' ? 'update_locked' : 'first_transition_lock_failed'); }
  try {
    await lock.writeFile(JSON.stringify({ pid: process.pid, target: host.target })); await lock.sync();
    return await runLocked(host, request, recover, ports);
  } finally {
    await lock.close(); const observed = await lstat(file);
    requireThat(observed.isFile() && !observed.isSymbolicLink() && observed.dev === identity.dev && observed.ino === identity.ino, 'first_transition_lock_changed');
    await unlink(file);
  }
}
