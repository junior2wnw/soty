import { readFile, writeFile, readdir, lstat, realpath, mkdir, open, rename, unlink } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { DockerApi, SafeError, httpJson } from '../connector/docker-api.mjs';
import { createConfig, preservationHash, hash } from '../connector/rollout.mjs';
import { modelReadiness } from '../connector/runtime.mjs';
// Pinned host code must stay available when the replaceable module is rolled back.
import { applyRelease, recoverRelease, fetchRelease, canonical, sha256 } from './update-engine.mjs';

const requireThat = (ok, code) => { if (!ok) throw new SafeError(code); };
const IMAGE = /^sha256:[a-f0-9]{64}$/;
const ID = /^[a-f0-9]{64}$/;
const TREE = /^[a-f0-9]{64}$/;
const LABEL = 'io.soty.connector-rollout';
const FEED = '/run/connect-releases';
const ORIGINS = ['https://xn--n1afe0b.online', 'https://soty.pochinit.online'];
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const exists = async file => { try { await lstat(file); return true; } catch (e) { if (e.code === 'ENOENT') return false; throw e; } };
const inside = (a, b) => { const relative = path.relative(a, b); return !relative || (!relative.startsWith(`..${path.sep}`) && relative !== '..' && !path.isAbsolute(relative)); };

export function validateConfig(input) {
  requireThat(input && typeof input === 'object', 'config_invalid');
  const c = { channel: 'stable', deploymentId: 'soty-dev', dockerSocket: '/var/run/docker.sock', ...input };
  let source, health;
  try { source = new URL(c.source); health = new URL(c.healthOrigin); } catch { throw new SafeError('config_url_invalid'); }
  requireThat(source.protocol === 'https:' && !source.username && !source.password && !source.hash, 'config_source_invalid');
  requireThat(health.protocol === 'http:' && ['127.0.0.1', '[::1]'].includes(health.hostname) && !health.username && !health.password && health.pathname === '/' && !health.search && !health.hash, 'config_health_invalid');
  for (const key of ['trustFile', 'sourceRoot', 'stateDir', 'releaseDirectory', 'dockerSocket']) requireThat(typeof c[key] === 'string' && path.isAbsolute(c[key]), 'config_path_invalid');
  const canary = c.canary === true;
  requireThat((canary ? /^soty-connect-canary-[a-z0-9-]+$/.test(c.runtimeName || '') && health.port !== '18182' : c.runtimeName === 'soty-online-chat' && health.port === '18182') && /^[a-f0-9]{40}$/.test(c.revision) && ['stable', 'preview'].includes(c.channel), 'config_runtime_invalid');
  requireThat(typeof c.initialRuntimeHasConnect === 'boolean', 'config_initial_runtime_required');
  requireThat(!inside(c.sourceRoot, c.stateDir) && !inside(c.stateDir, c.sourceRoot), 'config_state_must_be_external');
  requireThat(!inside(c.sourceRoot, c.releaseDirectory), 'config_feed_must_be_external');
  requireThat(Array.isArray(c.backupCommand) && c.backupCommand.length >= 2 && c.backupCommand.every(x => typeof x === 'string' && x && !x.includes('\0')) && path.isAbsolute(c.backupCommand[0]) && c.backupCommand.includes('{containerId}'), 'config_backup_command_invalid');
  return c;
}

async function safePaths(c) {
  for (const name of ['sourceRoot', 'stateDir', 'releaseDirectory', 'trustFile']) {
    let cursor = path.resolve(c[name]);
    while (true) {
      if (await exists(cursor)) requireThat(!(await lstat(cursor)).isSymbolicLink(), 'config_symlink');
      const parent = path.dirname(cursor); if (parent === cursor) break; cursor = parent;
    }
  }
  requireThat(await realpath(c.sourceRoot) === path.resolve(c.sourceRoot), 'config_source_alias');
}

export async function atomicState(file, value) {
  const tmp = `${file}.${process.pid}.${randomBytes(6).toString('hex')}.tmp`;
  const f = await open(tmp, 'wx', 0o600);
  try { await f.writeFile(JSON.stringify(value) + '\n'); await f.sync(); } finally { await f.close(); }
  await rename(tmp, file);
  // The production controller is Linux. Windows is supported only by unit tests.
  if (process.platform !== 'win32') { const dir = await open(path.dirname(file), 'r'); try { await dir.sync(); } finally { await dir.close(); } }
}

export async function moduleTree(directory) {
  const files = [];
  async function walk(dir, prefix = '') {
    for (const entry of await readdir(dir, { withFileTypes: true })) {
      if (entry.name.startsWith('.') || ['node_modules', 'data', 'output'].includes(entry.name)) continue;
      const name = prefix + entry.name;
      requireThat(!entry.isSymbolicLink(), 'module_symlink');
      if (entry.isDirectory()) await walk(path.join(dir, entry.name), name + '/');
      else { requireThat(entry.isFile(), 'module_file_invalid'); files.push({ path: name, hash: sha256(await readFile(path.join(dir, entry.name))) }); }
    }
  }
  await walk(directory); files.sort((a, b) => a.path.localeCompare(b.path));
  const pkg = JSON.parse(await readFile(path.join(directory, 'package.json'), 'utf8'));
  requireThat(pkg.name === '@soty/connect' && /^\d+\.\d+\.\d+$/.test(pkg.version), 'module_package_invalid');
  return { tree: sha256(canonical(files)), version: pkg.version };
}

// Stdout is returned only to internal parsers; stderr and child error text never escape.
export function runCommand(argv, { cwd, timeoutMs = 60_000, capture = false } = {}) {
  return new Promise((resolve, reject) => {
    let done = false, total = 0; const chunks = [];
    const finish = (error, value) => { if (done) return; done = true; clearTimeout(timer); error ? reject(new SafeError(error)) : resolve(value); };
    const child = spawn(argv[0], argv.slice(1), { cwd, shell: false, windowsHide: true, stdio: ['ignore', capture ? 'pipe' : 'ignore', 'ignore'] });
    const timer = setTimeout(() => { child.kill('SIGKILL'); finish('command_timeout'); }, timeoutMs);
    if (capture) child.stdout.on('data', chunk => { total += chunk.length; if (total > 1024 * 1024) { child.kill('SIGKILL'); finish('command_output_limit'); } else chunks.push(chunk); });
    child.on('error', () => finish('command_start_failed'));
    child.on('exit', code => finish(code === 0 ? null : 'command_failed', capture ? Buffer.concat(chunks).toString('utf8') : undefined));
  });
}

export function candidateConfig(original, image, transaction, c, moduleTreeHash) {
  const result = createConfig(original, image, transaction, c.revision);
  if (moduleTreeHash !== undefined) {
    requireThat(TREE.test(moduleTreeHash), 'candidate_tree_invalid');
    result.Labels['io.soty.connect.tree'] = moduleTreeHash;
  }
  for (const mount of original.Mounts || []) requireThat(!/docker\.sock$/i.test(mount.Source || '') && !/docker\.sock$/i.test(mount.Destination || ''), 'runtime_docker_socket_forbidden');
  const env = new Map();
  for (const item of result.Env || []) { const i = item.indexOf('='); requireThat(i > 0 && !env.has(item.slice(0, i)), 'runtime_env_duplicate'); env.set(item.slice(0, i), item.slice(i + 1)); }
  env.set('SOTY_CONNECT_RELEASE_DIR', FEED);
  const origins = (env.get('SOTY_CONNECT_ORIGINS') || '').split(',').map(x => x.trim()).filter(Boolean);
  env.set('SOTY_CONNECT_ORIGINS', [...new Set([...origins, ...ORIGINS])].join(','));
  result.Env = [...env].map(([key, value]) => `${key}=${value}`);
  const realised = (original.Mounts || []).filter(m => m.Destination === FEED);
  requireThat(realised.length <= 1, 'runtime_feed_mount_conflict');
  if (realised.length) requireThat(realised[0].Type === 'bind' && realised[0].Source === c.releaseDirectory && realised[0].RW === false, 'runtime_feed_mount_conflict');
  else {
    requireThat(!(result.HostConfig.Mounts || []).some(m => m.Target === FEED) && !(result.HostConfig.Binds || []).some(m => m.split(':')[1] === FEED), 'runtime_feed_mount_conflict');
    result.HostConfig.Mounts ||= [];
    result.HostConfig.Mounts.push({ Type: 'bind', Source: c.releaseDirectory, Target: FEED, ReadOnly: true });
  }
  return result;
}

function statusValue(s) {
  requireThat(s?.ok === true && s.schema === 'soty.connect.maintenance.v1' && Number.isSafeInteger(s.count) && s.count >= 0 && typeof s.maintenance === 'boolean' && typeof s.owned === 'boolean', 'maintenance_status_invalid');
  return { ok: true, schema: s.schema, count: s.count, maintenance: s.maintenance, owned: s.owned };
}

function decodeDocker(bytes) {
  const parts = []; let offset = 0;
  while (offset < bytes.length) { requireThat(offset + 8 <= bytes.length, 'probe_output_invalid'); const length = bytes.readUInt32BE(offset + 4); requireThat(length <= bytes.length - offset - 8, 'probe_output_invalid'); if (bytes[offset] === 1) parts.push(bytes.subarray(offset + 8, offset + 8 + length)); offset += 8 + length; }
  const result = Buffer.concat(parts); requireThat(result.length <= 4096, 'probe_output_limit');
  try { return statusValue(JSON.parse(result.toString('utf8'))); } catch { throw new SafeError('probe_output_invalid'); }
}

export function productionReady(c, { get = httpJson, pause = sleep, deadlineMs = 60_000 } = {}) {
  return async ({ entry, maintenance, idle = false }) => {
    const deadline = Date.now() + deadlineMs;
    while (Date.now() < deadline) {
      try {
        const health = await get(c.healthOrigin, '/health', 5000);
        requireThat(health?.ok === true, 'health_failed');
        if (idle) for (const proxy of [health.agentModelProxy, health.applicationModelProxy]) {
          requireThat((proxy?.providers || []).every(provider => provider.active === 0 && provider.queued === 0), 'inference_busy');
        }
        const models = modelReadiness(health), policy = health.applicationModelProxy?.policySha256 || null;
        requireThat(policy === null || TREE.test(policy), 'policy_hash_invalid');
        const storage = await get(c.healthOrigin, '/api/connectors/storage-ready', 5000);
        requireThat(storage?.ok === true && storage.schema === 'soty.connector-storage-ready.v1' && storage.storageReady === true && storage.maintenance === maintenance, 'storage_not_ready');
        if (entry.hasConnect) {
          const capabilities = await get(c.healthOrigin, '/api/connect/capabilities', 5000);
          requireThat(capabilities?.module === '@soty/connect' && capabilities.version === entry.version && capabilities.protocol === 1 && capabilities.projectId === 'soty' && capabilities.encryptedWorkspace === true, 'connect_not_ready');
        }
        return { modelsHash: hash(models), policyHash: policy };
      } catch { /* Retry only bounded read-only probes; errors never include bodies. */ }
      await pause(250);
    }
    throw new SafeError('readiness_deadline');
  };
}

export class HostController {
  constructor(config, dependencies = {}) {
    this.config = validateConfig(config);
    this.engine = dependencies.engine || new DockerApi({ socketPath: this.config.dockerSocket, timeoutMs: 30_000 });
    this.command = dependencies.command || runCommand;
    this.ready = dependencies.ready || productionReady(this.config);
    this.pause = dependencies.pause || sleep;
    this.write = dependencies.write || atomicState;
    this.fetch = dependencies.fetch || fetchRelease;
    this.apply = dependencies.apply || applyRelease;
    this.recover = dependencies.recover || recoverRelease;
    this.probeOverride = dependencies.probe;
    this.checkSourceOverride = dependencies.checkSource;
    this.target = path.join(this.config.sourceRoot, 'modules', 'connect');
    this.file = path.join(this.config.stateDir, 'host-state.json');
    this.moduleStateDir = path.join(this.config.stateDir, 'module-update');
  }
  async save() { await this.write(this.file, this.state); }
  async note(phase, fields = {}) { this.state.transaction = { ...this.state.transaction, ...fields, phase }; await this.save(); }
  async checkSource() {
    if (this.checkSourceOverride) return this.checkSourceOverride();
    const head = (await this.command(['git', 'rev-parse', 'HEAD'], { cwd: this.config.sourceRoot, capture: true })).trim();
    requireThat(head === this.config.revision, 'source_revision_changed');
    const paths = await this.command(['git', 'status', '--porcelain=v1', '--untracked-files=all'], { cwd: this.config.sourceRoot, capture: true });
    for (const line of paths.split(/\r?\n/).filter(Boolean)) requireThat(/^.. modules\/connect(?:\/|\.(?:stage-|previous-))/.test(line), 'source_host_changed');
  }
  async poll(id, predicate) {
    for (let i = 0; i < 20; i++) { try { const c = await this.engine.inspect(id); if (predicate(c)) return c; } catch {} if (i < 19) await this.pause(250); }
    throw new SafeError('docker_operation_unresolved');
  }
  async action(kind, id, value, predicate) {
    await this.note(this.state.transaction.phase, { operation: { kind, id, ...(value === undefined ? {} : { value }) } });
    try {
      if (kind === 'restartPolicy') await this.engine.request('POST', `/containers/${id}/update`, { RestartPolicy: value });
      else await this.engine[kind](id, value);
    } catch { /* A lost response is not evidence of failure. */ }
    const result = await this.poll(id, predicate);
    await this.note(this.state.transaction.phase, { operation: null }); return result;
  }
  async reconcileOperation() {
    const o = this.state.transaction?.operation; if (!o) return;
    const predicates = { stop: c => !c.State.Running, start: c => c.State.Running, rename: c => c.Name === '/' + o.value, restartPolicy: c => hash(c.HostConfig.RestartPolicy) === hash(o.value) };
    requireThat(predicates[o.kind] && ID.test(o.id), 'host_operation_invalid');
    // Never resubmit a delayed mutation. An unobserved result requires an operator.
    await this.poll(o.id, predicates[o.kind]); await this.note(this.state.transaction.phase, { operation: null });
  }
  async probe(verb, runtime) {
    if (this.probeOverride) return statusValue(await this.probeOverride(verb, runtime, this));
    const tx = this.state?.transaction;
    const script = await readFile(new URL('./maintenance-probe.mjs', import.meta.url), 'utf8');
    if (verb === 'status' && runtime.State.Running) {
      const created = await this.engine.request('POST', `/containers/${runtime.Id}/exec`, { AttachStdout: true, AttachStderr: false, Tty: false, Env: ['SOTY_CONNECT_PROBE=1'], Cmd: ['node', '--input-type=module', '-e', script, 'status', tx?.id || ''] });
      requireThat(ID.test(created?.Id || ''), 'probe_exec_invalid');
      const bytes = await this.engine.request('POST', `/exec/${created.Id}/start`, { Detach: false, Tty: false }, true);
      const end = await this.engine.request('GET', `/exec/${created.Id}/json`);
      requireThat(!end.Running && end.ExitCode === 0, 'probe_exec_failed'); return decodeDocker(bytes);
    }
    requireThat(tx, 'offline_probe_transaction_missing');
    if (tx.helper) await this.reconcileHelper();
    const seq = (this.state.transaction.helperSequence || 0) + 1;
    const name = `soty-connect-helper-${tx.id}-${seq}`;
    const source = createConfig(runtime, runtime.Image, tx.id);
    const body = { Image: runtime.Image, User: source.User, WorkingDir: source.WorkingDir || '/app', Env: [...(source.Env || []).filter(v => !v.startsWith('SOTY_CONNECT_PROBE=')), 'SOTY_CONNECT_PROBE=1'], Entrypoint: ['node'], Cmd: ['--input-type=module', '-e', script, verb, tx.id], Tty: false, Labels: { 'io.soty.connect.helper': tx.id, 'io.soty.connect.verb': verb }, HostConfig: { Binds: source.HostConfig.Binds, Mounts: source.HostConfig.Mounts, GroupAdd: source.HostConfig.GroupAdd, UsernsMode: source.HostConfig.UsernsMode, NetworkMode: 'none', RestartPolicy: { Name: 'no' }, ReadonlyRootfs: true, Memory: 268435456, NanoCpus: 500000000, PidsLimit: 32, CapDrop: ['ALL'], SecurityOpt: ['no-new-privileges'], Tmpfs: { '/tmp': 'rw,noexec,nosuid,size=16777216' } }, NetworkingConfig: { EndpointsConfig: {} } };
    await this.note(tx.phase, { helperSequence: seq, helper: { name, verb, image: runtime.Image, phase: 'creating' } });
    try { await this.engine.create(name, body); } catch {}
    const created = await this.poll(name, c => c.Name === '/' + name && c.Image === runtime.Image && c.Config.Labels?.['io.soty.connect.helper'] === tx.id);
    requireThat(created.State.Status === 'created' && !created.State.Running, 'maintenance_helper_unresolved');
    await this.note(tx.phase, { helper: { ...this.state.transaction.helper, id: created.Id, phase: 'starting' } });
    try { await this.engine.start(created.Id); } catch {}
    return this.reconcileHelper();
  }
  async reconcileHelper() {
    const h = this.state.transaction?.helper; if (!h) return;
    const c = await this.poll(h.id || h.name, c => c.Name === '/' + h.name && c.Image === h.image && c.Config.Labels?.['io.soty.connect.helper'] === this.state.transaction.id && !c.State.Running && c.State.Status === 'exited');
    requireThat(c.State.ExitCode === 0, 'maintenance_helper_failed');
    const value = statusValue(await this.engine.helperOutput(c.Id));
    // Retain helpers (no live process, no published ports) as reconciliation evidence.
    await this.note(this.state.transaction.phase, { helper: null }); return value;
  }
  async imageEntry(tree) {
    const entry = this.state.images[tree]; requireThat(entry && IMAGE.test(entry.image), 'image_mapping_missing');
    const image = await this.engine.image(entry.image); requireThat(image.Id === entry.image, 'mapped_image_missing'); return entry;
  }
  async validate(directory) {
    await this.checkSource(); const module = await moduleTree(directory);
    if (this.state.images[module.tree]) { const existing = await this.imageEntry(module.tree); requireThat(existing.hasConnect, 'baseline_image_cannot_activate_connect'); return; }
    const iidfile = path.join(this.config.stateDir, `image-${module.tree}.iid`);
    requireThat(!await exists(iidfile), 'build_receipt_exists');
    await this.command(['docker', '--host', `unix://${this.config.dockerSocket}`, 'build', '--build-arg', `REVISION=${this.config.revision}`, '--label', `io.soty.connect.tree=${module.tree}`, '--iidfile', iidfile, '--file', path.join(this.config.sourceRoot, 'Dockerfile'), this.config.sourceRoot], { cwd: this.config.sourceRoot, timeoutMs: 20 * 60_000 });
    const imageId = (await readFile(iidfile, 'utf8')).trim(); requireThat(IMAGE.test(imageId), 'build_image_invalid');
    const image = await this.engine.image(imageId);
    requireThat(image.Id === imageId && image.Config?.Labels?.['org.opencontainers.image.revision'] === this.config.revision && image.Config?.Labels?.['io.soty.connect.tree'] === module.tree, 'built_image_guard_failed');
    // This map is durable before either the updater or Docker is allowed to activate it.
    this.state.images[module.tree] = { image: imageId, version: module.version, hasConnect: true, revision: this.config.revision };
    await this.save();
  }
  validateCandidate(c) {
    const t = this.state.transaction;
    const checked = structuredClone(c);
    // Only our durable rollback intent permits this exact restart-policy change.
    // A crash after Docker applies it must not make the retained candidate foreign.
    if (t.candidateRestartDisabled === true && c.Id === t.nextId && hash(c.HostConfig.RestartPolicy) === hash({ Name: 'no', MaximumRetryCount: 0 })) checked.HostConfig.RestartPolicy = t.restartPolicy;
    requireThat(c.Image === t.nextImage && c.Config.Labels?.[LABEL] === t.id && c.Config.Labels?.[LABEL + '.original'] === t.oldId && c.Config.Labels?.['io.soty.connect.tree'] === t.nextTree && preservationHash(createConfig(checked, t.nextImage, t.id, this.config.revision)) === t.configHash, 'candidate_identity_changed');
  }
  async original() {
    const t = this.state.transaction; const c = await this.engine.inspect(t.oldId);
    requireThat(c.Id === t.oldId && c.Image === t.oldImage, 'original_identity_changed');
    const clone = structuredClone(c); clone.HostConfig.RestartPolicy = t.restartPolicy;
    requireThat(preservationHash(createConfig(clone, t.oldImage, t.id)) === t.oldConfigHash, 'original_configuration_changed'); return c;
  }
  async ensureStopped(c) { if (c.State.Running) await this.action('stop', c.Id, undefined, x => !x.State.Running); else requireThat(!c.State.Running, 'container_stop_unconfirmed'); }
  async backup(containerId) {
    await this.note('backing_up');
    const stdout = await this.command(this.config.backupCommand.map(v => v === '{containerId}' ? containerId : v), { cwd: this.config.sourceRoot, timeoutMs: 300_000, capture: true });
    let receipt; try { receipt = JSON.parse(stdout); } catch { throw new SafeError('backup_receipt_invalid'); }
    requireThat(receipt?.ok === true && receipt.encrypted === true && path.isAbsolute(receipt.receiptPath || '') && TREE.test(receipt.sha256 || ''), 'backup_receipt_invalid');
    await this.note('backed_up', { backup: { receiptPath: receipt.receiptPath, sha256: receipt.sha256, encrypted: true } });
  }
  async activate(directory) {
    const module = await moduleTree(directory), entry = await this.imageEntry(module.tree);
    if (this.state.transaction) {
      await this.reconcileOperation();
      if (this.state.transaction.helper) await this.reconcileHelper();
      requireThat(module.tree === this.state.transaction.oldTree, 'activation_recovery_target_invalid');
      await this.restore(); return;
    }
    if (this.state.active.tree === module.tree) {
      const current = await this.engine.inspect(this.state.active.containerId);
      requireThat(current.State.Running && current.Image === entry.image && current.Name === '/' + this.config.runtimeName, 'active_runtime_changed'); return;
    }
    const original = await this.engine.inspect(this.config.runtimeName);
    requireThat(original.Id === this.state.active.containerId && original.Image === this.state.active.image && original.State.Running, 'active_runtime_changed');
    const id = randomBytes(16).toString('hex'), config = candidateConfig(original, entry.image, id, this.config, module.tree);
    const oldEntry = await this.imageEntry(this.state.active.tree);
    const health = await this.ready({ entry: oldEntry, maintenance: false, idle: true });
    const preflight = await this.probe('status', original);
    requireThat(preflight.count === 0 && !preflight.maintenance, 'precheck_not_quiescent');
    this.state.transaction = { id, phase: 'preparing', oldId: original.Id, oldImage: original.Image, oldTree: this.state.active.tree, nextTree: module.tree, nextImage: entry.image, candidateName: 'soty-connect-next-' + id, previousName: 'soty-connect-previous-' + id, restartPolicy: original.HostConfig.RestartPolicy || { Name: 'no' }, oldConfigHash: preservationHash(createConfig(original, original.Image, id)), configHash: preservationHash(config), modelsHash: health.modelsHash, policyHash: health.policyHash, operation: null, helper: null };
    await this.save();
    try {
      await this.note('creating_candidate');
      try { await this.engine.create(this.state.transaction.candidateName, config); } catch {}
      const next = await this.poll(this.state.transaction.candidateName, c => c.Name === '/' + this.state.transaction.candidateName);
      this.validateCandidate(next); requireThat(!next.State.Running && next.State.Status === 'created', 'candidate_already_started');
      await this.note('prepared', { nextId: next.Id });
      const old = await this.original();
      const before = await this.probe('status', old);
      requireThat(before.count === 0 && !before.maintenance, 'precheck_not_quiescent');
      this.compareHealth(await this.ready({ entry: oldEntry, maintenance: false, idle: true }));
      await this.note('stopping_original'); await this.ensureStopped(old);
      await this.note('original_stopped');
      let offline = await this.probe('status', await this.original());
      requireThat(offline.count === 0 && !offline.maintenance, 'offline_admission_race');
      await this.backup(old.Id);
      offline = await this.probe('enter', await this.original());
      requireThat(offline.count === 0 && offline.maintenance && offline.owned, 'maintenance_enter_failed');
      await this.note('maintenance_entered');
      await this.action('restartPolicy', old.Id, { Name: 'no', MaximumRetryCount: 0 }, c => c.HostConfig.RestartPolicy.Name === 'no');
      await this.action('rename', old.Id, this.state.transaction.previousName, c => c.Name === '/' + this.state.transaction.previousName);
      await this.action('rename', next.Id, this.config.runtimeName, c => c.Name === '/' + this.config.runtimeName);
      await this.note('starting_candidate', { candidateStartAttempted: true });
      requireThat(!(await this.original()).State.Running, 'original_restarted');
      await this.action('start', next.Id, undefined, c => c.State.Running);
      await this.note('candidate_started');
      this.validateCandidate(await this.engine.inspect(next.Id));
      const fresh = await this.ready({ entry, maintenance: true }); this.compareHealth(fresh);
      const status = await this.probe('status', await this.engine.inspect(next.Id));
      requireThat(status.count === 0 && status.maintenance && status.owned, 'candidate_not_quiescent');
      await this.note('releasing_maintenance');
      const released = await this.probe('leave', await this.engine.inspect(next.Id));
      requireThat(!released.maintenance, 'maintenance_leave_failed');
      await this.note('admitted');
    } catch (error) {
      // applyRelease restores the code first, then calls this.activate(oldTree).
      // Unresolved Docker mutations are kept durable and block that restoration.
      throw new SafeError(/^[a-z_]+$/.test(error?.code || '') ? error.code : 'activation_failed');
    }
  }
  compareHealth(value) { const t = this.state.transaction; requireThat(value.modelsHash === t.modelsHash && value.policyHash === t.policyHash, 'runtime_model_readiness_changed'); }
  async restore() {
    const t = this.state.transaction; await this.reconcileOperation();
    if (t.helper) await this.reconcileHelper();
    let next;
    if (t.nextId) { next = await this.engine.inspect(t.nextId); this.validateCandidate(next); }
    else if (t.phase === 'creating_candidate') {
      // A CREATE may still be pending. Never infer absence from a transient404.
      next = await this.poll(t.candidateName, c => c.Name === '/' + t.candidateName);
      this.validateCandidate(next); await this.note(t.phase, { nextId: next.Id });
    }
    let old = await this.original();
    if (next) {
      // Fence daemon-driven restart before STOP, including a resumed rollback.
      await this.note(this.state.transaction.phase, { candidateRestartDisabled: true });
      await this.action('restartPolicy', next.Id, { Name: 'no', MaximumRetryCount: 0 }, c => hash(c.HostConfig.RestartPolicy) === hash({ Name: 'no', MaximumRetryCount: 0 }));
      next = await this.engine.inspect(next.Id);
    }
    if (next?.State.Running) {
      const status = await this.probe('status', next);
      requireThat(status.count === 0 && (!status.maintenance || status.owned), 'rollback_active_jobs');
      await this.ensureStopped(next);
      const offline = await this.probe('status', await this.engine.inspect(next.Id));
      requireThat(offline.count === 0 && (!offline.maintenance || offline.owned), 'rollback_admission_race');
    }
    old = await this.original();
    if (!old.State.Running) {
      const current = await this.probe('status', old);
      requireThat((!t.candidateStartAttempted || current.count === 0) && (!current.maintenance || current.owned), 'rollback_not_quiescent');
      // The same latest SQLite files stay mounted; no backup or JSON is restored.
      if (!current.maintenance && t.candidateStartAttempted) {
        const entered = await this.probe('enter', old); requireThat(entered.owned && entered.maintenance, 'rollback_marker_failed');
      }
      if (next?.Name === '/' + this.config.runtimeName) await this.action('rename', next.Id, t.candidateName, c => c.Name === '/' + t.candidateName);
      if (old.Name !== '/' + this.config.runtimeName) await this.action('rename', old.Id, this.config.runtimeName, c => c.Name === '/' + this.config.runtimeName);
      await this.action('restartPolicy', old.Id, t.restartPolicy, c => hash(c.HostConfig.RestartPolicy) === hash(t.restartPolicy));
      await this.note('restoring_original');
      if (next) await this.ensureStopped(await this.engine.inspect(next.Id));
      await this.action('start', old.Id, undefined, c => c.State.Running && c.Image === t.oldImage);
      const marked = current.maintenance || Boolean(t.candidateStartAttempted);
      const fresh = await this.ready({ entry: await this.imageEntry(t.oldTree), maintenance: marked }); this.compareHealth(fresh);
      if (marked) { const cleared = await this.probe('leave', await this.original()); requireThat(!cleared.maintenance, 'restored_marker_uncleared'); }
    } else {
      requireThat(!next?.State.Running, 'two_writers_detected');
      const status = await this.probe('status', old);
      requireThat(!status.maintenance || status.owned, 'external_maintenance');
      if (status.owned) { this.compareHealth(await this.ready({ entry: await this.imageEntry(t.oldTree), maintenance: true })); const cleared = await this.probe('leave', old); requireThat(!cleared.maintenance, 'restored_marker_uncleared'); }
      else this.compareHealth(await this.ready({ entry: await this.imageEntry(t.oldTree), maintenance: false }));
    }
    await this.note('restored');
  }
  async settle() {
    const t = this.state.transaction; if (!t) return;
    requireThat(!t.operation && !t.helper, 'host_recovery_required');
    const current = await moduleTree(this.target);
    if (t.phase === 'admitted' && current.tree === t.nextTree) {
      const c = await this.engine.inspect(t.nextId); this.validateCandidate(c);
      requireThat(c.State.Running && c.Name === '/' + this.config.runtimeName && !(await this.original()).State.Running, 'admitted_runtime_invalid');
      this.compareHealth(await this.ready({ entry: await this.imageEntry(t.nextTree), maintenance: false }));
      this.state.active = { tree: t.nextTree, image: t.nextImage, containerId: t.nextId };
    } else if (t.phase === 'restored' && current.tree === t.oldTree) {
      const c = await this.original(); requireThat(c.State.Running && c.Name === '/' + this.config.runtimeName, 'restored_runtime_invalid');
      this.state.active = { tree: t.oldTree, image: t.oldImage, containerId: t.oldId };
    } else throw new SafeError('host_recovery_required');
    this.state.lastTransaction = { id: t.id, outcome: t.phase, ...(t.backup ? { backup: t.backup } : {}) };
    this.state.transaction = null; await this.save();
  }
  async load() {
    if (await exists(this.file)) {
      this.state = JSON.parse(await readFile(this.file, 'utf8'));
      requireThat(this.state.schema === 'soty.connect.host.v1' && this.state.revision === this.config.revision && this.state.sourceRoot === this.config.sourceRoot && this.state.images && this.state.active && TREE.test(this.state.active.tree) && IMAGE.test(this.state.active.image) && ID.test(this.state.active.containerId), 'host_state_invalid');
      for (const [tree, entry] of Object.entries(this.state.images)) requireThat(TREE.test(tree) && IMAGE.test(entry.image) && typeof entry.hasConnect === 'boolean', 'host_mapping_invalid');
      return;
    }
    requireThat(!await exists(path.join(this.moduleStateDir, 'state.json')), 'host_state_missing');
    const current = await this.engine.inspect(this.config.runtimeName), module = await moduleTree(this.target);
    requireThat(ID.test(current.Id) && IMAGE.test(current.Image) && current.State.Running && current.Name === '/' + this.config.runtimeName, 'baseline_runtime_invalid');
    const bindings = current.HostConfig.PortBindings?.['8080/tcp'] || [];
    requireThat(bindings.some(b => b.HostPort === new URL(this.config.healthOrigin).port && ['127.0.0.1', '::1', ''].includes(b.HostIp || '')), 'runtime_health_binding_mismatch');
    candidateConfig(current, current.Image, '0'.repeat(32), this.config);
    const entry = { image: current.Image, version: module.version, hasConnect: this.config.initialRuntimeHasConnect, revision: this.config.revision, baseline: true };
    await this.ready({ entry, maintenance: false });
    const status = await this.probe('status', current); requireThat(status.count === 0 && !status.maintenance, 'baseline_not_quiescent');
    this.state = { schema: 'soty.connect.host.v1', revision: this.config.revision, sourceRoot: this.config.sourceRoot, images: { [module.tree]: entry }, active: { tree: module.tree, image: current.Image, containerId: current.Id }, transaction: null };
    await this.save();
  }
  async verifyActive() {
    const active = this.state.active, entry = await this.imageEntry(active.tree);
    const module = await moduleTree(this.target), runtime = await this.engine.inspect(active.containerId);
    requireThat(module.tree === active.tree && entry.image === active.image && runtime.Image === entry.image && runtime.State.Running && runtime.Name === '/' + this.config.runtimeName, 'active_runtime_changed');
    await this.ready({ entry, maintenance: false });
  }
  async run({ bootstrapRelease } = {}) {
    await safePaths(this.config); await mkdir(this.config.stateDir, { recursive: true, mode: 0o700 });
    const lockFile = path.join(this.config.stateDir, 'host-controller.lock'); let lock;
    try { lock = await open(lockFile, 'wx', 0o600); } catch (error) { if (error.code === 'EEXIST') throw new SafeError('host_locked_operator_verification_required'); throw new SafeError('host_lock_failed'); }
    let failed;
    try {
      await lock.writeFile(JSON.stringify({ pid: process.pid, revision: this.config.revision })); await lock.sync();
      await this.load();
      await this.recover({ target: this.target, stateDir: this.moduleStateDir, activate: target => this.activate(target) });
      await this.settle();
      await this.verifyActive();
      await this.checkSource();
      let release;
      if (bootstrapRelease) {
        requireThat(path.isAbsolute(bootstrapRelease) && this.config.initialRuntimeHasConnect === false && this.state.images[this.state.active.tree]?.baseline === true, 'bootstrap_not_initial');
        const moduleStateFile = path.join(this.moduleStateDir, 'state.json');
        const moduleState = await exists(moduleStateFile) ? JSON.parse(await readFile(moduleStateFile, 'utf8')) : { lastSequence: 0 };
        requireThat(moduleState.lastSequence === 0 && !moduleState.pending, 'bootstrap_not_initial');
        requireThat((await lstat(bootstrapRelease)).isFile() && (await lstat(bootstrapRelease)).size <= 12 * 1024 * 1024, 'bootstrap_release_invalid');
        release = JSON.parse(await readFile(bootstrapRelease, 'utf8'));
      } else release = await this.fetch(this.config.source);
      const trust = JSON.parse(await readFile(this.config.trustFile, 'utf8'));
      const result = await this.apply({ target: this.target, stateDir: this.moduleStateDir, release, trust, deploymentId: this.config.deploymentId, channel: this.config.channel, validate: target => this.validate(target), activate: target => this.activate(target) });
      await this.settle(); return { ok: true, status: result.status, version: result.version };
    } catch (error) {
      failed = error;
      // A successful updater rollback may have completed the runtime transaction.
      if (this.state?.transaction?.phase === 'restored') await this.settle().catch(() => {});
      throw new SafeError(/^[a-z0-9_]+$/.test(error?.code || '') ? error.code : 'controller_failed');
    } finally {
      try { await lock.close(); await unlink(lockFile); } catch { if (!failed) throw new SafeError('host_lock_release_failed'); }
    }
  }
}

export async function main(argv = process.argv.slice(2)) {
  requireThat(argv.length === 1 || (argv.length === 3 && argv[1] === '--bootstrap-release'), 'usage_config_path_required');
  requireThat(path.isAbsolute(argv[0]), 'config_path_absolute_required');
  const c = JSON.parse(await readFile(argv[0], 'utf8'));
  return new HostController(c).run({ bootstrapRelease: argv[2] });
}
if (process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url) {
  try { console.log(JSON.stringify(await main())); }
  catch (error) { console.error(JSON.stringify({ ok: false, code: /^[a-z0-9_]+$/.test(error?.code || '') ? error.code : 'controller_failed' })); process.exitCode = 1; }
}
