import { constants } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { lstat, realpath, mkdir, open, link, unlink } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { DockerApi, SafeError } from '../connector/docker-api.mjs';
import { HostController, validateConfig, moduleTree, candidateConfig, productionReady } from './host-controller.mjs';
import { sha256 } from './update-engine.mjs';

const requireThat = (ok, code) => { if (!ok) throw new SafeError(code); };
const HASH = /^[a-f0-9]{64}$/;
const IMAGE = /^sha256:[a-f0-9]{64}$/;
const REVISION = /^[a-f0-9]{40}$/;
const VERSION = /^\d+\.\d+\.\d+$/;
const LIMIT = 2 * 1024 * 1024;
const samePath = (a, b) => typeof a === 'string' && typeof b === 'string' && path.isAbsolute(a) && path.isAbsolute(b) && path.relative(a, b) === '';
const inside = (root, file) => { const relative = path.relative(root, file); return !relative || (relative !== '..' && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative)); };
const overlaps = (a, b) => inside(a, b) || inside(b, a);
const exists = async file => { try { await lstat(file); return true; } catch (error) { if (error.code === 'ENOENT') return false; throw error; } };

async function location(file) {
  requireThat(typeof file === 'string' && path.isAbsolute(file) && !file.includes('\0'), 'rebase_absolute_paths_required');
  let current = path.resolve(file); const missing = [];
  while (!await exists(current)) { missing.unshift(path.basename(current)); current = path.dirname(current); }
  let ancestor = current;
  while (true) {
    requireThat(!(await lstat(ancestor)).isSymbolicLink(), 'rebase_symlink');
    const parent = path.dirname(ancestor); if (parent === ancestor) break; ancestor = parent;
  }
  return path.join(await realpath(current), ...missing);
}

async function regularBytes(file) {
  const before = await lstat(file);
  requireThat(before.isFile() && !before.isSymbolicLink() && before.size <= LIMIT, 'rebase_file_invalid');
  const handle = await open(file, constants.O_RDONLY | (constants.O_NOFOLLOW || 0));
  try {
    const opened = await handle.stat();
    requireThat(opened.isFile() && opened.dev === before.dev && opened.ino === before.ino && opened.size <= LIMIT, 'rebase_file_changed');
    const buffer = Buffer.alloc(LIMIT + 1); let length = 0;
    while (length < buffer.length) {
      const { bytesRead } = await handle.read(buffer, length, buffer.length - length, null);
      if (!bytesRead) break; length += bytesRead;
    }
    const after = await handle.stat();
    requireThat(length <= LIMIT && after.size === length && opened.size === length && after.mtimeMs === opened.mtimeMs, 'rebase_file_changed');
    return buffer.subarray(0, length);
  } finally { await handle.close(); }
}

function json(bytes) {
  try { return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); }
  catch { throw new SafeError('rebase_json_invalid'); }
}

async function syncDirectory(directory) {
  if (process.platform === 'win32') return; // The production operation runs on Linux.
  const handle = await open(directory, 'r');
  try { await handle.sync(); } finally { await handle.close(); }
}

// A hard link publishes complete, fsynced bytes atomically without replacing an
// existing destination, including one created after our preliminary check.
async function publishJson(file, value) {
  const temporary = path.join(path.dirname(file), `.rebase-${process.pid}-${randomUUID()}.tmp`);
  const handle = await open(temporary, 'wx', 0o600);
  try {
    try { await handle.writeFile(JSON.stringify(value, null, 2) + '\n'); await handle.sync(); }
    finally { await handle.close(); }
    await link(temporary, file);
    await unlink(temporary);
    await syncDirectory(path.dirname(file));
  } catch (error) { await unlink(temporary).catch(() => {}); throw error; }
}

async function acquireLock(file, code) {
  let handle;
  try { handle = await open(file, 'wx', 0o600); }
  catch (error) { if (error.code === 'EEXIST') throw new SafeError(code); throw error; }
  const identity = await handle.stat();
  try { await handle.writeFile(JSON.stringify({ pid: process.pid, operation: 'rebase-host' }) + '\n'); await handle.sync(); }
  catch (error) { await handle.close(); await unlink(file); throw error; }
  return { file, handle, identity };
}

async function releaseLocks(locks) {
  let failed = false;
  for (const lock of locks.reverse()) {
    try { await lock.handle.close(); } catch { failed = true; }
    try {
      const now = await lstat(lock.file);
      requireThat(now.dev === lock.identity.dev && now.ino === lock.identity.ino && now.isFile() && !now.isSymbolicLink(), 'rebase_lock_changed');
      await unlink(lock.file);
    } catch { failed = true; }
  }
  requireThat(!failed, 'rebase_lock_release_failed');
}

function checkedState(host, module, config, target) {
  requireThat(host?.schema === 'soty.connect.host.v1' && host.revision === config.revision && host.sourceRoot === config.sourceRoot && host.images && typeof host.images === 'object' && !Array.isArray(host.images), 'rebase_host_state_invalid');
  requireThat(host.transaction === null && module?.pending === null, 'rebase_pending_recovery_required');
  requireThat(module.format === 1 && samePath(module.target, target) && Number.isSafeInteger(module.lastSequence) && module.lastSequence > 0 && HASH.test(module.releaseHash || '') && VERSION.test(module.version || ''), 'rebase_module_state_invalid');
  const active = host.active, entry = host.images[active?.tree];
  requireThat(active && HASH.test(active.tree || '') && IMAGE.test(active.image || '') && HASH.test(active.containerId || ''), 'rebase_host_state_invalid');
  for (const [tree, value] of Object.entries(host.images)) requireThat(HASH.test(tree) && value && IMAGE.test(value.image || '') && typeof value.hasConnect === 'boolean', 'rebase_host_mapping_invalid');
  requireThat(entry && entry.image === active.image && entry.hasConnect === true && entry.version === module.version && REVISION.test(entry.revision || ''), 'rebase_baseline_mapping_invalid');
  return { active, entry };
}

async function checkedRuntime(engine, config, active, entry) {
  const runtime = await engine.inspect(active.containerId);
  requireThat(runtime?.Id === active.containerId && runtime.Image === entry.image && runtime.State?.Running === true && runtime.Name === '/' + config.runtimeName, 'rebase_active_runtime_changed');
  const named = await engine.inspect(config.runtimeName);
  requireThat(named?.Id === runtime.Id && named.Image === runtime.Image && named.State?.Running === true, 'rebase_active_runtime_changed');
  const image = await engine.image(entry.image);
  requireThat(image?.Id === entry.image, 'rebase_image_missing');
  for (const labels of [image.Config?.Labels, runtime.Config?.Labels]) {
    requireThat(labels?.['io.soty.connect.tree'] === active.tree && labels?.['org.opencontainers.image.revision'] === entry.revision, 'rebase_image_identity_changed');
  }
  const bindings = runtime.HostConfig?.PortBindings?.['8080/tcp'] || [];
  requireThat(bindings.some(binding => binding.HostPort === new URL(config.healthOrigin).port && ['127.0.0.1', '::1', ''].includes(binding.HostIp || '')), 'rebase_health_binding_mismatch');
  // This only validates an in-memory copy. No environment values or mount
  // configuration are persisted in a journal or returned to the operator.
  candidateConfig(runtime, entry.image, '0'.repeat(32), config, active.tree);
}

/** Prepare an independent pinned generation; never activate it or alter Docker. */
export async function rebaseHost({ oldConfigFile, newConfigFile, sourceRoot, stateDir, revision, appOriginTemplate }, dependencies = {}) {
  oldConfigFile = await location(oldConfigFile);
  newConfigFile = await location(newConfigFile);
  sourceRoot = await location(sourceRoot);
  stateDir = await location(stateDir);
  const configBytes = await regularBytes(oldConfigFile);
  const previousConfig = validateConfig(json(configBytes));
  const config = validateConfig({ ...previousConfig, sourceRoot, stateDir, revision, initialRuntimeHasConnect: true, ...(appOriginTemplate === undefined ? {} : { appOriginTemplate }) });
  requireThat(config.revision !== previousConfig.revision && !samePath(newConfigFile, oldConfigFile), 'rebase_new_generation_required');
  const oldSource = await location(previousConfig.sourceRoot), oldState = await location(previousConfig.stateDir);
  requireThat(samePath(oldSource, previousConfig.sourceRoot) && samePath(oldState, previousConfig.stateDir), 'rebase_config_alias');
  for (const directory of [oldSource, oldState, sourceRoot, previousConfig.releaseDirectory]) requireThat((await lstat(await location(directory))).isDirectory(), 'rebase_directory_invalid');
  const trustFile = await location(previousConfig.trustFile), trustBytes = await regularBytes(trustFile);
  const feed = await location(previousConfig.releaseDirectory);
  const oldTarget = await location(path.join(oldSource, 'modules', 'connect'));
  const target = await location(path.join(sourceRoot, 'modules', 'connect'));
  requireThat(!overlaps(oldSource, sourceRoot) && !overlaps(oldState, stateDir) && !overlaps(oldSource, stateDir) && !overlaps(oldState, sourceRoot), 'rebase_paths_overlap');
  for (const location of [sourceRoot, stateDir]) {
    requireThat(!overlaps(location, feed) && !inside(location, trustFile) && !inside(location, oldConfigFile), 'rebase_paths_overlap');
  }
  for (const root of [oldSource, sourceRoot, oldState, stateDir, feed]) requireThat(!inside(root, newConfigFile), 'rebase_config_must_be_external');
  requireThat(!inside(oldTarget, oldConfigFile) && !inside(target, oldConfigFile) && !inside(oldTarget, trustFile) && !inside(target, trustFile) && !samePath(newConfigFile, trustFile), 'rebase_paths_overlap');
  requireThat((await lstat(path.dirname(stateDir))).isDirectory() && (await lstat(path.dirname(newConfigFile))).isDirectory(), 'rebase_parent_required');
  requireThat(!await exists(stateDir), 'rebase_state_exists');
  requireThat(!await exists(newConfigFile), 'rebase_config_exists');

  const oldHostFile = await location(path.join(oldState, 'host-state.json'));
  const oldModuleDir = await location(path.join(oldState, 'module-update'));
  const oldModuleFile = await location(path.join(oldModuleDir, 'state.json'));
  // All controllers take host then module locks. If the second lock is busy,
  // release only our first lock; never steal or rewrite another owner's lock.
  const locks = [];
  try {
    locks.push(await acquireLock(path.join(oldState, 'host-controller.lock'), 'rebase_host_locked'));
    locks.push(await acquireLock(path.join(oldModuleDir, 'update.lock'), 'rebase_module_locked'));
    requireThat((await regularBytes(oldConfigFile)).equals(configBytes), 'rebase_config_changed');
    const hostBytes = await regularBytes(oldHostFile), moduleBytes = await regularBytes(oldModuleFile);
    const { active, entry } = checkedState(json(hostBytes), json(moduleBytes), previousConfig, oldTarget);
    const module = json(moduleBytes);
    const engine = dependencies.engine || new DockerApi({ socketPath: previousConfig.dockerSocket, timeoutMs: 30_000 });
    const ready = dependencies.ready || productionReady(previousConfig);
    const controllers = [previousConfig, config].map(c => new HostController(c, { engine, ...(dependencies.command ? { command: dependencies.command } : {}) }));

    const verifySnapshot = async () => {
      for (const [file, before] of [[oldConfigFile, configBytes], [oldHostFile, hostBytes], [oldModuleFile, moduleBytes], [trustFile, trustBytes]]) requireThat((await regularBytes(file)).equals(before), 'rebase_state_changed');
      for (const controller of controllers) await controller.checkSource();
      const before = await moduleTree(oldTarget), after = await moduleTree(target);
      requireThat(before.tree === active.tree && before.version === module.version, 'rebase_active_tree_changed');
      requireThat(after.tree === before.tree && after.version === before.version, 'rebase_baseline_mismatch');
      await checkedRuntime(engine, config, active, entry);
    };
    await verifySnapshot();
    await ready({ entry, maintenance: false, idle: true });
    await verifySnapshot();
    // The already verified serving image supplies only Node for trusted host
    // probe source. The future signed candidate cannot choose this helper.
    if (config.storageProbeImage === undefined) config.storageProbeImage = active.image;
    requireThat((await engine.image(config.storageProbeImage)).Id === config.storageProbeImage, 'rebase_probe_image_missing');
    requireThat(!await exists(newConfigFile), 'rebase_config_exists');
    // Recheck ancestors before reserving a new, exclusive generation. A failure
    // after reservation intentionally leaves evidence for operator inspection;
    // the helper never recursively deletes a partially prepared generation.
    requireThat(await location(stateDir) === stateDir && await location(newConfigFile) === newConfigFile, 'rebase_path_changed');
    try { await mkdir(stateDir, { mode: 0o700 }); }
    catch (error) { if (error.code === 'EEXIST') throw new SafeError('rebase_state_exists'); throw error; }
    const moduleStateDir = path.join(stateDir, 'module-update');
    await mkdir(moduleStateDir, { mode: 0o700 });
    const nextHost = {
      schema: 'soty.connect.host.v1', revision: config.revision, sourceRoot: config.sourceRoot,
      images: { [active.tree]: { image: entry.image, version: entry.version, hasConnect: true, revision: entry.revision, baseline: true } },
      active: { tree: active.tree, image: active.image, containerId: active.containerId }, transaction: null,
    };
    const nextModule = { format: 1, target, lastSequence: module.lastSequence, releaseHash: module.releaseHash, version: module.version, previous: null, pending: null };
    await publishJson(path.join(stateDir, 'host-state.json'), nextHost);
    await publishJson(path.join(moduleStateDir, 'state.json'), nextModule);
    await syncDirectory(stateDir);
    await syncDirectory(path.dirname(stateDir));
    // Config is the publication point. Both durable journals must already exist
    // so HostController.load cannot mistake this for a fresh sequence-zero host.
    await verifySnapshot();
    await publishJson(newConfigFile, config);
    return {
      ok: true, status: 'prepared', newConfigFile, sourceRoot, stateDir, revision: config.revision,
      baselineRevision: entry.revision, tree: active.tree, image: active.image, version: module.version,
      lastSequence: module.lastSequence, releaseHash: module.releaseHash,
      priorHostStateSha256: sha256(hostBytes), priorModuleStateSha256: sha256(moduleBytes),
    };
  } finally { await releaseLocks(locks); }
}

export async function main(argv = process.argv.slice(2)) {
  const names = new Map([['--old-config', 'oldConfigFile'], ['--new-config', 'newConfigFile'], ['--source-root', 'sourceRoot'], ['--state-dir', 'stateDir'], ['--revision', 'revision'], ['--app-origin-template', 'appOriginTemplate']]);
  const options = {};
  requireThat(argv.length % 2 === 0, 'rebase_usage_invalid');
  for (let i = 0; i < argv.length; i += 2) {
    const key = names.get(argv[i]);
    requireThat(key && !Object.hasOwn(options, key) && typeof argv[i + 1] === 'string' && argv[i + 1], 'rebase_usage_invalid');
    options[key] = argv[i + 1];
  }
  for (const key of ['oldConfigFile', 'newConfigFile', 'sourceRoot', 'stateDir', 'revision']) requireThat(Object.hasOwn(options, key), 'rebase_usage_invalid');
  return rebaseHost(options);
}

if (process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url) {
  try { console.log(JSON.stringify(await main())); }
  catch (error) { console.error(JSON.stringify({ ok: false, code: /^[a-z0-9_]+$/.test(error?.code || '') ? error.code : 'rebase_failed' })); process.exitCode = 1; }
}
