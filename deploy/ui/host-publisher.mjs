// Bounded host operations, derived from accepted core-ui-native/active-readonly.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { isDeepStrictEqual } from 'node:util';
import { createUiRelease, writeUiRelease, createCaddyOverlay } from './release.mjs';
import { sha256, decryptConfig, fsyncDirectory, swapConfig } from './config-swap.mjs';
export { createUiRelease, createCaddyOverlay } from './release.mjs';
export { sha256, writeEncryptedConfigs } from './config-swap.mjs';

const need = (value, code) => { if (!value) throw Object.assign(new Error(code), { code }); };
const exact = (value, keys) => value && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).sort().join(',') === keys.sort().join(',');
const hex = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
const absolute = value => typeof value === 'string' && /^\/[A-Za-z0-9_./-]+$/u.test(value) && path.posix.normalize(value) === value && !value.split('/').includes('..');
const natural = value => Number.isSafeInteger(value) && value >= 0;
function ownedDirectory(directory, privateOnly = false) {
  const stat = fs.lstatSync(directory);
  need(fs.realpathSync(directory) === directory && stat.isDirectory() && !stat.isSymbolicLink()
    && stat.uid === process.getuid?.() && !(stat.mode & (privateOnly ? 0o077 : 0o022)), 'host_directory_invalid');
}
const descriptor = value => exact(value, ['file', 'cipherSha256', 'plainSha256']) && absolute(value.file) && hex(value.cipherSha256) && hex(value.plainSha256);

export function validateHostPlan(plan) {
  const common = ['schema', 'action', 'transaction', 'releaseId', 'receiptDirectory'];
  need(plan?.schema === 'soty.ui.host.v1' && /^[a-f0-9]{32}$/u.test(plan.transaction) && hex(plan.releaseId)
    && absolute(plan.receiptDirectory), 'host_plan_invalid');
  if (['install', 'publish'].includes(plan.action)) {
    need(exact(plan, [...common, 'input', 'manifestFile', 'base', ...(plan.action === 'publish' ? ['expectedReleaseId'] : [])])
      && [plan.input, plan.manifestFile, plan.base].every(absolute)
      && (plan.action !== 'publish' || plan.expectedReleaseId === null || hex(plan.expectedReleaseId) && plan.expectedReleaseId !== plan.releaseId), 'host_plan_invalid');
  } else {
    need(['apply', 'rollback'].includes(plan.action) && exact(plan, [...common, 'releaseRoot', 'configPath', 'profile', 'process', 'baseline', 'candidate', 'overlay'])
      && absolute(plan.releaseRoot) && plan.releaseRoot.endsWith('/' + plan.releaseId) && absolute(plan.configPath)
      && exact(plan.profile, ['uid', 'gid', 'mode']) && natural(plan.profile.uid) && natural(plan.profile.gid)
      && [0o600, 0o640, 0o644].includes(plan.profile.mode)
      && exact(plan.process, ['pid', 'uid', 'startTicks', 'argvSha256']) && Number.isSafeInteger(plan.process.pid) && plan.process.pid > 1
      && natural(plan.process.uid) && typeof plan.process.startTicks === 'string' && /^[1-9][0-9]*$/u.test(plan.process.startTicks)
      && hex(plan.process.argvSha256) && descriptor(plan.baseline) && descriptor(plan.candidate)
      && plan.baseline.file !== plan.candidate.file && plan.baseline.plainSha256 !== plan.candidate.plainSha256
      && exact(plan.overlay, ['canonicalSha256', 'currentReleaseId', 'currentRoot', 'currentSha256', 'nextSha256', 'activeSha256'])
      && hex(plan.overlay.canonicalSha256) && hex(plan.overlay.currentReleaseId) && absolute(plan.overlay.currentRoot)
      && plan.overlay.currentRoot.endsWith('/' + plan.overlay.currentReleaseId) && hex(plan.overlay.activeSha256)
      && plan.overlay.currentSha256 === plan.baseline.plainSha256 && plan.overlay.nextSha256 === plan.candidate.plainSha256, 'host_plan_invalid');
  }
  return plan;
}

/** Both generations derive from the SAME approved unwrapped canonical ingress.
 * The actual-file active qualification must be acquired for the pinned process.
 * An unreproducible old overlay is a refusal, never a guessed unwrapping. */
export async function prepareOverlayReplacement({ canonical, currentDisk, activeQualification, currentRelease, currentRoot, release, root, adapt, host }) {
  need([canonical, currentDisk].every(value => Buffer.isBuffer(value) && value.length > 0 && value.length <= 131072)
    && !/\{\$/u.test(canonical.toString()), 'overlay_input_invalid');
  const previous = await createCaddyOverlay({ caddyfile: canonical.toString(), release: currentRelease, root: currentRoot, adapt, host });
  need(Buffer.from(previous.caddyfile).equals(currentDisk), 'current_overlay_not_reproduced');
  // stdin adaptation adds a DIFFERENT file_server.hide path. Never omit that
  // field: compare actual disk-file adapt to active in the independent probe.
  need(exact(activeQualification, ['readOnly', 'activeDiskEqual', 'diskSha256', 'activeFileSha256', 'activeAdminDisabled',
    'adaptedAdminDisabled', 'rawConfigurationStored', 'binarySha256', 'uid']) && activeQualification.readOnly === true
    && activeQualification.activeDiskEqual === true && activeQualification.diskSha256 === sha256(currentDisk)
    && hex(activeQualification.activeFileSha256) && hex(activeQualification.binarySha256) && natural(activeQualification.uid)
    && activeQualification.activeAdminDisabled === true && activeQualification.adaptedAdminDisabled === true
    && activeQualification.rawConfigurationStored === false && previous.config.admin?.disabled === true, 'active_probe_invalid');
  const next = await createCaddyOverlay({ caddyfile: canonical.toString(), release, root, adapt, host });
  need(next.candidateSha256 !== previous.candidateSha256, 'overlay_unchanged');
  return { baseline: Buffer.from(currentDisk), candidate: Buffer.from(next.caddyfile), overlay: {
    canonicalSha256: sha256(canonical), currentReleaseId: currentRelease.id, currentRoot,
    currentSha256: sha256(currentDisk), nextSha256: next.candidateSha256, activeSha256: activeQualification.activeFileSha256 } };
}

/** Read-only identity verification, immediately repeated before the fixed USR1. */
export function verifyCaddyProcess({ process: expected, configPath }) {
  need(process.platform === 'linux', 'host_linux_required');
  const directory = '/proc/' + expected.pid;
  try {
    const stat = fs.statSync(directory), raw = fs.readFileSync(directory + '/cmdline'), argv = raw.toString().split('\0').filter(Boolean);
    const fields = fs.readFileSync(directory + '/stat', 'utf8').split(') ').at(-1).split(' ');
    need(stat.uid === expected.uid && fs.readFileSync(directory + '/comm', 'utf8').trim() === 'caddy'
      && fields[19] === expected.startTicks && sha256(raw) === expected.argvSha256
      && argv.filter(arg => arg === '--config').length === 1 && argv[argv.indexOf('--config') + 1] === configPath
      && !argv.includes('--resume'), 'caddy_process_changed');
    process.kill(expected.pid, 0);
  } catch { need(false, 'caddy_process_changed'); }
  return true;
}

function releaseDirectories(base) {
  ownedDirectory(base);
  for (const directory of [path.join(base, 'soty-ui'), path.join(base, 'soty-ui', 'releases')]) {
    try { fs.mkdirSync(directory, { mode: 0o755 }); fsyncDirectory(path.dirname(directory)); }
    catch (error) { if (error.code !== 'EEXIST') throw error; }
    ownedDirectory(directory);
  }
  return path.join(base, 'soty-ui');
}

export function installPublicRelease({ input, manifest, base }) {
  releaseDirectories(base);
  const release = createUiRelease(input);
  need(release.id === manifest?.id && isDeepStrictEqual(release.manifest, manifest.manifest), 'public_inventory_changed');
  const root = path.join(base, 'soty-ui', 'releases', release.id);
  writeUiRelease(release, root); // Fresh directory only, no reuse of partial output.
  const installed = createUiRelease(root);
  need(installed.id === release.id && isDeepStrictEqual(installed.manifest, release.manifest), 'public_install_changed');
  const seal = directory => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) seal(file);
      else { fs.chmodSync(file, 0o444); const fd = fs.openSync(file, 'r'); try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); } }
    }
    fs.chmodSync(directory, 0o555); fsyncDirectory(directory);
  };
  seal(root); fsyncDirectory(path.dirname(root));
  verifyInstalledRelease(root, release.id);
  return { root, releaseId: release.id, fileCount: release.manifest.length,
    bytes: release.manifest.reduce((sum, file) => sum + file.bytes, 0), signalsSent: 0 };
}

export function verifyInstalledRelease(root, releaseId) {
  const release = createUiRelease(root), selected = new Set(release.manifest.map(file => file.name));
  need(release.id === releaseId && fs.realpathSync(root) === root, 'installed_release_changed');
  const visit = (directory, relative = '') => {
    const stat = fs.lstatSync(directory);
    need(stat.isDirectory() && !stat.isSymbolicLink() && stat.uid === process.getuid?.() && (stat.mode & 0o777) === 0o555, 'installed_release_changed');
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const name = relative ? relative + '/' + entry.name : entry.name, file = path.join(directory, entry.name);
      if (entry.isDirectory()) visit(file, name);
      else { const stat = fs.lstatSync(file); need(selected.has(name) && stat.isFile() && !stat.isSymbolicLink()
        && stat.nlink === 1 && stat.uid === process.getuid?.() && (stat.mode & 0o777) === 0o444, 'installed_release_changed'); }
    }
  };
  visit(root); return release;
}

const publishFingerprint = plan => sha256(Buffer.from(JSON.stringify(Object.entries(plan).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0))));
function currentRelease(directory) {
  ownedDirectory(directory); ownedDirectory(path.join(directory, 'releases'));
  const current = path.join(directory, 'current'); let stat;
  try { stat = fs.lstatSync(current); } catch (error) { if (error.code === 'ENOENT') return { id: null }; throw error; }
  need(stat.isSymbolicLink() && stat.uid === 0 && stat.nlink === 1, 'current_pointer_invalid');
  const target = fs.readlinkSync(current);
  need(/^releases\/[a-f0-9]{64}$/u.test(target), 'current_pointer_invalid');
  const id = target.slice('releases/'.length), root = path.join(directory, target);
  need(fs.realpathSync(current) === root, 'current_pointer_invalid');
  return { id, stat, release: verifyInstalledRelease(root, id) };
}

function publishRelease(plan, record) {
  const directory = releaseDirectories(plan.base), lockPath = path.join(directory, '.publish.lock');
  // flock(2) belongs to the inherited open-file description, so the Node FD
  // retains it after flock exits. Kernel close/exit releases it, including kill.
  // The permanent lock inode is never unlinked; there is no stale-lock protocol.
  const lock = fs.openSync(lockPath, fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_NOFOLLOW, 0o600);
  const identity = fs.fstatSync(lock), sameLock = () => {
    const actual = fs.lstatSync(lockPath);
    need(actual.isFile() && actual.uid === 0 && actual.nlink === 1 && (actual.mode & 0o777) === 0o600
      && actual.dev === identity.dev && actual.ino === identity.ino, 'publish_lock_invalid');
  };
  const current = path.join(directory, 'current'), temporary = path.join(directory, '.current-' + plan.transaction);
  let temporaryIdentity, switchAttempted = false;
  try {
    sameLock();
    try { execFileSync('/usr/bin/flock', ['--exclusive', '--nonblock', '3'], { stdio: ['ignore', 'pipe', 'pipe', lock], timeout: 5000 }); }
    catch (error) { need(false, error.status === 1 ? 'publish_busy' : 'publish_lock_unavailable'); }
    sameLock(); record('locked');
    const before = currentRelease(directory);
    need(before.id === plan.expectedReleaseId, 'current_release_changed');
    const manifest = JSON.parse(fs.readFileSync(plan.manifestFile, 'utf8'));
    need(manifest.id === plan.releaseId, 'host_plan_invalid');
    const root = path.join(directory, 'releases', plan.releaseId);
    if (!fs.existsSync(root)) installPublicRelease({ input: plan.input, manifest, base: plan.base });
    const release = verifyInstalledRelease(root, plan.releaseId);
    need(isDeepStrictEqual(release.manifest, manifest.manifest), 'public_inventory_changed');
    const files = new Map(release.manifest.map(file => [file.name, file]));
    need((before.release?.manifest ?? []).filter(file => file.immutable).every(file => isDeepStrictEqual(files.get(file.name), file)), 'ui_immutable_url_missing');
    record('installed');
    fs.symlinkSync('releases/' + plan.releaseId, temporary); temporaryIdentity = fs.lstatSync(temporary); fsyncDirectory(directory);
    record('switch-intent');
    const fresh = currentRelease(directory); sameLock();
    need(fresh.id === before.id && (!before.stat || fresh.stat.dev === before.stat.dev && fresh.stat.ino === before.stat.ino), 'current_release_changed');
    switchAttempted = true; fs.renameSync(temporary, current); fsyncDirectory(directory);
    need(currentRelease(directory).id === plan.releaseId, 'current_release_changed');
    record('published');
    return { published: true, currentReleaseId: plan.releaseId, previousReleaseId: before.id, root,
      fileCount: release.manifest.length, signalsSent: 0, reloadRequired: false };
  } catch (error) {
    if (switchAttempted) error.recoveryRequired = true;
    throw error;
  } finally {
    try {
      if (temporaryIdentity) {
        try {
          const stat = fs.lstatSync(temporary);
          if (stat.dev === temporaryIdentity.dev && stat.ino === temporaryIdentity.ino && stat.isSymbolicLink()) { fs.unlinkSync(temporary); fsyncDirectory(directory); }
        } catch (error) { if (error.code !== 'ENOENT') throw error; }
      }
    } finally { fs.closeSync(lock); }
  }
}

const ledgerPath = plan => path.join(plan.receiptDirectory, plan.action + '-' + plan.transaction + '.jsonl');
const phaseNames = new Set(['started', 'locked', 'installed', 'switch-intent', 'published', 'replace-intent', 'replaced', 'signal-sent', 'reload-unconfirmed', 'not-applied']);
/** A transport loss does not authorize replay. inspect never sends signals. */
export function inspectHostPlan(plan) {
  validateHostPlan(plan); ownedDirectory(plan.receiptDirectory, true);
  const file = ledgerPath(plan), operationExists = fs.existsSync(file), phases = [];
  if (operationExists) {
    const stat = fs.lstatSync(file); need(stat.isFile() && !stat.isSymbolicLink() && stat.size < 65536, 'host_journal_invalid');
    for (const line of fs.readFileSync(file, 'utf8').trim().split('\n')) {
      const value = JSON.parse(line); need(phaseNames.has(value.phase)
        && (plan.action !== 'publish' || value.planSha256 === publishFingerprint(plan)), 'host_journal_invalid'); phases.push(value.phase);
    }
  }
  if (plan.action === 'publish') {
    ownedDirectory(plan.base);
    const directory = path.join(plan.base, 'soty-ui'), actual = fs.existsSync(directory) ? currentRelease(directory) : { id: null };
    return { readOnly: true, operationExists, phases, currentReleaseId: actual.id,
      state: actual.id === plan.releaseId ? 'published' : actual.id === plan.expectedReleaseId ? 'not-applied' : 'changed',
      signalsSent: 0, reloadRequired: false };
  }
  if (plan.action === 'install') return { readOnly: true, operationExists, phases, activeReloadVerified: false, signalsSent: 0 };
  const diskSha256 = sha256(fs.readFileSync(plan.configPath)); let processMatches = false;
  try { processMatches = verifyCaddyProcess(plan); } catch { /* Safe mismatch only. */ }
  return { readOnly: true, operationExists, phases, diskSha256, processMatches,
    diskState: diskSha256 === plan.baseline.plainSha256 ? 'baseline' : diskSha256 === plan.candidate.plainSha256 ? 'candidate' : 'other',
    activeReloadVerified: false, signalsSent: 0 };
}

/** Admin/autosave acquisition is external. This compares bytes using real Caddy. */
export function verifyActiveDisk({ configPath, activeFile, binary, expectedDiskSha256, expectedActiveSha256, expectedBinarySha256, expectedUid }) {
  need(process.getuid?.() === expectedUid && absolute(configPath) && absolute(activeFile) && absolute(binary)
    && hex(expectedDiskSha256) && hex(expectedActiveSha256) && hex(expectedBinarySha256), 'active_probe_invalid');
  need(sha256(fs.readFileSync(binary)) === expectedBinarySha256, 'active_probe_changed');
  const disk = fs.readFileSync(configPath), activeBytes = fs.readFileSync(activeFile);
  need(sha256(disk) === expectedDiskSha256 && sha256(activeBytes) === expectedActiveSha256, 'active_probe_changed');
  need(!/\{\$/u.test(disk.toString()), 'config_environment_requires_explicit_binding');
  let adapted, active;
  try {
    active = JSON.parse(activeBytes);
    adapted = JSON.parse(execFileSync(binary, ['adapt', '--config', configPath, '--adapter', 'caddyfile'],
      { stdio: ['ignore', 'pipe', 'pipe'], timeout: 15000, maxBuffer: 8 * 1024 * 1024 }));
  } catch { need(false, 'config_adapt_failed'); }
  need(sha256(fs.readFileSync(configPath)) === expectedDiskSha256 && sha256(fs.readFileSync(activeFile)) === expectedActiveSha256, 'active_probe_changed');
  need(isDeepStrictEqual(active, adapted), 'active_disk_mismatch');
  return { readOnly: true, activeDiskEqual: true, uid: expectedUid, binarySha256: expectedBinarySha256,
    diskSha256: expectedDiskSha256, activeFileSha256: expectedActiveSha256,
    activeAdminDisabled: active.admin?.disabled === true, adaptedAdminDisabled: adapted.admin?.disabled === true, rawConfigurationStored: false };
}

export function executeHostPlan(plan, key) {
  validateHostPlan(plan); need(process.platform === 'linux' && process.getuid() === 0, 'host_profile_changed');
  ownedDirectory(plan.receiptDirectory, true);
  // Durable O_EXCL intent precedes all writes/signals; never use a new directory to replay.
  const ledger = fs.openSync(ledgerPath(plan), 'wx', 0o600); let baseline, candidate;
  const record = phase => {
    if (phase === 'replace-intent') verifyCaddyProcess(plan);
    fs.writeSync(ledger, JSON.stringify({ phase, at: new Date().toISOString(), ...(plan.action === 'publish' ? { planSha256: publishFingerprint(plan) } : {}) }) + '\n'); fs.fsyncSync(ledger);
  };
  try {
    fsyncDirectory(plan.receiptDirectory); record('started');
    if (plan.action === 'publish') return publishRelease(plan, record);
    if (plan.action === 'install') {
      const manifest = JSON.parse(fs.readFileSync(plan.manifestFile, 'utf8'));
      need(manifest.id === plan.releaseId, 'host_plan_invalid');
      const result = installPublicRelease({ input: plan.input, manifest, base: plan.base }); record('installed'); return result;
    }
    const decrypt = (value, purpose) => {
      const stat = fs.lstatSync(value.file); need(stat.isFile() && !stat.isSymbolicLink() && stat.nlink === 1 && !(stat.mode & 0o077), 'encrypted_file_invalid');
      const raw = fs.readFileSync(value.file); need(sha256(raw) === value.cipherSha256, 'encrypted_file_changed');
      return decryptConfig(raw, key, purpose, value.plainSha256);
    };
    baseline = decrypt(plan.baseline, 'baseline'); candidate = decrypt(plan.candidate, 'candidate');
    need(!/\{\$/u.test(baseline.toString()) && !/\{\$/u.test(candidate.toString()), 'config_environment_requires_explicit_binding');
    verifyInstalledRelease(plan.releaseRoot, plan.releaseId);
    verifyCaddyProcess(plan);
    const forward = plan.action === 'apply';
    return swapConfig({ filename: plan.configPath, profile: plan.profile, transaction: plan.transaction,
      expectedSha256: sha256(forward ? baseline : candidate), next: forward ? candidate : baseline,
      nextSha256: sha256(forward ? candidate : baseline), record,
      signal() { verifyCaddyProcess(plan); process.kill(plan.process.pid, 'SIGUSR1'); } });
  } finally { baseline?.fill(0); candidate?.fill(0); fs.closeSync(ledger); }
}

const safeCodes = new Set(['host_plan_invalid', 'host_linux_required', 'host_profile_changed', 'host_directory_invalid', 'host_journal_invalid',
  'public_inventory_changed', 'public_install_changed', 'installed_release_changed', 'caddy_process_changed', 'encrypted_file_invalid',
  'current_pointer_invalid', 'current_release_changed', 'ui_immutable_url_missing', 'publish_lock_invalid', 'publish_busy', 'publish_lock_unavailable',
  'encrypted_file_changed', 'encrypted_config_invalid', 'decrypted_config_mismatch', 'config_environment_requires_explicit_binding',
  'config_file_invalid', 'current_config_changed', 'candidate_bytes_changed', 'installed_config_changed', 'swap_arguments_invalid']);
async function main() {
  let key;
  try {
    need(process.argv.length === 4 && ['execute', 'inspect'].includes(process.argv[2]), 'host_plan_invalid');
    const plan = JSON.parse(fs.readFileSync(process.argv[3], 'utf8')); validateHostPlan(plan);
    if (process.argv[2] === 'inspect') { console.log(JSON.stringify({ ok: true, ...inspectHostPlan(plan) })); return; }
    if (!['install', 'publish'].includes(plan.action)) {
      let input = ''; for await (const chunk of process.stdin) { input += chunk; need(input.length <= 1024, 'host_plan_invalid'); }
      const value = JSON.parse(input); need(exact(value, ['key']) && typeof value.key === 'string', 'host_plan_invalid');
      key = Buffer.from(value.key, 'base64'); need(key.length === 32 && key.toString('base64') === value.key, 'host_plan_invalid');
    }
    console.log(JSON.stringify({ ok: true, action: plan.action, ...executeHostPlan(plan, key),
      ...(plan.action === 'publish' ? {} : { activeReloadVerified: false }), dataMounted: false }));
  } catch (error) {
    console.log(JSON.stringify({ ok: false, code: safeCodes.has(error.code) ? error.code : error.code === 'EEXIST' ? 'host_operation_already_exists' : 'bounded_host_action_failed',
      recoveryRequired: error.recoveryRequired === true, activeReloadVerified: false })); process.exitCode = 1;
  } finally { key?.fill(0); }
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
