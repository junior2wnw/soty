import test from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { readFileSync } from 'node:fs';
import { mkdtemp, writeFile, lstat, symlink, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { linuxDevice, parseMountInfoLine, assertPhysicalIsolation, assertArtifactMountIsolation, MOUNT_PROBE_SOURCE } from './gpg-mount-proof.mjs';

const NONCE = '1'.repeat(32), OP = '2'.repeat(32), ORIGINAL = '3'.repeat(64), HELPER = '4'.repeat(64);
const IMAGE = 'sha256:' + '5'.repeat(64), SOURCE_IMAGE = 'sha256:' + '6'.repeat(64);
const TARGETS = ['/data', '/soty-mount-proof/s01', '/soty-mount-proof/parent'];
const parentStat = { dev: 2049n, ino: 90n };
const linux = { skip: process.platform !== 'linux' };
const invalid = error => error.code === 'backup_mount_proof_invalid';
const overlap = error => error.code === 'backup_directory_overlaps_source';
const copy = value => JSON.parse(JSON.stringify(value));
function line(id, root, target, { device = '8:1', type = 'ext4', options = 'ro,relatime', optional = '' } = {}) {
  return `${id} 1 ${device} ${root} ${target} ${options}${optional ? ' ' + optional : ''} - ${type} /dev/synthetic rw`;
}
function rootRecord(id, root, target, { dev = '2049', ino = String(id), kind = 'directory', ...options } = {}) {
  return { target, mountInfo: line(id, root, target, options),
    stat: { dev, ino, mode: String(kind === 'directory' ? 0o40700 : 0o100600), nlink: kind === 'directory' ? '2' : '1', kind } };
}
function proof() {
  return { schema: 'soty.backup.mount-proof.v1', nonce: NONCE, roots: [
    rootRecord(11, '/docker/volumes/synthetic/_data', TARGETS[0]),
    rootRecord(12, '/own/work/settings.json', TARGETS[1], { kind: 'file' }),
    rootRecord(13, '/own/work', TARGETS[2], { ino: '90' }),
  ] };
}
const verify = (value, args = {}) => assertPhysicalIsolation(value, { nonce: NONCE, targets: TARGETS, artifactBase: 'artifacts', parentStat, ...args });

test('physical proof accepts config/artifact siblings and compares parent plus NEW basename', () => {
  assert.equal(verify(proof()).sourceRootCount, 2);
});
test('physical proof rejects output equal to an existing Source root under another mount target', () => {
  const value = proof(); value.roots[0].mountInfo = line(11, '/own/work/artifacts', TARGETS[0]);
  assert.throws(() => verify(value), overlap);
});
test('physical proof rejects output nested inside Source under a different namespace mountpoint', () => {
  const value = proof(); value.roots[0].mountInfo = line(11, '/own/work', TARGETS[0]);
  assert.throws(() => verify(value), overlap);
});
test('physical proof rejects Source nested inside future artifact directory', () => {
  const value = proof(); value.roots[0].mountInfo = line(11, '/own/work/artifacts/source', TARGETS[0]);
  assert.throws(() => verify(value), overlap);
});
test('physical proof honors segment boundaries', () => {
  const value = proof(); value.roots[0].mountInfo = line(11, '/own/work/artifacts-other', TARGETS[0]);
  assert.doesNotThrow(() => verify(value));
});
test('identical filesystem-root text on different devices is disjoint', () => {
  const value = proof(); value.roots[0] = rootRecord(11, '/own/work/artifacts', TARGETS[0], { device: '8:2', dev: '2050' });
  assert.doesNotThrow(() => verify(value));
});
test('filesystem root slash contains every descendant on the same device', () => {
  const value = proof(); value.roots[0].mountInfo = line(11, '/', TARGETS[0]);
  assert.throws(() => verify(value), overlap);
});
test('kernel space/backslash octal escapes decode once and preserve boundaries', () => {
  assert.equal(parseMountInfoLine(line(11, '/own/a\\040b/dir\\134040name', '/data'), '/data').root, '/own/a b/dir\\040name');
});
test('escaped space participates in physical alias rejection', () => {
  const value = proof();
  value.roots[0].mountInfo = line(11, '/own/a\\040b/artifacts', TARGETS[0]);
  value.roots[2].mountInfo = line(13, '/own/a\\040b', TARGETS[2]);
  assert.throws(() => verify(value), overlap);
});
test('unknown/truncated octal and control escapes are refused', () => {
  for (const root of ['/a\\057b', '/a\\04', '/a\\000b', '/a\\011b', '/a\\012b', '/a\\777b']) {
    assert.throws(() => parseMountInfoLine(line(11, root, '/data'), '/data'), invalid);
  }
});
test('noncanonical root segments, deleted roots and invalid UTF16 are refused', () => {
  for (const root of ['relative', '/a//b', '/a/./b', '/a/../b', '/a/', '/a (deleted)', '/a\ud800']) {
    assert.throws(() => parseMountInfoLine(line(11, root, '/data'), '/data'), invalid);
  }
});
test('unknown filesystem, rw and malformed device fields are refused', () => {
  for (const options of [{ type: 'overlay' }, { type: 'fuse.unknown' }, { options: 'rw,relatime' },
    { options: 'ro,rw' }, { device: '08:1' }, { device: '8:-1' }, { device: '4294967296:1' }]) {
    assert.throws(() => parseMountInfoLine(line(11, '/root', '/data', options), '/data'), invalid);
  }
});
test('target mismatch, unknown optional field, duplicate separator and duplicate mount options are refused', () => {
  assert.throws(() => parseMountInfoLine(line(11, '/root', '/other'), '/data'), invalid);
  for (const value of [line(11, '/root', '/data', { optional: 'unknown:3' }), line(11, '/root', '/data', { optional: 'shared:2 shared:3' }),
    line(11, '/root', '/data', { options: 'ro,ro' }), line(11, '/root', '/data') + ' -']) {
    assert.throws(() => parseMountInfoLine(value, '/data'), invalid);
  }
  assert.equal(parseMountInfoLine(line(11, '/root', '/data', { optional: 'shared:2 master:1' }), '/data').root, '/root');
});
test('Linux dev_t encoding includes high major and minor bits without truncation', () => {
  const major = 0x12345n, minor = 0x6789abcdn;
  const encoded = ((major & 0xfffn) << 8n) | ((major & 0xfffff000n) << 32n) | (minor & 0xffn) | ((minor & 0xffffff00n) << 12n);
  assert.equal(linuxDevice(String(encoded)), major.toString() + ':' + minor.toString());
  assert.equal(linuxDevice('0'), '0:0');
  for (const bad of ['-1', '00', '18446744073709551616', 'NaN']) assert.throws(() => linuxDevice(bad), invalid);
});
test('stat dev must match kernel mount device', () => {
  const value = proof(); value.roots[0].stat.dev = '2050'; assert.throws(() => verify(value), invalid);
});
test('artifact parent must be the exact host lstat dev/inode', () => {
  assert.throws(() => verify(proof(), { parentStat: { dev: 2049n, ino: 91n } }), invalid);
  assert.throws(() => verify(proof(), { parentStat: { dev: 2050n, ino: 90n } }), invalid);
});
test('missing/extra/duplicate roots, ambiguous mount ids and wrong nonce are refused', () => {
  for (const change of [value => value.roots.pop(), value => value.roots.push(copy(value.roots[0])),
    value => value.roots[1].target = TARGETS[0], value => value.roots[1].mountInfo = line(11, '/file', TARGETS[1]),
    value => value.nonce = 'f'.repeat(32), value => value.extra = true]) {
    const value = proof(); change(value); assert.throws(() => verify(value), invalid);
  }
});
test('symlink/special/zero-inode/hardlinked-file stat records are refused', () => {
  for (const change of [value => value.roots[0].stat.kind = 'symlink', value => value.roots[0].stat.mode = String(0o120777),
    value => value.roots[1].stat.nlink = '2', value => value.roots[0].stat.ino = '0',
    value => value.roots[0].stat.kind = 'file', value => value.roots[2].stat.kind = 'file']) {
    const value = proof(); change(value); assert.throws(() => verify(value), invalid);
  }
});
test('artifact basename cannot inject traversal or extra segments', () => {
  for (const artifactBase of ['..', '.', '', 'a/b', 'a\n', '\ud800']) assert.throws(() => verify(proof(), { artifactBase }), invalid);
});

async function adapterFixture(t, change = () => {}) {
  const parent = await mkdtemp(path.join(os.tmpdir(), 'soty-mount-proof-unit-'));
  t.after(() => rm(parent, { recursive: true, force: true }));
  const bind = path.join(parent, 'settings.json'); await writeFile(bind, 'public fixture\n', { mode: 0o600 });
  const stat = await lstat(parent, { bigint: true });
  const original = { Id: ORIGINAL, Image: SOURCE_IMAGE, Config: { Env: ['DATA_DIR=/data'] }, HostConfig: {},
    State: { Running: false }, RestartCount: 0, Mounts: [
      { Type: 'volume', Name: 'synthetic-data', Source: '/protected/synthetic/_data', Destination: '/data', RW: true },
      { Type: 'bind', Source: bind, Destination: '/run/settings.json', RW: false },
    ] };
  const state = { original, config: null, runs: 0, removes: 0, stops: 0, started: false };
  const value = () => ({ Id: HELPER, Name: '/' + state.name, Image: IMAGE, Config: copy(state.config),
    HostConfig: { ...copy(state.config.HostConfig), IpcMode: 'private', PidMode: '' },
    State: { Running: false, ExitCode: 0 }, Mounts: state.config.HostConfig.Mounts.map(mount => ({
      Type: mount.Type, Source: mount.Type === 'volume' ? original.Mounts[0].Source : mount.Source,
      ...(mount.Type === 'volume' ? { Name: mount.Source } : {}), Destination: mount.Target, RW: false,
    })) });
  const engine = {
    async image() { return { Id: IMAGE, Config: { Volumes: { '/data': {} } } }; },
    async request() { return { Name: 'synthetic-data', Driver: 'local', Scope: 'local', Options: {}, Mountpoint: original.Mounts[0].Source }; },
    async inspect(id) { if (id === ORIGINAL) return copy(original); if (id === HELPER) { const result = value(); change('inspect', result, state); return result; }
      throw Object.assign(Error('absent'), { code: 'engine_http_404' }); },
    async create(name, config) { state.name = name; state.config = config; change('create', state.config, state); return { Id: HELPER }; },
    async remove() { state.removes++; },
  };
  const run = (args, cap) => {
    state.runs++; assert.deepEqual(args, ['start', '--attach', HELPER]); assert.ok(cap.wallMs <= 30000);
    const input = JSON.parse(state.config.Cmd.at(-1)), dev = String(stat.dev), device = linuxDevice(dev);
    const roots = input.targets.map((target, index) => rootRecord(11 + index,
      target === '/data' ? '/docker/synthetic/_data' : target.endsWith('/parent') ? '/own/work' : '/own/work/settings.json', target,
      { dev, device, ino: target.endsWith('/parent') ? String(stat.ino) : String(100 + index), kind: target.startsWith('/soty-mount-proof/s') ? 'file' : 'directory' }));
    const output = { schema: 'soty.backup.mount-proof.v1', nonce: input.nonce, roots };
    change('output', output, state);
    const stdout = new PassThrough(), child = { stdout, stdin: { end() {} }, kill() {} };
    process.nextTick(() => stdout.end(JSON.stringify(output))); return { child, done: Promise.resolve() };
  };
  return { state, parent, bind, engine, original, invoke: () => assertArtifactMountIsolation({ engine, image: IMAGE,
    operationId: OP, original: copy(original), parent, parentStat: stat, artifactBase: 'artifacts', cap: { wallMs: 180000 },
    run, stop: async () => { state.stops++; } }) };
}
test('closed actual adapter creates only RO source+parent mounts and returns safe proof, then removes exact helper', linux, async t => {
  const f = await adapterFixture(t), result = await f.invoke();
  assert.equal(result.sourceRootCount, 2); assert.match(result.proofSha256, /^[a-f0-9]{64}$/u);
  assert.equal(f.state.runs, 1); assert.equal(f.state.removes, 1); assert.equal(f.state.stops, 1);
  const config = f.state.config;
  assert.equal(config.HostConfig.Mounts.length, 3); assert.ok(config.HostConfig.Mounts.every(mount => mount.ReadOnly === true));
  assert.equal(config.HostConfig.LogConfig.Type, 'none'); assert.equal(config.HostConfig.NetworkMode, 'none');
  assert.equal(config.Cmd[3], MOUNT_PROBE_SOURCE); assert.equal(JSON.stringify(config).includes('docker.sock'), false);
});
test('closed adapter rejects altered command before START', linux, async t => {
  const f = await adapterFixture(t, (phase, value) => { if (phase === 'inspect') value.Config.Cmd[3] += '\n'; });
  await assert.rejects(f.invoke(), invalid); assert.equal(f.state.runs, 0);
});
test('closed adapter rejects logging, writable mounts, extra socket mounts and missing limits before START', linux, async t => {
  for (const mutate of [value => value.HostConfig.LogConfig.Type = 'json-file', value => value.Mounts[0].RW = true,
    value => value.Mounts.push({ Type: 'bind', Source: '/var/run/docker.sock', Destination: '/socket', RW: false }),
    value => value.HostConfig.Memory = 0]) {
    const f = await adapterFixture(t, (phase, value) => { if (phase === 'inspect') mutate(value); });
    await assert.rejects(f.invoke(), invalid); assert.equal(f.state.runs, 0);
  }
});
test('closed adapter does not accept Source changes while probe runs', linux, async t => {
  const f = await adapterFixture(t, (phase, _value, state) => { if (phase === 'output') state.original.Config.Env.push('changed=1'); });
  await assert.rejects(f.invoke(), invalid); assert.equal(f.state.runs, 1); assert.equal(f.state.removes, 1);
});
test('closed adapter refuses physical overlap even when namespace targets look disjoint', linux, async t => {
  const f = await adapterFixture(t, (phase, value) => { if (phase === 'output') {
    const root = value.roots.find(item => item.target === '/data'); root.mountInfo = root.mountInfo.replace('/docker/synthetic/_data', '/own/work/artifacts');
  } });
  await assert.rejects(f.invoke(), overlap); assert.equal(f.state.removes, 1);
});
test('closed adapter refuses a Source bind symlink before any helper CREATE or START', linux, async t => {
  const f = await adapterFixture(t), alias = path.join(f.parent, 'alias'); await symlink(f.bind, alias);
  f.original.Mounts[1].Source = alias; await assert.rejects(f.invoke(), invalid);
  assert.equal(f.state.config, null); assert.equal(f.state.runs, 0);
});

function directoryPort({ errorCode = 'EACCES', errorType = 'volume', rejectProof = false, changedParent = false, overlapBind = false } = {}) {
  const source = readFileSync(new URL('./gpg-release-backup.mjs', import.meta.url), 'utf8');
  const first = source.indexOf('async function artifactDirectory('), last = source.indexOf('\n// POSIX tar metadata', first);
  assert.ok(first >= 0 && last > first);
  let proofCalls = 0, created = 0, parentReads = 0;
  const stats = ino => ({ dev: 1n, ino, mode: 0o40700n, uid: BigInt(process.getuid()), gid: 1n,
    isDirectory: () => true, isSymbolicLink: () => false });
  const parent = '/own/work', volume = '/protected/volume';
  const original = { Mounts: [
    { Type: 'volume', Source: volume, Destination: '/data' },
    { Type: 'bind', Source: overlapBind ? parent + '/artifacts/config' : parent + '/config', Destination: '/config' },
  ] };
  const realpath = async name => {
    if (name === (errorType === 'volume' ? volume : original.Mounts[1].Source)) throw Object.assign(Error('denied'), { code: errorCode });
    return name;
  };
  const lstat = async name => { if (name === parent) return stats(changedParent && parentReads++ > 0 ? 8n : 7n);
    return { isDirectory: () => true, isSymbolicLink: () => false, mode: 0o40700, uid: process.getuid() }; };
  const bad = code => { throw Object.assign(Error(code), { code }); };
  const implementation = new Function('path', 'realpath', 'lstat', 'mkdir', 'bad', 'assertArtifactMountIsolation', 'processDocker', 'stopOwned',
    source.slice(first, last) + '\nreturn artifactDirectory;')(path, realpath, lstat, async () => { created++; }, bad,
    async value => { proofCalls++; assert.equal(value.original.Mounts.length, 2); assert.equal(value.artifactBase, 'artifacts');
      assert.equal(value.parentStat.ino, 7n); if (rejectProof) bad('backup_directory_overlaps_source'); }, () => {}, () => {});
  return { invoke: () => implementation(parent + '/artifacts', original, {}, IMAGE, OP, {}),
    counts: () => ({ proofCalls, created }) };
}
test('actual artifactDirectory invokes proof for protected data volume before exclusive mkdir', linux, async () => {
  const f = directoryPort(); await f.invoke(); assert.deepEqual(f.counts(), { proofCalls: 1, created: 1 });
});
test('actual artifactDirectory does not bypass bind EACCES or other volume errors', linux, async () => {
  for (const options of [{ errorType: 'bind' }, { errorCode: 'ENOENT' }, { errorCode: 'ELOOP' }]) {
    const f = directoryPort(options); await assert.rejects(f.invoke(), { code: options.errorCode ?? 'EACCES' });
    assert.deepEqual(f.counts(), { proofCalls: 0, created: 0 });
  }
});
test('actual artifactDirectory preserves accessible canonical overlap before protected-volume proof', linux, async () => {
  const f = directoryPort({ overlapBind: true }); await assert.rejects(f.invoke(), overlap);
  assert.deepEqual(f.counts(), { proofCalls: 0, created: 0 });
});
test('actual artifactDirectory creates nothing after rejected proof or changed parent identity', linux, async () => {
  for (const options of [{ rejectProof: true }, { changedParent: true }]) {
    const f = directoryPort(options); await assert.rejects(f.invoke(), { code: options.rejectProof ? 'backup_directory_overlaps_source' : 'backup_directory_invalid' });
    assert.deepEqual(f.counts(), { proofCalls: 1, created: 0 });
  }
});
