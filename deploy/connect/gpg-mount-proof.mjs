// A trusted operator adapter for an inaccessible local Docker volume root.
// It proves physical separation; it never reconstructs or guesses host realpath.
import { createHash, randomBytes } from 'node:crypto';
import { lstat } from 'node:fs/promises';
import path from 'node:path';

const LABEL = 'io.soty.backup.mount-proof', PREFIX = '/soty-mount-proof';
const MAX_ROOTS = 64, MAX_OUTPUT = 1024 * 1024;
const IMAGE = /^sha256:[a-f0-9]{64}$/u, ID = /^[a-f0-9]{64}$/u, NONCE = /^[a-f0-9]{32}$/u;
const TYPES = new Set(['ext4', 'xfs', 'btrfs', 'tmpfs']);
const sha = value => createHash('sha256').update(value).digest('hex');
const fail = (code = 'backup_mount_proof_invalid') => { throw Object.assign(new Error(code), { code }); };
const requireThat = value => { if (!value) fail(); };
const exact = (value, keys) => value && typeof value === 'object' && !Array.isArray(value)
  && Reflect.ownKeys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
const decimal = value => typeof value === 'string' && /^(0|[1-9][0-9]{0,19})$/u.test(value) && BigInt(value) <= 0xffffffffffffffffn;
const stableMounts = value => value.map(item => ({ item, key: JSON.stringify(item) }))
  .sort((a, b) => a.key < b.key ? -1 : a.key > b.key ? 1 : 0).map(entry => entry.item);
function sourceStamp(value) {
  return sha(JSON.stringify({ Id: value.Id, Image: value.Image, Config: value.Config, HostConfig: value.HostConfig,
    Mounts: stableMounts(value.Mounts), State: value.State, RestartCount: value.RestartCount }));
}

export function linuxDevice(value) {
  requireThat(decimal(value)); const device = BigInt(value);
  const major = ((device >> 8n) & 0xfffn) | ((device >> 32n) & 0xfffff000n);
  const minor = (device & 0xffn) | ((device >> 12n) & 0xffffff00n);
  return major.toString() + ':' + minor.toString();
}
function kernelPath(raw) {
  requireThat(typeof raw === 'string' && raw.isWellFormed() && Buffer.byteLength(raw) <= 16384);
  let decoded = '';
  for (let i = 0; i < raw.length; i++) {
    if (raw[i] !== '\\') { decoded += raw[i]; continue; }
    const octal = raw.slice(i + 1, i + 4);
    requireThat(['040', '011', '012', '134'].includes(octal));
    decoded += String.fromCharCode(parseInt(octal, 8)); i += 3;
  }
  requireThat(decoded.startsWith('/') && Buffer.byteLength(decoded) <= 4096
    && !/[\u0000-\u001f\u007f-\u009f]/u.test(decoded) && !decoded.endsWith(' (deleted)'));
  if (decoded !== '/') requireThat(decoded.slice(1).split('/').every(part => part && part !== '.' && part !== '..' && Buffer.byteLength(part) <= 255));
  return decoded;
}

export function parseMountInfoLine(line, target) {
  requireThat(typeof line === 'string' && line.isWellFormed() && line.length <= 32768 && !/[\r\n\u0000]/u.test(line));
  const fields = line.split(' '), separator = fields.indexOf('-');
  requireThat(separator >= 6 && fields.length === separator + 4 && fields.every(Boolean)
    && fields.filter(value => value === '-').length === 1
    && /^[1-9][0-9]{0,9}$/u.test(fields[0]) && /^[1-9][0-9]{0,9}$/u.test(fields[1]));
  const device = fields[2].split(':');
  requireThat(device.length === 2 && device.every(value => /^(0|[1-9][0-9]{0,9})$/u.test(value) && BigInt(value) <= 0xffffffffn));
  const options = fields[5].split(',');
  requireThat(options.includes('ro') && !options.includes('rw') && new Set(options).size === options.length);
  const optional = fields.slice(6, separator);
  requireThat(optional.every(value => /^(?:(?:shared|master|propagate_from):[1-9][0-9]{0,9}|unbindable)$/u.test(value))
    && new Set(optional.map(value => value.split(':')[0])).size === optional.length);
  requireThat(TYPES.has(fields[separator + 1]) && kernelPath(fields[4]) === target);
  return { id: fields[0], device: fields[2], root: kernelPath(fields[3]), type: fields[separator + 1] };
}

export function assertPhysicalIsolation(proof, { nonce, targets, artifactBase, parentStat }) {
  requireThat(exact(proof, ['schema', 'nonce', 'roots']) && proof.schema === 'soty.backup.mount-proof.v1'
    && proof.nonce === nonce && NONCE.test(nonce) && Array.isArray(proof.roots) && proof.roots.length === targets.length
    && targets.length >= 2 && targets.length <= MAX_ROOTS + 1
    && typeof artifactBase === 'string' && artifactBase.isWellFormed() && artifactBase && artifactBase !== '.' && artifactBase !== '..'
    && !/[\/\u0000-\u001f\u007f-\u009f]/u.test(artifactBase) && Buffer.byteLength(artifactBase) <= 255);
  const records = new Map(), mountIds = new Set(), deviceTypes = new Map();
  for (const entry of proof.roots) {
    requireThat(exact(entry, ['target', 'mountInfo', 'stat']) && targets.includes(entry.target) && !records.has(entry.target)
      && exact(entry.stat, ['dev', 'ino', 'mode', 'nlink', 'kind'])
      && ['dev', 'ino', 'mode', 'nlink'].every(key => decimal(entry.stat[key])) && BigInt(entry.stat.ino) > 0n && BigInt(entry.stat.nlink) > 0n
      && ['directory', 'file'].includes(entry.stat.kind));
    requireThat(BigInt(entry.stat.mode) <= 0xffffn);
    const actual = parseMountInfoLine(entry.mountInfo, entry.target), mode = BigInt(entry.stat.mode) & 0o170000n;
    requireThat(actual.device === linuxDevice(entry.stat.dev) && !mountIds.has(actual.id)
      && (entry.stat.kind === 'directory' ? mode === 0o040000n : mode === 0o100000n && entry.stat.nlink === '1'));
    requireThat(!deviceTypes.has(actual.device) || deviceTypes.get(actual.device) === actual.type);
    deviceTypes.set(actual.device, actual.type); mountIds.add(actual.id); records.set(entry.target, { ...actual, ...entry.stat });
  }
  const parent = records.get(targets.at(-1));
  requireThat(parent?.kind === 'directory' && parent.dev === String(parentStat.dev) && parent.ino === String(parentStat.ino));
  requireThat(records.get('/data')?.kind === 'directory');
  const candidate = path.posix.join(parent.root, artifactBase);
  const contains = (a, b) => a === b || b.startsWith(a === '/' ? '/' : a + '/');
  for (const target of targets.slice(0, -1)) {
    const source = records.get(target); requireThat(source);
    if (source.device === parent.device && (contains(source.root, candidate) || contains(candidate, source.root))) fail('backup_directory_overlaps_source');
  }
  return { sourceRootCount: targets.length - 1, proofSha256: sha(JSON.stringify(proof)) };
}

// This public source is carried as one exact Node -e argument. Raw roots only
// traverse a bounded pipe with logging disabled; callers return hashes/counts.
export const MOUNT_PROBE_SOURCE = String.raw`
import fs from 'node:fs';
try {
  if(Number(process.versions.node.split('.')[0])!==24 || process.getuid()!==0 || process.argv.length!==2)throw Error();
  const input=JSON.parse(process.argv[1]);
  if(Object.keys(input).sort().join()!=='nonce,targets'||!/^[a-f0-9]{32}$/.test(input.nonce)||!Array.isArray(input.targets)||input.targets.length<2||input.targets.length>65)throw Error();
  const fd=fs.openSync('/proc/self/mountinfo',fs.constants.O_RDONLY),chunks=[];let total=0;
  try{for(;;){const bytes=Buffer.alloc(65536),n=fs.readSync(fd,bytes,0,bytes.length,null);if(!n)break;total+=n;if(total>1048576)throw Error();chunks.push(bytes.subarray(0,n));}}finally{fs.closeSync(fd);}
  const text=new TextDecoder('utf-8',{fatal:true}).decode(Buffer.concat(chunks));
  const lines=text.split('\n').filter(Boolean);
  const roots=input.targets.map(target=>{
    if(!/^\/(?:data|soty-mount-proof\/(?:s[0-9]{2}|parent))$/.test(target))throw Error();
    const found=lines.filter(line=>line.split(' ')[4]===target);
    if(found.length!==1||lines.some(line=>line.split(' ')[4]?.startsWith(target+'/')))throw Error();
    const stat=fs.lstatSync(target,{bigint:true});
    if(stat.isSymbolicLink()||!stat.isFile()&&!stat.isDirectory())throw Error();
    return {target,mountInfo:found[0],stat:{dev:String(stat.dev),ino:String(stat.ino),mode:String(stat.mode),nlink:String(stat.nlink),kind:stat.isDirectory()?'directory':'file'}};
  });
  process.stdout.write(JSON.stringify({schema:'soty.backup.mount-proof.v1',nonce:input.nonce,roots})+'\n');
}catch{process.stdout.write('{"ok":false}\n');process.exitCode=1;}
`;

function boundedOutput(stream, child) {
  return new Promise((resolve, reject) => {
    const parts = []; let total = 0;
    stream.on('data', value => { total += value.length; if (total > MAX_OUTPUT) { child.kill('SIGKILL'); reject(new Error('backup_mount_proof_failed')); } else parts.push(value); });
    stream.once('error', () => reject(new Error('backup_mount_proof_failed')));
    stream.once('end', () => resolve(Buffer.concat(parts)));
  });
}
function actualMounts(actual, expected, volumePath) {
  requireThat(Array.isArray(actual) && actual.length === expected.length);
  for (const mount of expected) {
    const hit = actual.filter(value => value.Destination === mount.Target && value.Type === mount.Type);
    requireThat(hit.length === 1 && hit[0].RW === false
      && (mount.Type === 'volume' ? hit[0].Name === mount.Source && hit[0].Source === volumePath : hit[0].Source === mount.Source));
  }
}

/** Trusted ports are supplied by the operator module, never by request JSON. */
export async function assertArtifactMountIsolation({ engine, image, operationId, original, parent, parentStat, artifactBase, cap, run, stop }) {
  requireThat(IMAGE.test(image) && NONCE.test(operationId) && ID.test(original?.Id ?? '')
    && IMAGE.test(original.Image ?? '') && original.State?.Running === false
    && Array.isArray(original.Mounts) && original.Mounts.length >= 1 && original.Mounts.length <= MAX_ROOTS
    && path.posix.isAbsolute(parent) && typeof run === 'function' && typeof stop === 'function');
  const sources = stableMounts(original.Mounts);
  requireThat(sources.filter(mount => mount.Type === 'volume' && mount.Destination === '/data').length === 1
    && sources.every(mount => ['volume', 'bind'].includes(mount.Type) && path.posix.isAbsolute(mount.Source ?? '')
      && (mount.Type !== 'volume' || mount.Destination === '/data' && /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/u.test(mount.Name ?? ''))));
  for (const mount of sources.filter(value => value.Type === 'bind')) {
    const stat = await lstat(mount.Source, { bigint: true });
    requireThat(!stat.isSymbolicLink() && (stat.isDirectory() || stat.isFile() && stat.nlink === 1n));
  }
  const volume = sources.find(mount => mount.Type === 'volume');
  const volumeValue = await engine.request('GET', '/volumes/' + encodeURIComponent(volume.Name));
  requireThat(volumeValue?.Name === volume.Name && volumeValue.Driver === 'local' && volumeValue.Scope === 'local'
    && volumeValue.Mountpoint === volume.Source && Object.keys(volumeValue.Options ?? {}).length === 0);
  const sourceHash = sourceStamp(original), nonce = randomBytes(16).toString('hex'), name = 'soty-mount-proof-' + operationId;
  const imageValue = await engine.image(image);
  requireThat(imageValue?.Id === image && Object.keys(imageValue.Config?.Volumes ?? {}).every(target => target === '/data'));
  const current = async () => requireThat(sourceStamp(await engine.inspect(original.Id)) === sourceHash);
  await current();
  try { await engine.inspect(name); fail(); } catch (error) { if (error.code !== 'engine_http_404') throw error; }
  const mounts = sources.map((mount, index) => mount.Type === 'volume'
    ? { Type: 'volume', Source: mount.Name, Target: '/data', ReadOnly: true, VolumeOptions: { NoCopy: true } }
    : { Type: 'bind', Source: mount.Source, Target: PREFIX + '/s' + String(index).padStart(2, '0'), ReadOnly: true });
  mounts.push({ Type: 'bind', Source: parent, Target: PREFIX + '/parent', ReadOnly: true });
  const targets = mounts.map(mount => mount.Target), entrypoint = ['node'];
  const command = ['--max-old-space-size=32', '--input-type=module', '-e', MOUNT_PROBE_SOURCE, '--', JSON.stringify({ nonce, targets })];
  const config = { Image: image, Entrypoint: entrypoint, Cmd: command, WorkingDir: '/', User: '0:0',
    Env: ['PATH=/usr/local/bin:/usr/bin:/bin'], Labels: { [LABEL]: nonce }, Healthcheck: { Test: ['NONE'] },
    AttachStdin: false, AttachStdout: true, AttachStderr: false, OpenStdin: false, Tty: false,
    HostConfig: { NetworkMode: 'none', ReadonlyRootfs: true, Privileged: false, LogConfig: { Type: 'none', Config: {} },
      CapDrop: ['ALL'], CapAdd: ['DAC_READ_SEARCH'], SecurityOpt: ['no-new-privileges'], RestartPolicy: { Name: 'no' },
      PortBindings: {}, PublishAllPorts: false, Memory: 134217728, MemorySwap: 134217728,
      PidsLimit: 32, CpuPeriod: 100000, CpuQuota: 100000, Mounts: mounts } };
  let id, running;
  const inspect = async () => {
    const value = await engine.inspect(id), host = value.HostConfig;
    requireThat(value.Id === id && value.Name === '/' + name && value.Image === image && value.Config?.Labels?.[LABEL] === nonce
      && value.Config.User === '0:0' && value.Config.WorkingDir === '/'
      && JSON.stringify(value.Config.Entrypoint) === JSON.stringify(entrypoint) && JSON.stringify(value.Config.Cmd) === JSON.stringify(command)
      && sha(value.Config.Cmd[3]) === sha(MOUNT_PROBE_SOURCE) && value.Config.Healthcheck?.Test?.join() === 'NONE'
      && value.Config.OpenStdin === false && value.Config.Tty === false
      && host?.NetworkMode === 'none' && host.ReadonlyRootfs === true && host.Privileged === false
      && host.LogConfig?.Type === 'none' && Object.keys(host.LogConfig.Config ?? {}).length === 0
      && Object.keys(host.PortBindings ?? {}).length === 0 && host.PublishAllPorts !== true
      && !(host.Binds ?? []).length && !(host.VolumesFrom ?? []).length && !Object.keys(host.Tmpfs ?? {}).length && host.AutoRemove !== true
      && (host.CapDrop ?? []).join() === 'ALL' && (host.CapAdd ?? []).join() === 'DAC_READ_SEARCH'
      && (host.SecurityOpt ?? []).join() === 'no-new-privileges' && host.RestartPolicy?.Name === 'no'
      && host.Memory === 134217728 && host.MemorySwap === 134217728 && host.PidsLimit === 32
      && host.CpuPeriod === 100000 && host.CpuQuota === 100000 && !(host.Devices ?? []).length
      && !(host.DeviceRequests ?? []).length && !host.PidMode && host.IpcMode === 'private' && !host.UtsMode);
    actualMounts(value.Mounts, mounts, volume.Source); return value;
  };
  try {
    const created = await engine.create(name, config); requireThat(ID.test(created?.Id ?? '')); id = created.Id;
    requireThat((await inspect()).State?.Running === false); await current();
    running = run(['start', '--attach', id], { ...cap, wallMs: Math.min(cap.wallMs, 30000) });
    running.child.stdin.end();
    const [bytes] = await Promise.all([boundedOutput(running.child.stdout, running.child), running.done]);
    const completed = await inspect(); requireThat(completed.State?.Running === false && completed.State.ExitCode === 0);
    let proof; try { proof = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); } catch { fail(); }
    const result = assertPhysicalIsolation(proof, { nonce, targets, parentStat, artifactBase });
    await current(); return result;
  } finally {
    if (running) { running.child.kill('SIGKILL'); await running.done.catch(() => {}); }
    if (id) {
      await stop(id, LABEL, nonce, image);
      requireThat((await inspect()).State?.Running === false);
      await engine.remove(id);
    }
  }
}
