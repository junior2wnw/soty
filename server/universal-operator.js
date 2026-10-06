import { createServer, createConnection } from 'node:net';
import { mkdir, lstat, realpath, chmod, unlink, link } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';
import { parseContractJson } from '../modules/app-contract/json.mjs';
import { UNIVERSAL_RUNTIME_SCHEMA, UNIVERSAL_RUNTIME_MAX_BYTES, validateUniversalPreparedness } from '../modules/app-contract/universal-preparedness.mjs';

export const universalOperatorPath = '/tmp/soty-operator/universal.sock';
export const UNIVERSAL_OPERATOR_LIMITS = Object.freeze({ connections: 8, requestBytes: 128, responseBytes: UNIVERSAL_RUNTIME_MAX_BYTES,
  timeoutMs: 1000, staleProbeMs: 250 });
const directory = '/tmp/soty-operator', requestBytes = Buffer.from(UNIVERSAL_RUNTIME_SCHEMA + '\n');
export class UniversalOperatorError extends Error {
  constructor(code = 'universal_operator_unavailable') { super(code); this.name = 'UniversalOperatorError'; this.code = code; }
}
const require = (ok, code) => { if (!ok) throw new UniversalOperatorError(code); };
const supported = () => process.platform !== 'win32' && typeof process.getuid === 'function';
const owner = () => process.getuid();
const same = (a, b) => a.dev === b.dev && a.ino === b.ino && a.uid === b.uid && a.mode === b.mode && a.ctimeNs === b.ctimeNs;
async function named(path) { try { return await lstat(path, { bigint: true }); } catch (error) { if (error.code === 'ENOENT') return null; throw new UniversalOperatorError(); } }
async function ownedDirectory() {
  const temporary = await named('/tmp');
  require(temporary?.isDirectory() && !temporary.isSymbolicLink() && await realpath('/tmp') === '/tmp'
    && (temporary.uid === 0n || temporary.uid === BigInt(owner()))
    && ((temporary.mode & 0o022n) === 0n || (temporary.mode & 0o1000n) !== 0n), 'universal_operator_path_invalid');
  const current = await named(directory);
  require(current?.isDirectory() && !current.isSymbolicLink() && current.uid === BigInt(owner())
    && (current.mode & 0o777n) === 0o700n && await realpath(directory) === directory, 'universal_operator_path_invalid');
  return current;
}
function ownedSocket(stat) {
  return Boolean(stat?.isSocket() && !stat.isSymbolicLink() && stat.uid === BigInt(owner()) && stat.nlink === 1n && (stat.mode & 0o777n) === 0o600n);
}
async function listenerAlive() {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ path: universalOperatorPath }); let finished = false;
    const timer = setTimeout(() => finish(new UniversalOperatorError('universal_operator_socket_busy')), UNIVERSAL_OPERATOR_LIMITS.staleProbeMs);
    const finish = (error, alive) => { if (finished) return; finished = true; clearTimeout(timer); socket.destroy(); error ? reject(error) : resolve(alive); };
    socket.once('connect', () => finish(null, true));
    socket.once('error', error => error.code === 'ECONNREFUSED' ? finish(null, false) : finish(new UniversalOperatorError('universal_operator_socket_busy')));
  });
}
async function checkNames(directoryIdentity, socketIdentity) {
  const currentDirectory = await ownedDirectory(), currentSocket = await named(universalOperatorPath);
  // Directory mtime/ctime change when its socket is created/unlinked; inode/owner/mode must remain.
  require(currentDirectory.dev === directoryIdentity.dev && currentDirectory.ino === directoryIdentity.ino
    && currentDirectory.uid === directoryIdentity.uid && currentDirectory.mode === directoryIdentity.mode
    && ownedSocket(currentSocket) && same(currentSocket, socketIdentity), 'universal_operator_path_changed');
}
/** Trusted host callback only. No TCP/HTTP endpoint, actor, bearer, request params or mutation commands. */
export async function startUniversalOperator(options) {
  require(options && typeof options === 'object' && [Object.prototype, null].includes(Object.getPrototypeOf(options)), 'universal_operator_capture_invalid');
  const fields = Object.getOwnPropertyDescriptors(options);
  require(Reflect.ownKeys(fields).length === 1 && fields.capture?.enumerable && 'value' in fields.capture
    && typeof fields.capture.value === 'function' && fields.capture.value.constructor?.name !== 'AsyncFunction', 'universal_operator_capture_invalid');
  const capture = fields.capture.value;
  if (!supported()) return Object.freeze({ supported: false, path: null, async close() { return false; } });
  try { await mkdir(directory, { mode: 0o700 }); } catch (error) { if (error.code !== 'EEXIST') throw new UniversalOperatorError('universal_operator_path_invalid'); }
  let directoryIdentity = await ownedDirectory();
  const stale = await named(universalOperatorPath);
  if (stale) {
    require(ownedSocket(stale), 'universal_operator_path_invalid');
    require(!await listenerAlive(), 'universal_operator_socket_busy');
    const now = await named(universalOperatorPath), parent = await ownedDirectory();
    require(parent.dev === directoryIdentity.dev && parent.ino === directoryIdentity.ino && parent.uid === directoryIdentity.uid
      && parent.mode === directoryIdentity.mode && ownedSocket(now) && same(stale, now), 'universal_operator_path_changed'); await unlink(universalOperatorPath);
  }
  const sockets = new Set(); let active = false, socketIdentity;
  // libuv unlinks its original bound pathname on server.close(). Bind an
  // unguessable private name, publish via no-replace hardlink, then remove that
  // original name. Only our inode-checked code can unlink the fixed public name.
  const listenerPath = directory + '/.universal-' + randomBytes(16).toString('hex') + '.sock';
  const server = createServer({ allowHalfOpen: true }, socket => {
    if (!active || sockets.size >= UNIVERSAL_OPERATOR_LIMITS.connections) { socket.destroy(); return; }
    sockets.add(socket); let length = 0, chunks = [], responded = false;
    const timer = setTimeout(() => socket.destroy(), UNIVERSAL_OPERATOR_LIMITS.timeoutMs); timer.unref();
    const replyError = () => { if (!responded && !socket.destroyed) { responded = true; socket.end('{"error":"universal_operator_unavailable"}\n'); } };
    socket.on('error', () => {}); socket.once('close', () => { clearTimeout(timer); sockets.delete(socket); chunks = []; });
    socket.on('data', chunk => {
      length += chunk.length;
      if (responded || length > UNIVERSAL_OPERATOR_LIMITS.requestBytes) { chunks = []; socket.destroy(); return; }
      chunks.push(chunk);
    });
    socket.once('end', async () => {
      try {
        require(active && !responded && length === requestBytes.length && Buffer.concat(chunks, length).equals(requestBytes)); chunks = [];
        await checkNames(directoryIdentity, socketIdentity); require(active && !socket.destroyed);
        const value = capture();
        if (value instanceof Promise) { Promise.prototype.catch.call(value, () => {}); throw new UniversalOperatorError(); }
        const measurement = validateUniversalPreparedness(value), output = Buffer.from(JSON.stringify(measurement) + '\n');
        require(active && !socket.destroyed && output.length <= UNIVERSAL_OPERATOR_LIMITS.responseBytes);
        responded = true; socket.end(output);
      } catch { replyError(); }
    });
  });
  server.on('error', () => { active = false; for (const socket of sockets) socket.destroy(); });
  try {
    await new Promise((resolve, reject) => {
      server.once('error', reject); server.listen(listenerPath, () => { server.removeListener('error', reject); resolve(); });
    });
    await chmod(listenerPath, 0o600); const bound = await named(listenerPath); require(ownedSocket(bound), 'universal_operator_path_invalid');
    await link(listenerPath, universalOperatorPath);
    const published = await named(universalOperatorPath);
    require(published?.isSocket() && published.dev === bound.dev && published.ino === bound.ino && published.uid === bound.uid
      && published.mode === bound.mode && published.nlink === 2n, 'universal_operator_path_changed');
    await unlink(listenerPath); socketIdentity = await named(universalOperatorPath); require(ownedSocket(socketIdentity), 'universal_operator_path_invalid');
    await checkNames(directoryIdentity, socketIdentity); active = true;
  } catch {
    active = false; for (const socket of sockets) socket.destroy();
    if (socketIdentity) {
      const current = await named(universalOperatorPath);
      if (ownedSocket(current) && same(current, socketIdentity)) await unlink(universalOperatorPath);
    }
    if (server.listening) await new Promise(resolve => server.close(resolve));
    throw new UniversalOperatorError('universal_operator_start_failed');
  }
  let closing;
  return Object.freeze({ supported: true, path: universalOperatorPath,
    close() {
      if (closing) return closing;
      active = false; for (const socket of sockets) socket.destroy();
      closing = (async () => {
        let failure;
        try {
          const parent = await ownedDirectory(), current = await named(universalOperatorPath);
          if (parent.dev === directoryIdentity.dev && parent.ino === directoryIdentity.ino && ownedSocket(current) && same(current, socketIdentity)) await unlink(universalOperatorPath);
        } catch { failure = new UniversalOperatorError('universal_operator_close_failed'); }
        finally { await new Promise(resolve => server.close(resolve)); }
        if (failure) throw failure; return true;
      })(); return closing;
    } });
}
/** Fixed request for a trusted Docker-exec reader. No caller-supplied path or request body. */
export async function readUniversalOperator() {
  require(supported(), 'universal_operator_unsupported');
  const directoryIdentity = await ownedDirectory(), socketIdentity = await named(universalOperatorPath);
  require(ownedSocket(socketIdentity), 'universal_operator_path_invalid');
  return new Promise((resolve, reject) => {
    const socket = createConnection({ path: universalOperatorPath }), chunks = []; let length = 0, done = false, ended = false;
    const timer = setTimeout(() => finish(new UniversalOperatorError()), UNIVERSAL_OPERATOR_LIMITS.timeoutMs);
    const finish = (error, value) => { if (done) return; done = true; clearTimeout(timer); socket.destroy(); error ? reject(error) : resolve(value); };
    socket.once('connect', () => { socket.end(requestBytes); });
    socket.on('data', chunk => { length += chunk.length;
      if (length > UNIVERSAL_OPERATOR_LIMITS.responseBytes) finish(new UniversalOperatorError()); else chunks.push(chunk); });
    socket.once('error', () => finish(new UniversalOperatorError()));
    socket.once('end', async () => {
      ended = true;
      try {
        require(!done && length > 0); await checkNames(directoryIdentity, socketIdentity); require(!done);
        const value = validateUniversalPreparedness(parseContractJson(Buffer.concat(chunks, length))); finish(null, value);
      } catch { finish(new UniversalOperatorError()); }
    });
    socket.once('close', () => { if (!done && !ended) finish(new UniversalOperatorError()); });
  });
}
