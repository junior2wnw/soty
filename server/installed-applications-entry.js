import { openSync, closeSync, fstatSync, lstatSync, readSync, constants } from 'node:fs';
import path from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { types, TextDecoder } from 'node:util';
import { AccessError, canonicalHash } from '../modules/capabilities/server/validation.mjs';
import { captureQueryData } from '../modules/capabilities/server/readonly-queries.mjs';
import { loadInstalledExistingApplications } from './installed-existing-applications.js';
import { captureNativeSourceHandlers } from './native-source-handlers.js';
import { createTavyshNativeDeviceBroker } from './tavysh-native-device-broker.js';
import { createCurrentContextSourceApplication } from './current-context-source.js';
import { createTavyshDeviceStatusAdapter } from './tavysh-device-read-adapter.js';

const invalid = () => { throw new AccessError('installed_applications_entry_invalid'); };
const need = value => { if (!value) invalid(); };
function fields(input, required, optional = []) {
  need(input && typeof input === 'object' && !types.isProxy(input) && !Array.isArray(input)
    && [Object.prototype, null].includes(Object.getPrototypeOf(input)));
  const descriptors = Object.getOwnPropertyDescriptors(input);
  need(Reflect.ownKeys(descriptors).every(key => typeof key === 'string'
    && [...required, ...optional].includes(key) && descriptors[key].enumerable && Object.hasOwn(descriptors[key], 'value'))
    && required.every(key => Object.hasOwn(descriptors, key)));
  return Object.fromEntries(Object.entries(descriptors).map(([key, item]) => [key, item.value]));
}
function field(env, name) {
  const descriptor = Object.getOwnPropertyDescriptor(env, name);
  need(!descriptor || Object.hasOwn(descriptor, 'value'));
  const value = descriptor?.value;
  need(value === undefined || typeof value === 'string');
  return value ?? '';
}
const stampEqual = (a, b) => ['dev', 'ino', 'size', 'mtimeNs', 'ctimeNs'].every(key => a[key] === b[key]);
function captureCode(filename, expectedHash) {
  need(filename.length <= 4096 && path.isAbsolute(filename) && filename === filename.trim()
    && !/[\u0000-\u001f]/u.test(filename) && path.extname(filename) === '.mjs'
    && /^[a-f0-9]{64}$/u.test(expectedHash));
  if (process.platform === 'win32') need(/^[A-Za-z]:[\\/]$/u.test(path.parse(filename).root));
  let fd;
  const buffer = Buffer.alloc(131073);
  try {
    const before = lstatSync(filename, { bigint: true });
    need(before.isFile() && !before.isSymbolicLink() && before.size > 0n && before.size <= 131072n);
    fd = openSync(filename, constants.O_RDONLY | (constants.O_NONBLOCK || 0) | (constants.O_NOFOLLOW || 0));
    const held = fstatSync(fd, { bigint: true }); need(stampEqual(before, held));
    let used = 0;
    while (used < buffer.length) {
      const count = readSync(fd, buffer, used, buffer.length - used, used);
      if (!count) break;
      used += count;
    }
    need(used === Number(held.size) && used <= 131072);
    need(stampEqual(held, fstatSync(fd, { bigint: true })) && stampEqual(held, lstatSync(filename, { bigint: true })));
    const captured = buffer.subarray(0, used);
    need(createHash('sha256').update(captured).digest('hex') === expectedHash);
    new TextDecoder('utf-8', { fatal: true }).decode(captured);
    // Import these exact captured bytes, never re-read a path after the hash check.
    return 'data:text/javascript;base64,' + captured.toString('base64') + '#installed=' + randomUUID();
  } catch { invalid(); }
  finally { buffer.fill(0); if (fd !== undefined) closeSync(fd); }
}

/** Startup-only opt-in to ONE operator-installed, hash-approved CODE module.
 * No HTTP/manifest/module-discovery interface and no authority/fixture toggles.
 * The module is trusted operator code, not an untrusted-code sandbox. Its import
 * must be pure; effectful factory work registers ownClose before creating it. */
export async function loadInstalledApplicationsEntry(env = process.env) {
  let moduleUrl;
  try {
    need(env && typeof env === 'object' && !types.isProxy(env) && !Array.isArray(env));
    const filename = field(env, 'SOTY_INSTALLED_APPLICATIONS_MODULE');
    const digest = field(env, 'SOTY_INSTALLED_APPLICATIONS_SHA256');
    if (!filename && !digest) return Object.freeze({ httpOptions: Object.freeze({}), sourceReferences: Object.freeze([]), close: async () => {} });
    need(filename && digest); moduleUrl = captureCode(filename, digest);
  } catch { invalid(); }

  const closers = new Map(); let stopped = false, cleanup, nativeSources;
  const ownClose = callback => {
    need(!stopped && typeof callback === 'function' && !types.isProxy(callback));
    if (closers.has(callback)) return closers.get(callback);
    let called = false, completion;
    const once = () => {
      if (called) return completion ?? Promise.resolve();
      called = true;
      try { completion = Promise.resolve(callback()).catch(() => {}); }
      catch { completion = Promise.resolve(); }
      return completion;
    };
    closers.set(callback, once); return once;
  };
  const close = () => {
    if (stopped) return cleanup ?? Promise.resolve();
    stopped = true;
    // Fence dispatch immediately; all original resource callbacks share once refs.
    const pending = [nativeSources?.close(), ...[...closers.values()].map(callback => callback())];
    cleanup = Promise.all(pending).then(() => undefined); return cleanup;
  };
  try {
    const module = await import(moduleUrl);
    need(Reflect.ownKeys(module).filter(key => typeof key === 'string').join(',') === 'default');
    const factory = Object.getOwnPropertyDescriptor(module, 'default')?.value;
    need(typeof factory === 'function' && !types.isProxy(factory));
    const api = Object.freeze({ loadInstalledExistingApplications, createTavyshNativeDeviceBroker,
      createTavyshDeviceStatusAdapter, createCurrentContextSourceApplication, ownClose });
    const produced = factory(api);
    need(!types.isProxy(produced));
    const result = fields(types.isPromise(produced) ? await produced : produced, ['httpOptions', 'sourceReferences', 'close'], ['sourceCredentialFor']);
    ownClose(result.close);
    const sourceCredentialFor = result.sourceCredentialFor;
    need(sourceCredentialFor === undefined || typeof sourceCredentialFor === 'function' && !types.isProxy(sourceCredentialFor)
      && !types.isAsyncFunction(sourceCredentialFor) && !types.isGeneratorFunction(sourceCredentialFor));
    const options = fields(result.httpOptions, ['externalApplications'], ['nativeSourceHandlers']);
    const composition = await loadInstalledExistingApplications({ createRegistry: () => options.externalApplications });
    need(canonicalHash(captureQueryData(result.sourceReferences)) === canonicalHash(composition.sourceReferences));
    const rows = options.nativeSourceHandlers === undefined ? [] : options.nativeSourceHandlers;
    // The existing validator rejects all accessors/proxies before these DATA refs
    // are copied; its second capture owns the shared once-wrapped close functions.
    captureNativeSourceHandlers({ nativeSourceHandlers: rows });
    const retained = Object.freeze(Array.from(rows, row => {
      const descriptors = Object.getOwnPropertyDescriptors(row);
      return Object.freeze({ handle: descriptors.handle.value, close: ownClose(descriptors.close.value) });
    }));
    nativeSources = captureNativeSourceHandlers({ nativeSourceHandlers: retained });
    const lifecycle = Object.freeze({
      async handle(req, res) {
        let continued = false;
        await nativeSources.middleware(req, res, () => { continued = true; });
        return !continued;
      }, close,
    });
    return Object.freeze({ httpOptions: Object.freeze({ ...composition.httpOptions,
      nativeSourceHandlers: Object.freeze([lifecycle]) }), sourceReferences: composition.sourceReferences,
      ...(sourceCredentialFor === undefined ? {} : { sourceCredentialFor }), close });
  } catch {
    await close();
    // Never retain/serialize an operator exception, getter, proxy, path or secret.
    throw new AccessError('installed_applications_entry_unavailable');
  }
}
