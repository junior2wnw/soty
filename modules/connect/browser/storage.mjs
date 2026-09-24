import { ConnectError, canonicalJson, validateInstallationKeys } from './crypto.mjs';

export const DATABASE_VERSION = 1;
export const STATE_SCHEMA = 'connect.browser-state.v1';
const STORE = 'state';

const corrupt = message => { throw new ConnectError('CORRUPT_LOCAL_STATE', message + ' Existing data was not replaced.'); };
const id = value => typeof value === 'string' && value.length > 0 && value.length <= 256;

export function emptyState(projectId, endpoint) {
  return { schema: STATE_SCHEMA, minReaderEpoch: 1, extensions: {}, projectId, endpoint, localRevision: 0, activeDeviceId: null, installations: [], pendingEnrollment: null, pendingRecovery: null, preparedRecovery: null };
}

export function validateState(state, { projectId, endpoint }) {
  if (!state || typeof state !== 'object' || state.schema !== STATE_SCHEMA) {
    throw new ConnectError(state?.schema && state.schema !== STATE_SCHEMA ? 'UNSUPPORTED_LOCAL_STATE' : 'CORRUPT_LOCAL_STATE', 'The saved state cannot be read by this version. Existing data was not replaced.');
  }
  if (state.projectId !== projectId || state.endpoint !== endpoint) throw new ConnectError('LOCAL_SCOPE_MISMATCH', 'This local vault belongs to another project or server.');
  const readerEpoch = state.minReaderEpoch ?? 1;
  if (!Number.isSafeInteger(readerEpoch) || readerEpoch < 1 || readerEpoch > 1) throw new ConnectError('UNSUPPORTED_LOCAL_STATE', 'This saved state requires a newer reader. Existing data was not replaced.');
  if (state.extensions !== undefined && (!state.extensions || Array.isArray(state.extensions) || typeof state.extensions !== 'object')) corrupt('The extension data is invalid.');
  if (canonicalJson(state.extensions ?? {}).length > 256 * 1024) corrupt('The extension data is too large.');
  const allowed = new Set(['schema', 'minReaderEpoch', 'extensions', 'projectId', 'endpoint', 'localRevision', 'activeDeviceId', 'installations', 'pendingEnrollment', 'pendingRecovery', 'preparedRecovery']);
  if (Object.keys(state).some(key => !allowed.has(key))) throw new ConnectError('UNSUPPORTED_LOCAL_STATE', 'The saved state has unknown fields. Existing data was not replaced.');
  if (!Number.isSafeInteger(state.localRevision) || state.localRevision < 0 || !Array.isArray(state.installations) || state.installations.length > 64) corrupt('The saved profile list is invalid.');
  const ids = new Set();
  for (const installation of state.installations) {
    if (!installation || !id(installation.deviceId) || ids.has(installation.deviceId) || typeof installation.label !== 'string' || installation.label.length > 80 ||
        typeof installation.createdAt !== 'string' || !Number.isFinite(Date.parse(installation.createdAt)) ||
        installation.accountId !== null && !id(installation.accountId) || !Number.isSafeInteger(installation.vaultRevision) || installation.vaultRevision < 0 ||
        typeof installation.revoked !== 'boolean') corrupt('A saved profile is invalid.');
    validateInstallationKeys(installation);
    if (installation.rootEnvelope !== null && (!installation.rootEnvelope || installation.rootEnvelope.schema !== 'connect.local-root.v1' ||
        installation.rootEnvelope.projectId !== projectId || installation.rootEnvelope.deviceId !== installation.deviceId ||
        typeof installation.rootEnvelope.iv !== 'string' || typeof installation.rootEnvelope.ciphertext !== 'string')) corrupt('A saved profile vault is invalid.');
    if (installation.accountId !== null && !installation.rootEnvelope) corrupt('An existing account is missing its data key.');
    ids.add(installation.deviceId);
  }
  if (state.activeDeviceId !== null && !ids.has(state.activeDeviceId)) corrupt('The active profile is missing.');
  if (state.activeDeviceId === null && state.installations.some(item => item.accountId !== null)) corrupt('Existing profiles have lost their active selection.');
  const pending = state.pendingEnrollment;
  if (pending !== null && (!pending || !ids.has(pending.deviceId) || typeof pending.label !== 'string' ||
      pending.requestId !== null && !id(pending.requestId) || pending.expiresAt !== null && !Number.isFinite(pending.expiresAt))) corrupt('The pending enrollment is invalid.');
  const recovery = state.pendingRecovery;
  if (recovery !== null && (!recovery || !ids.has(recovery.deviceId) || !id(recovery.accountId) || !id(recovery.recoveryId) ||
      typeof recovery.kitFingerprint !== 'string' || typeof recovery.label !== 'string')) corrupt('The pending recovery is invalid.');
  if (state.preparedRecovery !== null && (!state.preparedRecovery || !id(state.preparedRecovery.accountId) || !id(state.preparedRecovery.recoveryId) ||
      typeof state.preparedRecovery.kitFingerprint !== 'string')) corrupt('The recovery preparation is invalid.');
  return state;
}

/** Fixed-version IndexedDB storage. All writes resolve only after transaction commit. */
export function createIndexedDbStorage({ dbName, projectId, endpoint, indexedDB = globalThis.indexedDB }) {
  const scope = { projectId, endpoint };
  const key = projectId;
  function open() {
    if (!indexedDB?.open) return Promise.reject(new ConnectError('STORAGE_UNAVAILABLE', 'Persistent browser storage is unavailable.'));
    return new Promise((resolve, reject) => {
      let request;
      try { request = indexedDB.open(dbName, DATABASE_VERSION); }
      catch (error) { reject(new ConnectError('STORAGE_UNAVAILABLE', 'The local vault could not be opened.', { cause: error })); return; }
      let failed = false;
      const fail = error => { failed = true; reject(error); };
      request.onupgradeneeded = event => {
        if (event.oldVersion !== 0) {
          request.transaction.abort();
          fail(new ConnectError('UNSUPPORTED_DATABASE', 'The database version is not supported.'));
          return;
        }
        request.result.createObjectStore(STORE);
      };
      request.onblocked = () => fail(new ConnectError('STORAGE_BLOCKED', 'Another tab is blocking the local vault. Close the old tab and retry.'));
      request.onerror = () => fail(new ConnectError(request.error?.name === 'VersionError' ? 'UNSUPPORTED_DATABASE' : 'STORAGE_READ_FAILED', 'The local vault could not be read. Existing data was not replaced.', { cause: request.error }));
      request.onsuccess = () => {
        const database = request.result;
        database.onversionchange = () => database.close();
        if (failed) { database.close(); return; }
        if (database.version !== DATABASE_VERSION || !database.objectStoreNames.contains(STORE)) {
          database.close(); fail(new ConnectError('CORRUPT_DATABASE', 'The local vault schema is missing. Existing data was not replaced.')); return;
        }
        resolve(database);
      };
    });
  }

  async function transact(mode, update) {
    const database = await open();
    try {
      return await new Promise((resolve, reject) => {
        let transaction, result, failure;
        try { transaction = database.transaction(STORE, mode); }
        catch (error) { reject(new ConnectError('STORAGE_TRANSACTION_FAILED', 'The local vault transaction could not start.', { cause: error })); return; }
        const store = transaction.objectStore(STORE), request = store.get(key);
        request.onsuccess = () => {
          try {
            const value = request.result === undefined ? null : validateState(request.result, scope);
            const next = update(value);
            if (next?.then) throw new ConnectError('ASYNC_STORAGE_MUTATION', 'Local vault mutations must be synchronous.');
            result = next?.result;
            if (next?.write) { validateState(next.write, scope); store.put(next.write, key); }
          } catch (error) { failure = error; transaction.abort(); }
        };
        request.onerror = () => { failure = new ConnectError('STORAGE_READ_FAILED', 'The local vault could not be read.', { cause: request.error }); };
        transaction.oncomplete = () => resolve(result);
        transaction.onerror = () => { failure ||= new ConnectError('STORAGE_WRITE_FAILED', 'The local vault could not be committed.', { cause: transaction.error }); };
        transaction.onabort = () => reject(failure || new ConnectError('STORAGE_ABORTED', 'The local vault transaction was aborted. Existing data was not replaced.', { cause: transaction.error }));
      });
    } finally { database.close(); }
  }

  return {
    read: () => transact('readonly', value => ({ result: value })),
    claim: candidate => transact('readwrite', value => value ? { result: value } : { write: candidate, result: candidate }),
    compareAndSwap: (expectedRevision, candidate) => transact('readwrite', value => {
      if (!value || value.localRevision !== expectedRevision) throw new ConnectError('LOCAL_CONFLICT', 'Another tab changed this profile. Read it again and retry.');
      if (candidate.localRevision !== expectedRevision + 1) throw new ConnectError('INVALID_LOCAL_REVISION', 'The local revision must advance exactly once.');
      for (const previous of value.installations) {
        const next = candidate.installations.find(item => item.deviceId === previous.deviceId);
        if (!next || previous.accountId !== null && previous.accountId !== next.accountId ||
            previous.signingPublicJwk.x !== next.signingPublicJwk.x || previous.signingPublicJwk.y !== next.signingPublicJwk.y ||
            previous.encryptionPublicJwk.x !== next.encryptionPublicJwk.x || previous.encryptionPublicJwk.y !== next.encryptionPublicJwk.y) {
          throw new ConnectError('IMMUTABLE_PROFILE', 'An existing profile or device key cannot be silently replaced.');
        }
      }
      return { write: candidate, result: candidate };
    }),
  };
}
