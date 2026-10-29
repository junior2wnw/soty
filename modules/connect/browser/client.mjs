import {
  ConnectError, canonicalJson, base64url, unbase64url, sha256, cryptoApi, createInstallation,
  verifyInstallation, signMessage, publicJwk, sealLocalRoot, openLocalRoot,
  wrapEnrollmentRoot, unwrapEnrollmentRoot, wrapRecoveryRoot, unwrapRecoveryRoot, sealVault, openVault,
  normalizeCredentialInput, credentialLookup, makeCredential, unlockCredential,
} from './crypto.mjs';
import { createIndexedDbStorage, emptyState, validateState } from './storage.mjs';
import { capturePlatformInvitation } from './platform-invitation.mjs';

// Fixed voice ports leave the local queue but retain account and generation fences.
const detachedVoiceOperations = new Set(['voice.orb.status', 'voice.orb.start', 'voice.orb.turn', 'voice.orb.cancel', 'voice.orb.end']);

const parallelMetadataReads = new Set([
  'world.profile.get', 'world.community.list', 'world.directory.search', 'world.directory.resolve', 'world.field.get',
  'apps.directory.search', 'apps.directory.resolve', 'apps.devices', 'contacts.list',
  // Startup polls and visible covers must not hold the field's identity queue.
  'call.inbox', 'notifications.status', 'apps.cover.read', 'apps.discussion.inbox',
  'ai.account.get', 'ai.keys.list',
]);

function requiredString(value, name, max = 256) {
  if (typeof value !== 'string' || !value.length || value.length > max) throw new ConnectError('INVALID_ARGUMENT', name + ' is required.');
  return value;
}
function deviceLabel(value) {
  const result = requiredString(typeof value === 'string' ? value.normalize('NFC').trim() : value, 'Device label', 80);
  if (/[\u0000-\u001f\u007f]/u.test(result)) throw new ConnectError('INVALID_ARGUMENT', 'The label contains control characters.');
  return result;
}
function revision(value) {
  if (!Number.isSafeInteger(value) || value < 0) throw new ConnectError('INVALID_REVISION', 'The revision must be a nonnegative integer.');
  return value;
}
function responseId(value, name) {
  if (typeof value !== 'string' || !value.length || value.length > 256) throw new ConnectError('INVALID_SERVER_RESPONSE', 'The server returned an invalid ' + name + '.');
  return value;
}
function expires(value) {
  const time = typeof value === 'number' ? value : Date.parse(value);
  if (!Number.isFinite(time)) throw new ConnectError('INVALID_SERVER_RESPONSE', 'The server returned an invalid expiration time.');
  return time;
}

function normalizeOptions(options) {
  const projectId = requiredString(options?.projectId, 'Project ID', 128);
  const endpoint = new URL(requiredString(options?.endpoint, 'RPC endpoint', 2048), globalThis.location?.href || undefined);
  if (endpoint.username || endpoint.password || endpoint.hash || endpoint.search || endpoint.protocol !== 'https:' &&
      !(endpoint.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(endpoint.hostname))) {
    throw new ConnectError('UNSAFE_ENDPOINT', 'The RPC endpoint must use HTTPS, or localhost for development, without credentials or a fragment.');
  }
  const fetcher = options.fetch || globalThis.fetch?.bind(globalThis);
  if (typeof fetcher !== 'function') throw new ConnectError('FETCH_UNAVAILABLE', 'A fetch implementation is required.');
  if(options.scopedVoiceCleanup!==undefined && typeof options.scopedVoiceCleanup!=='boolean')throw new ConnectError('INVALID_ARGUMENT','Invalid private voice cleanup profile.');
  if(options.scopedAppCleanup!==undefined && typeof options.scopedAppCleanup!=='boolean')throw new ConnectError('INVALID_ARGUMENT','Invalid private cleanup profile.');
  return { ...options, projectId, endpoint: endpoint.href, fetcher };
}

function publicLocalState(state) {
  if (!state) return { schema: 'connect.local-view.v1', localRevision: null, accountId: null, deviceId: null, label: null, current: null, profiles: [], pendingEnrollment: null, pendingRecovery: null, recoveryPrepared: false };
  const describe = item => ({ accountId: item.accountId, deviceId: item.deviceId, label: item.label, active: item.deviceId === state.activeDeviceId,
    revoked: item.revoked, vaultRevision: item.vaultRevision, createdAt: item.createdAt });
  const active = state.installations.find(item => item.deviceId === state.activeDeviceId);
  return {
    schema: 'connect.local-view.v1', localRevision: state.localRevision, accountId: active?.accountId ?? null, deviceId: active?.deviceId ?? null, label: active?.label ?? null, current: active ? describe(active) : null,
    profiles: state.installations.filter(item => item.accountId !== null).map(describe),
    pendingEnrollment: state.pendingEnrollment ? { requestId: state.pendingEnrollment.requestId, label: state.pendingEnrollment.label, expiresAt: state.pendingEnrollment.expiresAt } : null,
    pendingRecovery: state.pendingRecovery ? { accountId: state.pendingRecovery.accountId, recoveryId: state.pendingRecovery.recoveryId, label: state.pendingRecovery.label } : null,
    recoveryPrepared: Boolean(active && state.preparedRecovery?.accountId === active.accountId),
  };
}

/** Public factory. Importing this module performs no I/O and creates no identity. */
export function createConnectClient(options) {
  const normalized = normalizeOptions(options);
  const dbName = options.dbName || 'connect-browser-v1:' + normalized.projectId;
  requiredString(dbName, 'Database name', 256);
  return createClientWithStorage(normalized, createIndexedDbStorage({ dbName, projectId: normalized.projectId, endpoint: normalized.endpoint }));
}

/** Storage injection is for isolated tests; production uses the public factory. */
export function createClientWithStorage(options, storage) {
  const normalized = options.fetcher ? options : normalizeOptions(options);
  const { projectId, endpoint, fetcher, onState, onError } = normalized;
  const scope = { projectId, endpoint };
  const platformInvitation = normalized.capturePlatformInvitation ? capturePlatformInvitation(projectId + ':' + endpoint) : null;
  let queue = Promise.resolve();
  const enrollmentPreviews = new Map();
  const appCleanupReplies = new WeakMap(), appCleanups = new Set();
  const voiceCleanupReplies = new WeakMap(), voiceCleanups = new Set();
  let disposed = false, notificationError = null, lastNotifiedRevision = -1;
  const view = state => ({ ...publicLocalState(state), notificationError: notificationError ? { code: notificationError.code, message: notificationError.message } : null });
  const serialize = task => {
    if (disposed) return Promise.reject(new ConnectError('CLIENT_DISPOSED', 'This browser client has been disposed.'));
    const result = queue.then(task, task);
    queue = result.then(() => undefined, () => undefined);
    return result;
  };
  const channelName = 'connect-browser:v1:' + (normalized.dbName || projectId) + ':' + endpoint;
  const channelFactory = normalized.createChannel || (globalThis.window && typeof globalThis.BroadcastChannel === 'function' ? name => new BroadcastChannel(name) : null);
  let channel = null;
  function recordNotificationError(error) {
    notificationError = new ConnectError(error instanceof ConnectError ? error.code : 'NOTIFICATION_FAILED', 'A browser state notification could not be applied. Read the current profile again.');
    if (onError) {
      try { Promise.resolve(onError(notificationError)).catch(() => { notificationError = new ConnectError('ERROR_OBSERVER_FAILED', 'The state error observer failed. Read the current profile again.'); }); }
      catch { notificationError = new ConnectError('ERROR_OBSERVER_FAILED', 'The state error observer failed. Read the current profile again.'); }
    }
  }
  function dispatchState(state) {
    if (!onState || disposed) return;
    // Observers may call this client's serialized methods. Never await their
    // promises inside the same queue, but always observe their failures.
    try { Promise.resolve(onState(view(state))).catch(recordNotificationError); }
    catch (error) { recordNotificationError(error); }
  }
  if (channelFactory) {
    try {
      channel = channelFactory(channelName);
      channel.onmessage = event => {
        if (disposed || event.data?.schema !== 'connect.local-change.v1' || event.data.projectId !== projectId ||
            !Number.isSafeInteger(event.data.localRevision) || event.data.localRevision <= lastNotifiedRevision) return;
        serialize(async () => {
          const state = await read();
          if (!state || state.localRevision <= lastNotifiedRevision) return;
          lastNotifiedRevision = state.localRevision;
          dispatchState(state);
        }).catch(recordNotificationError);
      };
      channel.onmessageerror = () => recordNotificationError(new ConnectError('NOTIFICATION_FAILED', 'The change notification was unreadable.'));
    } catch (error) { recordNotificationError(error); }
  }
  async function read() {
    const value = await storage.read();
    if (value === null) return null;
    return validateState(value, scope);
  }
  async function ensureState() { return await read() || validateState(await storage.claim(emptyState(projectId, endpoint)), scope); }
  async function mutate(update, options) {
    for (let attempt = 0; attempt < 5; attempt++) {
      const previous = await ensureState();
      const candidate = update(structuredClone(previous));
      if (!candidate) return previous;
      candidate.localRevision = previous.localRevision + 1;
      validateState(candidate, scope);
      try { return await storage.compareAndSwap(previous.localRevision, candidate, options); }
      catch (error) { if (error.code !== 'LOCAL_CONFLICT' || attempt === 4) throw error; }
    }
    throw new ConnectError('LOCAL_CONFLICT', 'Another tab kept changing this profile. Retry after it finishes.');
  }
  async function notify() {
    const state = await read();
    if (state) {
      lastNotifiedRevision = state.localRevision;
      try { channel?.postMessage({ schema: 'connect.local-change.v1', projectId, localRevision: state.localRevision }); }
      catch (error) { recordNotificationError(error); }
    }
    dispatchState(state);
  }
  async function assertCurrent(actor) {
    if ((await read())?.activeDeviceId !== actor.deviceId) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The active profile changed while this request was running. Its result was not applied to the new profile.');
  }
  function assertExpectedAccount(actor, expectedAccountId) {
    if (expectedAccountId !== undefined && actor.accountId !== requiredString(expectedAccountId, 'Expected account ID')) {
      throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The displayed profile changed before this action. Review the current profile and try again.');
    }
  }
  async function installation(deviceId, { requireAccount = true, allowRevoked = false } = {}) {
    const state = await read();
    const item = state?.installations.find(value => value.deviceId === (deviceId || state.activeDeviceId));
    if (!item) throw new ConnectError('NO_LOCAL_PROFILE', 'No local profile is open. Start or connect a profile first.');
    if (!allowRevoked && item.revoked) throw new ConnectError('DEVICE_REVOKED', 'This device was disconnected. Use another saved sign-in method.');
    if (requireAccount && !item.accountId) throw new ConnectError('BOOTSTRAP_REQUIRED', 'This local profile has not finished connecting to the server.');
    try {
      await verifyInstallation(item);
      if (item.rootEnvelope) { const root = await openLocalRoot(item, projectId); root.fill(0); }
    }
    catch (error) { throw error instanceof ConnectError ? error : new ConnectError('CORRUPT_LOCAL_STATE', 'The saved device keys could not be verified. Existing data was not replaced.', { cause: error }); }
    return item;
  }
  async function post(body) {
    let response;
    // Only fixed signed work ports receive longer bounded deadlines.
    // Challenges, cancellation and all other RPCs retain the normal bound.
    const timeoutMs = !body.proof ? 20_000 : body.op === 'agent.turn' || body.op === 'voice.orb.turn' ? 120_000
      : body.op === 'voice.orb.start' ? 90_000 : body.op === 'voice.orb.end' ? 45_000 : 20_000;
    const controller = new AbortController(), timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      response = await fetcher(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: canonicalJson({ protocol: 1, ...body }),
        credentials: 'same-origin', cache: 'no-store', redirect: 'error', referrerPolicy: 'no-referrer', signal: controller.signal });
      const text = await response.text();
      const keyList = Boolean(body.proof && body.op === 'ai.keys.list'), maximum = (keyList ? 8 : 3) * 1024 * 1024;
      if (text.length > maximum || keyList && new TextEncoder().encode(text).byteLength > maximum)
        throw new ConnectError('INVALID_SERVER_RESPONSE', 'The server response was too large.');
      let value;
      try { value = JSON.parse(text); } catch { throw new ConnectError('INVALID_SERVER_RESPONSE', 'The server did not return JSON.'); }
      if (!response.ok || value?.ok !== true) {
        const code = typeof value?.error?.code === 'string' && /^[A-Z0-9_-]{1,80}$/i.test(value.error.code) ? value.error.code : 'SERVER_ERROR';
        const message = typeof value?.error?.message === 'string' ? value.error.message.slice(0, 500) : 'The server could not complete this request.';
        throw new ConnectError(code, message, { status: response.status });
      }
      return value;
    } catch (error) {
      if (error instanceof ConnectError) throw error;
      throw new ConnectError(error.name === 'AbortError' ? 'NETWORK_TIMEOUT' : 'NETWORK_ERROR', 'The request could not be confirmed. Existing local data was kept; retry the same action.', { cause: error });
    } finally { clearTimeout(timer); }
  }
  const cleanupCredential = actor => canonicalJson({accountId:actor.accountId,deviceId:actor.deviceId,createdAt:actor.createdAt,
    signingPublicJwk:actor.signingPublicJwk,encryptionPublicJwk:actor.encryptionPublicJwk});
  function releaseCleanup(state) {
    clearTimeout(state.timer);state.timer=null;state.actor=null;state.finished=true;appCleanups.delete(state);
  }
  async function performCleanup(state) {
    if(state.finished)return;
    if(disposed || Date.now()>=state.expiresAt){releaseCleanup(state);return;}
    if(state.pending)return state.pending;
    const action=(async()=>{
      const actor=state.actor, current=await installation(actor.deviceId);
      if(cleanupCredential(current)!==state.credential)throw new ConnectError('CLEANUP_CREDENTIAL_CHANGED','The original installation changed.');
      // Only this fixed close is permitted with a retained signer. It grants no
      // access, renewal, application mutation, arbitrary operation or target.
      await rpc('apps.scoped.close',state.args,actor,false);
      releaseCleanup(state);
    })();
    state.pending=action;
    try {await action;}catch(error){state.pending=null;throw error;}
  }
  function captureAppCleanup(result, capture, args) {
    if(!capture || !['soty.selected-human-embed.v1','soty.selected-human-embed.v2'].includes(result.runtimeProfile))return null;
    const appId=result.entry?.appId,handle=result.scopedCloseHandle,expiresAt=result.scopedSlotExpiresAt;
    if(appId!==args.appId || typeof appId!=='string' || !/^app-[a-f0-9]{32}$/.test(appId)
      ||typeof handle!=='string'||!/^[A-Za-z0-9_-]{43}$/.test(handle)||!Number.isSafeInteger(expiresAt)
      ||expiresAt<=Date.now()||expiresAt>Date.now()+310000)
      throw new ConnectError('INVALID_SERVER_RESPONSE','Invalid closed application cleanup reply.');
    for(const value of appCleanups)if(value.expiresAt<=Date.now())releaseCleanup(value);
    if(appCleanups.size>=256)throw new ConnectError('CLEANUP_CAPACITY','Too many active application admissions.');
    const deadline=Math.min(expiresAt,capture.startedAt+300000);
    const state={actor:capture.actor,credential:capture.credential,args:Object.freeze({appId,handle}),expiresAt:deadline,finished:false,pending:null,timer:null};
    const callback=(...parameters)=>{
      if(parameters.length)return Promise.reject(new ConnectError('INVALID_ARGUMENT','Cleanup takes no arguments.'));
      return serialize(()=>performCleanup(state));
    };
    state.timer=setTimeout(()=>releaseCleanup(state),Math.max(1,deadline-Date.now()));state.timer.unref?.();
    appCleanups.add(state);appCleanupReplies.set(result,Object.freeze({callback,state}));return state;
  }
  function releaseVoiceCleanup(state) {
    clearTimeout(state.timer); state.timer = null; state.actor = null; state.finished = true; voiceCleanups.delete(state);
  }
  async function performVoiceCleanup(state) {
    if (state.finished) return state.reply;
    if (Date.now() >= state.expiresAt) { releaseVoiceCleanup(state); throw new ConnectError('CLEANUP_EXPIRED', 'The owned voice cleanup expired.'); }
    if (state.pending) return state.pending;
    const action = (async () => {
      const actor = state.actor, current = await installation(actor.deviceId);
      if (cleanupCredential(current) !== state.credential) throw new ConnectError('CLEANUP_CREDENTIAL_CHANGED', 'The original installation changed.');
      // This retained signer can ONLY end this one original voice session.
      // The Root authority/revocation fence still verifies the original actor.
      // No caller-selected operation, target, new task or profile is accepted.
      const reply = await rpc('voice.orb.end', state.args, actor, false);
      state.reply = reply; releaseVoiceCleanup(state); return reply;
    })();
    state.pending = action;
    try { return await action; } catch (error) { state.pending = null; throw error; }
  }
  async function captureVoiceCleanup(result, capture) {
    if (!capture) return null;
    if (result.schema !== 'soty.voice-orb-started.v1' || typeof result.sessionId !== 'string'
      || !/^[A-Za-z0-9_.:-]{1,160}$/u.test(result.sessionId)) throw new ConnectError('INVALID_SERVER_RESPONSE', 'Invalid owned voice cleanup reply.');
    const duration = Number.isSafeInteger(result.remainingDurationMs) && result.remainingDurationMs > 0 && result.remainingDurationMs <= 900000
      ? result.remainingDurationMs : 30000;
    const state = { actor: capture.actor, credential: capture.credential, args: Object.freeze({ sessionId: result.sessionId }),
      expiresAt: Date.now() + duration + 90000, finished: false, pending: null, reply: undefined, timer: null };
    for (const old of voiceCleanups) if (Date.now() >= old.expiresAt) releaseVoiceCleanup(old);
    if (voiceCleanups.size >= 256) { await performVoiceCleanup(state); throw new ConnectError('CLEANUP_CAPACITY', 'Too many owned voice sessions.'); }
    const callback = (...parameters) => parameters.length
      ? Promise.reject(new ConnectError('INVALID_ARGUMENT', 'Voice cleanup takes no arguments.')) : performVoiceCleanup(state);
    state.timer = setTimeout(() => releaseVoiceCleanup(state), duration + 90000); state.timer.unref?.();
    voiceCleanups.add(state); voiceCleanupReplies.set(result, Object.freeze({ callback, state })); return state;
  }
  async function rpc(op, args, identity, requireCurrent = !identity, checkCurrent = assertCurrent) {
    if (op === 'card.resolve') return post({ op, args });
    const actor = identity || await installation();
    if (requireCurrent) await checkCurrent(actor);
    // Capture the real authenticated local signer before any challenge/network
    // await. The caller receives no signer object or generic retained RPC port.
    const cleanupCapture=normalized.scopedAppCleanup===true && ['apps.launch','apps.scoped.renew'].includes(op)
      ?Object.freeze({actor,credential:cleanupCredential(actor),startedAt:Date.now()}):null;
    const voiceCapture=normalized.scopedVoiceCleanup===true && op==='voice.orb.start'
      ?Object.freeze({actor,credential:cleanupCredential(actor)}):null;
    const digest = await sha256(canonicalJson(args));
    const challenge = await post({ op: 'challenge', args: { operation: op, digest } });
    responseId(challenge.challengeId, 'challenge ID');
    if (typeof challenge.message !== 'string' || challenge.message.length > 8192 || expires(challenge.expiresAt) <= Date.now()) {
      throw new ConnectError('INVALID_CHALLENGE', 'The server challenge is invalid or expired.');
    }
    // The structured challenge is checked before signing the server's exact UTF-8 bytes.
    let message;
    try { message = JSON.parse(challenge.message); } catch { throw new ConnectError('INVALID_CHALLENGE', 'The server challenge is not structured JSON.'); }
    const expectedFields = ['schema', 'projectId', 'origin', 'operation', 'digest', 'challengeId', 'expiresAt'];
    if (!message || typeof message !== 'object' || Object.keys(message).length !== expectedFields.length || Object.keys(message).some(key => !expectedFields.includes(key)) ||
        message.schema !== 'connect.proof.v1' || message.projectId !== projectId || message.operation !== op || message.digest !== digest || message.challengeId !== challenge.challengeId ||
        message.expiresAt !== challenge.expiresAt || typeof message.origin !== 'string' ||
        globalThis.location?.origin && message.origin !== globalThis.location.origin) {
      throw new ConnectError('INVALID_CHALLENGE', 'The challenge is bound to another request, project or browser.');
    }
    if (requireCurrent) await checkCurrent(actor);
    const proof = { challengeId: challenge.challengeId, publicJwk: actor.signingPublicJwk, signature: await signMessage(actor, challenge.message) };
    const result = await post({ op, args, proof });
    const cleanup=captureAppCleanup(result,cleanupCapture,args);
    const voiceCleanup=await captureVoiceCleanup(result,voiceCapture);
    if (requireCurrent && op !== 'bootstrap') {
      try {await checkCurrent(actor);} catch(error) {
        if(cleanup){try{await performCleanup(cleanup);}catch{/* Original failure wins; no rights or automatic effect retry. */}}
        if(voiceCleanup){try{await performVoiceCleanup(voiceCleanup);}catch{/* Fixed owned end only; original fence error wins. */}}
        throw error;
      }
    }
    return result;
  }
  function checkAccountResult(result, actor, expectedAccountId = actor.accountId) {
    responseId(result.accountId, 'account ID');
    if (result.deviceId !== actor.deviceId || expectedAccountId !== null && expectedAccountId !== undefined && result.accountId !== expectedAccountId) {
      throw new ConnectError('ACCOUNT_MISMATCH', 'The server returned a different account or device. Existing data was not replaced.');
    }
  }
  async function withRoot(actor, callback) {
    const root = await openLocalRoot(actor, projectId);
    try { return await callback(root); } finally { root.fill(0); }
  }
  function approvalRecords(state) {
    const records = state?.extensions?.['connect.browser.approvals.v1'] ?? [];
    if (!Array.isArray(records) || records.some(item => !item || typeof item.requestId !== 'string' || typeof item.accountId !== 'string' ||
        typeof item.deviceId !== 'string' || typeof item.recipient !== 'string' || !item.wrappedKey || !Number.isFinite(item.expiresAt))) {
      throw new ConnectError('CORRUPT_LOCAL_STATE', 'The saved enrollment approvals are invalid. Existing data was not replaced.');
    }
    return records;
  }
  async function commitAccount(actor, accountId, rootEnvelope, complete, options) {
    const state = await mutate(draft => {
      const stored = draft.installations.find(item => item.deviceId === actor.deviceId);
      if (!stored || stored.accountId !== null && stored.accountId !== accountId) throw new ConnectError('ACCOUNT_MISMATCH', 'An existing device cannot change account ownership.');
      stored.accountId = accountId; stored.rootEnvelope = rootEnvelope;
      if (complete) complete(draft, stored);
      return draft;
    }, options);
    await notify();
    return state;
  }

  function validateKit(kit) {
    const fields = ['schema', 'projectId', 'accountId', 'recoveryId', 'secret', 'wrappedKey', 'createdAt'];
    if (!kit || kit.schema !== 'connect.recovery-kit.v1' || kit.projectId !== projectId || Object.keys(kit).some(key => !fields.includes(key))) {
      throw new ConnectError('INVALID_RECOVERY_KIT', 'This recovery kit belongs to another project or format.');
    }
    requiredString(kit.accountId, 'Recovery account'); requiredString(kit.recoveryId, 'Recovery ID');
    if (!kit.wrappedKey || kit.wrappedKey.schema !== 'connect.recovery-key.v1') throw new ConnectError('INVALID_RECOVERY_KIT', 'The recovery kit is missing its encrypted account key.');
    return unbase64url(kit.secret, 32);
  }

  const credentialLedgerKey = 'connect.browser.credentials.v1';
  function credentialLedger(state) {
    const ledger = state?.extensions?.[credentialLedgerKey] ?? { actions: [], logins: [], receipts: [] };
    if (!ledger || !['actions', 'logins', 'receipts'].every(key => Array.isArray(ledger[key]))
        || ledger.actions.length > 128 || ledger.logins.length > 64 || ledger.receipts.length > 64
        || ledger.actions.some(item => !item || !['set', 'remove'].includes(item.operation) || !item.args?.requestId || !item.accountId)
        || ledger.logins.some(item => !item?.record?.credentialId || !item.deviceId || !item.lookup || !item.label)
        || ledger.receipts.some(item => !item?.credentialId || !item.deviceId || !item.accountId || !item.lookup)) {
      throw new ConnectError('CORRUPT_LOCAL_STATE', 'The saved sign-in operations are invalid. Existing profiles were kept.');
    }
    return ledger;
  }
  function editCredentialLedger(draft, edit) {
    const ledger = credentialLedger(draft); edit(ledger);
    draft.extensions = { ...(draft.extensions ?? {}), [credentialLedgerKey]: ledger }; return draft;
  }
  async function assertSnapshot(snapshot) {
    const current = await read();
    if (current?.activeDeviceId !== snapshot.activeDeviceId || current?.localRevision !== snapshot.localRevision) {
      throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The selected profile changed during sign-in. Review it and try again.');
    }
  }
  function sameSnapshot(draft, snapshot) {
    if (draft.activeDeviceId !== snapshot.activeDeviceId || draft.localRevision !== snapshot.localRevision) {
      throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The selected profile changed during sign-in. Review it and try again.');
    }
  }
  function checkedCredentialStatus(value, accountId) {
    if (value?.schema !== 'connect.credentials.v1' || value.accountId !== accountId
        || !['password', 'phrase'].every(kind => typeof value[kind]?.enabled === 'boolean'
          && Number.isSafeInteger(value[kind].revision) && value[kind].revision >= 0)) {
      throw new ConnectError('INVALID_SERVER_RESPONSE', 'The sign-in settings could not be confirmed.');
    }
    return value;
  }
  function credentialInputOperation(kind, input, creating, action) {
    try { const captured = normalizeCredentialInput(kind, input, { creating }); return serialize(() => action(captured)); }
    catch (error) { return Promise.reject(error); }
  }
  async function clearCredentialAction(pending, state) {
    await mutate(draft => { sameSnapshot(draft, state); return editCredentialLedger(draft, ledger => {
      ledger.actions = ledger.actions.filter(item => item.args.requestId !== pending.args.requestId);
    }); }); await notify();
  }

  const client = {
    /** Private host effect cleanup. JSON clones cannot obtain a capability. */
    appSlotCleanup: reply => !disposed && reply && typeof reply==='object' ? appCleanupReplies.get(reply)?.callback ?? null : null,
    voiceSessionCleanup: reply => !disposed && reply && typeof reply==='object' ? voiceCleanupReplies.get(reply)?.callback ?? null : null,
    getLocalState: () => serialize(async () => { const state = await read(); if (publicLocalState(state).accountId) platformInvitation?.clear(); return view(state); }),
    extension: (operation, args = {}, context) => {
      if (typeof operation !== 'string' || !/^[a-z][a-z0-9]*(?:\.[a-z][a-zA-Z0-9]*){1,4}$/u.test(operation)
        || operation.length > 100) return Promise.reject(new ConnectError('INVALID_ARGUMENT', 'Invalid extension operation.'));
      // Capture JSON before queueing so the caller cannot change an in-flight payload.
      let payload;
      try { payload = JSON.parse(canonicalJson(args)); }
      catch { return Promise.reject(new ConnectError('INVALID_ARGUMENT', 'Extension arguments must be JSON.')); }
      let expectedAccountId;
      if (context !== undefined) {
        try {
          if (!context || typeof context !== 'object' || Array.isArray(context) || Object.keys(context).some(key => key !== 'expectedAccountId')) {
            throw new ConnectError('INVALID_ARGUMENT', 'Invalid extension account context.');
          }
          expectedAccountId = requiredString(context.expectedAccountId, 'Expected account ID');
        } catch (error) { return Promise.reject(error); }
      }
      if (detachedVoiceOperations.has(operation) && expectedAccountId === undefined) {
        return Promise.reject(new ConnectError('INVALID_ARGUMENT', 'Voice controls require the displayed account context.'));
      }
      if (expectedAccountId !== undefined && (parallelMetadataReads.has(operation) || detachedVoiceOperations.has(operation))) {
        // Bind one local generation, then let these fixed signed operations leave the state queue.
        return serialize(async () => {
          if (disposed) throw new ConnectError('CLIENT_DISPOSED', 'This browser client has been disposed.');
          const localRevision = (await read())?.localRevision;
          const actor = await installation(); assertExpectedAccount(actor, expectedAccountId);
          return { actor, localRevision };
        }).then(async ({ actor, localRevision }) => {
          const checkReadCurrent = async () => {
            if (disposed) throw new ConnectError('CLIENT_DISPOSED', 'This browser client has been disposed.');
            const state = await read();
            if (disposed) throw new ConnectError('CLIENT_DISPOSED', 'This browser client has been disposed.');
            if (state?.activeDeviceId !== actor.deviceId || state.localRevision !== localRevision) {
              throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The local profile changed while this read was running. Its result was not applied.');
            }
          };
          return rpc(operation, payload, actor, true, checkReadCurrent);
        });
      }
      if (operation === 'agent.turn' || operation === 'agent.cancel' || operation === 'voice.agent.turn' || operation === 'voice.agent.cancel') {
        // Capture the displayed account and actual private installation in the normal state queue.
        // Await only these long turn/control RPCs outside it: getLocalState/cancel must remain usable.
        // No signer escapes this closure; rpc still checks current state before challenge/sign and after response.
        return serialize(async () => {
          if (disposed) throw new ConnectError('CLIENT_DISPOSED', 'This browser client has been disposed.');
          const actor = await installation(); assertExpectedAccount(actor, expectedAccountId);
          return { actor };
        }).then(async ({ actor }) => {
          if (disposed) throw new ConnectError('CLIENT_DISPOSED', 'This browser client has been disposed.');
          const result = await rpc(operation, payload, actor, true);
          if (disposed) throw new ConnectError('CLIENT_DISPOSED', 'This browser client has been disposed.');
          return result;
        });
      }
      return serialize(async () => {
        if (expectedAccountId === undefined) return rpc(operation, payload);
        // Admission is tied to the displayed account, not whichever profile an
        // earlier queued request or another tab happens to leave selected.
        const actor = await installation();
        assertExpectedAccount(actor, expectedAccountId);
        return rpc(operation, payload, actor, true);
      });
    },
    dispose: () => { disposed = true; channel?.close(); channel = null;for(const state of appCleanups)releaseCleanup(state);
      for(const state of voiceCleanups)void performVoiceCleanup(state).catch(()=>releaseVoiceCleanup(state)); },
    bootstrap: (label, { invitationToken } = {}) => serialize(async () => {
      label = deviceLabel(label);
      let state = await read();
      if (!state?.activeDeviceId) {
        const candidate = await createInstallation(label), raw = cryptoApi().getRandomValues(new Uint8Array(32));
        try { candidate.rootEnvelope = await sealLocalRoot(candidate, projectId, raw); } finally { raw.fill(0); }
        if (!state) {
          const initial = emptyState(projectId, endpoint); initial.installations.push(candidate); initial.activeDeviceId = candidate.deviceId;
          state = validateState(await storage.claim(initial), scope);
        }
        if (!state.activeDeviceId) state = await mutate(draft => {
          if (draft.activeDeviceId) return null;
          draft.installations.push(candidate); draft.activeDeviceId = candidate.deviceId; return draft;
        });
      }
      const actor = await installation(state.activeDeviceId, { requireAccount: false });
      // A readable, already committed root must exist before a server account can be created.
      await withRoot(actor, async () => undefined);
      const token = actor.accountId ? null : invitationToken ?? platformInvitation?.get();
      const result = await rpc('bootstrap', { label: actor.label, encryptionPublicJwk: actor.encryptionPublicJwk,
        ...(token !== null && token !== undefined ? { invitationToken: token } : {}) }, actor, true);
      checkAccountResult(result, actor);
      await commitAccount(actor, result.accountId, actor.rootEnvelope);
      platformInvitation?.clear();
      await assertCurrent(actor);
      return result;
    }),
    status: () => serialize(async () => {
      const actor = await installation(), result = await rpc('status', {}, actor, true);
      checkAccountResult(result, actor); return result;
    }),
    credentialStatus: expectedAccountId => serialize(async () => {
      const state = await read(), actor = await installation();
      assertExpectedAccount(actor, requiredString(expectedAccountId, 'Expected account ID'));
      return checkedCredentialStatus(await rpc('credentials.status', {}, actor, true, () => assertSnapshot(state)), actor.accountId);
    }),
    setAccountCredential: (kind, input, expectedAccountId) => credentialInputOperation(kind, input, true, async captured => {
      let state = await read(); const actor = await installation();
      assertExpectedAccount(actor, requiredString(expectedAccountId, 'Expected account ID'));
      let pending = credentialLedger(state).actions.find(item => item.accountId === actor.accountId && item.kind === kind);
      if (pending) {
        if (pending.operation !== 'set') throw new ConnectError('CREDENTIAL_PENDING', 'Finish the saved sign-in change first.');
        const opened = await unlockCredential(pending.record, captured, { projectId });
        try { await withRoot(actor, raw => { if (!raw.every((byte, index) => byte === opened.raw[index])) throw new ConnectError('credential_unavailable', 'The saved sign-in change belongs to a different key.'); }); }
        finally { opened.raw.fill(0); }
        await assertSnapshot(state);
      } else {
        const current = checkedCredentialStatus(await rpc('credentials.status', {}, actor, true, () => assertSnapshot(state)), actor.accountId);
        const record = await withRoot(actor, raw => makeCredential(raw, captured, { projectId, accountId: actor.accountId }));
        await assertSnapshot(state);
        pending = { accountId: actor.accountId, kind, operation: 'set', record,
          args: { kind, requestId: 'credential_' + base64url(cryptoApi().getRandomValues(new Uint8Array(24))),
            expectedRevision: current[kind].revision, credentialId: 'credential_' + base64url(cryptoApi().getRandomValues(new Uint8Array(24))),
            lookup: record.lookup, login: record.login, salt: record.salt, iterations: record.iterations,
            verifier: record.verifier, wrappedKey: record.wrappedKey } };
        state = await mutate(draft => { sameSnapshot(draft, state); return editCredentialLedger(draft, ledger => { ledger.actions.push(pending); }); });
      }
      let result;
      try { result = checkedCredentialStatus(await rpc('credentials.set', pending.args, actor, true, () => assertSnapshot(state)), actor.accountId); }
      catch (error) {
        if (['credential_exists', 'invalid_login', 'revision_conflict', 'credential_unavailable', 'request_conflict'].includes(error.code)) await clearCredentialAction(pending, state);
        throw error;
      }
      if (!result[kind].enabled || result[kind].revision !== pending.args.expectedRevision + 1) throw new ConnectError('INVALID_SERVER_RESPONSE', 'The sign-in change was not confirmed.');
      await clearCredentialAction(pending, state); return result;
    }),
    removeAccountCredential: (kind, expectedAccountId) => serialize(async () => {
      if (!['password', 'phrase'].includes(kind)) throw new ConnectError('credential_unavailable', 'This sign-in method is unavailable.');
      let state = await read(); const actor = await installation();
      assertExpectedAccount(actor, requiredString(expectedAccountId, 'Expected account ID'));
      let pending = credentialLedger(state).actions.find(item => item.accountId === actor.accountId && item.kind === kind && item.operation === 'remove');
      if (!pending) {
        const current = checkedCredentialStatus(await rpc('credentials.status', {}, actor, true, () => assertSnapshot(state)), actor.accountId);
        pending = { accountId: actor.accountId, kind, operation: 'remove', args: { kind,
          requestId: 'credential_' + base64url(cryptoApi().getRandomValues(new Uint8Array(24))), expectedRevision: current[kind].revision } };
        state = await mutate(draft => { sameSnapshot(draft, state); return editCredentialLedger(draft, ledger => {
          ledger.actions = ledger.actions.filter(item => item.accountId !== actor.accountId || item.kind !== kind); ledger.actions.push(pending);
        }); });
      }
      let result;
      try { result = checkedCredentialStatus(await rpc('credentials.remove', pending.args, actor, true, () => assertSnapshot(state)), actor.accountId); }
      catch (error) { if (['revision_conflict', 'request_conflict'].includes(error.code)) await clearCredentialAction(pending, state); throw error; }
      if (result[kind].enabled || result[kind].revision !== pending.args.expectedRevision + 1) throw new ConnectError('INVALID_SERVER_RESPONSE', 'The sign-in removal was not confirmed.');
      await clearCredentialAction(pending, state); return result;
    }),
    loginWithCredential: (kind, input, label = 'Новое устройство', { signal } = {}) => credentialInputOperation(kind, input, false, async captured => {
      const checkOpen = () => { if (signal?.aborted) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'This sign-in screen was closed. Reopen it to continue the saved request.'); };
      const checkCurrent = async snapshot => { checkOpen(); await assertSnapshot(snapshot); checkOpen(); };
      checkOpen();
      label = deviceLabel(label); let state = await ensureState(), opened;
      try {
        const lookup = await credentialLookup(captured, projectId); await checkCurrent(state);
        let pending = credentialLedger(state).logins.findLast(item => item.kind === kind && item.lookup === lookup), record = pending?.record;
        if (record) { try { opened = await unlockCredential(record, captured, { projectId }); } catch (error) { if (error.code !== 'credential_unavailable') throw error; } }
        if (!opened) {
          const signer = state.activeDeviceId ? await installation(state.activeDeviceId, { requireAccount: false }) : await createInstallation(label);
          record = await rpc('credentials.lookup', { kind, lookup }, signer, true, () => checkCurrent(state));
          if (record?.kind !== kind || record?.lookup !== lookup || typeof record?.credentialId !== 'string') throw new ConnectError('credential_unavailable', 'This sign-in method could not be opened.');
          opened = await unlockCredential(record, captured, { projectId }); pending = undefined;
        }
        await checkCurrent(state);
        const completed = credentialLedger(state).receipts.find(item => item.credentialId === record.credentialId && item.lookup === lookup);
        if (completed) {
          const actor = await installation(completed.deviceId);
          await withRoot(actor, raw => { if (!raw.every((byte, index) => byte === opened.raw[index])) throw new ConnectError('credential_unavailable', 'The saved profile key does not match.'); });
          const result = await rpc('status', {}, actor, true, () => checkCurrent(state)); checkAccountResult(result, actor, completed.accountId);
          await mutate(draft => { checkOpen(); sameSnapshot(draft, state); if (draft.activeDeviceId === actor.deviceId) return null; draft.activeDeviceId = actor.deviceId; return draft; }, { beforeCommit: checkOpen, signal });
          await notify(); return { accountId: actor.accountId, deviceId: actor.deviceId, label: actor.label, alreadyCompleted: true };
        }
        if (!pending) {
          const candidate = await createInstallation(label); candidate.rootEnvelope = await sealLocalRoot(candidate, projectId, opened.raw);
          pending = { kind, lookup, record, deviceId: candidate.deviceId, accountId: record.accountId, label };
          state = await mutate(draft => { checkOpen(); sameSnapshot(draft, state); draft.installations.push(candidate);
            return editCredentialLedger(draft, ledger => { ledger.logins.push(pending); }); });
        }
        const actor = await installation(pending.deviceId, { requireAccount: false });
        const result = await rpc('credentials.login', { kind, lookup, credentialId: record.credentialId,
          authentication: opened.proof, encryptionPublicJwk: actor.encryptionPublicJwk, label: pending.label }, actor, true, () => checkCurrent(state));
        checkAccountResult(result, actor, record.accountId);
        if (result.credentialId !== record.credentialId || canonicalJson(result.wrappedKey) !== canonicalJson(record.wrappedKey)) throw new ConnectError('credential_unavailable', 'The sign-in key changed.');
        await commitAccount(actor, result.accountId, actor.rootEnvelope, draft => {
          checkOpen(); sameSnapshot(draft, state); draft.activeDeviceId = actor.deviceId;
          editCredentialLedger(draft, ledger => { ledger.logins = ledger.logins.filter(item => item.deviceId !== actor.deviceId);
            ledger.receipts.push({ kind, lookup, credentialId: record.credentialId, accountId: record.accountId, deviceId: actor.deviceId }); });
        }, { beforeCommit: checkOpen, signal });
        return { accountId: result.accountId, deviceId: result.deviceId, label: result.label };
      } finally { opened?.raw.fill(0); }
    }),
    rename: (label, expectedAccountId) => serialize(async () => {
      label = deviceLabel(label);
      const actor = await installation();
      assertExpectedAccount(actor, expectedAccountId);
      const result = await rpc('profile.rename', { label }, actor, true);
      await mutate(draft => { for (const item of draft.installations) if (item.accountId === actor.accountId) item.label = label; return draft; });
      await notify(); return result;
    }),
    card: () => serialize(() => rpc('card.get', {})),
    rotateCard: () => serialize(() => rpc('card.rotate', {})),
    resolveCard: cardId => serialize(() => rpc('card.resolve', { cardId: requiredString(cardId, 'Card ID') })),
    requestContact: cardId => serialize(() => rpc('contacts.request', { cardId: requiredString(cardId, 'Card ID') })),
    contacts: () => serialize(() => rpc('contacts.list', {})),
    acceptContact: requestId => serialize(() => rpc('contacts.accept', { requestId: requiredString(requestId, 'Request ID') })),
    declineContact: requestId => serialize(() => rpc('contacts.decline', { requestId: requiredString(requestId, 'Request ID') })),
    cancelContact: requestId => serialize(() => rpc('contacts.cancel', { requestId: requiredString(requestId, 'Request ID') })),
    removeContact: relationshipId => serialize(() => rpc('contacts.remove', { relationshipId: requiredString(relationshipId, 'Relationship ID') })),
    blockContact: peerAccountId => serialize(() => rpc('contacts.block', { peerAccountId: requiredString(peerAccountId, 'Peer account ID') })),
    unblockContact: peerAccountId => serialize(() => rpc('contacts.unblock', { peerAccountId: requiredString(peerAccountId, 'Peer account ID') })),
    sendContactInvite: (relationshipId, url, label) => serialize(() => rpc('contacts.sendInvite', {
      relationshipId: requiredString(relationshipId, 'Relationship ID'), url: requiredString(url, 'Invitation URL', 4096), label: deviceLabel(label),
    })),
    dismissContactInvite: invitationId => serialize(() => rpc('contacts.dismissInvite', { invitationId: requiredString(invitationId, 'Invitation ID') })),
    startEnrollment: label => serialize(async () => {
      label = deviceLabel(label);
      let state = await ensureState();
      if (state.pendingEnrollment && state.pendingEnrollment.label !== label) throw new ConnectError('ENROLLMENT_PENDING', 'Another enrollment is pending. Finish or explicitly discard it first.');
      if (!state.pendingEnrollment) {
        const candidate = await createInstallation(label);
        state = await mutate(draft => {
          if (draft.pendingEnrollment) return null;
          draft.installations.push(candidate);
          draft.pendingEnrollment = { deviceId: candidate.deviceId, label, requestId: null, expiresAt: null };
          return draft;
        });
      }
      const pending = state.pendingEnrollment;
      if (pending.label !== label) throw new ConnectError('ENROLLMENT_PENDING', 'Another tab started a different enrollment.');
      if (pending.requestId) {
        if (pending.expiresAt <= Date.now()) throw new ConnectError('ENROLLMENT_EXPIRED', 'This request expired. Check its confirmation for a completed retry, or explicitly start another request.');
        return { requestId: pending.requestId, expiresAt: pending.expiresAt };
      }
      const actor = await installation(pending.deviceId, { requireAccount: false });
      const result = await rpc('enrollment.start', { label: pending.label, encryptionPublicJwk: actor.encryptionPublicJwk }, actor);
      const requestId = responseId(result.requestId, 'enrollment ID'), expiresAt = expires(result.expiresAt);
      await mutate(draft => {
        if (draft.pendingEnrollment?.deviceId !== actor.deviceId) throw new ConnectError('LOCAL_CONFLICT', 'The pending enrollment changed.');
        if (draft.pendingEnrollment.requestId && draft.pendingEnrollment.requestId !== requestId) throw new ConnectError('ENROLLMENT_CONFLICT', 'A different enrollment was already saved for this device.');
        draft.pendingEnrollment = { ...draft.pendingEnrollment, requestId, expiresAt };
        return draft;
      });
      await notify(); return { ...result, expiresAt };
    }),
    inspectEnrollment: requestId => serialize(() => rpc('enrollment.inspect', { requestId: requiredString(requestId, 'Request ID') })),
    approveEnrollment: (requestId, expectedAccountId) => serialize(async () => {
      requestId = requiredString(requestId, 'Request ID');
      const actor = await installation();
      assertExpectedAccount(actor, expectedAccountId);
      const target = await rpc('enrollment.inspect', { requestId }, actor, true);
      if (target.requestId !== requestId || expires(target.expiresAt) <= Date.now()) throw new ConnectError('INVALID_ENROLLMENT', 'The enrollment request changed or expired.');
      publicJwk(target.encryptionPublicJwk);
      const recipient = canonicalJson(publicJwk(target.encryptionPublicJwk));
      let record = approvalRecords(await read()).find(item => item.requestId === requestId);
      if (!record) {
        const wrappedKey = await withRoot(actor, raw => wrapEnrollmentRoot(raw, { projectId, accountId: actor.accountId, requestId, encryptionPublicJwk: target.encryptionPublicJwk }));
        const candidate = { requestId, accountId: actor.accountId, deviceId: actor.deviceId, recipient, wrappedKey, expiresAt: expires(target.expiresAt) };
        const state = await mutate(draft => {
          if (draft.activeDeviceId !== actor.deviceId) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The active profile changed before approval.');
          const records = approvalRecords(draft);
          if (records.some(item => item.requestId === requestId)) return null;
          draft.extensions = { ...(draft.extensions || {}), 'connect.browser.approvals.v1': [...records.filter(item => item.expiresAt > Date.now()), candidate] };
          return draft;
        });
        record = approvalRecords(state).find(item => item.requestId === requestId);
      }
      if (record.accountId !== actor.accountId || record.deviceId !== actor.deviceId || record.recipient !== recipient) {
        throw new ConnectError('ENROLLMENT_CONFLICT', 'The saved approval belongs to another account or recipient.');
      }
      return rpc('enrollment.approve', { requestId, wrappedKey: record.wrappedKey }, actor, true);
    }),
    previewEnrollment: requestId => serialize(async () => {
      requestId = requiredString(requestId, 'Request ID');
      const state = await read();
      const completed = state?.installations.find(item => item.enrollmentRequestId === requestId && item.accountId);
      const deviceId = state?.pendingEnrollment?.requestId === requestId ? state.pendingEnrollment.deviceId : completed?.deviceId;
      if (!deviceId) throw new ConnectError('NO_PENDING_ENROLLMENT', 'This browser did not start this enrollment.');
      const actor = await installation(deviceId, { requireAccount: false });
      const result = await rpc('enrollment.preview', { requestId }, actor);
      if (result.requestId !== requestId || result.recipientDeviceId !== actor.deviceId || !['pending', 'approved', 'finished'].includes(result.status)) {
        throw new ConnectError('INVALID_ENROLLMENT', 'The enrollment preview belongs to another request or recipient.');
      }
      const preview = { requestId, recipientDeviceId: actor.deviceId, status: result.status, expiresAt: expires(result.expiresAt), account: null, source: null };
      if (result.status !== 'pending') {
        preview.account = { accountId: responseId(result.account?.accountId, 'preview account'), label: deviceLabel(result.account?.label) };
        preview.source = { deviceId: responseId(result.source?.deviceId, 'source device'), label: deviceLabel(result.source?.label) };
        if (completed && completed.accountId !== preview.account.accountId) throw new ConnectError('ACCOUNT_MISMATCH', 'The completed enrollment belongs to another account.');
      } else if (result.account !== null || result.source !== null) throw new ConnectError('INVALID_ENROLLMENT', 'An unconfirmed request cannot name an approving account.');
      const confirmed = await read();
      if (confirmed?.activeDeviceId !== state.activeDeviceId || confirmed?.localRevision !== state.localRevision) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The active profile changed while checking the confirmation. Review it again.');
      if (preview.account) enrollmentPreviews.set(requestId, { accountId: preview.account.accountId, activeDeviceId: state.activeDeviceId, localRevision: state.localRevision, deviceId: actor.deviceId });
      else enrollmentPreviews.delete(requestId);
      return preview;
    }),
    finishEnrollment: (requestId, expectedAccountId, { signal } = {}) => serialize(async () => {
      const checkOpen = () => { if (signal?.aborted) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The device connection was closed. Reopen it to continue the same request.'); };
      checkOpen();
      requestId = requiredString(requestId, 'Request ID');
      expectedAccountId = requiredString(expectedAccountId, 'Expected account ID');
      const state = await read();
      const checkScope = async () => { checkOpen(); await assertSnapshot(state); checkOpen(); };
      const preview = enrollmentPreviews.get(requestId);
      if (!preview || preview.accountId !== expectedAccountId) throw new ConnectError('ENROLLMENT_PREVIEW_REQUIRED', 'Review the approving account before opening it on this device.');
      const completed = state?.installations.find(item => item.enrollmentRequestId === requestId && item.accountId);
      if ((!completed || state.activeDeviceId !== completed.deviceId)
          && (state?.activeDeviceId !== preview.activeDeviceId || state?.localRevision !== preview.localRevision)) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The active profile changed after the confirmation preview. Review it again.');
      if (completed) {
        assertExpectedAccount(completed, expectedAccountId);
        const actor = await installation(completed.deviceId);
        const checked = await rpc('status', {}, actor, true, checkScope); checkAccountResult(checked, actor, expectedAccountId);
        checkOpen();
        if (state.activeDeviceId !== completed.deviceId) {
          await mutate(draft => {
            checkOpen();
            if (draft.activeDeviceId !== preview.activeDeviceId || draft.localRevision !== state.localRevision) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The profile changed before opening the approved account.');
            draft.activeDeviceId = completed.deviceId; return draft;
          }, { beforeCommit: checkOpen, signal });
          await notify();
        }
        const committed = await read();
        enrollmentPreviews.set(requestId, { ...preview, activeDeviceId: completed.deviceId, localRevision: committed.localRevision });
        return { accountId: completed.accountId, deviceId: completed.deviceId, label: completed.label, alreadyCompleted: true };
      }
      if (state?.pendingEnrollment?.requestId !== requestId) throw new ConnectError('NO_PENDING_ENROLLMENT', 'This browser did not start this enrollment.');
      const actor = await installation(state.pendingEnrollment.deviceId, { requireAccount: false });
      if (actor.deviceId !== preview.deviceId) throw new ConnectError('ENROLLMENT_PREVIEW_REQUIRED', 'Review this recipient before opening the approved account.');
      const result = await rpc('enrollment.finish', { requestId, expectedAccountId }, actor, true, checkScope);
      checkOpen();
      checkAccountResult(result, actor, expectedAccountId);
      const raw = await unwrapEnrollmentRoot(result.wrappedKey, actor, { projectId, accountId: result.accountId, requestId });
      let rootEnvelope;
      try { rootEnvelope = await sealLocalRoot(actor, projectId, raw); } finally { raw.fill(0); }
      await commitAccount(actor, result.accountId, rootEnvelope, (draft, stored) => {
        checkOpen();
        if (draft.pendingEnrollment?.deviceId !== actor.deviceId) throw new ConnectError('LOCAL_CONFLICT', 'The pending enrollment changed.');
        if (draft.activeDeviceId !== state.activeDeviceId || draft.localRevision !== state.localRevision) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The profile changed during enrollment. Retry to explicitly activate this approved profile.');
        stored.enrollmentRequestId = requestId;
        draft.activeDeviceId = actor.deviceId; draft.pendingEnrollment = null;
      }, { beforeCommit: checkOpen, signal });
      const committed = await read();
      enrollmentPreviews.set(requestId, { ...preview, activeDeviceId: actor.deviceId, localRevision: committed.localRevision });
      return { accountId: result.accountId, deviceId: result.deviceId, label: result.label };
    }),
    discardPendingEnrollment: () => serialize(async () => {
      // Keep the unregistered keys archived: abandoning a screen must never delete an existing profile.
      await mutate(draft => { if (draft.pendingEnrollment?.requestId) enrollmentPreviews.delete(draft.pendingEnrollment.requestId); draft.pendingEnrollment = null; return draft; }); await notify();
    }),
    switchProfile: accountId => serialize(async () => {
      accountId = requiredString(accountId, 'Account ID');
      const state = await read();
      const target = state?.installations.find(item => item.accountId === accountId && !item.revoked);
      if (!target) throw new ConnectError('PROFILE_NOT_SAVED', 'That profile is not saved in this browser.');
      await installation(target.deviceId); await withRoot(target, async () => undefined);
      await mutate(draft => { draft.activeDeviceId = target.deviceId; return draft; }); await notify();
      return view(await read());
    }),
    revokeDevice: deviceId => serialize(async () => {
      deviceId = requiredString(deviceId, 'Device ID');
      const actor = await installation(), result = await rpc('device.revoke', { deviceId }, actor, true);
      await mutate(draft => { const target = draft.installations.find(item => item.deviceId === deviceId && item.accountId === actor.accountId); if (!target) return null; target.revoked = true; return draft; });
      await notify(); return result;
    }),
    saveVault: (payload, expectedRevision, expectedAccountId) => serialize(async () => {
      const actor = await installation(), expected = revision(expectedRevision ?? actor.vaultRevision);
      assertExpectedAccount(actor, expectedAccountId);
      if (expected < actor.vaultRevision) throw new ConnectError('VAULT_ROLLBACK', 'The requested revision is older than the verified local revision.');
      const envelope = await withRoot(actor, raw => sealVault(raw, payload, { projectId, accountId: actor.accountId, revision: expected + 1 }));
      const result = await rpc('vault.put', { expectedRevision: expected, envelope }, actor, true);
      if (result.revision !== expected + 1) throw new ConnectError('INVALID_SERVER_RESPONSE', 'The server returned an unexpected backup revision.');
      await mutate(draft => { const stored = draft.installations.find(item => item.deviceId === actor.deviceId); stored.vaultRevision = Math.max(stored.vaultRevision, result.revision); return draft; });
      await notify(); await assertCurrent(actor); return { revision: result.revision };
    }),
    loadVault: expectedAccountId => serialize(async () => {
      const actor = await installation();
      assertExpectedAccount(actor, expectedAccountId);
      const result = await rpc('vault.get', {}, actor, true);
      const current = revision(result.revision);
      if (current < actor.vaultRevision) throw new ConnectError('VAULT_ROLLBACK', 'The server returned an older backup. Existing data was not replaced.');
      if (current === 0) {
        if (result.envelope !== null) throw new ConnectError('INVALID_SERVER_RESPONSE', 'An empty backup has an unexpected envelope.');
        return { revision: 0, payload: null };
      }
      const payload = await withRoot(actor, raw => openVault(raw, result.envelope, { projectId, accountId: actor.accountId, revision: current }));
      await assertCurrent(actor);
      await mutate(draft => { const stored = draft.installations.find(item => item.deviceId === actor.deviceId); stored.vaultRevision = Math.max(stored.vaultRevision, current); return draft; });
      await notify(); await assertCurrent(actor); return { revision: current, payload };
    }),
    prepareRecovery: () => serialize(async () => {
      const actor = await installation(), secret = cryptoApi().getRandomValues(new Uint8Array(32));
      try {
        const verifier = await sha256(base64url(secret));
        const wrappedKey = await withRoot(actor, raw => wrapRecoveryRoot(raw, secret, { projectId, accountId: actor.accountId }));
        const result = await rpc('recovery.set', { verifier, wrappedKey }, actor, true);
        const kit = { schema: 'connect.recovery-kit.v1', projectId, accountId: actor.accountId,
          recoveryId: responseId(result.recoveryId, 'recovery ID'), secret: base64url(secret), wrappedKey, createdAt: new Date().toISOString() };
        const kitFingerprint = await sha256(canonicalJson(kit));
        await mutate(draft => { draft.preparedRecovery = { accountId: actor.accountId, recoveryId: kit.recoveryId, kitFingerprint }; return draft; });
        await notify(); return kit;
      } finally { secret.fill(0); }
    }),
    confirmRecovery: kit => serialize(async () => {
      const secret = validateKit(kit);
      try {
        const actor = await installation();
        if (kit.accountId !== actor.accountId) throw new ConnectError('ACCOUNT_MISMATCH', 'This recovery kit is for another profile.');
        const raw = await unwrapRecoveryRoot(kit.wrappedKey, secret, { projectId, accountId: actor.accountId });
        try { await withRoot(actor, async expected => { if (!expected.every((byte, index) => byte === raw[index])) throw new ConnectError('RECOVERY_KEY_MISMATCH', 'This kit does not recover the current account key.'); }); }
        finally { raw.fill(0); }
        const result = await rpc('recovery.confirm', { recoveryId: kit.recoveryId, verifier: await sha256(kit.secret) }, actor, true);
        if (result.recoveryId !== kit.recoveryId || result.verified !== true) throw new ConnectError('INVALID_SERVER_RESPONSE', 'Recovery verification was not confirmed.');
        await mutate(draft => { if (draft.preparedRecovery?.recoveryId === kit.recoveryId) draft.preparedRecovery = null; return draft; });
        await notify(); return result;
      } finally { secret.fill(0); }
    }),
    recover: (kit, label, { signal } = {}) => serialize(async () => {
      const checkOpen = () => { if (signal?.aborted) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'This recovery screen was closed. Reopen it to continue the saved request.'); };
      checkOpen();
      label = deviceLabel(label);
      const secret = validateKit(kit);
      let raw;
      try {
        raw = await unwrapRecoveryRoot(kit.wrappedKey, secret, { projectId, accountId: kit.accountId });
        const kitFingerprint = await sha256(canonicalJson(kit));
        let state = await ensureState();
        const completed = state.installations.find(item => item.recoveryFingerprint === kitFingerprint && item.accountId === kit.accountId);
        if (completed) {
          const actor = await installation(completed.deviceId);
          await withRoot(actor, expected => {
            if (expected.length !== raw.length || !expected.every((byte, index) => byte === raw[index])) {
              throw new ConnectError('RECOVERY_KEY_MISMATCH', 'This kit does not recover the saved account key.');
            }
          });
          // A consumed kit may reopen its original recipient, never a revoked device.
          const result = await rpc('status', {}, actor, true, async () => { checkOpen(); await assertSnapshot(state); checkOpen(); });
          checkAccountResult(result, actor, kit.accountId);
          await mutate(draft => {
            checkOpen();
            if (draft.localRevision !== state.localRevision) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The saved profiles changed during recovery. Review them before opening the recovered profile.');
            const target = draft.installations.find(item => item.deviceId === actor.deviceId);
            if (target.revoked) throw new ConnectError('DEVICE_REVOKED', 'This recovered device was disconnected. Use another saved sign-in method.');
            if (draft.activeDeviceId === actor.deviceId) return null;
            draft.activeDeviceId = actor.deviceId; return draft;
          }, { beforeCommit: checkOpen, signal });
          await notify(); await assertCurrent(actor);
          return { accountId: actor.accountId, deviceId: actor.deviceId, label: actor.label, alreadyCompleted: true };
        }
        if (state.pendingRecovery && (state.pendingRecovery.kitFingerprint !== kitFingerprint || state.pendingRecovery.label !== label)) throw new ConnectError('RECOVERY_PENDING', 'Another recovery is pending. Retry it with the same kit and device name.');
        if (!state.pendingRecovery) {
          const candidate = await createInstallation(label);
          candidate.rootEnvelope = await sealLocalRoot(candidate, projectId, raw);
          state = await mutate(draft => {
            if (draft.pendingRecovery) return null;
            draft.installations.push(candidate);
            draft.pendingRecovery = { deviceId: candidate.deviceId, accountId: kit.accountId, recoveryId: kit.recoveryId, kitFingerprint, label };
            return draft;
          });
        }
        const pending = state.pendingRecovery;
        if (pending.kitFingerprint !== kitFingerprint || pending.label !== label) throw new ConnectError('RECOVERY_PENDING', 'Another tab started a different recovery.');
        const actor = await installation(pending.deviceId, { requireAccount: false });
        const result = await rpc('recovery.use', { accountId: kit.accountId, recoveryId: kit.recoveryId, secret: kit.secret, encryptionPublicJwk: actor.encryptionPublicJwk, label }, actor, true, async () => { checkOpen(); await assertSnapshot(state); checkOpen(); });
        checkAccountResult(result, actor, kit.accountId);
        if (canonicalJson(result.wrappedKey) !== canonicalJson(kit.wrappedKey)) throw new ConnectError('RECOVERY_KEY_MISMATCH', 'The server returned another encrypted recovery key.');
        await commitAccount(actor, result.accountId, actor.rootEnvelope, (draft, stored) => {
          checkOpen();
          if (draft.pendingRecovery?.deviceId !== actor.deviceId) throw new ConnectError('LOCAL_CONFLICT', 'The pending recovery changed.');
          if (draft.activeDeviceId !== state.activeDeviceId || draft.localRevision !== state.localRevision) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The profile changed during recovery. Retry to explicitly activate the recovered profile.');
          stored.recoveryFingerprint = kitFingerprint;
          draft.activeDeviceId = actor.deviceId; draft.pendingRecovery = null;
        }, { beforeCommit: checkOpen, signal });
        return { accountId: result.accountId, deviceId: result.deviceId, label: result.label, recoveryConsumed: true };
      } finally { secret.fill(0); raw?.fill(0); }
    }),
  };
  return Object.freeze(client);
}
