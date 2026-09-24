import {
  ConnectError, canonicalJson, base64url, unbase64url, sha256, cryptoApi, createInstallation,
  verifyInstallation, signMessage, publicJwk, sealLocalRoot, openLocalRoot,
  wrapEnrollmentRoot, unwrapEnrollmentRoot, wrapRecoveryRoot, unwrapRecoveryRoot, sealVault, openVault,
} from './crypto.mjs';
import { createIndexedDbStorage, emptyState, validateState } from './storage.mjs';

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
  return { ...options, projectId, endpoint: endpoint.href, fetcher };
}

function publicLocalState(state) {
  if (!state) return { schema: 'connect.local-view.v1', accountId: null, deviceId: null, label: null, current: null, profiles: [], pendingEnrollment: null, pendingRecovery: null, recoveryPrepared: false };
  const describe = item => ({ accountId: item.accountId, deviceId: item.deviceId, label: item.label, active: item.deviceId === state.activeDeviceId,
    revoked: item.revoked, vaultRevision: item.vaultRevision, createdAt: item.createdAt });
  const active = state.installations.find(item => item.deviceId === state.activeDeviceId);
  return {
    schema: 'connect.local-view.v1', accountId: active?.accountId ?? null, deviceId: active?.deviceId ?? null, label: active?.label ?? null, current: active ? describe(active) : null,
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
  let queue = Promise.resolve();
  const enrollmentPreviews = new Map();
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
  async function mutate(update) {
    for (let attempt = 0; attempt < 5; attempt++) {
      const previous = await ensureState();
      const candidate = update(structuredClone(previous));
      if (!candidate) return previous;
      candidate.localRevision = previous.localRevision + 1;
      validateState(candidate, scope);
      try { return await storage.compareAndSwap(previous.localRevision, candidate); }
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
    const controller = new AbortController(), timer = setTimeout(() => controller.abort(), 20_000);
    try {
      response = await fetcher(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: canonicalJson({ protocol: 1, ...body }),
        credentials: 'same-origin', cache: 'no-store', redirect: 'error', referrerPolicy: 'no-referrer', signal: controller.signal });
      const text = await response.text();
      if (text.length > 3 * 1024 * 1024) throw new ConnectError('INVALID_SERVER_RESPONSE', 'The server response was too large.');
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
  async function rpc(op, args, identity, requireCurrent = !identity) {
    if (op === 'card.resolve') return post({ op, args });
    const actor = identity || await installation();
    if (requireCurrent) await assertCurrent(actor);
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
    if (requireCurrent) await assertCurrent(actor);
    const proof = { challengeId: challenge.challengeId, publicJwk: actor.signingPublicJwk, signature: await signMessage(actor, challenge.message) };
    const result = await post({ op, args, proof });
    if (requireCurrent && op !== 'bootstrap') await assertCurrent(actor);
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
  async function commitAccount(actor, accountId, rootEnvelope, complete) {
    const state = await mutate(draft => {
      const stored = draft.installations.find(item => item.deviceId === actor.deviceId);
      if (!stored || stored.accountId !== null && stored.accountId !== accountId) throw new ConnectError('ACCOUNT_MISMATCH', 'An existing device cannot change account ownership.');
      stored.accountId = accountId; stored.rootEnvelope = rootEnvelope;
      if (complete) complete(draft, stored);
      return draft;
    });
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

  const client = {
    getLocalState: () => serialize(async () => view(await read())),
    dispose: () => { disposed = true; channel?.close(); channel = null; },
    bootstrap: label => serialize(async () => {
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
      const result = await rpc('bootstrap', { label: actor.label, encryptionPublicJwk: actor.encryptionPublicJwk }, actor, true);
      checkAccountResult(result, actor);
      await commitAccount(actor, result.accountId, actor.rootEnvelope);
      await assertCurrent(actor);
      return result;
    }),
    status: () => serialize(async () => {
      const actor = await installation(), result = await rpc('status', {}, actor, true);
      checkAccountResult(result, actor); return result;
    }),
    rename: label => serialize(async () => {
      label = deviceLabel(label);
      const actor = await installation(), result = await rpc('profile.rename', { label }, actor, true);
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
      if ((await read())?.activeDeviceId !== state.activeDeviceId) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The active profile changed while checking the confirmation. Review it again.');
      if (preview.account) enrollmentPreviews.set(requestId, { accountId: preview.account.accountId, activeDeviceId: state.activeDeviceId, deviceId: actor.deviceId });
      else enrollmentPreviews.delete(requestId);
      return preview;
    }),
    finishEnrollment: (requestId, expectedAccountId) => serialize(async () => {
      requestId = requiredString(requestId, 'Request ID');
      expectedAccountId = requiredString(expectedAccountId, 'Expected account ID');
      const state = await read();
      const preview = enrollmentPreviews.get(requestId);
      if (!preview || preview.accountId !== expectedAccountId) throw new ConnectError('ENROLLMENT_PREVIEW_REQUIRED', 'Review the approving account before opening it on this device.');
      if (state?.activeDeviceId !== preview.activeDeviceId) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The active profile changed after the confirmation preview. Review it again.');
      const completed = state?.installations.find(item => item.enrollmentRequestId === requestId && item.accountId);
      if (completed) {
        assertExpectedAccount(completed, expectedAccountId);
        await installation(completed.deviceId);
        if (state.activeDeviceId !== completed.deviceId) {
          await mutate(draft => {
            if (draft.activeDeviceId !== preview.activeDeviceId) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The profile changed before opening the approved account.');
            draft.activeDeviceId = completed.deviceId; return draft;
          });
          await notify();
        }
        enrollmentPreviews.set(requestId, { ...preview, activeDeviceId: completed.deviceId });
        return { accountId: completed.accountId, deviceId: completed.deviceId, label: completed.label, alreadyCompleted: true };
      }
      if (state?.pendingEnrollment?.requestId !== requestId) throw new ConnectError('NO_PENDING_ENROLLMENT', 'This browser did not start this enrollment.');
      const actor = await installation(state.pendingEnrollment.deviceId, { requireAccount: false });
      if (actor.deviceId !== preview.deviceId) throw new ConnectError('ENROLLMENT_PREVIEW_REQUIRED', 'Review this recipient before opening the approved account.');
      const result = await rpc('enrollment.finish', { requestId, expectedAccountId }, actor);
      checkAccountResult(result, actor, expectedAccountId);
      const raw = await unwrapEnrollmentRoot(result.wrappedKey, actor, { projectId, accountId: result.accountId, requestId });
      let rootEnvelope;
      try { rootEnvelope = await sealLocalRoot(actor, projectId, raw); } finally { raw.fill(0); }
      await commitAccount(actor, result.accountId, rootEnvelope, (draft, stored) => {
        if (draft.pendingEnrollment?.deviceId !== actor.deviceId) throw new ConnectError('LOCAL_CONFLICT', 'The pending enrollment changed.');
        if (draft.activeDeviceId !== state.activeDeviceId) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The profile changed during enrollment. Retry to explicitly activate this approved profile.');
        stored.enrollmentRequestId = requestId;
        draft.activeDeviceId = actor.deviceId; draft.pendingEnrollment = null;
      });
      enrollmentPreviews.set(requestId, { ...preview, activeDeviceId: actor.deviceId });
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
    recover: (kit, label) => serialize(async () => {
      label = deviceLabel(label);
      const secret = validateKit(kit);
      let raw;
      try {
        raw = await unwrapRecoveryRoot(kit.wrappedKey, secret, { projectId, accountId: kit.accountId });
        const kitFingerprint = await sha256(canonicalJson(kit));
        let state = await ensureState();
        const completed = state.installations.find(item => item.recoveryFingerprint === kitFingerprint && item.accountId === kit.accountId);
        if (completed) return { accountId: completed.accountId, deviceId: completed.deviceId, label: completed.label, alreadyCompleted: true };
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
        const result = await rpc('recovery.use', { accountId: kit.accountId, recoveryId: kit.recoveryId, secret: kit.secret, encryptionPublicJwk: actor.encryptionPublicJwk, label }, actor);
        checkAccountResult(result, actor, kit.accountId);
        if (canonicalJson(result.wrappedKey) !== canonicalJson(kit.wrappedKey)) throw new ConnectError('RECOVERY_KEY_MISMATCH', 'The server returned another encrypted recovery key.');
        await commitAccount(actor, result.accountId, actor.rootEnvelope, (draft, stored) => {
          if (draft.pendingRecovery?.deviceId !== actor.deviceId) throw new ConnectError('LOCAL_CONFLICT', 'The pending recovery changed.');
          if (draft.activeDeviceId !== state.activeDeviceId) throw new ConnectError('ACTIVE_PROFILE_CHANGED', 'The profile changed during recovery. Retry to explicitly activate the recovered profile.');
          stored.recoveryFingerprint = kitFingerprint;
          draft.activeDeviceId = actor.deviceId; draft.pendingRecovery = null;
        });
        return { accountId: result.accountId, deviceId: result.deviceId, label: result.label, recoveryConsumed: true };
      } finally { secret.fill(0); raw?.fill(0); }
    }),
  };
  return Object.freeze(client);
}
