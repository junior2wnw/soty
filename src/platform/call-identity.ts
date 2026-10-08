import type { ConnectClient, LocalState } from '../../modules/connect/browser/index.mjs';

export interface CallIdentityLifecycle {
  setIdentity(value: { accountId: string; deviceId: string } | null): { ok: boolean; code?: string };
  end(): unknown;
  dispose(): unknown;
}
export interface CallIdentityPorts {
  lifecycle: CallIdentityLifecycle;
  client: Pick<ConnectClient, 'getLocalState'>;
  observeAccount(listener: (state: LocalState) => void): () => void;
  pageEvents: EventTarget;
}
type Identity = { accountId: string; deviceId: string } | null;
type Pending = { generation: number; identity: Identity; retries: number };
// Binding ownership only, not authority or a cross-instance resource quota.
const bound = new WeakSet<CallIdentityLifecycle>();
const viewKeys = ['schema', 'accountId', 'deviceId', 'label', 'current', 'profiles', 'pendingEnrollment', 'pendingRecovery', 'recoveryPrepared', 'notificationError'];
const profileKeys = ['accountId', 'deviceId', 'label', 'active', 'revoked', 'vaultRevision', 'createdAt'];

function fields(value: unknown, allowed: string[]): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  try {
    const keys = Reflect.ownKeys(value);
    if (keys.length > allowed.length || keys.some(key => typeof key !== 'string' || !allowed.includes(key))) return null;
    const copy: Record<string, unknown> = Object.create(null);
    for (const key of keys) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor || !Object.hasOwn(descriptor, 'value')) return null;
      copy[key as string] = descriptor.value;
    }
    return copy;
  } catch { return null; }
}
const id = (value: unknown): value is string => typeof value === 'string' && value.length > 0 && value.length <= 256;
function identity(value: unknown): Identity {
  const view = fields(value, viewKeys), current = fields(view?.current, profileKeys);
  if (!view || view.schema !== 'connect.local-view.v1' || view.notificationError !== null || !current
    || !id(view.accountId) || !id(view.deviceId) || current.accountId !== view.accountId || current.deviceId !== view.deviceId
    || current.active !== true || current.revoked !== false) return null;
  return Object.freeze({ accountId: view.accountId, deviceId: view.deviceId });
}

/** Bind one existing trusted owner to the ordered public SDK view; no call factory or proxy. */
export function bindCallIdentity({ lifecycle, client, observeAccount, pageEvents }: CallIdentityPorts): () => void {
  if (bound.has(lifecycle)) throw new TypeError('call_identity_already_bound');
  bound.add(lifecycle);
  let closed = false, suspended = false, generation = 0, applying = false, queued = false, hideDepth = 0, resumeQueued = false, pageEpoch = 0;
  let resumeTicket: { pageEpoch: number; event: Event } | null = null;
  let pending: Pending | null = null, applied: string | undefined, unobserve: (() => void) | undefined;
  const key = (value: Identity) => value ? JSON.stringify([value.accountId, value.deviceId]) : 'null';
  const safely = (callback: () => unknown) => { try { callback(); } catch {} };
  const dispose = () => {
    if (closed) return;
    closed = true; generation++; pageEpoch++; pending = null; resumeTicket = null;
    safely(() => unobserve?.()); unobserve = undefined;
    safely(() => pageEvents.removeEventListener('pagehide', hide));
    safely(() => pageEvents.removeEventListener('pageshow', show));
    safely(() => lifecycle.end());
    safely(() => lifecycle.setIdentity(null));
    safely(() => lifecycle.dispose());
  };
  const enqueue = () => {
    if (queued || closed) return;
    queued = true;
    queueMicrotask(() => { queued = false; flush(); });
  };
  const flush = () => {
    if (closed || applying || !pending) return;
    const item = pending;
    if (item.generation !== generation || (suspended && item.identity !== null)) return;
    if (applied === key(item.identity)) { pending = null; return; }
    applying = true;
    try {
      applied = undefined; // The setter can change the owner before a newer view reenters.
      const result = lifecycle.setIdentity(item.identity);
      if (closed || generation !== item.generation || pending !== item) return;
      if (result.ok === true) { applied = key(item.identity); pending = null; }
      else if (result.code === 'call_transition' && item.retries === 0) {
        item.retries++; lifecycle.end();
      } else dispose(); // A second failed attempt is terminal, never a retry loop.
    } catch { dispose(); }
    finally { applying = false; if (pending && !closed) enqueue(); }
  };
  const accept = (value: unknown) => {
    if (closed) return;
    const ticket = ++generation;
    const next = suspended ? null : identity(value);
    if (closed || ticket !== generation) return;
    pending = { generation: ticket, identity: next, retries: 0 };
    flush();
  };
  const refresh = () => {
    if (closed || suspended) return;
    const ticket = generation;
    let work: Promise<LocalState>;
    try {
      const getState = client.getLocalState;
      if (closed || suspended || ticket !== generation) return;
      work = Reflect.apply(getState, client, []);
    }
    catch { if (!closed && !suspended && ticket === generation) accept(null); return; }
    void Promise.resolve(work).then(value => {
      if (!closed && !suspended && ticket === generation) accept(value);
    }, () => { if (!closed && !suspended && ticket === generation) accept(null); });
  };
  const hide: EventListener = event => {
    if (closed) return;
    pageEpoch++;
    if (!(event as PageTransitionEvent).persisted) { dispose(); return; }
    suspended = true;
    generation++; pending = null;
    hideDepth++;
    try { lifecycle.end(); if (!closed) accept(null); }
    finally { hideDepth--; }
  };
  const show: EventListener = event => {
    if (closed || !suspended) return;
    // A trusted close callback can deliver resume before hide finishes clearing
    // identity. Keep one resume after that barrier, not a read invalidated by it.
    if (hideDepth > 0) {
      resumeTicket = { pageEpoch, event };
      if (!resumeQueued) {
        resumeQueued = true;
        queueMicrotask(() => {
          resumeQueued = false;
          const ticket = resumeTicket; resumeTicket = null;
          if (!closed && suspended && ticket?.pageEpoch === pageEpoch) show(ticket.event);
        });
      }
      return;
    }
    suspended = false; generation++; pending = null;
    refresh(); // Data revalidation only; never join or unmute.
  };
  try {
    accept(null); // Until the public view arrives, no old identity can be reused.
    if (closed) return dispose;
    pageEvents.addEventListener('pagehide', hide);
    if (closed) return dispose;
    pageEvents.addEventListener('pageshow', show);
    if (closed) return dispose;
    const remove = observeAccount(accept); // Subscribe before the first asynchronous read.
    if (closed) { safely(remove); return dispose; }
    unobserve = remove;
    if (!suspended) refresh();
    return dispose;
  } catch (error) { dispose(); throw error; }
}
