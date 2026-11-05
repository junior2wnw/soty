const PROTOCOL_VERSION = 1;
const DEFAULT_SPACE = "shared";
const DEFAULT_TTL_MS = 12_000;
const MAX_TTL_MS = 30_000;
const MAX_CLOCK_SKEW_MS = 5_000;
const DEFAULT_MIN_INTERVAL_MS = 2_500;
const DEFAULT_WINDOW_MS = 60_000;
const DEFAULT_WINDOW_LIMIT = 6;
const MAX_SEEN_EVENTS = 256;

const SPACE_PATTERN = /^[a-z0-9][a-z0-9._:-]{0,63}$/;
const EVENT_ID_PATTERN = /^[A-Za-z0-9_-]{12,32}$/;

const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

const finiteNumber = (value) => (
  typeof value === "number" && Number.isFinite(value) ? value : null
);

const normalizeSpace = (value = DEFAULT_SPACE) => {
  const space = String(value).toLowerCase();
  if (!SPACE_PATTERN.test(space)) {
    throw new TypeError(
      "Presence space must be 1-64 lowercase letters, digits, dots, colons, underscores or dashes.",
    );
  }
  return space;
};

const createEventId = (cryptoImpl = globalThis.crypto) => {
  const bytes = new Uint8Array(12);
  if (cryptoImpl?.getRandomValues) {
    cryptoImpl.getRandomValues(bytes);
  } else {
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Math.floor(Math.random() * 256);
    }
  }
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
};

/**
 * Returns the only payload shape accepted by the presence network.
 * Unknown input properties are deliberately discarded.
 */
export const normalizePresencePulse = (input, {
  now = Date.now(),
  ttlMs = DEFAULT_TTL_MS,
  allowExpired = false,
} = {}) => {
  if (!input || typeof input !== "object" || Array.isArray(input)) return null;
  if (input.protocol !== PROTOCOL_VERSION || input.kind !== "pulse") return null;

  let space;
  try {
    space = normalizeSpace(input.space);
  } catch {
    return null;
  }

  const eventId = String(input.eventId ?? "");
  const x = finiteNumber(input.x);
  const y = finiteNumber(input.y);
  const tone = finiteNumber(input.tone);
  const createdAt = finiteNumber(input.createdAt);
  const expiresAt = finiteNumber(input.expiresAt);

  if (!EVENT_ID_PATTERN.test(eventId) || x === null || y === null || tone === null) {
    return null;
  }
  if (x < 0 || x > 1 || y < 0 || y > 1 || tone < 0 || tone > 1) return null;
  if (createdAt === null || expiresAt === null) return null;
  if (createdAt > now + MAX_CLOCK_SKEW_MS) return null;

  const maximumLifetime = clamp(ttlMs, 250, MAX_TTL_MS);
  if (expiresAt <= createdAt || expiresAt - createdAt > maximumLifetime) return null;
  if (!allowExpired && expiresAt <= now) return null;

  return Object.freeze({
    protocol: PROTOCOL_VERSION,
    kind: "pulse",
    eventId,
    space,
    x,
    y,
    tone,
    createdAt,
    expiresAt,
  });
};

const createRateLimiter = ({
  now,
  minIntervalMs,
  windowMs,
  windowLimit,
}) => {
  let lastAcceptedAt = -Infinity;
  const accepted = [];

  return () => {
    const current = now();
    while (accepted.length && accepted[0] <= current - windowMs) accepted.shift();
    if (current - lastAcceptedAt < minIntervalMs || accepted.length >= windowLimit) {
      return false;
    }
    lastAcceptedAt = current;
    accepted.push(current);
    return true;
  };
};

const getLocalHub = () => {
  const key = Symbol.for("volumetric-glass.community-presence.local-hub.v1");
  if (!globalThis[key]) globalThis[key] = new Map();
  return globalThis[key];
};

/**
 * Cross-tab fallback. BroadcastChannel is preferred; localStorage is used only
 * when BroadcastChannel is missing. A tiny in-process hub also supports two
 * widgets on the same page and never leaves the JavaScript process.
 */
export const createLocalPresenceTransport = ({
  space = DEFAULT_SPACE,
  channelPrefix = "community-presence-v1",
  BroadcastChannelImpl = globalThis.BroadcastChannel,
  storage = globalThis.localStorage,
  eventTarget = globalThis,
} = {}) => {
  const normalizedSpace = normalizeSpace(space);
  const channelName = `${channelPrefix}:${normalizedSpace}`;
  const storageKey = `__${channelName}`;
  const listeners = new Set();
  const hub = getLocalHub();
  const hubListeners = hub.get(channelName) ?? new Set();
  hub.set(channelName, hubListeners);
  let closed = false;

  const deliver = (value) => {
    if (closed) return;
    for (const listener of listeners) listener(value);
  };

  const hubListener = (value, sender) => {
    if (sender !== transport) deliver(value);
  };
  hubListeners.add(hubListener);

  let channel = null;
  try {
    if (typeof BroadcastChannelImpl === "function") {
      channel = new BroadcastChannelImpl(channelName);
      channel.addEventListener?.("message", (event) => deliver(event.data));
      if (!("addEventListener" in channel)) channel.onmessage = (event) => deliver(event.data);
    }
  } catch {
    channel = null;
  }

  const onStorage = (event) => {
    if (event.key !== storageKey || !event.newValue) return;
    try {
      deliver(JSON.parse(event.newValue).pulse);
    } catch {
      // Malformed data from another script is ignored.
    }
  };
  if (!channel) eventTarget?.addEventListener?.("storage", onStorage);

  const transport = {
    async publish(pulse) {
      if (closed) return { local: false };
      for (const listener of hubListeners) listener(pulse, transport);
      if (channel) {
        channel.postMessage(pulse);
      } else {
        try {
          storage?.setItem?.(storageKey, JSON.stringify({ pulse }));
          storage?.removeItem?.(storageKey);
        } catch {
          // Storage may be blocked; the in-process fallback still works.
        }
      }
      return { local: true };
    },
    subscribe(listener) {
      if (typeof listener !== "function") throw new TypeError("subscribe() needs a function");
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    close() {
      if (closed) return;
      closed = true;
      listeners.clear();
      hubListeners.delete(hubListener);
      if (!hubListeners.size) hub.delete(channelName);
      channel?.close?.();
      eventTarget?.removeEventListener?.("storage", onStorage);
    },
  };

  return transport;
};

/** POST + EventSource transport for a shared cross-site relay. */
export const createRemotePresenceTransport = ({
  endpoint,
  space = DEFAULT_SPACE,
  fetchImpl = globalThis.fetch,
  EventSourceImpl = globalThis.EventSource,
} = {}) => {
  if (!endpoint) throw new TypeError("A presence endpoint is required");
  const normalizedSpace = normalizeSpace(space);
  const baseUrl = String(endpoint).replace(/\/$/, "");
  const listeners = new Set();
  let source = null;
  let closed = false;

  const deliver = (value) => {
    for (const listener of listeners) listener(value);
  };

  const ensureSource = () => {
    if (closed || source || typeof EventSourceImpl !== "function") return;
    const streamUrl = new URL(`${baseUrl}/stream`, globalThis.location?.href);
    streamUrl.searchParams.set("space", normalizedSpace);
    source = new EventSourceImpl(streamUrl.href, { withCredentials: false });
    const onMessage = (event) => {
      try {
        deliver(JSON.parse(event.data));
      } catch {
        // A relay response is untrusted input and may be malformed.
      }
    };
    source.addEventListener?.("pulse", onMessage);
    source.addEventListener?.("message", onMessage);
    if (!("addEventListener" in source)) source.onmessage = onMessage;
  };

  const transport = {
    async publish(pulse) {
      if (closed || typeof fetchImpl !== "function") return { remote: false };
      try {
        const response = await fetchImpl(baseUrl, {
          method: "POST",
          mode: "cors",
          credentials: "omit",
          cache: "no-store",
          referrerPolicy: "no-referrer",
          keepalive: true,
          headers: { "content-type": "application/json" },
          body: JSON.stringify(pulse),
        });
        return { remote: response.ok };
      } catch {
        return { remote: false };
      }
    },
    subscribe(listener) {
      if (typeof listener !== "function") throw new TypeError("subscribe() needs a function");
      listeners.add(listener);
      ensureSource();
      return () => listeners.delete(listener);
    },
    close() {
      closed = true;
      listeners.clear();
      source?.close?.();
      source = null;
    },
  };
  return transport;
};

/** Local delivery always works; the remote relay is an optional enhancement. */
export const createPresenceTransport = ({ endpoint, space, ...options } = {}) => {
  const local = createLocalPresenceTransport({ space, ...options });
  const remote = endpoint
    ? createRemotePresenceTransport({ endpoint, space, ...options })
    : null;

  return {
    async publish(pulse) {
      const localResult = await local.publish(pulse);
      const remoteResult = remote ? await remote.publish(pulse) : { remote: false };
      return { ...localResult, ...remoteResult };
    },
    subscribe(listener) {
      const stopLocal = local.subscribe(listener);
      const stopRemote = remote?.subscribe(listener);
      return () => {
        stopLocal();
        stopRemote?.();
      };
    },
    close() {
      local.close();
      remote?.close();
    },
  };
};

const resolveTarget = (target, documentImpl) => {
  if (!target) return null;
  if (typeof target === "string") return documentImpl?.querySelector?.(target) ?? null;
  return target;
};

/** Accessible, deliberately isolated visual surface for community pulses. */
export const createPresenceField = (target, {
  documentImpl = globalThis.document,
  ttlMs = DEFAULT_TTL_MS,
  label = "Зажечь огонёк",
  title = "Зажечь огонёк",
  hint = "Короткий знак: «я здесь»",
  successTitle = "Ваш огонёк горит",
  successHint = "Он мягко погаснет через несколько секунд",
  remoteTitle = "Кто-то ещё здесь",
  remoteHint = "Зажгите огонёк в ответ",
  maxVisible = 18,
} = {}) => {
  const host = resolveTarget(target, documentImpl);
  if (!host || !documentImpl?.createElement) {
    return {
      element: null,
      render() {},
      setHint() {},
      announce() {},
      onActivate() { return () => {}; },
      destroy() {},
    };
  }

  const root = documentImpl.createElement("div");
  root.className = "community-presence";
  root.dataset.communityPresence = "";

  const surface = documentImpl.createElement("button");
  surface.type = "button";
  surface.className = "community-presence__surface";
  surface.setAttribute("aria-label", label);

  const visual = documentImpl.createElement("span");
  visual.className = "community-presence__visual";
  visual.setAttribute("aria-hidden", "true");

  const effectLayer = documentImpl.createElement("span");
  effectLayer.className = "community-presence__effects";
  effectLayer.setAttribute("aria-hidden", "true");

  const copy = documentImpl.createElement("span");
  copy.className = "community-presence__copy";
  const titleNode = documentImpl.createElement("strong");
  titleNode.className = "community-presence__title";
  titleNode.textContent = title;
  const hintNode = documentImpl.createElement("span");
  hintNode.className = "community-presence__hint";
  hintNode.textContent = hint;
  copy.append(titleNode, hintNode);
  const status = documentImpl.createElement("span");
  status.className = "community-presence__status";
  status.setAttribute("aria-live", "polite");
  surface.append(effectLayer, visual, copy, status);
  root.append(surface);
  host.append(root);

  const activations = new Set();
  const pulseNodes = [];
  let idleHint = hint;
  let stateTimer = null;

  const showState = (state) => {
    if (state === "remote" && root.dataset.state === "local") return;
    clearTimeout(stateTimer);
    root.dataset.state = state;
    surface.dataset.state = state;
    titleNode.textContent = state === "local" ? successTitle : remoteTitle;
    hintNode.textContent = state === "local" ? successHint : remoteHint;
    stateTimer = setTimeout(() => {
      delete root.dataset.state;
      delete surface.dataset.state;
      titleNode.textContent = title;
      hintNode.textContent = idleHint;
    }, state === "local" ? 3200 : 2800);
    stateTimer.unref?.();
  };

  const activate = (event) => {
    const rect = surface.getBoundingClientRect();
    const pointer = event?.clientX !== undefined && event?.clientY !== undefined;
    const x = pointer && rect.width ? clamp((event.clientX - rect.left) / rect.width, 0, 1) : 0.5;
    const y = pointer && rect.height ? clamp((event.clientY - rect.top) / rect.height, 0, 1) : 0.5;
    for (const listener of activations) listener({ x, y });
  };
  surface.addEventListener("pointerdown", activate);
  surface.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") activate();
  });

  return {
    element: root,
    render(pulse, { local = false } = {}) {
      if (local) status.textContent = "Ваш огонёк зажёгся";
      showState(local ? "local" : "remote");
      const node = documentImpl.createElement("i");
      node.className = "community-presence__pulse";
      node.dataset.local = local ? "true" : "false";
      node.style.setProperty("--presence-x", `${pulse.x * 100}%`);
      node.style.setProperty("--presence-y", `${pulse.y * 100}%`);
      node.style.setProperty("--presence-tone", String(pulse.tone));
      node.style.setProperty("--presence-tone-percent", `${pulse.tone * 100}%`);
      const visualLife = clamp(pulse.expiresAt - Date.now(), 700, 2200);
      node.style.setProperty("--presence-life", `${visualLife}ms`);
      effectLayer.append(node);
      pulseNodes.push(node);
      while (pulseNodes.length > maxVisible) pulseNodes.shift()?.remove();
      const timer = setTimeout(() => {
        node.remove();
        const index = pulseNodes.indexOf(node);
        if (index >= 0) pulseNodes.splice(index, 1);
      }, visualLife + 100);
      timer.unref?.();
    },
    setHint(value) {
      idleHint = String(value ?? "");
      if (!root.dataset.state) hintNode.textContent = idleHint;
    },
    announce(value) {
      status.textContent = String(value ?? "");
    },
    onActivate(listener) {
      activations.add(listener);
      return () => activations.delete(listener);
    },
    destroy() {
      clearTimeout(stateTimer);
      activations.clear();
      root.remove();
    },
  };
};

/**
 * Coordinates consent-by-action, validation, deduplication, TTL and rendering.
 * It emits nothing until publish() is called by a deliberate user action.
 */
export const createCommunityPresence = ({
  target,
  endpoint,
  space = DEFAULT_SPACE,
  transport,
  field,
  now = Date.now,
  cryptoImpl = globalThis.crypto,
  ttlMs = DEFAULT_TTL_MS,
  minIntervalMs = DEFAULT_MIN_INTERVAL_MS,
  windowMs = DEFAULT_WINDOW_MS,
  windowLimit = DEFAULT_WINDOW_LIMIT,
  tone = () => Math.random(),
  onPulse,
  ...fieldOptions
} = {}) => {
  const normalizedSpace = normalizeSpace(space);
  const ownedTransport = !transport;
  const activeTransport = transport ?? createPresenceTransport({ endpoint, space: normalizedSpace });
  const resolvedFieldOptions = { ...fieldOptions };
  if (!Object.prototype.hasOwnProperty.call(resolvedFieldOptions, "label")) {
    resolvedFieldOptions.label = "Зажечь огонёк";
  }
  if (!Object.prototype.hasOwnProperty.call(resolvedFieldOptions, "title")) {
    resolvedFieldOptions.title = "Зажечь огонёк";
  }
  if (!Object.prototype.hasOwnProperty.call(resolvedFieldOptions, "hint")) {
    resolvedFieldOptions.hint = endpoint
      ? "Покажите другим посетителям, что вы здесь"
      : "Короткий знак: «я здесь»";
  }
  const activeField = field ?? createPresenceField(target, {
    ttlMs,
    ...resolvedFieldOptions,
  });
  const canPublish = createRateLimiter({ now, minIntervalMs, windowMs, windowLimit });
  const seen = new Map();
  let stopSubscription = null;
  let stopActivation = null;
  let started = false;
  let destroyed = false;

  const remember = (pulse) => {
    const current = now();
    for (const [id, expiresAt] of seen) {
      if (expiresAt <= current) seen.delete(id);
    }
    if (seen.has(pulse.eventId)) return false;
    seen.set(pulse.eventId, pulse.expiresAt);
    while (seen.size > MAX_SEEN_EVENTS) seen.delete(seen.keys().next().value);
    return true;
  };

  const receive = (candidate, local = false) => {
    const pulse = normalizePresencePulse(candidate, { now: now(), ttlMs });
    if (!pulse || pulse.space !== normalizedSpace || !remember(pulse)) return false;
    activeField.render?.(pulse, { local });
    onPulse?.(pulse, { local });
    return true;
  };

  const publish = async ({ x = 0.5, y = 0.5 } = {}) => {
    if (destroyed || !started) return { status: "stopped" };
    if (!canPublish()) {
      activeField.announce?.("Огонёк уже горит — подождите немного");
      return { status: "rate-limited" };
    }
    const current = now();
    const pulse = normalizePresencePulse({
      protocol: PROTOCOL_VERSION,
      kind: "pulse",
      eventId: createEventId(cryptoImpl),
      space: normalizedSpace,
      x: clamp(finiteNumber(x) ?? 0.5, 0, 1),
      y: clamp(finiteNumber(y) ?? 0.5, 0, 1),
      tone: clamp(finiteNumber(tone()) ?? 0.5, 0, 1),
      createdAt: current,
      expiresAt: current + clamp(ttlMs, 250, MAX_TTL_MS),
    }, { now: current, ttlMs });

    receive(pulse, true);
    const delivery = await activeTransport.publish(pulse);
    if (endpoint && delivery.remote === false) {
      activeField.setHint?.("Связь временно пропала, но ваш огонёк всё равно зажёгся");
      activeField.announce?.("Ваш огонёк зажёгся только на этом экране");
    } else if (endpoint && delivery.remote) {
      activeField.setHint?.(resolvedFieldOptions.hint);
      activeField.announce?.("Огонёк появился у других посетителей");
    }
    return { status: "sent", pulse, delivery };
  };

  return {
    start() {
      if (destroyed) throw new Error("A destroyed presence instance cannot be restarted");
      if (started) return this;
      started = true;
      stopSubscription = activeTransport.subscribe((pulse) => receive(pulse, false));
      stopActivation = activeField.onActivate?.((point) => { void publish(point); });
      return this;
    },
    publish,
    stop() {
      if (!started) return;
      started = false;
      stopSubscription?.();
      stopActivation?.();
      stopSubscription = null;
      stopActivation = null;
    },
    destroy() {
      if (destroyed) return;
      this.stop();
      destroyed = true;
      activeField.destroy?.();
      if (ownedTransport) activeTransport.close?.();
      seen.clear();
    },
  };
};

export const COMMUNITY_PRESENCE_DEFAULTS = Object.freeze({
  protocol: PROTOCOL_VERSION,
  space: DEFAULT_SPACE,
  ttlMs: DEFAULT_TTL_MS,
  minIntervalMs: DEFAULT_MIN_INTERVAL_MS,
  windowMs: DEFAULT_WINDOW_MS,
  windowLimit: DEFAULT_WINDOW_LIMIT,
});

export default createCommunityPresence;
