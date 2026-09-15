export const trafficFabricSchema = "soty.traffic-fabric.v1";

const transportOrder = Object.freeze(["local", "quic", "tls", "relay"]);

export function createTrafficFabric({
  now = () => new Date().toISOString(),
  uuid = () => { throw new Error("uuid-required"); },
  secret = () => { throw new Error("secret-required"); },
  digest = () => { throw new Error("digest-required"); }
} = {}) {
  return {
    normalizeState(value = {}) {
      value = value && typeof value === "object" ? value : {};
      const exits = Array.isArray(value.exits) ? value.exits.map(normalizeExit).filter(Boolean) : [];
      const clients = Array.isArray(value.clients) ? value.clients.map(normalizeClient).filter(Boolean) : [];
      return {
        schema: trafficFabricSchema,
        revision: trafficFabricSafeInteger(value.revision, 0),
        enabled: value.enabled === true,
        defaultPolicy: normalizePolicy(value.defaultPolicy),
        exits: uniqueBy(exits, (item) => item.id),
        clients: uniqueBy(clients, (item) => item.id),
        updatedAt: trafficFabricSafeText(value.updatedAt, 40) || now()
      };
    },

    emptyState() {
      return this.normalizeState({});
    },

    upsertExit(state, input) {
      const current = this.normalizeState(state);
      const exit = normalizeExit(input);
      if (!exit) throw new Error("invalid-exit");
      return touch(current, {
        exits: [exit, ...current.exits.filter((item) => item.id !== exit.id)]
      }, now);
    },

    removeExit(state, exitId) {
      const current = this.normalizeState(state);
      const id = trafficFabricSafeId(exitId);
      return touch(current, {
        exits: current.exits.filter((item) => item.id !== id),
        clients: current.clients.map((client) => ({
          ...client,
          allowedExitIds: client.allowedExitIds.filter((item) => item !== id),
          preferredExitId: client.preferredExitId === id ? "" : client.preferredExitId
        }))
      }, now);
    },

    issueClient(state, input = {}) {
      const current = this.normalizeState(state);
      const id = trafficFabricSafeId(input.id) || `client_${uuid()}`;
      const enrollmentSecret = secret();
      const allowedExitIds = cleanIds(input.allowedExitIds).filter((exitId) => current.exits.some((item) => item.id === exitId));
      const client = normalizeClient({
        ...input,
        id,
        allowedExitIds,
        credentialHash: digest(enrollmentSecret),
        credentialHint: enrollmentSecret.slice(0, 6),
        issuedAt: now(),
        revokedAt: ""
      });
      return {
        state: touch(current, { clients: [client, ...current.clients.filter((item) => item.id !== id)] }, now),
        client,
        secret: enrollmentSecret
      };
    },

    revokeClient(state, clientId) {
      const current = this.normalizeState(state);
      const id = trafficFabricSafeId(clientId);
      return touch(current, {
        clients: current.clients.map((item) => item.id === id ? { ...item, revokedAt: now() } : item)
      }, now);
    },

    chooseExit(state, clientId, observations = []) {
      const current = this.normalizeState(state);
      const client = current.clients.find((item) => item.id === trafficFabricSafeId(clientId) && !item.revokedAt);
      if (!client) return { ok: false, reason: "client-unavailable", failClosed: true };
      const policy = { ...current.defaultPolicy, ...client.policy };
      const candidates = current.exits
        .filter((item) => item.enabled && client.allowedExitIds.includes(item.id))
        .map((exit) => scoreExit(exit, observations.find((item) => item?.exitId === exit.id), policy))
        .filter((item) => item.eligible)
        .sort(compareCandidates);
      const selected = candidates[0] || null;
      return selected
        ? { ok: true, exit: selected.exit, transport: selected.transport, score: selected.score, failClosed: policy.failClosed }
        : { ok: false, reason: "no-healthy-exit", failClosed: policy.failClosed };
    },

    publicState(state) {
      const current = this.normalizeState(state);
      return {
        ...current,
        clients: current.clients.map(({ credentialHash, ...client }) => client)
      };
    }
  };
}

function normalizeExit(value) {
  if (!value || typeof value !== "object") return null;
  const id = trafficFabricSafeId(value.id);
  if (!id) return null;
  const transports = (Array.isArray(value.transports) ? value.transports : [])
    .map(normalizeTransport).filter(Boolean);
  return {
    id,
    label: trafficFabricSafeText(value.label, 80) || id,
    deviceId: trafficFabricSafeId(value.deviceId),
    enabled: value.enabled !== false,
    route: value.route === "vpn" ? "vpn" : "direct",
    vpnInterface: trafficFabricSafeText(value.vpnInterface, 120),
    failClosed: value.failClosed !== false,
    priority: boundedInteger(value.priority, 0, 1000, 100),
    maxClients: boundedInteger(value.maxClients, 1, 10000, 64),
    transports: uniqueBy(transports, (item) => item.id),
    updatedAt: trafficFabricSafeText(value.updatedAt, 40) || new Date().toISOString()
  };
}

function normalizeTransport(value) {
  if (!value || typeof value !== "object") return null;
  const kind = transportOrder.includes(value.kind) ? value.kind : "";
  const id = trafficFabricSafeId(value.id) || (kind ? `${kind}-${trafficFabricSafeId(value.endpoint) || "default"}` : "");
  if (!kind || !id) return null;
  return {
    id,
    kind,
    endpoint: trafficFabricSafeText(value.endpoint, 500),
    enabled: value.enabled !== false,
    priority: boundedInteger(value.priority, 0, 1000, transportOrder.indexOf(kind) * 100),
    serverName: trafficFabricSafeText(value.serverName, 253),
    pinnedKey: trafficFabricSafeText(value.pinnedKey, 200)
  };
}

function normalizeClient(value) {
  if (!value || typeof value !== "object") return null;
  const id = trafficFabricSafeId(value.id);
  if (!id) return null;
  return {
    id,
    label: trafficFabricSafeText(value.label, 80) || id,
    platform: ["android", "ios", "windows", "macos", "linux", "other"].includes(value.platform) ? value.platform : "other",
    allowedExitIds: cleanIds(value.allowedExitIds),
    preferredExitId: trafficFabricSafeId(value.preferredExitId),
    policy: normalizePolicy(value.policy),
    credentialHash: trafficFabricSafeText(value.credentialHash, 128),
    credentialHint: trafficFabricSafeText(value.credentialHint, 12),
    issuedAt: trafficFabricSafeText(value.issuedAt, 40),
    revokedAt: trafficFabricSafeText(value.revokedAt, 40)
  };
}

function normalizePolicy(value = {}) {
  return {
    failClosed: value?.failClosed !== false,
    requireVpn: value?.requireVpn === true,
    allowDirectFallback: value?.allowDirectFallback === true,
    strategy: ["stable", "fastest", "preferred"].includes(value?.strategy) ? value.strategy : "stable"
  };
}

function scoreExit(exit, observation = {}, policy) {
  const healthy = observation?.healthy === true;
  const verifiedRoute = observation?.verifiedRoute === exit.route;
  const vpnRequired = policy.requireVpn || (exit.route === "vpn" && exit.failClosed);
  const eligible = healthy && (!vpnRequired || (exit.route === "vpn" && verifiedRoute));
  const transports = exit.transports
    .filter((item) => item.enabled)
    .map((item) => ({ ...item, healthy: observation?.transports?.[item.id]?.healthy === true }))
    .filter((item) => item.healthy)
    .sort((a, b) => a.priority - b.priority || transportOrder.indexOf(a.kind) - transportOrder.indexOf(b.kind));
  const transport = transports[0] || null;
  const latency = boundedInteger(observation?.latencyMs, 0, 120000, 120000);
  const loss = Math.max(0, Math.min(Number(observation?.loss) || 0, 1));
  const preferred = observation?.preferred === true ? -10000 : 0;
  return {
    exit,
    transport,
    eligible: eligible && Boolean(transport),
    score: exit.priority * 1000 + latency + Math.round(loss * 100000) + preferred
  };
}

function compareCandidates(a, b) {
  return a.score - b.score || a.exit.id.localeCompare(b.exit.id);
}

function touch(state, patch, now) {
  return { ...state, ...patch, revision: state.revision + 1, updatedAt: now() };
}

function cleanIds(value) {
  return [...new Set((Array.isArray(value) ? value : []).map(trafficFabricSafeId).filter(Boolean))].slice(0, 10000);
}

function uniqueBy(items, key) {
  const seen = new Set();
  return items.filter((item) => {
    const value = key(item);
    if (seen.has(value)) return false;
    seen.add(value);
    return true;
  });
}

function trafficFabricSafeId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u.test(text) ? text : "";
}

function trafficFabricSafeText(value, max) {
  return typeof value === "string" ? value.trim().slice(0, max) : "";
}

function trafficFabricSafeInteger(value, fallback) {
  return Number.isSafeInteger(value) ? value : fallback;
}

function boundedInteger(value, min, max, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isSafeInteger(parsed) ? Math.max(min, Math.min(parsed, max)) : fallback;
}
