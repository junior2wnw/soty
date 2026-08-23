import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { chmod, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { EventEmitter } from "node:events";

const storeSchema = "soty.connector-store.v2";
const previousStoreSchema = "soty.connector-store.v1";
const jobSchema = "soty.connector-job.v2";
const previousJobSchema = "soty.connector-job.v1";
const connectorFreshMs = 90_000;
const defaultLeaseMs = 75_000;
const finishedRetentionMs = 7 * 24 * 60 * 60_000;
const maxJobs = 2_000;
const maxEvents = 512;
const terminalStatuses = new Set(["succeeded", "failed", "cancelled"]);

export function createConnectorStore(dataDir, options = {}) {
  return new ConnectorStore(path.join(dataDir || "data", "connector-store.json"), options);
}

class ConnectorStore {
  constructor(filePath, options) {
    this.filePath = filePath;
    this.now = typeof options.now === "function" ? options.now : () => Date.now();
    this.leaseMs = safeInteger(options.leaseMs, 5_000, 10 * 60_000, defaultLeaseMs);
    this.events = new EventEmitter();
    this.events.setMaxListeners(0);
    this.state = emptyState();
    this.writeQueue = this.load();
  }

  async load() {
    try {
      const parsed = JSON.parse(await readFile(this.filePath, "utf8"));
      this.state = normalizeState(parsed);
    } catch {
      this.state = emptyState();
    }
    this.expire(this.now());
  }

  async register(input, token) {
    const clean = cleanRegistration(input);
    const secret = safeToken(token);
    if (!clean || !secret) return { ok: false, error: "invalid-registration" };
    return await this.mutate(() => {
      const now = this.now();
      this.expire(now);
      const tokenHash = hashToken(secret);
      const existing = this.state.connectors.find((item) => connectorKey(item) === connectorKey(clean));
      if (existing && !sameHash(existing.tokenHash, tokenHash)) {
        return { ok: false, error: "connector-auth-failed" };
      }
      const connector = existing || { ...clean, tokenHash, createdAt: now };
      Object.assign(connector, clean, { tokenHash, lastSeenAt: now });
      if (!existing) this.state.connectors.push(connector);
      this.signal(clean.linkId, clean.deviceId);
      return { ok: true, connector: publicConnector(connector, now) };
    });
  }

  async status(linkId, deviceId = "") {
    await this.writeQueue;
    const link = safeLinkId(linkId);
    const device = safeDeviceId(deviceId);
    if (!link) return { ok: false, error: "invalid-link" };
    const now = this.now();
    const connectors = this.state.connectors
      .filter((item) => item.linkId === link && (!device || item.deviceId === device))
      .map((item) => publicConnector(item, now));
    const devices = aggregateDevices(connectors);
    return {
      ok: true,
      schema: "soty.connector-status.v1",
      connected: devices.some((item) => item.connected),
      devices
    };
  }

  isConnected(linkId, now = this.now()) {
    const link = safeLinkId(linkId);
    return Boolean(link && this.state.connectors.some((item) => item.linkId === link && now - item.lastSeenAt <= connectorFreshMs));
  }

  async authenticateModelToken(token) {
    await this.writeQueue;
    const secret = safeToken(token);
    if (!secret) return false;
    const tokenHash = hashToken(secret);
    return this.state.connectors.some((item) => item.protocol === 2 && sameHash(item.tokenHash, tokenHash));
  }


  async createJob(input) {
    const clean = cleanNewJob(input);
    if (!clean) return { ok: false, error: "invalid-job" };
    return await this.mutate(() => {
      const now = this.now();
      this.expire(now);
      const activeJobs = this.state.jobs.filter((job) => !terminalStatuses.has(job.status)).length;
      if (activeJobs >= maxJobs) return { ok: false, error: "connector-queue-full" };
      const job = {
        schema: jobSchema,
        id: `job_${randomUUID().replace(/-/gu, "")}`,
        linkId: clean.linkId,
        deviceId: clean.deviceId,
        threadId: clean.threadId,
        kind: clean.kind,
        input: clean.input,
        permissions: clean.permissions,
        status: "queued",
        attempts: 0,
        connectorId: "",
        leaseUntil: 0,
        cancelRequested: false,
        events: [],
        result: null,
        createdAt: now,
        updatedAt: now,
        finishedAt: 0
      };
      this.state.jobs.push(job);
      this.pushEvent(job, { type: "queued", text: "Задание принято" }, now);
      this.expire(now);
      this.signal(job.linkId, job.deviceId);
      return { ok: true, job: publicJob(job) };
    });
  }

  async getJob(linkId, jobId) {
    await this.writeQueue;
    const job = this.findOwnedJob(linkId, jobId);
    return job ? { ok: true, job: publicJob(job) } : { ok: false, error: "job-not-found" };
  }

  async getEvents(linkId, jobId, after = 0) {
    await this.writeQueue;
    const job = this.findOwnedJob(linkId, jobId);
    if (!job) return { ok: false, error: "job-not-found" };
    const cursor = Math.max(0, Number.isSafeInteger(after) ? after : 0);
    return {
      ok: true,
      job: publicJob(job, { events: false }),
      events: job.events.filter((event) => event.seq > cursor).map(publicEvent),
      cursor: job.events.at(-1)?.seq || cursor,
      done: terminalStatuses.has(job.status)
    };
  }

  async cancelJob(linkId, jobId) {
    return await this.mutate(() => {
      const job = this.findOwnedJob(linkId, jobId);
      if (!job) return { ok: false, error: "job-not-found" };
      if (terminalStatuses.has(job.status)) return { ok: true, job: publicJob(job) };
      const now = this.now();
      job.cancelRequested = true;
      job.updatedAt = now;
      this.pushEvent(job, { type: "cancel_requested", text: "Запрошена отмена" }, now);
      if (job.status === "queued") {
        job.status = "cancelled";
        job.finishedAt = now;
        job.result = { ok: false, text: "Отменено", exitCode: 130 };
      }
      this.signal(job.linkId, job.deviceId);
      return { ok: true, job: publicJob(job) };
    });
  }

  async poll(auth, waitMs = 0, signal) {
    const first = await this.lease(auth);
    if (!first.ok || first.jobs.length > 0 || first.cancel.length > 0 || waitMs <= 0) return first;
    await this.waitForChange(first.connector.linkId, first.connector.deviceId, waitMs, signal);
    return await this.lease(auth);
  }

  async lease(auth) {
    return await this.mutate(() => {
      const connector = this.authenticate(auth);
      if (!connector) return { ok: false, error: "connector-auth-failed" };
      const now = this.now();
      connector.lastSeenAt = now;
      this.expire(now);
      const cancel = this.state.jobs
        .filter((job) => job.connectorId === connector.connectorId && job.deviceId === connector.deviceId && job.cancelRequested && !terminalStatuses.has(job.status))
        .map((job) => job.id);
      const candidate = this.state.jobs.find((job) => canLease(job, connector, now));
      const jobs = [];
      if (candidate) {
        candidate.status = "leased";
        candidate.connectorId = connector.connectorId;
        candidate.deviceId = candidate.deviceId || connector.deviceId;
        candidate.leaseUntil = now + this.leaseMs;
        candidate.attempts += 1;
        candidate.updatedAt = now;
        this.pushEvent(candidate, { type: "leased", text: connector.deviceNick || connector.deviceId }, now);
        jobs.push(connectorJob(candidate));
      }
      return { ok: true, connector: publicConnector(connector, now), jobs, cancel };
    });
  }

  async appendEvent(auth, jobId, value) {
    const event = cleanEvent(value);
    if (!event) return { ok: false, error: "invalid-event" };
    return await this.mutate(() => {
      const connector = this.authenticate(auth);
      const job = connector ? this.assignedJob(connector, jobId) : null;
      if (!connector || !job) return { ok: false, error: "connector-auth-failed" };
      if (terminalStatuses.has(job.status)) return { ok: true, job: publicJob(job) };
      const now = this.now();
      connector.lastSeenAt = now;
      job.status = job.cancelRequested ? job.status : "running";
      job.leaseUntil = now + this.leaseMs;
      job.updatedAt = now;
      this.pushEvent(job, event, now);
      this.signal(job.linkId, job.deviceId);
      return { ok: true, job: publicJob(job) };
    });
  }

  async finishJob(auth, jobId, value) {
    const result = cleanResult(value);
    if (!result) return { ok: false, error: "invalid-result" };
    return await this.mutate(() => {
      const connector = this.authenticate(auth);
      const job = connector ? this.assignedJob(connector, jobId) : null;
      if (!connector || !job) return { ok: false, error: "connector-auth-failed" };
      if (terminalStatuses.has(job.status)) return { ok: true, job: publicJob(job) };
      const now = this.now();
      connector.lastSeenAt = now;
      job.result = result;
      job.status = job.cancelRequested || result.exitCode === 130 ? "cancelled" : result.ok ? "succeeded" : "failed";
      job.leaseUntil = 0;
      job.finishedAt = now;
      job.updatedAt = now;
      this.pushEvent(job, { type: job.status, text: result.text.slice(0, 4_000) }, now);
      this.signal(job.linkId, job.deviceId);
      return { ok: true, job: publicJob(job) };
    });
  }


  findOwnedJob(linkId, jobId) {
    const link = safeLinkId(linkId);
    const id = safeId(jobId, 160);
    return link && id ? this.state.jobs.find((job) => job.linkId === link && job.id === id) || null : null;
  }

  authenticate(auth) {
    const linkId = safeLinkId(auth?.linkId);
    const deviceId = safeDeviceId(auth?.deviceId);
    const connectorId = safeId(auth?.connectorId, 160);
    const token = safeToken(auth?.token);
    if (!linkId || !deviceId || !connectorId || !token) return null;
    const connector = this.state.connectors.find((item) => item.linkId === linkId && item.deviceId === deviceId && item.connectorId === connectorId);
    return connector && sameHash(connector.tokenHash, hashToken(token)) ? connector : null;
  }

  assignedJob(connector, jobId) {
    const id = safeId(jobId, 160);
    return id ? this.state.jobs.find((job) => job.id === id && job.linkId === connector.linkId && job.deviceId === connector.deviceId && job.connectorId === connector.connectorId) || null : null;
  }

  pushEvent(job, event, now = this.now()) {
    const clean = cleanEvent(event);
    if (!clean) return;
    const seq = (job.events.at(-1)?.seq || 0) + 1;
    job.events.push({ ...clean, seq, at: now });
    if (job.events.length > maxEvents) job.events.splice(0, job.events.length - maxEvents);
  }

  expire(now) {
    for (const job of this.state.jobs) {
      if (!terminalStatuses.has(job.status) && now - job.createdAt >= finishedRetentionMs) {
        job.status = "failed";
        job.result = { ok: false, text: "Срок хранения задания истёк", exitCode: 124 };
        job.leaseUntil = 0;
        job.finishedAt = now;
        job.updatedAt = now;
        this.pushEvent(job, { type: "failed", text: "Срок хранения задания истёк" }, now);
        continue;
      }
      if ((job.status === "leased" || job.status === "running") && job.leaseUntil > 0 && job.leaseUntil <= now) {
        if (job.cancelRequested) {
          job.status = "cancelled";
          job.result = { ok: false, text: "Отменено", exitCode: 130 };
          job.finishedAt = now;
        } else {
          job.status = "queued";
          job.connectorId = "";
          job.leaseUntil = 0;
          this.pushEvent(job, { type: "retry", text: "Коннектор отключился, задание возвращено в очередь" }, now);
        }
        job.updatedAt = now;
      }
    }
    this.state.connectors = this.state.connectors.filter((item) => now - item.lastSeenAt < finishedRetentionMs);
    const retained = this.state.jobs
      .filter((job) => !job.finishedAt || now - job.finishedAt < finishedRetentionMs)
      .sort((a, b) => a.createdAt - b.createdAt);
    const active = retained.filter((job) => !terminalStatuses.has(job.status));
    const finished = retained.filter((job) => terminalStatuses.has(job.status));
    const finishedSlots = Math.max(0, maxJobs - active.length);
    const keep = [...active, ...finished.slice(Math.max(0, finished.length - finishedSlots))];
    this.state.jobs = keep.sort((a, b) => a.createdAt - b.createdAt);
  }

  signal(linkId, deviceId) {
    this.events.emit(changeKey(linkId, deviceId));
    this.events.emit(changeKey(linkId, ""));
  }

  waitForChange(linkId, deviceId, waitMs, signal) {
    if (signal?.aborted) return Promise.resolve();
    return new Promise((resolve) => {
      const key = changeKey(linkId, deviceId);
      let timer;
      const done = () => {
        clearTimeout(timer);
        this.events.removeListener(key, done);
        signal?.removeEventListener("abort", done);
        resolve();
      };
      timer = setTimeout(done, safeInteger(waitMs, 100, 30_000, 25_000));
      timer.unref?.();
      this.events.once(key, done);
      signal?.addEventListener("abort", done, { once: true });
    });
  }

  async mutate(callback) {
    const run = this.writeQueue.then(async () => {
      const result = callback();
      await this.persist();
      return result;
    });
    this.writeQueue = run.then(() => undefined, () => undefined);
    return await run;
  }

  async persist() {
    await mkdir(path.dirname(this.filePath), { recursive: true });
    const next = `${this.filePath}.${process.pid}.next`;
    await writeFile(next, `${JSON.stringify(this.state, null, 2)}\n`, { mode: 0o600 });
    await chmod(next, 0o600).catch(() => undefined);
    await rename(next, this.filePath);
  }
}

function canLease(job, connector, now) {
  if (job.status !== "queued" || job.cancelRequested || job.leaseUntil > now) return false;
  if (job.linkId !== connector.linkId || (job.deviceId && job.deviceId !== connector.deviceId)) return false;
  if (job.kind === "agent") return connector.protocol === 2 && connector.agent?.available === true;
  if (!connector.capabilities.includes(job.kind)) return false;
  const runAs = job.input?.runAs === "system" ? "system" : "user";
  return runAs === (connector.scope === "Machine" ? "system" : "user");
}

function emptyState() {
  return { schema: storeSchema, connectors: [], jobs: [] };
}

function normalizeState(value) {
  const state = emptyState();
  if (![storeSchema, previousStoreSchema].includes(value?.schema)) return state;
  state.connectors = Array.isArray(value.connectors)
    ? value.connectors.filter(validStoredConnector).map(normalizeStoredConnector)
    : [];
  state.jobs = Array.isArray(value.jobs)
    ? value.jobs
      .filter((job) => [jobSchema, previousJobSchema].includes(job?.schema) && safeId(job.id, 160) && safeLinkId(job.linkId))
      .map((job) => {
        const { adapterId: _adapterId, requestedAdapterId: _requestedAdapterId, ...rest } = job;
        return {
          ...rest,
          schema: jobSchema,
          kind: cleanJobKind(job.kind || job.input?.kind),
          events: Array.isArray(job.events) ? job.events.map(normalizeStoredEvent).filter(Boolean).slice(-maxEvents) : []
        };
      })
    : [];
  return state;
}

function cleanRegistration(value) {
  const linkId = safeLinkId(value?.linkId);
  const deviceId = safeDeviceId(value?.deviceId);
  const connectorId = safeId(value?.connectorId, 160);
  if (!linkId || !deviceId || !connectorId) return null;
  return {
    linkId,
    deviceId,
    connectorId,
    deviceNick: safeText(value?.deviceNick, 120) || deviceId,
    version: safeText(value?.version, 40),
    platform: safeText(value?.platform, 40),
    scope: safeText(value?.scope, 40),
    capabilities: cleanStringList(value?.capabilities, 64, 80),
    protocol: value?.agent?.id === "opencode" && value?.agent?.provider === "gonka" ? 2 : 1,
    agent: cleanAgent(value?.agent) || legacyAgent(value?.adapters)
  };
}

function normalizeStoredConnector(value) {
  const clean = cleanRegistration(value);
  return { ...value, ...clean };
}

function cleanAgent(value) {
  if (!value || typeof value !== "object" || value.id !== "opencode" || value.provider !== "gonka") return null;
  return {
    id: "opencode",
    name: "OpenCode",
    provider: "gonka",
    model: safeText(value?.model, 200),
    available: value?.available === true,
    version: safeText(value?.version, 80),
    reason: safeText(value?.reason, 240),
    capabilities: cleanStringList(value?.capabilities, 32, 80)
  };
}

function legacyAgent(value) {
  if (!Array.isArray(value)) return null;
  return cleanAgent({ id: "opencode", provider: "gonka", available: false, reason: "Требуется обновление Soty Agent" });
}

function cleanNewJob(value) {
  const linkId = safeLinkId(value?.linkId);
  const requestedKind = value?.kind || value?.input?.kind;
  if (!["agent", "chat", "command", "script"].includes(requestedKind)) return null;
  const kind = cleanJobKind(requestedKind);
  const script = safeMultiline(value?.input?.script, 8_000_000);
  const text = safeMultiline(value?.input?.text ?? value?.text, 64_000) || (kind === "script" && script ? safeText(value?.input?.name, 120) || "script" : "");
  if (!linkId || !text || (kind === "script" && !script)) return null;
  return {
    linkId,
    deviceId: safeDeviceId(value?.deviceId),
    threadId: safeId(value?.threadId || `thread_${randomUUID().replace(/-/gu, "")}`, 160),
    kind,
    input: {
      text,
      context: safeMultiline(value?.input?.context ?? value?.context, 128_000),
      cwd: safeText(value?.input?.cwd, 2_000),
      sessionId: safeText(value?.input?.sessionId, 200),
      kind,
      name: safeText(value?.input?.name, 120),
      shell: safeText(value?.input?.shell, 80),
      script,
      runAs: value?.input?.runAs === "system" ? "system" : "user",
      timeoutMs: safeInteger(value?.input?.timeoutMs, 1_000, 24 * 60 * 60_000, 30 * 60_000)
    },
    permissions: cleanPermissions(value?.permissions)
  };
}

function cleanJobKind(value) {
  if (value === "command" || value === "script") return value;
  return "agent";
}

function cleanPermissions(value) {
  const sandbox = ["read-only", "workspace-write", "danger-full-access"].includes(value?.sandbox) ? value.sandbox : "workspace-write";
  return { sandbox, approval: value?.approval === "on-request" ? "on-request" : "never" };
}

function cleanEvent(value) {
  const type = safeId(value?.type || "message", 80);
  if (!type) return null;
  const data = value?.data && typeof value.data === "object" && !Array.isArray(value.data) ? safeJsonObject(value.data) : undefined;
  return { type, text: safeMultiline(value?.text, 64_000), ...(data ? { data } : {}) };
}

function normalizeStoredEvent(value) {
  const event = cleanEvent(value);
  const seq = safeInteger(value?.seq, 1, Number.MAX_SAFE_INTEGER, 0);
  const at = Number(value?.at);
  return event && seq > 0 && Number.isFinite(at) ? { ...event, seq, at } : null;
}

function cleanResult(value) {
  if (!value || typeof value !== "object") return null;
  const exitCode = safeInteger(value.exitCode, 0, 65_535, value.ok === true ? 0 : 1);
  return {
    ok: value.ok === true && exitCode === 0,
    text: safeMultiline(value.text, 1_000_000),
    exitCode,
    sessionId: safeText(value.sessionId, 200),
    agentId: value.agentId === "opencode" ? "opencode" : ""
  };
}

function connectorJob(job) {
  return {
    schema: job.schema,
    id: job.id,
    threadId: job.threadId,
    kind: job.kind,
    // Kept only until protocol-v1 installations finish upgrading; protocol v2 ignores it.
    adapterId: job.kind === "agent" ? "opencode" : job.input?.runAs === "system" ? "shell-system" : "shell-user",
    input: job.input,
    permissions: job.permissions,
    attempt: job.attempts,
    cancelRequested: job.cancelRequested
  };
}

function publicJob(job, options = {}) {
  return {
    schema: job.schema,
    id: job.id,
    deviceId: job.deviceId,
    threadId: job.threadId,
    kind: job.kind,
    status: job.status,
    attempts: job.attempts,
    cancelRequested: job.cancelRequested,
    result: job.result,
    ...(options.events === false ? {} : { events: job.events.map(publicEvent) }),
    createdAt: new Date(job.createdAt).toISOString(),
    updatedAt: new Date(job.updatedAt).toISOString(),
    finishedAt: job.finishedAt ? new Date(job.finishedAt).toISOString() : ""
  };
}

function publicEvent(event) {
  return { seq: event.seq, type: event.type, text: event.text, ...(event.data ? { data: event.data } : {}), at: new Date(event.at).toISOString() };
}

function publicConnector(connector, now) {
  return {
    connectorId: connector.connectorId,
    deviceId: connector.deviceId,
    deviceNick: connector.deviceNick,
    version: connector.version,
    platform: connector.platform,
    scope: connector.scope,
    protocol: connector.protocol,
    capabilities: connector.capabilities,
    agent: connector.agent,
    connected: now - connector.lastSeenAt <= connectorFreshMs,
    lastSeenAt: new Date(connector.lastSeenAt).toISOString()
  };
}

function aggregateDevices(connectors) {
  const connectorGroups = new Map();
  for (const connector of connectors) {
    const group = connectorGroups.get(connector.deviceId) || [];
    group.push(connector);
    connectorGroups.set(connector.deviceId, group);
  }

  const devices = [...connectorGroups.entries()].map(([deviceId, group]) => {
    const connected = group.filter((connector) => connector.connected);
    const active = connected.length > 0 ? connected : group;
    const availableAgents = active.filter((connector) => connector.agent?.available);
    const representative = newestConnector(availableAgents.length > 0 ? availableAgents : active);
    const newest = newestConnector(active);

    return {
      deviceId,
      deviceNick: representative.deviceNick,
      connected: connected.length > 0,
      version: representative.version,
      platform: representative.platform,
      scopes: [...new Set(active.map((connector) => connector.scope).filter(Boolean))],
      capabilities: [...new Set(active.flatMap((connector) => connector.capabilities))],
      agent: representative.agent,
      lastSeenAt: newest.lastSeenAt
    };
  });

  return [...devices.values()].sort((a, b) => Number(b.connected) - Number(a.connected) || b.lastSeenAt.localeCompare(a.lastSeenAt));
}

function newestConnector(connectors) {
  return connectors.reduce((newest, connector) => connector.lastSeenAt > newest.lastSeenAt ? connector : newest);
}

function validStoredConnector(value) {
  return Boolean(cleanRegistration(value) && /^[a-f0-9]{64}$/u.test(value.tokenHash) && Number.isFinite(value.lastSeenAt));
}

function connectorKey(value) {
  return `${value.linkId}\u0000${value.deviceId}\u0000${value.connectorId}`;
}

function changeKey(linkId, deviceId) {
  return `${linkId}\u0000${deviceId || "*"}`;
}

function hashToken(value) {
  return createHash("sha256").update(value).digest("hex");
}

function sameHash(left, right) {
  if (!/^[a-f0-9]{64}$/u.test(left) || !/^[a-f0-9]{64}$/u.test(right)) return false;
  return timingSafeEqual(Buffer.from(left, "hex"), Buffer.from(right, "hex"));
}

function safeToken(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{40,160}$/u.test(text) ? text : "";
}

function safeLinkId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{32,192}$/u.test(text) ? text : "";
}

function safeDeviceId(value) {
  return safeId(value, 180);
}

function safeId(value, max = 120) {
  const text = String(value || "").trim().slice(0, max);
  return /^[A-Za-z0-9][A-Za-z0-9_.:-]*$/u.test(text) ? text : "";
}

function safeText(value, max) {
  return typeof value === "string" ? value.replace(/[\r\n\t]+/gu, " ").trim().slice(0, max) : "";
}

function safeMultiline(value, max) {
  return typeof value === "string" ? value.replace(/\r\n?/gu, "\n").trim().slice(0, max) : "";
}

function safeInteger(value, min, max, fallback) {
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= min && number <= max ? number : fallback;
}

function cleanStringList(value, maxItems, maxChars) {
  return [...new Set((Array.isArray(value) ? value : []).map((item) => safeText(item, maxChars)).filter(Boolean))].slice(0, maxItems);
}

function safeJsonObject(value) {
  try {
    const text = JSON.stringify(value);
    return text.length <= 32_000 ? JSON.parse(text) : undefined;
  } catch {
    return undefined;
  }
}
