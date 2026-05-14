import express from "express";
import { randomUUID } from "node:crypto";

const maxChatChars = 12_000;
const maxContextChars = 16_000;
const maxReplyChars = 12_000;
const maxReplyMessages = 64;
const maxSourceChars = 180;
const maxTaskTimeoutMs = 2 * 60 * 60_000;
const defaultSourceTimeoutMs = 10 * 60_000;
const leaseMs = maxTaskTimeoutMs + 10 * 60_000;
const connectedMs = 70_000;
const requestTtlMs = maxTaskTimeoutMs + 20 * 60_000;
const idleChannelTtlMs = 30 * 60_000;
const sourceConnectedMs = 90_000;
const sourceJobPickupBaseMs = 90_000;
const sourceJobTtlMs = maxTaskTimeoutMs + 20 * 60_000;
const sourceCancelTtlMs = 5 * 60_000;
const maxJobsPerChannel = 80;
const maxDiagnosticSources = 16;
const channels = new Map();
const agentSources = new Map();
const pollWaiters = new Map();
const replyWaiters = new Map();
const eventWaiters = new Map();
const sourcePollWaiters = new Map();
const sourceReplyWaiters = new Map();
const jsonParser = express.json({ limit: "2mb", type: "application/json" });
const configuredServerCodexRelayId = normalizeRelayId(process.env.SOTY_SERVER_CODEX_RELAY_ID || process.env.SOTY_AGENT_RELAY_ID || "");

export function attachAgentRelay(app) {
  app.get("/api/agent/relay/status", (req, res) => {
    const relayId = normalizeRelayId(req.query.relayId);
    if (!relayId) {
      res.status(400).json({ ok: false });
      return;
    }
    cleanupChannels();
    const now = Date.now();
    const target = resolveRequestChannel(relayId, now);
    const channel = target.connected ? channels.get(target.relayId) : channels.get(relayId);
    res.json({
      ok: true,
      connected: Boolean(target.connected),
      lastSeenAt: channel ? new Date(channelActivityAt(channel, now)).toISOString() : "",
      version: channel?.agentVersion || "",
      codex: channel?.codex === true,
      serverRelay: Boolean(target.clientRelayId),
      deviceId: channel?.deviceId || "",
      deviceNick: channel?.deviceNick || ""
    });
  });

  app.post("/api/agent/relay/request", jsonParser, (req, res) => {
    const relayId = normalizeRelayId(req.body?.relayId);
    const text = cleanText(req.body?.text, maxChatChars);
    const context = cleanText(req.body?.context, maxContextChars);
    let source = cleanAgentSource(req.body?.source);
    if (!relayId || !text.trim()) {
      res.status(400).json({ ok: false });
      return;
    }
    cleanupChannels();
    const target = resolveRequestChannel(relayId, Date.now(), { preferServer: req.body?.preferServer === true });
    if (!target.connected) {
      res.status(409).json({ ok: false, error: "relay-not-connected" });
      return;
    }
    source = withSourceRelay(enrichAgentSource(target.relayId, relayId, source, text), relayId);
    const channel = getChannel(target.relayId);
    const job = {
      id: `agent_${randomUUID()}`,
      text,
      context,
      source,
      createdAt: Date.now(),
      leaseUntil: 0,
      reply: null,
      events: [],
      nextEventSeq: 0,
      clientRelayId: target.clientRelayId
    };
    channel.jobs.push(job);
    while (channel.jobs.length > maxJobsPerChannel) {
      channel.jobs.shift();
    }
    flushPollWaiters(target.relayId);
    res.json({ ok: true, id: job.id, relayId: target.relayId });
  });

  app.get("/api/agent/relay/poll", (req, res) => {
    const relayId = normalizeRelayId(req.query.relayId);
    if (!relayId) {
      res.status(400).json({ ok: false, jobs: [] });
      return;
    }
    cleanupChannels();
    const channel = getChannel(relayId);
    channel.lastPollAt = Date.now();
    channel.agentVersion = cleanText(req.query.version, 32);
    channel.codex = req.query.codex === "1";
    channel.agentScope = cleanText(req.query.scope, 40);
    channel.deviceId = cleanText(req.query.deviceId, maxSourceChars) || channel.deviceId || "";
    channel.deviceNick = cleanText(req.query.deviceNick, maxSourceChars) || channel.deviceNick || "";
    const jobs = leasePendingJobs(channel);
    if (jobs.length > 0 || req.query.wait !== "1") {
      res.json({ ok: true, jobs });
      return;
    }
    addWaiter(pollWaiters, relayId, req, res, () => ({ ok: true, jobs: leasePendingJobs(channel) }));
  });

  app.post("/api/agent/relay/reply", jsonParser, (req, res) => {
    const relayId = normalizeRelayId(req.body?.relayId);
    const id = cleanText(req.body?.id, 120);
    if (!relayId || !id) {
      res.status(400).json({ ok: false });
      return;
    }
    cleanupChannels();
    const channel = channels.get(relayId);
    const job = channel?.jobs.find((item) => item.id === id);
    if (!job) {
      res.status(404).json({ ok: false });
      return;
    }
    job.reply = {
      ok: Boolean(req.body?.ok),
      text: cleanText(req.body?.text, maxReplyChars),
      ...cleanReplyMessages(req.body?.messages),
      ...cleanReplyTerminal(req.body?.terminal),
      ...(Number.isSafeInteger(req.body?.exitCode) ? { exitCode: req.body.exitCode } : {})
    };
    job.leaseUntil = 0;
    flushReplyWaiters(relayId, id);
    if (job.clientRelayId) {
      flushReplyWaiters(job.clientRelayId, id);
    }
    flushEventWaiters(relayId, id);
    if (job.clientRelayId) {
      flushEventWaiters(job.clientRelayId, id);
    }
    res.json({ ok: true });
  });

  app.post("/api/agent/relay/event", jsonParser, (req, res) => {
    const relayId = normalizeRelayId(req.body?.relayId);
    const id = cleanText(req.body?.id, 120);
    const text = cleanText(req.body?.text, maxReplyChars);
    const type = cleanText(req.body?.type, 40) || "agent_message";
    if (!relayId || !id || !text.trim()) {
      res.status(400).json({ ok: false });
      return;
    }
    cleanupChannels();
    const channel = channels.get(relayId);
    const job = channel?.jobs.find((item) => item.id === id);
    if (!job) {
      res.status(404).json({ ok: false });
      return;
    }
    const event = {
      seq: ++job.nextEventSeq,
      type,
      text,
      createdAt: new Date().toISOString()
    };
    job.events.push(event);
    while (job.events.length > maxReplyMessages) {
      job.events.shift();
    }
    flushEventWaiters(relayId, id);
    if (job.clientRelayId) {
      flushEventWaiters(job.clientRelayId, id);
    }
    res.json({ ok: true, event });
  });

  app.get("/api/agent/relay/reply", (req, res) => {
    const relayId = normalizeRelayId(req.query.relayId);
    const id = cleanText(req.query.id, 120);
    if (!relayId || !id) {
      res.status(400).json({ ok: false, reply: null });
      return;
    }
    cleanupChannels();
    const reply = findReply(relayId, id);
    if (reply || req.query.wait !== "1") {
      res.json({ ok: true, reply });
      return;
    }
    addWaiter(replyWaiters, replyKey(relayId, id), req, res, () => ({ ok: true, reply: findReply(relayId, id) }));
  });

  app.get("/api/agent/relay/events", (req, res) => {
    const relayId = normalizeRelayId(req.query.relayId);
    const id = cleanText(req.query.id, 120);
    const after = Number.parseInt(String(req.query.after || "0"), 10);
    if (!relayId || !id) {
      res.status(400).json({ ok: false, events: [], done: true });
      return;
    }
    cleanupChannels();
    const payload = relayEventsPayload(relayId, id, Number.isSafeInteger(after) ? after : 0);
    if (payload.events.length > 0 || payload.done || req.query.wait !== "1") {
      res.json(payload);
      return;
    }
    addWaiter(eventWaiters, replyKey(relayId, id), req, res, () => relayEventsPayload(relayId, id, Number.isSafeInteger(after) ? after : 0));
  });

  app.post("/api/agent/source/grant", jsonParser, (req, res) => {
    const relayId = normalizeRelayId(req.body?.relayId);
    const deviceId = cleanText(req.body?.deviceId, maxSourceChars);
    if (!relayId || !deviceId) {
      res.status(400).json({ ok: false });
      return;
    }
    const source = getAgentSource(relayId, deviceId);
    source.access = req.body?.enabled !== false;
    source.deviceNick = cleanText(req.body?.deviceNick, maxSourceChars) || source.deviceNick || "";
    applyAgentSourceClientInfo(source, req.body || {});
    touchAgentSource(source);
    flushPollWaiters(relayId);
    res.json({ ok: true, target: publicAgentSourceTarget(source) });
  });

  app.get("/api/agent/source/targets", (req, res) => {
    const relayId = normalizeRelayId(req.query.relayId);
    if (!relayId) {
      res.status(400).json({ ok: false, targets: [] });
      return;
    }
    cleanupAgentSources();
    res.json({
      ok: true,
      targets: connectedAgentSources(relayId).map(publicAgentSourceTarget)
    });
  });

  app.get("/api/agent/source/status", (req, res) => {
    const relayId = normalizeRelayId(req.query.relayId);
    const deviceId = cleanText(req.query.deviceId, maxSourceChars);
    cleanupAgentSources();
    const diagnostic = agentSourceDiagnostics(relayId, deviceId);
    res.status(relayId ? 200 : 400).json({
      ok: Boolean(relayId),
      ...diagnostic
    });
  });

  app.get("/api/agent/source/poll", (req, res) => {
    const relayId = normalizeRelayId(req.query.relayId);
    const deviceId = cleanText(req.query.deviceId, maxSourceChars);
    if (!relayId || !deviceId) {
      res.status(400).json({ ok: false, jobs: [] });
      return;
    }
    const source = getAgentSource(relayId, deviceId);
    source.deviceNick = cleanText(req.query.deviceNick, maxSourceChars) || source.deviceNick || "";
    applyAgentSourceClientInfo(source, req.query || {});
    touchAgentSource(source);
    if (!pollRequesterCanLeaseSourceJobs(req.query || {})) {
      res.json({ ok: true, jobs: [] });
      return;
    }
    const requester = sourcePollRequester(req.query || {});
    const jobs = leasePendingSourceJobs(source, requester);
    if (jobs.length > 0 || req.query.wait !== "1") {
      res.json({ ok: true, jobs });
      return;
    }
    addWaiter(sourcePollWaiters, source.key, req, res, () => ({ ok: true, jobs: leasePendingSourceJobs(source, requester) }));
  });

  app.post("/api/agent/source/output", jsonParser, (req, res) => {
    const relayId = normalizeRelayId(req.body?.relayId);
    const deviceId = cleanText(req.body?.deviceId, maxSourceChars);
    const id = cleanText(req.body?.id, 120);
    if (!relayId || !deviceId || !id) {
      res.status(400).json({ ok: false });
      return;
    }
    const source = findAgentSource(relayId, deviceId);
    const job = source?.jobs.find((item) => item.id === id);
    if (!source || !job) {
      res.status(404).json({ ok: false });
      return;
    }
    touchAgentSource(source);
    job.text = `${job.text || ""}${cleanText(req.body?.text, maxReplyChars)}`.slice(-maxReplyChars);
    if (Number.isSafeInteger(req.body?.exitCode)) {
      if (!(job.cancelRequested === true && job.exitCode === 130 && req.body.exitCode !== 130)) {
        job.exitCode = req.body.exitCode;
      }
      flushWaiters(sourceReplyWaiters, job.id, () => sourceJobReply(job));
    }
    res.json({ ok: true });
  });

  app.post("/api/agent/source/cancel", jsonParser, (req, res) => {
    const relayId = normalizeRelayId(req.body?.relayId);
    const deviceId = cleanText(req.body?.deviceId, maxSourceChars);
    const id = cleanSourceJobId(req.body?.id);
    if (!relayId || !deviceId || !id) {
      res.status(400).json({ ok: false });
      return;
    }
    const source = findAgentSource(relayId, deviceId);
    const job = source?.jobs.find((item) => item.id === id);
    if (!source || !job) {
      res.status(404).json({ ok: false });
      return;
    }
    touchAgentSource(source);
    job.cancelRequested = true;
    job.text = `${job.text || ""}${job.text ? "\n" : ""}! cancelled`.slice(-maxReplyChars);
    job.exitCode = 130;
    source.cancels = source.cancels || [];
    if (!source.cancels.some((item) => item.commandId === id)) {
      source.cancels.push({
        id: `cancel_${randomUUID()}`,
        type: "cancel",
        commandId: id,
        createdAt: Date.now(),
        leaseUntil: 0
      });
    }
    flushSourcePollWaiters(source);
    flushWaiters(sourceReplyWaiters, job.id, () => sourceJobReply(job));
    res.json({ ok: true, id });
  });

  app.post("/api/agent/source/start", jsonParser, (req, res) => {
    const created = createSourceJobFromRequest(req.body);
    if (!created.ok) {
      res.status(created.httpStatus).json(created.payload);
      return;
    }
    res.json(sourceJobStarted(created.job));
  });

  app.get("/api/agent/source/job", (req, res) => {
    const relayId = normalizeRelayId(req.query.relayId);
    const deviceId = cleanText(req.query.deviceId, maxSourceChars);
    const id = cleanSourceJobId(req.query.id);
    if (!relayId || !deviceId || !id) {
      res.status(400).json({ ok: false, text: "! request", exitCode: 400 });
      return;
    }
    cleanupAgentSources();
    const source = findAgentSource(relayId, deviceId);
    if (source) {
      expireQueuedSourceJobs(source);
    }
    const job = source?.jobs.find((item) => item.id === id);
    if (!source || !job) {
      res.status(404).json({
        ok: false,
        status: "missing",
        text: "! source-job",
        exitCode: 404,
        diagnostic: agentSourceDiagnostics(relayId, deviceId)
      });
      return;
    }
    res.json(sourceJobStatus(job));
  });

  app.post("/api/agent/source/run", jsonParser, (req, res) => {
    const created = createSourceJobFromRequest({ ...req.body, type: "run" });
    if (!created.ok) {
      res.status(created.httpStatus).json(created.payload);
      return;
    }
    addWaiter(sourceReplyWaiters, created.job.id, req, res, () => sourceJobReply(created.job), sourceReplyWaitTimeout(created.job.timeoutMs));
  });

  app.post("/api/agent/source/script", jsonParser, (req, res) => {
    const created = createSourceJobFromRequest({ ...req.body, type: "script" });
    if (!created.ok) {
      res.status(created.httpStatus).json(created.payload);
      return;
    }
    addWaiter(sourceReplyWaiters, created.job.id, req, res, () => sourceJobReply(created.job), sourceReplyWaitTimeout(created.job.timeoutMs));
  });

  app.use("/api/agent/source", (error, _req, res, _next) => {
    if (error) {
      res.status(400).json({ ok: false });
      return;
    }
    res.status(404).json({ ok: false });
  });

  app.use("/api/agent/relay", (error, _req, res, _next) => {
    if (error) {
      res.status(400).json({ ok: false });
      return;
    }
    res.status(404).json({ ok: false });
  });
}

function getChannel(relayId) {
  let channel = channels.get(relayId);
  if (!channel) {
    channel = {
      jobs: [],
      lastPollAt: 0,
      agentVersion: "",
      codex: false,
      touchedAt: Date.now()
    };
    channels.set(relayId, channel);
  }
  channel.touchedAt = Date.now();
  return channel;
}

function resolveRequestChannel(relayId, now = Date.now(), options = {}) {
  if (options.preferServer === true) {
    const server = bestServerCodexChannel(relayId, now);
    if (server) {
      return { relayId: server.relayId, clientRelayId: relayId, connected: true };
    }
    return { relayId, clientRelayId: "", connected: false };
  }
  const requested = channels.get(relayId);
  const requestedConnected = requested ? isCodexChannelConnected(requested, now) : false;
  if (requested?.codex === true && requestedConnected) {
    return { relayId, clientRelayId: "", connected: true };
  }
  const server = bestServerCodexChannel(relayId, now);
  if (server) {
    return { relayId: server.relayId, clientRelayId: relayId, connected: true };
  }
  return { relayId, clientRelayId: "", connected: false };
}

function bestServerCodexChannel(clientRelayId, now = Date.now()) {
  return [...channels.entries()]
    .filter(([relayId, channel]) => relayId !== clientRelayId && isServerCodexChannel(relayId, channel, now))
    .map(([relayId, channel]) => ({
      relayId,
      load: channel.jobs.filter((job) => !job.reply).length,
      activity: channelActivityAt(channel, now)
    }))
    .sort((left, right) => left.load - right.load || right.activity - left.activity)[0] || null;
}

function isServerCodexChannel(relayId, channel, now = Date.now()) {
  if (!channel?.codex || !isCodexChannelConnected(channel, now)) {
    return false;
  }
  if (configuredServerCodexRelayId && relayId === configuredServerCodexRelayId) {
    return true;
  }
  if (String(channel.agentScope || "").toLowerCase() === "server") {
    return true;
  }
  return /^srv_codex_/u.test(relayId);
}

function isCodexChannelConnected(channel, now = Date.now()) {
  return now - (channel.lastPollAt || 0) < connectedMs || hasActiveLeasedJob(channel, now);
}

function hasActiveLeasedJob(channel, now = Date.now()) {
  return channel.jobs.some((job) => !job.reply && job.leaseUntil && job.leaseUntil > now);
}

function channelActivityAt(channel, now = Date.now()) {
  const activeLeaseUntil = channel.jobs
    .filter((job) => !job.reply && job.leaseUntil && job.leaseUntil > now)
    .reduce((latest, job) => Math.max(latest, job.leaseUntil || 0), 0);
  return Math.max(channel.lastPollAt || 0, activeLeaseUntil ? Math.min(activeLeaseUntil, now) : 0);
}

function leasePendingJobs(channel) {
  const now = Date.now();
  const jobs = channel.jobs
    .filter((job) => !job.reply && (!job.leaseUntil || job.leaseUntil < now))
    .slice(0, 2);
  for (const job of jobs) {
    job.leaseUntil = now + leaseMs;
  }
  return jobs.map((job) => ({
    id: job.id,
    text: job.text,
    context: job.context,
    source: job.source || {},
    createdAt: new Date(job.createdAt).toISOString()
  }));
}

function getAgentSource(relayId, deviceId) {
  const key = sourceKey(relayId, deviceId);
  let source = agentSources.get(key);
  if (!source) {
    source = {
      key,
      relayId,
      deviceId,
      deviceNick: "",
      access: false,
      lastSeenAt: 0,
      clientProtocol: "",
      clientCapabilities: [],
      localAgent: {},
      workers: {},
      jobs: [],
      cancels: []
    };
    agentSources.set(key, source);
  }
  return source;
}

function findAgentSource(relayId, deviceId) {
  return agentSources.get(sourceKey(relayId, deviceId)) || null;
}

function findRunnableAgentSource(relayId, deviceId) {
  const source = findAgentSource(relayId, deviceId);
  return isRunnableAgentSource(source) ? source : null;
}

function isRunnableAgentSource(source, now = Date.now()) {
  return Boolean(source && source.access === true && now - source.lastSeenAt < sourceConnectedMs);
}

function agentSourceUnavailableText(diagnostic) {
  return `! agent-source: ${cleanText(diagnostic?.reason, 80) || "unavailable"}`;
}

function agentSourceDiagnostics(relayId, deviceId, now = Date.now()) {
  const source = relayId && deviceId ? findAgentSource(relayId, deviceId) : null;
  const sourceDiagnostic = source ? publicAgentSourceDiagnostic(source, now) : null;
  const reason = agentSourceDiagnosticReason({ relayId, deviceId, source, sourceDiagnostic });
  return {
    relayId,
    deviceId,
    runnable: reason === "ok",
    reason,
    now: new Date(now).toISOString(),
    sourceConnectedMs,
    source: sourceDiagnostic,
    candidates: agentSourceDiagnosticCandidates(relayId, deviceId, now)
  };
}

function agentSourceDiagnosticReason({ relayId, deviceId, source, sourceDiagnostic }) {
  if (!relayId) {
    return "missing-relay";
  }
  if (!deviceId) {
    return "missing-device";
  }
  if (!source) {
    return "not-found";
  }
  if (sourceDiagnostic.access !== true) {
    return "access-denied";
  }
  if (sourceDiagnostic.lastSeenAgeMs >= sourceConnectedMs) {
    return "source-stale";
  }
  return "ok";
}

function agentSourceDiagnosticCandidates(relayId, deviceId, now = Date.now()) {
  const sources = [...agentSources.values()]
    .filter((source) => (relayId && source.relayId === relayId) || (deviceId && source.deviceId === deviceId))
    .sort((left, right) => {
      const leftExact = left.relayId === relayId && left.deviceId === deviceId ? 1 : 0;
      const rightExact = right.relayId === relayId && right.deviceId === deviceId ? 1 : 0;
      if (leftExact !== rightExact) {
        return rightExact - leftExact;
      }
      const leftRelay = left.relayId === relayId ? 1 : 0;
      const rightRelay = right.relayId === relayId ? 1 : 0;
      if (leftRelay !== rightRelay) {
        return rightRelay - leftRelay;
      }
      return right.lastSeenAt - left.lastSeenAt;
    })
    .slice(0, maxDiagnosticSources);
  return sources.map((source) => publicAgentSourceDiagnostic(source, now));
}

function publicAgentSourceDiagnostic(source, now = Date.now()) {
  const lastSeenAgeMs = Math.max(0, now - (source.lastSeenAt || 0));
  const directWorkerAgeMs = source.directWorkerSeenAt ? Math.max(0, now - source.directWorkerSeenAt) : null;
  const jobs = Array.isArray(source.jobs) ? source.jobs : [];
  const cancels = Array.isArray(source.cancels) ? source.cancels : [];
  const lastJob = jobs
    .slice()
    .sort((left, right) => (right.createdAt || 0) - (left.createdAt || 0))[0] || null;
  return {
    relayId: source.relayId,
    deviceId: source.deviceId,
    deviceNick: source.deviceNick || "",
    access: source.access === true,
    connected: source.access === true && lastSeenAgeMs < sourceConnectedMs,
    lastSeenAt: source.lastSeenAt ? new Date(source.lastSeenAt).toISOString() : "",
    lastSeenAgeMs,
    sourceConnectedMs,
    directWorkerSeenAt: source.directWorkerSeenAt ? new Date(source.directWorkerSeenAt).toISOString() : "",
    directWorkerAgeMs,
    directWorkerFresh: agentSourceDirectWorkerFresh(source, now),
    clientProtocol: source.clientProtocol || "",
    clientCapabilities: Array.isArray(source.clientCapabilities) ? source.clientCapabilities : [],
    localAgent: publicSourceLocalAgent(source.localAgent),
    workers: publicSourceWorkers(source.workers, now),
    pendingJobs: jobs.filter((job) => !Number.isSafeInteger(job.exitCode) && (!job.leaseUntil || job.leaseUntil <= now)).length,
    leasedJobs: jobs.filter((job) => !Number.isSafeInteger(job.exitCode) && job.leaseUntil && job.leaseUntil > now).length,
    finishedJobs: jobs.filter((job) => Number.isSafeInteger(job.exitCode)).length,
    cancels: cancels.length,
    lastJob: lastJob ? publicSourceJobDiagnostic(lastJob, now) : null
  };
}

function publicSourceJobDiagnostic(job, now = Date.now()) {
  const leased = Boolean(job.leaseUntil);
  const finished = Number.isSafeInteger(job.exitCode);
  return {
    id: cleanText(job.id, 120),
    type: cleanText(job.type, 40),
    createdAt: job.createdAt ? new Date(job.createdAt).toISOString() : "",
    ageMs: Math.max(0, now - (job.createdAt || now)),
    timeoutMs: safeTimeout(job.timeoutMs),
    leased,
    leasedAt: job.leasedAt ? new Date(job.leasedAt).toISOString() : "",
    leaseActive: Boolean(job.leaseUntil && job.leaseUntil > now),
    leaseUntil: job.leaseUntil ? new Date(job.leaseUntil).toISOString() : "",
    finished,
    ...(finished ? { exitCode: job.exitCode } : {}),
    textChars: String(job.text || "").length
  };
}

function publicSourceWorkers(workers, now = Date.now()) {
  const value = workers && typeof workers === "object" ? workers : {};
  return {
    user: publicSourceWorker(value.user, now),
    system: publicSourceWorker(value.system, now)
  };
}

function publicSourceWorker(worker, now = Date.now()) {
  const seenAt = Number.isFinite(worker?.seenAt) ? worker.seenAt : 0;
  const ageMs = seenAt ? Math.max(0, now - seenAt) : null;
  return {
    connected: Boolean(seenAt && ageMs < sourceConnectedMs),
    seenAt: seenAt ? new Date(seenAt).toISOString() : "",
    ageMs,
    clientProtocol: cleanText(worker?.clientProtocol, 80),
    clientCapabilities: Array.isArray(worker?.clientCapabilities) ? worker.clientCapabilities : [],
    localAgent: publicSourceLocalAgent(worker?.localAgent)
  };
}

function connectedAgentSources(relayId) {
  const now = Date.now();
  return [...agentSources.values()]
    .filter((source) => source.relayId === relayId && source.access === true && now - source.lastSeenAt < sourceConnectedMs);
}

function touchAgentSource(source) {
  source.lastSeenAt = Date.now();
  source.jobs = source.jobs.filter((job) => source.lastSeenAt - job.createdAt < sourceJobTtlMs);
  source.cancels = (source.cancels || []).filter((item) => source.lastSeenAt - item.createdAt < sourceCancelTtlMs);
}

function applyAgentSourceClientInfo(source, value) {
  const protocol = cleanText(value?.clientProtocol, 80);
  if (protocol) {
    source.clientProtocol = protocol;
  }
  const capabilities = cleanCapabilityList(value?.clientCapabilities);
  const isDirectWorkerHeartbeat = capabilities.includes("direct-device-worker");
  if (capabilities.length > 0 && (capabilities.includes("direct-device-worker") || !source.clientCapabilities?.includes("direct-device-worker"))) {
    source.clientCapabilities = capabilities;
  }
  const localAgent = localAgentInfoFrom(value);
  const workerPlane = sourceWorkerPlane(localAgent);
  if (isDirectWorkerHeartbeat) {
    source.directWorkerSeenAt = Date.now();
  }
  if (localAgent && isDirectWorkerHeartbeat) {
    source.workers = source.workers && typeof source.workers === "object" ? source.workers : {};
    source.workers[workerPlane] = {
      seenAt: Date.now(),
      clientProtocol: source.clientProtocol || "",
      clientCapabilities: capabilities,
      localAgent
    };
  }
  if (localAgent && (isDirectWorkerHeartbeat || !agentSourceDirectWorkerFresh(source))) {
    source.localAgent = localAgent;
  }
  source.localAgent = preferredSourceLocalAgent(source) || source.localAgent || {};
}

function cleanCapabilityList(value) {
  const items = Array.isArray(value)
    ? value
    : typeof value === "string"
      ? value.split(",")
      : [];
  return [...new Set(items
    .map((item) => cleanText(item, 40))
    .filter((item) => /^[a-z][a-z0-9-]{0,39}$/u.test(item)))]
    .slice(0, 24);
}

function localAgentInfoFrom(value) {
  const nested = value?.localAgent && typeof value.localAgent === "object" ? value.localAgent : null;
  const read = (name) => nested?.[name] ?? value?.[`localAgent${name[0].toUpperCase()}${name.slice(1)}`];
  const hasAny = nested
    || ["Ok", "Version", "Scope", "Companion", "ExecutionPlane", "InteractiveTaskBridge", "AutoUpdate", "System", "SourceWorker"].some((name) => value?.[`localAgent${name}`] !== undefined);
  if (!hasAny) {
    return null;
  }
  return {
    ok: readBoolean(read("ok")),
    version: cleanText(read("version"), 40),
    scope: cleanText(read("scope"), 40),
    companion: readBoolean(read("companion")),
    executionPlane: cleanText(read("executionPlane"), 80),
    interactiveTaskBridge: readBoolean(read("interactiveTaskBridge")),
    autoUpdate: readBoolean(read("autoUpdate")),
    system: readBoolean(read("system")),
    sourceWorker: readBoolean(read("sourceWorker"))
  };
}

function sourceWorkerPlane(localAgent) {
  return localAgent?.system === true ? "system" : "user";
}

function preferredSourceLocalAgent(source, now = Date.now()) {
  const user = freshSourceWorker(source, "user", now);
  if (user?.localAgent) {
    return user.localAgent;
  }
  const system = freshSourceWorker(source, "system", now);
  return system?.localAgent || null;
}

function readBoolean(value) {
  return value === true || value === "true" || value === "1";
}

function publicSourceLocalAgent(value) {
  return {
    ok: value?.ok === true,
    version: cleanText(value?.version, 40),
    scope: cleanText(value?.scope, 40),
    companion: value?.companion === true,
    executionPlane: cleanText(value?.executionPlane, 80),
    interactiveTaskBridge: value?.interactiveTaskBridge === true,
    autoUpdate: value?.autoUpdate === true,
    system: value?.system === true,
    sourceWorker: value?.sourceWorker === true
  };
}

function sourceCapabilityBlocker(source, runAs) {
  return sourceWorkerRoute(source, runAs).blocker;
}

function sourceWorkerRoute(source, runAs, now = Date.now()) {
  const plane = safeRunAs(runAs) === "system" ? "system" : "user";
  const worker = freshSourceWorker(source, plane, now);
  if (!worker) {
    if (plane === "user") {
      const systemBridge = workerRoute(source, freshSourceWorker(source, "system", now), "system");
      if (!systemBridge.blocker && systemBridge.worker?.localAgent?.interactiveTaskBridge === true) {
        return {
          ...systemBridge,
          plane: "system",
          requestedPlane: "user",
          via: "interactive-user-bridge"
        };
      }
    }
    return {
      plane,
      blocker: plane === "system" ? "system-plane-unavailable" : "user-session-agent-unavailable"
    };
  }
  return workerRoute(source, worker, plane);
}

function workerRoute(source, worker, plane) {
  if (!worker) {
    return {
      plane,
      blocker: plane === "system" ? "system-plane-unavailable" : "user-session-agent-unavailable"
    };
  }
  const capabilities = new Set(Array.isArray(worker.clientCapabilities) ? worker.clientCapabilities : []);
  const localAgent = worker.localAgent || {};
  if (!capabilities.has("runas") || !capabilities.has("local-agent-health")) {
    return { plane, worker, blocker: "source-client-update-required" };
  }
  if (!capabilities.has("direct-device-worker") || localAgent.sourceWorker !== true) {
    return { plane, worker, blocker: "direct-device-worker-required" };
  }
  if (localAgent.ok !== true) {
    return { plane, worker, blocker: "local-agent-unavailable" };
  }
  if (plane === "system") {
    return localAgent.system === true
      ? { plane, worker, blocker: "" }
      : { plane, worker, blocker: "system-plane-unavailable" };
  }
  if (localAgent.system === true) {
    return { plane, worker, blocker: "user-session-agent-unavailable" };
  }
  return { plane, worker, blocker: "" };
}

function freshSourceWorker(source, plane, now = Date.now()) {
  const worker = source?.workers?.[plane];
  if (!worker?.seenAt || now - worker.seenAt >= sourceConnectedMs) {
    return null;
  }
  return worker;
}

function sourceCapabilityBlockedText(reason) {
  if (reason === "source-client-update-required") {
    return "! source client must refresh before this device can be controlled safely";
  }
  if (reason === "local-agent-unavailable") {
    return "! local Soty Agent is not reachable from the source device";
  }
  if (reason === "direct-device-worker-required") {
    return "! installed Soty Agent must refresh before direct device control is available";
  }
  if (reason === "system-plane-unavailable") {
    return "! this task needs the machine agent, but the source device is not running in system mode";
  }
  if (reason === "user-session-agent-unavailable") {
    return "! user-session Soty Agent companion is not running on the source device";
  }
  return "! source device capability is unavailable";
}

function pollRequesterCanLeaseSourceJobs(value) {
  const capabilities = new Set(cleanCapabilityList(value?.clientCapabilities));
  return capabilities.has("direct-device-worker");
}

function sourcePollRequester(value) {
  const localAgent = localAgentInfoFrom(value) || {};
  return {
    plane: sourceWorkerPlane(localAgent),
    capabilities: cleanCapabilityList(value?.clientCapabilities),
    localAgent
  };
}

function agentSourceDirectWorkerFresh(source, now = Date.now()) {
  return Boolean(source?.directWorkerSeenAt && now - source.directWorkerSeenAt < sourceConnectedMs);
}

function enrichAgentSource(targetRelayId, clientRelayId, source, text = "") {
  const mentionedSource = findMentionedAgentSource(targetRelayId, clientRelayId, text);
  if (!source.deviceId) {
    return mentionedSource ? enrichWithAgentSourceTarget(source, mentionedSource, true) : source;
  }
  const agentSource = mentionedSource
    || findRunnableAgentSource(targetRelayId, source.deviceId)
    || findRunnableAgentSource(clientRelayId, source.deviceId);
  if (!agentSource) {
    return source;
  }
  return enrichWithAgentSourceTarget(source, agentSource, Boolean(mentionedSource), text);
}

function withSourceRelay(source, sourceRelayId) {
  return {
    ...source,
    sourceRelayId: normalizeRelayId(sourceRelayId) || normalizeRelayId(source?.sourceRelayId)
  };
}

function enrichWithAgentSourceTarget(source, agentSource, forcePreferred, text = "") {
  const target = publicAgentSourceTarget(agentSource);
  const existing = Array.isArray(source.operatorTargets) ? source.operatorTargets : [];
  const hasPreferred = preferredTargetMentioned(text, source, existing);
  return {
    ...source,
    preferredTargetId: !forcePreferred && hasPreferred ? source.preferredTargetId : target.id,
    preferredTargetLabel: !forcePreferred && hasPreferred ? source.preferredTargetLabel : target.label,
    operatorTargets: [
      ...existing.filter((item) => item.id !== target.id),
      target
    ]
  };
}

function findMentionedAgentSource(targetRelayId, clientRelayId, text) {
  const prefix = targetPrefix(text);
  if (!prefix) {
    return null;
  }
  const merged = new Map();
  for (const source of [...connectedAgentSources(targetRelayId), ...connectedAgentSources(clientRelayId)]) {
    merged.set(sourceKey(source.relayId, source.deviceId), source);
  }
  return [...merged.values()]
    .sort((left, right) => right.lastSeenAt - left.lastSeenAt)
    .find((source) => agentSourceMatchesPrefix(source, prefix)) || null;
}

function agentSourceMatchesPrefix(source, prefix) {
  const nick = normalizeTargetText(source?.deviceNick);
  const deviceId = normalizeTargetText(source?.deviceId);
  return (nick && (prefix === nick || nick.includes(prefix) || prefix.includes(nick)))
    || (deviceId && prefix === deviceId)
    || (deviceId && prefix === `agent-source:${deviceId}`);
}

function preferredTargetMentioned(text, source, targets) {
  const prefix = targetPrefix(text);
  if (!prefix) {
    return false;
  }
  const preferredId = normalizeTargetText(source.preferredTargetId);
  const preferredLabel = normalizeTargetText(source.preferredTargetLabel);
  if (preferredId && prefix === preferredId) {
    return true;
  }
  if (preferredLabel && (prefix === preferredLabel || preferredLabel.includes(prefix) || prefix.includes(preferredLabel))) {
    return true;
  }
  return targets.some((target) => {
    const id = normalizeTargetText(target?.id);
    const label = normalizeTargetText(target?.label);
    return (id && prefix === id)
      || (label && (prefix === label || label.includes(prefix) || prefix.includes(label)));
  });
}

function targetPrefix(text) {
  const match = /^([^:\n]{1,80})\s*:/u.exec(String(text || "").trim());
  return match ? normalizeTargetText(match[1]) : "";
}

function normalizeTargetText(value) {
  return String(value || "").trim().toLowerCase();
}

function publicAgentSourceTarget(source) {
  return {
    id: agentSourceTargetId(source.deviceId),
    label: source.deviceNick || "Agent device",
    deviceIds: [source.deviceId],
    hostDeviceId: source.deviceId,
    access: true,
    host: true,
    selected: true,
    rank: 0,
    lastActionAt: source.lastSeenAt ? new Date(source.lastSeenAt).toISOString() : ""
  };
}

function createSourceJob(source, payload) {
  touchAgentSource(source);
  const id = cleanSourceJobId(payload.id) || cleanSourceJobId(payload.clientJobId) || `source_${randomUUID()}`;
  const job = {
    ...payload,
    id,
    sourceRelayId: source.relayId,
    sourceDeviceId: source.deviceId,
    createdAt: Date.now(),
    leaseUntil: 0,
    text: ""
  };
  source.jobs.push(job);
  while (source.jobs.length > maxJobsPerChannel) {
    source.jobs.shift();
  }
  flushSourcePollWaiters(source);
  return job;
}

function leasePendingSourceJobs(source, requester = null) {
  const now = Date.now();
  expireQueuedSourceJobs(source, now);
  const cancels = (source.cancels || [])
    .filter((item) => !item.leaseUntil || item.leaseUntil < now)
    .slice(0, 8);
  for (const cancel of cancels) {
    cancel.leaseUntil = now + 15_000;
  }
  const jobs = [];
  for (const job of source.jobs.filter((item) => !Number.isSafeInteger(item.exitCode) && (!item.leaseUntil || item.leaseUntil < now))) {
    const route = sourceWorkerRoute(source, job.runAs || "user", now);
    if (route.blocker) {
      job.exitCode = 409;
      job.text = sourceCapabilityBlockedText(route.blocker);
      flushWaiters(sourceReplyWaiters, job.id, () => sourceJobReply(job));
      continue;
    }
    if (requester?.plane && requester.plane !== route.plane) {
      continue;
    }
    jobs.push(job);
    if (jobs.length >= 2) {
      break;
    }
  }
  for (const job of jobs) {
    job.leasedAt = job.leasedAt || now;
    job.leaseUntil = now + leaseMs;
  }
  return [
    ...cancels.map((item) => ({
      id: item.id,
      type: "cancel",
      commandId: item.commandId,
      createdAt: new Date(item.createdAt).toISOString()
    })),
    ...jobs.map((job) => ({
      id: job.id,
      type: job.type,
      command: job.command || "",
      script: job.script || "",
      name: job.name || "",
      shell: job.shell || "",
      runAs: job.runAs || "user",
      timeoutMs: safeTimeout(job.timeoutMs),
      createdAt: new Date(job.createdAt).toISOString()
    }))
  ];
}

function createSourceJobFromRequest(body) {
  const relayId = normalizeRelayId(body?.relayId);
  const deviceId = cleanText(body?.deviceId, maxSourceChars);
  const type = body?.type === "run" ? "run" : body?.type === "script" ? "script" : body?.script ? "script" : "run";
  const command = cleanText(body?.command, 8_000);
  const script = cleanText(body?.script, 1_000_000);
  const runAs = safeRunAs(body?.runAs);
  const timeoutMs = safeTimeout(body?.timeoutMs);
  if (!relayId || !deviceId || (type === "run" ? !command.trim() : !script.trim())) {
    return {
      ok: false,
      httpStatus: 400,
      payload: { ok: false, text: "! request", exitCode: 400 }
    };
  }
  cleanupAgentSources();
  const source = findRunnableAgentSource(relayId, deviceId);
  if (!source) {
    const diagnostic = agentSourceDiagnostics(relayId, deviceId);
    return {
      ok: false,
      httpStatus: 404,
      payload: {
        ok: false,
        text: agentSourceUnavailableText(diagnostic),
        exitCode: 404,
        diagnostic
      }
    };
  }
  const blocker = sourceCapabilityBlocker(source, runAs);
  if (blocker) {
    return {
      ok: false,
      httpStatus: 409,
      payload: {
        ok: false,
        text: sourceCapabilityBlockedText(blocker),
        exitCode: 409,
        diagnostic: agentSourceDiagnostics(relayId, deviceId)
      }
    };
  }
  const job = createSourceJob(source, {
    type,
    id: cleanSourceJobId(body?.clientJobId || body?.id),
    ...(type === "run" ? { command } : {
      script,
      name: cleanText(body?.name, 120) || "script",
      shell: cleanText(body?.shell, 40)
    }),
    runAs,
    timeoutMs
  });
  return { ok: true, httpStatus: 200, job };
}

function sourceJobStarted(job) {
  return {
    ok: true,
    id: job.id,
    status: "created",
    text: "",
    diagnostic: sourceJobDiagnostic(job)
  };
}

function sourceJobStatus(job, now = Date.now()) {
  const finished = Number.isSafeInteger(job.exitCode);
  const exitCode = finished ? job.exitCode : undefined;
  const status = finished
    ? exitCode === 0
      ? "ok"
      : exitCode === 130
        ? "cancelled"
        : exitCode === 124
          ? "timeout"
          : "failed"
    : job.leaseUntil
      ? "running"
      : "queued";
  return {
    ok: finished ? exitCode === 0 : true,
    id: job.id,
    status,
    text: job.text || "",
    ...(finished ? { exitCode } : {}),
    diagnostic: sourceJobDiagnostic(job, now)
  };
}

function sourceJobReply(job) {
  const exitCode = Number.isSafeInteger(job.exitCode) ? job.exitCode : 124;
  return {
    ok: exitCode === 0,
    text: job.text || (exitCode === 124 ? "! timeout" : ""),
    exitCode,
    ...(exitCode === 0 ? {} : { diagnostic: sourceJobDiagnostic(job) })
  };
}

function sourceJobDiagnostic(job, now = Date.now()) {
  const source = findAgentSource(job.sourceRelayId, job.sourceDeviceId);
  const leased = Boolean(job.leaseUntil);
  const finished = Number.isSafeInteger(job.exitCode);
  const reason = finished
    ? (job.exitCode === 0 ? "ok" : "nonzero-exit")
    : leased
      ? "running-on-device"
      : "waiting-for-device-pickup";
  return {
    reason,
    job: publicSourceJobDiagnostic(job, now),
    source: source ? publicAgentSourceDiagnostic(source, now) : null
  };
}

function expireQueuedSourceJobs(source, now = Date.now()) {
  for (const job of source.jobs) {
    if (Number.isSafeInteger(job.exitCode) || job.leaseUntil) {
      continue;
    }
    if (now - job.createdAt <= sourceJobPickupTimeoutMs(job)) {
      continue;
    }
    job.exitCode = 124;
    job.text = `${job.text || ""}${job.text ? "\n" : ""}! pickup timeout\n`.slice(-maxReplyChars);
    flushWaiters(sourceReplyWaiters, job.id, () => sourceJobReply(job));
  }
}

function sourceJobPickupTimeoutMs(job) {
  const timeoutMs = safeTimeout(job?.timeoutMs);
  return Math.max(sourceJobPickupBaseMs, Math.min(10 * 60_000, timeoutMs + sourceJobPickupBaseMs));
}

function flushSourcePollWaiters(source) {
  flushWaiters(sourcePollWaiters, source.key);
}

function cleanupAgentSources() {
  const now = Date.now();
  for (const [key, source] of agentSources) {
    expireQueuedSourceJobs(source, now);
    source.jobs = source.jobs.filter((job) => now - job.createdAt < sourceJobTtlMs);
    source.cancels = (source.cancels || []).filter((item) => now - item.createdAt < sourceCancelTtlMs);
    if (now - source.lastSeenAt > idleChannelTtlMs && source.jobs.length === 0 && source.cancels.length === 0) {
      agentSources.delete(key);
    }
  }
}

function sourceKey(relayId, deviceId) {
  return `${relayId}:${deviceId}`;
}

function cleanSourceJobId(value) {
  const text = cleanText(value, 120);
  return /^[A-Za-z0-9_.:-]{8,120}$/u.test(text) ? text : "";
}

function agentSourceTargetId(deviceId) {
  return `agent-source:${deviceId}`;
}

function cleanAgentSource(value) {
  if (!value || typeof value !== "object") {
    return {};
  }
  return {
    tunnelId: cleanText(value.tunnelId, maxSourceChars),
    tunnelLabel: cleanText(value.tunnelLabel, maxSourceChars),
    deviceId: cleanText(value.deviceId, maxSourceChars),
    deviceNick: cleanText(value.deviceNick, maxSourceChars),
    appOrigin: cleanText(value.appOrigin, maxSourceChars),
    sourceRelayId: normalizeRelayId(value.sourceRelayId),
    preferredTargetId: cleanText(value.preferredTargetId, maxSourceChars),
    preferredTargetLabel: cleanText(value.preferredTargetLabel, maxSourceChars),
    localAgentDirect: value.localAgentDirect === true,
    operatorTargets: cleanOperatorTargets(value.operatorTargets)
  };
}

function cleanOperatorTargets(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => ({
      id: cleanText(item?.id, maxSourceChars),
      label: cleanText(item?.label, maxSourceChars),
      deviceIds: Array.isArray(item?.deviceIds)
        ? [...new Set(item.deviceIds.map((entry) => cleanText(entry, maxSourceChars)).filter(Boolean))].slice(0, 16)
        : [],
      hostDeviceId: cleanText(item?.hostDeviceId, maxSourceChars),
      access: typeof item?.access === "boolean" ? item.access : undefined,
      host: typeof item?.host === "boolean" ? item.host : undefined,
      selected: typeof item?.selected === "boolean" ? item.selected : undefined,
      rank: Number.isSafeInteger(item?.rank) ? Math.max(1, Math.min(item.rank, 999)) : undefined,
      lastActionAt: cleanText(item?.lastActionAt, 80)
    }))
    .filter((item) => item.id && item.label)
    .slice(0, 32);
}

function findReply(relayId, id) {
  const job = channels.get(relayId)?.jobs.find((item) => item.id === id)
    || Array.from(channels.values())
      .flatMap((channel) => channel.jobs)
      .find((item) => item.id === id && item.clientRelayId === relayId);
  return job?.reply || null;
}

function addWaiter(map, key, req, res, buildPayload, timeoutMs = 30_000) {
  const waiter = {
    res,
    buildPayload,
    timer: setTimeout(() => {
      removeWaiter(map, key, waiter);
      res.json(buildPayload());
    }, Math.max(1000, timeoutMs))
  };
  res.on("close", () => {
    if (res.writableEnded) {
      return;
    }
    clearTimeout(waiter.timer);
    removeWaiter(map, key, waiter);
  });
  const waiters = map.get(key) || new Set();
  waiters.add(waiter);
  map.set(key, waiters);
}

function removeWaiter(map, key, waiter) {
  const waiters = map.get(key);
  if (!waiters) {
    return;
  }
  waiters.delete(waiter);
  if (waiters.size === 0) {
    map.delete(key);
  }
}

function flushPollWaiters(relayId) {
  const channel = channels.get(relayId);
  if (!channel) {
    return;
  }
  flushWaiters(pollWaiters, relayId, () => ({ ok: true, jobs: leasePendingJobs(channel) }));
}

function flushReplyWaiters(relayId, id) {
  flushWaiters(replyWaiters, replyKey(relayId, id), () => ({ ok: true, reply: findReply(relayId, id) }));
}

function flushEventWaiters(relayId, id) {
  flushWaiters(eventWaiters, replyKey(relayId, id), () => relayEventsPayload(relayId, id, 0));
}

function flushWaiters(map, key, buildPayload = null) {
  const waiters = map.get(key);
  if (!waiters) {
    return;
  }
  map.delete(key);
  for (const waiter of waiters) {
    clearTimeout(waiter.timer);
    waiter.res.json((buildPayload || waiter.buildPayload)());
  }
}

function cleanupChannels() {
  const now = Date.now();
  for (const [relayId, channel] of channels) {
    channel.jobs = channel.jobs.filter((job) => now - job.createdAt < requestTtlMs);
    const idle = now - Math.max(channel.touchedAt || 0, channel.lastPollAt || 0) > idleChannelTtlMs;
    if (idle && channel.jobs.length === 0) {
      channels.delete(relayId);
    }
  }
  cleanupAgentSources();
}

function replyKey(relayId, id) {
  return `${relayId}:${id}`;
}

function relayEventsPayload(relayId, id, after) {
  const found = findRelayJob(relayId, id);
  if (!found) {
    return { ok: true, events: [], done: true };
  }
  const events = (found.job.events || [])
    .filter((event) => Number.isSafeInteger(event.seq) && event.seq > after)
    .map((event) => ({
      seq: event.seq,
      type: event.type || "agent_message",
      text: event.text || "",
      createdAt: event.createdAt || ""
    }));
  return { ok: true, events, done: Boolean(found.job.reply) };
}

function findRelayJob(relayId, id) {
  const direct = channels.get(relayId);
  const directJob = direct?.jobs.find((job) => job.id === id);
  if (directJob) {
    return { relayId, job: directJob };
  }
  for (const [channelRelayId, channel] of channels) {
    const job = channel.jobs.find((item) => item.id === id && item.clientRelayId === relayId);
    if (job) {
      return { relayId: channelRelayId, job };
    }
  }
  return null;
}

function normalizeRelayId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{32,192}$/u.test(text) ? text : "";
}

function cleanText(value, max) {
  return typeof value === "string" ? value.slice(0, max) : "";
}

function safeRunAs(value) {
  const text = String(value || "").trim().toLowerCase();
  return text === "system" || text === "machine" || text === "elevated" ? "system" : "user";
}

function cleanReplyMessages(value) {
  if (!Array.isArray(value)) {
    return {};
  }
  const messages = value
    .filter((item) => typeof item === "string")
    .map((item) => item.replace(/\r\n?/gu, "\n").trim().slice(0, maxReplyChars))
    .filter(Boolean)
    .slice(0, maxReplyMessages);
  return messages.length > 0 ? { messages } : {};
}

function cleanReplyTerminal(value) {
  if (!Array.isArray(value)) {
    return {};
  }
  const terminal = value
    .filter((item) => typeof item === "string")
    .map((item) => item.replace(/\r\n?/gu, "\n").trim().slice(0, maxReplyChars))
    .filter(Boolean)
    .slice(0, maxReplyMessages);
  return terminal.length > 0 ? { terminal } : {};
}

function safeTimeout(value) {
  return Number.isSafeInteger(value) ? Math.max(1000, Math.min(value, maxTaskTimeoutMs)) : defaultSourceTimeoutMs;
}

function sourceReplyWaitTimeout(value) {
  const timeout = safeTimeout(value);
  const grace = timeout <= 60_000
    ? 3_000
    : Math.min(30_000, Math.max(5_000, Math.round(timeout * 0.05)));
  return Math.min(maxTaskTimeoutMs + 60_000, timeout + grace);
}
