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
const sourceJobTtlMs = maxTaskTimeoutMs + 20 * 60_000;
const sourceCancelTtlMs = 5 * 60_000;
const maxJobsPerChannel = 80;
const channels = new Map();
const agentSources = new Map();
const pollWaiters = new Map();
const replyWaiters = new Map();
const eventWaiters = new Map();
const sourcePollWaiters = new Map();
const sourceReplyWaiters = new Map();
const jsonParser = express.json({ limit: "180kb", type: "application/json" });
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

  app.get("/api/agent/source/poll", (req, res) => {
    const relayId = normalizeRelayId(req.query.relayId);
    const deviceId = cleanText(req.query.deviceId, maxSourceChars);
    if (!relayId || !deviceId) {
      res.status(400).json({ ok: false, jobs: [] });
      return;
    }
    const source = getAgentSource(relayId, deviceId);
    touchAgentSource(source);
    const jobs = leasePendingSourceJobs(source);
    if (jobs.length > 0 || req.query.wait !== "1") {
      res.json({ ok: true, jobs });
      return;
    }
    addWaiter(sourcePollWaiters, source.key, req, res, () => ({ ok: true, jobs: leasePendingSourceJobs(source) }));
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

  app.post("/api/agent/source/run", jsonParser, (req, res) => {
    const relayId = normalizeRelayId(req.body?.relayId);
    const deviceId = cleanText(req.body?.deviceId, maxSourceChars);
    const command = cleanText(req.body?.command, 8_000);
    const timeoutMs = safeTimeout(req.body?.timeoutMs);
    const source = findRunnableAgentSource(relayId, deviceId);
    if (!relayId || !deviceId || !command.trim()) {
      res.status(400).json({ ok: false, text: "! request", exitCode: 400 });
      return;
    }
    if (!source) {
      res.status(404).json({ ok: false, text: "! agent-source", exitCode: 404 });
      return;
    }
    const job = createSourceJob(source, {
      type: "run",
      id: cleanSourceJobId(req.body?.clientJobId || req.body?.id),
      command,
      timeoutMs
    });
    addWaiter(sourceReplyWaiters, job.id, req, res, () => sourceJobReply(job), sourceReplyWaitTimeout(timeoutMs));
  });

  app.post("/api/agent/source/script", jsonParser, (req, res) => {
    const relayId = normalizeRelayId(req.body?.relayId);
    const deviceId = cleanText(req.body?.deviceId, maxSourceChars);
    const script = cleanText(req.body?.script, 1_000_000);
    const timeoutMs = safeTimeout(req.body?.timeoutMs);
    const source = findRunnableAgentSource(relayId, deviceId);
    if (!relayId || !deviceId || !script.trim()) {
      res.status(400).json({ ok: false, text: "! request", exitCode: 400 });
      return;
    }
    if (!source) {
      res.status(404).json({ ok: false, text: "! agent-source", exitCode: 404 });
      return;
    }
    const job = createSourceJob(source, {
      type: "script",
      id: cleanSourceJobId(req.body?.clientJobId || req.body?.id),
      script,
      name: cleanText(req.body?.name, 120) || "script",
      shell: cleanText(req.body?.shell, 40),
      timeoutMs
    });
    addWaiter(sourceReplyWaiters, job.id, req, res, () => sourceJobReply(job), sourceReplyWaitTimeout(timeoutMs));
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
  return source && source.access === true && Date.now() - source.lastSeenAt < sourceConnectedMs ? source : null;
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

function leasePendingSourceJobs(source) {
  const now = Date.now();
  const cancels = (source.cancels || [])
    .filter((item) => !item.leaseUntil || item.leaseUntil < now)
    .slice(0, 8);
  for (const cancel of cancels) {
    cancel.leaseUntil = now + 15_000;
  }
  const jobs = source.jobs
    .filter((job) => !Number.isSafeInteger(job.exitCode) && (!job.leaseUntil || job.leaseUntil < now))
    .slice(0, 2);
  for (const job of jobs) {
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
      timeoutMs: safeTimeout(job.timeoutMs),
      createdAt: new Date(job.createdAt).toISOString()
    }))
  ];
}

function sourceJobReply(job) {
  const exitCode = Number.isSafeInteger(job.exitCode) ? job.exitCode : 124;
  return {
    ok: exitCode === 0,
    text: job.text || (exitCode === 124 ? "! timeout" : ""),
    exitCode
  };
}

function flushSourcePollWaiters(source) {
  flushWaiters(sourcePollWaiters, source.key, () => ({ ok: true, jobs: leasePendingSourceJobs(source) }));
}

function cleanupAgentSources() {
  const now = Date.now();
  for (const [key, source] of agentSources) {
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

function flushWaiters(map, key, buildPayload) {
  const waiters = map.get(key);
  if (!waiters) {
    return;
  }
  map.delete(key);
  for (const waiter of waiters) {
    clearTimeout(waiter.timer);
    waiter.res.json(buildPayload());
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
  return Math.min(maxTaskTimeoutMs + 60_000, safeTimeout(value) + 30_000);
}
