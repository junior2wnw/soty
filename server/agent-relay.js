import express from "express";
import { randomUUID } from "node:crypto";

const maxChatChars = 12_000;
const maxContextChars = 16_000;
const maxReplyChars = 12_000;
const maxSourceChars = 180;
const leaseMs = 180_000;
const connectedMs = 70_000;
const requestTtlMs = 15 * 60_000;
const idleChannelTtlMs = 30 * 60_000;
const maxJobsPerChannel = 80;
const channels = new Map();
const pollWaiters = new Map();
const replyWaiters = new Map();
const jsonParser = express.json({ limit: "180kb", type: "application/json" });

export function attachAgentRelay(app) {
  app.get("/api/agent/relay/current", (_req, res) => {
    cleanupChannels();
    const now = Date.now();
    const current = Array.from(channels.entries())
      .filter(([, channel]) => channel.codex === true && now - (channel.lastPollAt || 0) < connectedMs)
      .sort((left, right) => (right[1].lastPollAt || 0) - (left[1].lastPollAt || 0))[0];
    if (!current) {
      res.json({ ok: true, connected: false, relayId: "", lastSeenAt: "", version: "" });
      return;
    }
    const [relayId, channel] = current;
    res.json({
      ok: true,
      connected: true,
      relayId,
      lastSeenAt: new Date(channel.lastPollAt).toISOString(),
      version: channel.agentVersion || "",
      codex: true,
      deviceId: channel.deviceId || "",
      deviceNick: channel.deviceNick || ""
    });
  });

  app.get("/api/agent/relay/status", (req, res) => {
    const relayId = normalizeRelayId(req.query.relayId);
    if (!relayId) {
      res.status(400).json({ ok: false });
      return;
    }
    cleanupChannels();
    const channel = channels.get(relayId);
    const lastPollAt = channel?.lastPollAt || 0;
    res.json({
      ok: true,
      connected: Date.now() - lastPollAt < connectedMs,
      lastSeenAt: lastPollAt ? new Date(lastPollAt).toISOString() : "",
      version: channel?.agentVersion || "",
      codex: channel?.codex === true,
      deviceId: channel?.deviceId || "",
      deviceNick: channel?.deviceNick || ""
    });
  });

  app.post("/api/agent/relay/request", jsonParser, (req, res) => {
    const relayId = normalizeRelayId(req.body?.relayId);
    const text = cleanText(req.body?.text, maxChatChars);
    const context = cleanText(req.body?.context, maxContextChars);
    const source = cleanAgentSource(req.body?.source);
    if (!relayId || !text.trim()) {
      res.status(400).json({ ok: false });
      return;
    }
    cleanupChannels();
    const target = resolveRequestChannel(relayId);
    const channel = getChannel(target.relayId);
    const job = {
      id: `agent_${randomUUID()}`,
      text,
      context,
      source,
      createdAt: Date.now(),
      leaseUntil: 0,
      reply: null,
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
      ...(Number.isSafeInteger(req.body?.exitCode) ? { exitCode: req.body.exitCode } : {})
    };
    job.leaseUntil = 0;
    flushReplyWaiters(relayId, id);
    if (job.clientRelayId) {
      flushReplyWaiters(job.clientRelayId, id);
    }
    res.json({ ok: true });
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

function resolveRequestChannel(relayId) {
  const requested = channels.get(relayId);
  const requestedConnected = Date.now() - (requested?.lastPollAt || 0) < connectedMs;
  if (requested?.codex === true && requestedConnected) {
    return { relayId, clientRelayId: "" };
  }
  const current = currentCodexEntry();
  if (!current || current[0] === relayId) {
    return { relayId, clientRelayId: "" };
  }
  return { relayId: current[0], clientRelayId: relayId };
}

function currentCodexEntry() {
  const now = Date.now();
  return Array.from(channels.entries())
    .filter(([, channel]) => channel.codex === true && now - (channel.lastPollAt || 0) < connectedMs)
    .sort((left, right) => (right[1].lastPollAt || 0) - (left[1].lastPollAt || 0))[0] || null;
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

function addWaiter(map, key, req, res, buildPayload) {
  const waiter = {
    res,
    timer: setTimeout(() => {
      removeWaiter(map, key, waiter);
      res.json(buildPayload());
    }, 30_000)
  };
  req.on("close", () => {
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
}

function replyKey(relayId, id) {
  return `${relayId}:${id}`;
}

function normalizeRelayId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{32,192}$/u.test(text) ? text : "";
}

function cleanText(value, max) {
  return typeof value === "string" ? value.slice(0, max) : "";
}
