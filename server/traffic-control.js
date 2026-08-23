import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";
import express from "express";

const execFileAsync = promisify(execFile);
const stateSchema = "soty.traffic-control.v1";
const inboundTag = "soty-vless";
const compatibilityInboundTag = "soty-vless-ws";
const bodyLimit = "64kb";
const jsonParser = express.json({ limit: bodyLimit, type: "application/json" });

export function attachTrafficControl(app, { dataDir, isRelayConnected } = {}) {
  const apiServer = safeApiServer(process.env.SOTY_TRAFFIC_XRAY_API || "");
  if (!apiServer) {
    return { enabled: false };
  }

  const statePath = path.join(dataDir || "/data", "traffic-control.json");
  const poolPath = process.env.SOTY_TRAFFIC_EXIT_POOL || path.join(dataDir || "/data", "traffic-exit-pool.json");
  let queue = Promise.resolve();

  const reconcile = () => {
    const run = queue.then(async () => await reconcileTrafficState(apiServer, statePath));
    queue = run.catch(() => undefined);
  };
  const initialReconcile = setTimeout(reconcile, 2_000);
  initialReconcile.unref?.();
  const reconcileTimer = setInterval(reconcile, 30_000);
  reconcileTimer.unref?.();

  app.post("/api/traffic/exit", jsonParser, (req, res) => {
    runExclusive(async () => {
      const relayId = safeRelayId(req.body?.relayId);
      if (!relayId || !isRelayConnected?.(relayId)) return responseError(res, 409, "agent-not-connected");
      const state = await readState(statePath);
      const existing = state.exits.find((item) => item.relayId === relayId && !item.revokedAt);
      if (existing) return res.json({ ok: true, schema: stateSchema, exit: publicExit(existing), bridge: bridgeSettings(existing) });

      const pool = await readExitPool(poolPath);
      const usedSlots = new Set(state.exits.filter((item) => !item.revokedAt).map((item) => item.slotId));
      const slot = pool.slots.find((item) => !usedSlots.has(item.id));
      if (!slot) return responseError(res, 503, "exit-capacity-exhausted");
      const id = `exit_${randomUUID().replace(/-/gu, "")}`;
      const exit = {
        id,
        slotId: slot.id,
        relayId,
        label: safeText(req.body?.label, 80) || "Этот компьютер",
        bridgeId: slot.bridgeId,
        bridgeEmail: slot.email,
        reverseTag: slot.reverseTag,
        createdAt: new Date().toISOString(),
        revokedAt: ""
      };
      state.exits.push(exit);
      await writeState(statePath, state);
      res.status(201).json({ ok: true, schema: stateSchema, exit: publicExit(exit), bridge: bridgeSettings(exit) });
    }, res);
  });

  app.post("/api/traffic/client", jsonParser, (req, res) => {
    runExclusive(async () => {
      const relayId = safeRelayId(req.body?.relayId);
      if (!relayId || !isRelayConnected?.(relayId)) return responseError(res, 409, "agent-not-connected");
      const state = await readState(statePath);
      const exit = state.exits.find((item) => item.id === safeId(req.body?.exitId) && item.relayId === relayId && !item.revokedAt);
      if (!exit) return responseError(res, 404, "exit-not-found");

      const id = `client_${randomUUID().replace(/-/gu, "")}`;
      const client = {
        id,
        exitId: exit.id,
        relayId,
        label: safeText(req.body?.label, 80) || "Новое устройство",
        platform: safeText(req.body?.platform, 30) || "other",
        credential: randomUUID(),
        email: `${id}@client.soty`,
        ruleTag: `soty-route-${id.slice(7)}`,
        createdAt: new Date().toISOString(),
        revokedAt: ""
      };
      await addInboundUsers(apiServer, [clientUser(client)]);
      try {
        await addRoutingRules(apiServer, [clientRule(client, exit)]);
      } catch (error) {
        await removeInboundUser(apiServer, client.email).catch(() => undefined);
        throw error;
      }
      state.clients.push(client);
      await writeState(statePath, state);
      const profiles = clientProfiles(client);
      res.status(201).json({ ok: true, schema: stateSchema, client: publicClient(client), profile: profiles.compatible, profiles });
    }, res);
  });

  app.post("/api/traffic/revoke", jsonParser, (req, res) => {
    runExclusive(async () => {
      const relayId = safeRelayId(req.body?.relayId);
      if (!relayId || !isRelayConnected?.(relayId)) return responseError(res, 409, "agent-not-connected");
      const state = await readState(statePath);
      const client = state.clients.find((item) => item.id === safeId(req.body?.clientId) && item.relayId === relayId && !item.revokedAt);
      if (!client) return responseError(res, 404, "client-not-found");
      await removeRoutingRule(apiServer, client.ruleTag);
      await removeInboundUser(apiServer, client.email);
      client.revokedAt = new Date().toISOString();
      client.credential = "";
      await writeState(statePath, state);
      res.json({ ok: true, schema: stateSchema, client: publicClient(client) });
    }, res);
  });

  app.get("/api/traffic/status", (req, res) => {
    runExclusive(async () => {
      const relayId = safeRelayId(req.query.relayId);
      if (!relayId || !isRelayConnected?.(relayId)) return responseError(res, 409, "agent-not-connected");
      const state = await readState(statePath);
      const exits = state.exits.filter((item) => item.relayId === relayId && !item.revokedAt);
      const outboundTags = await listOutboundTags(apiServer);
      res.json({
        ok: true,
        schema: stateSchema,
        exits: exits.map((item) => ({ ...publicExit(item), online: outboundTags.has(item.reverseTag) })),
        clients: state.clients.filter((item) => item.relayId === relayId).map(publicClient)
      });
    }, res);
  });

  function runExclusive(task, res) {
    const run = queue.then(task, task);
    queue = run.catch(() => undefined);
    void run.catch((error) => responseError(res, 502, safeOperationError(error)));
  }

  return { enabled: true, apiServer };
}

async function addInboundUsers(apiServer, users) {
  const completed = [];
  try {
    await addInboundUsersToTag(apiServer, inboundTag, 24444, users);
    completed.push(inboundTag);
    await addInboundUsersToTag(apiServer, compatibilityInboundTag, 24446, users);
    completed.push(compatibilityInboundTag);
  } catch (error) {
    for (const tag of completed) {
      for (const user of users) await removeInboundUserFromTag(apiServer, tag, user.email).catch(() => undefined);
    }
    throw error;
  }
}

async function addInboundUsersToTag(apiServer, tag, port, users) {
  const inbound = { tag, listen: "0.0.0.0", port, protocol: "vless", settings: { users, decryption: "none" } };
  await withConfig({ inbounds: [inbound] }, async (file) => {
    const output = await runXray(["api", "adu", `--server=${apiServer}`, file]);
    if (!String(output).includes(`Added ${users.length} user(s) in total.`)) {
      throw new Error(`xray-users-not-added:${tag}`);
    }
  });
}

async function addRoutingRules(apiServer, rules) {
  await withConfig({ routing: { rules } }, async (file) => {
    await runXray(["api", "adrules", `--server=${apiServer}`, "-append", file]);
  });
}

async function removeInboundUser(apiServer, email) {
  await removeInboundUserFromTag(apiServer, inboundTag, email);
  await removeInboundUserFromTag(apiServer, compatibilityInboundTag, email);
}

async function removeInboundUserFromTag(apiServer, tag, email) {
  try {
    await runXray(["api", "rmu", `--server=${apiServer}`, `-tag=${tag}`, email]);
  } catch (error) {
    if (!isNotFoundError(error)) throw error;
  }
}

async function removeRoutingRule(apiServer, ruleTag) {
  try {
    await runXray(["api", "rmrules", `--server=${apiServer}`, ruleTag]);
  } catch (error) {
    if (!isNotFoundError(error)) throw error;
  }
}

async function reconcileTrafficState(apiServer, statePath) {
  const state = await readState(statePath);
  const exits = state.exits.filter((item) => !item.revokedAt);
  const clients = state.clients.filter((item) => !item.revokedAt && item.credential);
  if (exits.length === 0 && clients.length === 0) return;
  const existingUsers = await listInboundUsers(apiServer);
  const missingUsers = clients.map(clientUser).filter((user) => !existingUsers.has(user.email));
  if (missingUsers.length > 0) await addInboundUsersToTag(apiServer, inboundTag, 24444, missingUsers);
  const existingCompatibilityUsers = await listInboundUsers(apiServer, compatibilityInboundTag);
  const missingCompatibilityUsers = clients.map(clientUser).filter((user) => !existingCompatibilityUsers.has(user.email));
  if (missingCompatibilityUsers.length > 0) await addInboundUsersToTag(apiServer, compatibilityInboundTag, 24446, missingCompatibilityUsers);
  const existingRules = await listRoutingRules(apiServer);
  const exitById = new Map(exits.map((exit) => [exit.id, exit]));
  const missingRules = clients
    .map((client) => ({ client, exit: exitById.get(client.exitId) }))
    .filter((item) => item.exit && !existingRules.has(item.client.ruleTag))
    .map((item) => clientRule(item.client, item.exit));
  if (missingRules.length > 0) await addRoutingRules(apiServer, missingRules);
}

async function listInboundUsers(apiServer, tag = inboundTag) {
  const output = await runXray(["api", "inbounduser", `--server=${apiServer}`, `-tag=${tag}`]);
  const parsed = JSON.parse(output || "{}");
  return new Set((Array.isArray(parsed.users) ? parsed.users : []).map((user) => String(user?.email || "")).filter(Boolean));
}

async function listRoutingRules(apiServer) {
  const output = await runXray(["api", "lsrules", `--server=${apiServer}`]);
  const parsed = JSON.parse(output || "{}");
  return new Set((Array.isArray(parsed.rules) ? parsed.rules : []).map((rule) => String(rule?.tag || "")).filter(Boolean));
}

async function listOutboundTags(apiServer) {
  const output = await runXray(["api", "lso", `--server=${apiServer}`]);
  const parsed = JSON.parse(output || "{}");
  return new Set((Array.isArray(parsed.outbounds) ? parsed.outbounds : []).map((item) => String(item?.tag || "")).filter(Boolean));
}

async function runXray(args) {
  const binary = process.env.SOTY_TRAFFIC_XRAY_CLI || "xray";
  const { stdout } = await execFileAsync(binary, args, { timeout: 15_000, windowsHide: true, maxBuffer: 2_000_000 });
  return stdout;
}

async function withConfig(config, callback) {
  const dir = process.env.SOTY_TRAFFIC_CONTROL_TMP || "/tmp";
  await mkdir(dir, { recursive: true });
  const file = path.join(dir, `soty-traffic-${randomUUID()}.json`);
  try {
    await writeFile(file, `${JSON.stringify(config)}\n`, { mode: 0o600 });
    return await callback(file);
  } finally {
    await rm(file, { force: true }).catch(() => undefined);
  }
}

async function readState(statePath) {
  try {
    const parsed = JSON.parse(await readFile(statePath, "utf8"));
    return normalizeState(parsed);
  } catch {
    return normalizeState({});
  }
}

async function readExitPool(poolPath) {
  const parsed = JSON.parse(await readFile(poolPath, "utf8"));
  const slots = (Array.isArray(parsed?.slots) ? parsed.slots : []).map((item) => ({
    id: safeId(item?.id),
    bridgeId: safeUuid(item?.bridgeId),
    email: safeText(item?.email, 120),
    reverseTag: safeId(item?.reverseTag)
  })).filter((item) => item.id && item.bridgeId && item.email && item.reverseTag);
  if (slots.length === 0) throw new Error("exit-pool-unavailable");
  return { slots };
}

async function writeState(statePath, state) {
  await mkdir(path.dirname(statePath), { recursive: true });
  const next = `${statePath}.next`;
  await writeFile(next, `${JSON.stringify(normalizeState(state), null, 2)}\n`, { mode: 0o600 });
  await chmod(next, 0o600).catch(() => undefined);
  await rename(next, statePath);
}

function normalizeState(value) {
  return {
    schema: stateSchema,
    exits: Array.isArray(value?.exits) ? value.exits.filter((item) => item && typeof item === "object") : [],
    clients: Array.isArray(value?.clients) ? value.clients.filter((item) => item && typeof item === "object") : []
  };
}

function clientUser(client) {
  return { id: client.credential, email: client.email };
}

function clientRule(client, exit) {
  return { ruleTag: client.ruleTag, type: "field", user: [client.email], outboundTag: exit.reverseTag };
}

function bridgeSettings(exit) {
  return {
    gatewayHost: "pochinit.online",
    gatewaySni: "pochinit.online",
    gatewayHttpHost: "xn--n1afe0b.online",
    gatewayPort: 443,
    gatewayPath: "/api/traffic/tunnel",
    bridgeId: exit.bridgeId,
    reverseTag: exit.reverseTag,
    tls: true
  };
}

function clientProfiles(client) {
  const address = "pochinit.online";
  const httpHost = "xn--n1afe0b.online";
  const common = { encryption: "none", security: "tls", sni: address, fp: "chrome", host: httpHost };
  const modern = new URLSearchParams({ ...common, alpn: "h2", type: "xhttp", path: "/api/traffic/tunnel", mode: "auto" });
  const compatible = new URLSearchParams({ ...common, alpn: "http/1.1", type: "ws", path: "/api/traffic/ws" });
  const name = encodeURIComponent(client.label || "Интернет через Соты");
  const base = `vless://${client.credential}@${address}:443`;
  return { modern: `${base}?${modern.toString()}#${name}`, compatible: `${base}?${compatible.toString()}#${name}` };
}

function publicExit(exit) {
  return { id: exit.id, label: exit.label, createdAt: exit.createdAt, revokedAt: exit.revokedAt || "" };
}

function publicClient(client) {
  return { id: client.id, exitId: client.exitId, label: client.label, platform: client.platform, createdAt: client.createdAt, revokedAt: client.revokedAt || "" };
}

function responseError(res, status, error) {
  if (!res.headersSent) res.status(status).json({ ok: false, schema: stateSchema, error });
}

function safeApiServer(value) {
  const text = String(value || "").trim();
  return /^(?:127\.0\.0\.1|172\.17\.0\.1|[a-zA-Z0-9.-]+):\d{2,5}$/u.test(text) ? text : "";
}

function safeRelayId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{20,160}$/u.test(text) ? text : "";
}

function safeId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u.test(text) ? text : "";
}

function safeUuid(value) {
  const text = String(value || "").trim().toLowerCase();
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(text) ? text : "";
}

function safeText(value, max) {
  return typeof value === "string" ? value.trim().replace(/[\r\n\t]+/gu, " ").slice(0, max) : "";
}

function safeOperationError(error) {
  const text = String(error instanceof Error ? error.message : error || "traffic-control-error");
  return text.replace(/[\r\n]+/gu, " ").replace(/[0-9a-f]{8}-[0-9a-f-]{27,}/giu, "<id>").slice(0, 220);
}

function isNotFoundError(error) {
  return /not found|not exist|code = NotFound/iu.test(String(error?.stderr || error?.message || error));
}
