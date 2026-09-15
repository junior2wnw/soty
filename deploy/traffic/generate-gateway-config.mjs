#!/usr/bin/env node
import { randomUUID } from "node:crypto";
import { chmod, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

const count = boundedCount(arg("--count") || "64");
const poolPath = resolve(arg("--pool") || "traffic-exit-pool.json");
const configPath = resolve(arg("--config") || "server.json");
const legacyConfigPath = arg("--legacy-config") ? resolve(arg("--legacy-config")) : "";
const existing = await readPool(poolPath);
const legacy = legacyConfigPath ? await readLegacyConfig(legacyConfigPath) : { users: [], rules: [] };
const slots = [...existing];
while (slots.length < count) {
  const index = slots.length + 1;
  slots.push({
    id: `slot_${String(index).padStart(3, "0")}`,
    bridgeId: randomUUID(),
    email: `slot-${String(index).padStart(3, "0")}@bridge.soty`,
    reverseTag: `soty-reverse-slot-${String(index).padStart(3, "0")}`
  });
}
slots.length = count;

const pool = { schema: "soty.traffic-exit-pool.v1", generatedAt: new Date().toISOString(), slots };
const config = {
  log: { loglevel: "warning" },
  api: {
    tag: "api",
    listen: "0.0.0.0:24445",
    services: ["HandlerService", "RoutingService", "StatsService"]
  },
  inbounds: [
    {
      tag: "soty-vless",
      listen: "0.0.0.0",
      port: 24444,
      protocol: "vless",
      settings: {
        decryption: "none",
        users: [
          ...slots.map((slot) => ({ id: slot.bridgeId, email: slot.email, reverse: { tag: slot.reverseTag } })),
          ...legacy.users
        ]
      },
      streamSettings: { network: "xhttp", security: "none", xhttpSettings: { path: "/inside", mode: "auto" } },
      sniffing: { enabled: true, destOverride: ["http", "tls", "quic"] }
    },
    {
      tag: "soty-vless-ws",
      listen: "0.0.0.0",
      port: 24446,
      protocol: "vless",
      settings: { decryption: "none", users: legacy.users },
      streamSettings: { network: "ws", security: "none", wsSettings: { path: "/inside-ws" } },
      sniffing: { enabled: true, destOverride: ["http", "tls", "quic"] }
    }
  ],
  routing: { domainStrategy: "IPIfNonMatch", rules: legacy.rules },
  outbounds: [{ tag: "unmatched-block", protocol: "blackhole" }]
};

await atomicJson(poolPath, pool, 0o600);
await atomicJson(configPath, config, 0o644);
process.stdout.write(`traffic gateway config: slots=${slots.length} config=${configPath}\n`);

async function readPool(path) {
  try {
    const parsed = JSON.parse(await readFile(path, "utf8"));
    return (Array.isArray(parsed?.slots) ? parsed.slots : []).filter(validSlot);
  } catch {
    return [];
  }
}

async function readLegacyConfig(path) {
  const parsed = JSON.parse(await readFile(path, "utf8"));
  const inbound = (Array.isArray(parsed?.inbounds) ? parsed.inbounds : []).find((item) => item?.tag === "soty-vless");
  const candidates = Array.isArray(inbound?.settings?.users)
    ? inbound.settings.users
    : Array.isArray(inbound?.settings?.clients)
      ? inbound.settings.clients
      : [];
  const users = candidates
    .filter((item) => validLegacyUser(item))
    .map((item) => ({ id: item.id, email: item.email }));
  const emails = new Set(users.map((item) => item.email));
  const rules = (Array.isArray(parsed?.routing?.rules) ? parsed.routing.rules : [])
    .filter((item) => item?.type === "field" && Array.isArray(item.user) && item.user.some((email) => emails.has(email)))
    .map((item) => ({ ...item, user: item.user.filter((email) => emails.has(email)) }));
  return { users, rules };
}

async function atomicJson(path, value, mode) {
  await mkdir(dirname(path), { recursive: true });
  const next = `${path}.next`;
  await writeFile(next, `${JSON.stringify(value, null, 2)}\n`, { mode });
  await chmod(next, mode);
  await rename(next, path);
}

function validSlot(value) {
  return value && /^slot_\d{3}$/u.test(value.id) && /^[0-9a-f-]{36}$/u.test(value.bridgeId) && /^soty-reverse-[A-Za-z0-9-]+$/u.test(value.reverseTag);
}

function validLegacyUser(value) {
  return value
    && !value.reverse
    && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(String(value.id || ""))
    && /^[A-Za-z0-9@._:-]{1,120}$/u.test(String(value.email || ""));
}

function boundedCount(value) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 512) throw new Error("--count must be between 1 and 512");
  return parsed;
}

function arg(name) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] || "" : "";
}
