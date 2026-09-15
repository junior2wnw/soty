#!/usr/bin/env node
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { createConnectorStore } from "../server/connector-store.js";

const root = await mkdtemp(path.join(tmpdir(), "soty-connector-store-"));
let now = Date.parse("2026-08-09T00:00:00.000Z");
const token = "t".repeat(48);
const linkId = "l".repeat(43);
const auth = { linkId, deviceId: "device-1", connectorId: "install-1:user", token };

try {
  const store = createConnectorStore(root, { now: () => now, leaseMs: 5_000 });
  const aggregateRoot = path.join(root, "aggregate");
  await mkdir(aggregateRoot);
  let aggregateNow = now;
  const aggregateStore = createConnectorStore(aggregateRoot, { now: () => aggregateNow, leaseMs: 5_000 });
  await aggregateStore.register({
    linkId,
    deviceId: "aggregate-device",
    connectorId: "old-install:user",
    deviceNick: "Old connector",
    version: "1.2.2",
    platform: "win32-x64-old",
    scope: "CurrentUser",
    capabilities: ["agent"],
    agent: { id: "opencode", provider: "gonka", available: true, version: "1.0.0", capabilities: ["chat"] }
  }, "a".repeat(48));
  aggregateNow += 90_001;
  await aggregateStore.register({
    linkId,
    deviceId: "aggregate-device",
    connectorId: "new-install:machine",
    deviceNick: "Current connector",
    version: "1.2.8",
    platform: "win32-x64",
    scope: "Machine",
    capabilities: ["command", "script"],
    agent: { id: "opencode", provider: "gonka", available: false, capabilities: [] }
  }, "b".repeat(48));
  aggregateNow += 1;
  await aggregateStore.register({
    linkId,
    deviceId: "aggregate-device",
    connectorId: "new-install:user",
    deviceNick: "Current connector",
    version: "1.2.8",
    platform: "win32-x64",
    scope: "CurrentUser",
    capabilities: ["agent"],
    agent: { id: "opencode", provider: "gonka", available: true, version: "1.18.15", capabilities: ["chat", "files"] }
  }, "c".repeat(48));
  const aggregateStatus = await aggregateStore.status(linkId, "aggregate-device");
  assert.equal(aggregateStatus.devices.length, 1);
  assert.equal(aggregateStatus.devices[0].connected, true);
  assert.equal(aggregateStatus.devices[0].version, "1.2.8");
  assert.equal(aggregateStatus.devices[0].deviceNick, "Current connector");
  assert.deepEqual(aggregateStatus.devices[0].scopes, ["Machine", "CurrentUser"]);
  assert.equal(aggregateStatus.devices[0].agent.available, true);
  assert.equal(aggregateStatus.devices[0].agent.version, "1.18.15");

  const registration = await store.register({
    ...auth,
    deviceNick: "Рабочий компьютер",
    version: "1.0.0",
    platform: "win32-x64",
    scope: "CurrentUser",
    capabilities: ["agent", "command", "script"],
    agent: { id: "opencode", name: "OpenCode", provider: "gonka", model: "deepseek-ai/DeepSeek-V4-Flash-0731", available: true, version: "1.18.15", capabilities: ["chat", "files"] }
  }, token);
  assert.equal(registration.ok, true);
  assert.equal((await store.status(linkId)).connected, true);
  assert.equal((await store.register({ ...auth, agent: { id: "opencode", provider: "gonka", available: true } }, "x".repeat(48))).error, "connector-auth-failed");
  assert.equal((await store.createJob({ linkId, kind: "unknown", text: "Не запускать" })).error, "invalid-job");

  const created = await store.createJob({ linkId, deviceId: "device-1", threadId: "thread-1", kind: "agent", text: "Проверь проект" });
  assert.equal(created.ok, true);
  assert.equal(created.job.status, "queued");

  const leased = await store.poll(auth);
  assert.equal(leased.jobs.length, 1);
  assert.equal(leased.jobs[0].id, created.job.id);
  assert.equal((await store.poll({ ...auth, token: "z".repeat(48) })).error, "connector-auth-failed");

  assert.equal((await store.appendEvent(auth, created.job.id, { type: "message", text: "Начинаю" })).ok, true);
  const events = await store.getEvents(linkId, created.job.id, 0);
  assert.deepEqual(events.events.map((event) => event.type), ["queued", "leased", "message"]);

  const finished = await store.finishJob(auth, created.job.id, { ok: true, text: "Готово", exitCode: 0, sessionId: "session-1" });
  assert.equal(finished.job.status, "succeeded");

  const persisted = createConnectorStore(root, { now: () => now, leaseMs: 5_000 });
  assert.equal((await persisted.getJob(linkId, created.job.id)).job.result.text, "Готово");

  const retry = await persisted.createJob({ linkId, deviceId: "device-1", kind: "agent", text: "Повтори" });
  assert.equal((await persisted.poll(auth)).jobs[0].id, retry.job.id);
  now += 6_000;
  const reLeased = await persisted.poll(auth);
  assert.equal(reLeased.jobs[0].id, retry.job.id);
  assert.equal(reLeased.jobs[0].attempt, 2);

  const cancelled = await persisted.cancelJob(linkId, retry.job.id);
  assert.equal(cancelled.job.cancelRequested, true);
  assert.deepEqual((await persisted.poll(auth)).cancel, [retry.job.id]);
  const cancellationResult = await persisted.finishJob(auth, retry.job.id, { ok: false, text: "Отменено", exitCode: 130 });
  assert.equal(cancellationResult.job.status, "cancelled");

  const stale = await persisted.createJob({ linkId, deviceId: "offline-device", kind: "agent", text: "Не хранить вечно" });
  now += 7 * 24 * 60 * 60_000 + 1;
  await persisted.createJob({ linkId, deviceId: "device-1", kind: "agent", text: "Запустить очистку" });
  const staleState = await persisted.getJob(linkId, stale.job.id);
  assert.equal(staleState.job.status, "failed");
  assert.equal(staleState.job.result.exitCode, 124);

  const legacyRoot = path.join(root, "legacy");
  await mkdir(legacyRoot);
  const legacyState = JSON.parse(await readFile(path.join(root, "connector-store.json"), "utf8"));
  legacyState.schema = "soty.connector-store.v1";
  legacyState.connectors = [{
    linkId,
    deviceId: auth.deviceId,
    connectorId: auth.connectorId,
    deviceNick: "Рабочий компьютер",
    version: "1.0.0",
    platform: "win32-x64",
    scope: "CurrentUser",
    capabilities: ["agent", "command", "script"],
    tokenHash: createHash("sha256").update(token).digest("hex"),
    createdAt: now,
    lastSeenAt: now,
    adapters: [{ id: "codex", available: true, capabilities: ["chat"] }]
  }];
  legacyState.jobs = legacyState.jobs.slice(0, 1).map(({ kind: _kind, ...job }) => ({
    ...job,
    schema: "soty.connector-job.v1",
    adapterId: "codex",
    requestedAdapterId: "auto-chat",
    input: { ...job.input, kind: "chat" },
    createdAt: now,
    updatedAt: now,
    finishedAt: now
  }));
  await writeFile(path.join(legacyRoot, "connector-store.json"), JSON.stringify(legacyState));
  const migrated = createConnectorStore(legacyRoot, { now: () => now, leaseMs: 5_000 });
  const migratedStatus = await migrated.status(linkId);
  assert.equal(migratedStatus.devices[0].agent.available, false);
  assert.equal(migratedStatus.devices[0].agent.reason, "Требуется обновление Soty Agent");
  const migratedJob = await migrated.getJob(linkId, legacyState.jobs[0].id);
  assert.equal(migratedJob.job.kind, "agent");
  assert.equal("adapterId" in migratedJob.job, false);

  process.stdout.write("connector-store:selftest:ok\n");
} finally {
  await rm(root, { recursive: true, force: true });
}
