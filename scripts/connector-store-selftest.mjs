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
  assert.equal((await store.getAssignedConnectorJob(auth, created.job.id)).ok, true);
  assert.equal((await store.getAssignedConnectorJob({ ...auth, token: "z".repeat(48) }, created.job.id)).error, "connector-auth-failed");
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

  const controllerLinkId = "r".repeat(43);
  const controllerDeviceId = "controller-device";
  await persisted.register({
    linkId,
    deviceId: "device-2",
    connectorId: "install-2:user",
    deviceNick: "Другой компьютер",
    version: "1.0.0",
    platform: "win32-x64",
    scope: "CurrentUser",
    capabilities: ["agent", "command", "script"],
    agent: { id: "opencode", provider: "gonka", available: true }
  }, "u".repeat(48));
  const ownerOnly = await persisted.createJob({ linkId, deviceId: "device-1", kind: "agent", text: "Только владелец" });
  assert.equal(ownerOnly.ok, true);
  assert.equal((await persisted.createJob(
    { linkId, deviceId: "device-1", kind: "agent", text: "Без grant" },
    controllerLinkId
  )).error, "connector-access-denied");
  assert.equal((await persisted.createJob({ linkId: "q".repeat(43), kind: "agent", text: "Orphan" })).error, "connector-access-denied");

  const issued = await persisted.createAccessGrant(linkId, {
    deviceId: "device-1",
    controllerDeviceId,
    capabilities: ["status", "agent", "command", "script", "events", "cancel"],
    expiresInMs: 60_000
  });
  assert.equal(issued.ok, true);
  assert.match(issued.token, /^[A-Za-z0-9_-]{40,160}$/u);
  assert.deepEqual(issued.grant.capabilities, ["status", "agent", "command", "script", "events", "cancel"]);
  assert.equal("token" in issued.grant, false);
  assert.equal((await readFile(path.join(root, "connector-store.json"), "utf8")).includes(issued.token), false);
  const grantStore = createConnectorStore(root, { now: () => now, leaseMs: 5_000 });
  const delegatedAuth = {
    linkId: controllerLinkId,
    grantId: issued.grant.id,
    controllerDeviceId,
    token: issued.token
  };
  const delegatedStatus = await grantStore.status(delegatedAuth);
  assert.deepEqual(delegatedStatus.devices.map((item) => item.deviceId), ["device-1"]);
  assert.equal((await grantStore.status({ ...delegatedAuth, controllerDeviceId: "intruder" })).error, "connector-access-denied");
  assert.equal((await grantStore.status({ ...delegatedAuth, token: "v".repeat(48) })).error, "connector-access-denied");
  assert.equal((await grantStore.status(delegatedAuth, "device-2")).error, "connector-access-denied");

  const delegated = await grantStore.createJob({ deviceId: "device-1", kind: "agent", text: "Запусти с другого Link" }, delegatedAuth);
  assert.equal(delegated.ok, true);
  const delegatedState = JSON.parse(await readFile(path.join(root, "connector-store.json"), "utf8"));
  const delegatedRecord = delegatedState.jobs.find((job) => job.id === delegated.job.id);
  assert.equal(delegatedRecord.schema, "soty.connector-job.v3");
  assert.equal(
    delegatedState.jobs.some((job) => ["soty.connector-job.v1", "soty.connector-job.v2"].includes(job.schema) && job.id === delegated.job.id),
    false
  );
  assert.equal((await grantStore.createJob({ deviceId: "device-2", kind: "agent", text: "Не тот target" }, delegatedAuth)).error, "connector-access-denied");
  assert.equal((await grantStore.getEvents(delegatedAuth, ownerOnly.job.id, 0)).error, "connector-access-denied");
  assert.equal((await grantStore.poll(auth)).jobs[0].id, ownerOnly.job.id);
  await grantStore.finishJob(auth, ownerOnly.job.id, { ok: true, text: "owner", exitCode: 0 });
  assert.equal((await grantStore.poll(auth)).jobs[0].id, delegated.job.id);
  await grantStore.appendEvent(auth, delegated.job.id, { type: "message", text: "Делегированное событие" });
  await grantStore.finishJob(auth, delegated.job.id, { ok: true, text: "Делегировано", exitCode: 0 });
  const delegatedEvents = await grantStore.getEvents(delegatedAuth, delegated.job.id, 0);
  assert.equal(delegatedEvents.done, true);
  assert.ok(delegatedEvents.events.some((event) => event.text === "Делегированное событие"));

  const statusOnly = await grantStore.createAccessGrant(linkId, {
    deviceId: "device-1",
    controllerDeviceId,
    capabilities: ["status"],
    expiresInMs: 60_000
  });
  const statusOnlyAuth = { grantId: statusOnly.grant.id, controllerDeviceId, token: statusOnly.token };
  assert.equal((await grantStore.status(statusOnlyAuth)).ok, true);
  assert.equal((await grantStore.createJob({ deviceId: "device-1", kind: "agent", text: "Scope isolation" }, statusOnlyAuth)).error, "connector-access-denied");
  assert.equal((await grantStore.getEvents(statusOnlyAuth, delegated.job.id, 0)).error, "connector-access-denied");

  const wildcard = await grantStore.createAccessGrant(linkId, {
    deviceId: "device-1",
    controllerDeviceId: "*",
    capabilities: ["link.control"],
    expiresInMs: 60_000
  });
  const wildcardAuth = { grantId: wildcard.grant.id, controllerDeviceId: "late-controller", token: wildcard.token };
  assert.equal((await grantStore.status(wildcardAuth)).ok, true);

  const revokedQueued = await grantStore.createJob({ deviceId: "device-1", kind: "agent", text: "Отозвать в очереди" }, wildcardAuth);
  assert.equal((await grantStore.revokeAccessGrant(linkId, wildcard.grant.id)).ok, true);
  assert.equal((await grantStore.getJob(wildcardAuth, revokedQueued.job.id)).error, "connector-access-revoked");
  assert.equal((await grantStore.getJob(linkId, revokedQueued.job.id)).job.status, "cancelled");

  const activeGrant = await grantStore.createAccessGrant(linkId, {
    deviceId: "device-1",
    controllerDeviceId,
    capabilities: ["link.control"],
    expiresInMs: 60_000
  });
  const activeAuth = { grantId: activeGrant.grant.id, controllerDeviceId, token: activeGrant.token };
  const activeDelegated = await grantStore.createJob({ deviceId: "device-1", kind: "agent", text: "Отозвать во время работы" }, activeAuth);
  assert.equal((await grantStore.poll(auth)).jobs[0].id, activeDelegated.job.id);
  await grantStore.revokeAccessGrant(linkId, activeGrant.grant.id);
  assert.deepEqual((await grantStore.poll(auth)).cancel, [activeDelegated.job.id]);
  await grantStore.finishJob(auth, activeDelegated.job.id, { ok: false, text: "Отменено", exitCode: 130 });

  const expiringGrant = await grantStore.createAccessGrant(linkId, {
    deviceId: "device-1",
    controllerDeviceId,
    capabilities: ["link.control"],
    expiresInMs: 1_000
  });
  const expiringAuth = { grantId: expiringGrant.grant.id, controllerDeviceId, token: expiringGrant.token };
  const expiringJob = await grantStore.createJob({ deviceId: "device-1", kind: "agent", text: "Дождаться expiry" }, expiringAuth);
  now += 1_001;
  assert.equal((await grantStore.getJob(expiringAuth, expiringJob.job.id)).error, "connector-access-expired");
  await grantStore.poll(auth);
  assert.equal((await grantStore.getJob(linkId, expiringJob.job.id)).job.status, "cancelled");

  const stale = await grantStore.createJob({ linkId, deviceId: "device-1", kind: "agent", text: "Не хранить вечно" });
  now += 7 * 24 * 60 * 60_000 + 1;
  await grantStore.createJob({ linkId, deviceId: "device-1", kind: "agent", text: "Запустить очистку" });
  const staleState = await grantStore.getJob(linkId, stale.job.id);
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
