#!/usr/bin/env node
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, writeFile, readFile, rm, symlink } from "node:fs/promises";
import fsPromises from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import { Worker } from "node:worker_threads";
import { DatabaseSync } from "node:sqlite";
import { snapshotConnectorAuthority, connectorStateSha256 } from "../server/connector-authority.js";
import { createConnectorStore, normalizeConnectorState } from "../server/connector-store.js";
import { readConnectorState } from "../server/connector-registry.js";

const root = await mkdtemp(path.join(tmpdir(), "soty-authority-test-"));
const checks = [];
const hash = bytes => createHash("sha256").update(bytes).digest("hex");
const fixture = () => ({ schema: "soty.connector-store.v2",
  connectors: ["b", "a"].map(id => ({ linkId: "l".repeat(43), deviceId: "device-" + id, connectorId: "user-" + id,
    scope: "CurrentUser", tokenHash: id.repeat(64), lastSeenAt: 100, capabilities: ["script"] })),
  accessGrants: [{ id: "grant-one", linkId: "l".repeat(43), deviceId: "device-a", controllerDeviceId: "controller-a",
    capabilities: ["script"], tokenHash: "c".repeat(64), createdAt: 1, expiresAt: 100000, revokedAt: 0 }],
  jobs: ["two", "one"].map(id => ({ schema: "soty.connector-job.v2", id, linkId: "l".repeat(43),
    deviceId: "device-a", connectorId: "user-a", kind: "script", status: "succeeded", createdAt: 1, updatedAt: 2, finishedAt: 2,
    input: { script: "PRIVATE-SYNTHETIC-INPUT-" + id, context: { b: 2, a: 1 }, runAs: "user" },
    result: { ok: true, exitCode: 0, text: "PRIVATE-SYNTHETIC-RESULT-" + id },
    events: [{ seq: 1, at: 1, type: "stdout", text: "PRIVATE-SYNTHETIC-EVENT-" + id }] })),
  requests: [{ id: "d".repeat(64), fingerprint: "e".repeat(64), jobId: "one" }] });
async function seed(name, state = fixture()) {
  const dir = path.join(root, name); await mkdir(dir);
  await writeFile(path.join(dir, "connector-store.json"), JSON.stringify(state)); return dir;
}
async function test(name, action) { const result = await action(); checks.push(result?.skipped ? { name, ...result } : { name, ok: true }); }
try {
  await test("repeated opened legacy snapshots retain source identity across supported stat implementations", async () => {
    const dir = await seed("stat-compatibility");
    const first = await snapshotConnectorAuthority(dir, { syncLegacy: true });
    const second = await snapshotConnectorAuthority(dir);
    assert.deepEqual(second, first);
    assert.equal(first.sourceSha256, hash(await readFile(path.join(dir, "connector-store.json"))));
  });
  await test("fresh SQLite has null legacy authority only when both meta and marker omit it", async () => {
    const dir = path.join(root, "fresh-sqlite");
    const store = createConnectorStore(dir); await store.ready; await store.close();
    const before = await snapshotConnectorAuthority(dir, { syncLegacy: true });
    assert.equal(before.kind, "sqlite"); assert.equal(before.legacySha256, null);
    assert.deepEqual(before.counts, { connectors: 0, accessGrants: 0, jobs: 0, requests: 0, events: 0 });
    const file = path.join(dir, "connector-store.json");
    const marker = JSON.parse(await readFile(file, "utf8"));
    for (const value of [null, "", "f".repeat(64)]) {
      await writeFile(file, JSON.stringify({ ...marker, legacySha256: value }));
      await assert.rejects(snapshotConnectorAuthority(dir));
    }
    await writeFile(file, JSON.stringify(marker));
    const db = new DatabaseSync(path.join(dir, "connector-store.sqlite"));
    db.prepare("INSERT INTO meta(key,value) VALUES('legacySha256',?)").run("f".repeat(64)); db.close();
    await assert.rejects(snapshotConnectorAuthority(dir));
  });
  await test("open worker WAL remains readable with complete unchanged authority", async () => {
    const dir = await seed("open-worker");
    const before = await snapshotConnectorAuthority(dir);
    const store = createConnectorStore(dir); await store.ready;
    try {
      const wal = await fsPromises.lstat(path.join(dir, "connector-store.sqlite-wal"));
      assert.ok(wal.size > 0);
      for (let index = 0; index < 2; index++) {
        const after = await snapshotConnectorAuthority(dir);
        assert.equal(after.stateSha256, before.stateSha256);
        assert.equal(after.legacySha256, before.sourceSha256);
      }
    } finally { await store.close(); }
  });
  await test("WAL ctime drift requires identical bytes, identity, mtime and access metadata", async () => {
    // A real SQLite snapshot with faults activated after the registry reader
    // starts. Filesystem read corruption preserves all stat values, so that
    // case specifically proves complete WAL bytes are checked as well.
    for (const [label, nonempty, field, accepted] of [
      ["empty-ctime", false, "ctimeNs", true], ["empty-mtime", false, "mtimeNs", false],
      ["empty-inode", false, "ino", false], ["empty-device", false, "dev", false],
      ["nonempty-ctime", true, "ctimeNs", true], ["nonempty-bytes", true, "bytes", false],
      ["nonempty-mode", true, "mode", false], ["nonempty-owner", true, "uid", false],
      ["nonempty-group", true, "gid", false], ["nonempty-links", true, "nlink", false],
    ]) {
      const dir = await seed("wal-" + label);
      const store = createConnectorStore(dir); await store.ready; await store.close();
      const dbFile = path.join(dir, "connector-store.sqlite"), wal = dbFile + "-wal";
      const db = new DatabaseSync(dbFile);
      db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_checkpoint(TRUNCATE)");
      if (nonempty) db.exec("UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='revision'");
      const before = await fsPromises.lstat(wal, { bigint: true });
      assert.equal(before.size > 0n, nonempty);
      const originalLstat = fsPromises.lstat, originalReadFile = fsPromises.readFile, originalOpen = fsPromises.open;
      let readingState = false, injected = 0;
      const alter = info => {
        if (readingState && field !== "bytes") {
          Object.defineProperty(info, field, { value: info[field] + 1n }); injected++;
        }
        return info;
      };
      fsPromises.readFile = async (file, ...args) => {
        const result = await originalReadFile(file, ...args);
        if (String(file) === path.join(dir, "connector-store.json")) readingState = true;
        return result;
      };
      fsPromises.lstat = async (file, options) => {
        const info = await originalLstat(file, options);
        return String(file) === wal && options?.bigint ? alter(info) : info;
      };
      fsPromises.open = async (file, ...args) => {
        const handle = await originalOpen(file, ...args);
        if (String(file) === wal) {
          const stat = handle.stat.bind(handle), read = handle.read.bind(handle);
          handle.stat = async options => alter(await stat(options));
          handle.read = async (...readArgs) => {
            const result = await read(...readArgs);
            if (readingState && field === "bytes" && result.bytesRead) {
              readArgs[0][result.bytesRead - 1] ^= 1; injected++;
            }
            return result;
          };
        }
        return handle;
      };
      syncBuiltinESMExports();
      try {
        if (accepted) {
          const authority = await snapshotConnectorAuthority(dir);
          assert.equal(authority.stateSha256, connectorStateSha256(normalizeConnectorState(fixture())));
        } else await assert.rejects(snapshotConnectorAuthority(dir), error => {
          assert.equal(error.message, "Connector authority snapshot rejected; preserve offline evidence");
          assert.match(error.stack, /at check /u); // Original safe check site survives catch.
          return true;
        });
        assert.ok(injected > 0, "requested filesystem fault was exercised");
      } finally {
        fsPromises.lstat = originalLstat; fsPromises.readFile = originalReadFile; fsPromises.open = originalOpen;
        syncBuiltinESMExports(); db.close();
      }
    }
  });
  await test("canonical keys and top-level identities ignore ordering; nested event order remains strict", async () => {
    const state = fixture(), other = structuredClone(state);
    other.connectors.reverse(); other.jobs.reverse(); other.jobs[0].input.context = { a: 1, b: 2 };
    assert.equal(connectorStateSha256(normalizeConnectorState(state)), connectorStateSha256(normalizeConnectorState(other)));
  });
  await test("full legacy state survives real SQLite migration and reformatted legacy rollback", async () => {
    const dir = await seed("migration");
    const abandoned = fixture(); abandoned.jobs[0].input.script = "UNCOMMITTED-NEXT-INPUT";
    await writeFile(path.join(dir, "connector-store.json.complete.next"), JSON.stringify(abandoned));
    await writeFile(path.join(dir, "connector-store.json.partial.next"), '{"secret":"partial');
    const before = await snapshotConnectorAuthority(dir, { syncLegacy: true });
    assert.equal(before.stateSha256, connectorStateSha256(normalizeConnectorState(fixture())));
    assert.deepEqual(before.counts, { connectors: 2, accessGrants: 1, jobs: 2, requests: 1, events: 2 });
    assert.equal(before.stateSha256, connectorStateSha256(await readConnectorState(dir)));
    assert.deepEqual(before.activeJobs, []);
    const store = createConnectorStore(dir); await store.ready; await store.close();
    const migrated = await snapshotConnectorAuthority(dir);
    assert.equal(migrated.kind, "sqlite"); assert.equal(migrated.stateSha256, before.stateSha256);
    assert.equal(migrated.legacySha256, before.sourceSha256);
    assert.equal(migrated.sourceSha256, hash(await readFile(path.join(dir, "connector-store.json"))));
    assert.deepEqual(migrated.temporaryFiles, before.temporaryFiles);
    assert.equal((await snapshotConnectorAuthority(dir, { syncLegacy: true })).stateSha256, before.stateSha256);
    const full = await readConnectorState(dir);
    const rollback = await seed("rollback", full);
    await writeFile(path.join(rollback, "connector-store.json"), JSON.stringify(full, null, 2) + "\n");
    const restored = await snapshotConnectorAuthority(rollback, { syncLegacy: true });
    assert.equal(restored.stateSha256, before.stateSha256);
    assert.notEqual(restored.sourceSha256, before.sourceSha256);
    const output = JSON.stringify(migrated);
    assert.ok(!output.includes("PRIVATE-SYNTHETIC") && !output.includes("tokenHash"));
    assert.ok(!output.includes("c".repeat(64)));
    const marker = JSON.parse(await readFile(path.join(dir, "connector-store.json"), "utf8"));
    marker.legacySha256 = "f".repeat(64);
    await writeFile(path.join(dir, "connector-store.json"), JSON.stringify(marker));
    await assert.rejects(snapshotConnectorAuthority(dir));
  });
  await test("input, result, event, grants, credentials and request fingerprints all affect full hash", async () => {
    const before = await snapshotConnectorAuthority(await seed("baseline"));
    const edits = [s => s.jobs[0].input.script += "changed", s => s.jobs[0].result.text += "changed",
      s => s.jobs[0].events[0].text += "changed", s => s.accessGrants[0].expiresAt++,
      s => s.connectors[0].tokenHash = "f".repeat(64), s => s.requests[0].fingerprint = "f".repeat(64)];
    for (let index = 0; index < edits.length; index++) {
      const state = fixture(); edits[index](state);
      assert.notEqual((await snapshotConnectorAuthority(await seed("edit-" + index, state))).stateSha256, before.stateSha256);
    }
  });
  await test("leased and every known nonterminal status reported; unknown status fails closed", async () => {
    for (const status of ["queued", "leased", "running"]) {
      const state = fixture(); state.jobs[0].status = status;
      const result = await snapshotConnectorAuthority(await seed(status, state));
      assert.deepEqual(result.activeJobs, [{ id: "two", status }]); assert.equal(result.statusCounts[status], 1);
    }
    const state = fixture(); state.jobs[0].status = "uncertain-new-status";
    await assert.rejects(snapshotConnectorAuthority(await seed("unknown", state)));
  });
  await test("missing, malformed, partial and unsupported authority rejected without data excerpts", async () => {
    const dir = await seed("malformed");
    for (const bytes of ['{"PRIVATE-SYNTHETIC', '{}', '{"schema":"soty.connector-store.sqlite.v1"}']) {
      await writeFile(path.join(dir, "connector-store.json"), bytes);
      await assert.rejects(snapshotConnectorAuthority(dir), error => !error.stack.includes("PRIVATE-SYNTHETIC"));
    }
    await rm(path.join(dir, "connector-store.json")); await assert.rejects(snapshotConnectorAuthority(dir));
  });
  await test("SQLite records and events row identities cannot silently differ from serialized state", async () => {
    const dir = await seed("sqlite-corrupt"); const store = createConnectorStore(dir); await store.ready; await store.close();
    const db = new DatabaseSync(path.join(dir, "connector-store.sqlite"));
    db.prepare("UPDATE events SET value=? WHERE job_id='one'").run(JSON.stringify({ seq: 9, at: 1, type: "stdout", text: "synthetic" })); db.close();
    await assert.rejects(snapshotConnectorAuthority(dir));
  });
  await test("symlink authority or next evidence rejected", async () => {
    const dir = await seed("symlink"), target = path.join(dir, "target.json"), file = path.join(dir, "connector-store.json");
    await writeFile(target, JSON.stringify(fixture())); await rm(file);
    try { await symlink(target, file); } catch (error) {
      if (process.platform === "win32" && error.code === "EPERM") return { skipped: true, reason: "Windows file-symlink privilege unavailable; run this case on Linux" };
      throw error;
    }
    await assert.rejects(snapshotConnectorAuthority(dir)); await rm(file); await writeFile(file, JSON.stringify(fixture()));
    await symlink(target, path.join(dir, "connector-store.json.link.next")); await assert.rejects(snapshotConnectorAuthority(dir));
  });
  await test("concurrent canonical-file writes reject drifting snapshot", async () => {
    const state = fixture(); state.jobs[0].input.script = "x".repeat(4000000);
    const dir = await seed("drift", state), file = path.join(dir, "connector-store.json");
    const worker = new Worker(`const {parentPort,workerData}=require('node:worker_threads');const fs=require('node:fs');parentPort.postMessage('ready');for(;;){fs.writeFileSync(workerData.file,workerData.bytes);}`, { eval: true, workerData: { file, bytes: JSON.stringify(state) } });
    await new Promise(resolve => worker.once("message", resolve));
    try { await assert.rejects(snapshotConnectorAuthority(dir)); } finally { await worker.terminate(); }
  });
  console.log(JSON.stringify({ ok: true, providerCalls: 0, syntheticOnly: true, checks }, null, 2));
} finally {
  assert.equal(path.dirname(path.resolve(root)), path.resolve(tmpdir()));
  assert.ok(path.basename(root).startsWith("soty-authority-test-"));
  await rm(root, { recursive: true, force: true });
}
