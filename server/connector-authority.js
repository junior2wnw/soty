import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { lstat, open, readdir } from "node:fs/promises";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { readConnectorState } from "./connector-registry.js";
import { connectorKey, syncDirectory } from "./connector-persistence.js";

const MAX_BYTES = 256 * 1024 * 1024;
const MAX_RECORDS = 100000;
const MAX_EVENTS = 2000000;
const terminal = new Set(["succeeded", "failed", "cancelled"]);
class AuthorityError extends Error {}
const fail = () => { throw new AuthorityError("Connector authority snapshot rejected; preserve offline evidence"); };
const check = value => { if (!value) fail(); };
const signature = s => [s.dev, s.ino, s.size, s.mtimeNs, s.ctimeNs].join(":");

async function regular(file, optional = false) {
  try {
    const info = await lstat(file, { bigint: true });
    check(info.isFile() && !info.isSymbolicLink() && info.size <= BigInt(MAX_BYTES));
    return signature(info);
  } catch (error) {
    if (optional && error.code === "ENOENT") return null;
    throw error;
  }
}

async function source(file, { sync = false } = {}) {
  const before = await regular(file);
  const handle = await open(file, (sync && process.platform === "win32" ? constants.O_RDWR : constants.O_RDONLY) | (constants.O_NOFOLLOW || 0));
  try {
    const info = await handle.stat({ bigint: true });
    const openedSignature = signature(info);
    // Node22 Windows lstat reports dev=0 while fstat reports the real volume.
    // Compare its available path identity, retaining full fstat identity for
    // the read and the second source read. Linux keeps the exact dev check.
    const pathComparable = process.platform === "win32" && before.startsWith("0:")
      ? [0n, info.ino, info.size, info.mtimeNs, info.ctimeNs].join(":") : openedSignature;
    check(info.isFile() && pathComparable === before && info.size <= BigInt(MAX_BYTES));
    const hash = createHash("sha256");
    const buffer = Buffer.alloc(65536);
    const small = info.size <= 65536n ? [] : null;
    let bytes = 0;
    for (;;) {
      const read = await handle.read(buffer, 0, buffer.length, null);
      if (!read.bytesRead) break;
      bytes += read.bytesRead;
      check(bytes <= MAX_BYTES);
      hash.update(buffer.subarray(0, read.bytesRead));
      if (small) small.push(Buffer.from(buffer.subarray(0, read.bytesRead)));
    }
    if (sync) await handle.sync();
    check(BigInt(bytes) === info.size && signature(await handle.stat({ bigint: true })) === openedSignature);
    check(await regular(file) === before);
    return { bytes, sha256: hash.digest("hex"), signature: before, handleSignature: openedSignature,
      access: [info.mode, info.uid, info.gid, info.nlink].join(":"), text: small ? Buffer.concat(small).toString("utf8") : null };
  } finally { await handle.close(); }
}

export function connectorStateSha256(state) {
  const hash = createHash("sha256");
  let nodes = 0;
  function value(item, depth = 0) {
    check(depth <= 64 && ++nodes <= 5000000);
    if (Array.isArray(item)) {
      hash.update("[");
      item.forEach((entry, index) => { if (index) hash.update(","); value(entry ?? null, depth + 1); });
      hash.update("]");
    } else if (item !== null && typeof item === "object") {
      hash.update("{");
      Object.keys(item).filter(key => item[key] !== undefined).sort().forEach((key, index) => {
        if (index) hash.update(",");
        hash.update(JSON.stringify(key) + ":"); value(item[key], depth + 1);
      });
      hash.update("}");
    } else {
      check(item === null || ["string", "number", "boolean"].includes(typeof item));
      check(typeof item !== "number" || Number.isFinite(item));
      hash.update(JSON.stringify(item));
    }
  }
  const sorted = (items, identity) => [...items].sort((a, b) => {
    const left = identity(a), right = identity(b); return left < right ? -1 : left > right ? 1 : 0;
  });
  value({ ...state, connectors: sorted(state.connectors, connectorKey),
    accessGrants: sorted(state.accessGrants, item => item.id), jobs: sorted(state.jobs, item => item.id),
    requests: sorted(state.requests, item => item.id) });
  return hash.digest("hex");
}

function sqliteMetadata(file, marker) {
  const db = new DatabaseSync(file, { readOnly: true });
  try {
    db.exec("PRAGMA query_only=ON; PRAGMA busy_timeout=1000; BEGIN");
    const meta = key => db.prepare("SELECT value FROM meta WHERE key=?").get(key)?.value;
    check(meta("schema") === "soty.connector-sqlite.v1");
    const legacySha256 = meta("legacySha256");
    const noLegacy = legacySha256 === undefined && !Object.hasOwn(marker, "legacySha256");
    check(marker.schema === "soty.connector-store.sqlite.v1" && marker.database === "connector-store.sqlite"
      && marker.databaseId === meta("uuid") && (noLegacy || (/^[a-f0-9]{64}$/u.test(legacySha256)
      && marker.legacySha256 === legacySha256)));
    for (const table of ["records", "jobs", "requests", "inputs", "results", "events"]) {
      const count = db.prepare(`SELECT count(*) AS count FROM ${table}`).get().count;
      check(count <= (table === "events" ? MAX_EVENTS : table === "records" ? MAX_RECORDS * 2 : MAX_RECORDS));
    }
    for (const row of db.prepare("SELECT kind,id,value FROM records").all()) {
      const record = JSON.parse(row.value);
      check(["connectors", "accessGrants"].includes(row.kind));
      check(row.id === (row.kind === "connectors" ? connectorKey(record) : record.id));
    }
    for (const row of db.prepare("SELECT id,value FROM requests").all()) check(JSON.parse(row.value).id === row.id);
    for (const row of db.prepare("SELECT seq,value FROM events").all()) check(JSON.parse(row.value).seq === row.seq);
    check(/^[0-9]+$/u.test(meta("revision")) && Number.isSafeInteger(Number(meta("revision"))));
    return { legacySha256: noLegacy ? null : legacySha256, revision: meta("revision"), uuid: meta("uuid") };
  } finally { db.close(); }
}

// Caller must establish the stopped-original or maintenance admission barrier.
// This reader never imports .next files, creates a store, acquires ownership,
// outputs protected state, or claims to measure an unobservable legacy queue.
export async function snapshotConnectorAuthority(dataDir, { syncLegacy = false } = {}) {
  try {
    const dir = path.resolve(dataDir);
    for (let ancestor = dir;; ancestor = path.dirname(ancestor)) {
      const info = await lstat(ancestor);
      check(info.isDirectory() && !info.isSymbolicLink());
      if (path.dirname(ancestor) === ancestor) break;
    }
    const dbFile = path.join(dir, "connector-store.sqlite");
    const legacyFile = path.join(dir, "connector-store.json");
    const databaseBefore = await regular(dbFile, true);
    const walBefore = await regular(dbFile + "-wal", true);
    const shmBefore = await regular(dbFile + "-shm", true);
    check(databaseBefore !== null || (walBefore === null && shmBefore === null));
    const walSourceBefore = walBefore === null ? null : await source(dbFile + "-wal");
    if (walSourceBefore) check(walSourceBefore.signature === walBefore);
    const names = async () => (await readdir(dir)).filter(name => /^connector-store\.json\..+\.next$/u.test(name)).sort();
    const beforeNames = await names();
    check(beforeNames.length <= 32);
    const temporaryFiles = [];
    const temporarySignatures = [];
    for (const name of beforeNames) {
      const item = await source(path.join(dir, name));
      temporaryFiles.push({ name, bytes: item.bytes, sha256: item.sha256 });
      temporarySignatures.push(item.signature);
      check(temporaryFiles.reduce((sum, file) => sum + file.bytes, 0) <= MAX_BYTES * 2);
    }
    const original = await source(legacyFile, { sync: syncLegacy && databaseBefore === null });
    if (syncLegacy && databaseBefore === null) await syncDirectory(dir);
    const metadata = databaseBefore === null ? null : sqliteMetadata(dbFile, JSON.parse(original.text));
    const state = await readConnectorState(dir);
    const counts = { connectors: state.connectors.length, accessGrants: state.accessGrants.length,
      jobs: state.jobs.length, requests: state.requests.length, events: state.jobs.reduce((sum, job) => sum + job.events.length, 0) };
    check(Object.entries(counts).every(([name, count]) => count <= (name === "events" ? MAX_EVENTS : MAX_RECORDS)));
    const stateSha256 = connectorStateSha256(state);
    const statusCounts = {};
    const activeJobs = [];
    for (const job of [...state.jobs].sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0)) {
      statusCounts[job.status] = (statusCounts[job.status] || 0) + 1;
      if (!terminal.has(job.status)) activeJobs.push({ id: job.id, status: job.status });
    }
    const after = await source(legacyFile);
    check(after.signature === original.signature && after.handleSignature === original.handleSignature && after.sha256 === original.sha256);
    check(await regular(dbFile, true) === databaseBefore);
    const walAfter = await regular(dbFile + "-wal", true);
    const walSourceAfter = walAfter === null ? null : await source(dbFile + "-wal");
    if (walSourceAfter) check(walSourceAfter.signature === walAfter);
    // Opening SQLite readonly can chmod an existing WAL to its existing mode,
    // changing ctime even when the writer is frozen and the WAL has frames.
    // Stream and compare every byte, both file identities, mtime and access
    // metadata. Only ctime may differ; it is never a substitute for byte proof.
    const contentIdentity = item => item.split(":").slice(0, 4).join(":");
    const sameWal = walSourceBefore && walSourceAfter
      && walSourceBefore.sha256 === walSourceAfter.sha256 && walSourceBefore.access === walSourceAfter.access
      && contentIdentity(walSourceBefore.signature) === contentIdentity(walSourceAfter.signature)
      && contentIdentity(walSourceBefore.handleSignature) === contentIdentity(walSourceAfter.handleSignature);
    // A readonly connection may also create an initially absent, empty WAL.
    check((walBefore === null && walAfter === null) || (metadata && sameWal)
      || (metadata && walBefore === null && walSourceAfter?.bytes === 0));
    await regular(dbFile + "-shm", true);
    if (metadata) check(JSON.stringify(sqliteMetadata(dbFile, JSON.parse(after.text))) === JSON.stringify(metadata));
    check(JSON.stringify(await names()) === JSON.stringify(beforeNames));
    for (let index = 0; index < beforeNames.length; index++) check(await regular(path.join(dir, beforeNames[index])) === temporarySignatures[index]);
    return { schema: "soty.connector-authority.v1", kind: metadata ? "sqlite" : "legacy", stateSha256,
      sourceSha256: original.sha256, legacySha256: metadata ? metadata.legacySha256 : original.sha256,
      bytes: original.bytes, counts, statusCounts, activeJobs, temporaryFiles };
  } catch (error) {
    // Preserve the original controlled-check stack, never arbitrary input errors.
    if (error instanceof AuthorityError) throw error;
    // JSON/SQLite errors can contain input excerpts: do not propagate them.
    fail();
  }
}
