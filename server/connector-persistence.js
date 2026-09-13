import { Worker, isMainThread, parentPort, workerData } from "node:worker_threads";
import { randomUUID, createHash } from "node:crypto";
import { mkdir, readFile, open, rename, chmod, unlink, stat } from "node:fs/promises";
import path from "node:path";

// SQLite's synchronous API lives off the HTTP event loop. Only changed records
// cross this boundary; neither heartbeat nor cancellation rewrites history.
export class ConnectorPersistence {
  constructor(filePath) {
    this.pending = new Map();
    this.sequence = 0;
    this.worker = new Worker(new URL(import.meta.url), {
      workerData: { connectorPersistence: true, filePath },
      execArgv: process.execArgv.filter((arg) => !arg.startsWith("--input-type")),
      resourceLimits: { maxOldGenerationSizeMb: 512 }
    });
    this.worker.on("message", ({ id, value, error }) => {
      const request = this.pending.get(id);
      if (!request) return;
      this.pending.delete(id);
      if (error) request.reject(Object.assign(new Error(error.message), { code: error.code }));
      else request.resolve(value);
      if (!this.pending.size) this.worker.unref();
    });
    const failed = () => {
      this.failed = new Error("Connector persistence worker stopped; restart required to reconcile durable state");
      for (const request of this.pending.values()) request.reject(this.failed);
      this.pending.clear();
    };
    this.worker.on("error", failed);
    this.worker.on("exit", failed);
    this.worker.unref();
  }

  call(action, value) {
    if (this.failed) return Promise.reject(this.failed);
    this.worker.ref();
    return new Promise((resolve, reject) => {
      const id = ++this.sequence;
      this.pending.set(id, { resolve, reject });
      this.worker.postMessage({ id, action, value });
    });
  }

  async close() {
    if (!this.failed) await this.call("close");
    await this.worker.terminate();
  }
}

export const databaseName = "connector-store.sqlite";
export const sqliteMarkerSchema = "soty.connector-store.sqlite.v1";

if (!isMainThread && workerData?.connectorPersistence) {
  const { DatabaseSync } = await import("node:sqlite");
  const filePath = workerData.filePath;
  const dbPath = path.join(path.dirname(filePath), databaseName);
  let db;
  let owner;
  let revision = 0;
  let chain = Promise.resolve();
  const json = JSON.stringify;
  const parse = JSON.parse;
  const getMeta = (key) => db.prepare("SELECT value FROM meta WHERE key=?").get(key)?.value;
  const putMeta = (key, value) => db.prepare("INSERT OR REPLACE INTO meta VALUES (?,?)").run(key, String(value));

  async function load() {
    const { normalizeConnectorState } = await import("./connector-store.js");
    await mkdir(path.dirname(filePath), { recursive: true });
    owner = await acquireConnectorOwner(path.dirname(filePath));
    await rejectRollbackIntent(path.dirname(filePath));
    let legacy;
    try { legacy = await readFile(filePath, "utf8"); } catch (error) { if (error.code !== "ENOENT") throw error; }
    let parsed;
    try { parsed = legacy === undefined ? undefined : parse(legacy); } catch { throw new Error("Malformed connector store JSON; preserve existing file"); }
    let existed = true;
    try { const handle = await open(dbPath, "r"); await handle.close(); } catch (error) { if (error.code !== "ENOENT") throw error; existed = false; }
    // Validate before creating any new persistent file. Unknown/partial data
    // never means a new installation, including a marker with a missing DB.
    const initial = !existed ? normalizeConnectorState(parsed ?? { schema: "soty.connector-store.v2", connectors: [], jobs: [], accessGrants: [] }) : null;
    db = new DatabaseSync(dbPath);
    await chmod(dbPath, 0o600);
    db.exec("PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;");
    if (!existed) {
      db.exec(`BEGIN IMMEDIATE;
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE records (kind TEXT NOT NULL, id TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(kind,id));
        CREATE TABLE jobs (id TEXT PRIMARY KEY, value TEXT NOT NULL, route TEXT NOT NULL);
        CREATE TABLE inputs (id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE, value TEXT NOT NULL);
        CREATE TABLE results (id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE, value TEXT NOT NULL);
        CREATE TABLE events (job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE, seq INTEGER NOT NULL, value TEXT NOT NULL, PRIMARY KEY(job_id,seq));
        CREATE TABLE requests (id TEXT PRIMARY KEY, value TEXT NOT NULL);
      `);
      try {
        putMeta("schema", "soty.connector-sqlite.v1");
        putMeta("uuid", randomUUID());
        putMeta("revision", 0);
        if (legacy !== undefined) putMeta("legacySha256", createHash("sha256").update(legacy).digest("hex"));
        for (const kind of ["connectors", "accessGrants"]) {
          for (const record of initial[kind]) putRecord(kind, kind === "connectors" ? connectorKey(record) : record.id, record);
        }
        for (const job of initial.jobs) putJob({ id: job.id, metadata: metadata(job), input: job.input, result: job.result, events: job.events });
        for (const request of initial.requests || []) db.prepare("INSERT INTO requests VALUES (?,?)").run(request.id, json(request));
        db.exec("COMMIT");
      } catch (error) { db.exec("ROLLBACK"); throw error; }
    }
    if (getMeta("schema") !== "soty.connector-sqlite.v1" || db.prepare("PRAGMA quick_check").get().quick_check !== "ok") throw new Error("Invalid connector database; preserve files for recovery");
    revision = Number(getMeta("revision"));
    if (!Number.isSafeInteger(revision)) throw new Error("Invalid connector database revision");
    const state = {
      schema: "soty.connector-store.v2",
      connectors: db.prepare("SELECT value FROM records WHERE kind='connectors'").all().map((r) => parse(r.value)),
      accessGrants: db.prepare("SELECT value FROM records WHERE kind='accessGrants'").all().map((r) => parse(r.value)),
      jobs: db.prepare("SELECT j.value, i.value AS input, r.value AS result FROM jobs j LEFT JOIN inputs i ON i.id=j.id LEFT JOIN results r ON r.id=j.id").all().map((r) => ({ ...parse(r.value), input: r.input ? parse(r.input) : null, result: r.result ? parse(r.result) : null, events: [] })),
      requests: db.prepare("SELECT value FROM requests").all().map((r) => parse(r.value))
    };
    if (db.prepare("PRAGMA foreign_key_check").all().length) throw new Error("Connector database foreign key violation");
    for (const row of db.prepare("SELECT id,value FROM jobs").all()) { if (parse(row.value).id !== row.id) throw new Error("Connector job identity mismatch"); }
    for (const row of db.prepare("SELECT kind,id,value FROM records").all()) { const record=parse(row.value); if (row.id !== (row.kind === "connectors" ? connectorKey(record) : record.id)) throw new Error("Connector identity mismatch"); }
    const jobs = new Map(state.jobs.map((job) => [job.id, job]));
    for (const row of db.prepare("SELECT job_id,seq,value FROM events ORDER BY job_id,seq").all()) { const event=parse(row.value); if(event.seq !== row.seq || !jobs.has(row.job_id)) throw new Error("Connector event identity mismatch"); jobs.get(row.job_id).events.push(event); }
    const validatedState = normalizeConnectorState(state);
    if (legacy !== undefined) {
      const matchesMarker = parsed?.schema === sqliteMarkerSchema && parsed.databaseId === getMeta("uuid");
      const matchesImport = createHash("sha256").update(legacy).digest("hex") === getMeta("legacySha256");
      if (!matchesMarker && !matchesImport) throw new Error("Legacy connector store diverged from migrated database; recovery required");
      if (!matchesMarker) {
        // FULL commit is durable before replacing the old secret-bearing JSON.
        // A crash before the replacement resumes by exact import hash above.
        db.exec("PRAGMA wal_checkpoint(FULL)");
        await atomicFile(filePath, json({ schema: sqliteMarkerSchema, database: databaseName, databaseId: getMeta("uuid"), legacySha256: getMeta("legacySha256") }) + "\n");
      }
    } else {
      await atomicFile(filePath, json({ schema: sqliteMarkerSchema, database: databaseName, databaseId: getMeta("uuid") }) + "\n");
    }
    return { state: validatedState, revision };
  }

  function putRecord(kind, id, record) {
    db.prepare("INSERT OR REPLACE INTO records VALUES (?,?,?)").run(kind, id, json(record));
  }

  function putJob(change) {
    const oldRoute = change.input === undefined ? db.prepare("SELECT route FROM jobs WHERE id=?").get(change.id)?.route : null;
    const route = oldRoute || json({
      name: String(change.input?.name || ""), text: String(change.input?.text || "").slice(0,120),
      runAs: change.input?.runAs, timeoutMs: change.input?.timeoutMs,
      scriptSha256: createHash("sha256").update(String(change.input?.script || "")).digest("hex")
    });
    db.prepare("INSERT INTO jobs VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value,route=excluded.route").run(change.id, json(change.metadata), route);
    if (change.input !== undefined) db.prepare("INSERT OR REPLACE INTO inputs VALUES (?,?)").run(change.id, json(change.input));
    if (change.result !== undefined) db.prepare("INSERT OR REPLACE INTO results VALUES (?,?)").run(change.id, json(change.result));
    for (const event of change.events || []) db.prepare("INSERT INTO events VALUES (?,?,?)").run(change.id, event.seq, json(event));
    if (change.firstSeq) db.prepare("DELETE FROM events WHERE job_id=? AND seq<?").run(change.id, change.firstSeq);
  }

  function commit(delta) {
    db.exec("BEGIN IMMEDIATE");
    try {
      if (Number(getMeta("revision")) !== delta.revision) throw Object.assign(new Error("Connector store has another writer; restart required"), { code: "STORE_STALE_WRITER" });
      for (const kind of ["connectors", "accessGrants"]) {
        for (const id of delta[kind].removed) db.prepare("DELETE FROM records WHERE kind=? AND id=?").run(kind,id);
        for (const record of delta[kind].changed) putRecord(kind, kind === "connectors" ? connectorKey(record) : record.id, record);
      }
      for (const id of delta.jobs.removed) db.prepare("DELETE FROM jobs WHERE id=?").run(id);
      for (const change of delta.jobs.changed) putJob(change);
      for (const request of delta.requests) db.prepare("INSERT INTO requests VALUES (?,?)").run(request.id, json(request));
      putMeta("revision", ++revision);
      db.exec("COMMIT");
      return { revision };
    } catch (error) {
      let rolledBack = false;
      try {
        // A revision read on this same connection is not independent evidence
        // while an uncommitted transaction might still be open. Failed
        // ROLLBACK always fences; only its successful completion permits reuse.
        db.exec("ROLLBACK");
        revision = Number(getMeta("revision"));
        rolledBack = revision === delta.revision;
      } catch { /* Reconciliation requires a fresh process. */ }
      if (!rolledBack) throw Object.assign(new Error("Connector commit outcome is ambiguous; restart required"), { code: "STORE_COMMIT_AMBIGUOUS" });
      throw error;
    }
  }

  parentPort.on("message", (request) => {
    chain = chain.then(async () => {
      try {
        let value;
        if (request.action === "load") value = await load();
        else if (request.action === "commit") value = commit(request.value);
        else if (request.action === "close") { db?.close(); db=null; owner?.close(); owner=null; value = true; }
        else throw new Error("Unknown persistence action");
        parentPort.postMessage({ id: request.id, value });
      } catch (error) {
        if (request.action === "load") { db?.close(); db=null; owner?.close(); owner=null; }
        parentPort.postMessage({ id: request.id, error: { message: error instanceof SyntaxError ? "Malformed connector database record" : error.message, code: error.code } });
      }
    });
  });
}

export function connectorKey(record) { return JSON.stringify([record.linkId, record.deviceId, record.connectorId]); }
export function metadata(job) { const { input, result, events, ...rest } = job; return rest; }

export async function atomicFile(filePath, content) {
  const temporary = `${filePath}.${randomUUID()}.next`;
  try {
    const handle = await open(temporary, "wx", 0o600);
    try { await handle.writeFile(content, "utf8"); await handle.sync(); } finally { await handle.close(); }
    await rename(temporary, filePath);
    await syncDirectory(path.dirname(filePath));
  } catch (error) {
    await unlink(temporary).catch((cleanup) => { if (cleanup.code !== "ENOENT") throw cleanup; });
    throw error;
  }
}

export async function syncDirectory(directoryPath) {
  if (process.platform !== "win32") {
    const directory = await open(directoryPath, "r");
    try { await directory.sync(); } finally { await directory.close(); }
  }
}

// A separate, nonsecret SQLite file holds only an OS-backed exclusive lock.
// Its transaction is never committed and vanishes with the owner process.
// Read-only registry tools use the data DB without taking this writer lock.
export async function acquireConnectorOwner(dataDir) {
  const { DatabaseSync } = await import("node:sqlite");
  const file = path.join(dataDir,"connector-owner.sqlite");
  const guard = new DatabaseSync(file);
  try {
    await chmod(file,0o600);
    guard.exec("PRAGMA busy_timeout=0; BEGIN EXCLUSIVE;");
    return guard;
  } catch {
    guard.close();
    throw new Error("Connector store already has a serving or maintenance owner");
  }
}

export async function rejectRollbackIntent(dataDir) {
  try { await stat(path.join(dataDir,"connector-rollback.json")); }
  catch (error) { if(error.code === "ENOENT") return; throw error; }
  throw new Error("Connector rollback is incomplete; resume offline rollback before startup");
}
