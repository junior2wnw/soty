import { readFile, stat, unlink } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { atomicFile, acquireConnectorOwner, rejectRollbackIntent, syncDirectory } from "./connector-persistence.js";
import { readConnectorState } from "./connector-registry.js";
import { normalizeConnectorState } from "./connector-store.js";

const terminal = new Set(["succeeded", "failed", "cancelled"]);
const canonical = (value) => Array.isArray(value) ? value.map(canonical)
  : value && typeof value === "object" ? Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonical(value[key])])) : value;
export function queuedFingerprint(jobs) {
  const pending = jobs.filter((job) => !terminal.has(job.status));
  // A retried lease is not proof that execution never began on a legacy client.
  if (!pending.length || pending.some((job) => job.status !== "queued" || job.attempts !== 0 || job.connectorId
    || job.events.some((event) => ["leased", "started", "running", "retry"].includes(event.type)))) return null;
  return createHash("sha256").update(JSON.stringify(canonical(pending.sort((a,b) => a.id.localeCompare(b.id))))).digest("hex");
}
const allowedPending = (status, expected) => status.count === 0 ? !expected
  : /^[a-f0-9]{64}$/u.test(expected || "") && status.queuedSha256 === expected;

export async function maintenanceStatus(dataDir) {
  await rejectRollbackIntent(dataDir);
  let maintenance = false;
  try { await stat(path.join(dataDir,"connector-maintenance.json")); maintenance = true; } catch (error) { if (error.code !== "ENOENT") throw error; }
  let db;
  let jobs;
  try {
    await stat(path.join(dataDir,"connector-store.sqlite"));
    db = new DatabaseSync(path.join(dataDir,"connector-store.sqlite"), { readOnly: true });
    if (db.prepare("SELECT value FROM meta WHERE key='schema'").get()?.value !== "soty.connector-sqlite.v1") throw new Error("Unknown connector database");
    db.exec("BEGIN");
    jobs = db.prepare("SELECT j.value,i.value AS input,r.value AS result FROM jobs j LEFT JOIN inputs i ON i.id=j.id LEFT JOIN results r ON r.id=j.id WHERE json_extract(j.value,'$.status') NOT IN ('succeeded','failed','cancelled')").all()
      .map((row) => ({ ...JSON.parse(row.value), input: JSON.parse(row.input), result: JSON.parse(row.result || "null"), events: [] }));
    const events = db.prepare("SELECT value FROM events WHERE job_id=? ORDER BY seq");
    for (const job of jobs) job.events = events.all(job.id).map((row) => JSON.parse(row.value));
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    jobs = (await readConnectorState(dataDir)).jobs;
  } finally { db?.close(); }
  const activeJobs = jobs.filter((job) => !["succeeded","failed","cancelled"].includes(job.status)).map(({ id,status }) => ({ id,status }));
  return { ok: true, schema: "soty.connector-maintenance.v1", activeJobs, count: activeJobs.length, maintenance, queuedSha256: queuedFingerprint(jobs) };
}

// Enter/rollback are OFFLINE operations. The rollout must first stop and
// re-check the exact serving container; the old c0ca image ignores the marker.
export async function connectorMaintenance(dataDir, action, { checkpoint = async () => {}, preserveQueuedSha256 = null } = {}) {
  const marker = path.join(dataDir,"connector-maintenance.json");
  if (action === "status") return await maintenanceStatus(dataDir);
  if (action === "leave") {
    await rejectRollbackIntent(dataDir);
    await unlink(marker).catch((error) => { if (error.code !== "ENOENT") throw error; });
    await syncDirectory(dataDir);
    return await maintenanceStatus(dataDir);
  }
  if (!["enter","rollback"].includes(action)) throw new Error("Unknown connector maintenance operation");
  const owner = await acquireConnectorOwner(dataDir);
  try {
    if (action === "enter") {
      const before = await maintenanceStatus(dataDir);
      if (!allowedPending(before, preserveQueuedSha256)) throw new Error("Connector maintenance requires no queued or assigned jobs unless an exact reviewed never-leased queued fingerprint matches");
      await atomicFile(marker, JSON.stringify({ schema: "soty.connector-maintenance.v1", preserveQueuedSha256 }) + "\n");
      const after = await maintenanceStatus(dataDir);
      if (!allowedPending(after, preserveQueuedSha256)) throw new Error("Jobs changed before offline admission barrier; abort rollout");
      return after;
    }
    const maintenance = JSON.parse(await readFile(marker, "utf8")); // No implicit downgrade outside a reviewed offline cutover.
    const expectedQueued = maintenance.preserveQueuedSha256 || null;
    if (expectedQueued !== preserveQueuedSha256) throw new Error("Queued rollback approval does not match maintenance");
    const journalPath = path.join(dataDir,"connector-rollback.json");
    const legacyPath = path.join(dataDir,"connector-store.json");
    const dbPath = path.join(dataDir,"connector-store.sqlite");
    const digest = (text) => createHash("sha256").update(text).digest("hex");
    let intent;
    try { intent = JSON.parse(await readFile(journalPath,"utf8")); } catch (error) { if (error.code !== "ENOENT") throw error; }
    let databaseId = null;
    let partial = false;
    try {
      await stat(dbPath);
      const db = new DatabaseSync(dbPath,{readOnly:true});
      try {
        partial = db.prepare("SELECT count(*) AS count FROM sqlite_master WHERE type='table'").get().count === 0;
        if (!partial) {
          if (db.prepare("SELECT value FROM meta WHERE key='schema'").get()?.value !== "soty.connector-sqlite.v1") throw new Error("Unknown rollback database");
          databaseId = db.prepare("SELECT value FROM meta WHERE key='uuid'").get()?.value;
        }
      } finally { db.close(); }
    } catch (error) { if (error.code !== "ENOENT") throw error; }
    let content;
    if (intent) {
      if (intent.schema !== "soty.connector-rollback.v1" || !/^[a-f0-9]{64}$/u.test(intent.sha256) || (databaseId && databaseId !== intent.databaseId)) throw new Error("Invalid rollback intent or database identity");
      const current = await readFile(legacyPath,"utf8");
      if (digest(current) === intent.sha256) content = current;
    }
    if (!content) {
      let state;
      if (partial) {
        // Atomic SQLite schema creation can leave a genuinely empty DB when
        // interrupted. Its still-valid legacy source is the recovery authority.
        state = normalizeConnectorState(JSON.parse(await readFile(legacyPath,"utf8")));
      } else state = await readConnectorState(dataDir,{ignoreRollbackIntent:true});
      if (!allowedPending({count:state.jobs.filter((job)=>!terminal.has(job.status)).length,queuedSha256:queuedFingerprint(state.jobs)}, expectedQueued)) throw new Error("Rollback cannot change pending jobs");
      if (state.requests.length) throw new Error("Cannot downgrade accepted durable request identities to a legacy server");
      content = JSON.stringify(state,null,2) + "\n";
      if (intent && digest(content) !== intent.sha256) throw new Error("Rollback state changed after durable intent");
    }
    const checked = normalizeConnectorState(JSON.parse(content));
    const checkedActive = checked.jobs.filter((job)=>!terminal.has(job.status));
    if (checked.requests.length || !allowedPending({count:checkedActive.length,queuedSha256:queuedFingerprint(checked.jobs)}, expectedQueued)) throw new Error("Rollback cannot discard accepted identities or active work");
    if (!intent) {
      intent = {schema:"soty.connector-rollback.v1",databaseId,sha256:digest(content)};
      await atomicFile(journalPath,JSON.stringify(intent)+"\n");
    }
    await checkpoint("intent");
    await atomicFile(legacyPath,content);
    await checkpoint("legacy");
    // The durable intent makes every interrupted disposition explicit. No
    // reader or server may choose either store until this operation resumes.
    for (const suffix of ["-wal","-shm",""]) {
      await unlink(`${dbPath}${suffix}`).catch((error)=>{if(error.code!=="ENOENT")throw error;});
      await checkpoint(`remove${suffix || "-database"}`);
    }
    await unlink(journalPath);
    await syncDirectory(dataDir);
    return { ok:true,schema:"soty.connector-maintenance.v1",maintenance:true,count:checkedActive.length,
      activeJobs:checkedActive.map(({id,status})=>({id,status})),queuedSha256:queuedFingerprint(checked.jobs),
      rollback:"legacy-json",bytes:Buffer.byteLength(content),sha256:intent.sha256 };
  } finally {
    owner.close();
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const preserveQueuedSha256 = process.argv[3] === "--preserve-queued-sha256" ? process.argv[4] : null;
    if (process.argv.length > 3 && !/^[a-f0-9]{64}$/u.test(preserveQueuedSha256 || "")) throw new Error("Invalid queued fingerprint argument");
    const value = await connectorMaintenance(process.env.DATA_DIR || "/data", process.argv[2], { preserveQueuedSha256 });
    console.log(JSON.stringify(value));
  } catch (error) {
    // Error text is controlled or an OS path/code; never include state/config.
    console.error(JSON.stringify({ ok: false, error: error.message }));
    process.exitCode = 1;
  }
}
