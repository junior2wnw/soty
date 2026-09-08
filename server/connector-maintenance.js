import { readFile, stat, unlink } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { atomicFile, acquireConnectorOwner, rejectRollbackIntent, syncDirectory } from "./connector-persistence.js";
import { readConnectorState } from "./connector-registry.js";
import { normalizeConnectorState } from "./connector-store.js";

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
    jobs = db.prepare("SELECT value FROM jobs").all().map((row) => JSON.parse(row.value));
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    jobs = (await readConnectorState(dataDir)).jobs;
  } finally { db?.close(); }
  const activeJobs = jobs.filter((job) => !["succeeded","failed","cancelled"].includes(job.status)).map(({ id,status }) => ({ id,status }));
  return { ok: true, schema: "soty.connector-maintenance.v1", activeJobs, count: activeJobs.length, maintenance };
}

// Enter/rollback are OFFLINE operations. The rollout must first stop and
// re-check the exact serving container; the old c0ca image ignores the marker.
export async function connectorMaintenance(dataDir, action, { checkpoint = async () => {} } = {}) {
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
      if (before.count) throw new Error("Connector maintenance requires no queued or assigned jobs");
      await atomicFile(marker, JSON.stringify({ schema: "soty.connector-maintenance.v1" }) + "\n");
      const after = await maintenanceStatus(dataDir);
      if (after.count) throw new Error("Jobs appeared before offline admission barrier; abort rollout");
      return after;
    }
    await stat(marker); // No implicit downgrade outside a reviewed offline cutover.
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
      if (state.jobs.some((job)=>!["succeeded","failed","cancelled"].includes(job.status))) throw new Error("Rollback requires no active jobs");
      if (state.requests.length) throw new Error("Cannot downgrade accepted durable request identities to a legacy server");
      content = JSON.stringify(state,null,2) + "\n";
      if (intent && digest(content) !== intent.sha256) throw new Error("Rollback state changed after durable intent");
    }
    const checked = normalizeConnectorState(JSON.parse(content));
    if (checked.requests.length || checked.jobs.some((job)=>!["succeeded","failed","cancelled"].includes(job.status))) throw new Error("Rollback cannot discard accepted identities or active work");
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
    return { ok:true,schema:"soty.connector-maintenance.v1",maintenance:true,count:0,
      rollback:"legacy-json",bytes:Buffer.byteLength(content),sha256:intent.sha256 };
  } finally {
    owner.close();
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const value = await connectorMaintenance(process.env.DATA_DIR || "/data", process.argv[2]);
    console.log(JSON.stringify(value));
  } catch (error) {
    // Error text is controlled or an OS path/code; never include state/config.
    console.error(JSON.stringify({ ok: false, error: error.message }));
    process.exitCode = 1;
  }
}
