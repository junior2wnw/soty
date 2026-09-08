import { DatabaseSync } from "node:sqlite";
import { readFile, stat } from "node:fs/promises";
import { createHash } from "node:crypto";
import path from "node:path";
import { rejectRollbackIntent } from "./connector-persistence.js";
import { normalizeConnectorState } from "./connector-store.js";

const markerSchema = "soty.connector-store.sqlite.v1";
const parse = JSON.parse;

async function database(dataDir, { ignoreRollbackIntent = false } = {}) {
  if (!ignoreRollbackIntent) await rejectRollbackIntent(dataDir);
  const file = path.join(dataDir, "connector-store.sqlite");
  try { await stat(file); } catch (error) { if (error.code === "ENOENT") return null; throw error; }
  const db = new DatabaseSync(file, { readOnly: true });
  try {
    db.exec("PRAGMA query_only=ON; PRAGMA busy_timeout=1000; BEGIN");
    if (db.prepare("SELECT value FROM meta WHERE key='schema'").get()?.value !== "soty.connector-sqlite.v1") throw new Error("Unsupported connector registry schema");
    let markerText, marker;
    try { markerText=await readFile(path.join(dataDir,"connector-store.json"),"utf8"); marker=parse(markerText); } catch { throw new Error("Connector registry authority marker missing or malformed"); }
    const meta = (key) => db.prepare("SELECT value FROM meta WHERE key=?").get(key)?.value;
    if (!(marker.schema === markerSchema && marker.databaseId === meta("uuid"))
      && createHash("sha256").update(markerText).digest("hex") !== meta("legacySha256")) throw new Error("Connector registry authority diverged; offline recovery required");
    return db;
  } catch (error) { db.close(); throw error; }
}

async function legacy(dataDir) {
  let value;
  try { value = parse(await readFile(path.join(dataDir, "connector-store.json"), "utf8")); } catch { throw new Error("Legacy connector registry is unreadable or malformed"); }
  if (value?.schema === markerSchema) throw new Error("Migrated connector database is missing; no legacy fallback");
  return normalizeConnectorState(value);
}

const proven = (job) => job.status === "succeeded" && job.input?.runAs === "user"
  && String(job.input?.name || job.input?.text || "") === "laptop-corporate-bootstrap-ufn-corp-1.0.0.ps1"
  && String(job.result?.text || "").includes("UFN-CORP-1.0.0");

function compact(job, jobId) {
  const { events, input, result, ...rest } = job;
  return {
    ...rest,
    input: { name: input?.name || "", text: String(input?.text || "").slice(0,120), runAs: input?.runAs, timeoutMs: input?.timeoutMs,
      scriptSha256: input?.scriptSha256 || createHash("sha256").update(String(input?.script || "")).digest("hex") },
    result: job.id === jobId && result ? { ...result, text: String(result.text || "").slice(0,65536), truncated: String(result.text || "").length > 65536 } : null
  };
}

// This is an operator-side read-only adapter, not a public authentication route.
// Link IDs stay inside the existing privileged bridge process; hashes, scripts,
// unrelated job results and event history are never returned to the bridge.
export async function readConnectorRegistry(dataDir, { jobId = "" } = {}) {
  const db = await database(dataDir);
  let connectors, jobs, routeJob;
  if (db) {
    try {
      connectors = db.prepare("SELECT value FROM records WHERE kind='connectors'").all().map((row) => parse(row.value));
      jobs = db.prepare("SELECT value,route FROM jobs ORDER BY json_extract(value,'$.createdAt')").all().map((row) => ({ ...parse(row.value), input: parse(row.route) }));
      const requested = jobs.find((job) => job.id === jobId);
      if (requested) requested.result = parse(db.prepare("SELECT value FROM results WHERE id=?").get(jobId)?.value || "null");
      for (const job of [...jobs].reverse()) {
        if (job.status !== "succeeded" || job.input?.runAs !== "user" || String(job.input?.name || job.input?.text || "") !== "laptop-corporate-bootstrap-ufn-corp-1.0.0.ps1") continue;
        const result = parse(db.prepare("SELECT value FROM results WHERE id=?").get(job.id)?.value || "null");
        if (proven({ ...job, result })) { routeJob = { ...job, result }; break; }
      }
    } finally { db.close(); }
  } else {
    const state = await legacy(dataDir);
    connectors = state.connectors;
    jobs = state.jobs;
    routeJob = [...jobs].reverse().find(proven);
  }
  return {
    schema: "soty.connector-registry.v1",
    connectors: connectors.map(({ tokenHash, ...item }) => item),
    jobs: jobs.map((job) => compact(job,jobId)),
    routeJob: routeJob ? { ...compact(routeJob, ""), result: routeJob.result } : null
  };
}

// Complete explicit offline retrieval for migration/rollback and regression
// fixtures. Never print this object or serve it without owner/grant checks.
export async function readConnectorState(dataDir, options = {}) {
  const db = await database(dataDir, options);
  if (!db) return await legacy(dataDir);
  try {
    const state = {
      schema: "soty.connector-store.v2",
      connectors: db.prepare("SELECT value FROM records WHERE kind='connectors'").all().map((row) => parse(row.value)),
      accessGrants: db.prepare("SELECT value FROM records WHERE kind='accessGrants'").all().map((row) => parse(row.value)),
      jobs: db.prepare("SELECT j.value,i.value AS input,r.value AS result FROM jobs j LEFT JOIN inputs i ON i.id=j.id LEFT JOIN results r ON r.id=j.id").all().map((row) => ({ ...parse(row.value), input: row.input ? parse(row.input) : null, result: parse(row.result || "null"), events: [] })),
      requests: db.prepare("SELECT value FROM requests").all().map((row) => parse(row.value))
    };
    if (db.prepare("PRAGMA foreign_key_check").all().length) throw new Error("Connector database foreign key violation");
    for (const row of db.prepare("SELECT id,value FROM jobs").all()) if (parse(row.value).id !== row.id) throw new Error("Connector job identity mismatch");
    const jobs = new Map(state.jobs.map((job) => [job.id,job]));
    for (const event of db.prepare("SELECT job_id,value FROM events ORDER BY job_id,seq").all()) jobs.get(event.job_id).events.push(parse(event.value));
    return normalizeConnectorState(state);
  } finally { db.close(); }
}
