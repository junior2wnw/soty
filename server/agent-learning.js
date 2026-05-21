import express from "express";
import { randomUUID } from "node:crypto";
import { appendFile, mkdir, readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { buildMemoryQuery, buildTeacherReport, parseReceiptLine, safeLimit, summarizeLearningScope } from "./agent-learning/report.js";
import { cleanEnum, cleanHash, cleanIso, cleanSignature, cleanTaskSignature, cleanText, hashShort, redactLearningText } from "./agent-learning/sanitize.js";

export { buildMemoryControl, buildMemoryQuery, buildTeacherReport } from "./agent-learning/report.js";

const maxReceiptsPerRequest = 80;
const maxReceiptText = 900;
const defaultMemoryScanFiles = Math.max(7, Math.min(Number.parseInt(process.env.SOTY_MEMORY_SCAN_FILES || "90", 10) || 90, 3650));
const jsonParser = express.json({ limit: "320kb", type: "application/json" });

export function attachAgentLearning(app, { dataDir } = {}) {
  const learningDir = process.env.SOTY_LEARNING_DIR || path.join(dataDir || process.cwd(), "learning");

  app.get("/api/agent/memory/health", async (_req, res) => {
    const files = await learningFiles(learningDir).catch(() => []);
    const lines = await readRecentLearningReceiptsFromDir(learningDir, 500).catch(() => []);
    const receipts = lines.map(parseReceiptLine).filter(Boolean);
    res.json({
      ok: true,
      enabled: true,
      schema: "soty.memory-plane.v1",
      controller: "soty.memctl.v1",
      backend: "append-only-jsonl",
      files: files.length,
      receipts: receipts.length,
      scope: summarizeLearningScope(receipts),
      queryUrl: "/api/agent/memory/query",
      receiptsUrl: "/api/agent/memory/receipts",
      reportUrl: "/api/agent/memory/report",
      dir: path.basename(learningDir)
    });
  });

  app.get("/api/agent/memory/report", async (req, res) => {
    const limit = safeLimit(req.query?.limit, 800);
    const lines = await readRecentLearningReceiptsFromDir(learningDir, limit).catch(() => []);
    const receipts = lines.map(parseReceiptLine).filter(Boolean);
    res.json(buildTeacherReport(receipts, { limit }));
  });

  app.get("/api/agent/memory/query", async (req, res) => {
    const limit = safeLimit(req.query?.limit, 800);
    const family = cleanText(req.query?.family, 80);
    const platform = cleanText(req.query?.platform, 40);
    const taskSig = cleanText(req.query?.taskSig, 160);
    const lines = await readRecentLearningReceiptsFromDir(learningDir, limit).catch(() => []);
    const receipts = lines.map(parseReceiptLine).filter(Boolean);
    const report = buildTeacherReport(receipts, { limit, family, platform, taskSig });
    res.json(buildMemoryQuery(report, { family, platform, taskSig }));
  });

  app.post("/api/agent/memory/receipts", jsonParser, async (req, res) => {
    await handleReceiptPost(req, res, learningDir);
  });

  app.get("/api/agent/learning/health", async (_req, res) => {
    const files = await learningFiles(learningDir).catch(() => []);
    const lines = await readRecentLearningReceiptsFromDir(learningDir, 500).catch(() => []);
    const receipts = lines.map(parseReceiptLine).filter(Boolean);
    res.json({
      ok: true,
      enabled: true,
      files: files.length,
      receipts: receipts.length,
      scope: summarizeLearningScope(receipts),
      teacherUrl: "/api/agent/learning/teacher",
      dir: path.basename(learningDir)
    });
  });

  app.get("/api/agent/learning/teacher", async (req, res) => {
    const limit = safeLimit(req.query?.limit, 800);
    const lines = await readRecentLearningReceiptsFromDir(learningDir, limit).catch(() => []);
    const receipts = lines.map(parseReceiptLine).filter(Boolean);
    res.json(buildTeacherReport(receipts, { limit }));
  });

  app.post("/api/agent/learning/receipts", jsonParser, async (req, res) => {
    await handleReceiptPost(req, res, learningDir);
  });
}

async function handleReceiptPost(req, res, learningDir) {
  const envelope = cleanEnvelope(req.body);
  const receipts = cleanReceipts(req.body?.receipts);
  if (receipts.length === 0) {
    res.status(400).json({ ok: false, accepted: 0 });
    return;
  }

  const now = new Date();
  const partition = now.toISOString().slice(0, 10);
  const file = path.join(learningDir, `${partition}.jsonl`);
  await mkdir(learningDir, { recursive: true, mode: 0o700 });
  const lines = receipts.map((receipt) => JSON.stringify({
    schema: "soty.memory.receipt.v1",
    id: `mem_${randomUUID()}`,
    receivedAt: now.toISOString(),
    installHash: hashShort(envelope.installId || envelope.relayId || "unknown"),
    agentVersion: envelope.agentVersion,
    privacy: "sanitized",
    ...receipt
  }));
  await appendFile(file, `${lines.join("\n")}\n`, { encoding: "utf8", mode: 0o600 });
  res.json({ ok: true, accepted: receipts.length });
}

async function learningFiles(dir) {
  const items = await readdir(dir, { withFileTypes: true });
  return items.filter((item) => item.isFile() && item.name.endsWith(".jsonl"));
}

export async function readRecentLearningReceipts(dataDir, limit = 50) {
  const learningDir = process.env.SOTY_LEARNING_DIR || path.join(dataDir || process.cwd(), "learning");
  return await readRecentLearningReceiptsFromDir(learningDir, limit);
}

async function readRecentLearningReceiptsFromDir(learningDir, limit = 50) {
  const files = (await learningFiles(learningDir))
    .map((item) => item.name)
    .sort()
    .slice(-defaultMemoryScanFiles);
  const lines = [];
  for (const file of files) {
    const text = await readFile(path.join(learningDir, file), "utf8").catch(() => "");
    for (const line of text.split(/\r?\n/u)) {
      if (line.trim()) {
        lines.push(line);
      }
    }
  }
  return lines.slice(-Math.max(1, Math.min(2000, limit)));
}

function cleanEnvelope(value) {
  const record = value && typeof value === "object" ? value : {};
  return {
    installId: cleanText(record.installId, 120),
    relayId: cleanText(record.relayId, 192),
    agentVersion: cleanText(record.agentVersion, 40)
  };
}

function cleanReceipts(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map(cleanReceipt)
    .filter(Boolean)
    .slice(0, maxReceiptsPerRequest);
}

function cleanReceipt(value) {
  if (!value || typeof value !== "object") {
    return null;
  }
  const kind = cleanEnum(value.kind, ["codex-turn", "source-command", "agent-runtime", "action-job", "route-improvement"], "agent-runtime");
  const result = cleanEnum(value.result, ["ok", "failed", "partial", "blocked", "timeout", "cancelled"], "failed");
  const exitCode = Number.isSafeInteger(value.exitCode) ? Math.max(-32768, Math.min(32767, value.exitCode)) : undefined;
  return {
    kind,
    result,
    toolkit: cleanText(value.toolkit, 80),
    phase: cleanText(value.phase, 80),
    family: cleanText(value.family, 80),
    platform: cleanText(value.platform, 40),
    codexMode: cleanText(value.codexMode, 80),
    route: cleanText(value.route, 120),
    commandSig: cleanSignature(value.commandSig, cleanText(value.family, 80)),
    taskSig: cleanTaskSignature(value.taskSig),
    proof: redactLearningText(value.proof).slice(0, maxReceiptText),
    targetLabel: cleanText(value.targetLabel, 80),
    sourceDeviceNick: cleanText(value.sourceDeviceNick, 80),
    targetHash: cleanHash(value.targetHash),
    sourceDeviceHash: cleanHash(value.sourceDeviceHash),
    dialogHash: cleanHash(value.dialogHash),
    durationMs: Number.isSafeInteger(value.durationMs) ? Math.max(0, Math.min(86_400_000, value.durationMs)) : undefined,
    ...(exitCode === undefined ? {} : { exitCode }),
    memorySchema: cleanText(value.memorySchema || "soty.memory.receipt.v1", 80),
    createdAt: cleanIso(value.createdAt) || new Date().toISOString()
  };
}
