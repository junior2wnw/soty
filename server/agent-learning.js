import express from "express";
import { createHash, randomUUID } from "node:crypto";
import { appendFile, mkdir, readdir, readFile } from "node:fs/promises";
import path from "node:path";

const maxReceiptsPerRequest = 80;
const maxReceiptText = 900;
const signatureMaxSource = 2000;
const jsonParser = express.json({ limit: "320kb", type: "application/json" });

export function attachAgentLearning(app, { dataDir } = {}) {
  const learningDir = process.env.SOTY_LEARNING_DIR || path.join(dataDir || process.cwd(), "learning");

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
      schema: "soty.learning.receipt.v1",
      id: `learn_${randomUUID()}`,
      receivedAt: now.toISOString(),
      installHash: hashShort(envelope.installId || envelope.relayId || "unknown"),
      agentVersion: envelope.agentVersion,
      ...receipt
    }));
    await appendFile(file, `${lines.join("\n")}\n`, { encoding: "utf8", mode: 0o600 });
    res.json({ ok: true, accepted: receipts.length });
  });
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
    .slice(-7);
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

export function buildTeacherReport(receipts, { limit = 800 } = {}) {
  const rows = receipts.filter((item) => item && typeof item === "object");
  const familyCounts = countBy(rows, (item) => item.family || "generic");
  const resultCounts = countBy(rows, (item) => item.result || "unknown");
  const routeCounts = countBy(rows, (item) => item.route || "unknown");
  const topFailures = groupRows(rows
    .filter((item) => ["failed", "blocked", "timeout", "partial"].includes(item.result)), (item) => [
      item.family || "generic",
      item.result || "failed",
      item.toolkit || "",
      item.phase || "",
      item.route || "unknown",
      Number.isSafeInteger(item.exitCode) ? String(item.exitCode) : "no-exit"
    ].join("|"));
  const topSuccesses = groupRows(rows
    .filter((item) => item.result === "ok"), (item) => [
      item.family || "generic",
      item.toolkit || "",
      item.phase || "",
      item.route || "unknown"
    ].join("|"));
  const recommendations = buildRecommendations(rows, topFailures);
  const candidates = buildPromotionCandidates(rows, topFailures, topSuccesses);
  const scope = summarizeLearningScope(rows);
  return {
    ok: true,
    schema: "soty.learning.teacher.v1",
    generatedAt: new Date().toISOString(),
    source: "server-global-sanitized-receipts",
    limit,
    receipts: rows.length,
    scope,
    families: topEntries(familyCounts, 12),
    results: topEntries(resultCounts, 8),
    routes: topEntries(routeCounts, 8),
    topFailures: topFailures.slice(0, 12),
    topSuccesses: topSuccesses.slice(0, 8),
    recommendations,
    candidates,
    oneCommand: "sotyctl learn doctor",
    reviewMergeCommand: "sotyctl learn review-merge",
    publishModel: "reviewed-ops-patch-then-build-release-deploy"
  };
}

function parseReceiptLine(line) {
  try {
    const value = JSON.parse(line);
    return value && typeof value === "object" ? value : null;
  } catch {
    return null;
  }
}

function safeLimit(value, fallback) {
  const parsed = Number.parseInt(String(value || ""), 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(1, Math.min(2000, parsed));
}

function countBy(rows, keyFn) {
  const counts = new Map();
  for (const row of rows) {
    const key = cleanText(keyFn(row), 120) || "unknown";
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return counts;
}

function topEntries(counts, limit) {
  return Array.from(counts.entries())
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .slice(0, limit)
    .map(([key, count]) => ({ key, count }));
}

function groupRows(rows, keyFn) {
  const groups = new Map();
  for (const row of rows) {
    const key = cleanText(keyFn(row), 240) || "unknown";
    const current = groups.get(key) || {
      key,
      count: 0,
      family: cleanText(row.family, 80) || "generic",
      toolkit: cleanText(row.toolkit, 80),
      phase: cleanText(row.phase, 80),
      result: cleanText(row.result, 40),
      route: cleanText(row.route, 120),
      exitCode: Number.isSafeInteger(row.exitCode) ? row.exitCode : undefined,
      firstSeenAt: cleanIso(row.createdAt) || cleanIso(row.receivedAt) || "",
      lastSeenAt: "",
      proofShape: proofShape(row.proof)
    };
    current.count += 1;
    current.lastSeenAt = cleanIso(row.createdAt) || cleanIso(row.receivedAt) || current.lastSeenAt;
    if (!current.proofShape && row.proof) {
      current.proofShape = proofShape(row.proof);
    }
    groups.set(key, current);
  }
  return Array.from(groups.values())
    .sort((left, right) => right.count - left.count || left.key.localeCompare(right.key));
}

function summarizeLearningScope(rows) {
  const timestamps = rows
    .map((item) => cleanIso(item.createdAt) || cleanIso(item.receivedAt) || "")
    .filter(Boolean)
    .sort();
  return {
    kind: "server-global-sanitized-receipts",
    deviceCount: uniqueCount(rows, (item) => item.installHash),
    platformCounts: topEntries(countBy(rows, (item) => item.platform || "unknown"), 8),
    agentVersions: topEntries(countBy(rows, (item) => item.agentVersion || "unknown"), 8),
    skillBundles: topEntries(countBy(rows, (item) => item.skillSha ? String(item.skillSha).slice(0, 12) : "unknown"), 8),
    firstSeenAt: timestamps[0] || "",
    lastSeenAt: timestamps.at(-1) || ""
  };
}

function uniqueCount(rows, keyFn) {
  const values = new Set();
  for (const row of rows) {
    const key = cleanText(keyFn(row), 120);
    if (key && key !== "unknown") {
      values.add(key);
    }
  }
  return values.size;
}

function buildRecommendations(rows, topFailures) {
  const recommendations = [];
  const windowsRows = rows.filter((item) => item.family === "windows-reinstall");
  const postArmLosses = postArmSourceDisconnectRows(rows);
  const slowRoutineGroups = slowRoutineCodexGroups(rows);
  const lowQualityGroups = lowQualityRouteGroups(rows);
  const reusableCapsules = reusableRouteCapsuleGroups(rows);
  for (const item of reusableCapsules.slice(0, 3)) {
    recommendations.push({
      priority: item.count >= 2 ? "normal" : "low",
      family: item.family,
      title: "Promote reusable route capsule",
      action: `Route capsule ${item.reuseKey} proved ${item.count} time(s). Reuse it for comparable tasks before broad discovery, and keep success criteria plus context boundary attached.`
    });
  }
  for (const item of lowQualityGroups.slice(0, 3)) {
    recommendations.push({
      priority: item.count >= 2 || item.score < 60 ? "high" : "normal",
      family: item.family,
      title: "Improve low-quality automatic route",
      action: `A runtime route returned quality=${item.quality} score=${item.score} for ${item.family}. Tighten the route, add a proof check, or fall back to Codex before answering. Evidence count: ${item.count}.`
    });
  }
  for (const item of slowRoutineGroups.slice(0, 3)) {
    recommendations.push({
      priority: item.totalTokens >= 100000 || item.durationMs >= 60000 ? "high" : "normal",
      family: item.family,
      title: "Promote slow routine Codex turn to fast runtime route",
      action: `A repeated routine used ${item.route || "codex"} with about ${item.durationMs}ms and ${item.totalTokens} tokens. Prefer a small source-scoped runtime script with structured proof before the next comparable chat.`
    });
  }
  if (postArmLosses.length > 0) {
    recommendations.push({
      priority: "high",
      family: "windows-reinstall",
      title: "Stop source probes after managed arm reboot",
      action: "When a managed reinstall arm succeeds with rebooting=true, treat source-stale/timeouts for the next boot window as expected. Do not call live source status; give the post-arm handoff and wait for the designed return path."
    });
  }
  const postArmLossSet = new Set(postArmLosses);
  const postArmFailureKeys = new Set(postArmLosses.map(failureGroupKey));
  const windowsFailures = windowsRows.filter((item) => item.result !== "ok" && !postArmLossSet.has(item));
  if (windowsRows.length > 0) {
    if (windowsFailures.length > 0) {
      recommendations.push({
        priority: "high",
        family: "windows-reinstall",
        title: "Do not start destructive reinstall until gates are proven",
        action: "Run the Soty/ops reinstall fast-lane gates first: exact target, data boundary, machine worker, trusted media, return path, and explicit destructive confirmation. Missing gates mean readiness-building, not manual reset."
      });
    } else {
      recommendations.push({
        priority: "normal",
        family: "windows-reinstall",
        title: "Promote the proven reinstall route into the device profile",
        action: "Store the working route, proof, and timing in windows-reinstall-profile.py so the next run starts from the proven path."
      });
    }
  }
  for (const failure of topFailures.filter((item) => !postArmFailureKeys.has(item.key)).slice(0, 5)) {
    if (failure.count >= 2) {
      recommendations.push({
        priority: failure.family === "windows-reinstall" ? "high" : "normal",
        family: failure.family,
        title: `Repeated ${failure.result} on ${failure.family}`,
        action: `Review ${failure.route || "unknown-route"} and promote a hot-route, helper fix, or stop gate. Evidence count: ${failure.count}.`
      });
    }
  }
  if (recommendations.length === 0) {
    recommendations.push({
      priority: "normal",
      family: "learning",
      title: "No repeated blocker yet",
      action: "Keep syncing receipts. One-off events stay as candidates; repeated proof becomes a route/profile patch."
    });
  }
  return recommendations.slice(0, 8);
}

function buildPromotionCandidates(rows, failures, successes) {
  const candidates = [];
  const memoryMarkers = latestMemoryMarkerRows(rows);
  for (const marker of memoryMarkers) {
    candidates.push({
      scope: marker.count >= 2 ? "promote" : "dialog",
      family: marker.family,
      marker: marker.marker
    });
  }
  const postArmLosses = postArmSourceDisconnectRows(rows);
  if (postArmLosses.length > 0) {
    candidates.push({
      scope: postArmLosses.length >= 2 ? "promote" : "candidate",
      family: "windows-reinstall",
      marker: `ops-memory: goal=Soty windows-reinstall post-arm reboot window | actual=source disconnect after managed arm reboot | success=do not probe source after rebooting=true | env=soty.learning.teacher count=${postArmLosses.length}`
    });
  }
  const postArmFailureKeys = new Set(postArmLosses.map(failureGroupKey));
  for (const failure of failures.filter((item) => !postArmFailureKeys.has(item.key)).slice(0, 6)) {
    const scope = failure.count >= 2 ? "promote" : "candidate";
    candidates.push({
      scope,
      family: failure.family,
      marker: `ops-memory: goal=Soty ${failure.family} ${failure.result} route | actual=${failure.result} via ${failure.route || "unknown-route"} | success=${failure.proofShape || "sanitized receipt"} | env=soty.learning.teacher count=${failure.count}`
    });
  }
  for (const success of successes.filter((item) => item.count >= 2).slice(0, 4)) {
    candidates.push({
      scope: "profile",
      family: success.family,
      marker: `ops-memory: goal=Soty ${success.family} proven route | actual=ok via ${success.route || "unknown-route"} | success=${success.proofShape || "sanitized receipt"} | env=soty.learning.teacher count=${success.count}`
    });
  }
  for (const slow of slowRoutineCodexGroups(rows).slice(0, 4)) {
    candidates.push({
      scope: slow.count >= 2 ? "promote" : "candidate",
      family: slow.family,
      marker: `ops-memory: goal=Soty ${slow.family} fast-route promotion | actual=slow routine via ${slow.route || "codex"} | success=durationMs=${slow.durationMs}; tokens=${slow.totalTokens}; prefer proofed runtime route | env=soty.learning.teacher count=${slow.count}`
    });
  }
  for (const quality of lowQualityRouteGroups(rows).slice(0, 4)) {
    candidates.push({
      scope: quality.count >= 2 ? "promote" : "candidate",
      family: quality.family,
      marker: `ops-memory: goal=Soty ${quality.family} route quality | actual=quality=${quality.quality}; score=${quality.score}; route=${quality.route || "unknown"} | success=patch proof checks or fallback before final answer | env=soty.learning.teacher count=${quality.count}`
    });
  }
  for (const capsule of reusableRouteCapsuleGroups(rows).slice(0, 4)) {
    candidates.push({
      scope: capsule.count >= 2 ? "profile" : "candidate",
      family: capsule.family,
      marker: `ops-memory: goal=Soty reusable route capsule ${capsule.reuseKey} | actual=${capsule.scriptUse || "reused-script"} via ${capsule.route || "unknown-route"} | success=count=${capsule.count}; successCriteria=${capsule.successCriteria ? "set" : "unset"}; context=${capsule.context || "unknown"} | env=soty.learning.teacher`
    });
  }
  return candidates.slice(0, 10);
}

function reusableRouteCapsuleGroups(rows) {
  const groups = new Map();
  for (const row of rows) {
    const capsule = learningRouteCapsule(row.proof);
    if (!capsule.reuseKey) {
      continue;
    }
    const family = cleanText(row.family, 80) || "generic";
    const route = cleanText(row.route, 120) || "unknown";
    const key = `${family}|${route}|${capsule.reuseKey}`;
    const current = groups.get(key) || {
      family,
      route,
      reuseKey: capsule.reuseKey,
      scriptUse: capsule.scriptUse,
      successCriteria: capsule.successCriteria,
      context: capsule.context,
      count: 0,
      lastSeenAt: ""
    };
    current.count += 1;
    current.lastSeenAt = cleanIso(row.createdAt) || cleanIso(row.receivedAt) || current.lastSeenAt;
    groups.set(key, current);
  }
  return Array.from(groups.values())
    .sort((left, right) => right.count - left.count || right.lastSeenAt.localeCompare(left.lastSeenAt));
}

function learningRouteCapsule(proof) {
  const text = String(proof || "").toLowerCase();
  return {
    reuseKey: proofField(text, "reusekey"),
    scriptUse: proofField(text, "scriptuse"),
    successCriteria: text.includes("successcriteria=set"),
    context: proofField(text, "context")
  };
}

function proofField(text, name) {
  const match = new RegExp(`(?:^|; )${name}=([a-z0-9_.:-]{1,80})`, "u").exec(text);
  return match ? match[1] : "";
}

function lowQualityRouteGroups(rows) {
  const groups = new Map();
  for (const row of rows) {
    const quality = learningQuality(row.proof);
    if (!quality.low) {
      continue;
    }
    const family = cleanText(row.family, 80) || "generic";
    const route = cleanText(row.route, 120) || "unknown";
    const key = `${family}|${route}|${quality.quality}`;
    const current = groups.get(key) || {
      family,
      route,
      quality: quality.quality,
      score: quality.score,
      count: 0,
      lastSeenAt: ""
    };
    current.count += 1;
    current.score = Math.min(current.score, quality.score);
    current.lastSeenAt = cleanIso(row.createdAt) || cleanIso(row.receivedAt) || current.lastSeenAt;
    groups.set(key, current);
  }
  return Array.from(groups.values())
    .sort((left, right) => right.count - left.count || left.score - right.score || right.lastSeenAt.localeCompare(left.lastSeenAt));
}

function learningQuality(proof) {
  const text = String(proof || "").toLowerCase();
  const scoreMatch = /qualityscore=(\d{1,3})/u.exec(text);
  const score = scoreMatch ? Math.max(0, Math.min(100, Number.parseInt(scoreMatch[1], 10) || 0)) : 100;
  const qualityMatch = /quality=([a-z0-9_-]+)/u.exec(text);
  const quality = qualityMatch ? qualityMatch[1] : "";
  return {
    quality,
    score,
    low: quality === "fail" || score < 80 || text.includes("semantic-mismatch") || text.includes("quality=low")
  };
}

function slowRoutineCodexGroups(rows) {
  const routineFamilies = new Set(["program-control", "file-work", "system-check", "service-check", "script-task", "web-lookup", "source-scoped-dialog"]);
  const groups = new Map();
  for (const row of rows) {
    const family = cleanText(row.family, 80) || "generic";
    if (row.kind !== "codex-turn" || !routineFamilies.has(family)) {
      continue;
    }
    const durationMs = Number.isSafeInteger(row.durationMs) ? row.durationMs : 0;
    const totalTokens = learningTotalTokens(row.proof);
    if (durationMs < 15000 && totalTokens < 20000) {
      continue;
    }
    const route = cleanText(row.route, 120) || "codex";
    const key = `${family}|${route}`;
    const current = groups.get(key) || {
      family,
      route,
      count: 0,
      durationMs: 0,
      totalTokens: 0,
      lastSeenAt: ""
    };
    current.count += 1;
    current.durationMs = Math.max(current.durationMs, durationMs);
    current.totalTokens = Math.max(current.totalTokens, totalTokens);
    current.lastSeenAt = cleanIso(row.createdAt) || cleanIso(row.receivedAt) || current.lastSeenAt;
    groups.set(key, current);
  }
  return Array.from(groups.values())
    .sort((left, right) => right.count - left.count || right.durationMs - left.durationMs || right.totalTokens - left.totalTokens);
}

function learningTotalTokens(proof) {
  const match = /tokens=(?:actual|estimated);\s*input=(\d+);\s*output=(\d+);\s*total=(\d+);\s*cached=(\d+)/u.exec(String(proof || ""));
  return match ? Number.parseInt(match[3], 10) || 0 : 0;
}

function latestMemoryMarkerRows(rows) {
  const groups = new Map();
  for (const row of rows) {
    const marker = cleanMemoryMarker(row.proof);
    if (!marker) {
      continue;
    }
    const family = cleanText(row.family, 80) || "dialog-memory";
    const key = `${family}|${marker}`;
    const current = groups.get(key) || {
      family,
      marker,
      count: 0,
      lastSeenAt: ""
    };
    current.count += 1;
    current.lastSeenAt = cleanIso(row.createdAt) || cleanIso(row.receivedAt) || current.lastSeenAt;
    groups.set(key, current);
  }
  return Array.from(groups.values())
    .sort((left, right) => right.lastSeenAt.localeCompare(left.lastSeenAt) || right.count - left.count)
    .slice(0, 5);
}

function cleanMemoryMarker(value) {
  const text = cleanText(value, 900).replace(/^`|`$/gu, "");
  if (/^ops-memory\s*:/iu.test(text)) {
    return text;
  }
  if (/^soty-memory\s*:/iu.test(text)) {
    return `ops-memory:${text.replace(/^soty-memory\s*:/iu, "")}`.slice(0, 900);
  }
  return "";
}

function postArmSourceDisconnectRows(rows) {
  const armEvents = rows
    .filter((item) => item.family === "windows-reinstall" && item.result === "ok" && isArmReceipt(item))
    .map((item) => ({
      time: Date.parse(cleanIso(item.createdAt) || cleanIso(item.receivedAt) || ""),
      installHash: cleanText(item.installHash, 120)
    }))
    .filter((item) => Number.isFinite(item.time))
    .sort((left, right) => left.time - right.time);
  if (armEvents.length === 0) {
    return [];
  }
  return rows.filter((item) => {
    if (item.family !== "windows-reinstall" || item.result === "ok") {
      return false;
    }
    if (!isSourceDisconnectReceipt(item)) {
      return false;
    }
    const time = Date.parse(cleanIso(item.createdAt) || cleanIso(item.receivedAt) || "");
    if (!Number.isFinite(time)) {
      return false;
    }
    const installHash = cleanText(item.installHash, 120);
    return armEvents.some((arm) => {
      if (installHash && arm.installHash && installHash !== arm.installHash) {
        return false;
      }
      if (installHash && !arm.installHash) {
        return false;
      }
      return time >= arm.time && time - arm.time <= 30 * 60_000;
    });
  });
}

function failureGroupKey(item) {
  return [
    item.family || "generic",
    item.result || "failed",
    item.toolkit || "",
    item.phase || "",
    item.route || "unknown",
    Number.isSafeInteger(item.exitCode) ? String(item.exitCode) : "no-exit"
  ].join("|");
}

function isArmReceipt(item) {
  const phase = String(item.phase || "").toLowerCase();
  const text = [
    item.toolkit,
    phase,
    item.proof,
    item.commandSig
  ].map((value) => String(value || "").toLowerCase()).join(" ");
  return phase === "arm"
    || /\bphase\s*[:=]\s*arm\b/u.test(text)
    || /\baction\s*[:=]\s*arm\b/u.test(text)
    || /\bkind\s*[:=]\s*arm\b/u.test(text);
}

function isSourceDisconnectReceipt(item) {
  const text = String(item.proof || "").toLowerCase();
  const route = String(item.route || "").toLowerCase();
  const sourceRoute = route.includes("agent-source") || text.includes("! agent-source");
  const timeoutish = item.result === "timeout" || item.exitCode === 124 || text.includes("timeout");
  return text.includes("sourceconnected=false")
    || text.includes("sourceconnected false")
    || text.includes("source-stale")
    || text.includes("no active source")
    || (sourceRoute && timeoutish);
}

function proofShape(value) {
  const text = redactLearningText(value).slice(0, 300).toLowerCase();
  if (!text) {
    return "";
  }
  if (text.includes("blocked-manual-windows-reinstall-handoff")) {
    return "blocked manual reinstall handoff";
  }
  if (text.includes("! agent-source")) {
    return "! agent-source";
  }
  if (text.includes("timeout")) {
    return "timeout";
  }
  if (text.includes("rebooting=true")) {
    return "rebooting=true";
  }
  if (text.includes("sourceconnected=false") || text.includes("source-stale")) {
    return "source disconnected";
  }
  if (text.includes("exitcode=0")) {
    return "exitCode=0";
  }
  if (text.includes("no final") || text.includes("final=empty")) {
    return "no final assistant message";
  }
  if (text.includes("nonempty")) {
    return "nonempty output";
  }
  return text.slice(0, 120);
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
  const kind = cleanEnum(value.kind, ["codex-turn", "source-command", "agent-runtime", "action-job"], "agent-runtime");
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
    skillSha: cleanText(value.skillSha, 80),
    createdAt: cleanIso(value.createdAt) || new Date().toISOString()
  };
}

function cleanEnum(value, allowed, fallback) {
  const text = String(value || "").trim();
  return allowed.includes(text) ? text : fallback;
}

function cleanText(value, max) {
  return String(value || "")
    .replace(/[\u0000-\u001F\u007F]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, max);
}

function cleanHash(value) {
  const text = String(value || "").trim().toLowerCase();
  return /^[a-f0-9]{8,32}$/u.test(text) ? text.slice(0, 32) : "";
}

function cleanSignature(value, family = "") {
  const text = cleanText(value, 160);
  if (/^[a-z][a-z0-9_-]{0,39}:[a-f0-9]{16,64}$/iu.test(text)) {
    return text.toLowerCase();
  }
  const normalized = redactLearningText(value).toLowerCase().slice(0, signatureMaxSource);
  const prefix = cleanText(family, 32).toLowerCase().replace(/[^a-z0-9_-]+/gu, "") || "generic";
  return `${prefix}:${hashShort(normalized).slice(0, 16)}`;
}

function cleanTaskSignature(value) {
  const text = cleanText(value, 160);
  if (/^task:[a-f0-9]{16,64}$/iu.test(text)) {
    return text.toLowerCase();
  }
  const normalized = redactLearningText(value).toLowerCase().slice(0, signatureMaxSource);
  return `task:${hashShort(normalized).slice(0, 16)}`;
}

function redactLearningText(value) {
  return cleanText(value, signatureMaxSource)
    .replace(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/giu, "<email>")
    .replace(/\b(?:\d{1,3}\.){3}\d{1,3}\b/gu, "<ip>")
    .replace(/\b[0-9A-F]{2}(?::[0-9A-F]{2}){5}\b/giu, "<mac>")
    .replace(/[A-Za-z]:\\[^\s'"]+/gu, "<path>")
    .replace(/\/(?:Users|home)\/[^\s'"]+/giu, "<path>")
    .replace(/\b[A-Za-z0-9_-]{48,}\b/gu, "<id>")
    .replace(/\b(?:sk|sess|key|token(?!s\b)|secret|password|pwd)[-_A-Za-z0-9]*\b\s*[:=]\s*['"]?[^'"\s]+/giu, "<secret>")
    .replace(/\s+/gu, " ")
    .trim();
}

function cleanIso(value) {
  const text = cleanText(value, 80);
  if (!text) {
    return "";
  }
  const time = Date.parse(text);
  return Number.isFinite(time) ? new Date(time).toISOString() : "";
}

function hashShort(value) {
  return createHash("sha256").update(String(value || "")).digest("hex").slice(0, 24);
}
