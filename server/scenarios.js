import express from "express";
import { createHash, randomUUID } from "node:crypto";
import { appendFile, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { buildMemoryQuery, buildTeacherReport, readRecentLearningReceipts } from "./agent-learning.js";

const jsonParser = express.json({ limit: "256kb", type: "application/json" });
const maxPromptChars = 20_000;
const maxTitleChars = 80;
const maxScenarios = 500;
const maxReturnedScenarios = 48;

export function attachScenarios(app, { dataDir } = {}) {
  const baseDir = process.env.SOTY_SCENARIOS_DIR || path.join(dataDir || process.cwd(), "scenarios");

  app.get("/api/scenarios/health", async (_req, res) => {
    const state = await loadScenarioState(baseDir);
    res.json({
      ok: true,
      schema: "soty.scenarios.v1",
      backend: "json-index+append-log",
      shared: state.scenarios.length,
      top: topScenarioList(state.scenarios, 3).length
    });
  });

  app.get(["/api/scenarios", "/api/agent/scenarios/search"], async (req, res) => {
    const q = cleanText(req.query?.q, 160);
    const limit = safeLimit(req.query?.limit, 24);
    const state = await loadScenarioState(baseDir);
    const scenarios = selectScenarios(state.scenarios, q, limit);
    res.json({
      ok: true,
      schema: "soty.scenarios.query.v1",
      query: q,
      top: topScenarioList(state.scenarios, 3),
      scenarios,
      memoryLinked: scenarios.filter((item) => item.memoryRefs.length > 0).length
    });
  });

  app.post("/api/scenarios", jsonParser, async (req, res) => {
    const input = cleanScenarioInput(req.body);
    if (!input.prompt) {
      res.status(400).json({ ok: false, error: "empty_prompt" });
      return;
    }
    if (input.scope !== "shared") {
      res.json({ ok: true, skipped: true, scope: "personal" });
      return;
    }
    const memoryRefs = await scenarioMemoryRefs(dataDir, input).catch(() => []);
    const result = await upsertSharedScenario(baseDir, {
      ...input,
      memoryRefs: mergeMemoryRefs(input.memoryRefs, memoryRefs)
    });
    await appendScenarioMemoryReceipt(dataDir, result.scenario, result.created ? "save" : "improve").catch(() => undefined);
    res.json({
      ok: true,
      created: result.created,
      improved: result.improved,
      scenario: publicScenario(result.scenario)
    });
  });

  app.post("/api/scenarios/:id/use", jsonParser, async (req, res) => {
    const id = cleanScenarioId(req.params.id);
    if (!id) {
      res.status(404).json({ ok: false, error: "missing" });
      return;
    }
    const result = await markScenarioUsed(baseDir, id);
    if (!result) {
      res.status(404).json({ ok: false, error: "missing" });
      return;
    }
    await appendScenarioMemoryReceipt(dataDir, result, "use").catch(() => undefined);
    res.json({ ok: true, scenario: publicScenario(result) });
  });
}

async function loadScenarioState(baseDir) {
  await mkdir(baseDir, { recursive: true, mode: 0o700 });
  const file = indexPath(baseDir);
  const parsed = await readJson(file).catch(() => null);
  const seeded = seedScenarios();
  const scenarios = [];
  const seen = new Set();
  let changed = !parsed;
  for (const item of [...(Array.isArray(parsed?.scenarios) ? parsed.scenarios : []), ...seeded]) {
    const scenario = cleanStoredScenario(item);
    if (!scenario || scenario.deleted || seen.has(scenario.id)) {
      continue;
    }
    if (!Array.isArray(parsed?.scenarios) || !parsed.scenarios.some((stored) => cleanScenarioId(stored?.id) === scenario.id)) {
      changed = true;
    }
    seen.add(scenario.id);
    scenarios.push(scenario);
  }
  const state = {
    schema: "soty.scenarios.store.v1",
    updatedAt: new Date().toISOString(),
    scenarios: scenarios
      .sort((left, right) => scenarioRank(right) - scenarioRank(left) || right.updatedAt.localeCompare(left.updatedAt))
      .slice(0, maxScenarios)
  };
  if (changed) {
    await saveScenarioState(baseDir, state).catch(() => undefined);
  }
  return state;
}

async function saveScenarioState(baseDir, state) {
  await mkdir(baseDir, { recursive: true, mode: 0o700 });
  const file = indexPath(baseDir);
  const temp = `${file}.${process.pid}.${Date.now()}.tmp`;
  await writeFile(temp, `${JSON.stringify(state, null, 2)}\n`, { encoding: "utf8", mode: 0o600 });
  await rename(temp, file);
}

async function upsertSharedScenario(baseDir, input) {
  const state = await loadScenarioState(baseDir);
  const now = new Date().toISOString();
  const existing = bestSimilarScenario(state.scenarios, input);
  const base = existing || {
    schema: "soty.scenario.v1",
    id: `scn_${hashShort(`${input.title}\n${input.prompt}\n${now}`).slice(0, 18)}`,
    scope: "shared",
    createdAt: now,
    useCount: 0,
    version: 0,
    memoryRefs: []
  };
  const improved = shouldImproveScenario(base, input);
  const next = {
    ...base,
    title: improved ? input.title : base.title,
    prompt: improved ? input.prompt : base.prompt,
    updatedAt: now,
    version: Number(base.version || 0) + 1,
    aliases: Array.from(new Set([...(base.aliases || []), input.title].filter(Boolean))).slice(0, 12),
    memoryRefs: mergeMemoryRefs(base.memoryRefs, input.memoryRefs),
    qualityScore: scenarioQuality(improved ? input.prompt : base.prompt),
    source: {
      deviceHash: hashShort(input.deviceId || input.deviceNick || "unknown"),
      deviceNick: cleanText(input.deviceNick, 40)
    }
  };
  const scenarios = [next, ...state.scenarios.filter((item) => item.id !== next.id)]
    .sort((left, right) => scenarioRank(right) - scenarioRank(left) || right.updatedAt.localeCompare(left.updatedAt))
    .slice(0, maxScenarios);
  await saveScenarioState(baseDir, { ...state, updatedAt: now, scenarios });
  await appendScenarioEvent(baseDir, existing ? "upsert" : "create", next);
  return { scenario: next, created: !existing, improved };
}

async function markScenarioUsed(baseDir, id) {
  const state = await loadScenarioState(baseDir);
  const now = new Date().toISOString();
  let updated = null;
  const scenarios = state.scenarios.map((item) => {
    if (item.id !== id) {
      return item;
    }
    updated = {
      ...item,
      useCount: Number(item.useCount || 0) + 1,
      lastUsedAt: now,
      updatedAt: now
    };
    return updated;
  });
  if (!updated) {
    return null;
  }
  await saveScenarioState(baseDir, { ...state, updatedAt: now, scenarios });
  await appendScenarioEvent(baseDir, "use", updated);
  return updated;
}

async function appendScenarioEvent(baseDir, event, scenario) {
  const line = JSON.stringify({
    schema: "soty.scenario.event.v1",
    id: `evt_${randomUUID()}`,
    event,
    scenarioId: scenario.id,
    title: scenario.title,
    at: new Date().toISOString()
  });
  await appendFile(path.join(baseDir, "events.jsonl"), `${line}\n`, { encoding: "utf8", mode: 0o600 });
}

async function scenarioMemoryRefs(dataDir, input) {
  const lines = await readRecentLearningReceipts(dataDir, 900).catch(() => []);
  const receipts = lines.map(parseJsonLine).filter(Boolean);
  const report = buildTeacherReport(receipts, {
    limit: 900,
    family: "",
    taskSig: cleanText(`${input.title} ${input.prompt}`, 160)
  });
  const query = buildMemoryQuery(report, {
    family: "",
    taskSig: cleanText(`${input.title} ${input.prompt}`, 160),
    limit: 8
  });
  return (query.items || []).slice(0, 6).map((item) => ({
    id: `mem_${hashShort(`${item.kind}|${item.family}|${item.title}|${item.route}|${item.guidance}`)}`,
    kind: cleanText(item.kind, 40),
    family: cleanText(item.family, 80),
    title: cleanText(item.title, 140),
    route: cleanText(item.route, 120),
    confidence: clamp01(item.confidence || 0)
  }));
}

async function appendScenarioMemoryReceipt(dataDir, scenario, action) {
  const learningDir = process.env.SOTY_LEARNING_DIR || path.join(dataDir || process.cwd(), "learning");
  await mkdir(learningDir, { recursive: true, mode: 0o700 });
  const now = new Date();
  const receipt = {
    schema: "soty.memory.receipt.v1",
    id: `mem_${randomUUID()}`,
    receivedAt: now.toISOString(),
    installHash: "server-scenarios",
    agentVersion: "server",
    privacy: "sanitized",
    kind: "agent-runtime",
    result: "ok",
    toolkit: "scenarios",
    phase: action,
    family: "scenario",
    platform: "soty",
    route: `scenario/${action}`,
    taskSig: `task:${hashShort(`${scenario.title}\n${scenario.prompt}`).slice(0, 16)}`,
    proof: `scenario=${scenario.id}; title=${scenario.title}; memoryRefs=${scenario.memoryRefs.length}; useCount=${Number(scenario.useCount || 0)}`,
    memorySchema: "soty.memory.receipt.v1",
    createdAt: now.toISOString()
  };
  await appendFile(path.join(learningDir, `${now.toISOString().slice(0, 10)}.jsonl`), `${JSON.stringify(receipt)}\n`, { encoding: "utf8", mode: 0o600 });
}

function selectScenarios(scenarios, q, limit) {
  const query = normalizeSearch(q);
  const ranked = scenarios
    .map((scenario) => ({ scenario, score: scenarioMatchScore(scenario, query) }))
    .filter((item) => !query || item.score > 0)
    .sort((left, right) => right.score - left.score || scenarioRank(right.scenario) - scenarioRank(left.scenario))
    .slice(0, Math.max(1, Math.min(maxReturnedScenarios, limit)));
  return ranked.map((item) => publicScenario(item.scenario));
}

function topScenarioList(scenarios, limit) {
  return scenarios
    .slice()
    .sort((left, right) => Number(right.useCount || 0) - Number(left.useCount || 0) || scenarioRank(right) - scenarioRank(left))
    .slice(0, limit)
    .map(publicScenario);
}

function publicScenario(scenario) {
  return {
    id: scenario.id,
    scope: "shared",
    title: scenario.title,
    prompt: scenario.prompt,
    useCount: Number(scenario.useCount || 0),
    version: Number(scenario.version || 1),
    qualityScore: Number(scenario.qualityScore || scenarioQuality(scenario.prompt)),
    memoryRefs: mergeMemoryRefs(scenario.memoryRefs, []),
    createdAt: scenario.createdAt,
    updatedAt: scenario.updatedAt,
    lastUsedAt: scenario.lastUsedAt || "",
    example: scenario.example === true
  };
}

function cleanScenarioInput(value) {
  const prompt = cleanScenarioPrompt(value?.prompt);
  const title = cleanScenarioTitle(value?.title || titleFromPrompt(prompt));
  const scope = value?.scope === "personal" ? "personal" : "shared";
  return {
    scope,
    title,
    prompt,
    deviceId: cleanText(value?.deviceId, 120),
    deviceNick: cleanText(value?.deviceNick, 80),
    sourceTunnelId: cleanText(value?.sourceTunnelId, 120),
    sourceText: cleanText(value?.sourceText, 2000),
    memoryRefs: cleanMemoryRefs(value?.memoryRefs)
  };
}

function cleanStoredScenario(value) {
  const prompt = cleanScenarioPrompt(value?.prompt);
  if (!prompt) {
    return null;
  }
  const title = cleanScenarioTitle(value?.title || titleFromPrompt(prompt));
  const now = new Date().toISOString();
  return {
    schema: "soty.scenario.v1",
    id: cleanScenarioId(value?.id) || `scn_${hashShort(`${title}\n${prompt}`).slice(0, 18)}`,
    scope: "shared",
    title,
    prompt,
    createdAt: cleanIso(value?.createdAt) || now,
    updatedAt: cleanIso(value?.updatedAt) || cleanIso(value?.createdAt) || now,
    lastUsedAt: cleanIso(value?.lastUsedAt),
    useCount: safeCount(value?.useCount),
    version: safeCount(value?.version) || 1,
    aliases: Array.isArray(value?.aliases) ? value.aliases.map((item) => cleanScenarioTitle(item)).filter(Boolean).slice(0, 12) : [],
    memoryRefs: cleanMemoryRefs(value?.memoryRefs),
    qualityScore: Number(value?.qualityScore || scenarioQuality(prompt)),
    example: value?.example === true,
    deleted: value?.deleted === true
  };
}

function seedScenarios() {
  return [cleanStoredScenario({
    id: "scn_soty_export_import_reinstall",
    title: "Сценарий 1: перенос Сот через флешку",
    prompt: [
      "Сценарий:",
      "Название: Сценарий 1 - перенос Сот через флешку",
      "Цель: сохранить состояние Сот одним JSON-файлом перед переустановкой Windows и восстановить его после установки.",
      "Когда использовать: перед чистой переустановкой Windows, сменой компьютера или переносом Сот на новое устройство.",
      "Где выполнять: в текущих Сотах на устройстве пользователя.",
      "Что нужно: флешка или другой внешний носитель, на который можно сохранить JSON-файл.",
      "Шаги:",
      "1. Открыть MORE -> SCENARIOS -> STATE -> EXPORT.",
      "2. Сохранить файл soty-export-...json на флешку.",
      "3. После переустановки открыть Соты и выбрать MORE -> SCENARIOS -> STATE -> IMPORT.",
      "4. Выбрать JSON-файл с флешки.",
      "Что считать готовым: диалоги, подключенные соты и сохраненные сценарии восстановились из одного файла.",
      "Важно: не полагаться на автоматический backup при переустановке; пользователь сам хранит этот export-файл."
    ].join("\n"),
    useCount: 0,
    version: 1,
    example: true,
    createdAt: "2026-05-18T00:00:00.000Z",
    updatedAt: "2026-05-18T00:00:00.000Z",
    memoryRefs: []
  })].filter(Boolean);
}

function bestSimilarScenario(scenarios, input) {
  const needle = normalizeSearch(input.title);
  let best = null;
  let bestScore = 0;
  for (const scenario of scenarios) {
    const score = Math.max(
      tokenOverlap(needle, normalizeSearch(scenario.title)),
      tokenOverlap(normalizeSearch(input.prompt), normalizeSearch(scenario.prompt)) * 0.72
    );
    if (score > bestScore) {
      best = scenario;
      bestScore = score;
    }
  }
  return bestScore >= 0.72 ? best : null;
}

function shouldImproveScenario(existing, input) {
  const current = scenarioQuality(existing.prompt);
  const next = scenarioQuality(input.prompt);
  return next >= current || input.prompt.length > String(existing.prompt || "").length + 120;
}

function scenarioRank(scenario) {
  return Number(scenario.useCount || 0) * 8
    + Number(scenario.qualityScore || scenarioQuality(scenario.prompt))
    + Number(scenario.memoryRefs?.length || 0) * 4
    + (scenario.example ? 2 : 0);
}

function scenarioMatchScore(scenario, query) {
  if (!query) {
    return scenarioRank(scenario);
  }
  const haystack = normalizeSearch(`${scenario.title} ${scenario.prompt} ${(scenario.aliases || []).join(" ")}`);
  const overlap = tokenOverlap(query, haystack);
  return overlap * 1000 + (haystack.includes(query) ? 500 : 0) + scenarioRank(scenario);
}

function scenarioQuality(prompt) {
  const text = String(prompt || "");
  let score = Math.min(40, Math.round(text.length / 260));
  for (const marker of ["Цель:", "Когда использовать:", "Где выполнять:", "Шаги:", "Что считать готовым:"]) {
    if (text.includes(marker)) {
      score += 8;
    }
  }
  return score;
}

function mergeMemoryRefs(left, right) {
  const refs = new Map();
  for (const item of [...cleanMemoryRefs(left), ...cleanMemoryRefs(right)]) {
    refs.set(item.id, item);
  }
  return Array.from(refs.values()).slice(0, 12);
}

function cleanMemoryRefs(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((item) => {
    const id = cleanScenarioId(item?.id);
    if (!id) {
      return null;
    }
    return {
      id,
      kind: cleanText(item?.kind, 40),
      family: cleanText(item?.family, 80),
      title: cleanText(item?.title, 140),
      route: cleanText(item?.route, 120),
      confidence: clamp01(item?.confidence)
    };
  }).filter(Boolean).slice(0, 12);
}

function titleFromPrompt(prompt) {
  return String(prompt || "")
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .find((line) => line && !/^сценарий:?$/iu.test(line))
    ?.replace(/^(?:название|цель):\s*/iu, "") || "Сценарий";
}

function cleanScenarioPrompt(value) {
  return String(value || "")
    .replace(/\r\n?/gu, "\n")
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/gu, "")
    .replace(/\n{4,}/gu, "\n\n")
    .trim()
    .slice(0, maxPromptChars);
}

function cleanScenarioTitle(value) {
  const title = cleanText(value, maxTitleChars).replace(/^(?:название|title|цель|goal):\s*/iu, "");
  if (title) {
    return title;
  }
  return cleanText(value, maxTitleChars) || "Сценарий";
}

function cleanScenarioId(value) {
  return String(value || "").trim().replace(/[^a-z0-9_-]/giu, "").slice(0, 96);
}

function cleanText(value, max) {
  return String(value || "")
    .replace(/[\u0000-\u001F\u007F]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, max);
}

function normalizeSearch(value) {
  return cleanText(value, 2000).toLowerCase().replace(/ё/gu, "е");
}

function tokenOverlap(left, right) {
  const a = new Set(normalizeSearch(left).split(/[^\p{L}\p{N}]+/u).filter((item) => item.length > 2));
  const b = new Set(normalizeSearch(right).split(/[^\p{L}\p{N}]+/u).filter((item) => item.length > 2));
  if (a.size === 0 || b.size === 0) {
    return 0;
  }
  let hit = 0;
  for (const token of a) {
    if (b.has(token)) {
      hit += 1;
    }
  }
  return hit / Math.max(a.size, b.size);
}

function safeLimit(value, fallback) {
  const parsed = Number.parseInt(String(value || ""), 10);
  return Number.isFinite(parsed) ? Math.max(1, Math.min(maxReturnedScenarios, parsed)) : fallback;
}

function safeCount(value) {
  const num = Number(value || 0);
  return Number.isFinite(num) ? Math.max(0, Math.min(1_000_000, Math.round(num))) : 0;
}

function clamp01(value) {
  const num = Number(value || 0);
  return Number.isFinite(num) ? Math.max(0, Math.min(1, num)) : 0;
}

function cleanIso(value) {
  const text = cleanText(value, 80);
  const time = Date.parse(text);
  return Number.isFinite(time) ? new Date(time).toISOString() : "";
}

function hashShort(value) {
  return createHash("sha256").update(String(value || "")).digest("hex").slice(0, 24);
}

async function readJson(file) {
  return JSON.parse(await readFile(file, "utf8"));
}

function parseJsonLine(line) {
  try {
    const value = JSON.parse(line);
    return value && typeof value === "object" ? value : null;
  } catch {
    return null;
  }
}

function indexPath(baseDir) {
  return path.join(baseDir, "index.json");
}
