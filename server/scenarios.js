import express from "express";
import { createHash, randomUUID } from "node:crypto";
import { appendFile, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { buildMemoryQuery, buildTeacherReport, readRecentLearningReceiptRecords } from "./agent-learning.js";

const jsonParser = express.json({ limit: "256kb", type: "application/json" });
const maxPromptChars = 20_000;
const maxTitleChars = 80;
const maxScenarios = 500;
const maxReturnedScenarios = 48;
const maxAgentPlanChars = 24_000;

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
    const agentView = req.path.startsWith("/api/agent/");
    const state = await loadScenarioState(baseDir);
    const scenarios = selectScenarios(state.scenarios, q, limit, { agentView });
    res.json({
      ok: true,
      schema: "soty.scenarios.query.v1",
      query: q,
      top: topScenarioList(state.scenarios, 3, { agentView }),
      scenarios,
      memoryLinked: agentView ? scenarios.filter((item) => item.memoryRefs.length > 0).length : 0
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
    const result = await markScenarioUsed(baseDir, id, { dataDir });
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
    memoryRefs: [],
    agentPlan: deriveScenarioAgentPlan(input)
  };
  const improved = shouldImproveScenario(base, input);
  const prompt = improved ? input.prompt : base.prompt;
  const title = improved ? input.title : base.title;
  const agentPlan = improveScenarioAgentPlan(
    mergeScenarioAgentPlan(base.agentPlan, input.agentPlan, { title, prompt }),
    { title, prompt, memoryRefs: mergeMemoryRefs(base.memoryRefs, input.memoryRefs), event: existing ? "improve" : "create" }
  );
  const next = {
    ...base,
    title,
    prompt,
    updatedAt: now,
    version: Number(base.version || 0) + 1,
    aliases: Array.from(new Set([...(base.aliases || []), input.title].filter(Boolean))).slice(0, 12),
    memoryRefs: mergeMemoryRefs(base.memoryRefs, input.memoryRefs),
    agentPlan,
    qualityScore: scenarioQuality(prompt, agentPlan),
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

async function markScenarioUsed(baseDir, id, { dataDir } = {}) {
  const state = await loadScenarioState(baseDir);
  const now = new Date().toISOString();
  const current = state.scenarios.find((item) => item.id === id) || null;
  const freshMemoryRefs = current ? await scenarioMemoryRefs(dataDir, current).catch(() => []) : [];
  let updated = null;
  const scenarios = state.scenarios.map((item) => {
    if (item.id !== id) {
      return item;
    }
    const memoryRefs = mergeMemoryRefs(item.memoryRefs, freshMemoryRefs);
    updated = {
      ...item,
      useCount: Number(item.useCount || 0) + 1,
      memoryRefs,
      agentPlan: improveScenarioAgentPlan(item.agentPlan, {
        title: item.title,
        prompt: item.prompt,
        memoryRefs,
        event: "use"
      }),
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
  const records = await readRecentLearningReceiptRecords(dataDir, 900).catch(() => []);
  const receipts = records.map((item) => item.receipt || parseJsonLine(item.text)).filter(Boolean);
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
  return (query.items || []).slice(0, 6).map((item, index) => {
    const source = memoryPointerForItem(records, item, index);
    return {
      id: `mem_${hashShort(`${item.kind}|${item.family}|${item.title}|${item.route}|${item.guidance}|${source.file}|${source.line}`)}`,
      kind: cleanText(item.kind, 40),
      family: cleanText(item.family, 80),
      title: cleanText(item.title, 140),
      route: cleanText(item.route, 120),
      confidence: clamp01(item.confidence || 0),
      source
    };
  });
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

function selectScenarios(scenarios, q, limit, { agentView = false } = {}) {
  const query = normalizeSearch(q);
  const ranked = scenarios
    .map((scenario) => ({ scenario, score: scenarioMatchScore(scenario, query) }))
    .filter((item) => !query || item.score > 0)
    .sort((left, right) => right.score - left.score || scenarioRank(right.scenario) - scenarioRank(left.scenario))
    .slice(0, Math.max(1, Math.min(maxReturnedScenarios, limit)));
  return ranked.map((item) => publicScenario(item.scenario, { agentView, score: item.score }));
}

function topScenarioList(scenarios, limit, { agentView = false } = {}) {
  return scenarios
    .slice()
    .sort((left, right) => Number(right.useCount || 0) - Number(left.useCount || 0) || scenarioRank(right) - scenarioRank(left))
    .slice(0, limit)
    .map((scenario) => publicScenario(scenario, { agentView, score: scenarioRank(scenario) }));
}

function publicScenario(scenario, { agentView = false, score = 0 } = {}) {
  return {
    id: scenario.id,
    scope: "shared",
    title: scenario.title,
    prompt: scenario.prompt,
    useCount: Number(scenario.useCount || 0),
    version: Number(scenario.version || 1),
    qualityScore: Number(scenario.qualityScore || scenarioQuality(scenario.prompt, scenario.agentPlan)),
    memoryRefs: agentView ? mergeMemoryRefs(scenario.memoryRefs, []) : [],
    ...(agentView ? { agentPlan: cleanAgentPlan(scenario.agentPlan, scenario), score: Number(score || 0) } : {}),
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
    memoryRefs: cleanMemoryRefs(value?.memoryRefs),
    agentPlan: cleanAgentPlan(value?.agentPlan, { title, prompt })
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
    agentPlan: cleanAgentPlan(value?.agentPlan, { title, prompt }),
    qualityScore: Number(value?.qualityScore || scenarioQuality(prompt, value?.agentPlan)),
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
  const current = scenarioQuality(existing.prompt, existing.agentPlan);
  const next = scenarioQuality(input.prompt, input.agentPlan);
  return next >= current || input.prompt.length > String(existing.prompt || "").length + 120;
}

function scenarioRank(scenario) {
  return Number(scenario.useCount || 0) * 8
    + Number(scenario.qualityScore || scenarioQuality(scenario.prompt, scenario.agentPlan))
    + Number(scenario.memoryRefs?.length || 0) * 4
    + Number(scenario.agentPlan?.branches?.length || 0) * 3
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

function scenarioQuality(prompt, agentPlan = null) {
  const text = String(prompt || "");
  let score = Math.min(40, Math.round(text.length / 260));
  for (const marker of ["Цель:", "Когда использовать:", "Где выполнять:", "Шаги:", "Что считать готовым:"]) {
    if (text.includes(marker)) {
      score += 8;
    }
  }
  const plan = cleanAgentPlan(agentPlan, { prompt });
  score += Math.min(18, plan.triggers.length * 3 + plan.branches.length * 4 + plan.successChecks.length * 2);
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
      confidence: clamp01(item?.confidence),
      source: cleanMemoryPointer(item?.source)
    };
  }).filter(Boolean).slice(0, 12);
}

function memoryPointerForItem(records, item, index = 0) {
  const family = cleanText(item?.family, 80);
  const route = cleanText(item?.route, 120);
  const kind = cleanText(item?.kind, 40);
  const candidates = records.slice().reverse();
  const found = candidates.find((record) => {
    const receipt = record.receipt || {};
    const receiptFamily = cleanText(receipt.family, 80);
    const receiptRoute = cleanText(receipt.route, 120);
    const receiptKind = cleanText(receipt.kind || receipt.toolkit, 40);
    return (!family || receiptFamily === family)
      && (!route || receiptRoute === route)
      && (!kind || receiptKind === kind || cleanText(receipt.toolkit, 40) === kind);
  }) || candidates[index] || candidates[0] || null;
  return cleanMemoryPointer({
    backend: "append-only-jsonl",
    file: found?.file || "",
    line: found?.line || 0,
    receiptId: found?.receipt?.id || "",
    receivedAt: found?.receipt?.receivedAt || found?.receipt?.createdAt || ""
  });
}

function cleanMemoryPointer(value) {
  const record = value && typeof value === "object" ? value : {};
  return {
    backend: "append-only-jsonl",
    file: cleanMemoryFile(record.file),
    line: safeCount(record.line),
    receiptId: cleanText(record.receiptId || record.id, 120),
    receivedAt: cleanIso(record.receivedAt || record.createdAt)
  };
}

function cleanMemoryFile(value) {
  return String(value || "")
    .replace(/\\/gu, "/")
    .split("/")
    .filter((part) => /^[A-Za-z0-9_.-]+$/u.test(part))
    .slice(-3)
    .join("/")
    .slice(0, 180);
}

function deriveScenarioAgentPlan(input) {
  const title = cleanScenarioTitle(input?.title || titleFromPrompt(input?.prompt));
  const prompt = cleanScenarioPrompt(input?.prompt);
  const lines = prompt.split(/\n/u).map((line) => line.trim()).filter(Boolean);
  const steps = lines
    .filter((line) => /^\d+[\.)]\s+/u.test(line))
    .map((line) => line.replace(/^\d+[\.)]\s*/u, ""))
    .slice(0, 12);
  const triggerText = normalizeSearch(`${title} ${prompt}`)
    .split(/[^\p{L}\p{N}]+/u)
    .filter((item) => item.length > 3)
    .slice(0, 12);
  return cleanAgentPlan({
    schema: "soty.scenario.plan.v1",
    title,
    intent: title,
    triggers: Array.from(new Set(triggerText)),
    selection: {
      mode: "agent-adaptive",
      autoUseWhen: "current request strongly matches triggers or the scenario has better proof than broad rediscovery",
      explorationRate: 0.12
    },
    branches: [
      {
        id: "normal",
        when: "all required context is available",
        steps: steps.length ? steps : lines.slice(0, 8),
        verify: ["fresh target/context proof", "task-specific success proof"]
      },
      {
        id: "blocked",
        when: "required device/file/permission/proof is missing",
        steps: ["ask only for the missing precondition", "avoid substituting another target or stale memory"],
        verify: ["blocker is concrete and source-scoped"]
      }
    ],
    successChecks: extractSuccessChecks(prompt),
    avoid: ["do not follow stale memory without fresh proof", "do not expose internal memory pointers to the user"],
    memoryPolicy: {
      updateOn: ["save", "use", "success", "failure", "route-change"],
      sourceOfTruth: "append-only memory pointers plus current proof"
    }
  }, { title, prompt });
}

function cleanAgentPlan(value, fallback = {}) {
  const source = value && typeof value === "object" ? value : derivePlanSeed(fallback);
  const title = cleanScenarioTitle(source.title || fallback.title || titleFromPrompt(fallback.prompt || ""));
  const intent = cleanText(source.intent || fallback.intent || title, 240);
  const triggers = cleanStringArray(source.triggers, 20, 80);
  const branches = cleanBranches(source.branches);
  const successChecks = cleanStringArray(source.successChecks || source.success || source.verify, 16, 180);
  const avoid = cleanStringArray(source.avoid || source.doNot, 16, 180);
  const memoryPointers = cleanMemoryRefs(source.memoryPointers || source.memoryRefs);
  const selection = source.selection && typeof source.selection === "object" ? source.selection : {};
  const plan = {
    schema: "soty.scenario.plan.v1",
    title,
    intent,
    triggers,
    selection: {
      mode: cleanText(selection.mode || "agent-adaptive", 60),
      autoUseWhen: cleanText(selection.autoUseWhen || "strong semantic match plus current proof path", 240),
      explorationRate: clamp01(selection.explorationRate ?? 0.12)
    },
    branches,
    successChecks,
    avoid,
    memoryPointers,
    memoryPolicy: {
      updateOn: cleanStringArray(source.memoryPolicy?.updateOn, 12, 60).length
        ? cleanStringArray(source.memoryPolicy.updateOn, 12, 60)
        : ["save", "use", "success", "failure", "route-change"],
      sourceOfTruth: cleanText(source.memoryPolicy?.sourceOfTruth || "append-only memory pointers plus current proof", 160)
    }
  };
  return JSON.stringify(plan).length > maxAgentPlanChars
    ? { ...plan, branches: plan.branches.slice(0, 4), memoryPointers: plan.memoryPointers.slice(0, 6) }
    : plan;
}

function derivePlanSeed(fallback = {}) {
  return deriveScenarioAgentPlan({
    title: fallback.title || titleFromPrompt(fallback.prompt || ""),
    prompt: fallback.prompt || fallback.sourceText || ""
  });
}

function mergeScenarioAgentPlan(basePlan, inputPlan, fallback = {}) {
  const base = cleanAgentPlan(basePlan, fallback);
  const input = cleanAgentPlan(inputPlan, fallback);
  return cleanAgentPlan({
    ...base,
    ...input,
    triggers: Array.from(new Set([...base.triggers, ...input.triggers])).slice(0, 20),
    branches: mergeBranches(base.branches, input.branches),
    successChecks: Array.from(new Set([...base.successChecks, ...input.successChecks])).slice(0, 16),
    avoid: Array.from(new Set([...base.avoid, ...input.avoid])).slice(0, 16),
    memoryPointers: mergeMemoryRefs(base.memoryPointers, input.memoryPointers)
  }, fallback);
}

function improveScenarioAgentPlan(plan, { title = "", prompt = "", memoryRefs = [], event = "use" } = {}) {
  const clean = mergeScenarioAgentPlan(plan, {
    title,
    prompt,
    memoryPointers: memoryRefs,
    memoryPolicy: {
      updateOn: ["save", "use", "success", "failure", "route-change"],
      sourceOfTruth: "append-only memory pointers plus current proof"
    }
  }, { title, prompt });
  return {
    ...clean,
    lastImprovedAt: new Date().toISOString(),
    lastImprovement: cleanText(event, 40),
    memoryPointers: mergeMemoryRefs(clean.memoryPointers, memoryRefs)
  };
}

function cleanBranches(value) {
  const branches = Array.isArray(value) ? value : [];
  const cleaned = branches.map((item, index) => {
    const record = item && typeof item === "object" ? item : { steps: [String(item || "")] };
    const id = cleanScenarioId(record.id || `branch_${index + 1}`) || `branch_${index + 1}`;
    const steps = cleanStringArray(record.steps || record.actions, 14, 220);
    return {
      id,
      when: cleanText(record.when || record.condition || "", 220),
      steps,
      verify: cleanStringArray(record.verify || record.proof, 8, 180)
    };
  }).filter((item) => item.steps.length > 0).slice(0, 8);
  return cleaned.length > 0 ? cleaned : [{
    id: "normal",
    when: "scenario matches current request",
    steps: ["adapt the visible scenario to current context", "verify fresh state before final answer"],
    verify: ["fresh proof"]
  }];
}

function mergeBranches(left, right) {
  const map = new Map();
  for (const branch of [...cleanBranches(left), ...cleanBranches(right)]) {
    const existing = map.get(branch.id);
    map.set(branch.id, existing ? {
      ...existing,
      when: branch.when || existing.when,
      steps: Array.from(new Set([...existing.steps, ...branch.steps])).slice(0, 14),
      verify: Array.from(new Set([...existing.verify, ...branch.verify])).slice(0, 8)
    } : branch);
  }
  return Array.from(map.values()).slice(0, 8);
}

function cleanStringArray(value, maxItems, maxChars) {
  return (Array.isArray(value) ? value : [])
    .map((item) => cleanText(item, maxChars))
    .filter(Boolean)
    .slice(0, maxItems);
}

function extractSuccessChecks(prompt) {
  const checks = [];
  for (const line of String(prompt || "").split(/\n/u)) {
    if (/готов|success|verify|proof|провер|считать/iu.test(line)) {
      checks.push(line.replace(/^[^:]{0,40}:\s*/u, ""));
    }
  }
  return checks.slice(0, 8);
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
