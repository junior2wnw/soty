import { readFile } from "node:fs/promises";
import path from "node:path";

const catalogSchema = "soty.frontend-capabilities.v1";
const actionSchema = "soty.frontend-action.v1";
const maxActions = 240;

export function attachFrontendCapabilities(app, { distDir } = {}) {
  app.get("/api/frontend/capabilities", async (_req, res) => {
    try {
      const catalog = await buildFrontendCapabilityCatalog(distDir);
      res.setHeader("Cache-Control", "no-store");
      res.json(catalog);
    } catch {
      res.status(500).json({ ok: false, schema: catalogSchema, error: "frontend_capabilities_failed" });
    }
  });
}

export async function buildFrontendCapabilityCatalog(distDir = "") {
  const agentManifest = await readJson(path.join(distDir, "agent", "manifest.json"));
  const miniAppManifest = await readJson(path.join(distDir, "mini-apps", "manifest.json"));
  const actions = dedupeActions([
    ...manifestProvidedActions(agentManifest, "agent-manifest"),
    ...manifestProvidedActions(miniAppManifest, "mini-app-manifest"),
    ...routeProfileActions(agentManifest),
    ...toolkitActions(agentManifest),
    ...runtimeCapabilityActions(agentManifest),
    ...computerUseCapabilityActions(agentManifest),
    ...miniAppActions(miniAppManifest)
  ]).slice(0, maxActions);

  return {
    ok: true,
    schema: catalogSchema,
    version: stringValue(agentManifest?.version, 40),
    generatedAt: new Date().toISOString(),
    sources: [
      ...(agentManifest ? [{ id: "agent-manifest", schema: stringValue(agentManifest.schema, 80), url: "/agent/manifest.json" }] : []),
      ...(miniAppManifest ? [{ id: "mini-app-manifest", schema: stringValue(miniAppManifest.schema, 80), url: "/mini-apps/manifest.json" }] : [])
    ],
    actions
  };
}

async function readJson(filePath) {
  if (!filePath) {
    return null;
  }
  try {
    const parsed = JSON.parse(await readFile(filePath, "utf8"));
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function manifestProvidedActions(manifest, source) {
  if (!isRecord(manifest)) {
    return [];
  }
  const candidates = [
    ...arrayValue(manifest.frontendActions),
    ...arrayValue(manifest.uiActions),
    ...arrayValue(isRecord(manifest.frontend) ? manifest.frontend.actions : null)
  ];
  return candidates
    .map((item) => normalizeProvidedAction(item, source))
    .filter(Boolean);
}

function routeProfileActions(manifest) {
  const profiles = arrayValue(isRecord(manifest?.routeProfiles) ? manifest.routeProfiles.profiles : null);
  return profiles.map((profile) => {
    const id = tokenValue(profile.id || profile.family || profile.title, 120);
    if (!id) {
      return null;
    }
    const title = cleanText(profile.title || humanTitle(profile.family || id), 100);
    const phases = stringList(profile.phases, 8, 32);
    const proof = stringList(profile.proof, 8, 40);
    const route = stringList(profile.route, 2, 160);
    return normalizeAction({
      id: `route-profile:${id}`,
      source: "agent-manifest",
      kind: "route-profile",
      title,
      label: labelFor(profile.family || profile.capability || id),
      summary: cleanText(profile.capability || route[0] || phases.join(", "), 180),
      tags: compactList([profile.family, profile.capability, profile.defaultOperation, profile.defaultAction, ...phases]),
      runtime: {
        routeProfile: id,
        family: stringValue(profile.family, 80),
        capability: stringValue(profile.capability, 80),
        entryTool: stringValue(profile.entryTool, 40),
        phases,
        proof
      }
    });
  }).filter(Boolean);
}

function toolkitActions(manifest) {
  const toolkits = arrayValue(isRecord(manifest?.automationToolkits) ? manifest.automationToolkits.toolkits : null);
  return toolkits.map((toolkit) => {
    const name = tokenValue(toolkit.name || toolkit.id || toolkit.kind, 100);
    if (!name) {
      return null;
    }
    const phases = stringList(toolkit.phases, 10, 36);
    const proof = stringList(toolkit.proof, 10, 40);
    const title = cleanText(toolkit.title || humanTitle(name), 100);
    const promotion = cleanText(toolkit.promotion, 180);
    return normalizeAction({
      id: `toolkit:${name}`,
      source: "agent-manifest",
      kind: "toolkit",
      title,
      label: labelFor(toolkit.kind || name),
      summary: promotion || cleanText([toolkit.kind, phases.join(", ")].filter(Boolean).join(": "), 180),
      tags: compactList([name, toolkit.kind, toolkit.entryTool, toolkit.routeProfile, ...phases]),
      runtime: {
        toolkit: name,
        kind: stringValue(toolkit.kind, 60),
        entryTool: stringValue(toolkit.entryTool, 40),
        routeProfile: stringValue(toolkit.routeProfile, 120),
        phases,
        proof
      }
    });
  }).filter(Boolean);
}

function runtimeCapabilityActions(manifest) {
  const capabilities = arrayValue(isRecord(manifest?.agentRuntime) ? manifest.agentRuntime.capabilities : null);
  return capabilities.map((capability) => {
    const family = tokenValue(capability.family || capability.id || capability.name, 80);
    if (!family) {
      return null;
    }
    const actions = stringList(capability.actions, 12, 36);
    const proof = stringList(capability.proof, 10, 40);
    const risk = stringValue(capability.risk, 24);
    return normalizeAction({
      id: `runtime:${family}`,
      source: "agent-manifest",
      kind: "runtime-capability",
      title: humanTitle(family),
      label: labelFor(family),
      summary: cleanText(`Actions: ${actions.join(", ")}${risk ? `. Risk: ${risk}` : ""}`, 180),
      tags: compactList([family, risk, ...actions]),
      runtime: {
        family,
        actions,
        risk,
        proof,
        requiresConfirmation: capability.requiresConfirmation === true
      }
    });
  }).filter(Boolean);
}

function computerUseCapabilityActions(manifest) {
  const capabilities = stringList(isRecord(manifest?.computerUsePlane) ? manifest.computerUsePlane.capabilities : null, 80, 80);
  return capabilities.map((capability) => normalizeAction({
    id: `computer:${tokenValue(capability, 100)}`,
    source: "agent-manifest",
    kind: "computer-capability",
    title: humanTitle(capability),
    label: labelFor(capability),
    summary: "Computer-use capability exposed by the Soty agent plane.",
    tags: compactList([capability, "computer", "agent"]),
    runtime: {
      capability,
      entryTool: stringValue(manifest?.computerUsePlane?.entryTool, 40) || "computer"
    }
  })).filter(Boolean);
}

function miniAppActions(manifest) {
  const apps = arrayValue(manifest?.apps);
  return apps.map((app) => {
    const id = tokenValue(app.id || app.title, 100);
    if (!id) {
      return null;
    }
    return normalizeAction({
      id: `mini-app:${id}`,
      source: "mini-app-manifest",
      kind: "mini-app",
      title: cleanText(app.title || humanTitle(id), 100),
      label: labelFor(app.icon || app.title || id),
      summary: cleanText(app.summary || app.description || "Mini app surface.", 180),
      tags: compactList([id, app.icon, app.profileId, app.profileTitle, ...stringList(app.tags, 24, 80), ...stringList(app.capabilities, 12, 80)]),
      runtime: {
        appId: id,
        url: stringValue(app.url, 240),
        profileId: stringValue(app.profileId, 80),
        profileTitle: stringValue(app.profileTitle, 100),
        tags: stringList(app.tags, 24, 80),
        capabilities: stringList(app.capabilities, 12, 80)
      }
    });
  }).filter(Boolean);
}

function normalizeProvidedAction(value, source) {
  if (!isRecord(value)) {
    return null;
  }
  const id = tokenValue(value.id || value.name || value.title, 140);
  if (!id) {
    return null;
  }
  return normalizeAction({
    id: `provided:${id}`,
    source,
    kind: stringValue(value.kind, 60) || "provided",
    title: cleanText(value.title || humanTitle(id), 100),
    label: stringValue(value.label, 12) || labelFor(value.title || id),
    summary: cleanText(value.summary || value.description || value.intent, 180),
    tags: stringList(value.tags, 24, 80),
    runtime: isRecord(value.runtime) ? value.runtime : {}
  });
}

function normalizeAction(action) {
  const id = tokenValue(action.id, 180);
  const title = cleanText(action.title, 100);
  if (!id || !title) {
    return null;
  }
  return {
    schema: actionSchema,
    id,
    source: stringValue(action.source, 60) || "manifest",
    kind: stringValue(action.kind, 60) || "capability",
    title,
    label: cleanText(action.label, 16) || labelFor(title),
    summary: cleanText(action.summary, 220),
    tags: stringList(action.tags, 32, 80),
    runtime: publicObject(action.runtime)
  };
}

function dedupeActions(actions) {
  const seen = new Set();
  const result = [];
  for (const action of actions) {
    if (!action?.id || seen.has(action.id)) {
      continue;
    }
    seen.add(action.id);
    result.push(action);
  }
  return result.sort((left, right) => actionRank(left) - actionRank(right) || left.title.localeCompare(right.title));
}

function actionRank(action) {
  if (action.kind === "route-profile") {
    return 10;
  }
  if (action.kind === "toolkit") {
    return 20;
  }
  if (action.kind === "mini-app") {
    return 30;
  }
  if (action.kind === "runtime-capability") {
    return 40;
  }
  return 50;
}

function publicObject(value) {
  if (!isRecord(value)) {
    return {};
  }
  const output = {};
  for (const [key, raw] of Object.entries(value).slice(0, 32)) {
    const cleanKey = tokenValue(key, 80);
    if (!cleanKey) {
      continue;
    }
    if (typeof raw === "boolean" || typeof raw === "number") {
      output[cleanKey] = raw;
    } else if (typeof raw === "string") {
      output[cleanKey] = cleanText(raw, 240);
    } else if (Array.isArray(raw)) {
      output[cleanKey] = stringList(raw, 32, 120);
    }
  }
  return output;
}

function compactList(values) {
  return values
    .flatMap((value) => Array.isArray(value) ? value : [value])
    .map((value) => cleanText(value, 80))
    .filter(Boolean);
}

function stringList(value, maxItems, maxLength) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => cleanText(item, maxLength))
    .filter(Boolean)
    .slice(0, maxItems);
}

function arrayValue(value) {
  return Array.isArray(value) ? value.filter(isRecord) : [];
}

function labelFor(value) {
  const clean = cleanText(value, 80);
  const words = clean.split(/[^A-Za-z0-9]+/u).filter(Boolean);
  if (words.length >= 2) {
    return words.slice(0, 2).map((word) => word[0]).join("").toUpperCase();
  }
  const token = (words[0] || clean).replace(/[^A-Za-z0-9]/gu, "");
  return token.slice(0, 4).toUpperCase() || "ACT";
}

function humanTitle(value) {
  return cleanText(value, 100)
    .replace(/[_:-]+/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .replace(/\b\w/gu, (char) => char.toUpperCase()) || "Capability";
}

function tokenValue(value, maxLength) {
  return String(value || "")
    .trim()
    .replace(/[^A-Za-z0-9._:-]+/gu, "-")
    .replace(/-+/gu, "-")
    .replace(/^-|-$/gu, "")
    .slice(0, maxLength);
}

function stringValue(value, maxLength) {
  return typeof value === "string" ? cleanText(value, maxLength) : "";
}

function cleanText(value, maxLength) {
  return String(value || "")
    .replace(/[\u0000-\u001F\u007F]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, maxLength);
}

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
