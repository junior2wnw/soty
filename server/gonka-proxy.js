import { createHash, timingSafeEqual } from "node:crypto";
import { readFileSync } from "node:fs";
import { createInferenceRelay } from "./inference-relay.js";

export const defaultGonkaProxyModel = "deepseek-ai/DeepSeek-V4-Flash-0731";
export const miniMaxProxyModel = "MiniMaxAI/MiniMax-M2.7";

const supportedModels = new Set([defaultGonkaProxyModel, miniMaxProxyModel]);

const maxRequestBytes = 4 * 1024 * 1024;
const maxConcurrentPerInstallation = 100;

export function createGonkaProxy({
  store,
  baseUrl = process.env.SOTY_GONKA_BASE_URL || "https://gate.joingonka.ai/v1",
  apiKey = process.env.SOTY_GONKA_API_KEY || "",
  model = defaultGonkaProxyModel,
  upstreamModel = process.env.SOTY_GONKA_UPSTREAM_MODEL || model,
  timeoutMs = process.env.SOTY_GONKA_REQUEST_TIMEOUT_MS,
  fallbackBaseUrl = process.env.SOTY_GONKA_FALLBACK_BASE_URL || "",
  fallbackApiKey = process.env.SOTY_GONKA_FALLBACK_API_KEY || "",
  firstTokenTimeoutMs = process.env.SOTY_GONKA_FIRST_TOKEN_TIMEOUT_MS,
  idleTimeoutMs = process.env.SOTY_GONKA_IDLE_TIMEOUT_MS,
  streamTimeoutMs = process.env.SOTY_GONKA_STREAM_TIMEOUT_MS,
  providerConcurrency = process.env.SOTY_GONKA_PROVIDER_CONCURRENCY,
  maximumQueued = process.env.SOTY_GONKA_MAX_QUEUED,
  queueWaitMs = process.env.SOTY_GONKA_QUEUE_WAIT_MS,
  internalStream = process.env.SOTY_GONKA_INTERNAL_STREAM !== "0",
  providerStrategy = process.env.SOTY_GONKA_PROVIDER_STRATEGY || "race",
  emptyToolFallback = process.env.SOTY_GONKA_EMPTY_TOOL_FALLBACK !== "0",
  onEvent = process.env.SOTY_GONKA_LOG_METRICS === "1" ? (event) => process.stdout.write(`${JSON.stringify(event)}\n`) : undefined,
  fetchImpl = fetch
} = {}) {
  const upstreamBaseUrl = safeUpstreamBaseUrl(baseUrl);
  const upstreamKey = safeSecret(apiKey);
  const requestTimeoutMs = safeInteger(timeoutMs, 10_000, 10 * 60_000, 120_000);
  const fallbackUrl = safeUpstreamBaseUrl(fallbackBaseUrl);
  const fallbackKey = safeSecret(fallbackApiKey);
  const fallbackValid = !fallbackBaseUrl && !fallbackApiKey || Boolean(fallbackUrl && fallbackKey);
  const ready = Boolean(store && upstreamBaseUrl && upstreamKey && fallbackValid && supportedModels.has(model) && supportedModels.has(upstreamModel));
  const active = new Map();
  const relay = createInferenceRelay({
    providers: [
      { name: "primary", baseUrl: upstreamBaseUrl, apiKey: upstreamKey },
      ...(fallbackUrl && fallbackKey ? [{ name: "fallback", baseUrl: fallbackUrl, apiKey: fallbackKey }] : [])
    ],
    fetchImpl, requestTimeoutMs,
    firstTokenTimeoutMs: safeInteger(firstTokenTimeoutMs, 1000, 150000, 20000),
    idleTimeoutMs: safeInteger(idleTimeoutMs, 1000, 120000, 30000),
    streamTimeoutMs: safeInteger(streamTimeoutMs, 10000, 3600000, 1800000),
    concurrency: safeInteger(providerConcurrency, 1, 100, 8),
    maximumQueued: safeInteger(maximumQueued, 0, 1000, 64),
    queueWaitMs: safeInteger(queueWaitMs, 100, 30000, 5000),
    internalStream: Boolean(internalStream), normalizeTools: upstreamModel === miniMaxProxyModel,
    providerStrategy, emptyToolFallback: Boolean(emptyToolFallback) && upstreamModel === miniMaxProxyModel, onEvent
  });

  return {
    ready,
    model,
    upstreamModel,
    providerStrategy,
    transport: "authenticated-server-proxy",
    upstreamStatus: () => relay.snapshot(),
    async handleChatCompletions(req, res, { authenticateToken, client = "connector" } = {}) {
      if (!ready) {
        respondJson(res, 503, { error: { message: "model-proxy-unavailable", type: "server_configuration" } });
        return;
      }
      const token = bearerToken(req);
      const authenticate = authenticateToken || ((candidate) => store.authenticateModelToken(candidate));
      const authenticatedAs = token ? await authenticate(token) : false;
      if (!authenticatedAs) {
        respondJson(res, 401, { error: { message: "connector-auth-failed", type: "authentication_error" } });
        return;
      }
      const body = cleanChatRequest(req.body, model, upstreamModel);
      if (!body) {
        respondJson(res, 400, { error: { message: "invalid-model-request", type: "invalid_request_error" } });
        return;
      }
      const bytes = Buffer.from(JSON.stringify(body));
      if (bytes.length > maxRequestBytes) {
        respondJson(res, 413, { error: { message: "model-request-too-large", type: "invalid_request_error" } });
        return;
      }
      const identity = typeof authenticatedAs === "string"
        ? authenticatedAs
        : createHash("sha256").update(token).digest("hex");
      const installation = `${client}:${identity}`;
      const running = active.get(installation) || 0;
      if (running >= maxConcurrentPerInstallation) {
        respondJson(res, 429, { error: { message: "model-concurrency-limit", type: "rate_limit_error" } });
        return;
      }
      active.set(installation, running + 1);
      const controller = new AbortController();
      const abort = () => { if (!res.writableEnded) controller.abort(new Error("model-client-disconnected")); };
      res.once("close", abort);
      try {
        await relay.forward({ body, res, signal: controller.signal });
      } catch (error) {
        if (!res.headersSent && !res.destroyed) {
          respondJson(res, error?.httpStatus || 502, { error: { message: error?.publicCode || "model-upstream-unavailable", type: "upstream_error" } });
        } else if (!res.destroyed) {
          if (String(res.getHeader("Content-Type") || "").includes("text/event-stream")) {
            res.write(`data: ${JSON.stringify({ error: { message: "model-upstream-incomplete", type: "upstream_error" } })}\n\n`);
          }
          res.end();
        }
      } finally {
        res.removeListener("close", abort);
        const remaining = (active.get(installation) || 1) - 1;
        if (remaining > 0) active.set(installation, remaining);
        else active.delete(installation);
      }
    }
  };
}

export function createApplicationTokenAuthenticator(
  value = process.env.SOTY_GONKA_APPLICATION_TOKENS || "",
  { filePath = process.env.SOTY_GONKA_APPLICATION_TOKENS_FILE || "" } = {}
) {
  const environmentEntries = applicationTokenEntries(value);
  const fileResult = applicationTokenFileEntries(filePath);
  const entries = fileResult.ok ? mergeApplicationTokenEntries(environmentEntries, fileResult.entries) : [];
  const authenticate = async (token) => {
    if (!token) return false;
    const digest = createHash("sha256").update(token).digest();
    for (const entry of entries) {
      if (timingSafeEqual(digest, entry.digest)) return entry.id;
    }
    return false;
  };
  authenticate.ready = entries.length > 0;
  return authenticate;
}

function applicationTokenFileEntries(filePath) {
  const path = String(filePath || "").trim();
  if (!path) return { ok: true, entries: [] };
  let parsed;
  try {
    parsed = JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return { ok: false, entries: [] };
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)
      || Object.keys(parsed).length !== 1 || !Array.isArray(parsed.applications)) {
    return { ok: false, entries: [] };
  }
  const entries = [];
  for (const item of parsed.applications) {
    if (!item || typeof item !== "object" || Array.isArray(item)
        || Object.keys(item).some((key) => !["id", "token"].includes(key))) {
      return { ok: false, entries: [] };
    }
    const entry = applicationTokenEntry(item.id, item.token);
    if (!entry) return { ok: false, entries: [] };
    entries.push(entry);
  }
  return uniqueApplicationTokenEntries(entries);
}

function applicationTokenEntries(value) {
  let parsed;
  try {
    parsed = JSON.parse(String(value || ""));
  } catch {
    return [];
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return [];
  const entries = [];
  for (const [rawId, rawToken] of Object.entries(parsed)) {
    const entry = applicationTokenEntry(rawId, rawToken);
    if (entry) entries.push(entry);
  }
  return entries;
}

function applicationTokenEntry(rawId, rawToken) {
  const id = String(rawId || "").trim();
  const token = String(rawToken || "").trim();
  if (!/^[a-z][a-z0-9_-]{1,63}$/u.test(id) || !/^[A-Za-z0-9_-]{40,160}$/u.test(token)) return null;
  return { id, digest: createHash("sha256").update(token).digest() };
}

function mergeApplicationTokenEntries(left, right) {
  return uniqueApplicationTokenEntries([...left, ...right]).entries;
}

function uniqueApplicationTokenEntries(entries) {
  const ids = new Set();
  const digests = new Set();
  for (const entry of entries) {
    const digest = entry.digest.toString("base64");
    if (ids.has(entry.id) || digests.has(digest)) return { ok: false, entries: [] };
    ids.add(entry.id);
    digests.add(digest);
  }
  return { ok: true, entries };
}

function cleanChatRequest(value, model, upstreamModel) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  // Existing DeepSeek and MiniMax client identifiers address the operator's
  // selected MiniMax upstream, including short OpenAI-compatible model aliases.
  if (!acceptedClientModel(value.model, model, upstreamModel) || !Array.isArray(value.messages) || value.messages.length < 1 || value.messages.length > 2_048) return null;
  if (value.stream != null && typeof value.stream !== "boolean") return null;
  if (value.n != null && value.n !== 1) return null;
  for (const name of ["max_tokens", "max_completion_tokens"]) {
    if (value[name] != null && (!Number.isSafeInteger(value[name]) || value[name] < 1 || value[name] > 8_192)) return null;
  }
  const body = { ...value, model: upstreamModel };
  if (upstreamModel === miniMaxProxyModel && Array.isArray(value.tools)) {
    body.tools = value.tools.map((tool) => tool?.type === "function" && tool.function?.parameters
      ? { ...tool, function: { ...tool.function, parameters: compatibleMiniMaxSchema(tool.function.parameters) } }
      : tool);
  }
  return body;
}

function acceptedClientModel(value, model, upstreamModel) {
  if (typeof value !== "string" || value.length > 160) return false;
  const name = value.trim().toLowerCase();
  if ([model, upstreamModel].some((item) => item.toLowerCase() === name)) return true;
  if (upstreamModel !== miniMaxProxyModel) return false;
  return /^(?:deepseek-ai\/|deepseek\/)?deepseek(?:[-_.][a-z0-9][a-z0-9._-]*)?$/u.test(name)
    || /^(?:minimaxai\/|minimax\/)?minimax(?:[-_.][a-z0-9][a-z0-9._-]*)?$/u.test(name);
}

function compatibleMiniMaxSchema(value) {
  if (Array.isArray(value)) return value.map(compatibleMiniMaxSchema);
  if (!value || typeof value !== "object") return value;
  const schema = Object.fromEntries(Object.entries(value).map(([key, item]) => [key, compatibleMiniMaxSchema(item)]));
  // OpenBroker validates patterns with RE2, which has no lookahead. Express
  // these existing client constraints with equivalent JSON Schema negation.
  if (schema.pattern === "^(?!)$") {
    delete schema.pattern;
    schema.allOf = [...(schema.allOf || []), { not: { type: "string" } }];
  } else if (schema.pattern === "^(?![\\s\\S]*(?:[hH][tT][tT][pP][sS]?://))[\\s\\S]+$") {
    schema.pattern = "^[\\s\\S]+$";
    schema.allOf = [...(schema.allOf || []), { not: { type: "string", pattern: "[hH][tT][tT][pP][sS]?://" } }];
  }
  return schema;
}

function safeUpstreamBaseUrl(value) {
  try {
    const url = new URL(String(value || ""));
    const local = ["127.0.0.1", "localhost", "::1"].includes(url.hostname);
    if (url.username || url.password || url.search || url.hash || (url.protocol !== "https:" && !(local && url.protocol === "http:"))) return "";
    return url.toString().replace(/\/$/u, "");
  } catch {
    return "";
  }
}

function safeSecret(value) {
  const secret = String(value || "").trim();
  return secret.length >= 20 && secret.length <= 512 ? secret : "";
}

function bearerToken(req) {
  const match = /^Bearer\s+([A-Za-z0-9_-]{40,160})$/u.exec(String(req.headers.authorization || ""));
  return match?.[1] || "";
}

function safeInteger(value, minimum, maximum, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isSafeInteger(parsed) && parsed >= minimum && parsed <= maximum ? parsed : fallback;
}

function respondJson(res, status, value) {
  if (res.headersSent || res.destroyed) return;
  res.status(status);
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.json(value);
}
