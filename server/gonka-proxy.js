import { createHash, timingSafeEqual } from "node:crypto";
import { Readable } from "node:stream";

export const defaultGonkaProxyModel = "deepseek-ai/DeepSeek-V4-Flash-0731";

const maxRequestBytes = 4 * 1024 * 1024;
const maxConcurrentPerInstallation = 2;

export function createGonkaProxy({
  store,
  baseUrl = process.env.SOTY_GONKA_BASE_URL || "https://gate.joingonka.ai/v1",
  apiKey = process.env.SOTY_GONKA_API_KEY || "",
  model = defaultGonkaProxyModel,
  timeoutMs = process.env.SOTY_GONKA_REQUEST_TIMEOUT_MS,
  fetchImpl = fetch
} = {}) {
  const upstreamBaseUrl = safeUpstreamBaseUrl(baseUrl);
  const upstreamKey = safeSecret(apiKey);
  const requestTimeoutMs = safeInteger(timeoutMs, 10_000, 10 * 60_000, 120_000);
  const ready = Boolean(store && upstreamBaseUrl && upstreamKey && model === defaultGonkaProxyModel);
  const active = new Map();

  return {
    ready,
    model,
    transport: "authenticated-server-proxy",
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
      const body = cleanChatRequest(req.body, model);
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
      const timer = setTimeout(() => controller.abort(new Error("model-upstream-timeout")), requestTimeoutMs);
      timer.unref?.();
      const abort = () => controller.abort(new Error("model-client-disconnected"));
      res.once("close", abort);
      try {
        const upstream = await fetchImpl(new URL("chat/completions", `${upstreamBaseUrl}/`), {
          method: "POST",
          redirect: "error",
          headers: {
            Authorization: `Bearer ${upstreamKey}`,
            Accept: body.stream === false ? "application/json" : "text/event-stream",
            "Content-Type": "application/json"
          },
          body: bytes,
          signal: controller.signal
        });
        if (res.destroyed) return;
        res.status(upstream.status);
        res.setHeader("Cache-Control", "no-store");
        res.setHeader("X-Content-Type-Options", "nosniff");
        res.setHeader("X-Soty-Model-Proxy", "gonka");
        const contentType = upstream.headers.get("content-type");
        if (contentType) res.setHeader("Content-Type", contentType);
        if (!upstream.body) {
          res.end();
          return;
        }
        await new Promise((resolvePipe, reject) => {
          const stream = Readable.fromWeb(upstream.body);
          stream.once("error", reject);
          res.once("error", reject);
          res.once("finish", resolvePipe);
          stream.pipe(res);
        });
      } catch (error) {
        if (!res.headersSent && !res.destroyed) {
          const timeout = controller.signal.aborted && !res.closed;
          respondJson(res, timeout ? 504 : 502, { error: { message: timeout ? "model-upstream-timeout" : "model-upstream-unavailable", type: "upstream_error" } });
        } else if (!res.destroyed) {
          res.end();
        }
      } finally {
        clearTimeout(timer);
        res.removeListener("close", abort);
        const remaining = (active.get(installation) || 1) - 1;
        if (remaining > 0) active.set(installation, remaining);
        else active.delete(installation);
      }
    }
  };
}

export function createApplicationTokenAuthenticator(value = process.env.SOTY_GONKA_APPLICATION_TOKENS || "") {
  const entries = applicationTokenEntries(value);
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
    const id = String(rawId || "").trim();
    const token = String(rawToken || "").trim();
    if (!/^[a-z][a-z0-9_-]{1,63}$/u.test(id) || !/^[A-Za-z0-9_-]{40,160}$/u.test(token)) continue;
    entries.push({ id, digest: createHash("sha256").update(token).digest() });
  }
  return entries;
}

function cleanChatRequest(value, model) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  if (value.model !== model || !Array.isArray(value.messages) || value.messages.length < 1 || value.messages.length > 2_048) return null;
  if (value.stream != null && typeof value.stream !== "boolean") return null;
  if (value.n != null && value.n !== 1) return null;
  for (const name of ["max_tokens", "max_completion_tokens"]) {
    if (value[name] != null && (!Number.isSafeInteger(value[name]) || value[name] < 1 || value[name] > 8_192)) return null;
  }
  return value;
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
