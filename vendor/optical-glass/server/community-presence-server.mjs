import { createHmac, randomBytes } from "node:crypto";
import { createServer } from "node:http";
import { pathToFileURL } from "node:url";
import { normalizePresencePulse } from "../src/community-presence.js";

const DEFAULT_TTL_MS = 12_000;
const DEFAULT_RATE_WINDOW_MS = 60_000;
const DEFAULT_RATE_LIMIT = 12;
const DEFAULT_MAX_RECENT = 32;
const DEFAULT_MAX_CONNECTIONS = 500;
const DEFAULT_MAX_CONNECTIONS_PER_SOURCE = 8;
const MAX_BODY_BYTES = 1_024;
const ALLOWED_KEYS = new Set([
  "protocol",
  "kind",
  "eventId",
  "space",
  "x",
  "y",
  "tone",
  "createdAt",
  "expiresAt",
]);

const splitOrigins = (value = "") => (
  Array.isArray(value)
    ? value
    : String(value).split(",").map((origin) => origin.trim()).filter(Boolean)
);

const send = (response, status, headers = {}) => {
  response.writeHead(status, {
    "cache-control": "no-store",
    ...headers,
  });
  response.end();
};

const readJson = (request) => new Promise((resolve, reject) => {
  let size = 0;
  let body = "";
  let tooLarge = false;
  request.setEncoding("utf8");
  request.on("data", (chunk) => {
    if (tooLarge) return;
    size += Buffer.byteLength(chunk);
    if (size > MAX_BODY_BYTES) {
      tooLarge = true;
      reject(Object.assign(new Error("body-too-large"), { status: 413 }));
      return;
    }
    body += chunk;
  });
  request.on("end", () => {
    try {
      const value = JSON.parse(body);
      resolve(value);
    } catch {
      reject(Object.assign(new Error("invalid-json"), { status: 400 }));
    }
  });
  request.on("error", reject);
});

const strictPulseShape = (value) => (
  value
  && typeof value === "object"
  && !Array.isArray(value)
  && Object.keys(value).every((key) => ALLOWED_KEYS.has(key))
);

/**
 * Dependency-free reference relay. It intentionally keeps only short-lived
 * pulses and transient, process-salted rate-limit buckets in memory.
 */
export const createCommunityPresenceServer = ({
  allowedOrigins = process.env.PRESENCE_ALLOWED_ORIGINS ?? "",
  now = Date.now,
  ttlMs = DEFAULT_TTL_MS,
  rateWindowMs = DEFAULT_RATE_WINDOW_MS,
  rateLimit = DEFAULT_RATE_LIMIT,
  maxRecent = DEFAULT_MAX_RECENT,
  maxConnections = DEFAULT_MAX_CONNECTIONS,
  maxConnectionsPerSource = DEFAULT_MAX_CONNECTIONS_PER_SOURCE,
  trustProxy = false,
} = {}) => {
  const originAllowlist = new Set(splitOrigins(allowedOrigins));
  const processSecret = randomBytes(32);
  const rateBuckets = new Map();
  const subscribers = new Map();
  const recentBySpace = new Map();
  const openResponses = new Set();
  const streamCounts = new Map();

  const corsHeaders = (origin) => ({
    "access-control-allow-origin": origin,
    "access-control-allow-methods": "GET, POST, OPTIONS",
    "access-control-allow-headers": "content-type",
    "access-control-max-age": "600",
    vary: "Origin",
  });

  const originIsAllowed = (origin) => originAllowlist.has(origin);

  const sourceBucket = (request, origin) => {
    const forwarded = trustProxy
      ? String(request.headers["x-forwarded-for"] ?? "").split(",")[0].trim()
      : "";
    const address = forwarded || request.socket.remoteAddress || "unknown";
    return createHmac("sha256", processSecret)
      .update(`${origin}|${address}`)
      .digest("base64url")
      .slice(0, 22);
  };

  const acceptsRate = (key) => {
    const current = now();
    const bucket = rateBuckets.get(key) ?? [];
    while (bucket.length && bucket[0] <= current - rateWindowMs) bucket.shift();
    if (bucket.length >= rateLimit) {
      rateBuckets.set(key, bucket);
      return false;
    }
    bucket.push(current);
    rateBuckets.set(key, bucket);
    return true;
  };

  const freshRecent = (space) => {
    const current = now();
    const fresh = (recentBySpace.get(space) ?? []).filter((pulse) => pulse.expiresAt > current);
    if (fresh.length) recentBySpace.set(space, fresh);
    else recentBySpace.delete(space);
    return fresh;
  };

  const broadcast = (pulse) => {
    const event = `event: pulse\ndata: ${JSON.stringify(pulse)}\n\n`;
    for (const response of subscribers.get(pulse.space) ?? []) {
      if (!response.destroyed && !response.writableEnded) response.write(event);
    }
  };

  const server = createServer(async (request, response) => {
    const origin = String(request.headers.origin ?? "");
    const requestUrl = new URL(request.url ?? "/", "http://presence.local");
    const isPulseEndpoint = requestUrl.pathname === "/v1/pulses";
    const isStreamEndpoint = requestUrl.pathname === "/v1/pulses/stream";

    if (!originIsAllowed(origin)) {
      send(response, 403);
      return;
    }
    const cors = corsHeaders(origin);

    if (request.method === "OPTIONS" && (isPulseEndpoint || isStreamEndpoint)) {
      send(response, 204, cors);
      return;
    }

    if (request.method === "POST" && isPulseEndpoint) {
      if (!String(request.headers["content-type"] ?? "").toLowerCase().startsWith("application/json")) {
        send(response, 415, cors);
        return;
      }
      if (!acceptsRate(sourceBucket(request, origin))) {
        send(response, 429, { ...cors, "retry-after": "60" });
        return;
      }

      try {
        const body = await readJson(request);
        if (!strictPulseShape(body)) {
          send(response, 400, cors);
          return;
        }
        const createdAt = now();
        const pulse = normalizePresencePulse({
          ...body,
          protocol: 1,
          kind: "pulse",
          createdAt,
          expiresAt: createdAt + ttlMs,
        }, { now: createdAt, ttlMs });
        if (!pulse) {
          send(response, 400, cors);
          return;
        }

        const recent = freshRecent(pulse.space);
        recent.push(pulse);
        recentBySpace.set(pulse.space, recent.slice(-maxRecent));
        broadcast(pulse);
        send(response, 202, cors);
      } catch (error) {
        if (!response.headersSent && !response.destroyed) send(response, error.status ?? 400, cors);
      }
      return;
    }

    if (request.method === "GET" && isStreamEndpoint) {
      const space = requestUrl.searchParams.get("space");
      const current = now();
      const probe = normalizePresencePulse({
        protocol: 1,
        kind: "pulse",
        eventId: "000000000000",
        space,
        x: 0.5,
        y: 0.5,
        tone: 0.5,
        createdAt: current,
        expiresAt: current + 250,
      }, { now: current, ttlMs: 250 });
      if (!probe) {
        send(response, 400, cors);
        return;
      }

      const streamKey = sourceBucket(request, origin);
      const activeForSource = streamCounts.get(streamKey) ?? 0;
      if (openResponses.size >= maxConnections || activeForSource >= maxConnectionsPerSource) {
        send(response, 429, { ...cors, "retry-after": "30" });
        return;
      }

      response.writeHead(200, {
        ...cors,
        "cache-control": "no-store, no-transform",
        "content-type": "text/event-stream; charset=utf-8",
        connection: "keep-alive",
        "x-accel-buffering": "no",
      });
      response.write(": connected\n\n");
      openResponses.add(response);
      streamCounts.set(streamKey, activeForSource + 1);
      const spaceSubscribers = subscribers.get(space) ?? new Set();
      subscribers.set(space, spaceSubscribers);
      spaceSubscribers.add(response);

      for (const pulse of freshRecent(space)) {
        response.write(`event: pulse\ndata: ${JSON.stringify(pulse)}\n\n`);
      }

      const heartbeat = setInterval(() => response.write(": keepalive\n\n"), 25_000);
      heartbeat.unref?.();
      const cleanup = () => {
        clearInterval(heartbeat);
        openResponses.delete(response);
        const remaining = (streamCounts.get(streamKey) ?? 1) - 1;
        if (remaining > 0) streamCounts.set(streamKey, remaining);
        else streamCounts.delete(streamKey);
        spaceSubscribers.delete(response);
        if (!spaceSubscribers.size) subscribers.delete(space);
      };
      request.once("close", cleanup);
      return;
    }

    send(response, 404, cors);
  });

  const cleanupTimer = setInterval(() => {
    const current = now();
    for (const [key, bucket] of rateBuckets) {
      const fresh = bucket.filter((timestamp) => timestamp > current - rateWindowMs);
      if (fresh.length) rateBuckets.set(key, fresh);
      else rateBuckets.delete(key);
    }
    for (const space of recentBySpace.keys()) freshRecent(space);
  }, Math.min(ttlMs, rateWindowMs));
  cleanupTimer.unref?.();

  return {
    server,
    listen(...args) {
      return server.listen(...args);
    },
    async close() {
      clearInterval(cleanupTimer);
      for (const response of openResponses) response.destroy();
      openResponses.clear();
      await new Promise((resolve, reject) => server.close((error) => (
        error ? reject(error) : resolve()
      )));
    },
  };
};

const isEntryPoint = process.argv[1]
  && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isEntryPoint) {
  const port = Number(process.env.PORT ?? 8787);
  const relay = createCommunityPresenceServer();
  relay.listen(port, "0.0.0.0", () => {
    // Startup metadata only; pulse and network data are never logged.
    console.log(`Community presence relay listening on :${port}`);
  });
}
