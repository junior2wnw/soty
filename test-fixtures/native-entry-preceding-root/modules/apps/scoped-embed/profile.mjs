import { createHash } from "node:crypto";

export const SCOPED_EMBED_PROFILE = "soty.selected-human-embed.v1";
export const SOURCE_PROOF_PROTOCOL = "soty.selected-source-request.v1";
export class ScopedEmbedError extends Error {
  constructor(code, status = 400) {
    super(code);
    this.code = code;
    this.status = status;
  }
}
export function need(ok, code = "scoped_embed_invalid", status = 400) {
  if (!ok) throw new ScopedEmbedError(code, status);
}
export function capture(input, depth = 0) {
  need(depth <= 16);
  if (input === null || typeof input === "boolean") return input;
  if (typeof input === "number") {
    need(Number.isSafeInteger(input));
    return input;
  }
  if (typeof input === "string") {
    need(input.length <= 8192 && input.isWellFormed());
    return input;
  }
  need(
    input &&
      typeof input === "object" &&
      Object.getOwnPropertySymbols(input).length === 0,
  );
  const descriptors = Object.getOwnPropertyDescriptors(input);
  if (Array.isArray(input)) {
    need(
      input.length <= 64 &&
        Object.keys(descriptors).length === input.length + 1,
    );
    return Object.freeze(
      Array.from({ length: input.length }, (_, i) => {
        need(descriptors[i] && Object.hasOwn(descriptors[i], "value"));
        return capture(descriptors[i].value, depth + 1);
      }),
    );
  }
  need([Object.prototype, null].includes(Object.getPrototypeOf(input)));
  const value = {};
  for (const [key, p] of Object.entries(descriptors)) {
    need(
      key.length <= 128 &&
        !["__proto__", "constructor", "prototype"].includes(key) &&
        p.enumerable &&
        Object.hasOwn(p, "value"),
    );
    value[key] = capture(p.value, depth + 1);
  }
  return Object.freeze(value);
}
export function closed(value, required, optional = []) {
  need(
    value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      required.every((key) => Object.hasOwn(value, key)) &&
      Object.keys(value).every(
        (key) => required.includes(key) || optional.includes(key),
      ),
  );
  return value;
}
export function identifier(value) {
  need(
    typeof value === "string" &&
      /^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,179}$/u.test(value) &&
      !value.includes(".."),
  );
  return value;
}
export function sha(value) {
  need(typeof value === "string" && /^[a-f0-9]{64}$/u.test(value));
  return value;
}
export function canonical(value) {
  if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
  if (value && typeof value === "object")
    return (
      "{" +
      Object.keys(value)
        .sort()
        .map((k) => JSON.stringify(k) + ":" + canonical(value[k]))
        .join(",") +
      "}"
    );
  return JSON.stringify(value);
}
export const hash = (value) =>
  createHash("sha256")
    .update(typeof value === "string" ? value : canonical(value))
    .digest("hex");
export function pin(value) {
  closed(value, ["id", "version", "digest"]);
  identifier(value.id);
  need(Number.isSafeInteger(value.version) && value.version > 0);
  sha(value.digest);
  return value;
}
export function continuation(value) {
  closed(value, ["id", "version", "digest"]);
  need(/^[A-Za-z0-9_-]{43}$/u.test(value.id) && value.version === 1);
  sha(value.digest);
  return value;
}
export function connector(value) {
  closed(value, ["linkId", "hostDeviceId", "connectorId"]);
  Object.values(value).forEach(identifier);
  return value;
}
export function approvedOrigin(value) {
  const url = new URL(value);
  need(
    value === url.origin &&
      !url.username &&
      !url.password &&
      (url.protocol === "https:" ||
        (url.protocol === "http:" &&
          ["127.0.0.1", "localhost", "[::1]"].includes(url.hostname) &&
          Number(url.port) >= 1024)),
    "scoped_embed_origin_invalid",
  );
  return value;
}

/** Approved host profile only. Neither an author descriptor nor request data
 * may install it, choose a destination/command or widen its selected resource. */
export function scopedEmbedProfile(input) {
  const value = capture(input);
  closed(value, [
    "schema",
    "appId",
    "connector",
    "target",
    "sourceProfile",
    "resource",
    "issuer",
    "clientId",
    "embedOrigin",
    "nativeOrigin",
    "parentOrigin",
  ]);
  need(
    value.schema === SCOPED_EMBED_PROFILE &&
      /^app-[a-f0-9]{32}$/u.test(value.appId),
  );
  connector(value.connector);
  closed(value.target, ["revision", "digest"]);
  need(
    Number.isSafeInteger(value.target.revision) && value.target.revision > 0,
  );
  sha(value.target.digest);
  pin(value.sourceProfile);
  closed(value.resource, [
    "registryId",
    "tenantId",
    "environmentId",
    "appId",
    "resourceId",
    "workspaceId",
  ]);
  Object.values(value.resource).forEach(identifier);
  need(value.resource.appId === value.appId);
  approvedOrigin(value.embedOrigin);
  approvedOrigin(value.nativeOrigin);
  approvedOrigin(value.parentOrigin);
  need(
    new URL(value.nativeOrigin).hostname !==
      new URL(value.embedOrigin).hostname &&
      value.parentOrigin !== value.embedOrigin,
  );
  const issuer = new URL(value.issuer);
  need(
    issuer.href === value.issuer &&
      issuer.pathname === "/human-identity" &&
      issuer.origin === value.parentOrigin &&
      !issuer.search &&
      !issuer.hash,
  );
  identifier(value.clientId);
  return Object.freeze({ ...value, digest: hash(value) });
}

/** Durable native consent binds semantic app/resource/profile identity. A new
 * launch/device or a UI-only deployment does not silently widen that consent. */
export function sourceConsentDigest(input) {
  const profile = scopedEmbedProfile(input);
  return hash({
    schema: "soty.selected-source-consent.v1",
    appId: profile.appId,
    sourceProfile: profile.sourceProfile,
    resource: profile.resource,
    issuer: profile.issuer,
    clientId: profile.clientId,
    embedOrigin: profile.embedOrigin,
    nativeOrigin: profile.nativeOrigin,
    parentOrigin: profile.parentOrigin,
  });
}

/** Prototype surface deliberately excludes legacy state/auth/keys/workspaces,
 * scheduler/configuration, arbitrary forwarding, WebSockets and commands. */
export function scopedRoute(method, input) {
  need(
    typeof input === "string" &&
      input.length <= 8192 &&
      input.startsWith("/") &&
      !input.startsWith("//") &&
      !/[\\\s\u0000-\u001f\u007f]/u.test(input),
  );
  const url = new URL(input, "https://fixed.invalid");
  need(
    url.pathname + url.search === input && !url.hash,
    "scoped_embed_route_denied",
    403,
  );
  let decoded;
  try {
    decoded = decodeURIComponent(url.pathname);
  } catch {
    need(false);
  }
  need(
    decoded === url.pathname &&
      !decoded.includes("..") &&
      !decoded.startsWith("//"),
    "scoped_embed_route_denied",
    403,
  );
  const path = url.pathname;
  if (
    ["GET", "HEAD"].includes(method) &&
    (path === "/embed" ||
      /^\/assets\/[A-Za-z0-9_.-]+\.(?:js|css|woff2|svg|png)$/u.test(path))
  )
    return "public-ui";
  if (["GET", "POST"].includes(method) && path === "/api/embed/login") return "auth-start";
  if (method === 'GET' && path === '/api/embed/session-status' && !url.search) return 'auth-read';
  if (method === 'POST' && path === '/api/embed/session-continue' && !url.search) return 'auth-continue';
  if (method === "GET" && path === "/api/embed/callback")
    return "auth-callback";
  if (method === "GET" && path === "/api/embed/complete-link")
    return "auth-completion";
  if (
    method === "GET" &&
    [
      "/api/embed/state",
      "/api/embed/search",
      "/api/embed/history",
      "/api/embed/audit",
    ].includes(path)
  )
    return "read";
  if (
    ["POST", "PATCH", "DELETE"].includes(method) &&
    /^\/api\/embed\/entities(?:\/[A-Za-z0-9_.:-]{1,180})?$/u.test(path)
  )
    return "write";
  if (
    (method === "POST" && path === "/api/embed/files") ||
    (["GET", "HEAD", "DELETE"].includes(method) &&
      /^\/api\/(?:embed\/)?files\/[A-Za-z0-9_.:-]{1,180}$/u.test(path))
  )
    return method === "GET" || method === "HEAD" ? "file-read" : "file-write";
  need(false, "scoped_embed_route_denied", 403);
}

export const SCOPED_EMBED_LIMITS = Object.freeze({
  requestBytes: 1048576,
  responseBytes: 4194304,
  headerBytes: 16384,
  continuations: 256,
  continuationMs: 300000,
  proofMs: 10000,
  inflight: 4,
  callMs: 8000,
});
