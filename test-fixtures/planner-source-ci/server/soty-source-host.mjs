// modules/apps/scoped-embed/source-proof.mjs
import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";

// modules/apps/scoped-embed/profile.mjs
import { createHash } from "node:crypto";
var SCOPED_EMBED_PROFILE = "soty.selected-human-embed.v1";
var SOURCE_PROOF_PROTOCOL = "soty.selected-source-request.v1";
var ScopedEmbedError = class extends Error {
  constructor(code, status = 400) {
    super(code);
    this.code = code;
    this.status = status;
  }
};
function need(ok, code = "scoped_embed_invalid", status = 400) {
  if (!ok) throw new ScopedEmbedError(code, status);
}
function capture(input, depth = 0) {
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
    input && typeof input === "object" && Object.getOwnPropertySymbols(input).length === 0
  );
  const descriptors = Object.getOwnPropertyDescriptors(input);
  if (Array.isArray(input)) {
    need(
      input.length <= 64 && Object.keys(descriptors).length === input.length + 1
    );
    return Object.freeze(
      Array.from({ length: input.length }, (_, i) => {
        need(descriptors[i] && Object.hasOwn(descriptors[i], "value"));
        return capture(descriptors[i].value, depth + 1);
      })
    );
  }
  need([Object.prototype, null].includes(Object.getPrototypeOf(input)));
  const value = {};
  for (const [key, p] of Object.entries(descriptors)) {
    need(
      key.length <= 128 && !["__proto__", "constructor", "prototype"].includes(key) && p.enumerable && Object.hasOwn(p, "value")
    );
    value[key] = capture(p.value, depth + 1);
  }
  return Object.freeze(value);
}
function closed(value, required, optional = []) {
  need(
    value && typeof value === "object" && !Array.isArray(value) && required.every((key) => Object.hasOwn(value, key)) && Object.keys(value).every(
      (key) => required.includes(key) || optional.includes(key)
    )
  );
  return value;
}
function identifier(value) {
  need(
    typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,179}$/u.test(value) && !value.includes("..")
  );
  return value;
}
function sha(value) {
  need(typeof value === "string" && /^[a-f0-9]{64}$/u.test(value));
  return value;
}
function canonical(value) {
  if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
  if (value && typeof value === "object")
    return "{" + Object.keys(value).sort().map((k) => JSON.stringify(k) + ":" + canonical(value[k])).join(",") + "}";
  return JSON.stringify(value);
}
var hash = (value) => createHash("sha256").update(typeof value === "string" ? value : canonical(value)).digest("hex");
function pin(value) {
  closed(value, ["id", "version", "digest"]);
  identifier(value.id);
  need(Number.isSafeInteger(value.version) && value.version > 0);
  sha(value.digest);
  return value;
}
function continuation(value) {
  closed(value, ["id", "version", "digest"]);
  need(/^[A-Za-z0-9_-]{43}$/u.test(value.id) && value.version === 1);
  sha(value.digest);
  return value;
}
function connector(value) {
  closed(value, ["linkId", "hostDeviceId", "connectorId"]);
  Object.values(value).forEach(identifier);
  return value;
}
function approvedOrigin(value) {
  const url = new URL(value);
  need(
    value === url.origin && !url.username && !url.password && (url.protocol === "https:" || url.protocol === "http:" && ["127.0.0.1", "localhost", "[::1]"].includes(url.hostname) && Number(url.port) >= 1024),
    "scoped_embed_origin_invalid"
  );
  return value;
}
function scopedEmbedProfile(input) {
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
    "parentOrigin"
  ]);
  need(
    value.schema === SCOPED_EMBED_PROFILE && /^app-[a-f0-9]{32}$/u.test(value.appId)
  );
  connector(value.connector);
  closed(value.target, ["revision", "digest"]);
  need(
    Number.isSafeInteger(value.target.revision) && value.target.revision > 0
  );
  sha(value.target.digest);
  pin(value.sourceProfile);
  closed(value.resource, [
    "registryId",
    "tenantId",
    "environmentId",
    "appId",
    "resourceId",
    "workspaceId"
  ]);
  Object.values(value.resource).forEach(identifier);
  need(value.resource.appId === value.appId);
  approvedOrigin(value.embedOrigin);
  approvedOrigin(value.nativeOrigin);
  approvedOrigin(value.parentOrigin);
  need(
    new URL(value.nativeOrigin).hostname !== new URL(value.embedOrigin).hostname && value.parentOrigin !== value.embedOrigin
  );
  const issuer = new URL(value.issuer);
  need(
    issuer.href === value.issuer && issuer.pathname === "/human-identity" && issuer.origin === value.parentOrigin && !issuer.search && !issuer.hash
  );
  identifier(value.clientId);
  return Object.freeze({ ...value, digest: hash(value) });
}
function sourceConsentDigest(input) {
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
    parentOrigin: profile.parentOrigin
  });
}
function scopedRoute(method, input) {
  need(
    typeof input === "string" && input.length <= 8192 && input.startsWith("/") && !input.startsWith("//") && !/[\\\s\u0000-\u001f\u007f]/u.test(input)
  );
  const url = new URL(input, "https://fixed.invalid");
  need(
    url.pathname + url.search === input && !url.hash,
    "scoped_embed_route_denied",
    403
  );
  let decoded;
  try {
    decoded = decodeURIComponent(url.pathname);
  } catch {
    need(false);
  }
  need(
    decoded === url.pathname && !decoded.includes("..") && !decoded.startsWith("//"),
    "scoped_embed_route_denied",
    403
  );
  const path = url.pathname;
  if (["GET", "HEAD"].includes(method) && (path === "/embed" || /^\/assets\/[A-Za-z0-9_.-]+\.(?:js|css|woff2|svg|png)$/u.test(path)))
    return "public-ui";
  if (["GET", "POST"].includes(method) && path === "/api/embed/login") return "auth-start";
  if (method === "GET" && path === "/api/embed/session-status" && !url.search) return "auth-read";
  if (method === "POST" && path === "/api/embed/session-continue" && !url.search) return "auth-continue";
  if (method === "GET" && path === "/api/embed/callback")
    return "auth-callback";
  if (method === "GET" && path === "/api/embed/complete-link")
    return "auth-completion";
  if (method === "GET" && [
    "/api/embed/state",
    "/api/embed/search",
    "/api/embed/history",
    "/api/embed/audit"
  ].includes(path))
    return "read";
  if (["POST", "PATCH", "DELETE"].includes(method) && /^\/api\/embed\/entities(?:\/[A-Za-z0-9_.:-]{1,180})?$/u.test(path))
    return "write";
  if (method === "POST" && path === "/api/embed/files" || ["GET", "HEAD", "DELETE"].includes(method) && /^\/api\/(?:embed\/)?files\/[A-Za-z0-9_.:-]{1,180}$/u.test(path))
    return method === "GET" || method === "HEAD" ? "file-read" : "file-write";
  need(false, "scoped_embed_route_denied", 403);
}
var SCOPED_EMBED_LIMITS = Object.freeze({
  requestBytes: 1048576,
  responseBytes: 4194304,
  headerBytes: 16384,
  continuations: 256,
  continuationMs: 3e5,
  proofMs: 1e4,
  inflight: 4,
  callMs: 8e3
});

// modules/apps/scoped-embed/source-proof.mjs
var SOURCE_PROOF_HEADER = "x-soty-selected-proof";
var SOURCE_MAC_HEADER = "x-soty-selected-mac";
var SOURCE_PROBE_HEADER = "x-soty-selected-probe";
var SOURCE_READY_HEADER = "x-soty-selected-ready";
var token = (value) => need(
  typeof value === "string" && /^[A-Za-z0-9_-]{43}$/u.test(value),
  "scoped_embed_proof_invalid",
  403
);
function hostKey(key) {
  need(Buffer.isBuffer(key) && key.length === 32, "scoped_embed_key_required");
  return Buffer.from(key);
}
function mac(key, text) {
  return createHmac("sha256", key).update(text).digest("base64url");
}
function wireContext(input, profile) {
  const context = capture(input);
  closed(context, [
    "schema",
    "reference",
    "profileDigest",
    "appId",
    "sourceProfile",
    "resource",
    "rootPrincipal",
    "humanPrincipal",
    "entry",
    "target",
    "policyEpoch",
    "expiresAt"
  ]);
  closed(context.rootPrincipal, ["accountId", "deviceId"]);
  Object.values(context.rootPrincipal).forEach(identifier);
  closed(context.humanPrincipal, ["issuer", "subject", "clientId", "clientProfileDigest", "clientGeneration"]);
  need(
    context.humanPrincipal.issuer === profile.issuer && context.humanPrincipal.clientId === profile.clientId && typeof context.humanPrincipal.subject === "string" && context.humanPrincipal.subject.length > 0 && context.humanPrincipal.subject.length <= 128 && /^[a-f0-9]{64}$/.test(context.humanPrincipal.clientProfileDigest) && Number.isSafeInteger(context.humanPrincipal.clientGeneration) && context.humanPrincipal.clientGeneration > 0,
    "scoped_embed_human_principal_invalid",
    403
  );
  closed(context.entry, ["domainId", "origin"]);
  identifier(context.entry.domainId);
  need(
    Number.isSafeInteger(context.policyEpoch) && context.policyEpoch > 0 && Number.isSafeInteger(context.expiresAt) && context.expiresAt > 0
  );
  continuation(context.reference);
  need(
    context.schema === "soty.verified-launch-continuation.v1" && context.profileDigest === profile.digest && context.appId === profile.appId && hash(context.sourceProfile) === hash(profile.sourceProfile) && hash(context.resource) === hash(profile.resource) && hash(context.target) === hash(profile.target) && context.entry.origin === profile.embedOrigin,
    "scoped_embed_context_mismatch",
    403
  );
  return context;
}
function createSourceProofVerifier({
  profile: raw,
  key,
  consumeNonce,
  clock = Date.now
} = {}) {
  const profile = scopedEmbedProfile(raw), secret = hostKey(key);
  need(typeof consumeNonce === "function", "scoped_embed_nonce_store_required");
  const seen = /* @__PURE__ */ new WeakMap();
  return Object.freeze({
    verifyReady(request) {
      need(
        request.method === "HEAD" && request.url === "/api/embed/transport-ready" && request.headers.host === new URL(profile.embedOrigin).host,
        "scoped_embed_probe_invalid",
        403
      );
      const text = request.headers[SOURCE_PROBE_HEADER], signature = request.headers[SOURCE_MAC_HEADER];
      token(signature);
      need(typeof text === "string" && text.length <= 2048 && /^[A-Za-z0-9_-]+$/.test(text), "scoped_embed_probe_invalid", 403);
      need(timingSafeEqual(Buffer.from(signature), Buffer.from(mac(secret, "probe\0" + text))), "scoped_embed_probe_invalid", 403);
      const value = capture(JSON.parse(Buffer.from(text, "base64url").toString("utf8")));
      closed(value, ["schema", "profileDigest", "nonce", "expiresAt"]);
      token(value.nonce);
      need(
        value.schema === "soty.selected-source-probe.v1" && value.profileDigest === profile.digest && Number.isSafeInteger(value.expiresAt) && value.expiresAt > clock() && value.expiresAt <= clock() + SCOPED_EMBED_LIMITS.proofMs && consumeNonce(value.nonce, value.expiresAt) === true,
        "scoped_embed_probe_invalid",
        403
      );
      return Object.freeze({ [SOURCE_READY_HEADER]: text, [SOURCE_MAC_HEADER]: mac(secret, "ready\0" + text) });
    },
    verify(request, { body = Buffer.alloc(0) } = {}) {
      need(!seen.has(request), "scoped_embed_proof_replayed", 403);
      need(
        request.headers.host === new URL(profile.embedOrigin).host,
        "scoped_embed_origin_invalid",
        403
      );
      const text = request.headers[SOURCE_PROOF_HEADER], signature = request.headers[SOURCE_MAC_HEADER];
      token(signature);
      for (const name of [SOURCE_PROOF_HEADER, SOURCE_MAC_HEADER]) {
        const count = (request.rawHeaders ?? []).filter(
          (_, i) => i % 2 === 0 && (request.rawHeaders[i] ?? "").toLowerCase() === name
        ).length;
        need(count <= 1, "scoped_embed_proof_invalid", 403);
      }
      need(
        typeof text === "string" && text.length <= 8192 && /^[A-Za-z0-9_-]+$/u.test(text),
        "scoped_embed_proof_invalid",
        403
      );
      need(
        timingSafeEqual(Buffer.from(signature), Buffer.from(mac(secret, text))),
        "scoped_embed_proof_invalid",
        403
      );
      let parsed;
      try {
        parsed = JSON.parse(Buffer.from(text, "base64url").toString("utf8"));
      } catch {
        need(false, "scoped_embed_proof_invalid", 403);
      }
      const value = capture(parsed);
      closed(value, [
        "schema",
        "context",
        "method",
        "path",
        "bodyDigest",
        "cookieDigest",
        "nonce",
        "expiresAt"
      ]);
      token(value.nonce);
      const context = wireContext(value.context, profile);
      need(
        value.schema === SOURCE_PROOF_PROTOCOL && value.method === request.method && value.path === request.url && Number.isSafeInteger(value.expiresAt) && value.expiresAt > clock() && value.expiresAt <= clock() + SCOPED_EMBED_LIMITS.proofMs && context.expiresAt >= value.expiresAt,
        "scoped_embed_proof_expired",
        401
      );
      scopedRoute(value.method, value.path);
      need(
        Buffer.isBuffer(body) && body.length <= SCOPED_EMBED_LIMITS.requestBytes && value.bodyDigest === hash(body.toString("base64")) && value.cookieDigest === hash(request.headers.cookie ?? ""),
        "scoped_embed_proof_mismatch",
        403
      );
      need(
        consumeNonce(value.nonce, value.expiresAt) === true,
        "scoped_embed_proof_replayed",
        403
      );
      seen.set(request, context);
      return context;
    },
    context(request) {
      const value = seen.get(request);
      need(value, "scoped_embed_transport_required", 401);
      return value;
    }
  });
}

// modules/apps/scoped-embed/local-broker.mjs
function createSourceCurrentSubjectPort({
  profile: raw,
  verifier,
  readAuthority,
  verifyHuman
} = {}) {
  const profile = scopedEmbedProfile(raw);
  need(
    verifier && typeof verifier.context === "function" && [readAuthority, verifyHuman].every((fn) => typeof fn === "function")
  );
  return async function currentSotySubject(request, expected) {
    need(
      expected && expected.proof && expected.continuation,
      "scoped_embed_human_proof_required",
      401
    );
    continuation(expected.continuation);
    const native = request.headers.host === new URL(profile.nativeOrigin).host;
    if (native)
      need(
        ["/soty/connect", "/soty/disconnect", "/soty/access"].includes(
          new URL(request.url, profile.nativeOrigin).pathname
        ),
        "scoped_embed_route_denied",
        403
      );
    else {
      const incoming = verifier.context(request);
      need(
        hash(incoming.reference) === hash(expected.continuation),
        "scoped_embed_profile_changed",
        401
      );
    }
    const authority = await readAuthority({
      reference: expected.continuation,
      connector: profile.connector
    });
    need(
      authority.profileDigest === profile.digest,
      "scoped_embed_context_mismatch",
      403
    );
    const human = await verifyHuman(expected.proof);
    need(
      human && human.issuer === profile.issuer && human.subject === authority.humanPrincipal.subject && human.issuer === authority.humanPrincipal.issuer,
      "scoped_embed_human_mismatch",
      401
    );
    const after = await readAuthority({
      reference: expected.continuation,
      connector: profile.connector
    });
    need(
      hash(after.reference) === hash(authority.reference) && hash(after.rootPrincipal) === hash(authority.rootPrincipal) && hash(after.humanPrincipal) === hash(authority.humanPrincipal),
      "scoped_embed_profile_changed",
      401
    );
    return Object.freeze({ issuer: human.issuer, subject: human.subject });
  };
}

// modules/apps/scoped-embed/source-authority-client.mjs
import { createHmac as createHmac2, timingSafeEqual as timingSafeEqual2, randomBytes as randomBytes2 } from "node:crypto";
import { request as httpRequest } from "node:http";
function createSourceAuthorityClient({ profile: raw, key, connectorPort = 49424, clock = Date.now } = {}) {
  const profile = scopedEmbedProfile(raw);
  need(Buffer.isBuffer(key) && key.length === 32 && Number.isSafeInteger(connectorPort) && connectorPort >= 1024 && connectorPort <= 65535);
  const privateKey = Buffer.from(key), mac2 = (text) => createHmac2("sha256", privateKey).update(text).digest("base64url");
  return async function readAuthority({ reference, connector: connector2 }) {
    need(hash(connector2) === hash(profile.connector), "scoped_embed_connector_mismatch", 403);
    const nonce = randomBytes2(32).toString("base64url"), text = JSON.stringify({
      schema: "soty.selected-source-authority.v1",
      appId: profile.appId,
      profileDigest: profile.digest,
      reference: capture(reference),
      nonce,
      expiresAt: clock() + 1e4
    });
    return new Promise((resolve, reject) => {
      const request = httpRequest({
        hostname: "127.0.0.1",
        port: connectorPort,
        path: "/apps/scoped/authority",
        method: "POST",
        agent: false,
        headers: { "content-type": "application/json", "x-soty-source-mac": mac2("authority\0" + text), "content-length": Buffer.byteLength(text) },
        signal: AbortSignal.timeout(8e3)
      }, (response) => {
        const parts = [];
        let bytes = 0;
        response.on("data", (part) => {
          bytes += part.length;
          if (bytes > 16384) response.destroy();
          else parts.push(part);
        });
        response.on("error", () => reject(Object.assign(new Error("scoped_embed_authority_unavailable"), { status: 503, code: "scoped_embed_authority_unavailable" })));
        response.on("end", () => {
          try {
            const body = Buffer.concat(parts).toString("utf8"), signature = response.headers["x-soty-source-mac"];
            need(
              response.statusCode === 200 && typeof signature === "string" && signature.length === 43 && timingSafeEqual2(Buffer.from(signature), Buffer.from(mac2("authority-result\0" + body))),
              "scoped_embed_authority_denied",
              response.statusCode === 403 ? 403 : 503
            );
            const value = capture(JSON.parse(body));
            need(
              value.nonce === nonce && value.context.profileDigest === profile.digest && hash(value.context.reference) === hash(reference),
              "scoped_embed_authority_mismatch",
              403
            );
            resolve(value.context);
          } catch (error) {
            reject(error);
          }
        });
      });
      request.on("error", () => reject(Object.assign(new Error("scoped_embed_authority_unavailable"), { status: 503, code: "scoped_embed_authority_unavailable" })));
      request.end(text);
    });
  };
}
export {
  SCOPED_EMBED_LIMITS,
  SCOPED_EMBED_PROFILE,
  ScopedEmbedError,
  createSourceAuthorityClient,
  createSourceCurrentSubjectPort,
  createSourceProofVerifier,
  scopedEmbedProfile,
  sourceConsentDigest
};
