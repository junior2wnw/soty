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

// modules/source-app/server/wire.mjs
import { createHash as createHash2, randomBytes as randomBytes2 } from "node:crypto";
function canonical2(input) {
  if (Array.isArray(input)) return "[" + input.map(canonical2).join(",") + "]";
  if (input && typeof input === "object") return "{" + Object.keys(input).sort().map((key) => JSON.stringify(key) + ":" + canonical2(input[key])).join(",") + "}";
  return JSON.stringify(input);
}
var digest = (value) => createHash2("sha256").update(typeof value === "string" ? value : canonical2(value)).digest("hex");
var deepFreeze = (value) => {
  if (value && typeof value === "object") {
    Object.values(value).forEach(deepFreeze);
    Object.freeze(value);
  }
  return value;
};

// modules/source-app/server/standard-profile.mjs
var rule = (kind, methods, path, requestBytes, responseBytes, queries = []) => ({ kind, methods, path, requestBytes, responseBytes, queries });
var STANDARD_SOURCE_CONTRACT = deepFreeze({
  schema: "soty.source-route-adapter.v1",
  kind: "soty.resource.v1",
  auth: {
    schema: "soty.source-embed-auth.v1",
    cookies: ["soty_rp_session", "soty_rp_intent", "soty_rp_link"],
    basicSeconds: 300,
    finiteSeconds: 86400,
    proof: "maintained-native-rp-current-userinfo",
    nativeConsent: "selected-current-native-acl",
    nativeHandoffPath: "/soty/connect",
    nativeCallbackPath: "/soty/callback",
    feedbackPermission: "explicit-current-native-report"
  },
  rules: [
    rule("public-ui", ["GET", "HEAD"], "/embed", 0, 4194304),
    rule("auth-start", ["GET", "POST"], "/api/embed/login", 16384, 65536),
    rule("auth-read", ["GET"], "/api/embed/session-status", 0, 65536),
    rule("auth-continue", ["POST"], "/api/embed/session-continue", 16384, 65536),
    rule("auth-callback", ["GET"], "/api/embed/callback", 0, 65536, ["code", "state", "iss", "error", "error_description"]),
    rule("auth-completion", ["GET"], "/api/embed/complete-link", 0, 65536, ["intent"]),
    rule("read", ["GET"], "/api/embed/context", 0, 65536),
    rule("read", ["POST"], "/api/embed/query", 65536, 65536),
    rule("write", ["POST"], "/api/embed/invoke", 65536, 65536),
    rule("read", ["POST"], "/api/embed/receipt", 16384, 65536),
    rule("feedback-read", ["GET"], "/api/embed/feedback/context", 0, 65536),
    rule("feedback-read", ["GET"], "/api/embed/feedback", 0, 65536, ["limit", "cursor"]),
    rule("feedback-write", ["POST"], "/api/embed/feedback", 15e5, 262144),
    rule("feedback-read", ["GET"], "/api/embed/feedback/ticket", 0, 15e5, ["ticketId"]),
    rule("feedback-write", ["POST"], "/api/embed/feedback/reply", 32768, 262144),
    rule("feedback-write", ["POST"], "/api/embed/feedback/status", 32768, 262144),
    rule("feedback-write", ["POST"], "/api/embed/feedback/accept", 32768, 262144)
  ],
  assets: { path: "/assets/", extensions: ["js", "css", "woff2", "svg", "png"], responseBytes: 4194304 }
});
var STANDARD_SELECTED_SOURCE = deepFreeze({ id: "soty.standard-resource", version: 1, digest: digest(STANDARD_SOURCE_CONTRACT) });
var { nativeCallbackPath: _nativePath, ...authV2 } = STANDARD_SOURCE_CONTRACT.auth;
var STANDARD_SOURCE_CONTRACT_2 = deepFreeze({
  ...STANDARD_SOURCE_CONTRACT,
  auth: {
    ...authV2,
    callbackPath: "/api/embed/callback",
    callbackOrigin: "approved-embed-https",
    loginProof: "durable-source-native-before-oidc.v1",
    correlation: "handoff-state-original-slot.v1",
    completionRecovery: "current-original-slot-readonly-receipt.v1"
  }
});
var STANDARD_SELECTED_SOURCE_V2 = deepFreeze({ id: "soty.standard-resource", version: 2, digest: digest(STANDARD_SOURCE_CONTRACT_2) });
var STANDARD_SOURCE_CONTRACT_3 = deepFreeze({
  ...STANDARD_SOURCE_CONTRACT_2,
  rules: STANDARD_SOURCE_CONTRACT_2.rules.map((row) => row.path === "/api/embed/feedback/ticket" ? { ...row, responseBytes: 2097152 } : row)
});
var STANDARD_SELECTED_SOURCE_V3 = deepFreeze({ id: "soty.standard-resource", version: 3, digest: digest(STANDARD_SOURCE_CONTRACT_3) });
var STANDARD_WORKSPACE_CONTRACT = deepFreeze({
  ...STANDARD_SOURCE_CONTRACT_3,
  kind: "soty.workspace.v1",
  context: {
    schema: "soty.current-context.v2",
    authority: "application-namespace-and-current-child-native-acl",
    readOperation: "soty.current_context",
    uiOperation: "soty.context.select",
    maxLeaseMs: 3e5,
    slotCapacity: "host-configured",
    instanceBinding: "optional-root-account-device-app-profile-main-or-widget",
    receipt: "soty.context-after.v1",
    projection: "soty.workspace-receipt.v1",
    witness: "soty.workspace-context-witness.v1"
  }
});
var STANDARD_WORKSPACE_SOURCE = deepFreeze({ id: "soty.standard-workspace", version: 1, digest: digest(STANDARD_WORKSPACE_CONTRACT) });
var STANDARD_ROOT_WORKSPACE_CONTRACT = deepFreeze({
  ...STANDARD_WORKSPACE_CONTRACT,
  // Feedback names the SAME current Native child as domain reads/effects.
  // This new workspace2 pin does not widen resource1/2/3 or workspace1.
  rules: STANDARD_WORKSPACE_CONTRACT.rules.map((row) => row.kind === "feedback-read" ? {
    ...row,
    queries: [...row.queries, "contextRef"],
    ...row.path === "/api/embed/feedback" ? { responseBytes: 262144 } : {}
  } : row),
  auth: {
    schema: "soty.root-workspace-auth.v1",
    cookies: ["soty_rp_session"],
    basicSeconds: 300,
    proof: "root-signed-current-context.v1",
    audience: "urn:soty:source-authority",
    witnessType: "soty-source-authority+jwt",
    witnessMaxSeconds: 10,
    session: "fresh-reference-short-native-anchor",
    feedbackPermission: "explicit-current-native-report"
  }
});
var STANDARD_ROOT_WORKSPACE_SOURCE = deepFreeze({ id: "soty.standard-workspace", version: 2, digest: digest(STANDARD_ROOT_WORKSPACE_CONTRACT) });
var isRootSignedWorkspaceSource = (pin2) => pin2?.id === STANDARD_ROOT_WORKSPACE_SOURCE.id && pin2.version === 2 && pin2.digest === STANDARD_ROOT_WORKSPACE_SOURCE.digest;

// modules/apps/scoped-embed/resource-profile.mjs
var SELECTED_RESOURCE_PROFILE = "soty.selected-human-embed.v2";
var validatedProfiles = /* @__PURE__ */ new WeakSet();
function nativeResourceId(value) {
  need(
    typeof value === "string" && value.length > 0 && value.isWellFormed() && Buffer.byteLength(value, "utf8") <= 4096 && !/[\u0000-\u001f\u007f-\u009f]/u.test(value),
    "scoped_embed_native_id_invalid"
  );
  return value;
}
function resourceKind(value) {
  need(typeof value === "string" && /^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*\.v[1-9][0-9]*$/u.test(value) && value.length <= 128, "scoped_embed_resource_kind_invalid");
  return value;
}
function selectedResource(value) {
  closed(value, ["registryId", "tenantId", "appId", "environmentId", "resourceId", "selection"]);
  for (const field of ["registryId", "tenantId", "appId", "environmentId", "resourceId"]) identifier(value[field]);
  closed(value.selection, ["kind", "nativeId", "incarnationId"]);
  resourceKind(value.selection.kind);
  nativeResourceId(value.selection.nativeId);
  identifier(value.selection.incarnationId);
  return value;
}
function selectedResourceProfile(input) {
  if (input && typeof input === "object" && validatedProfiles.has(input)) return input;
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
  need(value.schema === SELECTED_RESOURCE_PROFILE && /^app-[a-f0-9]{32}$/u.test(value.appId));
  connector(value.connector);
  closed(value.target, ["revision", "digest"]);
  need(Number.isSafeInteger(value.target.revision) && value.target.revision > 0);
  sha(value.target.digest);
  pin(value.sourceProfile);
  selectedResource(value.resource);
  need(value.resource.appId === value.appId);
  approvedOrigin(value.embedOrigin);
  approvedOrigin(value.nativeOrigin);
  approvedOrigin(value.parentOrigin);
  const sameOriginWorkspace = isRootSignedWorkspaceSource(value.sourceProfile) && value.resource.selection.kind === "soty.workspace.v1" && value.nativeOrigin === value.embedOrigin;
  need((new URL(value.nativeOrigin).hostname !== new URL(value.embedOrigin).hostname || sameOriginWorkspace) && value.parentOrigin !== value.embedOrigin);
  const issuer = new URL(value.issuer);
  need(issuer.href === value.issuer && issuer.pathname === "/human-identity" && issuer.origin === value.parentOrigin && !issuer.search && !issuer.hash);
  identifier(value.clientId);
  need(Buffer.byteLength(JSON.stringify(value), "utf8") <= 16384, "scoped_embed_profile_too_large");
  const profile = Object.freeze({ ...value, digest: hash(value) });
  validatedProfiles.add(profile);
  return profile;
}
var RESOURCE_TRANSPORT_LIMITS = Object.freeze({
  proofHeaderBytes: 12288,
  headerBytes: 16384,
  continuations: 256,
  continuationMs: 3e5,
  proofMs: 1e4,
  inflight: 4,
  callMs: 8e3
});

// modules/apps/scoped-embed/resource-route-adapters.mjs
var MiB = 1048576;
var rule2 = (kind, methods, path, requestBytes, responseBytes, queries = []) => Object.freeze({ kind, methods: Object.freeze(methods), path, requestBytes, responseBytes, queries: Object.freeze(queries) });
var hiveContract = capture({
  schema: "soty.source-route-adapter.v1",
  kind: "hive.project.v1",
  auth: {
    schema: "soty.source-embed-auth.v1",
    cookies: ["soty_rp_session", "soty_rp_intent", "soty_rp_link"],
    basicSeconds: 300,
    finiteSeconds: 86400,
    proof: "maintained-native-rp-current-userinfo",
    nativeConsent: "selected-current-native-acl",
    nativeHandoffPath: "/soty/connect",
    nativeCallbackPath: "/account/soty/callback",
    feedbackPermission: "explicit-current-native-report"
  },
  rules: [
    rule2("public-ui", ["GET", "HEAD"], "/embed", 0, 4 * MiB),
    rule2("auth-start", ["GET", "POST"], "/api/embed/login", 16384, 65536),
    rule2("auth-read", ["GET"], "/api/embed/session-status", 0, 65536),
    rule2("auth-continue", ["POST"], "/api/embed/session-continue", 16384, 65536),
    rule2("auth-callback", ["GET"], "/api/embed/callback", 0, 65536, ["code", "state", "iss", "error", "error_description"]),
    rule2("auth-completion", ["GET"], "/api/embed/complete-link", 0, 65536, ["intent"]),
    rule2("read", ["GET"], "/api/embed/context", 0, 65536),
    rule2("read", ["GET"], "/api/embed/project", 0, 4 * MiB),
    rule2("write", ["PUT"], "/api/embed/project", MiB, 4 * MiB),
    rule2("write", ["POST"], "/api/embed/operations", MiB, 4 * MiB),
    rule2("read", ["GET"], "/api/embed/changes", 0, MiB, ["after", "limit"]),
    rule2("read", ["GET"], "/api/embed/live", 0, 65536, ["clientId"]),
    rule2("read", ["GET"], "/api/embed/presence", 0, 65536, ["clientId"]),
    rule2("presence", ["POST", "DELETE"], "/api/embed/presence", 16384, 65536),
    rule2("feedback-read", ["GET"], "/api/embed/feedback/context", 0, 65536),
    rule2("feedback-read", ["GET"], "/api/embed/feedback", 0, 65536, ["limit", "cursor"]),
    rule2("feedback-write", ["POST"], "/api/embed/feedback", 15e5, 262144),
    rule2("feedback-read", ["GET"], "/api/embed/feedback/ticket", 0, 2 * MiB, ["ticketId"]),
    rule2("feedback-write", ["POST"], "/api/embed/feedback/reply", 32768, 262144),
    rule2("feedback-write", ["POST"], "/api/embed/feedback/status", 32768, 262144),
    rule2("feedback-write", ["POST"], "/api/embed/feedback/accept", 32768, 262144)
  ],
  assets: { path: "/assets/", extensions: ["js", "css", "woff2", "svg", "png"], responseBytes: 4 * MiB }
});
var HIVE_SELECTED_KERNEL_SOURCE = Object.freeze({ id: "hive.selected-project", version: 1, digest: hash(hiveContract) });
var editorContract = capture({ ...hiveContract, assets: {
  responseBytes: 4 * MiB,
  credentialFree: true,
  redirects: false,
  queries: false,
  paths: [
    { prefix: "/_next/static/chunks/", extensions: ["js"] },
    { prefix: "/_next/static/css/", extensions: ["css"] },
    { prefix: "/_next/static/media/", extensions: ["woff2", "svg", "png"] }
  ]
} });
var HIVE_SELECTED_SOURCE = Object.freeze({ id: "hive.selected-project", version: 2, digest: hash(editorContract) });
var hive = Object.freeze({ pin: HIVE_SELECTED_KERNEL_SOURCE, kind: hiveContract.kind, contract: hiveContract });
var editor = Object.freeze({ pin: HIVE_SELECTED_SOURCE, kind: editorContract.kind, contract: editorContract });
var adapters = new Map([hive, editor].map((adapter) => [adapter.pin.id + ":" + adapter.pin.version + ":" + adapter.pin.digest, adapter]));
adapters.set(
  STANDARD_SELECTED_SOURCE.id + ":1:" + STANDARD_SELECTED_SOURCE.digest,
  Object.freeze({ pin: STANDARD_SELECTED_SOURCE, kind: STANDARD_SOURCE_CONTRACT.kind, contract: STANDARD_SOURCE_CONTRACT })
);
adapters.set(
  STANDARD_SELECTED_SOURCE_V2.id + ":2:" + STANDARD_SELECTED_SOURCE_V2.digest,
  Object.freeze({ pin: STANDARD_SELECTED_SOURCE_V2, kind: STANDARD_SOURCE_CONTRACT_2.kind, contract: STANDARD_SOURCE_CONTRACT_2 })
);
adapters.set(
  STANDARD_SELECTED_SOURCE_V3.id + ":3:" + STANDARD_SELECTED_SOURCE_V3.digest,
  Object.freeze({ pin: STANDARD_SELECTED_SOURCE_V3, kind: STANDARD_SOURCE_CONTRACT_3.kind, contract: STANDARD_SOURCE_CONTRACT_3 })
);
adapters.set(
  STANDARD_WORKSPACE_SOURCE.id + ":1:" + STANDARD_WORKSPACE_SOURCE.digest,
  Object.freeze({ pin: STANDARD_WORKSPACE_SOURCE, kind: STANDARD_WORKSPACE_CONTRACT.kind, contract: STANDARD_WORKSPACE_CONTRACT })
);
adapters.set(
  STANDARD_ROOT_WORKSPACE_SOURCE.id + ":2:" + STANDARD_ROOT_WORKSPACE_SOURCE.digest,
  Object.freeze({ pin: STANDARD_ROOT_WORKSPACE_SOURCE, kind: STANDARD_ROOT_WORKSPACE_CONTRACT.kind, contract: STANDARD_ROOT_WORKSPACE_CONTRACT })
);
function selectedRouteAdapter(input) {
  const profile = selectedResourceProfile(input);
  const pin2 = profile.sourceProfile, adapter = adapters.get(pin2.id + ":" + pin2.version + ":" + pin2.digest);
  need(adapter && adapter.kind === profile.resource.selection.kind, "scoped_embed_adapter_unapproved", 403);
  return adapter;
}

// modules/apps/scoped-embed/profile-dispatch.mjs
function approvedEmbedProfile(input) {
  return input?.schema === SELECTED_RESOURCE_PROFILE ? selectedResourceProfile(input) : scopedEmbedProfile(input);
}
function requireEmbedAdapter(input) {
  const profile = approvedEmbedProfile(input);
  if (profile.schema === SELECTED_RESOURCE_PROFILE) selectedRouteAdapter(input);
  return profile;
}

// modules/apps/scoped-embed/local-broker.mjs
var PUBLIC_ASSET_LIMITS = Object.freeze({ active: 4, queued: 32, waitMs: 8e3 });
function createSourceCurrentSubjectPort({
  profile: raw,
  verifier,
  readAuthority,
  verifyHuman
} = {}) {
  const profile = approvedEmbedProfile(raw);
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
import { createHmac as createHmac2, timingSafeEqual as timingSafeEqual2, randomBytes as randomBytes3 } from "node:crypto";
import { request as httpRequest } from "node:http";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
var verifiedRoot = /* @__PURE__ */ new WeakMap();
var jose;
var maintainedJose = () => jose ??= import(
  /* @vite-ignore */
  pathToFileURL(createRequire(createRequire(import.meta.url).resolve("openid-client")).resolve("jose")).href
);
var AUTHORITY_ISSUANCE_CLOCK_SKEW_MS = 1e3;
function createSourceAuthorityClient({ profile: raw, key, connectorPort = 49424, clock = Date.now } = {}) {
  const profile = requireEmbedAdapter(raw);
  need(Buffer.isBuffer(key) && key.length === 32 && Number.isSafeInteger(connectorPort) && connectorPort >= 1024 && connectorPort <= 65535);
  const privateKey = Buffer.from(key), mac2 = (text) => createHmac2("sha256", privateKey).update(text).digest("base64url");
  const rootSigned = isRootSignedWorkspaceSource(profile.sourceProfile);
  let jwks;
  async function verifyIdentityWitness(value, challenge) {
    const { jwtVerify, createRemoteJWKSet, customFetch } = await maintainedJose();
    need(typeof value.identityWitness === "string" && value.identityWitness.length <= 8192, "source_root_witness_invalid", 403);
    jwks ??= createRemoteJWKSet(new URL(profile.issuer + "/jwks"), { timeoutDuration: 5e3, [customFetch]: async (url, init) => {
      need(String(url) === profile.issuer + "/jwks", "source_root_witness_invalid", 403);
      const response = await fetch(url, { ...init, redirect: "error", signal: AbortSignal.timeout(5e3) });
      need(response.status === 200 && response.body, "source_root_witness_unavailable", 503);
      const parts = [];
      let size = 0;
      for await (const part of response.body) {
        size += part.length;
        need(size <= 65536, "source_root_witness_unavailable", 503);
        parts.push(part);
      }
      return new Response(Buffer.concat(parts), { status: 200, headers: { "content-type": "application/json" } });
    } });
    try {
      const { payload, protectedHeader } = await jwtVerify(value.identityWitness, jwks, {
        algorithms: ["RS256"],
        issuer: profile.issuer,
        audience: "urn:soty:source-authority",
        typ: "soty-source-authority+jwt",
        currentDate: new Date(clock()),
        clockTolerance: 0
      });
      need(Object.keys(protectedHeader).every((key2) => ["alg", "typ", "kid"].includes(key2)) && Object.keys(payload).sort().join(",") === "aud,contextDigest,exp,iat,iss,sourceChallenge,sub" && payload.aud === "urn:soty:source-authority" && payload.sub === value.context.humanPrincipal.subject && payload.sourceChallenge === challenge && payload.contextDigest === hash(value.context) && Number.isSafeInteger(payload.iat) && Number.isSafeInteger(payload.exp) && payload.exp > payload.iat && payload.exp - payload.iat <= 10 && payload.iat * 1e3 <= clock() + 1e3 && payload.exp * 1e3 > clock() && payload.exp * 1e3 <= value.context.expiresAt, "source_root_witness_invalid", 403);
      verifiedRoot.set(value.context, { expiresAt: payload.exp * 1e3, clock });
      return value.context;
    } catch {
      need(false, "source_root_witness_invalid", 403);
    }
  }
  return async function readAuthority({ reference, connector: connector2 }) {
    need(hash(connector2) === hash(profile.connector), "scoped_embed_connector_mismatch", 403);
    const nonce = randomBytes3(32).toString("base64url"), text = JSON.stringify({
      schema: "soty.selected-source-authority.v1",
      appId: profile.appId,
      profileDigest: profile.digest,
      reference: capture(reference),
      nonce,
      expiresAt: clock() + 1e4 - AUTHORITY_ISSUANCE_CLOCK_SKEW_MS
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
        response.on("end", async () => {
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
            resolve(rootSigned ? await verifyIdentityWitness(value, nonce) : value.context);
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
