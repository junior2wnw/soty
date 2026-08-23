import { createHash, createPublicKey, randomUUID, verify as verifySignature } from "node:crypto";
import { chmod, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";

export const sotyIdentityAdapterSchema = "soty.identity-adapter.experimental.v1";
export const sotyIdentityLinkExchangeSchema = "soty.identity-adapter.link-exchange.experimental.v1";
export const sotyIdentityEventSchema = "soty.identity-adapter.event.experimental.v1";
export const sotyIdentityLinkExchangeType = "soty-identity-link-experimental+jwt";
export const sotyIdentityEventType = "soty-identity-event-experimental+jwt";

export const sotyIdentityEventTypes = Object.freeze([
  "identity.membership.upserted",
  "identity.membership.revoked",
  "identity.device.enrolled",
  "identity.device.revoked",
  "identity.device.recovery_started",
  "identity.device.recovered",
  "identity.link.unlinked"
]);

const stateSchema = "soty.identity-adapter-store.experimental.v1";
const defaultMaxTokenTtlSec = 60;
const defaultClockSkewSec = 30;
const defaultReplayRetentionMs = 24 * 60 * 60_000;
const maxArtifactChars = 32_000;
const maxListItems = 32;
const opaqueIdPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]{2,199}$/u;
const idempotencyPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]{15,127}$/u;
const allowedRoles = new Set(["member", "viewer", "project_admin"]);
const allowedLinkScopes = new Set([
  "identity:link",
  "identity:unlink",
  "membership:read",
  "device:enroll",
  "device:revoke",
  "device:recover"
]);
const forbiddenPiiNames = new Set([
  "address",
  "birthdate",
  "email",
  "email_verified",
  "family_name",
  "given_name",
  "locale",
  "name",
  "phone",
  "phone_number",
  "phone_number_verified",
  "picture",
  "preferred_username",
  "profile"
]);
const forbiddenSecretNamePattern = /(?:^|_)(?:api_token|authorization|cookie|credential|password|refresh_token|secret|session|token)(?:_|$)/u;

export class SotyIdentityAdapterError extends Error {
  constructor(code, status = 400) {
    super(code);
    this.name = "SotyIdentityAdapterError";
    this.code = code;
    this.status = status;
  }
}

const IdentityCompatError = SotyIdentityAdapterError;

export function createSotyIdentityAdapterService(options) {
  return new SotyIdentityAdapterService(options);
}

export function readSotyIdentityAdapterConfig(env = process.env) {
  const enabled = env.SOTY_IDENTITY_ADAPTER_EXPERIMENTAL_ENABLED === "1";
  if (!enabled) return { enabled: false };
  let projectId;
  try { projectId = cleanOpaqueId(env.SOTY_IDENTITY_ADAPTER_PROJECT_ID || "soty.online", "project_id"); }
  catch { throw new SotyIdentityAdapterError("identity_config_invalid", 500); }
  const audience = cleanAudience(env.SOTY_IDENTITY_ADAPTER_AUDIENCE || "urn:soty:identity");
  let issuers;
  try {
    issuers = JSON.parse(env.SOTY_IDENTITY_ADAPTER_ISSUERS_JSON || "[]");
  } catch {
    throw new SotyIdentityAdapterError("identity_config_invalid", 500);
  }
  if (!Array.isArray(issuers) || issuers.length === 0) throw new SotyIdentityAdapterError("identity_config_invalid", 500);
  const runtimeMode = normalizeRuntimeMode(env.SOTY_IDENTITY_ADAPTER_RUNTIME || "production");
  return { enabled: true, projectId, audience, issuers, runtimeMode };
}

class SotyIdentityAdapterService {
  constructor(options = {}) {
    this.filePath = String(options.filePath || "");
    try { this.projectId = cleanOpaqueId(options.projectId, "project_id"); }
    catch { throw new SotyIdentityAdapterError("identity_config_invalid", 500); }
    this.audience = cleanAudience(options.audience);
    this.issuers = normalizeIssuers(options.issuers);
    this.now = typeof options.now === "function" ? options.now : () => Date.now();
    this.maxTokenTtlSec = safeInteger(options.maxTokenTtlSec, 1, 60, defaultMaxTokenTtlSec);
    this.runtimeMode = normalizeRuntimeMode(options.runtimeMode || "development");
    this.clockSkewSec = safeInteger(options.clockSkewSec, 0, 120, defaultClockSkewSec);
    this.replayRetentionMs = safeInteger(options.replayRetentionMs, 60_000, 7 * 24 * 60 * 60_000, defaultReplayRetentionMs);
    this.state = emptyState();
    this.writeQueue = this.load();
  }

  capabilities() {
    return {
      ok: true,
      schema: sotyIdentityAdapterSchema,
      status: "experimental",
      role: "soty-relying-party-adapter",
      canonical_shared_contract: false,
      canonical_contract_authority: "external-ufa-repository-stable",
      project_id: this.projectId,
      audience: this.audience,
      algorithms: ["ES256"],
      exchange_transport: "opaque-server-to-server-post-body",
      max_exchange_ttl_seconds: this.maxTokenTtlSec,
      login_supported: false,
      required_login_architecture: "oidc-authorization-code-pkce-bff-local-server-session",
      workflow_ingress: "disabled-legacy-noncanonical",
      event_ingress: "disabled-legacy-noncanonical",
      local_experimental_event_types: sotyIdentityEventTypes,
      storage: {
        profile: "development-single-instance-file",
        transactional: false,
        distributed_replay_protection: false,
        durable_audit: false
      },
      rate_limit: { profile: "in-process-single-instance", distributed: false },
      production_ready: false,
      runtime_mode: this.runtimeMode
    };
  }

  readiness() {
    return {
      ok: false,
      schema: sotyIdentityAdapterSchema,
      status: "blocked",
      development_selftest_ready: this.runtimeMode !== "production",
      production_ready: false,
      runtime_mode: this.runtimeMode,
      blockers: [
        "legacy-wire-is-not-canonical-v1",
        "durable-transactional-replay-idempotency-audit-store-required",
        "distributed-rate-limiter-required"
      ]
    };
  }

  assertOperational() {
    if (this.runtimeMode === "production") {
      throw new SotyIdentityAdapterError("identity_production_store_required", 503);
    }
  }

  async consumeExchange(input) {
    this.assertOperational();
    const idempotencyKey = cleanIdempotencyKey(input?.idempotencyKey);
    const artifact = cleanArtifact(input?.exchange);
    const verified = verifyArtifact(artifact, sotyIdentityLinkExchangeType, this.issuers);
    const digest = sha256(artifact);
    const replay = await this.idempotentResponse(verified.issuer, "exchange", idempotencyKey, digest);
    if (replay) return replay;
    const claims = validateCommonClaims(verified.claims, {
      schema: sotyIdentityLinkExchangeSchema,
      audience: this.audience,
      projectId: this.projectId,
      nowMs: this.now(),
      maxTokenTtlSec: this.maxTokenTtlSec,
      clockSkewSec: this.clockSkewSec
    });
    rejectPii(claims);
    const membership = cleanMembershipClaims(claims);
    if (!membership.scopes.includes("identity:link")) throw new IdentityCompatError("identity_scope_denied", 403);

    return await this.mutate(() => {
      const now = this.now();
      this.expire(now);
      const previous = findIdempotency(this.state, verified.issuer, "exchange", idempotencyKey);
      if (previous) return sameDigest(previous, digest, "identity_idempotency_conflict");
      if (this.state.exchanges.some((item) => item.issuer === verified.issuer && item.jti === claims.jti)) {
        throw new IdentityCompatError("identity_exchange_replayed", 409);
      }

      const existing = this.state.bindings.find((item) => bindingMatches(item, verified.issuer, claims.sub, membership));
      const binding = existing || {
        bindingId: `ib_${randomUUID().replace(/-/gu, "")}`,
        issuer: verified.issuer,
        subjectId: claims.sub,
        organizationId: membership.organizationId,
        projectId: membership.projectId,
        membershipId: membership.membershipId,
        roles: [],
        scopes: [],
        membershipStatus: "active",
        linkStatus: "linked",
        devices: [],
        createdAt: now,
        linkedAt: now,
        updatedAt: now,
        unlinkedAt: 0
      };
      binding.roles = membership.roles;
      binding.scopes = membership.scopes;
      binding.membershipStatus = "active";
      binding.linkStatus = "linked";
      binding.linkedAt = now;
      binding.updatedAt = now;
      binding.unlinkedAt = 0;
      if (!existing) this.state.bindings.push(binding);

      const response = publicBindingReceipt(binding);
      this.state.exchanges.push({
        issuer: verified.issuer,
        jti: claims.jti,
        digest,
        consumedAt: now,
        expiresAt: Math.max(now + this.replayRetentionMs, claims.exp * 1000 + this.replayRetentionMs)
      });
      this.state.idempotency.push({
        issuer: verified.issuer,
        kind: "exchange",
        key: idempotencyKey,
        digest,
        response,
        expiresAt: now + this.replayRetentionMs
      });
      appendAudit(this.state, {
        at: now,
        action: "identity.linked",
        result: "accepted",
        issuer: verified.issuer,
        audience: this.audience,
        organizationId: binding.organizationId,
        projectId: binding.projectId,
        subjectId: binding.subjectId,
        bindingId: binding.bindingId,
        eventId: claims.jti
      });
      return response;
    });
  }

  async applyEvent(input) {
    this.assertOperational();
    const idempotencyKey = cleanIdempotencyKey(input?.idempotencyKey);
    const artifact = cleanArtifact(input?.event);
    const verified = verifyArtifact(artifact, sotyIdentityEventType, this.issuers);
    const digest = sha256(artifact);
    const replay = await this.idempotentResponse(verified.issuer, "event", idempotencyKey, digest);
    if (replay) return replay;
    const claims = validateCommonClaims(verified.claims, {
      schema: sotyIdentityEventSchema,
      audience: this.audience,
      projectId: this.projectId,
      nowMs: this.now(),
      maxTokenTtlSec: this.maxTokenTtlSec,
      clockSkewSec: this.clockSkewSec
    });
    rejectPii(claims);
    if (!Array.isArray(claims.scope) || !claims.scope.includes("identity:event")) {
      throw new IdentityCompatError("identity_scope_denied", 403);
    }
    const eventType = sotyIdentityEventTypes.includes(claims.event_type) ? claims.event_type : "";
    if (!eventType) throw new IdentityCompatError("identity_event_invalid");
    const membership = cleanMembershipIdentity(claims);
    const data = cleanEventData(eventType, claims.data);

    return await this.mutate(() => {
      const now = this.now();
      this.expire(now);
      const previous = findIdempotency(this.state, verified.issuer, "event", idempotencyKey);
      if (previous) return sameDigest(previous, digest, "identity_idempotency_conflict");
      if (this.state.events.some((item) => item.issuer === verified.issuer && item.eventId === claims.jti)) {
        throw new IdentityCompatError("identity_event_replayed", 409);
      }
      const binding = this.state.bindings.find((item) => bindingMatches(item, verified.issuer, claims.sub, membership));
      if (!binding) throw new IdentityCompatError("identity_binding_not_found", 404);
      applyIdentityEvent(binding, eventType, data, now);
      const response = {
        ok: true,
        schema: sotyIdentityAdapterSchema,
        applied: true,
        event_id: claims.jti,
        event_type: eventType,
        binding_id: binding.bindingId,
        link_status: binding.linkStatus,
        membership_status: binding.membershipStatus
      };
      this.state.events.push({
        issuer: verified.issuer,
        eventId: claims.jti,
        digest,
        appliedAt: now,
        expiresAt: Math.max(now + this.replayRetentionMs, claims.exp * 1000 + this.replayRetentionMs)
      });
      this.state.idempotency.push({
        issuer: verified.issuer,
        kind: "event",
        key: idempotencyKey,
        digest,
        response,
        expiresAt: now + this.replayRetentionMs
      });
      appendAudit(this.state, {
        at: now,
        action: eventType,
        result: "accepted",
        issuer: verified.issuer,
        audience: this.audience,
        organizationId: binding.organizationId,
        projectId: binding.projectId,
        subjectId: binding.subjectId,
        bindingId: binding.bindingId,
        eventId: claims.jti
      });
      return response;
    });
  }

  async snapshot() {
    await this.writeQueue;
    return structuredClone(this.state);
  }

  async idempotentResponse(issuer, kind, key, digest) {
    await this.writeQueue;
    const previous = findIdempotency(this.state, issuer, kind, key);
    return previous?.expiresAt > this.now() ? sameDigest(previous, digest, "identity_idempotency_conflict") : null;
  }

  async load() {
    if (!this.filePath) throw new IdentityCompatError("identity_config_invalid", 500);
    try {
      const parsed = JSON.parse(await readFile(this.filePath, "utf8"));
      if (!validState(parsed)) throw new Error("invalid identity state");
      this.state = parsed;
      this.expire(this.now());
    } catch (error) {
      if (error?.code === "ENOENT") {
        this.state = emptyState();
        return;
      }
      throw new IdentityCompatError("identity_store_unavailable", 503);
    }
  }

  async mutate(callback) {
    const run = this.writeQueue.then(async () => {
      const result = callback();
      await this.persist();
      return result;
    });
    this.writeQueue = run.then(() => undefined, () => undefined);
    return await run;
  }

  async persist() {
    await mkdir(path.dirname(this.filePath), { recursive: true, mode: 0o700 });
    const next = `${this.filePath}.${process.pid}.${randomUUID()}.next`;
    await writeFile(next, `${JSON.stringify(this.state, null, 2)}\n`, { encoding: "utf8", mode: 0o600 });
    await rename(next, this.filePath);
    await chmod(this.filePath, 0o600).catch(() => undefined);
  }

  expire(now) {
    this.state.exchanges = this.state.exchanges.filter((item) => item.expiresAt > now);
    this.state.events = this.state.events.filter((item) => item.expiresAt > now);
    this.state.idempotency = this.state.idempotency.filter((item) => item.expiresAt > now);
  }
}

function normalizeIssuers(value) {
  if (!Array.isArray(value) || value.length === 0 || value.length > 32) {
    throw new IdentityCompatError("identity_config_invalid", 500);
  }
  const issuers = new Map();
  for (const item of value) {
    const issuer = cleanIssuer(item?.issuer);
    const keys = Array.isArray(item?.jwks) ? item.jwks : Array.isArray(item?.keys) ? item.keys : [];
    if (!issuer || keys.length === 0 || keys.length > 16 || issuers.has(issuer)) {
      throw new IdentityCompatError("identity_config_invalid", 500);
    }
    const normalizedKeys = new Map();
    for (const jwk of keys) {
      let kid;
      try { kid = cleanOpaqueId(jwk?.kid, "kid"); }
      catch { throw new IdentityCompatError("identity_config_invalid", 500); }
      if (normalizedKeys.has(kid) || !validPublicSigningJwk(jwk)) {
        throw new IdentityCompatError("identity_config_invalid", 500);
      }
      try {
        normalizedKeys.set(kid, createPublicKey({ key: jwk, format: "jwk" }));
      } catch {
        throw new IdentityCompatError("identity_config_invalid", 500);
      }
    }
    issuers.set(issuer, normalizedKeys);
  }
  return issuers;
}

function verifyArtifact(artifact, expectedType, issuers) {
  const [headerPart, payloadPart, signaturePart, extra] = artifact.split(".");
  if (!headerPart || !payloadPart || !signaturePart || extra !== undefined) throw new IdentityCompatError("identity_artifact_invalid");
  const header = decodeJsonSegment(headerPart);
  const claims = decodeJsonSegment(payloadPart);
  if (
    header.alg !== "ES256"
    || header.typ !== expectedType
    || header.crit !== undefined
    || header.jku !== undefined
    || header.jwk !== undefined
    || header.x5u !== undefined
  ) {
    throw new IdentityCompatError("identity_artifact_invalid");
  }
  const issuer = cleanIssuer(claims.iss);
  const kid = cleanOpaqueId(header.kid, "kid");
  const key = issuers.get(issuer)?.get(kid);
  if (!key) throw new IdentityCompatError("identity_issuer_unknown", 401);
  const signature = decodeSegment(signaturePart);
  if (signature.length !== 64) throw new IdentityCompatError("identity_signature_invalid", 401);
  const ok = verifySignature(
    "sha256",
    Buffer.from(`${headerPart}.${payloadPart}`, "ascii"),
    { key, dsaEncoding: "ieee-p1363" },
    signature
  );
  if (!ok) throw new IdentityCompatError("identity_signature_invalid", 401);
  return { header, claims, issuer };
}

function validateCommonClaims(claims, options) {
  if (!isPlainObject(claims) || claims.schema !== options.schema) throw new IdentityCompatError("identity_claims_invalid");
  const nowSec = Math.floor(options.nowMs / 1000);
  const iat = safeTimestamp(claims.iat);
  const nbf = claims.nbf === undefined ? iat : safeTimestamp(claims.nbf);
  const exp = safeTimestamp(claims.exp);
  const jti = cleanOpaqueId(claims.jti, "jti");
  const sub = cleanOpaqueId(claims.sub, "sub");
  const projectId = cleanOpaqueId(claims.project_id, "project_id");
  if (!audienceContains(claims.aud, options.audience)) throw new IdentityCompatError("identity_audience_denied", 403);
  if (projectId !== options.projectId) throw new IdentityCompatError("identity_tenant_denied", 403);
  if (iat > nowSec + options.clockSkewSec || nbf > nowSec + options.clockSkewSec) throw new IdentityCompatError("identity_artifact_not_yet_valid", 401);
  if (exp <= nowSec - options.clockSkewSec) throw new IdentityCompatError("identity_artifact_expired", 401);
  if (exp <= iat || exp - iat > options.maxTokenTtlSec) throw new IdentityCompatError("identity_artifact_ttl_invalid", 401);
  return { ...claims, iat, nbf, exp, jti, sub, project_id: projectId };
}

function cleanMembershipClaims(claims) {
  const identity = cleanMembershipIdentity(claims);
  const roles = cleanList(claims.roles, allowedRoles, "identity_roles_invalid");
  const scopes = cleanList(claims.scope, allowedLinkScopes, "identity_scope_denied", 403);
  return { ...identity, roles, scopes };
}

function cleanMembershipIdentity(claims) {
  return {
    organizationId: cleanOpaqueId(claims.organization_id, "organization_id"),
    projectId: cleanOpaqueId(claims.project_id, "project_id"),
    membershipId: cleanOpaqueId(claims.membership_id, "membership_id")
  };
}

function cleanEventData(type, value) {
  if (!isPlainObject(value)) throw new IdentityCompatError("identity_event_invalid");
  rejectPii(value);
  if (type === "identity.membership.upserted") {
    assertOnlyKeys(value, ["roles", "scopes"]);
    return {
      roles: cleanList(value.roles, allowedRoles, "identity_roles_invalid"),
      scopes: cleanList(value.scopes, allowedLinkScopes, "identity_scope_denied", 403)
    };
  }
  if (type === "identity.membership.revoked" || type === "identity.link.unlinked") {
    assertOnlyKeys(value, []);
    return {};
  }
  if (type === "identity.device.enrolled") {
    assertOnlyKeys(value, ["device_id", "key_thumbprint"]);
    return { deviceId: cleanOpaqueId(value.device_id, "device_id"), keyThumbprint: cleanThumbprint(value.key_thumbprint) };
  }
  if (type === "identity.device.revoked") {
    assertOnlyKeys(value, ["device_id"]);
    return { deviceId: cleanOpaqueId(value.device_id, "device_id") };
  }
  if (type === "identity.device.recovery_started") {
    assertOnlyKeys(value, ["device_id", "recovery_id"]);
    return {
      deviceId: cleanOpaqueId(value.device_id, "device_id"),
      recoveryId: cleanOpaqueId(value.recovery_id, "recovery_id")
    };
  }
  if (type === "identity.device.recovered") {
    assertOnlyKeys(value, ["old_device_id", "new_device_id", "recovery_id", "key_thumbprint"]);
    const oldDeviceId = cleanOpaqueId(value.old_device_id, "old_device_id");
    const newDeviceId = cleanOpaqueId(value.new_device_id, "new_device_id");
    if (oldDeviceId === newDeviceId) throw new IdentityCompatError("identity_recovery_invalid");
    return {
      oldDeviceId,
      newDeviceId,
      recoveryId: cleanOpaqueId(value.recovery_id, "recovery_id"),
      keyThumbprint: cleanThumbprint(value.key_thumbprint)
    };
  }
  throw new IdentityCompatError("identity_event_invalid");
}

function applyIdentityEvent(binding, type, data, now) {
  if (type === "identity.membership.upserted") {
    binding.roles = data.roles;
    binding.scopes = data.scopes;
    binding.membershipStatus = "active";
    binding.updatedAt = now;
    return;
  }
  if (type === "identity.membership.revoked") {
    binding.membershipStatus = "revoked";
    unlinkBinding(binding, now);
    return;
  }
  if (type === "identity.link.unlinked") {
    unlinkBinding(binding, now);
    return;
  }
  requireActiveBinding(binding);
  if (type === "identity.device.enrolled") {
    const existing = binding.devices.find((item) => item.deviceId === data.deviceId);
    if (existing && existing.keyThumbprint !== data.keyThumbprint) throw new IdentityCompatError("identity_device_conflict", 409);
    if (existing) {
      existing.status = "active";
      existing.revokedAt = 0;
      existing.updatedAt = now;
    } else {
      binding.devices.push({
        deviceId: data.deviceId,
        keyThumbprint: data.keyThumbprint,
        status: "active",
        recoveryGeneration: 0,
        recovery: null,
        enrolledAt: now,
        updatedAt: now,
        revokedAt: 0,
        recoveredFrom: ""
      });
    }
    binding.updatedAt = now;
    return;
  }
  if (type === "identity.device.revoked") {
    const device = requireDevice(binding, data.deviceId);
    device.status = "revoked";
    device.revokedAt = now;
    device.updatedAt = now;
    device.recovery = null;
    binding.updatedAt = now;
    return;
  }
  if (type === "identity.device.recovery_started") {
    const device = requireDevice(binding, data.deviceId);
    if (device.status !== "active") throw new IdentityCompatError("identity_device_inactive", 409);
    if (device.recovery && device.recovery.recoveryId !== data.recoveryId) throw new IdentityCompatError("identity_recovery_conflict", 409);
    device.recovery = { recoveryId: data.recoveryId, startedAt: now };
    device.updatedAt = now;
    binding.updatedAt = now;
    return;
  }
  if (type === "identity.device.recovered") {
    const oldDevice = requireDevice(binding, data.oldDeviceId);
    if (!oldDevice.recovery || oldDevice.recovery.recoveryId !== data.recoveryId) throw new IdentityCompatError("identity_recovery_invalid", 409);
    if (binding.devices.some((item) => item.deviceId === data.newDeviceId)) throw new IdentityCompatError("identity_device_conflict", 409);
    oldDevice.status = "revoked";
    oldDevice.revokedAt = now;
    oldDevice.updatedAt = now;
    oldDevice.recovery = null;
    binding.devices.push({
      deviceId: data.newDeviceId,
      keyThumbprint: data.keyThumbprint,
      status: "active",
      recoveryGeneration: oldDevice.recoveryGeneration + 1,
      recovery: null,
      enrolledAt: now,
      updatedAt: now,
      revokedAt: 0,
      recoveredFrom: oldDevice.deviceId
    });
    binding.updatedAt = now;
  }
}

function unlinkBinding(binding, now) {
  binding.linkStatus = "unlinked";
  binding.unlinkedAt = now;
  binding.updatedAt = now;
  for (const device of binding.devices) {
    if (device.status === "active") {
      device.status = "revoked";
      device.revokedAt = now;
      device.updatedAt = now;
    }
    device.recovery = null;
  }
}

function requireActiveBinding(binding) {
  if (binding.linkStatus !== "linked" || binding.membershipStatus !== "active") {
    throw new IdentityCompatError("identity_binding_inactive", 409);
  }
}

function requireDevice(binding, deviceId) {
  const device = binding.devices.find((item) => item.deviceId === deviceId);
  if (!device) throw new IdentityCompatError("identity_device_not_found", 404);
  return device;
}

function publicBindingReceipt(binding) {
  return {
    ok: true,
    schema: sotyIdentityAdapterSchema,
    binding_id: binding.bindingId,
    organization_id: binding.organizationId,
    project_id: binding.projectId,
    membership_id: binding.membershipId,
    roles: binding.roles,
    scopes: binding.scopes,
    link_status: binding.linkStatus,
    membership_status: binding.membershipStatus,
    linked_at: new Date(binding.linkedAt).toISOString()
  };
}

function appendAudit(state, value) {
  const previous = state.audit[state.audit.length - 1];
  const entry = {
    seq: (previous?.seq || 0) + 1,
    id: `ia_${randomUUID().replace(/-/gu, "")}`,
    at: new Date(value.at).toISOString(),
    action: value.action,
    result: value.result,
    issuer: value.issuer,
    audience: value.audience,
    organizationId: value.organizationId,
    projectId: value.projectId,
    subjectHash: sha256(`${value.issuer}\u0000${value.organizationId}\u0000${value.projectId}\u0000${value.subjectId}`),
    bindingId: value.bindingId,
    eventId: value.eventId,
    previousHash: previous?.hash || ""
  };
  entry.hash = sha256(stableJson(entry));
  state.audit.push(entry);
}

export function verifySotyIdentityAuditChain(entries) {
  if (!Array.isArray(entries)) return false;
  let previousHash = "";
  let sequence = 1;
  for (const entry of entries) {
    if (!isPlainObject(entry) || entry.seq !== sequence || entry.previousHash !== previousHash || typeof entry.hash !== "string") return false;
    const { hash, ...unsigned } = entry;
    if (sha256(stableJson(unsigned)) !== hash) return false;
    previousHash = hash;
    sequence += 1;
  }
  return true;
}

function findIdempotency(state, issuer, kind, key) {
  return state.idempotency.find((item) => item.issuer === issuer && item.kind === kind && item.key === key);
}

function sameDigest(record, digest, conflictCode) {
  if (record.digest !== digest) throw new IdentityCompatError(conflictCode, 409);
  return structuredClone(record.response);
}

function bindingMatches(binding, issuer, subjectId, membership) {
  return binding.issuer === issuer
    && binding.subjectId === subjectId
    && binding.organizationId === membership.organizationId
    && binding.projectId === membership.projectId
    && binding.membershipId === membership.membershipId;
}

function emptyState() {
  return { schema: stateSchema, bindings: [], exchanges: [], events: [], idempotency: [], audit: [] };
}

function validState(value) {
  return isPlainObject(value)
    && value.schema === stateSchema
    && [value.bindings, value.exchanges, value.events, value.idempotency, value.audit].every(Array.isArray)
    && verifySotyIdentityAuditChain(value.audit);
}

function normalizeRuntimeMode(value) {
  if (value === "development" || value === "single-instance" || value === "production") return value;
  throw new SotyIdentityAdapterError("identity_config_invalid", 500);
}

function cleanArtifact(value) {
  const artifact = typeof value === "string" ? value.trim() : "";
  if (!artifact || artifact.length > maxArtifactChars || /\s/u.test(artifact)) throw new IdentityCompatError("identity_artifact_invalid");
  return artifact;
}

function cleanIdempotencyKey(value) {
  const key = typeof value === "string" ? value.trim() : "";
  if (!idempotencyPattern.test(key)) throw new IdentityCompatError("identity_idempotency_key_invalid");
  return key;
}

function cleanIssuer(value) {
  if (typeof value !== "string" || value.length > 300) return "";
  try {
    const url = new URL(value);
    if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash) return "";
    return url.href.replace(/\/$/u, "");
  } catch {
    return "";
  }
}

function cleanAudience(value) {
  const text = typeof value === "string" ? value.trim() : "";
  if (!text || text.length > 300 || /[\s?#]/u.test(text)) throw new IdentityCompatError("identity_config_invalid", 500);
  return text;
}

function cleanOpaqueId(value, name) {
  const text = typeof value === "string" ? value.trim() : "";
  if (!opaqueIdPattern.test(text)) throw new IdentityCompatError(`identity_${name}_invalid`);
  return text;
}

function cleanThumbprint(value) {
  const text = typeof value === "string" ? value.trim() : "";
  if (!/^sha256:[A-Za-z0-9_-]{43}$/u.test(text)) throw new IdentityCompatError("identity_key_thumbprint_invalid");
  return text;
}

function cleanList(value, allowed, errorCode, status = 400) {
  if (!Array.isArray(value) || value.length === 0 || value.length > maxListItems) throw new IdentityCompatError(errorCode, status);
  const list = [...new Set(value.map((item) => typeof item === "string" ? item.trim() : ""))];
  if (list.length !== value.length || list.some((item) => !allowed.has(item))) throw new IdentityCompatError(errorCode, status);
  return list.sort();
}

function audienceContains(value, expected) {
  if (typeof value === "string") return value === expected;
  return Array.isArray(value) && value.length > 0 && value.length <= 8 && value.every((item) => typeof item === "string") && value.includes(expected);
}

function rejectPii(value, depth = 0) {
  if (depth > 8) throw new IdentityCompatError("identity_claims_invalid");
  if (Array.isArray(value)) {
    for (const item of value) rejectPii(item, depth + 1);
    return;
  }
  if (!isPlainObject(value)) return;
  for (const [key, item] of Object.entries(value)) {
    const normalized = key.toLowerCase();
    if (forbiddenPiiNames.has(normalized)) throw new IdentityCompatError("identity_pii_forbidden", 400);
    if (forbiddenSecretNamePattern.test(normalized)) throw new IdentityCompatError("identity_secret_forbidden", 400);
    rejectPii(item, depth + 1);
  }
}

function assertOnlyKeys(value, allowed) {
  const expected = new Set(allowed);
  if (Object.keys(value).some((key) => !expected.has(key))) throw new IdentityCompatError("identity_event_invalid");
}

function validPublicSigningJwk(jwk) {
  return isPlainObject(jwk)
    && jwk.kty === "EC"
    && jwk.crv === "P-256"
    && jwk.alg === "ES256"
    && typeof jwk.x === "string"
    && typeof jwk.y === "string"
    && jwk.d === undefined
    && (jwk.use === undefined || jwk.use === "sig")
    && (jwk.key_ops === undefined || (Array.isArray(jwk.key_ops) && jwk.key_ops.includes("verify")));
}

function decodeJsonSegment(value) {
  try {
    const parsed = JSON.parse(decodeSegment(value).toString("utf8"));
    if (!isPlainObject(parsed)) throw new Error("not object");
    return parsed;
  } catch {
    throw new IdentityCompatError("identity_artifact_invalid");
  }
}

function decodeSegment(value) {
  if (!/^[A-Za-z0-9_-]+$/u.test(value)) throw new IdentityCompatError("identity_artifact_invalid");
  const decoded = Buffer.from(value, "base64url");
  if (decoded.toString("base64url") !== value) throw new IdentityCompatError("identity_artifact_invalid");
  return decoded;
}

function safeTimestamp(value) {
  if (!Number.isSafeInteger(value) || value < 1_600_000_000 || value > 4_102_444_800) throw new IdentityCompatError("identity_time_invalid");
  return value;
}

function safeInteger(value, min, max, fallback) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= min && parsed <= max ? parsed : fallback;
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (isPlainObject(value)) return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  return JSON.stringify(value);
}
