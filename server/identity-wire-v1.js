import {
  createHash,
  createPublicKey,
  randomUUID,
  verify as verifySignature
} from "node:crypto";
import { chmod, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const canonicalIdentityWireSchema = "identity.wire-conformance-manifest.v1";
export const canonicalIdentityEventSchema = "identity.service-event.v1";
export const canonicalIdentityEventType = "identity-service-event+jws";
export const canonicalWorkflowSchema = "identity.workflow-exchange-record.v1";
export const canonicalLoginSchema = "identity.browser-login-profile.v1";

const stateSchema = "soty.identity-wire-v1-shadow-store.v1";
const contractRoot = new URL("../contracts/identity/v1/", import.meta.url);
const vendorPin = loadContract("soty-vendor-pin.json");
const manifest = loadContract("data/contracts/identity/v1/conformance-manifest.json");
const loginProfile = loadContract("data/contracts/identity/v1/browser-login-profile.json");
const authorizationCatalog = loadContract("data/contracts/identity/v1/authorization-catalog.json");
const forbiddenNames = new Set([
  "access_token", "credential", "credentials", "email", "name", "password",
  "phone", "refresh_token", "secret", "token"
]);
const clientIdPattern = /^[a-z][a-z0-9.-]{2,127}$/u;
const stableIdPattern = /^[a-z][a-z0-9]*_[A-Za-z0-9][A-Za-z0-9_-]{4,127}$/u;
const subjectIdPattern = /^sub_[A-Za-z0-9_-]{5,127}$/u;
const organizationIdPattern = /^org_[A-Za-z0-9_-]{5,127}$/u;
const eventIdPattern = /^evt_[A-Za-z0-9_-]{5,127}$/u;
const exchangeIdPattern = /^xchg_[A-Za-z0-9_-]{5,127}$/u;
const membershipIdPattern = /^mem_[A-Za-z0-9_-]{5,127}$/u;
const projectIdPattern = /^prj_[A-Za-z0-9_-]{5,127}$/u;
const linkIdPattern = /^lnk_[A-Za-z0-9_-]{5,127}$/u;
const roleIdPattern = /^[a-z][a-z0-9_]{2,63}$/u;
const scopeIdPattern = /^[a-z][a-z0-9-]*(?::[a-z][a-z0-9-]*)+$/u;
const reasonPattern = /^[a-z][a-z0-9_]{2,63}$/u;
const base64urlPattern = /^[A-Za-z0-9_-]+$/u;
const canonicalEventTypes = new Set([
  "identity.subject.changed.v1",
  "identity.membership.changed.v1",
  "identity.project-access.changed.v1",
  "identity.subject.unlinked.v1",
  "identity.security-epoch.advanced.v1"
]);
const resourceKinds = new Set(["organization", "project", "feed_connection", "verification_case"]);

export class CanonicalIdentityError extends Error {
  constructor(code, status = 400) {
    super(code);
    this.name = "CanonicalIdentityError";
    this.code = code;
    this.status = status;
  }
}

verifyVendoredIdentityBundle();
validateCanonicalManifest(manifest);
validateBrowserLoginProfile(loginProfile);
validateAuthorizationCatalog(authorizationCatalog);

export function getCanonicalIdentityManifest() {
  return structuredClone(manifest);
}

export function getCanonicalIdentityVendorPin() {
  return structuredClone(vendorPin);
}

export function verifyVendoredIdentityBundle({ files } = {}) {
  exactObject(vendorPin, [
    "schema", "canonical_wire_version", "upstream", "synced_at",
    "synced_at_is_freshness_or_trust_claim", "digest_algorithm",
    "upstream_manifest", "contract_file_count", "files", "update_policy", "auto_merge"
  ], "identity_vendor_pin_invalid");
  exactObject(vendorPin.upstream, [
    "repository_path", "contract_root", "future_package_coordinate"
  ], "identity_vendor_pin_invalid");
  exactObject(vendorPin.upstream_manifest, ["path", "sha256"], "identity_vendor_pin_invalid");
  if (
    vendorPin.schema !== "soty.identity-wire-v1-vendor-pin.v1"
    || vendorPin.canonical_wire_version !== 1
    || vendorPin.synced_at_is_freshness_or_trust_claim !== false
    || vendorPin.digest_algorithm !== "sha256"
    || vendorPin.contract_file_count !== 23
    || typeof vendorPin.upstream.repository_path !== "string"
    || vendorPin.upstream.repository_path.length < 3
    || vendorPin.upstream.contract_root !== "."
    || vendorPin.upstream.future_package_coordinate !== null
    || !Number.isFinite(Date.parse(vendorPin.synced_at))
    || vendorPin.update_policy !== "explicit_reviewed_diff_only"
    || vendorPin.auto_merge !== false
    || !Array.isArray(vendorPin.files)
    || vendorPin.files.length !== vendorPin.contract_file_count
  ) {
    throw new CanonicalIdentityError("identity_vendor_pin_invalid", 500);
  }
  const expectedPaths = vendorPin.files.map((item) => item?.path);
  requireSortedUnique(expectedPaths, "identity_vendor_pin_invalid");
  const snapshot = files instanceof Map ? files : readVendoredFiles();
  const actualPaths = [...snapshot.keys()].sort();
  if (stableJson(actualPaths) !== stableJson(expectedPaths)) {
    throw new CanonicalIdentityError("identity_vendor_file_set_mismatch", 500);
  }
  for (const item of vendorPin.files) {
    exactObject(item, ["path", "sha256"], "identity_vendor_pin_invalid");
    if (typeof item.path !== "string" || !/^[a-zA-Z0-9_./-]+$/u.test(item.path) || !/^[0-9a-f]{64}$/u.test(item.sha256 || "")) {
      throw new CanonicalIdentityError("identity_vendor_pin_invalid", 500);
    }
    const value = snapshot.get(item.path);
    if (!Buffer.isBuffer(value) || sha256(normalizeVendoredContractBytes(value)) !== item.sha256) {
      throw new CanonicalIdentityError("identity_vendor_digest_mismatch", 500);
    }
  }
  const manifestEntry = vendorPin.files.find((item) => item.path === vendorPin.upstream_manifest?.path);
  if (!manifestEntry || manifestEntry.sha256 !== vendorPin.upstream_manifest.sha256) {
    throw new CanonicalIdentityError("identity_vendor_manifest_pin_mismatch", 500);
  }
  return {
    ok: true,
    canonical_wire_version: vendorPin.canonical_wire_version,
    contract_file_count: vendorPin.contract_file_count,
    upstream_manifest_sha256: vendorPin.upstream_manifest.sha256
  };
}

export function getCanonicalBrowserLoginProfile() {
  return structuredClone(loginProfile);
}

export function getCanonicalAuthorizationCatalog() {
  return structuredClone(authorizationCatalog);
}

export function readCanonicalIdentityConfig(env = process.env) {
  const enabled = env.SOTY_IDENTITY_V1_ENABLED === "1";
  if (!enabled) return { enabled: false };
  const runtimeMode = normalizeRuntimeMode(env.SOTY_IDENTITY_V1_RUNTIME || "production");
  const clientId = cleanClientId(env.SOTY_IDENTITY_V1_CLIENT_ID || "soty.online");
  const targetOrigin = cleanOrigin(env.SOTY_IDENTITY_V1_TARGET_ORIGIN || "https://soty.online");
  let issuers;
  try {
    issuers = JSON.parse(env.SOTY_IDENTITY_V1_ISSUERS_JSON || "[]");
  } catch {
    throw new CanonicalIdentityError("identity_v1_config_invalid", 500);
  }
  return { enabled: true, runtimeMode, clientId, targetOrigin, issuers };
}

export function createCanonicalIdentityService(options = {}) {
  return new CanonicalIdentityService(options);
}

export function createIdentityControlPlaneRedeemClient(options = {}) {
  if (
    typeof options.redeemTransport !== "function"
    || options.authenticatedServerToServer !== true
    || typeof options.validateResponse !== "function"
  ) {
    throw new CanonicalIdentityError("identity_control_plane_client_config_invalid", 500);
  }
  return Object.freeze({
    async redeemForExistingSession(input, context = {}) {
      if (context.hasOidcBffSession !== true) {
        throw new CanonicalIdentityError("identity_oidc_bff_session_required", 401);
      }
      const request = validateWorkflowRedeemRequest(input);
      let response;
      try {
        response = await options.redeemTransport(structuredClone(request));
      } catch {
        throw new CanonicalIdentityError("identity_control_plane_unavailable", 503);
      }
      if (!isPlainObject(response) || options.validateResponse(response) !== true) {
        throw new CanonicalIdentityError("identity_control_plane_response_invalid", 502);
      }
      return structuredClone(response);
    },
    capabilities() {
      return {
        role: "authenticated-control-plane-redeem-client",
        local_exchange_issuer: false,
        local_exchange_store: false,
        local_atomic_consumer: false,
        requires_existing_oidc_bff_session: true
      };
    }
  });
}

export function validateCanonicalManifest(value) {
  exactObject(value, [
    "schema", "canonical_wire_version", "authority_model", "browser_login",
    "workflow_exchange", "service_event", "authorization_catalog", "vectors",
    "consumer_migrations", "human_gates"
  ], "identity_v1_manifest_invalid");
  exactObject(value.authority_model, [
    "identity_provider", "organization_control_plane",
    "workflow_exchange_issuer_and_redeemer", "service_event_signer_and_outbox",
    "product_runtime_role", "product_may_implement_exchange_store_or_signer",
    "product_test_double_exception", "forbidden_master_idps"
  ], "identity_v1_manifest_invalid");
  if (
    value.schema !== canonicalIdentityWireSchema
    || value.canonical_wire_version !== 1
    || value.authority_model?.identity_provider !== "provider_neutral_oidc"
    || value.authority_model?.organization_control_plane !== "separate_provider_neutral_service"
    || value.authority_model?.workflow_exchange_issuer_and_redeemer !== "organization_control_plane_only"
    || value.authority_model?.service_event_signer_and_outbox !== "organization_control_plane_only"
    || value.authority_model?.product_runtime_role !== "relying_party_consumer_only"
    || value.authority_model?.product_may_implement_exchange_store_or_signer !== false
    || value.authority_model?.product_test_double_exception !== "isolated_nonproduction_conformance_only"
    || stableJson(value.authority_model?.forbidden_master_idps) !== stableJson(["soty", "tavysh"])
    || value.workflow_exchange?.serialization !== "opaque_base64url_no_padding"
    || value.workflow_exchange?.code_entropy_bytes !== 32
    || value.workflow_exchange?.max_ttl_seconds !== 60
    || value.workflow_exchange?.stored_form !== "sha256_of_ascii_code"
    || value.workflow_exchange?.redeem_transport !== "authenticated_server_to_server_post"
    || value.service_event?.serialization !== "rfc7515_flattened_jws_json"
    || value.service_event?.algorithm !== "EdDSA"
    || value.service_event?.curve !== "Ed25519"
    || value.service_event?.payload_canonicalization !== "rfc8785_jcs_utf8"
    || value.service_event?.key_resolution !== "pinned_issuer_and_kid_registry"
    || !Array.isArray(value.vectors)
    || value.vectors.length !== 12
    || value.consumer_migrations?.soty?.status !== "adapter_required"
  ) {
    throw new CanonicalIdentityError("identity_v1_manifest_invalid");
  }
  const vectorIds = new Set(value.vectors.map((item) => item?.id));
  if (vectorIds.size !== value.vectors.length) throw new CanonicalIdentityError("identity_v1_manifest_invalid");
  return true;
}

export function validateBrowserLoginProfile(value) {
  exactObject(value, [
    "schema", "protocol", "flow", "response_type", "pkce_method", "client_kind",
    "browser_session", "browser_token_storage", "custom_login_token_protocol", "required_checks"
  ], "identity_login_profile_invalid");
  if (
    value.schema !== canonicalLoginSchema
    || value.protocol !== "openid-connect-core-1.0"
    || value.flow !== "authorization_code"
    || value.response_type !== "code"
    || value.pkce_method !== "S256"
    || value.client_kind !== "confidential_bff"
    || value.browser_session !== "server_side_secure_http_only_samesite_lax_cookie"
    || value.browser_token_storage !== "forbidden"
    || value.custom_login_token_protocol !== false
    || stableJson(value.required_checks) !== stableJson(["issuer", "audience", "signature", "expiration", "state", "nonce"])
  ) {
    throw new CanonicalIdentityError("identity_login_profile_invalid");
  }
  return true;
}

export function validateAuthorizationCatalog(value) {
  exactObject(value, ["schema", "catalog_version", "scopes", "roles"], "identity_authorization_catalog_invalid");
  if (value.schema !== "identity.authorization-catalog.v1" || value.catalog_version !== 1) {
    throw new CanonicalIdentityError("identity_authorization_catalog_invalid");
  }
  const scopes = boundedArray(value.scopes, 1, 64, "identity_authorization_catalog_invalid");
  const roles = boundedArray(value.roles, 1, 32, "identity_authorization_catalog_invalid");
  const scopeIds = [];
  const scopeById = new Map();
  for (const scope of scopes) {
    exactObject(scope, ["id", "boundary", "allowed_actor_types"], "identity_authorization_catalog_invalid");
    if (!scopeIdPattern.test(scope.id) || !["tenant", "platform"].includes(scope.boundary)) {
      throw new CanonicalIdentityError("identity_authorization_catalog_invalid");
    }
    const actors = exactUniqueStrings(scope.allowed_actor_types, 1, 2, new Set(["human_session", "machine_client"]), "identity_authorization_catalog_invalid");
    if (scope.boundary === "platform" && stableJson(actors) !== stableJson(["human_session"])) {
      throw new CanonicalIdentityError("identity_authorization_catalog_invalid");
    }
    scopeIds.push(scope.id);
    scopeById.set(scope.id, scope);
  }
  requireSortedUnique(scopeIds, "identity_authorization_catalog_invalid");
  const roleIds = [];
  for (const role of roles) {
    exactObject(role, ["id", "boundary", "scopes", "requires_human_session"], "identity_authorization_catalog_invalid");
    if (!roleIdPattern.test(role.id) || !["tenant", "platform"].includes(role.boundary) || typeof role.requires_human_session !== "boolean") {
      throw new CanonicalIdentityError("identity_authorization_catalog_invalid");
    }
    const roleScopes = exactSortedUniqueStrings(role.scopes, 0, 64, null, "identity_authorization_catalog_invalid");
    for (const scopeId of roleScopes) {
      const scope = scopeById.get(scopeId);
      if (!scope || scope.boundary !== role.boundary) throw new CanonicalIdentityError("identity_authorization_catalog_invalid");
    }
    if (role.boundary === "platform" && role.requires_human_session !== true) {
      throw new CanonicalIdentityError("identity_authorization_catalog_invalid");
    }
    roleIds.push(role.id);
  }
  requireSortedUnique(roleIds, "identity_authorization_catalog_invalid");
  return true;
}

export function validateWorkflowExchangeRecord(value) {
  exactObject(value, [
    "schema", "exchange_id", "source_client_id", "target_client_id", "target_origin",
    "purpose", "state", "status", "subject_id", "organization_id", "resources",
    "issued_at", "expires_at", "code_hash", "consumed_at"
  ], "identity_workflow_record_invalid");
  if (
    value.schema !== canonicalWorkflowSchema
    || !exchangeIdPattern.test(value.exchange_id)
    || !clientIdPattern.test(value.source_client_id)
    || !clientIdPattern.test(value.target_client_id)
    || cleanOrigin(value.target_origin) !== value.target_origin
    || value.purpose !== "account_link"
    || typeof value.state !== "string"
    || value.state.length < 22
    || value.state.length > 128
    || !base64urlPattern.test(value.state)
    || !["issued", "consumed"].includes(value.status)
    || !subjectIdPattern.test(value.subject_id)
    || !organizationIdPattern.test(value.organization_id)
    || !/^sha256:[0-9a-f]{64}$/u.test(value.code_hash)
  ) {
    throw new CanonicalIdentityError("identity_workflow_record_invalid");
  }
  const issuedAt = parseCanonicalTime(value.issued_at, "identity_workflow_record_invalid");
  const expiresAt = parseCanonicalTime(value.expires_at, "identity_workflow_record_invalid");
  const ttlSeconds = (expiresAt - issuedAt) / 1000;
  if (!(ttlSeconds > 0 && ttlSeconds <= 60)) throw new CanonicalIdentityError("identity_workflow_ttl_invalid");
  if ((value.status === "issued" && value.consumed_at !== null) || (value.status === "consumed" && typeof value.consumed_at !== "string")) {
    throw new CanonicalIdentityError("identity_workflow_record_invalid");
  }
  if (typeof value.consumed_at === "string") parseCanonicalTime(value.consumed_at, "identity_workflow_record_invalid");
  const resources = validateResources(value.resources);
  if (!resources.some((item) => item.kind === "organization" && item.id === value.organization_id)) {
    throw new CanonicalIdentityError("identity_workflow_binding_invalid");
  }
  return structuredClone(value);
}

export function validateWorkflowRedeemRequest(value) {
  exactObject(value, [
    "code", "target_client_id", "target_origin", "purpose", "state",
    "organization_id", "resources"
  ], "identity_workflow_redeem_request_invalid");
  const code = cleanWorkflowCode(value.code);
  const targetClientId = cleanClientId(value.target_client_id);
  const targetOrigin = cleanOrigin(value.target_origin);
  if (
    value.purpose !== "account_link"
    || typeof value.state !== "string"
    || value.state.length < 22
    || value.state.length > 128
    || !base64urlPattern.test(value.state)
    || !organizationIdPattern.test(value.organization_id)
  ) {
    throw new CanonicalIdentityError("identity_workflow_redeem_request_invalid");
  }
  const resources = validateResources(value.resources);
  if (!resources.some((item) => item.kind === "organization" && item.id === value.organization_id)) {
    throw new CanonicalIdentityError("identity_workflow_redeem_request_invalid");
  }
  return {
    code,
    target_client_id: targetClientId,
    target_origin: targetOrigin,
    purpose: value.purpose,
    state: value.state,
    organization_id: value.organization_id,
    resources
  };
}

export function validateCanonicalEventPayload(value, { consumerClientId, catalog = authorizationCatalog } = {}) {
  exactObject(value, [
    "schema", "event_id", "event_type", "issuer", "audience", "occurred_at",
    "subject_id", "organization_id", "aggregate_id", "revision", "previous_event_id", "data"
  ], "identity_event_payload_invalid");
  const clientId = cleanClientId(consumerClientId);
  if (
    value.schema !== canonicalIdentityEventSchema
    || !eventIdPattern.test(value.event_id)
    || !canonicalEventTypes.has(value.event_type)
    || cleanIssuer(value.issuer) !== value.issuer
    || value.audience !== clientId
    || !subjectIdPattern.test(value.subject_id)
    || !stableIdPattern.test(value.aggregate_id)
    || !Number.isSafeInteger(value.revision)
    || value.revision < 1
  ) {
    throw new CanonicalIdentityError("identity_event_payload_invalid");
  }
  parseCanonicalTime(value.occurred_at, "identity_event_payload_invalid");
  if (value.revision === 1 ? value.previous_event_id !== null : !eventIdPattern.test(value.previous_event_id || "")) {
    throw new CanonicalIdentityError("identity_event_revision_invalid");
  }
  rejectPiiAndSecrets(value);
  validateEventData(value, catalog);
  return structuredClone(value);
}

export function verifyCanonicalServiceEvent(envelope, { consumerClientId, issuers } = {}) {
  exactObject(envelope, ["protected", "payload", "signature"], "identity_event_envelope_invalid");
  if (
    typeof envelope.protected !== "string"
    || envelope.protected.length < 2
    || envelope.protected.length > 32768
    || !base64urlPattern.test(envelope.protected)
    || typeof envelope.payload !== "string"
    || envelope.payload.length < 2
    || envelope.payload.length > 32768
    || !base64urlPattern.test(envelope.payload)
    || typeof envelope.signature !== "string"
    || envelope.signature.length !== 86
    || !base64urlPattern.test(envelope.signature)
  ) {
    throw new CanonicalIdentityError("identity_event_envelope_invalid");
  }
  const protectedBytes = decodeBase64url(envelope.protected, "identity_event_envelope_invalid");
  const payloadBytes = decodeBase64url(envelope.payload, "identity_event_envelope_invalid");
  const signature = decodeBase64url(envelope.signature, "identity_event_signature_invalid", 401);
  if (signature.length !== 64) throw new CanonicalIdentityError("identity_event_signature_invalid", 401);
  const header = parseJsonBytes(protectedBytes, "identity_event_header_invalid");
  exactObject(header, ["alg", "kid", "typ"], "identity_event_header_invalid");
  if (
    header.alg !== "EdDSA"
    || header.typ !== canonicalIdentityEventType
    || !/^key_[A-Za-z0-9_-]{5,127}$/u.test(header.kid)
    || !protectedBytes.equals(Buffer.from(stableJson(header), "utf8"))
  ) {
    throw new CanonicalIdentityError("identity_event_header_invalid");
  }
  const payload = parseJsonBytes(payloadBytes, "identity_event_payload_invalid");
  if (!payloadBytes.equals(Buffer.from(stableJson(payload), "utf8"))) {
    throw new CanonicalIdentityError("identity_event_payload_not_canonical");
  }
  const registry = issuers instanceof Map ? issuers : normalizeCanonicalIssuers(issuers);
  const key = registry.get(cleanIssuer(payload?.issuer))?.get(header.kid);
  if (!key) throw new CanonicalIdentityError("identity_event_issuer_unknown", 401);
  const verified = verifySignature(
    null,
    Buffer.from(`${envelope.protected}.${envelope.payload}`, "ascii"),
    key,
    signature
  );
  if (!verified) throw new CanonicalIdentityError("identity_event_signature_invalid", 401);
  const canonicalPayload = validateCanonicalEventPayload(payload, { consumerClientId });
  return {
    header,
    payload: canonicalPayload,
    digest: sha256(`${envelope.protected}.${envelope.payload}.${envelope.signature}`)
  };
}

class CanonicalIdentityService {
  constructor(options) {
    this.filePath = String(options.filePath || "");
    if (!this.filePath) throw new CanonicalIdentityError("identity_v1_config_invalid", 500);
    this.runtimeMode = normalizeRuntimeMode(options.runtimeMode || "development");
    this.clientId = cleanClientId(options.clientId);
    this.targetOrigin = cleanOrigin(options.targetOrigin);
    this.issuers = normalizeCanonicalIssuers(options.issuers);
    this.now = typeof options.now === "function" ? options.now : () => Date.now();
    this.state = emptyState();
    this.writeQueue = this.load();
  }

  capabilities() {
    return {
      ok: true,
      schema: canonicalIdentityWireSchema,
      canonical_wire_version: 1,
      canonical: true,
      integration_status: "disabled-adapter",
      sso_available: false,
      client_id: this.clientId,
      target_origin: this.targetOrigin,
      browser_login: getCanonicalBrowserLoginProfile(),
      workflow_exchange: {
        ownership: "neutral-identity-organization-control-plane-only",
        product_role: "authenticated-redeem-client-after-existing-oidc-bff-session",
        serialization: "opaque_base64url_no_padding",
        code_entropy_bytes: 32,
        max_ttl_seconds: 60,
        stored_form: "sha256_of_ascii_code",
        redeem_transport: "authenticated_server_to_server_post",
        local_issuer: false,
        local_store: false,
        local_redeem_endpoint: false
      },
      workflow_redeem_client: "not-wired",
      service_event: {
        product_role: "consumer-only",
        serialization: "rfc7515_flattened_jws_json",
        algorithm: "EdDSA",
        curve: "Ed25519",
        typ: canonicalIdentityEventType,
        payload_canonicalization: "rfc8785_jcs_utf8",
        local_signer: false,
        local_outbox: false
      },
      service_event_delivery: "fail-closed-no-authenticator-wired",
      authorization_effect: "shadow-projection-only",
      role_scope_mapping: {
        canonical_tenant_catalog: "exact-id-pass-through-to-shadow-projection",
        canonical_platform_catalog: "rejected-by-product-adapter",
        legacy_soty_roles: "unmapped-and-rejected",
        legacy_soty_scopes: "unmapped-and-rejected",
        existing_room_or_connector_grants: "none"
      },
      legacy_wire_accepted_as_v1: false,
      production_ready: false,
      runtime_mode: this.runtimeMode
    };
  }

  readiness({ serviceAuthenticationAvailable = false, oidcBffSessionAvailable = false } = {}) {
    const blockers = [];
    if (this.runtimeMode === "production") blockers.push("durable-transactional-store-required");
    if (!serviceAuthenticationAvailable) blockers.push("authenticated-server-to-server-transport-required");
    if (!oidcBffSessionAvailable) blockers.push("oidc-bff-local-session-required");
    blockers.push("human-cutover-approval-required");
    return {
      ok: false,
      schema: canonicalIdentityWireSchema,
      integration_status: "disabled-adapter",
      conformance_ready: true,
      production_ready: false,
      authorization_effect: "shadow-projection-only",
      blockers
    };
  }

  assertOperational() {
    if (this.runtimeMode === "production") {
      throw new CanonicalIdentityError("identity_v1_production_store_required", 503);
    }
  }

  async registerLocalBinding(binding) {
    this.assertOperational();
    const subjectId = requirePattern(binding?.subject_id, subjectIdPattern, "identity_binding_invalid");
    const organizationId = requirePattern(binding?.organization_id, organizationIdPattern, "identity_binding_invalid");
    const resources = validateResources(binding?.resources);
    if (!resources.some((item) => item.kind === "organization" && item.id === organizationId)) {
      throw new CanonicalIdentityError("identity_binding_invalid");
    }
    return await this.mutate(() => {
      const issuer = cleanIssuer(binding?.issuer);
      if (!issuer) throw new CanonicalIdentityError("identity_binding_invalid");
      this.state.bindings.push({
        binding_id: `cib_${randomUUID().replace(/-/gu, "")}`,
        issuer,
        subject_id: subjectId,
        organization_id: organizationId,
        resources,
        target_client_id: this.clientId,
        target_origin: this.targetOrigin,
        status: "linked",
        created_at: new Date(this.now()).toISOString()
      });
      return { ok: true };
    });
  }

  async applyServiceEvent(envelope) {
    this.assertOperational();
    const verified = verifyCanonicalServiceEvent(envelope, {
      consumerClientId: this.clientId,
      issuers: this.issuers
    });
    return await this.mutate(() => applyVerifiedEvent(this.state, verified, this.now()));
  }

  async snapshot() {
    await this.writeQueue;
    return structuredClone(this.state);
  }

  async load() {
    try {
      const parsed = JSON.parse(await readFile(this.filePath, "utf8"));
      if (!validState(parsed)) throw new Error("invalid canonical identity state");
      this.state = parsed;
    } catch (error) {
      if (error?.code === "ENOENT") return;
      throw new CanonicalIdentityError("identity_v1_store_unavailable", 503);
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
}

function validateEventData(event, catalog) {
  const data = event.data;
  const scopeById = new Map(catalog.scopes.map((item) => [item.id, item]));
  const roleById = new Map(catalog.roles.map((item) => [item.id, item]));
  const requireOrganization = () => requirePattern(event.organization_id, organizationIdPattern, "identity_event_tenant_invalid");
  if (event.event_type === "identity.subject.changed.v1") {
    if (event.organization_id !== null || event.aggregate_id !== event.subject_id) throw new CanonicalIdentityError("identity_event_binding_invalid");
    exactObject(data, ["status", "reason_code"], "identity_event_data_invalid");
    if (!["active", "suspended", "deleted"].includes(data.status) || !reasonPattern.test(data.reason_code)) throw new CanonicalIdentityError("identity_event_data_invalid");
    return;
  }
  if (event.event_type === "identity.membership.changed.v1") {
    requireOrganization();
    exactObject(data, ["membership_id", "status", "roles", "scopes"], "identity_event_data_invalid");
    if (!membershipIdPattern.test(data.membership_id) || event.aggregate_id !== data.membership_id || !["active", "suspended", "revoked"].includes(data.status)) {
      throw new CanonicalIdentityError("identity_event_data_invalid");
    }
    validateCatalogGrant(data.roles, data.scopes, roleById, scopeById);
    return;
  }
  if (event.event_type === "identity.project-access.changed.v1") {
    requireOrganization();
    exactObject(data, ["project_id", "membership_id", "status", "scopes"], "identity_event_data_invalid");
    if (!projectIdPattern.test(data.project_id) || !membershipIdPattern.test(data.membership_id) || event.aggregate_id !== data.project_id || !["active", "suspended", "revoked"].includes(data.status)) {
      throw new CanonicalIdentityError("identity_event_data_invalid");
    }
    validateCatalogGrant([], data.scopes, roleById, scopeById);
    return;
  }
  if (event.event_type === "identity.subject.unlinked.v1") {
    requireOrganization();
    exactObject(data, ["link_id", "status", "reason_code"], "identity_event_data_invalid");
    if (!linkIdPattern.test(data.link_id) || event.aggregate_id !== data.link_id || data.status !== "unlinked" || !reasonPattern.test(data.reason_code)) {
      throw new CanonicalIdentityError("identity_event_data_invalid");
    }
    return;
  }
  if (event.event_type === "identity.security-epoch.advanced.v1") {
    if (event.organization_id !== null || event.aggregate_id !== event.subject_id) throw new CanonicalIdentityError("identity_event_binding_invalid");
    exactObject(data, ["security_epoch", "reason_code"], "identity_event_data_invalid");
    if (!Number.isSafeInteger(data.security_epoch) || data.security_epoch < 1 || !reasonPattern.test(data.reason_code)) throw new CanonicalIdentityError("identity_event_data_invalid");
  }
}

function validateCatalogGrant(roles, scopes, roleById, scopeById) {
  const roleIds = exactSortedUniqueStrings(roles, 0, 16, null, "identity_event_authorization_invalid");
  const scopeIds = exactSortedUniqueStrings(scopes, 0, 32, null, "identity_event_authorization_invalid");
  for (const roleId of roleIds) {
    const role = roleById.get(roleId);
    if (!role || role.boundary !== "tenant") throw new CanonicalIdentityError("identity_event_authorization_invalid");
  }
  for (const scopeId of scopeIds) {
    const scope = scopeById.get(scopeId);
    if (!scope || scope.boundary !== "tenant") throw new CanonicalIdentityError("identity_event_authorization_invalid");
  }
}

function applyVerifiedEvent(state, verified, now) {
  const event = verified.payload;
  const existing = state.events.find((item) => item.issuer === event.issuer && item.event_id === event.event_id);
  if (existing) {
    if (existing.digest === verified.digest) {
      return { ok: true, status: "idempotent", event_id: event.event_id };
    }
    appendAudit(state, now, "identity.service-event.security-incident", {
      event_id: event.event_id,
      organization_id: event.organization_id,
      subject_id: event.subject_id
    });
    return { ok: false, status: "security_incident", event_id: event.event_id };
  }
  const quarantined = state.quarantine.find((item) => item.issuer === event.issuer && item.event_id === event.event_id);
  if (quarantined) {
    if (quarantined.digest === verified.digest) {
      return { ok: false, status: "quarantined", reason: quarantined.reason, event_id: event.event_id, idempotent: true };
    }
    appendAudit(state, now, "identity.service-event.security-incident", {
      event_id: event.event_id,
      organization_id: event.organization_id,
      subject_id: event.subject_id
    });
    return { ok: false, status: "security_incident", event_id: event.event_id };
  }
  assertLocalEventBinding(state, event);
  const head = state.heads.find((item) => item.issuer === event.issuer && item.aggregate_id === event.aggregate_id);
  const expectedRevision = head ? head.revision + 1 : 1;
  const expectedPrevious = head ? head.event_id : null;
  if (event.revision < expectedRevision) throw new CanonicalIdentityError("identity_event_stale", 409);
  if (event.revision > expectedRevision) {
    state.quarantine.push({ issuer: event.issuer, event_id: event.event_id, digest: verified.digest, reason: "revision_gap", quarantined_at: new Date(now).toISOString() });
    appendAudit(state, now, "identity.service-event.quarantined", { event_id: event.event_id, organization_id: event.organization_id, subject_id: event.subject_id });
    return { ok: false, status: "quarantined", reason: "revision_gap", event_id: event.event_id };
  }
  if (event.previous_event_id !== expectedPrevious) throw new CanonicalIdentityError("identity_event_predecessor_mismatch", 409);
  if (event.event_type === "identity.security-epoch.advanced.v1") {
    const current = state.securityEpochs.find((item) => item.issuer === event.issuer && item.subject_id === event.subject_id);
    if (current && event.data.security_epoch <= current.security_epoch) throw new CanonicalIdentityError("identity_security_epoch_stale", 409);
  }
  applyProjection(state, event, now);
  state.events.push({ issuer: event.issuer, event_id: event.event_id, digest: verified.digest, applied_at: new Date(now).toISOString() });
  if (head) {
    head.revision = event.revision;
    head.event_id = event.event_id;
  } else {
    state.heads.push({ issuer: event.issuer, aggregate_id: event.aggregate_id, revision: event.revision, event_id: event.event_id });
  }
  appendAudit(state, now, event.event_type, { event_id: event.event_id, organization_id: event.organization_id, subject_id: event.subject_id });
  return { ok: true, status: "applied", event_id: event.event_id, revision: event.revision };
}

function assertLocalEventBinding(state, event) {
  const candidates = state.bindings.filter((item) => (
    item.status === "linked"
    && item.issuer === event.issuer
    && item.subject_id === event.subject_id
  ));
  if (event.organization_id === null) {
    if (candidates.length === 0) throw new CanonicalIdentityError("identity_event_binding_not_found", 404);
    return;
  }
  const binding = candidates.find((item) => item.organization_id === event.organization_id);
  if (!binding) throw new CanonicalIdentityError("identity_event_binding_not_found", 404);
  if (event.event_type === "identity.project-access.changed.v1") {
    if (!binding.resources.some((item) => item.kind === "project" && item.id === event.data.project_id)) {
      throw new CanonicalIdentityError("identity_event_resource_denied", 403);
    }
  }
}

function applyProjection(state, event, now) {
  const updatedAt = new Date(now).toISOString();
  if (event.event_type === "identity.subject.changed.v1") {
    upsert(state.subjects, (item) => item.issuer === event.issuer && item.subject_id === event.subject_id, {
      issuer: event.issuer, subject_id: event.subject_id, status: event.data.status, updated_at: updatedAt
    });
    return;
  }
  if (event.event_type === "identity.membership.changed.v1") {
    upsert(state.memberships, (item) => item.issuer === event.issuer && item.membership_id === event.data.membership_id, {
      issuer: event.issuer,
      subject_id: event.subject_id,
      organization_id: event.organization_id,
      membership_id: event.data.membership_id,
      status: event.data.status,
      roles: structuredClone(event.data.roles),
      scopes: structuredClone(event.data.scopes),
      updated_at: updatedAt
    });
    return;
  }
  if (event.event_type === "identity.project-access.changed.v1") {
    upsert(state.projectAccess, (item) => item.issuer === event.issuer && item.project_id === event.data.project_id && item.membership_id === event.data.membership_id, {
      issuer: event.issuer,
      subject_id: event.subject_id,
      organization_id: event.organization_id,
      project_id: event.data.project_id,
      membership_id: event.data.membership_id,
      status: event.data.status,
      scopes: structuredClone(event.data.scopes),
      updated_at: updatedAt
    });
    return;
  }
  if (event.event_type === "identity.subject.unlinked.v1") {
    for (const binding of state.bindings) {
      if (
        binding.issuer === event.issuer
        && binding.subject_id === event.subject_id
        && binding.organization_id === event.organization_id
      ) binding.status = "unlinked";
    }
    state.links.push({ issuer: event.issuer, subject_id: event.subject_id, organization_id: event.organization_id, link_id: event.data.link_id, status: "unlinked", updated_at: updatedAt });
    return;
  }
  if (event.event_type === "identity.security-epoch.advanced.v1") {
    upsert(state.securityEpochs, (item) => item.issuer === event.issuer && item.subject_id === event.subject_id, {
      issuer: event.issuer, subject_id: event.subject_id, security_epoch: event.data.security_epoch, updated_at: updatedAt
    });
  }
}

function cleanWorkflowCode(value) {
  if (typeof value !== "string" || value.length !== 43 || !base64urlPattern.test(value)) {
    throw new CanonicalIdentityError("identity_workflow_code_invalid");
  }
  const decoded = decodeBase64url(value, "identity_workflow_code_invalid");
  if (decoded.length !== 32) throw new CanonicalIdentityError("identity_workflow_code_invalid");
  return value;
}

function validateResources(value) {
  const resources = boundedArray(value, 1, 32, "identity_workflow_resources_invalid").map((item) => {
    exactObject(item, ["kind", "id"], "identity_workflow_resources_invalid");
    if (!resourceKinds.has(item.kind) || !stableIdPattern.test(item.id)) throw new CanonicalIdentityError("identity_workflow_resources_invalid");
    return { kind: item.kind, id: item.id };
  });
  const serialized = resources.map((item) => `${item.kind}\u0000${item.id}`);
  requireSortedUnique(serialized, "identity_workflow_resources_invalid");
  return resources;
}

function normalizeCanonicalIssuers(value) {
  if (!Array.isArray(value) || value.length === 0 || value.length > 32) throw new CanonicalIdentityError("identity_v1_config_invalid", 500);
  const registry = new Map();
  for (const entry of value) {
    const issuer = cleanIssuer(entry?.issuer);
    const keys = Array.isArray(entry?.keys) ? entry.keys : Array.isArray(entry?.jwks) ? entry.jwks : [];
    if (!issuer || registry.has(issuer) || keys.length === 0 || keys.length > 16) throw new CanonicalIdentityError("identity_v1_config_invalid", 500);
    const keyMap = new Map();
    for (const jwk of keys) {
      if (
        !jwk
        || jwk.kty !== "OKP"
        || jwk.crv !== "Ed25519"
        || jwk.alg !== "EdDSA"
        || jwk.use !== "sig"
        || !/^key_[A-Za-z0-9_-]{5,127}$/u.test(jwk.kid || "")
        || typeof jwk.x !== "string"
        || keyMap.has(jwk.kid)
        || jwk.d !== undefined
      ) {
        throw new CanonicalIdentityError("identity_v1_config_invalid", 500);
      }
      try {
        keyMap.set(jwk.kid, createPublicKey({ key: jwk, format: "jwk" }));
      } catch {
        throw new CanonicalIdentityError("identity_v1_config_invalid", 500);
      }
    }
    registry.set(issuer, keyMap);
  }
  return registry;
}

function emptyState() {
  return {
    schema: stateSchema,
    bindings: [],
    events: [],
    heads: [],
    quarantine: [],
    subjects: [],
    memberships: [],
    projectAccess: [],
    links: [],
    securityEpochs: [],
    audit: []
  };
}

function validState(value) {
  return isPlainObject(value)
    && value.schema === stateSchema
    && [
      value.bindings, value.events, value.heads,
      value.quarantine, value.subjects, value.memberships, value.projectAccess,
      value.links, value.securityEpochs, value.audit
    ].every(Array.isArray)
    && verifyAudit(value.audit);
}

function appendAudit(state, now, action, value) {
  const previous = state.audit[state.audit.length - 1];
  const entry = {
    sequence: (previous?.sequence || 0) + 1,
    audit_id: `cia_${randomUUID().replace(/-/gu, "")}`,
    occurred_at: new Date(now).toISOString(),
    action,
    event_id: value.event_id || null,
    exchange_id: value.exchange_id || null,
    organization_id: value.organization_id || null,
    subject_hash: sha256(String(value.subject_id || "")),
    previous_hash: previous?.hash || ""
  };
  entry.hash = sha256(stableJson(entry));
  state.audit.push(entry);
}

export function verifyCanonicalIdentityAudit(entries) {
  return verifyAudit(entries);
}

function verifyAudit(entries) {
  if (!Array.isArray(entries)) return false;
  let previousHash = "";
  let sequence = 1;
  for (const entry of entries) {
    if (!isPlainObject(entry) || entry.sequence !== sequence || entry.previous_hash !== previousHash || typeof entry.hash !== "string") return false;
    const { hash, ...unsigned } = entry;
    if (sha256(stableJson(unsigned)) !== hash) return false;
    previousHash = hash;
    sequence += 1;
  }
  return true;
}

function upsert(list, predicate, next) {
  const existing = list.find(predicate);
  if (existing) Object.assign(existing, next);
  else list.push(next);
}

function rejectPiiAndSecrets(value, depth = 0) {
  if (depth > 10) throw new CanonicalIdentityError("identity_event_payload_invalid");
  if (Array.isArray(value)) {
    for (const child of value) rejectPiiAndSecrets(child, depth + 1);
    return;
  }
  if (!isPlainObject(value)) return;
  for (const [key, child] of Object.entries(value)) {
    if (forbiddenNames.has(key.toLowerCase())) throw new CanonicalIdentityError("identity_event_pii_or_secret_forbidden");
    rejectPiiAndSecrets(child, depth + 1);
  }
}

function exactObject(value, expectedKeys, code) {
  if (!isPlainObject(value)) throw new CanonicalIdentityError(code);
  const actual = Object.keys(value).sort();
  const expected = [...expectedKeys].sort();
  if (stableJson(actual) !== stableJson(expected)) throw new CanonicalIdentityError(code);
}

function exactSortedUniqueStrings(value, min, max, allowed, code) {
  const list = boundedArray(value, min, max, code);
  if (list.some((item) => typeof item !== "string" || (allowed && !allowed.has(item)))) throw new CanonicalIdentityError(code);
  requireSortedUnique(list, code);
  return list;
}

function exactUniqueStrings(value, min, max, allowed, code) {
  const list = boundedArray(value, min, max, code);
  if (list.some((item) => typeof item !== "string" || (allowed && !allowed.has(item))) || new Set(list).size !== list.length) {
    throw new CanonicalIdentityError(code);
  }
  return list;
}

function requireSortedUnique(list, code) {
  if (new Set(list).size !== list.length || stableJson(list) !== stableJson([...list].sort())) throw new CanonicalIdentityError(code);
}

function boundedArray(value, min, max, code) {
  if (!Array.isArray(value) || value.length < min || value.length > max) throw new CanonicalIdentityError(code);
  return value;
}

function requirePattern(value, pattern, code) {
  if (typeof value !== "string" || !pattern.test(value)) throw new CanonicalIdentityError(code);
  return value;
}

function cleanClientId(value) {
  if (typeof value !== "string" || !clientIdPattern.test(value)) throw new CanonicalIdentityError("identity_v1_config_invalid", 500);
  return value;
}

function cleanOrigin(value) {
  if (typeof value !== "string" || value.length < 12 || value.length > 255 || !/^https:\/\/[^/?#@]+$/u.test(value)) {
    throw new CanonicalIdentityError("identity_v1_config_invalid", 500);
  }
  try {
    const parsed = new URL(value);
    if (parsed.origin !== value || parsed.username || parsed.password) throw new Error("invalid origin");
    return value;
  } catch {
    throw new CanonicalIdentityError("identity_v1_config_invalid", 500);
  }
}

function cleanIssuer(value) {
  if (typeof value !== "string" || value.length > 255 || !value.startsWith("https://")) return "";
  try {
    return new URL(value).href.replace(/\/$/u, "");
  } catch {
    return "";
  }
}

function normalizeRuntimeMode(value) {
  if (["development", "single-instance", "production"].includes(value)) return value;
  throw new CanonicalIdentityError("identity_v1_config_invalid", 500);
}

function parseCanonicalTime(value, code) {
  if (typeof value !== "string" || !value.endsWith("Z")) throw new CanonicalIdentityError(code);
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) throw new CanonicalIdentityError(code);
  return parsed;
}

function parseJsonBytes(value, code) {
  try {
    return JSON.parse(value.toString("utf8"));
  } catch {
    throw new CanonicalIdentityError(code);
  }
}

function decodeBase64url(value, code, status = 400) {
  if (!base64urlPattern.test(value)) throw new CanonicalIdentityError(code, status);
  const decoded = Buffer.from(value, "base64url");
  if (decoded.toString("base64url") !== value) throw new CanonicalIdentityError(code, status);
  return decoded;
}

function stableJson(value) {
  if (value === null || typeof value === "boolean" || typeof value === "string") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) throw new CanonicalIdentityError("identity_canonical_json_invalid");
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (isPlainObject(value)) return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  throw new CanonicalIdentityError("identity_canonical_json_invalid");
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype;
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function normalizeVendoredContractBytes(value) {
  // The vendored identity bundle contains text-only JSON/Python contracts. Git
  // may materialize those files with CRLF on Windows, while the reviewed pin is
  // intentionally stable across operating systems and hashes canonical LF text.
  return Buffer.from(value.toString("utf8").replace(/\r\n?/gu, "\n"), "utf8");
}

function loadContract(relativePath) {
  return JSON.parse(readFileSync(new URL(relativePath, contractRoot), "utf8"));
}

function readVendoredFiles() {
  const rootPath = fileURLToPath(contractRoot);
  const files = new Map();
  const visit = (directory) => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const absolute = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        visit(absolute);
      } else if (entry.isFile()) {
        const relative = path.relative(rootPath, absolute).split(path.sep).join("/");
        if (relative !== "soty-vendor-pin.json" && relative !== ".gitignore") {
          files.set(relative, readFileSync(absolute));
        }
      }
    }
  };
  visit(rootPath);
  return files;
}
