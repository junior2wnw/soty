#!/usr/bin/env node
import assert from "node:assert/strict";
import { generateKeyPairSync, sign } from "node:crypto";
import { createServer } from "node:http";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import express from "express";
import { attachSotyIdentityAdapterApi } from "../server/soty-identity-adapter-api.js";
import {
  createSotyIdentityAdapterService,
  sotyIdentityAdapterSchema,
  sotyIdentityEventSchema,
  sotyIdentityEventType,
  sotyIdentityLinkExchangeSchema,
  sotyIdentityLinkExchangeType,
  SotyIdentityAdapterError,
  readSotyIdentityAdapterConfig,
  verifySotyIdentityAuditChain
} from "../server/soty-identity-adapter.js";

const fixture = JSON.parse(await readFile(
  new URL("../docs/identity/fixtures/soty-experimental-adapter-conformance.json", import.meta.url),
  "utf8"
));
const root = await mkdtemp(path.join(tmpdir(), "soty-identity-adapter-"));
const issuer = "https://identity.example.test";
const audience = "urn:soty:identity";
const projectId = "soty.online";
const subjectId = "sub_01JIDENTITYTEST0001";
const organizationId = "org_tenant_alpha";
const membershipId = "mem_soty_alpha_0001";
const key = signingKey("key_primary_0001");
const otherKey = signingKey("key_other_000001");
let now = Date.parse("2026-08-12T08:00:00.000Z");

try {
  assert.equal(fixture.canonical, false);
  assert.equal(fixture.upstream.status, "STABLE");
  assert.equal(fixture.upstream.adoption, "direct");
  assert.equal(fixture.upstream.translator_allowed, false);
  assert.equal(
    fixture.canonical_checkpoint.login,
    "oidc-authorization-code-pkce-s256-confidential-bff-local-httponly-session"
  );
  assert.equal(fixture.canonical_checkpoint.custom_login_token_allowed, false);
  assert.equal(fixture.canonical_checkpoint.workflow_max_ttl_seconds, 60);
  assert.equal(fixture.canonical_checkpoint.workflow_at_rest, "sha256-of-code-ascii-only");
  assert.equal(fixture.canonical_checkpoint.workflow_consume, "atomic-single-consume");
  assert.deepEqual(fixture.canonical_checkpoint.workflow_binding, [
    "target_client",
    "target_origin",
    "purpose",
    "state",
    "organization",
    "resources"
  ]);
  assert.equal(fixture.canonical_checkpoint.service_event_serialization, "rfc7515-flattened-jws-json");
  assert.equal(fixture.canonical_checkpoint.service_event_algorithm, "EdDSA");
  assert.equal(fixture.canonical_checkpoint.service_event_key, "Ed25519");
  assert.equal(fixture.canonical_checkpoint.service_event_type, "identity-service-event+jws");
  assert.equal(fixture.canonical_checkpoint.service_event_payload, "rfc8785-jcs-utf8");
  assert.equal(fixture.canonical_checkpoint.conformance_status, "STABLE");
  assert.equal(fixture.wire.adapter_schema, sotyIdentityAdapterSchema);
  assert.equal(fixture.wire.exchange_schema, sotyIdentityLinkExchangeSchema);
  assert.equal(fixture.wire.exchange_type, sotyIdentityLinkExchangeType);
  assert.equal(fixture.wire.event_schema, sotyIdentityEventSchema);
  assert.equal(fixture.wire.event_type, sotyIdentityEventType);
  assert.equal(fixture.wire.max_exchange_ttl_seconds, 60);
  assert.deepEqual(readSotyIdentityAdapterConfig({}), { enabled: false });
  assert.throws(
    () => readSotyIdentityAdapterConfig({ SOTY_IDENTITY_ADAPTER_EXPERIMENTAL_ENABLED: "1" }),
    (error) => error instanceof SotyIdentityAdapterError && error.code === "identity_config_invalid" && error.status === 500
  );
  const productionConfig = readSotyIdentityAdapterConfig({
    SOTY_IDENTITY_ADAPTER_EXPERIMENTAL_ENABLED: "1",
    SOTY_IDENTITY_ADAPTER_ISSUERS_JSON: JSON.stringify([{ issuer, jwks: [key.publicJwk] }])
  });
  assert.equal(productionConfig.runtimeMode, "production");
  const service = createService(path.join(root, "state.json"));
  assert.equal(service.capabilities().production_ready, false);
  assert.equal(service.capabilities().login_supported, false);
  assert.equal(service.readiness().status, "blocked");
  assert.equal(service.readiness().development_selftest_ready, true);
  const exchange = signArtifact(key, sotyIdentityLinkExchangeType, linkClaims("exchange_000000000001"));
  const linked = await service.consumeExchange({ exchange, idempotencyKey: "idem_exchange_000001" });
  assert.equal(linked.project_id, projectId);
  assert.equal(linked.organization_id, organizationId);
  assert.deepEqual(linked.roles, ["project_admin"]);
  assert.equal("subject_id" in linked, false);
  assert.equal("token" in linked, false);

  const idempotent = await service.consumeExchange({ exchange, idempotencyKey: "idem_exchange_000001" });
  assert.deepEqual(idempotent, linked);
  await expectError(
    () => service.consumeExchange({ exchange, idempotencyKey: "idem_exchange_000002" }),
    "identity_exchange_replayed",
    409
  );
  await expectError(
    () => service.consumeExchange({
      exchange: signArtifact(key, sotyIdentityLinkExchangeType, linkClaims("exchange_000000000002")),
      idempotencyKey: "idem_exchange_000001"
    }),
    "identity_idempotency_conflict",
    409
  );
  now += 3 * 60_000;
  const idempotentAfterTokenExpiry = await service.consumeExchange({ exchange, idempotencyKey: "idem_exchange_000001" });
  assert.deepEqual(idempotentAfterTokenExpiry, linked);

  await expectError(
    () => service.consumeExchange({
      exchange: signArtifact(key, sotyIdentityLinkExchangeType, { ...linkClaims("exchange_wrong_aud_01"), aud: "urn:other:identity" }),
      idempotencyKey: "idem_wrong_audience_01"
    }),
    "identity_audience_denied",
    403
  );
  await expectError(
    () => service.consumeExchange({
      exchange: signArtifact(key, sotyIdentityLinkExchangeType, { ...linkClaims("exchange_wrong_tenant"), project_id: "mykosyk" }),
      idempotencyKey: "idem_wrong_tenant_0001"
    }),
    "identity_tenant_denied",
    403
  );
  await expectError(
    () => service.consumeExchange({
      exchange: signArtifact(key, sotyIdentityLinkExchangeType, { ...linkClaims("exchange_pii_0000001"), email: "person@example.test" }),
      idempotencyKey: "idem_pii_forbidden_001"
    }),
    "identity_pii_forbidden",
    400
  );
  await expectError(
    () => service.consumeExchange({
      exchange: signArtifact(key, sotyIdentityLinkExchangeType, { ...linkClaims("exchange_secret_00001"), api_token: "must-not-cross-boundary" }),
      idempotencyKey: "idem_secret_forbidden01"
    }),
    "identity_secret_forbidden",
    400
  );
  await expectError(
    () => service.consumeExchange({
      exchange: signArtifact(otherKey, sotyIdentityLinkExchangeType, linkClaims("exchange_bad_signature"), key.kid),
      idempotencyKey: "idem_bad_signature_001"
    }),
    "identity_signature_invalid",
    401
  );
  await expectError(
    () => service.consumeExchange({
      exchange: signArtifact(key, sotyIdentityLinkExchangeType, linkClaims("exchange_unknown_key_01"), "key_unknown_000001"),
      idempotencyKey: "idem_unknown_key_000001"
    }),
    "identity_issuer_unknown",
    401
  );
  await expectError(
    () => service.consumeExchange({
      exchange: signArtifact(key, sotyIdentityLinkExchangeType, { ...linkClaims("exchange_unknown_issuer"), iss: "https://unknown-identity.example.test" }),
      idempotencyKey: "idem_unknown_issuer_001"
    }),
    "identity_issuer_unknown",
    401
  );
  await expectError(
    () => service.consumeExchange({
      exchange: replaceArtifactHeader(
        signArtifact(key, sotyIdentityLinkExchangeType, linkClaims("exchange_wrong_alg_001")),
        { alg: "HS256", kid: key.kid, typ: sotyIdentityLinkExchangeType }
      ),
      idempotencyKey: "idem_wrong_algorithm_001"
    }),
    "identity_artifact_invalid",
    400
  );
  await expectError(
    () => service.consumeExchange({
      exchange: signArtifact(key, sotyIdentityEventType, linkClaims("exchange_wrong_type_001")),
      idempotencyKey: "idem_wrong_type_000001"
    }),
    "identity_artifact_invalid",
    400
  );
  await expectError(
    () => service.consumeExchange({
      exchange: signArtifact(key, sotyIdentityLinkExchangeType, { ...linkClaims("exchange_future_nbf_001"), nbf: seconds() + 90 }),
      idempotencyKey: "idem_future_nbf_000001"
    }),
    "identity_artifact_not_yet_valid",
    401
  );
  await expectError(
    () => service.consumeExchange({
      exchange: signArtifact(key, sotyIdentityLinkExchangeType, { ...linkClaims("exchange_long_ttl_01"), exp: seconds() + 61 }),
      idempotencyKey: "idem_long_ttl_000001"
    }),
    "identity_artifact_ttl_invalid",
    401
  );

  const enrollArtifact = signArtifact(key, sotyIdentityEventType, eventClaims("identity.device.enrolled", "event_enroll_device_001", {
    device_id: "device_soty_primary_01",
    key_thumbprint: `sha256:${"A".repeat(43)}`
  }));
  const enroll = await service.applyEvent({ event: enrollArtifact, idempotencyKey: "idem_event_enroll_device_001" });
  assert.equal(enroll.applied, true);
  const enrollAgain = await service.applyEvent({ event: enrollArtifact, idempotencyKey: "idem_event_enroll_device_001" });
  assert.deepEqual(enrollAgain, enroll);
  await expectError(
    () => service.applyEvent({ event: enrollArtifact, idempotencyKey: "idem_event_enroll_replay01" }),
    "identity_event_replayed",
    409
  );
  await expectError(
    () => service.applyEvent({
      event: signArtifact(key, sotyIdentityEventType, eventClaims("identity.device.enrolled", "event_unknown_field_001", {
        device_id: "device_soty_unknown_01",
        key_thumbprint: `sha256:${"D".repeat(43)}`,
        unexpected: true
      })),
      idempotencyKey: "idem_unknown_field_0001"
    }),
    "identity_event_invalid",
    400
  );
  await expectError(
    () => service.applyEvent({
      event: signArtifact(key, sotyIdentityEventType, eventClaims("identity.device.unknown", "event_unknown_type_0001", {})),
      idempotencyKey: "idem_unknown_event_0001"
    }),
    "identity_event_invalid",
    400
  );

  await apply(service, "identity.device.recovery_started", "event_recovery_start_01", {
    device_id: "device_soty_primary_01",
    recovery_id: "recovery_soty_0000001"
  });
  await expectError(
    () => apply(service, "identity.device.recovered", "event_recovery_wrong_01", {
      old_device_id: "device_soty_primary_01",
      new_device_id: "device_soty_secondary_01",
      recovery_id: "recovery_soty_wrong_01",
      key_thumbprint: `sha256:${"B".repeat(43)}`
    }),
    "identity_recovery_invalid",
    409
  );
  await apply(service, "identity.device.recovered", "event_recovery_finish_01", {
    old_device_id: "device_soty_primary_01",
    new_device_id: "device_soty_secondary_01",
    recovery_id: "recovery_soty_0000001",
    key_thumbprint: `sha256:${"B".repeat(43)}`
  });

  await expectError(
    () => service.applyEvent({
      event: signArtifact(key, sotyIdentityEventType, eventClaims("identity.device.revoked", "event_cross_tenant_001", {
        device_id: "device_soty_secondary_01"
      }, { organization_id: "org_tenant_beta" })),
      idempotencyKey: "idem_cross_tenant_0001"
    }),
    "identity_binding_not_found",
    404
  );

  await apply(service, "identity.device.revoked", "event_revoke_device_01", { device_id: "device_soty_secondary_01" });
  await apply(service, "identity.link.unlinked", "event_unlink_binding_01", {});
  await expectError(
    () => apply(service, "identity.device.enrolled", "event_after_unlink_001", {
      device_id: "device_soty_after_unlink",
      key_thumbprint: `sha256:${"C".repeat(43)}`
    }),
    "identity_binding_inactive",
    409
  );
  const relink = signArtifact(key, sotyIdentityLinkExchangeType, linkClaims("exchange_relink_00000001"));
  await service.consumeExchange({ exchange: relink, idempotencyKey: "idem_exchange_relink_01" });
  await apply(service, "identity.membership.upserted", "event_membership_update1", {
    roles: ["project_admin"],
    scopes: ["identity:link", "membership:read"]
  });
  await apply(service, "identity.membership.revoked", "event_membership_revoke1", {});

  const snapshot = await service.snapshot();
  assert.equal(snapshot.bindings.length, 1);
  assert.equal(snapshot.bindings[0].subjectId, subjectId);
  assert.equal(snapshot.bindings[0].projectId, projectId);
  assert.equal(snapshot.bindings[0].devices.find((item) => item.deviceId === "device_soty_primary_01").status, "revoked");
  assert.equal(snapshot.bindings[0].devices.find((item) => item.deviceId === "device_soty_secondary_01").recoveryGeneration, 1);
  assert.equal(snapshot.bindings[0].linkStatus, "unlinked");
  assert.equal(verifySotyIdentityAuditChain(snapshot.audit), true);
  assert.ok(snapshot.audit.every((entry) => !Object.hasOwn(entry, "subjectId")));

  const betaExchange = signArtifact(key, sotyIdentityLinkExchangeType, {
    ...linkClaims("exchange_second_org_001"),
    organization_id: "org_tenant_beta",
    membership_id: "mem_soty_beta_00001",
    roles: ["member"]
  });
  const betaLinked = await service.consumeExchange({ exchange: betaExchange, idempotencyKey: "idem_second_org_000001" });
  assert.equal(betaLinked.organization_id, "org_tenant_beta");
  const multiTenantSnapshot = await service.snapshot();
  assert.equal(multiTenantSnapshot.bindings.length, 2);
  assert.equal(multiTenantSnapshot.bindings.find((item) => item.organizationId === organizationId).linkStatus, "unlinked");
  assert.equal(multiTenantSnapshot.bindings.find((item) => item.organizationId === "org_tenant_beta").linkStatus, "linked");
  assert.notEqual(
    multiTenantSnapshot.bindings.find((item) => item.organizationId === organizationId).bindingId,
    multiTenantSnapshot.bindings.find((item) => item.organizationId === "org_tenant_beta").bindingId
  );

  const persistedText = await readFile(path.join(root, "state.json"), "utf8");
  assert.equal(persistedText.includes(exchange), false);
  assert.equal(persistedText.includes("person@example.test"), false);

  now += 10 * 60_000;
  await expectError(
    () => service.consumeExchange({
      exchange: signArtifact(key, sotyIdentityLinkExchangeType, { ...linkClaims("exchange_expired_0001", now - 20 * 60_000), exp: Math.floor((now - 10 * 60_000) / 1000) }),
      idempotencyKey: "idem_expired_exchange1"
    }),
    "identity_artifact_expired",
    401
  );

  const corruptDir = path.join(root, "corrupt");
  await mkdir(corruptDir);
  await writeFile(path.join(corruptDir, "state.json"), "{broken", "utf8");
  const corruptService = createService(path.join(corruptDir, "state.json"));
  await expectError(
    () => corruptService.consumeExchange({
      exchange: signArtifact(key, sotyIdentityLinkExchangeType, linkClaims("exchange_corrupt_store")),
      idempotencyKey: "idem_corrupt_store_001"
    }),
    "identity_store_unavailable",
    503
  );

  const productionService = createService(path.join(root, "production-state.json"), "production");
  assert.equal(productionService.readiness().ok, false);
  await expectError(
    () => productionService.consumeExchange({ exchange, idempotencyKey: "idem_production_blocked_01" }),
    fixture.expected_failures.production_file_store,
    503
  );

  await apiSelftest();
  process.stdout.write("soty-identity-adapter:selftest:ok\n");
} finally {
  await rm(root, { recursive: true, force: true });
}

function createService(filePath, runtimeMode = "development") {
  return createSotyIdentityAdapterService({
    filePath,
    projectId,
    audience,
    issuers: [{ issuer, jwks: [key.publicJwk] }],
    runtimeMode,
    now: () => now,
    clockSkewSec: 0,
    replayRetentionMs: 10 * 60_000
  });
}

async function apply(service, eventType, eventId, data) {
  const event = signArtifact(key, sotyIdentityEventType, eventClaims(eventType, eventId, data));
  return await service.applyEvent({ event, idempotencyKey: `idem_${eventId}` });
}

function linkClaims(jti, at = now) {
  const iat = Math.floor(at / 1000);
  return {
    schema: sotyIdentityLinkExchangeSchema,
    iss: issuer,
    aud: audience,
    sub: subjectId,
    jti,
    iat,
    nbf: iat,
    exp: iat + 45,
    organization_id: organizationId,
    project_id: projectId,
    membership_id: membershipId,
    roles: ["project_admin"],
    scope: ["identity:link", "identity:unlink", "membership:read", "device:enroll", "device:revoke", "device:recover"]
  };
}

function eventClaims(eventType, jti, data, overrides = {}) {
  const iat = seconds();
  return {
    schema: sotyIdentityEventSchema,
    iss: issuer,
    aud: audience,
    sub: subjectId,
    jti,
    iat,
    nbf: iat,
    exp: iat + 45,
    organization_id: organizationId,
    project_id: projectId,
    membership_id: membershipId,
    scope: ["identity:event"],
    event_type: eventType,
    data,
    ...overrides
  };
}

function signingKey(kid) {
  const pair = generateKeyPairSync("ec", { namedCurve: "P-256" });
  return {
    kid,
    privateKey: pair.privateKey,
    publicJwk: {
      ...pair.publicKey.export({ format: "jwk" }),
      kid,
      alg: "ES256",
      use: "sig",
      key_ops: ["verify"]
    }
  };
}

function signArtifact(signing, typ, claims, kid = signing.kid) {
  const header = encodeJson({ alg: "ES256", kid, typ });
  const payload = encodeJson(claims);
  const signature = sign("sha256", Buffer.from(`${header}.${payload}`, "ascii"), {
    key: signing.privateKey,
    dsaEncoding: "ieee-p1363"
  }).toString("base64url");
  return `${header}.${payload}.${signature}`;
}

function replaceArtifactHeader(artifact, header) {
  const parts = artifact.split(".");
  parts[0] = encodeJson(header);
  return parts.join(".");
}

function encodeJson(value) {
  return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function seconds() {
  return Math.floor(now / 1000);
}

async function expectError(callback, code, status) {
  await assert.rejects(callback, (error) => {
    assert.ok(error instanceof SotyIdentityAdapterError);
    assert.equal(error.code, code);
    assert.equal(error.status, status);
    return true;
  });
}

async function apiSelftest() {
  const disabled = express();
  assert.equal(attachSotyIdentityAdapterApi(disabled, { config: { enabled: false } }).enabled, false);
  const disabledServer = createServer(disabled);
  await new Promise((resolveListen, reject) => {
    disabledServer.once("error", reject);
    disabledServer.listen(0, "127.0.0.1", resolveListen);
  });
  try {
    const address = disabledServer.address();
    const response = await fetch(`http://127.0.0.1:${address.port}/api/identity-adapter/experimental/capabilities`);
    assert.equal(response.status, 404);
    assert.deepEqual(await response.json(), { ok: false, error: "identity_adapter_disabled" });
    const withdrawn = await fetch(`http://127.0.0.1:${address.port}/api/identity-compat/v1/capabilities`);
    assert.equal(withdrawn.status, 410);
    assert.deepEqual(await withdrawn.json(), { ok: false, error: "identity_compat_contract_withdrawn" });
  } finally {
    await new Promise((resolveClose) => disabledServer.close(resolveClose));
  }

  const app = express();
  const attached = attachSotyIdentityAdapterApi(app, {
    dataDir: path.join(root, "api"),
    config: {
      enabled: true,
      projectId,
      audience,
      issuers: [{ issuer, jwks: [key.publicJwk] }],
      now: () => now,
      clockSkewSec: 0,
      replayRetentionMs: 10 * 60_000
    }
  });
  assert.equal(attached.enabled, true);
  const server = createServer(app);
  await new Promise((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolveListen);
  });
  try {
    const address = server.address();
    const base = `http://127.0.0.1:${address.port}`;
    const capabilities = await fetch(`${base}/api/identity-adapter/experimental/capabilities`);
    assert.equal(capabilities.status, 200);
    assert.equal(capabilities.headers.get("cache-control"), "no-store");
    const capabilityBody = await capabilities.json();
    assert.equal(capabilityBody.role, "soty-relying-party-adapter");
    assert.equal(capabilityBody.canonical_shared_contract, false);
    assert.equal(capabilityBody.production_ready, false);
    assert.equal(capabilityBody.login_supported, false);
    assert.equal(capabilityBody.max_exchange_ttl_seconds, 60);
    assert.equal(capabilityBody.workflow_ingress, "disabled-legacy-noncanonical");
    assert.equal(capabilityBody.event_ingress, "disabled-legacy-noncanonical");

    const readiness = await fetch(`${base}/api/identity-adapter/experimental/readiness`);
    assert.equal(readiness.status, 503);
    const readinessBody = await readiness.json();
    assert.equal(readinessBody.production_ready, false);
    assert.equal(readinessBody.development_selftest_ready, true);

    const artifact = signArtifact(key, sotyIdentityLinkExchangeType, linkClaims("exchange_api_000000001"));
    const browserAttempt = await fetch(`${base}/api/identity-adapter/experimental/exchanges/consume`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "idem_api_browser_00001",
        Origin: "https://soty.example"
      },
      body: JSON.stringify({ exchange: artifact })
    });
    assert.equal(browserAttempt.status, 403);
    assert.deepEqual(await browserAttempt.json(), { ok: false, error: fixture.expected_failures.browser_exchange });

    const pendingExchange = await fetch(`${base}/api/identity-adapter/experimental/exchanges/consume`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Idempotency-Key": "idem_api_exchange_0001" },
      body: JSON.stringify({ exchange: artifact })
    });
    assert.equal(pendingExchange.status, 503);
    assert.deepEqual(await pendingExchange.json(), { ok: false, error: fixture.expected_failures.canonical_workflow_pending });

    const malformed = await fetch(`${base}/api/identity-adapter/experimental/exchanges/consume`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Idempotency-Key": "idem_api_malformed_001" },
      body: "{broken"
    });
    assert.equal(malformed.status, 400);
    assert.deepEqual(await malformed.json(), { ok: false, error: "identity_json_invalid" });

    const oversized = await fetch(`${base}/api/identity-adapter/experimental/exchanges/consume`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Idempotency-Key": "idem_api_oversized_001" },
      body: JSON.stringify({ exchange: "x".repeat(40_000) })
    });
    assert.equal(oversized.status, 400);
    assert.deepEqual(await oversized.json(), { ok: false, error: "identity_json_invalid" });

    const eventIngress = await fetch(`${base}/api/identity-adapter/experimental/events/apply`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Idempotency-Key": "idem_api_event_pending_01" },
      body: JSON.stringify({ event: "opaque" })
    });
    assert.equal(eventIngress.status, 503);
    assert.deepEqual(await eventIngress.json(), { ok: false, error: fixture.expected_failures.canonical_events_pending });
  } finally {
    await new Promise((resolveClose) => server.close(resolveClose));
  }
}
