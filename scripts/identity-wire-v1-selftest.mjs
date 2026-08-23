#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  createHash,
  generateKeyPairSync,
  sign,
  timingSafeEqual
} from "node:crypto";
import { createServer } from "node:http";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { existsSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import express from "express";
import { attachCanonicalIdentityApi } from "../server/identity-wire-v1-api.js";
import {
  CanonicalIdentityError,
  createCanonicalIdentityService,
  createIdentityControlPlaneRedeemClient,
  getCanonicalAuthorizationCatalog,
  getCanonicalBrowserLoginProfile,
  getCanonicalIdentityManifest,
  getCanonicalIdentityVendorPin,
  readCanonicalIdentityConfig,
  validateAuthorizationCatalog,
  validateBrowserLoginProfile,
  validateCanonicalEventPayload,
  validateCanonicalManifest,
  validateWorkflowExchangeRecord,
  validateWorkflowRedeemRequest,
  verifyVendoredIdentityBundle,
  verifyCanonicalIdentityAudit,
  verifyCanonicalServiceEvent
} from "../server/identity-wire-v1.js";

const contractRoot = new URL("../contracts/identity/v1/", import.meta.url);
const root = await mkdtemp(path.join(tmpdir(), "soty-identity-wire-v1-"));
const manifest = getCanonicalIdentityManifest();
const catalog = getCanonicalAuthorizationCatalog();
const loginProfile = getCanonicalBrowserLoginProfile();
const fixtureRoot = "tests/fixtures/identity-wire-v1";
const positiveEvents = loadJson(`${fixtureRoot}/service-event-payloads-positive.json`);
const testEnvelope = loadJson(`${fixtureRoot}/service-event-jws-positive.json`);
const testPublicKey = loadJson(`${fixtureRoot}/service-event-test-public-key.json`);
const workflowRecord = loadJson(`${fixtureRoot}/workflow-exchange-positive.json`);
const testIssuers = [{ issuer: "https://identity.example", keys: [testPublicKey] }];
const vectorResults = new Map();

try {
  await vendorPinSelftest();
  assert.deepEqual(readCanonicalIdentityConfig({}), { enabled: false });
  const defaultEnabled = readCanonicalIdentityConfig({
    SOTY_IDENTITY_V1_ENABLED: "1",
    SOTY_IDENTITY_V1_ISSUERS_JSON: JSON.stringify(testIssuers)
  });
  assert.equal(defaultEnabled.runtimeMode, "production");

  await vector("authorization_catalog_positive", () => {
    validateCanonicalManifest(manifest);
    validateAuthorizationCatalog(catalog);
    const references = new Set([
      manifest.browser_login.schema_ref,
      manifest.browser_login.profile_ref,
      manifest.workflow_exchange.schema_ref,
      manifest.service_event.envelope_schema_ref,
      manifest.service_event.protected_header_schema_ref,
      manifest.service_event.payload_schema_ref,
      manifest.service_event.test_public_key_ref,
      manifest.authorization_catalog.schema_ref,
      manifest.authorization_catalog.catalog_ref,
      ...manifest.vectors.map((item) => item.path)
    ]);
    for (const reference of references) assert.equal(existsSync(resolveManifestPath(reference)), true, reference);
  });

  await vector("authority_model_negative", async () => {
    for (const item of loadJson(`${fixtureRoot}/authority-model-negative.json`)) {
      const candidate = structuredClone(manifest);
      Object.assign(candidate.authority_model, item.patch);
      await expectCanonicalError(() => validateCanonicalManifest(candidate), "identity_v1_manifest_invalid");
    }
  });

  await vector("browser_login_positive", () => {
    const fixture = loadJson(`${fixtureRoot}/browser-login-positive.json`);
    assert.deepEqual(fixture, loginProfile);
    validateBrowserLoginProfile(fixture);
  });

  await vector("browser_login_negative", async () => {
    for (const item of loadJson(`${fixtureRoot}/browser-login-negative.json`)) {
      await expectCanonicalError(() => validateBrowserLoginProfile(item.value), "identity_login_profile_invalid");
    }
  });

  await vector("workflow_exchange_positive", () => {
    validateWorkflowExchangeRecord(workflowRecord);
    const code = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    assert.equal(workflowRecord.code_hash, `sha256:${sha256(code)}`);
    assert.equal("code" in workflowRecord, false);
  });

  await vector("workflow_exchange_negative", async () => {
    for (const item of loadJson(`${fixtureRoot}/workflow-exchange-negative.json`)) {
      const candidate = structuredClone(workflowRecord);
      Object.assign(candidate, item.patch);
      await expectCanonicalError(() => validateWorkflowExchangeRecord(candidate));
    }
  });

  await vector("service_event_payloads_positive", () => {
    assert.deepEqual(
      new Set(positiveEvents.map((item) => item.event_type)),
      new Set([
        "identity.subject.changed.v1",
        "identity.membership.changed.v1",
        "identity.project-access.changed.v1",
        "identity.subject.unlinked.v1",
        "identity.security-epoch.advanced.v1"
      ])
    );
    for (const event of positiveEvents) validateCanonicalEventPayload(event, { consumerClientId: "ufanovostroyka" });
  });

  await vector("service_event_negative", async () => {
    for (const item of loadJson(`${fixtureRoot}/service-event-negative.json`)) {
      const candidate = structuredClone(positiveEvents[item.base_index]);
      Object.assign(candidate, item.patch || {});
      Object.assign(candidate.data, item.data_patch || {});
      await expectCanonicalError(() => validateCanonicalEventPayload(candidate, { consumerClientId: "ufanovostroyka" }));
    }
  });

  await vector("service_event_jws_positive", () => {
    const verified = verifyCanonicalServiceEvent(testEnvelope, {
      consumerClientId: "ufanovostroyka",
      issuers: testIssuers
    });
    assert.deepEqual(verified.payload, positiveEvents[1]);
    const tampered = { ...testEnvelope, signature: `${testEnvelope.signature[0] === "A" ? "B" : "A"}${testEnvelope.signature.slice(1)}` };
    assert.throws(
      () => verifyCanonicalServiceEvent(tampered, { consumerClientId: "ufanovostroyka", issuers: testIssuers }),
      (error) => error instanceof CanonicalIdentityError && error.code === "identity_event_signature_invalid"
    );
  });

  await vector("authorization_catalog_negative", async () => {
    for (const item of loadJson(`${fixtureRoot}/authorization-catalog-negative.json`)) {
      const candidate = structuredClone(catalog);
      if (item.scope_id) {
        Object.assign(candidate.scopes.find((scope) => scope.id === item.scope_id), item.patch);
      } else {
        const role = candidate.roles.find((value) => value.id === item.role_id);
        role.scopes.push(item.append_scope);
        role.scopes.sort();
      }
      await expectCanonicalError(() => validateAuthorizationCatalog(candidate), "identity_authorization_catalog_invalid");
    }
  });

  await vector("legacy_soty_es256_flat", async () => {
    const legacy = loadJson(`${fixtureRoot}/legacy-soty-es256-flat.json`);
    assert.equal(legacy.expected_canonical_v1, false);
    await expectCanonicalError(
      () => verifyCanonicalServiceEvent(legacy.compact, { consumerClientId: "soty.online", issuers: testIssuers }),
      "identity_event_envelope_invalid"
    );
  });

  await vector("legacy_tavysh_eddsa_nested", async () => {
    const legacy = loadJson(`${fixtureRoot}/legacy-tavysh-eddsa-nested.json`);
    assert.equal(legacy.expected_canonical_v1, false);
    await expectCanonicalError(
      () => verifyCanonicalServiceEvent(legacy.compact, { consumerClientId: "soty.online", issuers: testIssuers }),
      "identity_event_envelope_invalid"
    );
  });

  assert.deepEqual(
    [...vectorResults.keys()].sort(),
    manifest.vectors.map((item) => item.id).sort()
  );
  assert.ok([...vectorResults.values()].every((value) => value === "PASS"));

  await roleScopeMappingSelftest();
  await workflowClientSelftest();
  await eventConsumerSelftest();
  await apiSelftest();

  for (const item of manifest.vectors) process.stdout.write(`identity-wire-v1:${item.id}:${vectorResults.get(item.id)}\n`);
  process.stdout.write("identity-wire-v1:selftest:ok\n");
} finally {
  await rm(root, { recursive: true, force: true });
}

async function roleScopeMappingSelftest() {
  const roleIds = catalog.roles.map((item) => item.id);
  const scopeIds = catalog.scopes.map((item) => item.id);
  assert.deepEqual(roleIds, [...roleIds].sort());
  assert.deepEqual(scopeIds, [...scopeIds].sort());
  assert.equal(roleIds.length, 12);
  assert.equal(scopeIds.length, 23);
  const membership = structuredClone(positiveEvents[1]);
  membership.data.roles = ["member"];
  await expectCanonicalError(
    () => validateCanonicalEventPayload(membership, { consumerClientId: "ufanovostroyka" }),
    "identity_event_authorization_invalid"
  );
  membership.data.roles = ["feed_manager"];
  membership.data.scopes = ["membership:read"];
  await expectCanonicalError(
    () => validateCanonicalEventPayload(membership, { consumerClientId: "ufanovostroyka" }),
    "identity_event_authorization_invalid"
  );
  membership.data.roles = ["platform_superadmin"];
  membership.data.scopes = ["security:revoke-global"];
  await expectCanonicalError(
    () => validateCanonicalEventPayload(membership, { consumerClientId: "ufanovostroyka" }),
    "identity_event_authorization_invalid"
  );
}

async function vendorPinSelftest() {
  const pin = getCanonicalIdentityVendorPin();
  assert.equal(pin.canonical_wire_version, 1);
  assert.equal(pin.contract_file_count, 23);
  assert.equal(pin.synced_at_is_freshness_or_trust_claim, false);
  assert.equal(pin.auto_merge, false);
  const files = new Map(pin.files.map((item) => [
    item.path,
    readFileSync(new URL(item.path, contractRoot))
  ]));
  const verified = verifyVendoredIdentityBundle({ files });
  assert.equal(verified.upstream_manifest_sha256, pin.upstream_manifest.sha256);
  const missing = new Map(files);
  missing.delete(pin.files[0].path);
  await expectCanonicalError(
    () => verifyVendoredIdentityBundle({ files: missing }),
    "identity_vendor_file_set_mismatch"
  );
  const extra = new Map(files);
  extra.set("unexpected-contract.json", Buffer.from("{}", "utf8"));
  await expectCanonicalError(
    () => verifyVendoredIdentityBundle({ files: extra }),
    "identity_vendor_file_set_mismatch"
  );
  const changed = new Map(files);
  changed.set(pin.files[0].path, Buffer.concat([changed.get(pin.files[0].path), Buffer.from("\n")]));
  await expectCanonicalError(
    () => verifyVendoredIdentityBundle({ files: changed }),
    "identity_vendor_digest_mismatch"
  );
}

async function workflowClientSelftest() {
  const fake = createWorkflowControlPlaneFake(Date.parse("2026-08-12T10:00:30Z"));
  const code = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
  fake.seed(workflowRecord);
  const client = createIdentityControlPlaneRedeemClient({
    authenticatedServerToServer: true,
    redeemTransport: (request) => fake.redeem(request),
    validateResponse: (value) => value?.ok === true && subjectId(value.subject_id) && organizationId(value.organization_id)
  });
  assert.deepEqual(client.capabilities(), {
    role: "authenticated-control-plane-redeem-client",
    local_exchange_issuer: false,
    local_exchange_store: false,
    local_atomic_consumer: false,
    requires_existing_oidc_bff_session: true
  });
  const request = {
    code,
    target_client_id: workflowRecord.target_client_id,
    target_origin: workflowRecord.target_origin,
    purpose: workflowRecord.purpose,
    state: workflowRecord.state,
    organization_id: workflowRecord.organization_id,
    resources: workflowRecord.resources
  };
  validateWorkflowRedeemRequest(request);
  await expectCanonicalError(
    () => client.redeemForExistingSession(request, { hasOidcBffSession: false }),
    "identity_oidc_bff_session_required"
  );
  const redeemed = await client.redeemForExistingSession(request, { hasOidcBffSession: true });
  assert.equal(redeemed.subject_id, workflowRecord.subject_id);
  await expectCanonicalError(
    () => client.redeemForExistingSession(request, { hasOidcBffSession: true }),
    "identity_control_plane_unavailable"
  );
  assert.equal(JSON.stringify(fake.snapshot()).includes(code), false);

  const secondCode = Buffer.alloc(32, 7).toString("base64url");
  const secondRecord = {
    ...structuredClone(workflowRecord),
    exchange_id: "xchg_parallel_01K2A0",
    code_hash: `sha256:${sha256(secondCode)}`
  };
  fake.seed(secondRecord);
  const secondRequest = { ...request, code: secondCode };
  const outcomes = await Promise.allSettled([
    client.redeemForExistingSession(secondRequest, { hasOidcBffSession: true }),
    client.redeemForExistingSession(secondRequest, { hasOidcBffSession: true })
  ]);
  assert.equal(outcomes.filter((item) => item.status === "fulfilled").length, 1);
  assert.equal(outcomes.filter((item) => item.status === "rejected").length, 1);
}

async function eventConsumerSelftest() {
  const signing = eventSigningKey();
  const service = canonicalService("event-state.json", signing.publicJwk);
  await service.registerLocalBinding({
    issuer: "https://identity.example",
    subject_id: "sub_01K2A0EXAMPLE",
    organization_id: "org_01K2A0EXAMPLE",
    resources: [
      { kind: "organization", id: "org_01K2A0EXAMPLE" },
      { kind: "project", id: "prj_01K2A0EXAMPLE" }
    ]
  });
  const firstPayload = { ...structuredClone(positiveEvents[1]), audience: "soty.online" };
  const firstEnvelope = signEvent(signing, firstPayload);
  assert.equal((await service.applyServiceEvent(firstEnvelope)).status, "applied");
  assert.equal((await service.applyServiceEvent(firstEnvelope)).status, "idempotent");
  const conflict = signEvent(signing, { ...firstPayload, occurred_at: "2026-08-12T10:03:01Z" });
  assert.equal((await service.applyServiceEvent(conflict)).status, "security_incident");

  const gapPayload = {
    ...firstPayload,
    event_id: "evt_membership_gap_01K2A0",
    revision: 3,
    previous_event_id: firstPayload.event_id
  };
  const gapEnvelope = signEvent(signing, gapPayload);
  assert.equal((await service.applyServiceEvent(gapEnvelope)).status, "quarantined");
  const gapRetry = await service.applyServiceEvent(gapEnvelope);
  assert.equal(gapRetry.status, "quarantined");
  assert.equal(gapRetry.idempotent, true);
  const secondPayload = {
    ...firstPayload,
    event_id: "evt_membership_02K2A0",
    revision: 2,
    previous_event_id: firstPayload.event_id
  };
  assert.equal((await service.applyServiceEvent(signEvent(signing, secondPayload))).status, "applied");
  const badPrevious = {
    ...secondPayload,
    event_id: "evt_membership_03K2A0",
    revision: 3,
    previous_event_id: firstPayload.event_id
  };
  await expectCanonicalError(
    () => service.applyServiceEvent(signEvent(signing, badPrevious)),
    "identity_event_predecessor_mismatch"
  );

  const otherTenant = {
    ...structuredClone(firstPayload),
    event_id: "evt_membership_other_org",
    organization_id: "org_01K2OTHERORG",
    aggregate_id: "mem_01K2OTHERORG",
    data: { ...firstPayload.data, membership_id: "mem_01K2OTHERORG" }
  };
  await expectCanonicalError(
    () => service.applyServiceEvent(signEvent(signing, otherTenant)),
    "identity_event_binding_not_found"
  );
  const otherIssuerSigning = eventSigningKey("key_other_test_01K2A0");
  const crossIssuerService = createCanonicalIdentityService({
    filePath: path.join(root, "other-issuer-state.json"),
    runtimeMode: "development",
    clientId: "soty.online",
    targetOrigin: "https://soty.online",
    issuers: [
      { issuer: "https://identity.example", keys: [signing.publicJwk] },
      { issuer: "https://identity.other.example", keys: [otherIssuerSigning.publicJwk] }
    ]
  });
  await crossIssuerService.registerLocalBinding({
    issuer: "https://identity.example",
    subject_id: "sub_01K2A0EXAMPLE",
    organization_id: "org_01K2A0EXAMPLE",
    resources: [{ kind: "organization", id: "org_01K2A0EXAMPLE" }]
  });
  const otherIssuerEvent = {
    ...structuredClone(firstPayload),
    issuer: "https://identity.other.example",
    event_id: "evt_membership_other_issuer",
    aggregate_id: "mem_01K2OTHERISSUER",
    data: { ...firstPayload.data, membership_id: "mem_01K2OTHERISSUER" }
  };
  await expectCanonicalError(
    () => crossIssuerService.applyServiceEvent(signEvent(otherIssuerSigning, otherIssuerEvent)),
    "identity_event_binding_not_found"
  );

  const epochPayload = { ...structuredClone(positiveEvents[4]), audience: "soty.online" };
  assert.equal((await service.applyServiceEvent(signEvent(signing, epochPayload))).status, "applied");
  const staleEpoch = {
    ...epochPayload,
    event_id: "evt_epoch_02K2A0",
    revision: 2,
    previous_event_id: epochPayload.event_id,
    data: { ...epochPayload.data, security_epoch: 1 }
  };
  await expectCanonicalError(
    () => service.applyServiceEvent(signEvent(signing, staleEpoch)),
    "identity_security_epoch_stale"
  );

  const unlinkPayload = { ...structuredClone(positiveEvents[3]), audience: "soty.online" };
  assert.equal((await service.applyServiceEvent(signEvent(signing, unlinkPayload))).status, "applied");
  const snapshot = await service.snapshot();
  assert.equal(snapshot.bindings[0].status, "unlinked");
  assert.equal(snapshot.quarantine.length, 1);
  assert.equal(snapshot.memberships[0].roles[0], "feed_manager");
  assert.equal(verifyCanonicalIdentityAudit(snapshot.audit), true);
  const persisted = await readFile(path.join(root, "event-state.json"), "utf8");
  assert.equal(persisted.includes(firstEnvelope.signature), false);
  assert.equal(persisted.includes("person@example.test"), false);

  const production = canonicalService("production-state.json", signing.publicJwk, "production");
  await expectCanonicalError(
    () => production.applyServiceEvent(firstEnvelope),
    "identity_v1_production_store_required"
  );
  const corruptDir = path.join(root, "corrupt");
  await mkdir(corruptDir);
  await writeFile(path.join(corruptDir, "state.json"), "{broken", "utf8");
  const corrupt = createCanonicalIdentityService({
    filePath: path.join(corruptDir, "state.json"),
    runtimeMode: "development",
    clientId: "soty.online",
    targetOrigin: "https://soty.online",
    issuers: [{ issuer: "https://identity.example", keys: [signing.publicJwk] }]
  });
  await expectCanonicalError(() => corrupt.applyServiceEvent(firstEnvelope), "identity_v1_store_unavailable");
}

async function apiSelftest() {
  const disabledApp = express();
  attachCanonicalIdentityApi(disabledApp, { config: { enabled: false } });
  await withServer(disabledApp, async (base) => {
    const response = await fetch(`${base}/api/identity/v1/capabilities`);
    assert.equal(response.status, 404);
    assert.deepEqual(await response.json(), { ok: false, error: "identity_v1_disabled" });
  });

  const signing = eventSigningKey();
  const app = express();
  const attached = attachCanonicalIdentityApi(app, {
    dataDir: path.join(root, "api"),
    config: {
      enabled: true,
      runtimeMode: "development",
      clientId: "soty.online",
      targetOrigin: "https://soty.online",
      issuers: [{ issuer: "https://identity.example", keys: [signing.publicJwk] }]
    },
    authenticateServiceEvent: (req) => req.headers.authorization === "Bearer conformance-test"
  });
  await attached.service.registerLocalBinding({
    issuer: "https://identity.example",
    subject_id: "sub_01K2A0EXAMPLE",
    organization_id: "org_01K2A0EXAMPLE",
    resources: [
      { kind: "organization", id: "org_01K2A0EXAMPLE" },
      { kind: "project", id: "prj_01K2A0EXAMPLE" }
    ]
  });
  const envelope = signEvent(signing, { ...structuredClone(positiveEvents[1]), audience: "soty.online" });
  await withServer(app, async (base) => {
    const capabilities = await fetch(`${base}/api/identity/v1/capabilities`);
    assert.equal(capabilities.status, 200);
    const capabilityBody = await capabilities.json();
    assert.equal(capabilityBody.integration_status, "disabled-adapter");
    assert.equal(capabilityBody.sso_available, false);
    assert.equal(capabilityBody.workflow_exchange.local_store, false);
    assert.equal(capabilityBody.workflow_exchange.local_redeem_endpoint, false);
    assert.equal(capabilityBody.service_event.local_signer, false);
    assert.equal(capabilityBody.workflow_redeem_client, "not-wired");
    assert.equal(capabilityBody.service_event_delivery, "fail-closed-no-authenticator-wired");
    assert.equal(capabilityBody.legacy_wire_accepted_as_v1, false);

    const readiness = await fetch(`${base}/api/identity/v1/readiness`);
    assert.equal(readiness.status, 503);
    assert.equal((await readiness.json()).production_ready, false);

    const unauthenticated = await fetch(`${base}/api/identity/v1/service-events`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(envelope)
    });
    assert.equal(unauthenticated.status, 401);

    const applied = await fetch(`${base}/api/identity/v1/service-events`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: "Bearer conformance-test" },
      body: JSON.stringify(envelope)
    });
    assert.equal(applied.status, 200);
    assert.equal((await applied.json()).status, "applied");

    const noProductRedeem = await fetch(`${base}/api/identity/v1/workflow-exchanges/redeem`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}"
    });
    assert.equal(noProductRedeem.status, 404);
    const noCustomLogin = await fetch(`${base}/api/identity/v1/login`, { method: "POST" });
    assert.equal(noCustomLogin.status, 404);
  });
}

function canonicalService(name, publicJwk, runtimeMode = "development") {
  return createCanonicalIdentityService({
    filePath: path.join(root, name),
    runtimeMode,
    clientId: "soty.online",
    targetOrigin: "https://soty.online",
    issuers: [{ issuer: "https://identity.example", keys: [publicJwk] }],
    now: () => Date.parse("2026-08-12T10:07:00Z")
  });
}

function createWorkflowControlPlaneFake(now) {
  const records = [];
  let queue = Promise.resolve();
  const consume = (request) => {
    const digest = Buffer.from(`sha256:${sha256(request.code)}`, "ascii");
    const record = records.find((item) => {
      const stored = Buffer.from(item.code_hash, "ascii");
      return stored.length === digest.length && timingSafeEqual(stored, digest);
    });
    if (!record || record.status !== "issued" || Date.parse(record.expires_at) <= now) throw new Error("not redeemable");
    for (const field of ["target_client_id", "target_origin", "purpose", "state", "organization_id"]) {
      if (request[field] !== record[field]) throw new Error("binding mismatch");
    }
    if (stableJson(request.resources) !== stableJson(record.resources)) throw new Error("resource mismatch");
    record.status = "consumed";
    record.consumed_at = new Date(now).toISOString();
    return {
      ok: true,
      subject_id: record.subject_id,
      organization_id: record.organization_id,
      resources: structuredClone(record.resources)
    };
  };
  return {
    seed(record) {
      validateWorkflowExchangeRecord(record);
      records.push(structuredClone(record));
    },
    async redeem(request) {
      const run = queue.then(() => consume(request));
      queue = run.then(() => undefined, () => undefined);
      return await run;
    },
    snapshot() {
      return structuredClone(records);
    }
  };
}

function eventSigningKey(kid = "key_soty_test_01K2A0") {
  const pair = generateKeyPairSync("ed25519");
  return {
    privateKey: pair.privateKey,
    publicJwk: {
      ...pair.publicKey.export({ format: "jwk" }),
      kid,
      use: "sig",
      alg: "EdDSA"
    }
  };
}

function signEvent(signing, payload) {
  const protectedValue = Buffer.from(stableJson({
    alg: "EdDSA",
    kid: signing.publicJwk.kid,
    typ: "identity-service-event+jws"
  }), "utf8").toString("base64url");
  const payloadValue = Buffer.from(stableJson(payload), "utf8").toString("base64url");
  const signature = sign(
    null,
    Buffer.from(`${protectedValue}.${payloadValue}`, "ascii"),
    signing.privateKey
  ).toString("base64url");
  return { protected: protectedValue, payload: payloadValue, signature };
}

async function vector(id, callback) {
  await callback();
  vectorResults.set(id, "PASS");
}

async function expectCanonicalError(callback, code) {
  await assert.rejects(async () => await callback(), (error) => {
    assert.ok(error instanceof CanonicalIdentityError, String(error));
    if (code) assert.equal(error.code, code);
    return true;
  });
}

async function withServer(app, callback) {
  const server = createServer(app);
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  try {
    const address = server.address();
    await callback(`http://127.0.0.1:${address.port}`);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

function resolveManifestPath(reference) {
  return new URL(reference, contractRoot);
}

function loadJson(relativePath) {
  return JSON.parse(readFileSync(new URL(relativePath, contractRoot), "utf8"));
}

function subjectId(value) {
  return typeof value === "string" && /^sub_[A-Za-z0-9_-]{5,127}$/u.test(value);
}

function organizationId(value) {
  return typeof value === "string" && /^org_[A-Za-z0-9_-]{5,127}$/u.test(value);
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function stableJson(value) {
  if (value === null || typeof value === "boolean" || typeof value === "string" || typeof value === "number") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
}
