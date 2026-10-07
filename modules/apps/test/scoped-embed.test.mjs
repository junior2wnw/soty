import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { randomBytes } from "node:crypto";
import { createScopedEmbedAuthority } from "../scoped-embed/authority.mjs";
import {
  createSourceProofSigner,
  createSourceProofVerifier,
} from "../scoped-embed/source-proof.mjs";
import {
  createLocalScopedEmbedBroker,
  createSourceCurrentSubjectPort,
} from "../scoped-embed/local-broker.mjs";
import {
  SCOPED_EMBED_PROFILE,
  scopedEmbedProfile,
  scopedRoute,
  sourceConsentDigest,
} from "../scoped-embed/profile.mjs";

function fixture() {
  const appId = "app-" + "a".repeat(32),
    actor = Object.freeze({ accountId: "actor_a", deviceId: "device_a" }),
    foreign = Object.freeze({ accountId: "actor_b", deviceId: "device_b" });
  const profile = {
    schema: SCOPED_EMBED_PROFILE,
    appId,
    connector: {
      linkId: "link_a",
      hostDeviceId: "host_a",
      connectorId: "connector_a",
    },
    target: { revision: 1, digest: "a".repeat(64) },
    sourceProfile: {
      id: "planner.selected-workspace",
      version: 1,
      digest: "b".repeat(64),
    },
    resource: {
      registryId: "soty",
      tenantId: actor.accountId,
      environmentId: "fixture",
      appId,
      resourceId: "planner:selected",
      workspaceId: "selected",
    },
    issuer: "http://127.0.0.1:5312/human-identity",
    clientId: "planner-fixture",
    embedOrigin: "http://127.0.0.1:5313",
    nativeOrigin: "http://localhost:5313",
    parentOrigin: "http://127.0.0.1:5312",
  };
  let allowed = true,
    epoch = 1,
    seenActor = null;
  const authority = createScopedEmbedAuthority({
    profiles: [profile],
    withHumanSubjectAuthority(request, callback) {
      assert.ok(request.actor === actor || request.actor === foreign);
      if (!allowed) throw new Error('synthetic revoked');
      return callback(Object.freeze({ issuer: profile.issuer, subject: request.actor.accountId,
        clientId: profile.clientId, clientProfileDigest: 'c'.repeat(64), clientGeneration: 1 }));
    },
    withAppAuthority(request, callback) {
      assert.ok(request.actor === actor || request.actor === foreign);
      if (!allowed) throw new Error("synthetic revoked");
      seenActor = request.actor;
      return callback({
        appId,
        ownerId: actor.accountId,
        accountId: request.actor.accountId,
        policyEpoch: epoch,
        target: profile.target,
        entry: { origin: profile.embedOrigin, domainId: "domain_a" },
        appRevision: 1,
      });
    },
  });
  const context = authority.open({ actor, appId, domainId: "domain_a" }),
    key = randomBytes(32),
    nonces = new Set();
  const signer = createSourceProofSigner({ profile, key }),
    verifier = createSourceProofVerifier({
      profile,
      key,
      consumeNonce(nonce) {
        if (nonces.has(nonce)) return false;
        nonces.add(nonce);
        return true;
      },
    });
  const request = (headers, method = "GET", url = "/api/embed/state") => ({
    method,
    url,
    headers: { host: new URL(profile.embedOrigin).host, ...headers },
  });
  return {
    profile,
    actor,
    foreign,
    authority,
    context,
    key,
    signer,
    verifier,
    request,
    revoke() {
      allowed = false;
    },
    epoch() {
      epoch++;
    },
    get seenActor() {
      return seenActor;
    },
  };
}

test("launch continuation keeps the original opaque actor, connector, resource/profile and current policy; raw identities cannot replace it", () => {
  const f = fixture();
  assert.equal(
    f.authority.read({
      reference: f.context.reference,
      connector: f.profile.connector,
    }).rootPrincipal.accountId,
    "actor_a",
  );
  assert.equal(f.seenActor, f.actor);
  const consent = sourceConsentDigest(f.profile);
  assert.equal(
    sourceConsentDigest({
      ...f.profile,
      connector: { ...f.profile.connector, hostDeviceId: "rotated_device" },
      target: { revision: 2, digest: "c".repeat(64) },
    }),
    consent,
  );
  assert.notEqual(
    sourceConsentDigest({
      ...f.profile,
      sourceProfile: {
        ...f.profile.sourceProfile,
        version: 2,
        digest: "d".repeat(64),
      },
    }),
    consent,
  );
  assert.notEqual(
    sourceConsentDigest({
      ...f.profile,
      resource: { ...f.profile.resource, workspaceId: "another" },
    }),
    consent,
  );
  assert.throws(() =>
    f.authority.open({
      actor: { ...f.actor },
      appId: f.profile.appId,
      domainId: "domain_a",
    }),
  );
  assert.throws(() =>
    f.authority.read({
      reference: f.context.reference,
      connector: { ...f.profile.connector, hostDeviceId: "other" },
    }),
  );
  f.epoch();
  assert.throws(
    () =>
      f.authority.read({
        reference: f.context.reference,
        connector: f.profile.connector,
      }),
    /scoped_embed_authority_changed/u,
  );
  f.authority.close();
});

test("source proof binds every request and is one-use; unsigned/fake subject, tamper, cookie/body replay and cross profile fail", () => {
  const f = fixture(),
    body = Buffer.from('{"title":"Synthetic work"}'),
    path = "/api/embed/entities",
    cookie = "planner_soty_session=" + "a".repeat(43);
  const headers = f.signer.headers({
      context: f.context,
      method: "POST",
      path,
      body,
      cookie,
    }),
    original = f.request({ ...headers, cookie }, "POST", path);
  assert.equal(
    f.verifier.verify(original, { body }).rootPrincipal.accountId,
    "actor_a",
  );
  assert.throws(
    () =>
      f.verifier.verify(f.request({ ...headers, cookie }, "POST", path), {
        body,
      }),
    /scoped_embed_proof_replayed/u,
  );
  assert.throws(
    () =>
      f.verifier.verify(
        f.request({ "x-soty-subject": "actor_a" }, "POST", path),
        { body },
      ),
    /scoped_embed_proof_invalid/u,
  );
  assert.throws(
    () =>
      f.verifier.verify(
        f.request(
          {
            ...f.signer.headers({
              context: f.context,
              method: "POST",
              path,
              body,
              cookie,
            }),
            cookie,
          },
          "POST",
          path,
        ),
        { body: Buffer.from("{}") },
      ),
    /scoped_embed_proof_mismatch/u,
  );
  assert.throws(
    () =>
      f.signer.headers({
        context: f.context,
        method: "GET",
        path: "/api/state",
      }),
    /scoped_embed_route_denied/u,
  );
  assert.throws(
    () => scopedRoute("POST", "/api/embed/agent/keys"),
    /scoped_embed_route_denied/u,
  );
  for (const route of [
    "/api/embed/entities/../state",
    "/api/embed/state#fragment",
    "/api/embed/%73tate",
  ])
    assert.throws(
      () => scopedRoute("GET", route),
      /scoped_embed_route_denied/u,
    );
  assert.throws(() =>
    scopedEmbedProfile({
      ...f.profile,
      resource: { ...f.profile.resource, appId: "different" },
    }),
  );
  f.authority.close();
});

test("Root context alone never proves a human; separate verified issuer/sub joins original launch, Native continuation and profile switch", async () => {
  const f = fixture(),
    req = f.request(
      f.signer.headers({
        context: f.context,
        method: "GET",
        path: "/api/embed/state",
      }),
    );
  f.verifier.verify(req);
  const genuine = Object.freeze({
    issuer: f.profile.issuer,
    subject: f.actor.accountId,
  });
  const port = createSourceCurrentSubjectPort({
    profile: f.profile,
    verifier: f.verifier,
    readAuthority: (value) => f.authority.read(value),
    verifyHuman: async (proof) => {
      assert.equal(proof, genuine);
      return genuine;
    },
  });
  await assert.rejects(port(req, {}), /scoped_embed_human_proof_required/u);
  await assert.rejects(
    port(req, { continuation: f.context.reference, proof: { ...genuine } }),
  );
  assert.deepEqual(
    await port(req, { continuation: f.context.reference, proof: genuine }),
    genuine,
  );
  const other = f.authority.open({
    actor: f.foreign,
    appId: f.profile.appId,
    domainId: "domain_a",
  });
  const switched = f.request(
    f.signer.headers({
      context: other,
      method: "GET",
      path: "/api/embed/state",
    }),
  );
  f.verifier.verify(switched);
  await assert.rejects(
    port(switched, { continuation: f.context.reference, proof: genuine }),
    /scoped_embed_profile_changed/u,
  );
  const native = {
    url: "/soty/connect?intent=opaque",
    headers: { host: new URL(f.profile.nativeOrigin).host },
  };
  assert.deepEqual(
    await port(native, { continuation: f.context.reference, proof: genuine }),
    genuine,
  );
  f.revoke();
  await assert.rejects(
    port(native, { continuation: f.context.reference, proof: genuine }),
  );
  f.authority.close();
});

test("fixed-port real HTTP broker signs actual source request, keeps approved cookies private and refuses Origin/arbitrary paths/redirect", async (t) => {
  const f = fixture();
  let observed = 0;
  const source = createServer(async (req, res) => {
    try {
      const parts = [];
      for await (const part of req) parts.push(part);
      const body = Buffer.concat(parts);
      f.verifier.verify(req, { body });
      observed++;
      if (req.url === "/api/embed/search?redirect=bad") {
        res.writeHead(302, { location: "https://foreign.invalid/private" });
        res.end();
        return;
      }
      if (req.url === "/api/embed/login") {
        res.writeHead(302, {
          "set-cookie":
            "planner_soty_intent=" +
            "a".repeat(43) +
            "; Path=/; HttpOnly; SameSite=Lax; Max-Age=300",
          location:
            f.profile.issuer +
            "/authorize?client_id=planner-fixture&redirect_uri=" +
            encodeURIComponent(f.profile.embedOrigin + "/api/embed/callback") +
            "&response_type=code&code_challenge_method=S256&state=" + "s".repeat(43) + "&nonce=" + "n".repeat(43) + "&code_challenge=" + "c".repeat(43) + "&scope=openid+profile",
        });
        res.end();
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          selected: true,
          sourceVerified: true,
          cookiePresent: !!req.headers.cookie,
        }),
      );
    } catch (error) {
      res.writeHead(403, { "content-type": "application/json" });
      res.end(JSON.stringify({ code: error.code, host: req.headers.host }));
    }
  });
  await new Promise((done) => source.listen(0, "127.0.0.1", done));
  const port = source.address().port;
  const broker = createLocalScopedEmbedBroker({
    profile: f.profile,
    localPort: port,
    key: f.key,
    readAuthority: (value) => f.authority.read(value),
    assertBinding: () => true,
  });
  t.after(async () => {
    broker.close();
    f.authority.close();
    source.closeAllConnections();
    await new Promise((done) => source.close(done));
  });
  const started = await broker.dispatch({
    context: f.context,
    method: "GET",
    path: "/api/embed/login",
  });
  assert.equal(started.status, 302, started.body.toString());
  assert.equal(started.headers["set-cookie"], undefined);
  const state = await broker.dispatch({
    context: f.context,
    method: "GET",
    path: "/api/embed/state",
  });
  assert.deepEqual(JSON.parse(state.body), {
    selected: true,
    sourceVerified: true,
    cookiePresent: true,
  });
  assert.equal(observed, 2);
  await assert.rejects(
    broker.dispatch({
      context: f.context,
      method: "GET",
      path: "/api/embed/search?redirect=bad",
    }),
    /scoped_embed_redirect_denied/u,
  );
  await assert.rejects(
    broker.dispatch({ context: f.context, method: "GET", path: "/api/state" }),
    /scoped_embed_route_denied/u,
  );
  await assert.rejects(
    broker.dispatch({
      context: f.context,
      method: "POST",
      path: "/api/embed/entities",
      headers: { origin: "https://wrong.invalid" },
      body: Buffer.from("{}"),
    }),
    /scoped_embed_origin_invalid/u,
  );
  await assert.rejects(
    broker.dispatch({
      context: f.context,
      method: "POST",
      path: "/api/embed/files",
      headers: { origin: f.profile.embedOrigin },
      body: Buffer.alloc(1048577),
    }),
    (error) =>
      error.code === "scoped_embed_request_limit" && error.status === 413,
  );
  f.revoke();
  await assert.rejects(
    broker.dispatch({
      context: f.context,
      method: "GET",
      path: "/api/embed/state",
    }),
  );
  assert.equal(observed, 3);
});

test("broker freezes pending headers, drops expired source cookies and bounds returned headers", async (t) => {
  const f = fixture();
  let now = Date.now(),
    entered,
    release,
    observedOrigin,
    observedCookie,
    large = false;
  const waiting = new Promise((done) => {
    entered = done;
  });
  const gate = new Promise((done) => {
    release = done;
  });
  let reads = 0,
    calls = 0;
  const broker = createLocalScopedEmbedBroker({
    profile: f.profile,
    localPort: 5313,
    key: f.key,
    clock: () => now,
    async readAuthority(value) {
      if (reads++ === 0) {
        entered();
        await gate;
      }
      return f.authority.read(value);
    },
    assertBinding: () => true,
    async fetch(_url, options) {
      observedOrigin = options.headers.origin;
      observedCookie = options.headers.cookie;
      const first = calls++ === 0;
      return {
        status: 200,
        redirected: false,
        body: null,
        headers: {
          get: (name) =>
            name === "content-type"
              ? large
                ? "x".repeat(17000)
                : "application/json"
              : null,
          getSetCookie: () =>
            first
              ? [
                  "planner_soty_session=" +
                    "a".repeat(43) +
                    "; Path=/; HttpOnly; SameSite=Lax; Max-Age=1",
                ]
              : [],
        },
      };
    },
  });
  t.after(() => {
    broker.close();
    f.authority.close();
  });
  const headers = { origin: f.profile.embedOrigin },
    pending = broker.dispatch({
      context: f.context,
      method: "POST",
      path: "/api/embed/entities",
      headers,
      body: Buffer.from("{}"),
    });
  await waiting;
  headers.origin = "https://foreign.invalid";
  release();
  await pending;
  assert.equal(observedOrigin, f.profile.embedOrigin);
  await broker.dispatch({
    context: f.context,
    method: "GET",
    path: "/api/embed/state",
  });
  assert.ok(observedCookie);
  now += 1001;
  await broker.dispatch({
    context: f.context,
    method: "GET",
    path: "/api/embed/state",
  });
  assert.equal(observedCookie, undefined);
  large = true;
  await assert.rejects(
    broker.dispatch({
      context: f.context,
      method: "GET",
      path: "/api/embed/state",
    }),
    /scoped_embed_header_limit/u,
  );
  let getterCalled = false;
  await assert.rejects(
    broker.dispatch({
      context: f.context,
      method: "GET",
      path: "/api/embed/state",
      headers: Object.defineProperty({}, "accept", {
        enumerable: true,
        get() {
          getterCalled = true;
          return "*/*";
        },
      }),
    }),
  );
  assert.equal(getterCalled, false);
});
