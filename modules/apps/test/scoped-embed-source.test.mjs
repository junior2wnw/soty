import test from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { pathToFileURL } from "node:url";
import { join } from "node:path";
import { fixture, sourceAvailable } from "./support/planner-scoped-fixture.mjs";
import { createScopedEmbedAuthority } from "../scoped-embed/authority.mjs";
import { createLocalScopedEmbedBroker } from "../scoped-embed/local-broker.mjs";
import { SCOPED_EMBED_PROFILE } from "../scoped-embed/profile.mjs";

// Real signed Connect and maintained OIDC/Planner source. The app-registry
// authority shape and future channel hook are explicit constructor fixtures;
// this is not a claim that the restricted gateway/profile readers are wired.
test(
  "actual Source BFF joins signed device-scoped launch continuation with maintained Human proof and one current source workspace",
  {
    skip: sourceAvailable
      ? false
      : "Optional separately packaged selected Planner source absent.",
    timeout: 30000,
  },
  async (t) => {
    const f = await fixture(t, async (env) => {
      const appId = "app-" + "c".repeat(32),
        actor = env.actorRefs.get(env.primary.account.accountId),
        other = env.actorRefs.get(env.other.account.accountId);
      assert.ok(actor && other && env.connect.isActorActive(actor));
      const profile = {
        schema: SCOPED_EMBED_PROFILE,
        appId,
        connector: {
          linkId: "synthetic_signed_link",
          hostDeviceId: "synthetic_host",
          connectorId: "synthetic_connector",
        },
        target: { revision: 1, digest: "1".repeat(64) },
        sourceProfile: {
          id: "planner.selected-workspace",
          version: 1,
          digest: "2".repeat(64),
        },
        resource: {
          registryId: "soty",
          tenantId: actor.accountId,
          environmentId: "fixture",
          appId,
          resourceId: "planner:selected",
          workspaceId: env.workspaceId,
        },
        issuer: env.options.embed.profile.issuer,
        clientId: env.options.embed.profile.clientId,
        embedOrigin: env.embedded,
        nativeOrigin: env.native,
        parentOrigin: env.rootOrigin,
      };
      let allowed = true,
        providerUnavailable = false,
        currentContext;
      const authority = createScopedEmbedAuthority({
        profiles: [profile],
        withHumanSubjectAuthority: (request, callback) => env.identity.withSubjectAuthority(request, callback),
        withAppAuthority(request, callback) {
          assert.ok(request.actor === actor || request.actor === other);
          assert.equal(env.connect.isActorActive(request.actor), true);
          assert.equal(
            allowed,
            true,
            "synthetic source-app permission revoked",
          );
          return callback({
            appId,
            ownerId: actor.accountId,
            accountId: request.actor.accountId,
            policyEpoch: 1,
            target: profile.target,
            entry: { origin: env.embedded, domainId: "synthetic_domain" },
          });
        },
      });
      currentContext = authority.open({
        actor,
        appId,
        domainId: "synthetic_domain",
      });
      const initial = currentContext,
        key = randomBytes(32);
      const sdk = await import(
        pathToFileURL(join(env.sourceRoot, "server/soty-source-host.mjs")).href
      );
      const { tsImport } = await import(
        pathToFileURL(
          join(env.sourceRoot, "node_modules/tsx/dist/esm/api/index.mjs"),
        ).href
      );
      const { createSotyBffProtocol } = await tsImport(
        pathToFileURL(join(env.sourceRoot, "server/soty-protocol.ts")).href,
        import.meta.url,
      );
      const rp = createSotyBffProtocol(env.options.embed.profile);
      const verifier = sdk.createSourceProofVerifier({
        profile,
        key,
        consumeNonce(nonce, expiresAt) {
          const db = env.getPlanner().store.db;
          db.exec(
            "CREATE TABLE IF NOT EXISTS fixture_bridge_nonce (nonce TEXT PRIMARY KEY,expires_at INTEGER NOT NULL)",
          );
          db.prepare(
            "DELETE FROM fixture_bridge_nonce WHERE expires_at<=?",
          ).run(Date.now());
          try {
            db.prepare(
              "INSERT INTO fixture_bridge_nonce(nonce,expires_at) VALUES(?,?)",
            ).run(nonce, expiresAt);
            return true;
          } catch {
            return false;
          }
        },
      });
      const readAuthority = (value) => authority.read(value);
      env.options.embed.bridge = {
        consentDigest: sdk.sourceConsentDigest(profile),
        verifyRequest: (req, body) => {
          verifier.verify(req, { body });
        },
        continuation: (req) => verifier.context(req).reference,
      };
      env.options.embed.currentSotySubject = sdk.createSourceCurrentSubjectPort(
        {
          profile,
          verifier,
          readAuthority,
          verifyHuman: async (proof) => {
            if (providerUnavailable)
              throw Object.assign(new Error("Synthetic provider unavailable"), {
                status: 503,
              });
            assert.equal(proof.issuer, profile.issuer);
            assert.ok(proof.expiresAt > Date.now());
            return {
              issuer: profile.issuer,
              subject: await rp.currentSubject(
                proof.accessToken,
                proof.subject,
              ),
            };
          },
        },
      );
      const broker = createLocalScopedEmbedBroker({
        profile,
        localPort: env.options.port,
        key,
        readAuthority,
        assertBinding: () => allowed,
      });
      t.after(() => {
        broker.close();
        authority.close();
      });
      return {
        wrapWire(wire) {
          const direct = wire.request.bind(wire);
          wire.request = async (url, options = {}) => {
            const target = new URL(url);
            if (target.origin !== env.embedded) return direct(url, options);
            const method =
              options.method ??
              (options.body || options.fields ? "POST" : "GET");
            assert.equal(options.fields, undefined);
            const body = options.body
                ? Buffer.from(JSON.stringify(options.body))
                : Buffer.alloc(0),
              headers = { ...options.headers };
            if (!["GET", "HEAD"].includes(method)) {
              headers.origin = env.embedded;
              headers["content-type"] = "application/json";
            }
            const result = await broker.dispatch({
              context: currentContext,
              method,
              path: target.pathname + target.search,
              headers,
              body,
            });
            const text = result.body.toString("utf8");
            return {
              status: result.status,
              text,
              body: result.headers["content-type"]?.includes("application/json")
                ? JSON.parse(text)
                : null,
              headers: new Headers(result.headers),
              location: result.headers.location
                ? new URL(result.headers.location, target)
                : null,
            };
          };
        },
        revoke() {
          allowed = false;
        },
        providerOutage(value) {
          providerUnavailable = value;
        },
        switch() {
          authority.invalidate({
            reference: initial.reference,
            connector: profile.connector,
          });
          currentContext = authority.open({
            actor: other,
            appId,
            domainId: "synthetic_domain",
          });
        },
        get context() {
          return currentContext;
        },
        profile,
      };
    });
    const missing = await fetch(f.embedded + "/api/embed/state", {
      headers: { "x-soty-subject": f.primary.account.accountId },
    });
    assert.equal(missing.status, 403);
    assert.equal((await missing.json()).code, "scoped_embed_proof_invalid");
    const login = await f.login();
    assert.equal(login.status, 200);
    const url =
      /<a href="([^"]+\/soty\/connect\?intent=[A-Za-z0-9_-]+)">/u.exec(
        login.text,
      )?.[1];
    assert.ok(url);
    const consent = await f.wire.request(url);
    assert.equal(consent.status, 200);
    const intent = /name="intent" value="([A-Za-z0-9_-]+)"/u.exec(
      consent.text,
    )?.[1];
    const approved = await f.wire.request(f.native + "/soty/connect", {
      fields: { intent },
    });
    assert.equal(approved.status, 302);
    const completed = await f.wire.request(approved.location);
    assert.equal(completed.status, 200);
    const state = await f.wire.request(f.embedded + "/api/embed/state");
    assert.equal(state.status, 200);
    assert.equal(state.body.workspaces.length, 1);
    assert.equal(state.body.workspaces[0].id, f.workspaceId);
    f.extension.providerOutage(true);
    const delayed = await f.wire.request(f.embedded + "/api/embed/state");
    assert.equal(delayed.status, 503);
    assert.equal(delayed.body.code, "embed_provider_unavailable");
    f.extension.providerOutage(false);
    assert.equal(
      (await f.wire.request(f.embedded + "/api/embed/state")).status,
      200,
    );
    const created = await f.wire.request(f.embedded + "/api/embed/entities", {
      body: { title: "Synthetic broker-only work", workspaceId: f.workspaceId },
    });
    assert.equal(created.status, 201);
    assert.equal(created.body.entities.at(-1).plan.start, null);
    assert.equal(
      (await f.wire.request(f.native + "/soty/disconnect", { fields: {} }))
        .status,
      200,
    );
    assert.equal(
      (await f.wire.request(f.embedded + "/api/embed/state")).status,
      403,
    );
    assert.equal(
      f.planner.store.db
        .prepare("SELECT count(*) AS n FROM planner_soty_links")
        .get().n,
      1,
    );
    assert.equal(
      f.planner.store.db
        .prepare("SELECT count(*) AS n FROM planner_soty_grants")
        .get().n,
      0,
    );
    // A synthetic historical grant for a different semantic binding cannot
    // replace explicit current consent, even for the same issuer/sub/workspace.
    f.planner.store.db
      .prepare(
        "INSERT INTO planner_soty_grants(issuer,subject,workspace_id,binding_digest,created_at) SELECT issuer,subject,workspace_id,?,? FROM planner_soty_links",
      )
      .run("0".repeat(64), Date.now());
    assert.equal(
      (await f.wire.request(f.embedded + "/api/embed/state")).status,
      403,
    );
    const relogin = await f.login();
    assert.equal(relogin.status, 200);
    const relinkUrl =
      /<a href="([^"]+\/soty\/connect\?intent=[A-Za-z0-9_-]+)">/u.exec(
        relogin.text,
      )?.[1];
    assert.ok(
      relinkUrl,
      "same Human association without current resource grant still needs native consent",
    );
    const reconsent = await f.wire.request(relinkUrl),
      reintent = /name="intent" value="([A-Za-z0-9_-]+)"/u.exec(
        reconsent.text,
      )?.[1];
    assert.ok(reintent);
    const reapproved = await f.wire.request(f.native + "/soty/connect", {
      fields: { intent: reintent },
    });
    assert.equal(reapproved.status, 302);
    assert.equal((await f.wire.request(reapproved.location)).status, 200);
    assert.equal(
      (await f.wire.request(f.embedded + "/api/embed/state")).status,
      200,
    );
    f.extension.switch();
    const other = await f.wire.request(f.embedded + "/api/embed/state");
    assert.equal(other.status, 401);
    f.extension.revoke();
    await assert.rejects(f.wire.request(f.embedded + "/api/embed/state"));
    assert.equal(
      f.planner.store
        .read()
        .entities.filter((e) => e.workspaceId === f.workspaceId).length,
      2,
    );
  },
);
