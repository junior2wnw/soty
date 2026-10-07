import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import {
  capture,
  closed,
  continuation,
  hash,
  identifier,
  need,
  scopedEmbedProfile,
  scopedRoute,
  SOURCE_PROOF_PROTOCOL,
  SCOPED_EMBED_LIMITS,
} from "./profile.mjs";

export const SOURCE_PROOF_HEADER = "x-soty-selected-proof";
export const SOURCE_MAC_HEADER = "x-soty-selected-mac";
export const SOURCE_PROBE_HEADER = 'x-soty-selected-probe';
export const SOURCE_READY_HEADER = 'x-soty-selected-ready';
const token = (value) =>
  need(
    typeof value === "string" && /^[A-Za-z0-9_-]{43}$/u.test(value),
    "scoped_embed_proof_invalid",
    403,
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
    "expiresAt",
  ]);
  closed(context.rootPrincipal, ["accountId", "deviceId"]);
  Object.values(context.rootPrincipal).forEach(identifier);
  closed(context.humanPrincipal, ['issuer','subject','clientId','clientProfileDigest','clientGeneration']);
  need(context.humanPrincipal.issuer === profile.issuer && context.humanPrincipal.clientId === profile.clientId
    && typeof context.humanPrincipal.subject === 'string' && context.humanPrincipal.subject.length > 0 && context.humanPrincipal.subject.length <= 128
    && /^[a-f0-9]{64}$/.test(context.humanPrincipal.clientProfileDigest)
    && Number.isSafeInteger(context.humanPrincipal.clientGeneration) && context.humanPrincipal.clientGeneration > 0,
    'scoped_embed_human_principal_invalid', 403);
  closed(context.entry, ["domainId", "origin"]);
  identifier(context.entry.domainId);
  need(
    Number.isSafeInteger(context.policyEpoch) &&
      context.policyEpoch > 0 &&
      Number.isSafeInteger(context.expiresAt) &&
      context.expiresAt > 0,
  );
  continuation(context.reference);
  need(
    context.schema === "soty.verified-launch-continuation.v1" &&
      context.profileDigest === profile.digest &&
      context.appId === profile.appId &&
      hash(context.sourceProfile) === hash(profile.sourceProfile) &&
      hash(context.resource) === hash(profile.resource) &&
      hash(context.target) === hash(profile.target) &&
      context.entry.origin === profile.embedOrigin,
    "scoped_embed_context_mismatch",
    403,
  );
  return context;
}

/** Kept inside the authenticated local connector. It signs an already trusted
 * Root dispatch context, not user JSON. The key never enters a manifest/frame. */
export function createSourceProofSigner({
  profile: raw,
  key,
  clock = Date.now,
} = {}) {
  const profile = scopedEmbedProfile(raw),
    secret = hostKey(key);
  return Object.freeze({
    probeHeaders() {
      const value = Object.freeze({ schema: 'soty.selected-source-probe.v1', profileDigest: profile.digest,
        nonce: randomBytes(32).toString('base64url'), expiresAt: clock()+SCOPED_EMBED_LIMITS.proofMs });
      const text = Buffer.from(JSON.stringify(value)).toString('base64url');
      return { request: value, headers: { [SOURCE_PROBE_HEADER]: text, [SOURCE_MAC_HEADER]: mac(secret, 'probe\0'+text) } };
    },
    verifyReady(response, expected) {
      const text = response.headers.get(SOURCE_READY_HEADER), signature=response.headers.get(SOURCE_MAC_HEADER);
      token(signature); need(typeof text==='string' && text.length<=2048, 'scoped_embed_probe_invalid',503);
      need(timingSafeEqual(Buffer.from(signature),Buffer.from(mac(secret,'ready\0'+text))), 'scoped_embed_probe_invalid',503);
      const value=capture(JSON.parse(Buffer.from(text,'base64url').toString('utf8')));closed(value,['schema','profileDigest','nonce','expiresAt']);
      need(response.status===204 && hash(value)===hash(expected) && value.expiresAt>clock(), 'scoped_embed_probe_invalid',503);return true;
    },
    headers({ context, method, path, body = Buffer.alloc(0), cookie = "" }) {
      const trusted = wireContext(context, profile);
      scopedRoute(method, path);
      need(
        Buffer.isBuffer(body) &&
          body.length <= SCOPED_EMBED_LIMITS.requestBytes,
      );
      const value = {
        schema: SOURCE_PROOF_PROTOCOL,
        context: trusted,
        method,
        path,
        bodyDigest: hash(body.toString("base64")),
        cookieDigest: hash(cookie),
        nonce: randomBytes(32).toString("base64url"),
        expiresAt: Math.min(
          trusted.expiresAt,
          clock() + SCOPED_EMBED_LIMITS.proofMs,
        ),
      };
      const text = Buffer.from(JSON.stringify(value)).toString("base64url");
      need(text.length <= 8192);
      return Object.freeze({
        [SOURCE_PROOF_HEADER]: text,
        [SOURCE_MAC_HEADER]: mac(secret, text),
      });
    },
  });
}

/** This private verifier must run before source middleware/data. HTTP fields
 * carry only a MAC-bound request; they do not themselves prove a human login. */
export function createSourceProofVerifier({
  profile: raw,
  key,
  consumeNonce,
  clock = Date.now,
} = {}) {
  const profile = scopedEmbedProfile(raw),
    secret = hostKey(key);
  need(typeof consumeNonce === "function", "scoped_embed_nonce_store_required");
  const seen = new WeakMap();
  return Object.freeze({
    verifyReady(request) {
      need(request.method==='HEAD' && request.url==='/api/embed/transport-ready' && request.headers.host===new URL(profile.embedOrigin).host,
        'scoped_embed_probe_invalid',403);
      const text=request.headers[SOURCE_PROBE_HEADER], signature=request.headers[SOURCE_MAC_HEADER];token(signature);
      need(typeof text==='string' && text.length<=2048 && /^[A-Za-z0-9_-]+$/.test(text),'scoped_embed_probe_invalid',403);
      need(timingSafeEqual(Buffer.from(signature),Buffer.from(mac(secret,'probe\0'+text))), 'scoped_embed_probe_invalid',403);
      const value=capture(JSON.parse(Buffer.from(text,'base64url').toString('utf8')));closed(value,['schema','profileDigest','nonce','expiresAt']);token(value.nonce);
      need(value.schema==='soty.selected-source-probe.v1' && value.profileDigest===profile.digest && Number.isSafeInteger(value.expiresAt)
        && value.expiresAt>clock() && value.expiresAt<=clock()+SCOPED_EMBED_LIMITS.proofMs && consumeNonce(value.nonce,value.expiresAt)===true,
        'scoped_embed_probe_invalid',403);
      return Object.freeze({ [SOURCE_READY_HEADER]:text, [SOURCE_MAC_HEADER]:mac(secret,'ready\0'+text) });
    },
    verify(request, { body = Buffer.alloc(0) } = {}) {
      need(!seen.has(request), "scoped_embed_proof_replayed", 403);
      need(
        request.headers.host === new URL(profile.embedOrigin).host,
        "scoped_embed_origin_invalid",
        403,
      );
      const text = request.headers[SOURCE_PROOF_HEADER],
        signature = request.headers[SOURCE_MAC_HEADER];
      token(signature);
      for (const name of [SOURCE_PROOF_HEADER, SOURCE_MAC_HEADER]) {
        const count = (request.rawHeaders ?? []).filter(
          (_, i) =>
            i % 2 === 0 && (request.rawHeaders[i] ?? "").toLowerCase() === name,
        ).length;
        need(count <= 1, "scoped_embed_proof_invalid", 403);
      }
      need(
        typeof text === "string" &&
          text.length <= 8192 &&
          /^[A-Za-z0-9_-]+$/u.test(text),
        "scoped_embed_proof_invalid",
        403,
      );
      need(
        timingSafeEqual(Buffer.from(signature), Buffer.from(mac(secret, text))),
        "scoped_embed_proof_invalid",
        403,
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
        "expiresAt",
      ]);
      token(value.nonce);
      const context = wireContext(value.context, profile);
      need(
        value.schema === SOURCE_PROOF_PROTOCOL &&
          value.method === request.method &&
          value.path === request.url &&
          Number.isSafeInteger(value.expiresAt) &&
          value.expiresAt > clock() &&
          value.expiresAt <= clock() + SCOPED_EMBED_LIMITS.proofMs &&
          context.expiresAt >= value.expiresAt,
        "scoped_embed_proof_expired",
        401,
      );
      scopedRoute(value.method, value.path);
      need(
        Buffer.isBuffer(body) &&
          body.length <= SCOPED_EMBED_LIMITS.requestBytes &&
          value.bodyDigest === hash(body.toString("base64")) &&
          value.cookieDigest === hash(request.headers.cookie ?? ""),
        "scoped_embed_proof_mismatch",
        403,
      );
      need(
        consumeNonce(value.nonce, value.expiresAt) === true,
        "scoped_embed_proof_replayed",
        403,
      );
      seen.set(request, context);
      return context;
    },
    context(request) {
      const value = seen.get(request);
      need(value, "scoped_embed_transport_required", 401);
      return value;
    },
  });
}
