import { randomBytes } from "node:crypto";
import {
  capture,
  closed,
  connector,
  continuation,
  hash,
  identifier,
  need,
  scopedEmbedProfile,
  SCOPED_EMBED_LIMITS,
} from "./profile.mjs";

/** Private Root constructor port. Original actors/authority callbacks never
 * leave this host. A JSON accountId or an HTTP header cannot create authority. */
export function createScopedEmbedAuthority({
  profiles,
  withAppAuthority,
  clock = Date.now,
  random = () => randomBytes(32).toString("base64url"),
} = {}) {
  need(
    Array.isArray(profiles) &&
      profiles.length > 0 &&
      profiles.length <= 64 &&
      typeof withAppAuthority === "function",
  );
  const approved = new Map();
  for (const raw of profiles) {
    const profile = scopedEmbedProfile(raw);
    need(!approved.has(profile.appId));
    approved.set(profile.appId, profile);
  }
  const slots = new Map();
  let stopped = false;
  function fresh(slot, callback) {
    need(!stopped && slot.expiresAt > clock(), "scoped_embed_expired", 401);
    let entered = false,
      outcome;
    const returned = withAppAuthority(
      {
        actor: slot.actor,
        appId: slot.profile.appId,
        mode: "participant",
        domainId: slot.domainId,
        path: "/embed",
      },
      (snapshot) => {
        need(!entered, "scoped_embed_authority_invalid");
        entered = true;
        need(
          snapshot.appId === slot.profile.appId &&
            snapshot.ownerId === slot.profile.resource.tenantId &&
            snapshot.accountId === slot.accountId &&
            snapshot.entry.origin === slot.profile.embedOrigin &&
            snapshot.target.revision === slot.profile.target.revision &&
            snapshot.target.digest === slot.profile.target.digest &&
            snapshot.policyEpoch === slot.policyEpoch,
          "scoped_embed_authority_changed",
          403,
        );
        outcome = callback(snapshot);
        return outcome;
      },
    );
    need(
      entered && returned === outcome && !returned?.then,
      "scoped_embed_authority_invalid",
    );
    return outcome;
  }
  function find(reference, identity) {
    continuation(reference);
    connector(identity);
    const slot = slots.get(reference.id);
    need(
      slot && slot.reference.digest === reference.digest,
      "scoped_embed_expired",
      401,
    );
    need(
      hash(identity) === hash(slot.profile.connector),
      "scoped_embed_connector_mismatch",
      403,
    );
    return slot;
  }
  function view(slot) {
    return Object.freeze({
      schema: "soty.verified-launch-continuation.v1",
      reference: slot.reference,
      profileDigest: slot.profile.digest,
      appId: slot.profile.appId,
      sourceProfile: slot.profile.sourceProfile,
      resource: slot.profile.resource,
      rootPrincipal: Object.freeze({
        accountId: slot.accountId,
        deviceId: slot.deviceId,
      }),
      entry: Object.freeze({
        domainId: slot.domainId,
        origin: slot.profile.embedOrigin,
      }),
      target: slot.profile.target,
      policyEpoch: slot.policyEpoch,
      expiresAt: slot.expiresAt,
    });
  }
  return Object.freeze({
    open({ actor, appId, domainId }) {
      need(!stopped);
      const profile = approved.get(appId);
      need(profile, "scoped_embed_not_approved", 403);
      for (const [id, slot] of slots)
        if (slot.expiresAt <= clock()) slots.delete(id);
      need(
        slots.size < SCOPED_EMBED_LIMITS.continuations,
        "scoped_embed_capacity",
        429,
      );
      let result;
      const response = withAppAuthority(
        { actor, appId, mode: "participant", domainId, path: "/embed" },
        (snapshot) => {
          identifier(snapshot.accountId);
          identifier(actor.deviceId);
          need(
            snapshot.ownerId === profile.resource.tenantId &&
              snapshot.entry.origin === profile.embedOrigin &&
              snapshot.target.revision === profile.target.revision &&
              snapshot.target.digest === profile.target.digest,
            "scoped_embed_source_changed",
            403,
          );
          const id = random();
          need(/^[A-Za-z0-9_-]{43}$/u.test(id) && !slots.has(id));
          const reference = Object.freeze({
            id,
            version: 1,
            digest: hash({
              profileDigest: profile.digest,
              accountId: snapshot.accountId,
              deviceId: actor.deviceId,
              domainId: snapshot.entry.domainId,
              policyEpoch: snapshot.policyEpoch,
              nonce: id,
            }),
          });
          const slot = {
            reference,
            profile,
            actor,
            accountId: snapshot.accountId,
            deviceId: actor.deviceId,
            domainId: snapshot.entry.domainId,
            policyEpoch: snapshot.policyEpoch,
            expiresAt: clock() + SCOPED_EMBED_LIMITS.continuationMs,
          };
          slots.set(id, slot);
          result = view(slot);
          return result;
        },
      );
      need(
        response && !response.then && result,
        "scoped_embed_authority_invalid",
      );
      return result;
    },
    read({ reference, connector: identity }) {
      const slot = find(capture(reference), capture(identity));
      return fresh(slot, () => view(slot));
    },
    invalidate({ reference, connector: identity }) {
      const slot = find(capture(reference), capture(identity));
      slots.delete(slot.reference.id);
      return Object.freeze({ invalidated: true });
    },
    close() {
      stopped = true;
      slots.clear();
    },
  });
}
