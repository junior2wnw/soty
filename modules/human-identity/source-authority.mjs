import { createPrivateKey, randomBytes } from 'node:crypto';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { closed, data, digest, nonce, synchronous, requireHuman as require } from './profile.mjs';
import { selectedResourceProfile } from '../apps/scoped-embed/resource-profile.mjs';
import { isRootSignedWorkspaceSource } from '../source-app/server/standard-profile.mjs';
import { relayRequestDigest } from '../source-app/shared/relay-identity.mjs';

function issuerSigner(profile) {
  const jwk = profile.providerKeys().jwks.keys[0], key = createPrivateKey({ key: jwk, format: 'jwk' }), kid = jwk.kid;
  const requireProvider = createRequire(createRequire(import.meta.url).resolve('oidc-provider'));
  let jose;
  return async (payload, typ) => {
    jose ??= import(pathToFileURL(requireProvider.resolve('jose')).href);
    const { SignJWT } = await jose;
    return new SignJWT(payload).setProtectedHeader({ alg: 'RS256', kid, typ }).sign(key);
  };
}

function workspaceProfiles(profiles, issuer) {
  const approved = new Map();
  for (const raw of profiles) if (raw.schema === 'soty.selected-human-embed.v2' && isRootSignedWorkspaceSource(raw.sourceProfile)) {
    const value = selectedResourceProfile(raw);
    require(value.issuer === issuer && value.resource.selection.kind === 'soty.workspace.v1', 'human_identity_subject_mismatch', 403);
    approved.set(value.digest, value);
  }
  return approved;
}
function workspaceSubject(profile, selected, accountId) {
  require(typeof accountId === 'string' && accountId.length > 0 && accountId.length <= 128, 'human_identity_subject_mismatch', 403);
  // Keep the existing wire shape. These are the real admitted Apps profile and
  // durable target revision, not an RP registration or a synthetic generation.
  return Object.freeze({ issuer: profile.issuer, subject: accountId, clientId: selected.clientId,
    clientProfileDigest: digest({ issuerProfileDigest: profile.profileDigest, workspaceProfileDigest: selected.digest }),
    clientGeneration: selected.target.revision });
}

/** Private composition port: only the original Connect actor and an exact
 * configured Workspace2 profile can project the account subject. Apps retains
 * its independent target/policy/grant checks around every use. */
export function createRootWorkspaceSubjectAuthority({ profile, scopedProfiles, withActorAuthorityFence }) {
  require(profile?.enabled && typeof withActorAuthorityFence === 'function', 'human_identity_disabled', 503);
  const approved = workspaceProfiles(scopedProfiles, profile.issuer);
  return (request, callback) => {
    closed(request, ['actor', 'issuer', 'clientId', 'rootProfileDigest']);
    const selected = approved.get(request.rootProfileDigest);
    require(selected && request.issuer === profile.issuer && request.clientId === selected.clientId
      && typeof callback === 'function', 'human_identity_subject_mismatch', 403);
    let active = true, entered = false, outcome;
    try {
      const result = withActorAuthorityFence(request.actor, () => {
        require(active && !entered, 'human_identity_authority_fence_invalid', 500); entered = true;
        outcome = synchronous(callback(workspaceSubject(profile, selected, request.actor.accountId)));
        return outcome;
      });
      synchronous(result);
      require(entered && result === outcome, 'human_identity_authority_fence_invalid', 500);
      return outcome;
    } finally { active = false; }
  };
}

/** Private host constructor, never a Connect operation. Only the scoped gateway
 * supplies an original-actor-fenced context; signing is followed by its fresh
 * Human/Apps/reference check. Reuse the issuer key, not the connector's MAC key. */
export function createSourceAuthoritySigner({ profile, scopedProfiles = [], clock = Date.now }) {
  require(profile?.enabled && typeof clock === 'function', 'human_identity_disabled', 503);
  const sign = issuerSigner(profile);
  const approved = workspaceProfiles(scopedProfiles, profile.issuer);
  return async function signSourceAuthority(input) {
    closed(input, ['context', 'sourceChallenge']);
    const context = data(input.context), sourceChallenge = nonce(input.sourceChallenge), now = clock();
    require(Number.isSafeInteger(now) && now > 0 && Number.isSafeInteger(context.expiresAt), 'human_identity_clock_invalid', 503);
    const selected = isRootSignedWorkspaceSource(context.sourceProfile) ? approved.get(context.profileDigest) : null;
    const rootSubject = selected && context.appId === selected.appId && context.entry?.origin === selected.embedOrigin
      && digest(context.target) === digest(selected.target) && digest(context.resource) === digest(selected.resource)
      && digest(context.sourceProfile) === digest(selected.sourceProfile)
      && digest(context.humanPrincipal) === digest(workspaceSubject(profile, selected, context.rootPrincipal?.accountId));
    require(context.humanPrincipal?.issuer === profile.issuer && (isRootSignedWorkspaceSource(context.sourceProfile)
      ? rootSubject : profile.client(context.humanPrincipal.clientId))
      && typeof context.humanPrincipal.subject === 'string' && context.humanPrincipal.subject === context.rootPrincipal?.accountId,
      'human_identity_subject_mismatch', 403);
    const iat = Math.floor(now / 1000), exp = Math.floor(Math.min(now + 10000, context.expiresAt) / 1000);
    require(exp > iat, 'human_identity_authority_expired', 401);
    const witness = await sign({ iss: profile.issuer, sub: context.humanPrincipal.subject,
      aud: 'urn:soty:source-authority', iat, exp, sourceChallenge, contextDigest: digest(context) }, 'soty-source-authority+jwt');
    require(exp * 1000 > clock(), 'human_identity_authority_expired', 401);
    return witness;
  };
}

/** Private Apps host port: only the host may derive these fields from a fresh access decision. */
export function createRelayIdentitySigner({ profile, clock = Date.now }) {
  require(profile?.enabled && typeof clock === 'function', 'human_identity_disabled', 503);
  const sign = issuerSigner(profile);
  return async function signRelayIdentity({ appId, accountId, method, path, expiresAt }) {
    const now = clock();
    require(/^app-[a-f0-9]{32}$/u.test(appId) && typeof accountId === 'string' && accountId.length > 0
      && accountId.length <= 128 && Number.isSafeInteger(now) && Number.isSafeInteger(expiresAt), 'human_identity_subject_mismatch', 403);
    const iat = Math.floor(now / 1000), exp = Math.floor(Math.min(now + 10000, expiresAt) / 1000);
    require(exp > iat, 'human_identity_authority_expired', 401);
    const requestDigest = relayRequestDigest(method, path);
    const proof = await sign({ iss: profile.issuer, sub: accountId, aud: 'urn:soty:app:' + appId, iat, exp,
      jti: randomBytes(24).toString('base64url'), requestDigest }, 'soty-relay-identity+jwt');
    require(exp * 1000 > clock(), 'human_identity_authority_expired', 401);
    return proof;
  };
}
