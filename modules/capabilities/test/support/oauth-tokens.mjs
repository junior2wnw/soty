import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { fork } from 'node:child_process';
import { connectionsFixture } from './oauth-connections.mjs';
import { sha, CLIENT, ORIGIN } from './oauth-artifacts.mjs';

export { sha, CLIENT, ORIGIN };
export const opaque = () => randomBytes(32).toString('base64url');
export async function tokensFixture(t) {
  const f = await connectionsFixture(t);
  f.family = async () => {
    const approval = await f.approve(), binding = f.oauth.beginGrantBinding(approval.bindingArgs);
    const grant = f.save(binding); f.oauth.endGrantBinding(binding.context);
    return { ...binding.connection, grant };
  };
  f.primary = await f.family();
  f.payload = (model, own = f.primary, overrides = {}) => {
    const iat = Math.floor(f.now() / 1000), jti = opaque();
    return { iat, exp: Math.min(iat + ({ AuthorizationCode: 60, RefreshToken: 86400, AccessToken: 300 })[model], own.grant.exp),
      jti, kind: model, accountId: own.accountId, clientId: own.staticClientId, grantId: own.grant.jti,
      scope: 'notes.createDraft', expiresWithSession: false,
      ...(model === 'AuthorizationCode' ? { resource: own.resource, codeChallenge: opaque(),
        codeChallengeMethod: 'S256', redirectUri: 'http://127.0.0.1:19876/callback', authTime: iat }
        : model === 'RefreshToken' ? { resource: own.resource, gty: 'authorization_code', iiat: iat, rotations: 0 }
          : { aud: own.resource, gty: 'authorization_code', extra: {} }), ...overrides };
  };
  f.request = (model, own = f.primary, consuming = false) => ({ clientId: own.staticClientId,
    resource: own.resource, scope: 'notes.createDraft',
    ...(consuming ? { grantType: model === 'AuthorizationCode' ? 'authorization_code' : 'refresh_token' }
      : model === 'AuthorizationCode' ? {} : { grantType: 'authorization_code' }) });
  f.put = (payload, request = f.request(payload.kind), extra = {}) => f.oauth.artifactStore.upsert({
    model: payload.kind, id: payload.jti, payload, request, ...extra });
  f.find = payload => f.oauth.artifactStore.find({ model: payload.kind, id: payload.jti });
  f.consume = (payload, request = f.request(payload.kind, f.primary, true)) => f.oauth.artifactStore.consume({
    model: payload.kind, id: payload.jti, request });
  f.artifact = payload => f.db.prepare('SELECT * FROM cap_oauth_artifacts WHERE model=? AND id_hash=?').get(payload.kind, sha(payload.jti));
  f.link = payload => f.db.prepare(`SELECT l.*,k.revoked_at FROM cap_oauth_credentials l
    JOIN cap_credentials k ON k.id=l.credential_id WHERE l.token_digest=?`).get(sha(payload.jti));
  return f;
}

// Real children own independent Connect/Caps SQLite handles. The barrier is
// after both constructors; completion waits for actual process exit, not IPC.
export async function raceWriters(t, fixture, actions) {
  const children = [], results = [];
  t.after(() => { for (const child of children) if (child.exitCode === null) child.kill(); });
  const participants = actions.map(action => {
    const child = fork(new URL('./oauth-token-worker.mjs', import.meta.url), [], { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
    children.push(child); let outcome, readyResolve, readyReject, closeResolve, closeReject;
    const ready = new Promise((resolve, reject) => { readyResolve = resolve; readyReject = reject; });
    const closed = new Promise((resolve, reject) => { closeResolve = resolve; closeReject = reject; });
    // Observe even a startup failure before the caller reaches Promise.all.
    void closed.catch(() => {});
    const timer = setTimeout(() => { child.kill(); const error = new Error('OAuth worker exceeded 10 seconds');
      readyReject(error); closeReject(error); }, 10000);
    child.on('error', error => { readyReject(error); closeReject(error); });
    child.on('message', message => {
      if (message.ready) readyResolve();
      else if (message.result) outcome = message.result;
      else if (message.failure) { const error = new Error('OAuth worker failed: ' + message.failure); readyReject(error); closeReject(error); }
    });
    child.on('exit', code => {
      clearTimeout(timer);
      if (code !== 0 || !outcome) { const error = new Error('OAuth worker did not complete cleanly'); readyReject(error); closeReject(error); }
      else { results.push(outcome); closeResolve(); }
    });
    child.send({ initialize: true, files: fixture.files, now: fixture.now(), action });
    return { child, ready, closed };
  });
  await Promise.all(participants.map(item => item.ready));
  for (const item of participants) item.child.send({ run: true });
  await Promise.all(participants.map(item => item.closed));
  assert.equal(results.length, actions.length); return results;
}
