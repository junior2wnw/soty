import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, realpathSync, rmSync, readFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { initializeCapabilitiesSchema as historical } from '../fixtures/capabilities-v2/schema.mjs';
import { initializeCapabilitiesSchema } from '../../server/schema.mjs';

export const PROJECT = 'oauth-baseline-test', ORIGIN = 'https://oauth-baseline.test';
export const OWNER = Object.freeze({ accountId: 'account_oauth_owner', deviceId: 'device_oauth_owner' });
export const code = expected => error => error?.code === expected;
export const sha = value => createHash('sha256').update(value).digest('hex');
export function files(databasePath) {
  return Object.fromEntries(['', '-wal', '-shm'].map(suffix => [suffix,
    existsSync(databasePath + suffix) ? sha(readFileSync(databasePath + suffix)) : null]));
}
export function objects(db) {
  return db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all();
}
export function database(t, { version = 3 } = {}) {
  const parent = realpathSync(tmpdir()), directory = mkdtempSync(path.join(parent, 'soty-oauth-baseline-'));
  const databasePath = path.join(directory, 'capabilities.sqlite'), handles = [], disposers = [];
  const db = open();
  if (version > 0) historical(db, { projectId: PROJECT, allowNativeMigration: version >= 2 });
  if (version === 3) initializeCapabilitiesSchema(db, { projectId: PROJECT, allowOAuthMigration: true });
  function open() { const handle = new DatabaseSync(databasePath); handles.push(handle); return handle; }
  function close(handle) { const index = handles.indexOf(handle); if (index >= 0) { handles.splice(index, 1); handle.close(); } }
  t.after(() => {
    for (const dispose of disposers) dispose();
    for (const handle of [...handles]) close(handle);
    const actual = realpathSync(directory);
    assert.equal(path.dirname(actual), parent); assert.match(path.basename(actual), /^soty-oauth-baseline-/u);
    rmSync(actual, { recursive: true });
  });
  return { db, directory, databasePath, open, close, beforeClose(dispose) { disposers.push(dispose); } };
}

/** Explicit future OAuth rows for baseline policy tests. Cipher bytes are a
 * structural placeholder, NOT an AS token/crypto/consent implementation. */
export function seedOAuth(db, { suffix = 'a', accountId = OWNER.accountId, deviceId = OWNER.deviceId,
  createdAt = 1000, expiresAt = 86401000, bound = true, credential = true,
  staticClientId = 'soty-codex-cli', resource = ORIGIN, issuer = ORIGIN + '/oauth', beforeConnection } = {}) {
  const row = { id: 'oauth_' + suffix, clientId: 'client_' + suffix, principalId: 'principal_' + suffix,
    grantId: 'grant_' + suffix, accountId, deviceId, createdAt, expiresAt, staticClientId, resource, issuer,
    providerGrantId: sha('provider-grant-' + suffix).slice(0, 32), consentDigest: sha('consent-' + suffix) };
  const own = !db.isTransaction; if (own) db.exec('BEGIN IMMEDIATE');
  try {
    db.prepare("INSERT INTO cap_clients(id,account_id,label,state,created_at) VALUES(?,?,?,'active',?)")
      .run(row.clientId, accountId, 'Test-only future OAuth client', createdAt);
    db.prepare("INSERT INTO cap_principals(id,account_id,client_id,kind,label,state,creator_device_id,created_at) VALUES(?,?,?,'service',?,'active',?,?)")
      .run(row.principalId, accountId, row.clientId, 'Test-only future OAuth principal', deviceId, createdAt);
    db.prepare(`INSERT INTO cap_grants(id,account_id,client_id,principal_id,parent_id,root_id,creator_device_id,
      capabilities_json,resources_json,effects_json,recipients_json,allow_delegation,max_depth,depth,not_before,expires_at,created_at)
      VALUES(?,?,?,?,NULL,?,?,'[{"capabilityId":"notes.createDraft","version":1}]','["notes:new"]','["create"]','["soty:notes"]',0,0,0,?,?,?)`)
      .run(row.grantId, accountId, row.clientId, row.principalId, row.grantId, deviceId, createdAt, expiresAt, createdAt);
    db.prepare("INSERT INTO cap_budgets(root_grant_id,unit,limit_amount) VALUES(?,'invocations',20)").run(row.grantId);
    beforeConnection?.(row);
    db.prepare(`INSERT INTO cap_oauth_connections(id,account_id,client_id,principal_id,root_grant_id,creator_device_id,
      issuer,static_client_id,resource,scope,consent_digest,state,created_at,expires_at)
      VALUES(?,?,?,?,?,?,?,?,?,'notes.createDraft',?,'active',?,?)`)
      .run(row.id, accountId, row.clientId, row.principalId, row.grantId, deviceId, issuer, staticClientId, resource, row.consentDigest, createdAt, expiresAt);
    if (bound) {
      insertArtifact(db, { model: 'Grant', rawId: row.providerGrantId, connection: row, expiresAt });
      db.prepare('UPDATE cap_oauth_connections SET provider_grant_id=? WHERE id=?').run(row.providerGrantId, row.id);
    }
    if (credential) addCredential(db, row);
    if (own) db.exec('COMMIT');
  } catch (error) { if (own && db.isTransaction) db.exec('ROLLBACK'); throw error; }
  return row;
}
export function insertArtifact(db, { model, rawId, connection, createdAt = connection.createdAt,
  expiresAt, retainUntil = model === 'AccessToken' ? expiresAt : connection.expiresAt }) {
  db.prepare(`INSERT INTO cap_oauth_artifacts(model,id_hash,issuer,profile,key_id,payload_cipher,payload_digest,
    connection_id,provider_grant_id,created_at,expires_at,retain_until) VALUES(?,?,?,'oidc-provider-9.12.2-c1','synthetic',?,?,?,?,?,?,?)`)
    .run(model, sha(rawId), connection.issuer, Buffer.alloc(30, 7), sha('synthetic bounded payload'),
      connection.id, connection.providerGrantId, createdAt, expiresAt, retainUntil);
}
export function addCredential(db, connection, { id = 'credential_' + connection.id, token = randomBytes(32).toString('base64url'),
  createdAt = connection.createdAt, expiresAt = Math.min(createdAt + 300000, connection.expiresAt) } = {}) {
  const hash = sha(token);
  insertArtifact(db, { model: 'AccessToken', rawId: token, connection, createdAt, expiresAt });
  db.prepare(`INSERT INTO cap_credentials(id,digest,account_id,client_id,principal_id,grant_id,audience,expires_at,created_at)
    VALUES(?,?,?,?,?,?,?,?,?)`).run(id, hash, connection.accountId, connection.clientId, connection.principalId, connection.grantId, connection.resource, expiresAt, createdAt);
  db.prepare('INSERT INTO cap_oauth_credentials(credential_id,connection_id,token_digest,created_at,expires_at) VALUES(?,?,?,?,?)')
    .run(id, connection.id, hash, createdAt, expiresAt);
  return Object.assign(connection, { credentialId: id, token, tokenDigest: hash, credentialExpiresAt: expiresAt });
}
export function withDroppedGuard(db, name, change) {
  assert.match(name, /^cap_[a-z_]+$/u);
  const sql = db.prepare("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?").get(name).sql;
  db.exec(`DROP TRIGGER ${name}`);
  try { change(); } finally { db.exec(sql); }
}
export function insertInvocation(db, { id = 'inv_synthetic', accountId = OWNER.accountId, clientId = 'client_synthetic',
  principalId = 'principal_synthetic', grantId = 'grant_synthetic', credentialId = 'credential_synthetic', requestKey = sha(id),
  authorizationJson = JSON.stringify({ credentialId }) } = {}) {
  db.prepare(`INSERT INTO cap_invocations(id,account_id,client_id,principal_id,grant_id,root_grant_id,policy_epoch,
    capability_id,capability_version,capability_digest,request_key,request_digest,internal_request_id,input_json,
    target_json,authorization_json,status,created_at,updated_at)
    VALUES(?,?,?,?,?,?,1,'fixture.generic',1,?,?,?,?,'{}','{}',?,'queued',1000,1000)`)
    .run(id, accountId, clientId, principalId, grantId, grantId, sha('generic'), requestKey, sha(id + '-request'), 'request_' + id, authorizationJson);
}
export function seedInteraction(db, { redirect = 'http://127.0.0.1:8765/callback', suffix = 'a', connection } = {}) {
  const hash = sha('interaction-' + suffix);
  db.prepare(`INSERT INTO cap_oauth_interactions(uid_hash,issuer,static_client_id,resource,redirect_uri,request_digest,
    browser_nonce_hash,duration_ms,budget_limit,created_at,expires_at,decision)
    VALUES(?,?,?,?,?,?,?,86400000,20,900,600900,'pending')`)
    .run(hash, connection?.issuer ?? ORIGIN + '/oauth', connection?.staticClientId ?? 'soty-codex-cli',
      connection?.resource ?? ORIGIN, redirect, connection?.consentDigest ?? sha('consent-' + suffix), sha('nonce-' + suffix));
  return hash;
}
