import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { initializeCapabilitiesSchema } from '../../server/schema.mjs';
import { normalizeOAuthConfiguration } from '../../server/oauth-profile.mjs';
import { createOAuthArtifactStore } from '../../server/oauth-artifacts.mjs';

export const PROJECT = 'oauth-artifact-tests', ORIGIN = 'https://oauth-artifacts.test';
export const NOW = 1800000000123, CLIENT = 'soty-codex-cli';
export const sha = value => createHash('sha256').update(value).digest('hex');
export const code = value => error => error?.code === value;
export const id = value => sha(value).slice(0, 32);
export function config(overrides = {}) {
  return { issuer: ORIGIN + '/oauth', resources: { http: ORIGIN, mcp: ORIGIN + '/mcp' },
    withAuthorityFence: fn => fn(), // Never used by this non-authoritative increment.
    isRegisteredRedirect: ({ clientId, redirectUri }) => clientId === CLIENT
      && /^http:\/\/127\.0\.0\.1:[1-9][0-9]{0,4}\/callback$/u.test(redirectUri),
    artifactKey: Buffer.alloc(32, 23), artifactKeyId: 'fixture-key', ...overrides };
}
export function session(name = 'session', time = NOW) {
  const iat = Math.floor(time / 1000);
  return { kind: 'Session', jti: id(name), uid: id(name + '-uid'), iat, exp: iat + 600 };
}
export function interaction(name = 'interaction', time = NOW) {
  const iat = Math.floor(time / 1000), jti = id(name);
  return { kind: 'Interaction', jti, cid: id(name + '-cid'), iat, exp: iat + 600,
    params: { client_id: CLIENT, redirect_uri: 'http://127.0.0.1:19876/callback', response_type: 'code', scope: 'notes.createDraft',
      resource: ORIGIN + '/mcp', code_challenge: 'a'.repeat(43), code_challenge_method: 'S256', state: 'private-state-plaintext-canary' },
    prompt: { name: 'login', reasons: ['no_session'], details: {} }, returnTo: ORIGIN + '/oauth/authorize/' + jti };
}
export function fixture(t, { file = false, version = 3, configuration = config(), clock } = {}) {
  const parent = file ? realpathSync(tmpdir()) : null;
  const directory = file ? mkdtempSync(path.join(parent, 'soty-oauth-artifacts-')) : null;
  const databasePath = file ? path.join(directory, 'capabilities.sqlite') : ':memory:';
  const handles = new Set(), stores = new Set(); let time = NOW;
  function open() { const handle = new DatabaseSync(databasePath); handles.add(handle); return handle; }
  const db = open();
  let storage = initializeCapabilitiesSchema(db, { projectId: PROJECT, allowNativeMigration: version >= 2 });
  if (version === 3) storage = initializeCapabilitiesSchema(db, { projectId: PROJECT, allowOAuthMigration: true });
  function close(handle) { if (handles.delete(handle)) handle.close(); }
  function store({ handle = db, config: cfg = configuration, ...overrides } = {}) {
    const instance = createOAuthArtifactStore({ db: handle, projectId: PROJECT, ...storage,
      clock: clock ?? (() => time), ensureOpen: () => assert.ok(handles.has(handle)),
      configuration: normalizeOAuthConfiguration(cfg),
      transaction(fn, { busyMs } = {}) {
        assert.equal(handle.isTransaction, false);
        const old = handle.prepare('PRAGMA busy_timeout').get().timeout;
        try {
          if (busyMs !== undefined) handle.exec(`PRAGMA busy_timeout=${busyMs}`);
          handle.exec('BEGIN IMMEDIATE');
          const result = fn();
          assert.ok(!result || typeof result.then !== 'function');
          handle.exec('COMMIT'); return result;
        } catch (error) { if (handle.isTransaction) handle.exec('ROLLBACK'); throw error; }
        finally { handle.exec(`PRAGMA busy_timeout=${old}`); }
      }, ...overrides });
    stores.add(instance); return instance;
  }
  t.after(() => {
    for (const instance of stores) instance.close();
    for (const handle of [...handles]) close(handle);
    if (directory) {
      const actual = realpathSync(directory);
      assert.equal(path.dirname(actual), parent); assert.match(path.basename(actual), /^soty-oauth-artifacts-/u);
      rmSync(actual, { recursive: true });
    }
  });
  return { db, storage, databasePath, open, close, store, advance(ms) { time += ms; }, now() { return time; } };
}
