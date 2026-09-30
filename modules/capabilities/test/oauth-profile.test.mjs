import test from 'node:test';
import assert from 'node:assert/strict';
import { normalizeOAuthConfiguration, createOAuthUnboundProfile, canonicalOAuthJson, oauthRedirect } from '../server/oauth-profile.mjs';
import { createOAuthArtifactCodec } from '../server/oauth-crypto.mjs';
import { config, session, interaction, code, id, sha, NOW, ORIGIN } from './support/oauth-artifacts.mjs';

const profile = options => createOAuthUnboundProfile(normalizeOAuthConfiguration(config(options)));
const snap = (payload, options = {}) => profile().snapshot({ model: payload.kind, id: payload.jti, payload, nowMs: NOW, ...options });

test('configuration is closed, captures callbacks/resources and copies the optional key', () => {
  const raw = config(), captured = normalizeOAuthConfiguration(raw), originalKey = Buffer.from(raw.artifactKey);
  raw.artifactKey.fill(0); raw.resources.mcp = 'https://foreign.invalid'; raw.isRegisteredRedirect = () => false;
  assert.deepEqual(captured.artifactKey, originalKey); assert.equal(captured.resources.mcp, ORIGIN + '/mcp');
  assert.equal(captured.isRegisteredRedirect({ clientId: 'soty-codex-cli', redirectUri: 'http://127.0.0.1:80/callback' }), true);
  assert.equal(normalizeOAuthConfiguration(undefined), null);
  const noKey = config(); delete noKey.artifactKey; delete noKey.artifactKeyId;
  assert.equal(normalizeOAuthConfiguration(noKey).artifactKey, null);
  let getterRuns = 0;
  const getter = config(); Object.defineProperty(getter, 'artifactKey', { enumerable: true, get() { getterRuns++; return Buffer.alloc(32); } });
  for (const value of [getter, config({ extra: undefined }), config({ artifactKey: null }), config({ artifactKey: Buffer.alloc(31) }),
    config({ artifactKeyId: undefined }), config({ withAuthorityFence: async fn => fn() }), config({ issuer: ORIGIN + '/else' }),
    config({ resources: { http: ORIGIN, mcp: ORIGIN + '/foreign' } })]) {
    assert.throws(() => normalizeOAuthConfiguration(value), code('oauth_configuration_invalid'));
  }
  assert.equal(getterRuns, 0);
});

test('payload canonicalization is bounded while preserving well-formed Unicode and omitting optional undefined', () => {
  const value = { v: 'x'.repeat(16376) };
  assert.equal(Buffer.byteLength(canonicalOAuthJson(value)), 16384);
  assert.throws(() => canonicalOAuthJson({ v: value.v + 'x' }), code('oauth_invalid_artifact'));
  assert.equal(canonicalOAuthJson({ z: undefined, b: '😀е\u0301', a: 'é' }), '{"a":"é","b":"😀е́"}');
  let nested = {}; for (let i = 0; i < 13; i++) nested = { next: nested };
  const sparse = []; sparse.length = 1;
  for (const bad of [nested, Array(1024).fill(0), sparse, ['\ud800'], [undefined], { value: Infinity }, new Date(),
    { toJSON() { return {}; } }, JSON.parse('{"__proto__":{}}')]) {
    assert.throws(() => canonicalOAuthJson(bad), code('oauth_invalid_artifact'));
  }
  let calls = 0; const accessor = Object.defineProperty({}, 'value', { enumerable: true, get() { calls++; return 'secret'; } });
  assert.throws(() => canonicalOAuthJson(accessor), code('oauth_invalid_artifact')); assert.equal(calls, 0);
});

test('Session original second-based window remains absolute at a non-aligned admission time', () => {
  const initial = session();
  const first = snap(initial);
  assert.equal(first.createdAt, NOW - 123); assert.equal(first.retainUntil, NOW - 123 + 600000);
  const reset = { ...initial, jti: id('reset') };
  assert.equal(snap(reset, { nowMs: NOW + 10000, expiresIn: 590 }).createdAt, first.createdAt);
  for (const value of [{ ...reset, exp: initial.exp + 10 }, { ...initial, iat: initial.iat + 1 },
    { ...initial, iat: String(initial.iat) }, { ...initial, kind: 'Grant' }, { ...initial, uid: ['array'] },
    { ...initial, unsupported: undefined }]) assert.throws(() => snap(value), error => ['oauth_invalid_artifact', 'oauth_unavailable'].includes(error.code));
  assert.throws(() => snap(initial, { nowMs: initial.exp * 1000 }), code('oauth_invalid_artifact'));
  for (const expiresIn of [0, 601, '600', Infinity]) assert.throws(() => snap(initial, { expiresIn }), code('oauth_invalid_artifact'));
});

test('initial Interaction needs no result and rejects disabled protocol fields, wrong routes and mixed result authority', () => {
  const value = interaction();
  assert.deepEqual(snap(value).payload, value);
  const optional = { ...value, result: undefined, session: undefined, trusted: undefined,
    prompt: { ...value.prompt, details: { missingOIDCScope: undefined } } };
  assert.deepEqual(snap(optional).payload, value);
  assert.deepEqual(snap({ ...value, trusted: [] }).payload.trusted, []);
  const variants = [
    { ...value, deviceCode: undefined }, { ...value, returnTo: ORIGIN + '/oauth/authorize/' + id('other') },
    { ...value, returnTo: ORIGIN + '/oauth/auth/' + value.jti },
    { ...value, params: { ...value.params, resource: [ORIGIN + '/mcp'] } },
    { ...value, params: { ...value.params, scope: 'openid notes.createDraft' } },
    { ...value, result: { login: { accountId: 'account_1' }, error: 'access_denied' } },
    { ...value, result: { login: { accountId: 'account_1', unknown: undefined } } },
    { ...value, params: { ...value.params, state: '\ud800' } },
    { ...value, trusted: false }, { ...value, trusted: true },
    { ...value, trusted: ['client_id'] }, { ...value, trusted: [false] },
  ];
  for (const bad of variants) assert.throws(() => snap(bad), code('oauth_invalid_artifact'));
  assert.equal(snap({ ...value, result: { error: 'access_denied' } }).payload.result.error, 'access_denied');
  assert.throws(() => profile({ isRegisteredRedirect: () => false }).snapshot({ model: value.kind, id: value.jti, payload: value, nowMs: NOW }), code('oauth_invalid_artifact'));
});

test('native redirect structural validation keeps explicit :80 and rejects ambiguous port/authority forms', () => {
  for (const value of ['http://127.0.0.1:80/callback', 'http://127.0.0.1:65535/callback', 'http://localhost:1/callback', 'http://[::1]:80/callback']) {
    assert.equal(oauthRedirect(value), value);
  }
  for (const value of ['http://127.0.0.1/callback', 'http://127.0.0.1:080/callback', 'http://127.0.0.1:65536/callback',
    'http://127.0.0.1:0/callback', 'http://name@127.0.0.1:80/callback', 'http://127.0.0.1:80/callback#x',
    'http://127.0.0.1:80/callback?x', 'http://127.0.0.1:80/a/../callback', 'https://127.0.0.1:80/callback', 'http://127.0.0.2:80/callback']) {
    assert.throws(() => oauthRedirect(value), code('oauth_invalid_artifact'));
  }
});

test('rejected Promise from a trusted redirect callback is contained and never becomes an admission', async () => {
  const value = interaction();
  const invalid = profile({ isRegisteredRedirect: () => Promise.reject(new Error('synthetic wiring failure')) });
  assert.throws(() => invalid.snapshot({ model: value.kind, id: value.jti, payload: value, nowMs: NOW }), code('oauth_context_invalid'));
  await new Promise(resolve => setImmediate(resolve));
});

test('AEAD binds every registry/issuer/model/id/profile/key pin and rejects corruption without plaintext output', () => {
  const options = { registryId: '1'.repeat(32), issuer: ORIGIN + '/oauth', artifactKey: Buffer.alloc(32, 9), artifactKeyId: 'test-key' };
  const codec = createOAuthArtifactCodec(options), payload = session(), idHash = sha(payload.jti);
  options.artifactKey.fill(1);
  const sealed = codec.seal({ model: 'Session', idHash, payload }), other = codec.seal({ model: 'Session', idHash, payload });
  assert.notDeepEqual(sealed.payloadCipher, other.payloadCipher); assert.equal(sealed.payloadDigest, other.payloadDigest);
  const args = { model: 'Session', idHash, ...sealed };
  assert.deepEqual(codec.open(args), payload);
  const modified = codec.open(args); modified.uid = id('changed'); assert.deepEqual(codec.open(args), payload);
  for (const change of [{ model: 'Interaction' }, { idHash: sha('other') }, { profile: 'other' }, { payloadDigest: '0'.repeat(64) },
    { payloadCipher: Buffer.from(sealed.payloadCipher).fill(0, 12, 28) }]) {
    assert.throws(() => codec.open({ ...args, ...change }), code('capabilities_storage_corrupt'));
  }
  assert.throws(() => codec.open({ ...args, keyId: 'other-key' }), code('oauth_storage_key_unavailable'));
  for (const change of [{ registryId: '2'.repeat(32) }, { issuer: 'https://other.test/oauth' }, { artifactKey: Buffer.alloc(32, 7) }]) {
    const foreign = createOAuthArtifactCodec({ ...options, artifactKey: Buffer.alloc(32, 9), ...change });
    assert.throws(() => foreign.open(args), code('capabilities_storage_corrupt')); foreign.close();
  }
  codec.close(); assert.equal(codec.available(), false);
  assert.throws(() => codec.open(args), code('oauth_storage_key_unavailable'));
});
