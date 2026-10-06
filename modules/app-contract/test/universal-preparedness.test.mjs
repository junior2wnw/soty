import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { captureUniversalPreparedness, validateUniversalPreparedness } from '../universal-preparedness.mjs';
import { captureUniversalPreparedness as capturePolicyPreparedness } from '../../../deploy/connector/universal-policy.mjs';
import { createHumanIdentityHostProfile } from '../../human-identity/profile.mjs';
import { createReviewsService } from '../../reviews/server/index.mjs';

const origin = 'https://runtime.fixture.invalid', random = () => randomBytes(32).toString('base64url');
const privateJwk = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
Object.assign(privateJwk, { kid: 'fixture-runtime', alg: 'RS256', use: 'sig' });
function options() {
  return { enabled: true, issuer: origin + '/human-identity', registryId: 'soty', environmentId: 'production',
    clients: [{ id: 'app.alpha', label: 'App alpha', redirectUri: 'https://alpha.fixture.invalid/callback', version: 2, clientSecret: random() }],
    jwks: { keys: [privateJwk] }, cookieKeys: [random()], artifactKey: randomBytes(32), artifactKeyId: 'fixture-runtime' };
}
const profile = value => createHumanIdentityHostProfile(value, { shellOrigins: [origin] });
const capture = humanProfile => captureUniversalPreparedness({ compiledLegacyMode: false, universalConfigured: true, reviewsConfigured: true,
  humanHttpEnabled: Boolean(humanProfile?.enabled), humanProfile });

test('pure runtime capture and deploy re-export preserve exact default/v1 measurement and frozen bounded output', () => {
  const inputs = { compiledLegacyMode: true, universalConfigured: false, reviewsConfigured: false, humanProfile: null, humanHttpEnabled: false };
  const dto = captureUniversalPreparedness(inputs); assert.deepEqual(dto, capturePolicyPreparedness(inputs));
  assert.equal(dto.schema, 'soty.universal-preparedness.v1'); assert.deepEqual(dto.human, { configured: false });
  assert.equal(Object.isFrozen(dto.reviews), true); assert.equal(dto.reviews.providerCount, 0);
  const host = profile(options()), old = capture(host); assert.equal(old.human.renewal, undefined);
  assert.deepEqual(old, capturePolicyPreparedness({ ...inputs, compiledLegacyMode: false, universalConfigured: true, reviewsConfigured: true,
    humanHttpEnabled: true, humanProfile: host })); assert.equal(Buffer.byteLength(JSON.stringify(old)) < 65536, true);
});

test('v2 eligibility and operational admission off are measured without changing client pins or private fingerprinting', () => {
  const config = options(), on = profile({ ...config, renewal: { admissionEnabled: true, clientIds: ['app.alpha'] } });
  const off = profile({ ...config, renewal: { admissionEnabled: false, clientIds: ['app.alpha'] } });
  const a = capture(on), b = capture(off); assert.equal(a.human.clientsDigest, b.human.clientsDigest);
  assert.equal(a.human.renewal.admissionEnabled, true); assert.equal(b.human.renewal.admissionEnabled, false);
  assert.equal(b.human.renewal.maximumSessionSeconds, 86400); assert.equal(b.human.renewal.eligibleClientCount, 1);
  const rotated = profile({ ...config, clients: config.clients.map(client => ({ ...client, clientSecret: random() })), cookieKeys: [random()],
    artifactKey: randomBytes(32), artifactKeyId: 'another-private-key-id', renewal: { admissionEnabled: true, clientIds: ['app.alpha'] } });
  assert.deepEqual(capture(rotated), a);
  const text = JSON.stringify(a);
  for (const secret of [config.clients[0].clientSecret, config.cookieKeys[0], config.artifactKey.toString('base64url'), config.artifactKeyId, privateJwk.d]) {
    assert.equal(text.includes(secret), false);
  }
});

test('actual reviews service produces immutable scoped host measurement without exposing bindings or following input mutation', () => {
  const providerRef = { id: 'reviews:povedai', version: 1, digest: '1'.repeat(64) }, subjectRef = { id: 'subject:alpha', version: 1, digest: '2'.repeat(64) };
  const configuration = { providers: [{ ...providerRef, origin: 'https://reviews.fixture.invalid' }], bindings: [{
    scope: { registryId: 'soty', tenantId: 'private-owner', appId: 'private-app', environmentId: 'production' },
    localSubject: { kind: 'app', id: 'private-app' }, providerRef, subjectRef, providerSubjectId: 'subject_' + '3'.repeat(32), providerEntityType: 'product', mode: 'public-read' }] };
  const service = createReviewsService({ configuration, actorActive: () => false, withAppAuthority: () => null });
  try {
    const measured = service.preparedness(), legacy = captureUniversalPreparedness({ compiledLegacyMode: false, universalConfigured: true,
      reviewsConfigured: true, humanHttpEnabled: false, humanProfile: null, reviewsConfiguration: configuration });
    configuration.bindings[0].scope.appId = 'changed-private-app'; configuration.providers.length = 0;
    assert.equal(service.preparedness(), measured); assert.equal(Object.isFrozen(measured), true);
    const dto = captureUniversalPreparedness({ compiledLegacyMode: false, universalConfigured: true, reviewsConfigured: true,
      humanHttpEnabled: false, humanProfile: null, reviewsPreparedness: measured }); assert.deepEqual(dto.reviews, legacy.reviews);
    assert.equal(measured.providerCount, 1); assert.equal(measured.bindingCount, 1);
    for (const privateValue of ['private-owner', 'private-app', subjectRef.id, configuration.bindings[0].providerSubjectId]) assert.equal(JSON.stringify(dto).includes(privateValue), false);
    service.close(); assert.throws(() => service.preparedness(), error => error.code === 'reviews_closed');
  } finally { service.close(); }
});

test('measurement output is closed and rejects extra credentials, accessors, async/contradictory states before serialization', () => {
  const dto = capture(null), malformed = [ { ...dto, token: 'private-token' }, { ...dto, humanHttpEnabled: true },
    { ...dto, compiledLegacyMode: true }, { ...dto, human: { configured: false, artifactKey: 'private-key' } },
    { ...dto, reviews: { ...dto.reviews, bindings: [] } }, { ...dto, reviews: { ...dto.reviews, providerCount: 129 } } ];
  for (const value of malformed) assert.throws(() => validateUniversalPreparedness(value), error => error.code === 'universal_policy_runtime_invalid' && !error.cause);
  let called = 0; const accessor = { ...dto }; Object.defineProperty(accessor, 'human', { enumerable: true, get() { called++; return dto.human; } });
  assert.throws(() => validateUniversalPreparedness(accessor)); assert.equal(called, 0);
  const nested = {}; Object.defineProperty(nested, 'configured', { enumerable: true, get() { called++; return false; } });
  assert.throws(() => validateUniversalPreparedness({ ...dto, human: nested })); assert.equal(called, 0);
  assert.throws(() => validateUniversalPreparedness(Promise.resolve(dto)));
  assert.throws(() => captureUniversalPreparedness({ compiledLegacyMode: false, universalConfigured: false, reviewsConfigured: true, humanHttpEnabled: false, humanProfile: null }));
});

test('runtime capture has no dependency on deployment modules or operating-system IO', () => {
  const source = readFileSync(new URL('../universal-preparedness.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /from\s+['"][^'"]*(?:deploy\/|node:(?:fs|net|child_process|sqlite))/u);
});
