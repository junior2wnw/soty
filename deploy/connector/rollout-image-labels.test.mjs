import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture, args, original } from './test-support/rollout-engine.mjs';
import { createConfig } from './rollout.mjs';
import { storageReaderLabel, currentStorageReaders } from './storage-guard.mjs';
import { universalModeLabel } from './universal-policy.mjs';

const rolloutLabel = 'io.soty.connector-rollout';
const defaults = { 'io.soty.task': 'synthetic-qualified-build', owner: 'image-owner', 'io.soty.app': 'image-default-app',
  [storageReaderLabel]: currentStorageReaders, [universalModeLabel]: '0', 'io.soty.account-ai': '1',
  'org.opencontainers.image.revision': args.revision, [rolloutLabel]: 'image-default-transaction' };

function dockerLabelsFixture(change = () => {}) {
  const f = fixture(), image = f.engine.image, create = f.engine.create;
  f.engine.image = async id => { const result = await image(id); if (id === args.candidateImage) result.Config.Labels = structuredClone(defaults); return result; };
  // Docker merges immutable image labels with the explicit CREATE body.
  f.engine.create = async (name, body) => { const merged = { ...structuredClone(body), Labels: { ...defaults, ...body.Labels } };
    change(merged); return create(name, merged); };
  return f;
}

test('prospective candidate includes exact image label defaults while captured app labels and rollout pins win', async () => {
  const f = dockerLabelsFixture();
  f.map.get(args.originalId).Config.Labels['io.soty.app'] = 'existing-app';
  await f.run.prepare(args);
  assert.equal(f.run.config.Labels['io.soty.task'], defaults['io.soty.task']);
  assert.equal(f.run.candidate.Config.Labels['io.soty.task'], defaults['io.soty.task']);
  assert.equal(f.run.candidate.Config.Labels.owner, 'original');
  assert.equal(f.run.candidate.Config.Labels['io.soty.app'], 'existing-app');
  assert.equal(f.run.candidate.Config.Labels[rolloutLabel], args.transaction);
  assert.equal(f.run.candidate.Config.Labels[rolloutLabel + '.original'], args.originalId);
  assert.equal(f.run.candidate.Config.Labels['org.opencontainers.image.revision'], args.revision);
  for (const key of [storageReaderLabel, universalModeLabel, 'io.soty.account-ai']) {
    assert.equal(f.run.config.Labels[key], undefined, 'image-owned metadata is not inherited as a container override');
    assert.equal(f.run.candidate.Config.Labels[key], defaults[key]);
  }
  assert.ok(!f.events.some(event => event.startsWith('stop:')));
  await f.run.promote(); assert.equal(f.run.state.phase, 'committed');
});

test('createConfig retains its old no-default contract and only overlays explicitly supplied immutable image labels', () => {
  const old = original(); old.Config.Labels[storageReaderLabel] = 'old-reader';
  old.Config.Labels[universalModeLabel] = 'old-mode'; old.Config.Labels['io.soty.account-ai'] = 'old-ai';
  const copied = structuredClone(old), supplied = structuredClone(defaults);
  const result = createConfig(old, args.candidateImage, args.transaction, args.revision, undefined, undefined, supplied);
  assert.equal(result.Labels['io.soty.task'], defaults['io.soty.task']); assert.equal(result.Labels.owner, 'original');
  for (const key of [storageReaderLabel, universalModeLabel, 'io.soty.account-ai']) assert.equal(result.Labels[key], undefined);
  assert.equal(createConfig(old, args.candidateImage, args.transaction).Labels['io.soty.task'], undefined);
  assert.deepEqual(old, copied); assert.deepEqual(supplied, defaults);
});

for (const [name, change] of [
  ['missing image default', body => { delete body.Labels['io.soty.task']; }],
  ['changed image default', body => { body.Labels['io.soty.task'] = 'unreviewed-build'; }],
  ['unexpected actual label', body => { body.Labels.unreviewed = 'extra'; }],
  ['changed captured app label', body => { body.Labels.owner = 'another-owner'; }],
  ['image ENV default', body => { body.Env.push('IMAGE_ONLY_SETTING=unexpected'); }],
]) test(name + ' remains closed before original stop', async () => {
  const f = dockerLabelsFixture(change);
  await assert.rejects(f.run.prepare(args), { code: 'candidate_configuration_mismatch' });
  assert.equal(f.map.get(args.originalId).State.Running, true);
  assert.ok(!f.events.some(event => event.startsWith('stop:')));
});

test('late image-default label drift on a prepared candidate is rejected before stop', async () => {
  const f = dockerLabelsFixture(); await f.run.prepare(args);
  f.map.get(f.run.candidate.Id).Config.Labels['io.soty.task'] = 'changed-after-prepare';
  await assert.rejects(f.run.promote(), { code: 'candidate_configuration_mismatch' });
  assert.equal(f.map.get(args.originalId).State.Running, true); assert.ok(!f.events.some(event => event.startsWith('stop:')));
});
