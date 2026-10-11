import test from 'node:test';
import assert from 'node:assert/strict';
import { createBatch } from '../src/batch.mjs';
test('batch counts completed, verified, repeated and unidentified writes independently', () => {
  const batch = createBatch();
  assert.equal(batch.add({ written: false }), false);
  batch.add({ written: true, verified: false });
  batch.add({ written: true, verified: true, result: { serialNumber: 'AA:01' } });
  batch.add({ written: true, verified: true, result: { serialNumber: 'aa:01' } });
  batch.add({ written: true, verified: true, result: { serialNumber: '' } });
  assert.deepEqual(batch.counts, { verified: 1, unverified: 1, repeats: 1, withoutSerial: 1 });
  assert.equal('serialNumbers' in batch.report(), false);
  assert.deepEqual(batch.report({ includeIdentifiers: true }).serialNumbers, ['aa:01']);
  const counts = batch.counts; counts.verified = 99; assert.equal(batch.counts.verified, 1);
});
