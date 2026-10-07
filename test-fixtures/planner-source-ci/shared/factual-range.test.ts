import { test } from 'node:test';
import assert from 'node:assert/strict';
import { factualRangeForDrawing } from './factual-range.js';
import type { TimeRange } from './types.js';
import { canonicalRange, rangeFromNs } from './precise-time.js';

test('partial facts draw only the recorded instant and preserve the original unknown boundaries', () => {
  const startOnly: TimeRange = {
    start: '2026-10-02T10:00:00Z',
    end: null,
    timezone: 'UTC',
    precision: 'exact',
  };
  const before = JSON.stringify(startOnly);
  const drawn = factualRangeForDrawing(startOnly);
  assert.equal(drawn.start, startOnly.start);
  assert.equal(drawn.end, startOnly.start);
  assert.equal(JSON.stringify(startOnly), before);
  const endOnly: TimeRange = { ...startOnly, start: null, end: startOnly.start };
  assert.deepEqual(factualRangeForDrawing(endOnly), drawn);
  const recorded: TimeRange = { ...startOnly, end: '2026-10-02T11:00:00Z' };
  assert.equal(factualRangeForDrawing(recorded), recorded);
  const unknown: TimeRange = { ...startOnly, start: null, precision: 'unknown' };
  assert.equal(factualRangeForDrawing(unknown), unknown);
});

test('a precise partial fact draws its exact anchor even when its ISO mirror is unavailable', () => {
  const coordinate = -31556952000000000000000001n;
  const recorded = rangeFromNs(null, coordinate);
  recorded.precise!.resolutionNs = '10';
  const before = structuredClone(recorded);
  const drawn = factualRangeForDrawing(recorded);
  assert.equal(canonicalRange(drawn).start, coordinate);
  assert.equal(canonicalRange(drawn).end, coordinate);
  assert.equal(drawn.precise?.resolutionNs, '10');
  assert.equal(drawn.start, null);
  assert.deepEqual(recorded, before);
});
