import test from 'node:test';
import assert from 'node:assert/strict';
import { pinnedHistory, verifyPinnedHistory } from './support/pinned-history.mjs';

test('historical compatibility fixtures have the exact pinned Git blob and SHA-256', () => {
  for (const [commit, sources] of [
    ['e371d14be0aba67892a389f630a6c029f298ac77', ['server/connector-store.js','server/connector-persistence.js','server/connector-model-authorization.js']],
    ['2b770d8c2c34ae513e83a1c5bd489f1414c36b3b', ['public/agent/soty-connector.mjs','public/agent/manifest.json']],
    ['ff507100b3645f77e6a7fb84cbc0ad913ab3e34c', ['public/agent/soty-connector.mjs','public/agent/manifest.json']],
  ]) for (const source of sources) assert.ok(pinnedHistory(commit, source).length > 0);
});

test('unknown or corrupted historical fixture bytes fail closed', () => {
  const commit = 'e371d14be0aba67892a389f630a6c029f298ac77', source = 'server/connector-store.js';
  assert.throws(() => pinnedHistory(commit, '../connector-store.js'), /historical_fixture_unknown/);
  assert.throws(() => pinnedHistory('0000000000000000000000000000000000000000', source), /historical_fixture_unknown/);
  assert.throws(() => pinnedHistory(`${commit}/server`, 'connector-store.js'), /historical_fixture_unknown/);
  const changed = Buffer.from(pinnedHistory(commit, source)); changed[0] ^= 1;
  assert.throws(() => verifyPinnedHistory(commit, source, changed), /historical_fixture_corrupt/);
});
