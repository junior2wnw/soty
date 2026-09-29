import test from 'node:test';
import assert from 'node:assert/strict';
import { resolveJobExecutor } from './agent-modules/executor-policy.mjs';

test('a future, missing or prototype-named wire operation cannot gain an AI or shell executor', () => {
  for (const kind of [undefined, null, '', 'connector-http', 'constructor', '__proto__', 'toString', ['agent'], { toString: () => 'agent' }]) {
    assert.equal(resolveJobExecutor({ kind, input: { text: 'must not execute' } }), null);
  }
  for (const input of [null, undefined, [], 'command']) assert.equal(resolveJobExecutor({ kind: 'agent', input }), null);
});

test('conflicting outer and inner operations fail closed while explicit legacy chat remains compatible', () => {
  assert.equal(resolveJobExecutor({ kind: 'command', input: { kind: 'agent' } }), null);
  assert.equal(resolveJobExecutor({ kind: 'agent', input: { kind: 'script' } }), null);
  assert.equal(resolveJobExecutor({ kind: 'agent', input: { kind: 'connector-http' } }), null);
  assert.equal(resolveJobExecutor({ kind: 'chat', input: { kind: 'agent' } }), 'agent');
  assert.equal(resolveJobExecutor({ kind: 'agent', input: { kind: 'chat' } }), 'agent');
  for (const kind of ['agent', 'command', 'script']) assert.equal(resolveJobExecutor({ kind, input: { kind } }), kind);
});
