import test from 'node:test';
import assert from 'node:assert/strict';
import {
  money, chargeTokens, ceilDiv, normalizeLimits, enforceLimit, fingerprint, bodyDigest,
  canonicalJson, normalizeUsage, csvCell, createSecret, safeEqual, periodDate,
  validateChatBody, estimateReservationTokens,
} from '../../packages/domain/index.mjs';

test('money is exact beyond IEEE-754, ceiling is applied once across input/output', () => {
  assert.equal(money('9007199254740993'), 9007199254740993n);
  assert.equal(chargeTokens(1, 1, '1', '1'), 1n);
  assert.equal(chargeTokens(1_000_000, 0, '23000000', '23000000'), 23000000n);
  assert.equal(ceilDiv(100n, 10n), 10n);
  for (const invalid of [1, -1, '01', '0.1', '-1', '1e6', '9000000000000000001']) assert.throws(() => money(invalid));
});

test('null means unbounded and zero is an actual deny limit', () => {
  assert.equal(normalizeLimits({ dailyMicrorubles: '0' }).dailyMicrorubles, '0');
  assert.equal(normalizeLimits({ dailyMicrorubles: null }).dailyMicrorubles, null);
  assert.throws(() => enforceLimit('0', 0, 1, 'denied', 'Budget'), error => error.code === 'denied');
  assert.doesNotThrow(() => enforceLimit(null, 99999, 1, 'denied', 'Budget'));
  assert.throws(() => normalizeLimits({ rpm: -1 }));
  assert.throws(() => normalizeLimits({ nonexistent: 1 }));
});

test('credential entropy, keyed fingerprints and canonical idempotency', () => {
  const pepper = 'test-only-pepper-32-bytes-at-least-aaaa';
  const a = createSecret(); const b = createSecret();
  assert.notEqual(a, b); assert.match(a, /^sk-soty-live-[\w-]{43}$/);
  assert.notEqual(fingerprint(a, pepper), fingerprint(a, `${pepper}b`));
  assert.equal(bodyDigest({ z: [2, 1], a: 'value' }, pepper), bodyDigest({ a: 'value', z: [2, 1] }, pepper));
  assert.notEqual(bodyDigest({ a: 'value' }, pepper), bodyDigest({ a: 'changed' }, pepper));
  assert.equal(canonicalJson({ b: 1, a: 2 }), '{"a":2,"b":1}');
  assert.equal(safeEqual('token', 'token'), true); assert.equal(safeEqual('token', 'shorter'), false);
});

test('usage refuses inconsistent unknowns and avoids double-counting cache/reasoning', () => {
  assert.deepEqual(normalizeUsage({ prompt_tokens: 10, completion_tokens: 5, total_tokens: 15, completion_tokens_details: { reasoning_tokens: 3 } }), { inputTokens: 10, outputTokens: 5, totalTokens: 15 });
  assert.equal(normalizeUsage({ prompt_tokens: 10, completion_tokens: -1 }), null);
  assert.equal(normalizeUsage({ prompt_tokens: 10, completion_tokens: 5, total_tokens: 11 }), null);
  assert.equal(normalizeUsage(null), null);
});

test('CSV neutralizes formula injection and escapes multiline data', () => {
  assert.equal(csvCell('=WEBSERVICE("https://example.invalid")'), '"\'=WEBSERVICE(""https://example.invalid"")"');
  assert.equal(csvCell(' \t+CMD'), '"\' \t+CMD"');
  assert.equal(csvCell('a,"b"\nc'), '"a,""b""\nc"');
});

test('budget periods use configured timezone and reservation includes tool payload', () => {
  assert.equal(periodDate(new Date('2026-09-24T20:00:00Z')), '2026-09-25');
  const base = { model: 'model', messages: [{ role: 'user', content: 'Привет' }] };
  assert.doesNotThrow(() => validateChatBody(base));
  const plain = estimateReservationTokens(base, 128);
  const tools = estimateReservationTokens({ ...base, tools: [{ type: 'function', function: { name: 'search', description: 'x'.repeat(1000) } }] }, 128);
  assert.ok(tools.inputTokens > plain.inputTokens + 1000);
  assert.throws(() => validateChatBody({ ...base, n: 2 }));
  assert.throws(() => validateChatBody({ ...base, max_tokens: 1, max_completion_tokens: 1 }));
});
