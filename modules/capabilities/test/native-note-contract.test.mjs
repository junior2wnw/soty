import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { nativeNoteRequestDigest } from '../server/native-note-contract.mjs';
import { canonicalHash, canonicalJson } from '../server/validation.mjs';

const envelope = input => ({ capabilityId: 'notes.createDraft', version: 1,
  capabilityDigest: '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204', input,
  target: { kind: 'native', handler: 'notes.createDraft', version: 1 },
  resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'] });

test('fixed native fingerprint preserves the existing canonical bytes and digest', () => {
  for (const input of [{ title: '', body: '' }, { title: '😀', body: 'е\u0301\nточный текст' }]) {
    assert.equal(nativeNoteRequestDigest(input), canonicalHash(envelope(input)));
    assert.equal(nativeNoteRequestDigest({ body: input.body, title: input.title }), nativeNoteRequestDigest(input));
  }
});

test('native fingerprint accepts the declared input and Notes sizes when only the fixed envelope exceeds 256 KiB', () => {
  const input = { title: '', body: '中'.repeat(87350) };
  const doc = { ...input, items: [], color: 'plain', pinned: false, state: 'active' };
  const serialized = canonicalJson(envelope(input), { maxBytes: 294912 });
  assert.equal(Buffer.byteLength(canonicalJson(input)), 262072);
  assert.equal(Buffer.byteLength(JSON.stringify(doc)), 262131);
  assert.equal(Buffer.byteLength(serialized), 262359);
  assert.throws(() => canonicalHash(envelope(input)), { code: 'payload_too_large' });
  assert.equal(nativeNoteRequestDigest(input), createHash('sha256').update(serialized, 'utf8').digest('hex'));
});

test('native envelope allowance does not expand input bytes or accept caller-selected contract fields', () => {
  const largest = { title: '', body: '中'.repeat(87371) + 'x'.repeat(9) };
  assert.equal(Buffer.byteLength(canonicalJson(largest)), 262144);
  assert.match(nativeNoteRequestDigest(largest), /^[a-f0-9]{64}$/u);
  assert.throws(() => nativeNoteRequestDigest({ ...largest, body: largest.body + 'x' }), { code: 'payload_too_large' });
  assert.throws(() => nativeNoteRequestDigest({ title: '', body: '', resources: ['other'] }), { code: 'invalid_input' });
  let called = false;
  assert.throws(() => nativeNoteRequestDigest({ get title() { called = true; return ''; }, body: '' }), { code: 'invalid_input' });
  assert.equal(called, false);
});
