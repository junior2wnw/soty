import { createHash } from 'node:crypto';
import { assert, canonicalJson, exact } from './validation.mjs';

// Fixed Notes @1 request fingerprint. The input has its own 256 KiB ceiling;
// the envelope's allowance includes only this fixed contract's metadata.
export function nativeNoteRequestDigest(input) {
  exact(input, ['title', 'body']);
  for (const key of ['title', 'body']) {
    const property = Object.getOwnPropertyDescriptor(input, key);
    assert(property && Object.hasOwn(property, 'value') && typeof property.value === 'string');
  }
  const value = { title: input.title, body: input.body };
  canonicalJson(value, { maxBytes: 262144 });
  const envelope = canonicalJson({
    capabilityId: 'notes.createDraft', version: 1,
    capabilityDigest: '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204',
    input: value, target: { kind: 'native', handler: 'notes.createDraft', version: 1 },
    resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'],
  }, { maxBytes: 294912 });
  return createHash('sha256').update(envelope, 'utf8').digest('hex');
}
