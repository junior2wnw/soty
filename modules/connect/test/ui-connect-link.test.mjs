import test from 'node:test';
import assert from 'node:assert/strict';
import { parseConnectLink } from '../ui/index.mjs';

const origin = 'https://soty.example';
const id = 'synthetic_request_1234567890';

test('a connection link has exactly one same-origin purpose and target', () => {
  for (const [kind, field] of [['contact', 'card'], ['device', 'request']]) {
    for (const prefix of [origin + '/', '/', '']) {
      assert.deepEqual(parseConnectLink(`${prefix}?connect=${kind}&${field}=${id}`, origin), { kind, id });
    }
    assert.deepEqual(parseConnectLink(`  ${origin}/?${field}=${id}&connect=${kind}  `, origin), { kind, id });
  }
});

test('mixed, ambiguous, normalized or foreign connection links are refused', () => {
  const valid = `${origin}/?connect=contact&card=${id}`;
  for (const value of [null, {}, '', 'javascript:alert(1)',
    `https://other.example/?connect=contact&card=${id}`,
    `https://name:password@soty.example/?connect=contact&card=${id}`,
    `${origin}/other?connect=contact&card=${id}`, `${origin}/old/../?connect=contact&card=${id}`,
    `${valid}#profile/synthetic`, `${valid}#`, `${valid}&`, `${valid}&&`, `${valid}&extra=1`,
    `${valid}&request=${id}`, `${valid}&connect=device`, `${valid}&card=${id}`, `${valid}&card=another_card_1234567890`,
    valid.replace('soty', 'so\tty'), valid.replace('connect', 'con\nnect'), valid + '\u0000',
    `/?connect=device&request=${id}&card=${id}`, `/?connect=device&request=${id}&connect=device`,
    `/?connect=device&request=${id.slice(0, 15)}`, '/?connect=device&request=' + 'a'.repeat(161),
    valid + '&' + 'a'.repeat(4096), valid + '\ud800',
  ]) assert.equal(parseConnectLink(value, origin), null, String(value).slice(0, 200));
  for (const invalidOrigin of ['https://soty.example/', origin + '/path', 'file:///tmp', origin + '?x=1']) {
    assert.equal(parseConnectLink(valid, invalidOrigin), null, invalidOrigin);
  }
});
