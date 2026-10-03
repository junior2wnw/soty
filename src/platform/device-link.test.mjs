import test from 'node:test';
import assert from 'node:assert/strict';
import { parseDeviceLink } from './device-link.mjs';
const origin = 'https://soty.pochinit.online';
const link = { schema: 'soty.device-link.v1', origin, hostDeviceId: 'host_fixture', connectorId: 'connector_fixture', claimCode: 'a'.repeat(43) };
test('a device file is bound to the current Soty origin and only yields the existing claim contract', () => {
  assert.deepEqual(parseDeviceLink(JSON.stringify(link), origin), { hostDeviceId: link.hostDeviceId, connectorId: link.connectorId, claimCode: link.claimCode });
  assert.throws(() => parseDeviceLink(JSON.stringify(link), 'https://another.example'), /device_link_invalid/);
});
test('oversized, executable, partial and extended device files are refused before transmission', () => {
  for (const value of [null, [], { ...link, token: 'forbidden' }, { ...link, claimCode: '' }, { ...link, hostDeviceId: '<script>' }, { ...link, origin: origin + '/' }, { ...link, schema: 'unknown' }]) {
    assert.throws(() => parseDeviceLink(JSON.stringify(value), origin), /device_link_invalid/);
  }
  assert.throws(() => parseDeviceLink(' '.repeat(4097), origin), /device_link_invalid/);
});
