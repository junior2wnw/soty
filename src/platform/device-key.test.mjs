import test from 'node:test';
import assert from 'node:assert/strict';
import { deviceKey, resolveDeviceKey } from './device-key.mjs';

test('colon-containing IDs cannot retarget a selected device', () => {
  const first = { hostDeviceId: 'host:one', connectorId: 'user' };
  const second = { hostDeviceId: 'host', connectorId: 'one:user' };
  assert.notEqual(deviceKey(first), deviceKey(second));
  assert.equal(resolveDeviceKey('host:one:user', [second, first]), 'host:one:user', 'ambiguous old selection stays unavailable');
  assert.equal(resolveDeviceKey(deviceKey(first), [second, first]), deviceKey(first));
});

test('an unambiguous saved legacy binding migrates; a missing one never falls back', () => {
  const device = { hostDeviceId: 'host-one', connectorId: 'user-machine' };
  assert.equal(resolveDeviceKey('host-one:user-machine', [device]), deviceKey(device));
  assert.equal(resolveDeviceKey('missing:host', [device]), 'missing:host');
  assert.equal(resolveDeviceKey('', [device]), '');
});

test('removing one colliding legacy device cannot silently migrate to the survivor', () => {
  const survivor = { hostDeviceId: 'host', connectorId: 'one:user' };
  assert.equal(resolveDeviceKey('host:one:user', [survivor]), 'host:one:user');
  assert.equal(resolveDeviceKey(deviceKey({ hostDeviceId: 'host:one', connectorId: 'user' }), [survivor]),
    deviceKey({ hostDeviceId: 'host:one', connectorId: 'user' }));
});
