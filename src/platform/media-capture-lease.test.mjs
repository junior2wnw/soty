import test from 'node:test';
import assert from 'node:assert/strict';
import { createMediaCaptureCoordinator } from './media-capture-lease.mjs';

test('camera QR and video owners exclude one another while microphone remains independent', () => {
  const c = createMediaCaptureCoordinator(), qr = {}, video = {}, call = {};
  const first = c.acquire('camera', qr), audio = c.acquire('microphone', call);
  assert.ok(first?.isCurrent()); assert.ok(audio?.isCurrent());
  assert.equal(c.acquire('camera', video), null);
  first.release();
  const second = c.acquire('camera', video); assert.ok(second?.isCurrent());
  assert.equal(audio.isCurrent(), true);
  first.release(); assert.equal(second.isCurrent(), true);
  second.release(); audio.release(); assert.equal(c.busy('camera'), false); assert.equal(c.busy('microphone'), false);
});

test('leases have no takeover or track API and releasing one cannot release a newer owner', () => {
  const c = createMediaCaptureCoordinator(), first = c.acquire('camera', {});
  assert.deepEqual(Object.keys(first).sort(), ['isCurrent', 'release']);
  assert.equal(Object.isFrozen(first), true); assert.equal(Object.isFrozen(c), true);
  first.release(); const second = c.acquire('camera', {});
  first.release(); assert.equal(c.busy('camera'), true); assert.equal(second.isCurrent(), true);
  second.release(); second.release(); assert.equal(c.busy('camera'), false);
});

test('only the declared resources and opaque local owners can acquire a lease', () => {
  const c = createMediaCaptureCoordinator();
  for (const resource of ['', 'screen', 'account', undefined]) assert.throws(() => c.busy(resource), /media_capture_resource_invalid/);
  for (const owner of [null, undefined, '', 'account_fixture', 7]) assert.throws(() => c.acquire('camera', owner), /media_capture_owner_invalid/);
  assert.equal(c.busy('camera'), false);
});
