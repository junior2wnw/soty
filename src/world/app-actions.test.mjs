import test from 'node:test';
import assert from 'node:assert/strict';
import { isAppExternalRequest } from './app-actions.mjs';
test('only an activated request from the live app frame and its exact origin can open separately', () => {
  const frame = {}, event = { data: { schema: 'soty.app-action.v1', action: 'open-separately' }, source: frame, origin: 'https://nfc.example' };
  const context = { frameWindow: frame, origin: event.origin, current: true, activated: true };
  assert.equal(isAppExternalRequest(event, context), true);
  for (const change of [{ current: false }, { activated: false }, { frameWindow: null }, { frameWindow: {} }, { origin: 'https://other.example' }]) assert.equal(isAppExternalRequest(event, { ...context, ...change }), false);
  for (const data of [null, [], { ...event.data, url: 'https://other.example' }, { ...event.data, action: 'navigate' }]) assert.equal(isAppExternalRequest({ ...event, data }, context), false);
});
