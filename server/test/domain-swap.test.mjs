import assert from 'node:assert/strict';
import test from 'node:test';
import { connectAllowedOrigins } from '../connect-module.js';
import { productionShellOriginAllowed } from '../../scripts/agent-modules/production-origin.mjs';

test('only recognised production origins gain the new Soty origin', () => {
  const previous = process.env.SOTY_CONNECT_ORIGINS;
  try {
    process.env.SOTY_CONNECT_ORIGINS = 'https://xn--n1afe0b.online,https://soty.pochinit.online';
    assert.deepEqual(connectAllowedOrigins(), ['https://xn--n1afe0b.online', 'https://soty.pochinit.online', 'https://4-2.xn--p1ai']);
    process.env.SOTY_CONNECT_ORIGINS = 'https://shell.example';
    assert.deepEqual(connectAllowedOrigins(), ['https://shell.example']);
    const explicit = ['https://fixture.example'];
    assert.equal(connectAllowedOrigins(explicit), explicit);
  } finally {
    if (previous === undefined) delete process.env.SOTY_CONNECT_ORIGINS;
    else process.env.SOTY_CONNECT_ORIGINS = previous;
  }
});

test('installed connectors admit exact production shells and keep custom relays isolated', () => {
  for (const relay of ['https://xn--n1afe0b.online', 'https://soty.pochinit.online', 'https://4-2.xn--p1ai']) {
    for (const origin of ['https://xn--n1afe0b.online', 'https://soty.pochinit.online', 'https://4-2.xn--p1ai']) {
      assert.equal(productionShellOriginAllowed(origin, relay), true);
    }
    for (const origin of ['', 'null', 'http://4-2.xn--p1ai', 'https://nfc.xn--n1afe0b.online', 'https://4-2.xn--p1ai.evil.example']) {
      assert.equal(productionShellOriginAllowed(origin, relay), false);
    }
  }
  assert.equal(productionShellOriginAllowed('https://shell.example', 'https://shell.example'), true);
  assert.equal(productionShellOriginAllowed('https://4-2.xn--p1ai', 'https://shell.example'), false);
  assert.equal(productionShellOriginAllowed('https://shell.example', 'https://soty.pochinit.online'), false);
});
