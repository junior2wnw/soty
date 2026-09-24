// Creates only synthetic release-verification accounts. Never receives real user credentials.
import assert from 'node:assert/strict';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { ConnectError } from '../../modules/connect/browser/crypto.mjs';
const origin = new URL(process.argv[2]).origin;
const endpoint = `${origin}/api/connect/rpc`;
const make = () => {
  let state = null;
  const storage = {
    async read() { return structuredClone(state); },
    async claim(candidate) { state ||= structuredClone(candidate); return structuredClone(state); },
    async compareAndSwap(revision, candidate) {
      if (state?.localRevision !== revision) throw new ConnectError('LOCAL_CONFLICT', 'Concurrent write');
      state = structuredClone(candidate); return structuredClone(state);
    },
  };
  return createClientWithStorage({ projectId: 'soty', endpoint,
    fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin }, signal: AbortSignal.timeout(20000) }),
  }, storage);
};
const a = make(), b = make(), recovery = make();
try {
  const first = await a.bootstrap('Проверка выпуска A'), previous = await b.bootstrap('Проверка выпуска B');
  assert.equal((await a.bootstrap('Проверка выпуска A')).accountId, first.accountId);
  const card = await a.card(); const request = await b.requestContact(card.cardId);
  assert.equal((await b.status()).accountId, previous.accountId);
  await a.acceptContact(request.requestId);
  assert.equal((await b.contacts()).contacts.length, 1);
  const marker = { synthetic: true, message: 'Проверка зашифрованного переноса' };
  await a.saveVault(marker, 0, first.accountId);
  const pending = await b.startEnrollment('Проверка второго устройства');
  await a.approveEnrollment(pending.requestId, first.accountId);
  const preview = await b.previewEnrollment(pending.requestId);
  assert.equal((await b.getLocalState()).accountId, previous.accountId);
  assert.equal(preview.account.accountId, first.accountId);
  const linked = await b.finishEnrollment(pending.requestId, first.accountId);
  assert.notEqual(linked.deviceId, first.deviceId);
  assert.deepEqual((await b.loadVault()).payload, marker);
  await b.switchProfile(previous.accountId);
  assert.equal((await b.status()).accountId, previous.accountId);
  const kit = await a.prepareRecovery(); await a.confirmRecovery(kit);
  await recovery.recover(kit, 'Проверка восстановления');
  assert.deepEqual((await recovery.loadVault()).payload, marker);
  await recovery.revokeDevice(first.deviceId);
  await assert.rejects(a.status(), error => error.code?.toLowerCase() === 'device_revoked');
  const unauthorized = await fetch(endpoint, { method: 'POST', headers: { 'content-type': 'application/json', origin: 'https://untrusted.invalid' }, body: JSON.stringify({ protocol: 1, op: 'bootstrap', args: {} }) });
  assert.equal(unauthorized.ok, false);
  console.log(JSON.stringify({ ok: true, origin, checks: ['bootstrap-idempotent', 'contact-is-not-login', 'mutual-contact', 'recipient-preview',
    'separate-device-keys', 'encrypted-vault', 'old-profile-retained', 'recovery', 'revocation', 'origin-isolation'] }));
} catch (error) {
  console.error(JSON.stringify({ ok: false, code: /^[A-Za-z0-9_]+$/.test(error.code || '') ? error.code : 'smoke_failed' })); process.exitCode = 1;
} finally { a.dispose(); b.dispose(); recovery.dispose(); }
