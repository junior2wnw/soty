import test from 'node:test';
import assert from 'node:assert/strict';
import { createNfcController, nfcAvailability } from '../src/nfc.mjs';
const message = { records: [{ recordType: 'text', lang: 'ru', data: 'Тест' }] };
function fixture(config = {}, options = {}) {
  const states = [], readers = [];
  class Reader extends EventTarget {
    constructor() { super(); readers.push(this); }
    async write(value, opts) { this.value = value; this.options = opts; if (config.writeError) throw config.writeError; if (config.wait) return new Promise((resolve, reject) => opts.signal.addEventListener('abort', () => reject(new DOMException('abort', 'AbortError')), { once: true })); }
    async scan(opts) {
      this.scanOptions = opts; if (config.scanError) throw config.scanError;
      if (config.waitRead) return;
      if (config.readingError) this.dispatchEvent(new Event('readingerror'));
      queueMicrotask(() => { const event = new Event('reading'); event.message = config.message || this.value || message; event.serialNumber = '00:01'; this.dispatchEvent(event); });
    }
    async makeReadOnly(opts) { this.lockOptions = opts; if (config.lockError) throw config.lockError; }
  }
  const controller = createNfcController({ Reader, onState: state => states.push(state), timeoutMs: 30, verifyMs: 20, ...options });
  return { controller, states, readers };
}
test('NFC availability distinguishes embedded, insecure, iPhone and unsupported desktop', () => {
  assert.equal(nfcAvailability({ embedded: true }).reason, 'embedded');
  assert.equal(nfcAvailability({ secure: false }).reason, 'secure');
  assert.equal(nfcAvailability({ userAgent: 'iPhone' }).reason, 'ios');
  assert.equal(nfcAvailability({}).supported, false);
  assert.equal(nfcAvailability({ Reader: class {} }).supported, true);
});
test('read observes the first event, reports serial as supplied and stops scanning', async () => {
  const f = fixture(); const result = await f.controller.read();
  assert.equal(result.serialNumber, '00:01'); assert.equal(result.records[0].text, 'Тест');
  assert.equal(f.controller.busy, false); assert.equal(f.readers[0].scanOptions.signal.aborted, true);
  assert.deepEqual(f.states.map(s => s.phase), ['scanning', 'read']);
});
test('write requires opt-in to overwrite and confirms by reading the exact message', async () => {
  const f = fixture(); const result = await f.controller.write(message);
  assert.equal(f.readers[0].options.overwrite, false); assert.equal(result.verified, true);
  assert.deepEqual(f.states.map(s => s.phase), ['writing', 'verifying', 'verified']);
  await f.controller.write(message, { overwrite: true }); assert.equal(f.readers[1].options.overwrite, true);
});
test('a wrong tag and a failed permission for verification never become verified success or failed write', async () => {
  for (const config of [{ message: { records: [{ recordType: 'text', lang: 'ru', data: 'Другая метка' }] } }, { scanError: new DOMException('Permission', 'NotAllowedError') }]) {
    const f = fixture(config); const result = await f.controller.write(message);
    assert.deepEqual(result, { written: true, verified: false }); assert.equal(f.states.at(-1).phase, 'written'); assert.equal(f.controller.busy, false);
  }
});
test('readingerror keeps verification state and still allows a later readable tag', async () => {
  const f = fixture({ readingError: true }); const result = await f.controller.write(message);
  assert.equal(result.verified, true); assert.equal(f.states[2].phase, 'verifying');
});
test('cancel and hidden page abort before write; overlapping operations are refused', async () => {
  const f = fixture({ wait: true }); const pending = f.controller.write(message);
  await assert.rejects(f.controller.read(), { name: 'InvalidStateError' });
  f.controller.cancel('hidden'); await assert.rejects(pending, { name: 'AbortError' });
  assert.equal(f.states.at(-1).phase, 'cancelled'); assert.match(f.states.at(-1).message, /свёрнуто/); assert.equal(f.controller.busy, false);
});
test('verification timeout or cancellation preserves a successfully completed write', async () => {
  const f = fixture({ waitRead: true }); const result = await f.controller.write(message);
  assert.deepEqual(result, { written: true, verified: false });
  const g = fixture({ waitRead: true }); const pending = g.controller.write(message);
  await Promise.resolve(); g.controller.cancel(); assert.equal((await pending).written, true);
});
test('timeouts and malformed read events release the controller so a retry is possible', async () => {
  const f = fixture({ waitRead: true }, { timeoutMs: 4 }); await assert.rejects(f.controller.read(), { name: 'TimeoutError' }); assert.equal(f.controller.busy, false);
  const g = fixture({ message: { records: [{ recordType: 'text', data: new Uint8Array([1]), encoding: 'not-an-encoding' }] } });
  await assert.rejects(g.controller.read()); assert.equal(g.controller.busy, false);
});
test('permission failure and lock report their actual outcomes', async () => {
  const f = fixture({ writeError: new DOMException('Permission', 'NotAllowedError') });
  await assert.rejects(f.controller.write(message), { name: 'NotAllowedError' }); assert.equal(f.states.at(-1).phase, 'error');
  const g = fixture(); await g.controller.lock(); assert.equal(g.states.at(-1).phase, 'locked'); assert.equal(g.readers[0].lockOptions.signal.aborted, true);
});
