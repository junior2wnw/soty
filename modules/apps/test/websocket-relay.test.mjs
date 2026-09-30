import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { createWebSocketRelay, normalizeWebSocketLivenessTiming } from '../server/websocket-relay.mjs';
import { CHUNK_BYTES } from '../server/protocol.mjs';

const drain = async () => { for (let i = 0; i < 64; i++) await Promise.resolve(); };
function clock() {
  let at = 0, sequence = 0, scheduled = 0;
  const jobs = new Map();
  return {
    now: () => at,
    setTimeout(fn, delay) { const id = ++sequence; jobs.set(id, { at: at + delay, fn }); scheduled++; return id; },
    clearTimeout(id) { jobs.delete(id); },
    async advance(ms) {
      const until = at + ms;
      for (;;) {
        const item = [...jobs].filter(([, value]) => value.at <= until).sort((a, b) => a[1].at - b[1].at || a[0] - b[0])[0];
        if (!item) break;
        at = item[1].at; jobs.delete(item[0]); item[1].fn(); await drain();
      }
      at = until; await drain();
    },
    jump(ms) { at += ms; },
    get scheduled() { return scheduled; },
    get size() { return jobs.size; },
  };
}
function frame(opcode, payload = Buffer.alloc(0), { masked = false, fin = true, key = randomBytes(4) } = {}) {
  const data = Buffer.from(payload), extra = data.length < 126 ? 0 : data.length <= 65535 ? 2 : 8;
  const header = 2 + extra + (masked ? 4 : 0), bytes = Buffer.alloc(header + data.length);
  bytes[0] = opcode | (fin ? 128 : 0); bytes[1] = (masked ? 128 : 0) | (extra === 0 ? data.length : extra === 2 ? 126 : 127);
  if (extra === 2) bytes.writeUInt16BE(data.length, 2);
  if (extra === 8) bytes.writeBigUInt64BE(BigInt(data.length), 2);
  if (masked) key.copy(bytes, 2 + extra);
  for (let i = 0; i < data.length; i++) bytes[header + i] = data[i] ^ (masked ? key[i % 4] : 0);
  return bytes;
}
function frames(writes) {
  const bytes = Buffer.concat(writes), result = [];
  let offset = 0;
  while (offset < bytes.length) {
    const start = offset, first = bytes[offset++], second = bytes[offset++], masked = Boolean(second & 128);
    let length = second & 127;
    if (length === 126) { length = bytes.readUInt16BE(offset); offset += 2; }
    else if (length === 127) { length = Number(bytes.readBigUInt64BE(offset)); offset += 8; }
    const key = masked ? bytes.subarray(offset, offset += 4) : null;
    assert.ok(offset + length <= bytes.length, 'output contains a truncated frame');
    const payload = Buffer.from(bytes.subarray(offset, offset + length));
    if (key) for (let i = 0; i < payload.length; i++) payload[i] ^= key[i % 4];
    offset += length;
    result.push({ opcode: first & 15, fin: Boolean(first & 128), masked, key, payload, raw: bytes.subarray(start, offset) });
  }
  return result;
}
function fixture(options = {}) {
  const time = clock(), client = [], source = [], failures = [];
  let checks = 0;
  const relay = createWebSocketRelay({ clock: time,
    toClient: bytes => { client.push(Buffer.from(bytes)); return options.toClient?.(bytes); },
    toSource: bytes => { source.push(Buffer.from(bytes)); return options.toSource?.(bytes); },
    assertActive: () => { checks++; options.assertActive?.(); },
    onFailure: error => { failures.push(error); options.onFailure?.(error); },
    timing: options.timing,
  });
  if (options.start !== false) relay.start();
  return { relay, time, client, source, failures, get checks() { return checks; } };
}
async function chunks(method, bytes, size = CHUNK_BYTES) {
  for (let offset = 0; offset < bytes.length; offset += size) await method(bytes.subarray(offset, offset + size));
}
const wait = () => { let resolve; const promise = new Promise(yes => { resolve = yes; }); return { promise, resolve }; };

test('timing is strict, bounded and normalized without mutating the supplied object', () => {
  const input = { quietMs: 11 }, value = normalizeWebSocketLivenessTiming(input);
  assert.deepEqual(value, { quietMs: 11, insertMs: 30000, responseMs: 30000, frameMs: 30000 });
  assert.ok(Object.isFrozen(value)); assert.deepEqual(input, { quietMs: 11 });
  for (const wrong of [null, [], 'x', { quietMs: 0 }, { quietMs: 30001 }, { quietMs: Infinity }, { quietMs: NaN }, { quietMs: 1.5 }, { off: true }, { [Symbol('x')]: 1 }]) assert.throws(() => normalizeWebSocketLivenessTiming(wrong), /invalid_websocket_liveness/u);
});

test('relay starts only after the HTTP upgrade gate and cannot restart after close', async () => {
  const f = fixture({ start: false });
  await f.time.advance(100_000); assert.equal(f.client.length + f.source.length, 0);
  await assert.rejects(f.relay.clientBytes(frame(1, 'x', { masked: true })), /not_started/u);
  assert.equal(f.failures.length, 1); assert.throws(() => f.relay.start(), /not_started/u);
  const g = fixture(); g.relay.close(); g.relay.close();
  assert.throws(() => g.relay.start(), /closed/u); assert.equal(g.failures.length, 0); assert.equal(g.time.size, 0);
});

test('24k zero-payload frames coalesce writes, authority checks and timers', async () => {
  for (const masked of [false, true]) {
    const f = fixture(), bytes = Buffer.alloc(CHUNK_BYTES), stride = masked ? 6 : 2;
    for (let i = 0; i < bytes.length; i += stride) { bytes[i] = 0x82; bytes[i + 1] = masked ? 0x80 : 0; }
    const scheduled = f.time.scheduled, checks = f.checks;
    await (masked ? f.relay.clientBytes : f.relay.sourceBytes)(bytes);
    const output = masked ? f.source : f.client;
    assert.deepEqual(Buffer.concat(output), bytes);
    assert.ok(output.length <= 2, `writes=${output.length}`);
    assert.ok(f.checks - checks <= 16, `checks=${f.checks - checks}`);
    assert.ok(f.time.scheduled - scheduled <= 8, `timers=${f.time.scheduled - scheduled}`);
    assert.equal(f.failures.length, 0); f.relay.close();
  }
});

test('a 1MiB data frame completes every input chunk before the rest of its frame exists', async () => {
  const f = fixture(), bytes = frame(2, Buffer.alloc(1024 * 1024, 0xa5), { masked: true });
  await f.relay.clientBytes(bytes.subarray(0, CHUNK_BYTES));
  assert.deepEqual(Buffer.concat(f.source), bytes.subarray(0, CHUNK_BYTES));
  await chunks(f.relay.clientBytes, bytes.subarray(CHUNK_BYTES));
  assert.deepEqual(Buffer.concat(f.source), bytes); assert.equal(f.failures.length, 0); f.relay.close();
});

test('split header and control payload settle each chunk while preserving all original bytes', async () => {
  const f = fixture(), bytes = frame(9, Buffer.alloc(125, 17), { masked: true });
  await f.relay.clientBytes(bytes.subarray(0, 1)); assert.equal(f.source.length, 0);
  await f.relay.clientBytes(bytes.subarray(1, 20)); assert.equal(f.source.length, 0);
  await f.relay.clientBytes(bytes.subarray(20)); assert.deepEqual(Buffer.concat(f.source), bytes);
  f.relay.close();
});

test('each endpoint gets its own tag; repeated probes use fresh masks and all own pongs stay local', async () => {
  const f = fixture(); await f.time.advance(30000);
  const a = frames(f.client)[0], b = frames(f.source)[0];
  assert.equal(a.opcode, 9); assert.equal(a.masked, false); assert.equal(b.masked, true);
  assert.equal(a.payload.length, 32); assert.notDeepEqual(a.payload, b.payload);
  await f.relay.clientBytes(frame(10, a.payload, { masked: true }));
  await f.relay.sourceBytes(frame(10, b.payload));
  assert.equal(f.client.length, 1); assert.equal(f.source.length, 1);
  await f.time.advance(30000);
  const c = frames(f.source)[1]; assert.deepEqual(c.payload, b.payload); assert.notDeepEqual(c.key, b.key);
  await f.relay.clientBytes(frame(10, a.payload, { masked: true }));
  await f.relay.sourceBytes(frame(10, b.payload));
  assert.equal(f.failures.length, 0); f.relay.close();
});

test('ordinary traffic can supersede an own ping; delayed own pong is still consumed', async () => {
  const f = fixture(); await f.time.advance(30000);
  const clientTag = frames(f.client)[0].payload, sourceTag = frames(f.source)[0].payload;
  await f.time.advance(5000);
  const business = frame(2, 'business'); await f.relay.sourceBytes(business);
  await f.relay.clientBytes(frame(10, 'application-ping', { masked: true }));
  await f.time.advance(24000);
  await f.relay.sourceBytes(frame(10, sourceTag));
  await f.relay.clientBytes(frame(10, clientTag, { masked: true }));
  assert.equal(f.failures.length, 0);
  assert.deepEqual(frames(f.client).map(value => value.opcode), [9, 2]);
  assert.deepEqual(frames(f.source).map(value => value.opcode), [9, 10]);
  assert.equal(frames(f.source)[1].payload.toString(), 'application-ping'); f.relay.close();
});

test('fragmented binary data retains byte identity and heartbeat inserts only between frames', async () => {
  const f = fixture({ timing: { quietMs: 10, insertMs: 30, responseMs: 30, frameMs: 30 } });
  const first = frame(2, Buffer.alloc(60000, 0x91), { masked: true, fin: false });
  await f.relay.clientBytes(first.subarray(0, 20000));
  await f.time.advance(10); // source probe must wait for this frame boundary
  const continuation = frame(0, Buffer.alloc(30000, 0x81), { masked: true });
  await f.relay.clientBytes(Buffer.concat([first.subarray(20000), continuation.subarray(0, 500)]));
  await chunks(f.relay.clientBytes, continuation.subarray(500));
  const sent = frames(f.source);
  assert.deepEqual(sent.map(value => value.opcode), [2, 9, 0]);
  assert.deepEqual(sent[0].raw, first); assert.deepEqual(sent[2].raw, continuation);
  assert.equal(f.failures.length, 0); f.relay.close();
});

test('traffic from source cannot keep an unresponsive browser alive', async () => {
  const f = fixture({ timing: { quietMs: 10, insertMs: 10, responseMs: 10 } });
  await f.time.advance(10);
  await f.time.advance(5); await f.relay.sourceBytes(frame(2, 'source remains alive'));
  await f.time.advance(5);
  assert.equal(f.failures.length, 1); assert.match(f.failures[0].code, /liveness_timeout/u);
  assert.equal(f.time.size, 0);
});

test('one byte at a time cannot refresh the absolute frame deadline', async () => {
  const f = fixture({ timing: { quietMs: 100, frameMs: 30 } }), bytes = frame(2, 'abc', { masked: true });
  await f.relay.clientBytes(bytes.subarray(0, 1));
  await f.time.advance(10); await f.relay.clientBytes(bytes.subarray(1, 2));
  await f.time.advance(10); await f.relay.clientBytes(bytes.subarray(2, 3));
  await f.time.advance(10);
  assert.equal(f.failures.length, 1); assert.equal(f.failures[0].code, 'app_websocket_frame_timeout');
});

test('a blocked frame boundary has an absolute probe insertion deadline', async () => {
  const f = fixture({ timing: { quietMs: 5, insertMs: 5, responseMs: 30, frameMs: 30 } });
  await f.relay.clientBytes(frame(2, Buffer.alloc(100), { masked: true }).subarray(0, 10));
  await f.time.advance(10);
  assert.equal(f.failures.length, 1); assert.equal(f.failures[0].code, 'app_websocket_liveness_timeout');
  assert.equal(frames(f.client)[0].opcode, 9);
});

test('blocked writer rejects the current chunk and a late callback cannot resume output', async () => {
  const held = wait(), f = fixture({ timing: { insertMs: 5 }, toSource: () => held.promise });
  const sending = assert.rejects(f.relay.clientBytes(frame(1, 'pending', { masked: true })), /write_timeout/u);
  await f.time.advance(5); await sending;
  const writes = f.source.length; held.resolve(); await drain();
  assert.equal(f.source.length, writes); assert.equal(f.failures.length, 1); assert.equal(f.time.size, 0);
});

test('only one input can wait behind a heartbeat writer; concurrent input fails boundedly', async () => {
  const held = wait(), f = fixture({ timing: { quietMs: 5 }, toClient: () => held.promise });
  await f.time.advance(5);
  const first = assert.rejects(f.relay.sourceBytes(frame(1, 'one')), /concurrent_input/u);
  await assert.rejects(f.relay.sourceBytes(frame(1, 'two')), /concurrent_input/u);
  await first; held.resolve(); await drain();
  assert.equal(f.failures.length, 1); assert.equal(f.time.size, 0);
});

test('an early pong arriving before the writer callback is not lost', async () => {
  // Hold either writer until its endpoint's response is ingested. Holding
  // BOTH writer callbacks on the opposite queued pump creates an artificial
  // cyclic dependency: real socket write callbacks do not wait for peer pong.
  for (const early of ['client', 'source']) {
    let f;
    const answer = (bytes, side) => {
      const ping = frames([bytes])[0]; if (ping.opcode !== 9) return;
      const work = side === 'client' ? f.relay.clientBytes(frame(10, ping.payload, { masked: true })) : f.relay.sourceBytes(frame(10, ping.payload));
      if (side === early) return work;
      work.catch(() => {});
    };
    f = fixture({ timing: { quietMs: 10, responseMs: 10 }, toSource: bytes => answer(bytes, 'source'), toClient: bytes => answer(bytes, 'client') });
    await f.time.advance(50);
    assert.equal(f.failures.length, 0); assert.ok(f.source.length >= 4); assert.ok(f.client.length >= 4); f.relay.close();
  }
});

test('Close stops both heartbeats and pong-only traffic cannot extend closing', async () => {
  const f = fixture({ timing: { quietMs: 5, responseMs: 10 } });
  const close = frame(8, Buffer.from([3, 232])); await f.relay.sourceBytes(close);
  await f.time.advance(5); await f.relay.clientBytes(frame(10, 'still here', { masked: true }));
  await f.time.advance(5);
  assert.deepEqual(frames(f.client).map(value => value.opcode), [8]);
  assert.deepEqual(frames(f.source).map(value => value.opcode), [10]);
  assert.equal(f.failures[0].code, 'app_websocket_close_timeout'); assert.equal(f.time.size, 0);
});

test('authority is rechecked after write and before a timer probe', async () => {
  const held = wait(); let allowed = true;
  const f = fixture({ toSource: () => held.promise, assertActive: () => { if (!allowed) throw new Error('access_revoked'); } });
  const pending = assert.rejects(f.relay.clientBytes(frame(1, 'queued', { masked: true })), /access_revoked/u);
  allowed = false; held.resolve(); await pending; assert.equal(f.failures.length, 1);
  const g = fixture({ assertActive: () => { if (!allowed) throw new Error('access_revoked'); }, start: false });
  allowed = true; g.relay.start(); allowed = false; await g.time.advance(30000);
  assert.equal(g.source.length + g.client.length, 0); assert.equal(g.failures.length, 1);
});

test('a delayed timer cannot resurrect an expired response window', async () => {
  const f = fixture({ timing: { quietMs: 5, responseMs: 5 } }); await f.time.advance(5);
  f.time.jump(6);
  await assert.rejects(f.relay.sourceBytes(frame(1, 'late')), /liveness_timeout/u);
  assert.equal(f.failures.length, 1); assert.equal(f.time.size, 0);
});

test('invalid mask/control/fragment and excess message bytes preserve the structural boundary', async () => {
  const inputs = [frame(1, 'unmasked'), frame(9, Buffer.alloc(126), { masked: true }), frame(0, 'orphan', { masked: true })];
  for (const bytes of inputs) {
    const f = fixture(); await assert.rejects(f.relay.clientBytes(bytes), /app_websocket_invalid/u);
    assert.equal(f.source.length, 0); assert.equal(f.failures.length, 1);
  }
  const g = fixture(), huge = frame(2, Buffer.alloc(1024 * 1024 + 1), { masked: true });
  await assert.rejects(g.relay.clientBytes(huge.subarray(0, 14)), /message_too_large/u);
  assert.equal(g.source.length, 0);
});

test('failure and disposal are local to a relay, with no duplicate callbacks or writes', async () => {
  const bad = fixture({ timing: { quietMs: 5, responseMs: 5 } }), good = fixture();
  await bad.time.advance(10); bad.relay.close(); bad.relay.close();
  await assert.rejects(bad.relay.sourceBytes(frame(1, 'after close')), /timeout/u);
  await good.relay.sourceBytes(frame(1, 'sibling is alive'));
  assert.equal(bad.failures.length, 1); assert.equal(good.failures.length, 0);
  assert.equal(frames(good.client)[0].payload.toString(), 'sibling is alive'); good.relay.close();
});

test('explicit disposal settles held input without waiting for an unreturned writer callback', async () => {
  const held = wait(), f = fixture({ toClient: () => held.promise });
  const input = assert.rejects(f.relay.sourceBytes(frame(2, 'held')), /app_websocket_closed/u);
  f.relay.close(); await input;
  assert.equal(f.failures.length, 0); assert.equal(f.time.size, 0);
  const count = f.client.length; held.resolve(); await drain(); await f.time.advance(100000);
  assert.equal(f.client.length, count); assert.equal(f.failures.length, 0);
});

test('split late own Pong is consumed only after the complete masked control arrives', async () => {
  const f = fixture(); await f.time.advance(30000);
  const token = frames(f.client)[0].payload, pong = frame(10, token, { masked: true });
  await f.relay.clientBytes(frame(2, 'normal data first', { masked: true }));
  const before = Buffer.concat(f.source);
  await f.relay.clientBytes(pong.subarray(0, 1));
  await f.relay.clientBytes(pong.subarray(1, 18));
  await f.relay.clientBytes(pong.subarray(18));
  assert.deepEqual(Buffer.concat(f.source), before); assert.equal(f.failures.length, 0); f.relay.close();
});

test('disposal cancels both held writers and later callbacks cannot restart either pump', async () => {
  const heldClient = wait(), heldSource = wait();
  const f = fixture({ toClient: () => heldClient.promise, toSource: () => heldSource.promise });
  const a = assert.rejects(f.relay.clientBytes(frame(2, 'client', { masked: true })), /app_websocket_closed/u);
  const b = assert.rejects(f.relay.sourceBytes(frame(2, 'source')), /app_websocket_closed/u);
  f.relay.close(); await Promise.all([a, b]);
  assert.equal(f.client.length, 1); assert.equal(f.source.length, 1);
  heldSource.resolve(); heldClient.resolve(); await drain(); await f.time.advance(100000);
  assert.equal(f.client.length, 1); assert.equal(f.source.length, 1);
  assert.equal(f.time.size, 0); assert.equal(f.failures.length, 0);
});

test('writer throws, rejected promises and synchronous disposal settle the active write once', async () => {
  for (const kind of ['throw', 'reject', 'close']) {
    let f;
    f = fixture({ toSource() {
      if (kind === 'close') { f.relay.close(); return; }
      if (kind === 'throw') throw new Error('writer_failed');
      return Promise.reject(new Error('writer_failed'));
    } });
    await assert.rejects(f.relay.clientBytes(frame(2, 'outbound', { masked: true })), kind === 'close' ? /app_websocket_closed/u : /writer_failed/u);
    await drain();
    assert.equal(f.source.length, 1); assert.equal(f.time.size, 0);
    assert.equal(f.failures.length, kind === 'close' ? 0 : 1);
    f.relay.close();
  }
});
