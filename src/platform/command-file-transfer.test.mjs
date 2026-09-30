import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createCommandFileTransfer } from './command-file-transfer.mjs';
const encoded = value => Buffer.from(JSON.stringify(value)).toString('base64');
const begin = (size, total = 1) => `SOTY_FILE_BEGIN ${encoded({ id: 'file_1', name: 'данные.bin', size, total })}\n`;
const chunk = (value, index = 0) => `SOTY_FILE_CHUNK file_1 ${index} ${Buffer.from(value).toString('base64')}\n`;
const end = `SOTY_FILE_END ${encoded({ id: 'file_1' })}\n`;

test('command file parsing waits for ACK, preserves split lines and cannot report stored before the real last byte', async () => {
  let acknowledge; const reports = [], received = [];
  const value = Uint8Array.from({ length: 10000 }, (_, index) => index % 256);
  const parser = createCommandFileTransfer({ maxBytes: 20000, report: event => reports.push(event),
    sendChunk: async (_id, _meta, bytes) => { await new Promise(resolve => { acknowledge = resolve; }); received.push(bytes); } });
  assert.equal(await parser.write(`start\n${begin(value.length)}`), 'start\n');
  const wire = chunk(value);
  assert.equal(await parser.write('line\npart'), 'line\n');
  assert.equal(await parser.write('ial\n'), 'partial\n');
  await parser.write(wire.slice(0, 60));
  const pending = parser.write(wire.slice(60) + end + 'end\n', true);
  await new Promise(resolve => setTimeout(resolve, 0));
  assert.deepEqual(reports.map(event => event.state), ['started']);
  assert.equal(parser.hasPending(), true); acknowledge();
  assert.equal(await pending, 'end\n');
  assert.deepEqual(reports.map(event => event.state), ['started', 'stored']);
  assert.equal(parser.hasPending(), false);
  const hash = bytes => createHash('sha256').update(bytes).digest('hex');
  assert.equal(hash(received[0]), hash(value));
});

test('empty file is transferred, but malformed sizes, missing/out-of-order bytes and lost ACK never report completion', async () => {
  const reports = [];
  const fixture = sendChunk => createCommandFileTransfer({ maxBytes: 10, report: event => reports.push(event), sendChunk: sendChunk || (async () => {}) });
  await fixture().write(begin(0) + chunk([]) + end, true);
  assert.equal(reports.at(-1).state, 'stored');
  for (const wire of [begin(11), begin(1) + end, begin(1) + chunk([1], 1), begin(2) + chunk([1]) + end,
    begin(1) + chunk([1, 2]) + end, begin(1) + begin(1), begin(1) + chunk([1])]) {
    reports.length = 0;
    await assert.rejects(fixture().write(wire, true));
    assert.equal(reports.some(event => event.state === 'stored'), false);
  }
  reports.length = 0;
  await assert.rejects(fixture(async () => { throw new Error('file_transfer_timeout'); }).write(begin(1) + chunk([1]) + end, true));
  assert.equal(reports.some(event => event.state === 'stored'), false);
});
