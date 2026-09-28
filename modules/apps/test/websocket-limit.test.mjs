import test from 'node:test';
import assert from 'node:assert/strict';
import { createWebSocketLimiter } from '../server/protocol.mjs';

function frame(opcode, length, { fin = true, masked = true } = {}) {
  const wide = length >= 65536 ? 8 : length >= 126 ? 2 : 0;
  const bytes = Buffer.alloc(2 + wide + (masked ? 4 : 0) + length);
  bytes[0] = (fin ? 128 : 0) | opcode; bytes[1] = (masked ? 128 : 0) | (wide === 8 ? 127 : wide === 2 ? 126 : length);
  if (wide === 8) bytes.writeBigUInt64BE(BigInt(length), 2); else if (wide === 2) bytes.writeUInt16BE(length, 2);
  return bytes;
}
test('incremental frames are bounded across fragmentation and control frames', () => {
  const parser = createWebSocketLimiter({ masked: true, maxMessageBytes: 1024 });
  const data = Buffer.concat([frame(1, 500, { fin: false }), frame(9, 3), frame(0, 500)]);
  for (const byte of data) parser.push(Buffer.from([byte]));
  assert.doesNotThrow(() => parser.push(frame(2, 1024)));
  parser.push(frame(2, 600, { fin: false }));
  assert.throws(() => parser.push(frame(0, 600)), /app_websocket_message_too_large/u);
});
test('mask direction, extensions, control frames and unexpected continuation are rejected', () => {
  assert.throws(() => createWebSocketLimiter({ masked: true }).push(frame(1, 3, { masked: false })), /invalid_frame/u);
  const compressed = frame(1, 3); compressed[0] |= 64;
  assert.throws(() => createWebSocketLimiter({ masked: true }).push(compressed), /invalid_frame/u);
  assert.throws(() => createWebSocketLimiter({ masked: true }).push(frame(9, 126)), /invalid_control/u);
  assert.throws(() => createWebSocketLimiter({ masked: true }).push(frame(0, 1)), /invalid_fragment/u);
  const huge = frame(2, 1024 * 1024 + 1);
  assert.throws(() => createWebSocketLimiter({ masked: true }).push(huge.subarray(0, 14)), /message_too_large/u);
});
