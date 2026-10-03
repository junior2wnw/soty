import test from 'node:test';
import assert from 'node:assert/strict';
import { readableRecord } from '../src/readable.mjs';
import { compileRecord, makeRecord, snapshotRecord } from '../src/records.mjs';
const snapshot = (kind, values) => snapshotRecord(compileRecord({ ...makeRecord(kind), values: { ...makeRecord(kind).values, ...values } }));
test('Wi-Fi and contacts are readable without exposing the password as an ordinary field', () => {
  const wifi = readableRecord(snapshot('wifi', { ssid: 'Гости', password: '12345678' }));
  assert.deepEqual(wifi.fields, [['Сеть', 'Гости'], ['Защита', 'WPA2']]); assert.equal(wifi.password, '12345678');
  const contact = readableRecord(snapshot('contact', { name: 'Анна; Петрова', company: 'Первый, второй', phone: '+7 999 123-45-67' }));
  assert.deepEqual(contact.fields, [['Имя', 'Анна; Петрова'], ['Организация', 'Первый, второй'], ['Телефон', '+79991234567']]);
});
test('truncated, duplicate or non-UTF8 WSC attributes fall back to the original bytes', () => {
  for (const hex of ['100e0005', '100e0004ffff0001', '100e000b10450001ff100300020020', '100e000a10450001411045000142']) {
    assert.equal(readableRecord({ recordType: 'mime', mediaType: 'application/vnd.wfa.wsc', base64: Buffer.from(hex, 'hex').toString('base64') }), null);
  }
});
