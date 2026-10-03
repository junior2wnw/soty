import test from 'node:test';
import assert from 'node:assert/strict';
import { compileRecord, compileDraft, makeRecord, wifiPayload, estimateBytes, estimateTagBytes, snapshotRecord, restoreRecord, messageSignature, validateDraft } from '../src/records.mjs';
import { templateFile, draftLink, parseDraftLink, saveLocal, loadLocal, loadTemplates } from '../src/storage.mjs';
const record = (kind, values) => ({ ...makeRecord(kind), values: { ...makeRecord(kind).values, ...values } });
const draft = records => ({ schema: 'soty.nfc-template.v1', name: 'Проверка', records });
const decode = bytes => new TextDecoder().decode(bytes);

test('web links normalize; code, credentials and malformed links cannot be written', () => {
  assert.equal(compileRecord(record('url', { url: 'example.com/привет' })).data, 'https://example.com/%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82');
  for (const url of ['javascript:alert(1)', 'data:text/html,hello', 'https://secret@example.com/', 'https://']) assert.throws(() => compileRecord(record('url', { url })));
});
test('all ten builders produce Web NFC records; Unicode text and reserved URI characters are retained', () => {
  const values = {
    url: { url: 'example.com' }, text: { text: '  Привет\n' }, contact: { name: 'Анна; Петрова', phone: '+7 (999) 123-45-67' },
    wifi: { ssid: 'Guest', password: '12345678' }, phone: { phone: '+7 999 123-45-67' },
    email: { email: 'a?cc=b@example.com', subject: 'Раз & два', body: 'Привет\nмир' },
    sms: { phone: '+79991234567', body: 'Привет & ?' }, location: { latitude: '0', longitude: '-180' },
    json: { json: '{"имя":"метка"}' }, binary: { mediaType: 'application/octet-stream', hex: '00 0a FF' },
  };
  for (const [kind, value] of Object.entries(values)) assert.ok(['url', 'text', 'mime'].includes(compileRecord(record(kind, value)).recordType));
  assert.equal(compileRecord(record('text', values.text)).data, '  Привет\n');
  assert.equal(compileRecord(record('phone', values.phone)).data, 'tel:+79991234567');
  assert.match(compileRecord(record('email', values.email)).data, /^mailto:a%3Fcc%3Db@example.com\?subject=/);
  const card = decode(compileRecord(record('contact', values.contact)).data);
  assert.ok(card.includes('FN:Анна\\; Петрова\r\n'));
  assert.ok(card.endsWith('END:VCARD\r\n'));
});
test('WSC credential has a known open-network byte encoding; protected networks preserve spaces', () => {
  assert.equal(Buffer.from(wifiPayload({ ssid: 'A', security: 'open' })).toString('hex'),
    '100e002410260001011045000141100300020001100f000200011027000010200006ffffffffffff');
  const bytes = wifiPayload({ ssid: ' Guest ', security: 'wpa2', password: ' 12345678 ' });
  const attrs = new Map(); let offset = 4;
  while (offset < bytes.length) { const view = new DataView(bytes.buffer); const type = view.getUint16(offset), length = view.getUint16(offset + 2); attrs.set(type, bytes.slice(offset + 4, offset + 4 + length)); offset += 4 + length; }
  assert.equal(decode(attrs.get(0x1045)), ' Guest '); assert.equal(decode(attrs.get(0x1027)), ' 12345678 ');
  assert.deepEqual([...attrs.get(0x1003)], [0, 32]); assert.deepEqual([...attrs.get(0x100f)], [0, 8]);
  assert.throws(() => wifiPayload({ ssid: 'я'.repeat(17), security: 'open' }));
  assert.throws(() => wifiPayload({ ssid: 'A', security: 'wpa2', password: 'short' }));
});
test('payload bounds use UTF-8 and URI abbreviation; SMS has no prefix abbreviation', () => {
  assert.equal(estimateBytes({ records: [{ recordType: 'url', data: 'https://a.co' }] }), 9);
  assert.equal(estimateBytes({ records: [{ recordType: 'url', data: 'sms:123' }] }), 12);
  assert.equal(estimateBytes({ records: [{ recordType: 'text', lang: 'ru', data: 'я' }] }), 9);
  assert.equal(estimateTagBytes({ records: [{ recordType: 'empty' }] }), 6);
  assert.throws(() => compileDraft(draft([record('text', { text: 'я'.repeat(9000) })])));
  assert.throws(() => compileDraft(draft(Array.from({ length: 17 }, () => record('text', { text: 'A' })))));
});
test('invalid coordinates, JSON, hex, MIME and missing values are rejected before NFC permission', () => {
  for (const [kind, values] of [['location', { latitude: '91', longitude: '0' }], ['json', { json: '{bad}' }], ['binary', { hex: 'ABC' }], ['binary', { hex: 'ZZ' }], ['binary', { mediaType: 'text/html; injected=yes' }], ['contact', { name: '' }]]) assert.throws(() => compileRecord(record(kind, values)));
});
test('a read DataView offset is respected and binary copied through template retains every byte', () => {
  const backing = new Uint8Array([99, 0, 255, 42, 88]);
  const original = { recordType: 'mime', mediaType: 'application/octet-stream', id: '', data: new DataView(backing.buffer, 1, 3), toRecords: () => null };
  const snapshot = snapshotRecord(original);
  assert.equal(snapshot.base64, 'AP8q');
  assert.deepEqual([...restoreRecord(snapshot).data], [0, 255, 42]);
  assert.equal(messageSignature({ records: [original] }), messageSignature({ records: [restoreRecord(snapshot)] }));
});
test('read-back signatures compare decoded text, ignore its wire encoding and detect a wrong tag', () => {
  const written = compileDraft(draft([record('text', { text: 'Текст' }), record('url', { url: 'example.com' })]));
  const read = { records: [
    { recordType: 'text', lang: 'ru', id: '', encoding: 'utf-8', data: new DataView(new TextEncoder().encode('Текст').buffer) },
    { recordType: 'url', id: '', data: new DataView(new TextEncoder().encode('https://example.com/').buffer) },
  ] };
  assert.equal(messageSignature(written), messageSignature(read));
  read.records[0].lang = 'en'; assert.notEqual(messageSignature(written), messageSignature(read));
});
test('imports whitelist fields, reject executable links and excessive nested records', () => {
  const value = templateFile(draft([record('text', { text: 'Привет' })]));
  value.records[0].values.extra = 'discard'; assert.deepEqual(Object.keys(validateDraft(value).records[0].values), ['text']);
  assert.throws(() => validateDraft({ ...value, records: [{ kind: 'raw', snapshot: { recordType: 'url', text: 'javascript:alert(1)' } }] }));
  let nested = { recordType: 'unknown', base64: '' };
  for (let i = 0; i < 10; i++) nested = { recordType: 'smart-poster', records: [nested] };
  assert.throws(() => restoreRecord(nested));
  assert.throws(() => validateDraft({ ...value, name: 'a'.repeat(81) }));
});
test('QR drafts are fragment-only, round-trip Cyrillic and reject damaged or oversized payloads', () => {
  const source = draft([record('wifi', { ssid: 'Гости', password: '12345678' })]);
  const url = new URL(draftLink('https://nfc.example/old?session=private', source));
  assert.equal(url.pathname, '/'); assert.equal(url.search, '');
  const parsed = parseDraftLink(url.hash); assert.equal(parsed.records[0].values.ssid, 'Гости');
  assert.equal(parsed.records[0].values.password, '12345678');
  assert.throws(() => parseDraftLink('#draft=bm90LWpzb24'));
  assert.throws(() => draftLink(url.origin, draft([record('text', { text: 'я'.repeat(2000) })])));
});
test('storage failures remain visible; corrupted local templates are ignored without being deleted', () => {
  const broken = { getItem: () => '{bad', setItem: () => { throw new Error('Quota'); } };
  assert.equal(saveLocal(broken, 'key', {}), false); assert.equal(loadLocal(broken, 'key', 'fallback'), 'fallback');
  const valid = templateFile(draft([record('text', { text: 'A' })]));
  let writes = 0; const storage = { getItem: () => JSON.stringify([{ bad: true }, valid]), setItem: () => { writes++; } };
  assert.equal(loadTemplates(storage).length, 1); assert.equal(writes, 0);
});
