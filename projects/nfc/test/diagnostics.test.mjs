import test from 'node:test';
import assert from 'node:assert/strict';
import { analyzeTag, iphoneFormat, tagReport, importTagReport, copyTagDraft, recordBytes, recordDescription } from '../src/diagnostics.mjs';
import { compileDraft, compileRecord, makeRecord, snapshotRecord, snapshotMessage, restoreRecord, messageSignature, validateDraft } from '../src/records.mjs';
import { restoreLocalDraft } from '../src/storage.mjs';
const url = text => ({ recordType: 'url', text });
const read = records => ({ records, serialNumber: '04:11:22:33', origin: 'tag' });
test('iPhone analysis checks the first top-level URI and never certifies the physical chip', () => {
  const result = analyzeTag(read([url('https://example.com')]));
  assert.equal(result.iphone.status, 'supported'); assert.equal(result.records[0].tnf, 1);
  assert.deepEqual(result.hardware, { chipModel: null, technology: null, capacityBytes: null, writable: null });
  assert.equal(iphoneFormat([url('geo:0,0'), url('https://example.com')]).status, 'repairable');
  assert.equal(iphoneFormat([url('javascript:alert(1)')]).status, 'unsupported');
  assert.equal(iphoneFormat([url('https://secret@example.com')]).status, 'invalid');
  assert.equal(iphoneFormat([{ recordType: 'url', redacted: true }]).status, 'unknown');
});
test('text, absolute URI, nested poster and geo links can be prepared as a safe ordinary web link', () => {
  for (const records of [[{ recordType: 'text', text: 't.me/example', lang: 'ru' }], [{ recordType: 'absolute-url', text: 'https://example.com' }], [{ recordType: 'smart-poster', records: [url('https://example.com')] }]]) {
    const result = iphoneFormat(records); assert.equal(result.status, 'repairable'); assert.match(result.repairUrl, /^https:\/\//);
  }
  const result = iphoneFormat([url('geo:55.7,37.6')]); assert.match(result.repairUrl, /^https:\/\/www.google.com\/maps\//);
  assert.equal(iphoneFormat([{ recordType: 'text', text: 'Просто заметка', lang: 'ru' }]).repairUrl, null);
});
test('default reports exclude payloads, serial and every nested record ID', () => {
  const records = [{ recordType: 'smart-poster', id: 'private-id', records: [url('https://example.com/private-token'), { recordType: 'text', lang: 'ru', text: 'secret password', id: 'private-nested' }] }];
  const report = tagReport(read(records), { now: new Date('2026-10-11T00:00:00Z') });
  const json = JSON.stringify(report);
  for (const value of ['04:11', 'private-token', 'secret password', 'private-id', 'private-nested']) assert.equal(json.includes(value), false);
  const imported = importTagReport(report); assert.equal(imported.origin, 'file'); assert.equal(imported.records[0].records[0].redacted, true);
  assert.throws(() => copyTagDraft(imported));
});
test('opt-in report payloads round-trip exact API bytes including UTF-16; hardware claims are ignored on import', () => {
  const bytes = new Uint8Array([0xff, 0xfe, 0x41, 0]);
  const snapshot = snapshotRecord({ recordType: 'text', lang: 'en', encoding: 'utf-16', id: 'record-1', data: bytes });
  const report = tagReport(read([snapshot]), { includePayload: true, includeIdentifiers: true });
  report.hardware = { chipModel: 'NTAG213', writable: true };
  const imported = importTagReport(report);
  assert.deepEqual([...recordBytes(imported.records[0])], [...bytes]); assert.equal(imported.records[0].text, 'A');
  assert.equal(recordDescription(imported.records[0]).byteLength, 4); assert.equal(analyzeTag(imported).hardware.chipModel, null);
  assert.equal(compileDraft(copyTagDraft(imported)).records[0].data, 'A');
});
test('report imports reject forged representations, excessive nesting, invalid bytes and oversize payloads', () => {
  const base = tagReport(read([url('https://example.com')]), { includePayload: true });
  assert.throws(() => importTagReport({ ...base, dataRepresentation: 'raw-chip-memory' }));
  assert.throws(() => importTagReport({ ...base, records: [{ recordType: 'url', text: 'https://example.com', base64: 'YmFk' }] }));
  assert.throws(() => importTagReport({ ...base, records: [{ recordType: 'mime', base64: '***' }] }));
  assert.throws(() => importTagReport({ ...base, records: Array(129).fill(url('https://example.com')) }));
  let nested = url('https://example.com'); for (let i = 0; i < 10; i++) nested = { recordType: 'smart-poster', records: [nested] };
  assert.throws(() => importTagReport({ ...base, records: [nested] }));
  assert.throws(() => tagReport(read([{ recordType: 'text', lang: 'ru', text: 'x'.repeat(65_537) }]), { includePayload: true }));
  assert.throws(() => copyTagDraft(read(Array(17).fill(url('https://example.com')))));
  assert.throws(() => copyTagDraft(read([{ recordType: 'text', lang: 'ru', text: 'x'.repeat(20_000) }])));
});
test('additional builders create bounded NDEF records; coordinates use a cross-platform HTTPS map', () => {
  const record = (kind, values) => ({ ...makeRecord(kind), values: { ...makeRecord(kind).values, ...values } });
  assert.equal(compileRecord(record('app', { package: 'org.telegram.messenger' })).recordType, 'android.com:pkg');
  assert.throws(() => compileRecord(record('app', { package: 'bad package' })));
  assert.equal(compileRecord(record('external', { type: 'example.com:tool', text: 'данные' })).recordType, 'example.com:tool');
  assert.throws(() => compileRecord(record('external', { type: 'text', text: 'данные' })));
  const poster = compileRecord(record('poster', { url: 'example.com', title: 'Название' }));
  assert.deepEqual(poster.data.records.map(record => record.recordType), ['url', 'text']);
  assert.match(compileRecord(record('location', { latitude: '0', longitude: '-180' })).data, /^https:\/\/www.google.com\/maps\/search\//);
});
test('nested read-back ignores text wire encoding but catches different poster content', () => {
  const written = { records: [{ recordType: 'smart-poster', data: { records: [{ recordType: 'url', data: 'https://example.com' }, { recordType: 'text', lang: 'en', data: 'A' }] } }] };
  const read = { records: [{ recordType: 'smart-poster', toRecords: () => [{ recordType: 'url', data: 'https://example.com/' }, { recordType: 'text', lang: 'en', encoding: 'utf-16', data: new Uint8Array([0xff, 0xfe, 65, 0]) }] }] };
  assert.equal(messageSignature(written), messageSignature(read));
  written.records[0].data.records[1].data = 'B'; assert.notEqual(messageSignature(written), messageSignature(read));
});
test('file-sized HEX drafts and saved templates restore without losing bytes', () => {
  const value = { schema: 'soty.nfc-template.v1', name: 'Файл', records: [{ ...makeRecord('binary'), values: { mediaType: 'application/octet-stream', hex: 'ab'.repeat(12_000) } }] };
  const local = restoreLocalDraft(value), imported = validateDraft(value);
  assert.equal(compileDraft(local).records[0].data.length, 12_000); assert.equal(compileDraft(imported).records[0].data.length, 12_000);
  assert.equal(local.records[0].values.hex, value.records[0].values.hex);
});

test('real Web NFC toRecords failures preserve MIME, Android package and local payload bytes', () => {
  let calls = 0;
  const mime = { recordType: 'mime', mediaType: 'application/octet-stream', data: new Uint8Array([0, 255]), toRecords() { calls++; throw new DOMException('Not nested', 'NotSupportedError'); } };
  assert.equal(snapshotRecord(mime).base64, 'AP8='); assert.equal(calls, 0);
  const app = { recordType: 'android.com:pkg', data: new TextEncoder().encode('org.telegram.messenger'), toRecords() { throw new TypeError('Not an NDEF message'); } };
  assert.equal(messageSignature({ records: [app] }), messageSignature({ records: [{ recordType: app.recordType, data: app.data }] }));
  const local = { recordType: ':act', data: new Uint8Array([0]), toRecords() { throw new DOMException('Not nested', 'NotSupportedError'); } };
  const poster = snapshotRecord({ recordType: 'smart-poster', toRecords: () => [{ recordType: 'url', data: 'https://example.com' }, local] });
  assert.equal(restoreRecord(poster).data.records[1].recordType, ':act');
  assert.equal(compileDraft(copyTagDraft(read([poster]))).records[0].data.records[1].data[0], 0);
  assert.throws(() => restoreRecord(poster.records[1]));
});

test('live snapshots bound total payload bytes and nested record count', () => {
  const leaf = size => ({ recordType: 'mime', mediaType: 'application/octet-stream', data: new Uint8Array(size) });
  assert.throws(() => snapshotRecord(leaf(65_537)));
  assert.throws(() => snapshotMessage({ records: [leaf(40_000), leaf(40_000)] }));
  const nested = { recordType: 'smart-poster', data: { records: Array(128).fill(leaf(1)) } };
  assert.throws(() => snapshotMessage({ records: [nested, nested] }));
  assert.equal(snapshotMessage({ records: [leaf(64_000)] })[0].byteLength, 64_000);
});
