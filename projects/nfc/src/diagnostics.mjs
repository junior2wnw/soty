import { fromBase64, toBase64, normalUrl, locationUrl, restoreRecord, estimateBytes, compileDraft } from './records.mjs';

const encoder = new TextEncoder();
const backgroundSchemes = new Set(['https:', 'http:', 'tel:', 'mailto:', 'sms:', 'facetime:', 'facetime-audio:', 'x-hm:']);
const unknownHardware = Object.freeze({ chipModel: null, technology: null, capacityBytes: null, writable: null });
export function recordBytes(record) {
  if (typeof record.base64 === 'string') return fromBase64(record.base64);
  if (typeof record.text === 'string') return encoder.encode(record.text);
  return new Uint8Array();
}
export function recordDescription(record) {
  const types = { url: ['URI · U', 1], text: ['Текст · T', 1], 'absolute-url': ['Absolute URI', 3], mime: ['MIME', 2], empty: ['Пустая', 0], unknown: ['Unknown', 5], 'smart-poster': ['Smart Poster · Sp', 1] };
  const [label, tnf] = types[record.recordType] || [record.recordType, record.recordType.startsWith(':') ? 1 : record.recordType.includes(':') ? 4 : null];
  return { label, tnf, byteLength: record.redacted ? record.byteLength ?? null : record.records ? null : recordBytes(record).length };
}
function safeWebLink(text) {
  if (typeof text !== 'string' || /\s/.test(text.trim())) return null;
  try { const value = normalUrl(text); const url = new URL(value); return url.hostname.includes('.') || url.hostname.startsWith('[') || url.hostname === 'localhost' ? value : null; } catch { return null; }
}
function repairLink(records) {
  for (const record of records) {
    if (['url', 'absolute-url', 'text'].includes(record.recordType)) {
      const link = safeWebLink(record.text); if (link) return link;
      const geo = /^geo:([-+\d.]+),([-+\d.]+)$/.exec(record.text || '');
      if (geo) { try { return locationUrl({ latitude: geo[1], longitude: geo[2] }); } catch { /* Unsupported coordinates stay unchanged. */ } }
    }
    if (record.records) { const link = repairLink(record.records); if (link) return link; }
  }
  return null;
}
export function iphoneFormat(records) {
  const first = records.find(record => record.recordType === 'url');
  const candidate = repairLink(records);
  if (!first) return { status: candidate ? 'repairable' : 'no-uri', title: 'Обычной URL-записи нет', detail: candidate ? 'Адрес можно подготовить как отдельную ссылку.' : 'Для фонового открытия на iPhone нужна поддерживаемая URI-запись.', repairUrl: candidate };
  if (first.redacted || !first.text) return { status: 'unknown', title: 'Адрес не включён в отчёт', detail: 'Без содержимого нельзя проверить схему ссылки.', repairUrl: null };
  let url; try { url = new URL(first.text); } catch { return { status: 'invalid', title: 'Проверьте адрес', detail: 'Первая URI-запись содержит некорректный адрес.', repairUrl: candidate }; }
  if (url.username || url.password) return { status: 'invalid', title: 'В ссылке есть учётные данные', detail: 'Подготовьте обычную ссылку без логина и пароля.', repairUrl: null };
  if (!backgroundSchemes.has(url.protocol)) return { status: candidate ? 'repairable' : 'unsupported', title: 'Нужен другой формат ссылки', detail: 'Эта схема не входит в опубликованный список фонового чтения iPhone.', repairUrl: candidate };
  return { status: 'supported', title: 'Формат ссылки подходит для iPhone', detail: 'Совместимость самого чипа браузер не определяет.', repairUrl: candidate, scheme: url.protocol };
}
export function analyzeTag(result) {
  const records = result.records || [];
  let estimatedNdefBytes = null;
  try { if (records.length && !records.some(record => record.redacted)) estimatedNdefBytes = estimateBytes({ records: records.map(record => restoreRecord(record)) }); } catch { /* Readable records need not be writable by this application. */ }
  return { recordCount: records.length, records: records.map(recordDescription), estimatedNdefBytes, iphone: iphoneFormat(records), hardware: { ...unknownHardware }, origin: result.origin === 'file' ? 'file' : 'tag' };
}
function sanitized(record, { payload, identifiers }, budget, depth = 0) {
  if (!record || typeof record !== 'object' || Array.isArray(record) || depth > 8 || ++budget.records > 256 || typeof record.recordType !== 'string' || !record.recordType || record.recordType.length > 255) throw new Error('Некорректная запись в отчёте.');
  const value = { recordType: record.recordType };
  for (const key of ['lang', 'encoding', 'mediaType']) {
    if (record[key] !== undefined) { if (typeof record[key] !== 'string' || record[key].length > 255) throw new Error('Некорректные сведения записи.'); value[key] = record[key]; }
  }
  if (identifiers && record.id) { if (typeof record.id !== 'string' || record.id.length > 255) throw new Error('Некорректный ID записи.'); value.id = record.id; }
  if (record.records !== undefined) {
    if (!Array.isArray(record.records) || record.records.length > 128) throw new Error('Слишком много вложенных записей.');
    value.records = record.records.map(item => sanitized(item, { payload, identifiers }, budget, depth + 1));
  } else {
    if (record.byteLength !== undefined && record.byteLength !== null && (!Number.isSafeInteger(record.byteLength) || record.byteLength < 0 || record.byteLength > 65_536)) throw new Error('Некорректный размер данных.');
    if (payload && !record.redacted) {
      if (record.text !== undefined) { if (typeof record.text !== 'string' || record.text.length > 65_536) throw new Error('Слишком большой текст.'); value.text = record.text; }
      if (record.base64 !== undefined) { const bytes = fromBase64(record.base64); if (bytes.length > 65_536) throw new Error('Слишком большие данные.'); value.base64 = toBase64(bytes); }
      if (['text', 'url', 'absolute-url'].includes(record.recordType) && value.text === undefined) throw new Error('В отчёте отсутствует текст записи.');
      if (!['text', 'url', 'absolute-url', 'empty'].includes(record.recordType) && value.base64 === undefined) value.base64 = '';
      if (value.text !== undefined && value.base64 !== undefined) {
        const encoding = record.recordType === 'text' ? record.encoding || 'utf-8' : 'utf-8';
        if (new TextDecoder(encoding).decode(fromBase64(value.base64)) !== value.text) throw new Error('Текст отчёта отличается от его данных.');
      }
      value.byteLength = recordBytes(value).length;
    } else { value.redacted = true; value.byteLength = record.byteLength ?? (record.redacted ? null : recordBytes(record).length); }
    budget.bytes += value.byteLength || 0;
    if (budget.bytes > 65_536) throw new Error('Отчёт превышает 64 КБ данных NFC.');
  }
  return value;
}
export function tagReport(result, { includePayload = false, includeIdentifiers = false, now = new Date() } = {}) {
  const budget = { records: 0, bytes: 0 };
  if (!Array.isArray(result.records) || result.records.length > 128) throw new Error('Некорректный список записей.');
  const report = { schema: 'soty.nfc-report.v1', capturedAt: result.capturedAt || now.toISOString(), exportedAt: now.toISOString(), provenance: result.origin === 'file' ? 'imported-report' : 'web-nfc', payloadIncluded: includePayload, identifiersIncluded: includeIdentifiers,
    dataRepresentation: 'web-nfc-decoded-record-data', hardware: { ...unknownHardware }, records: result.records.map(record => sanitized(record, { payload: includePayload, identifiers: includeIdentifiers }, budget)) };
  if (includeIdentifiers && result.serialNumber) report.serialNumber = String(result.serialNumber).slice(0, 255);
  return report;
}
export function importTagReport(value) {
  if (!value || value.schema !== 'soty.nfc-report.v1' || value.dataRepresentation !== 'web-nfc-decoded-record-data' || !Array.isArray(value.records) || value.records.length > 128 || typeof value.payloadIncluded !== 'boolean' || typeof value.identifiersIncluded !== 'boolean') throw new Error('Это не отчёт NFC из Сот.');
  const budget = { records: 0, bytes: 0 }, records = value.records.map(record => sanitized(record, { payload: value.payloadIncluded, identifiers: value.identifiersIncluded }, budget));
  const capturedAt = typeof value.capturedAt === 'string' && value.capturedAt.length < 64 && Number.isFinite(Date.parse(value.capturedAt)) ? value.capturedAt : null;
  const serialNumber = value.identifiersIncluded && typeof value.serialNumber === 'string' && value.serialNumber.length <= 255 ? value.serialNumber : '';
  return { origin: 'file', records, capturedAt, serialNumber };
}
export function copyTagDraft(result) {
  if (!result.records?.length || result.records.some(record => record.redacted)) throw new Error('Для копирования нужен отчёт с содержимым записей.');
  const records = result.records.map(snapshot => { restoreRecord(snapshot); return { key: crypto.randomUUID(), kind: 'raw', snapshot }; });
  const draft = { schema: 'soty.nfc-template.v1', name: 'Копия метки', records };
  compileDraft(draft); return draft;
}
