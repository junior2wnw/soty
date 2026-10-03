const encoder = new TextEncoder();
export const recordTypes = Object.freeze([
  { id: 'url', title: 'Ссылка', detail: 'Сайт, записка или приложение', icon: 'link', defaults: { url: '' } },
  { id: 'text', title: 'Текст', detail: 'Инструкция или короткая записка', icon: 'text', defaults: { text: '' } },
  { id: 'contact', title: 'Контакт', detail: 'Имя, телефон и почта', icon: 'person', defaults: { name: '', phone: '', email: '', company: '', url: '' } },
  { id: 'wifi', title: 'Wi-Fi', detail: 'Название сети и пароль', icon: 'wifi', defaults: { ssid: '', security: 'wpa2', password: '' } },
  { id: 'phone', title: 'Телефон', detail: 'Открыть номер для звонка', icon: 'phone', defaults: { phone: '' } },
  { id: 'email', title: 'Письмо', detail: 'Адрес, тема и текст', icon: 'mail', defaults: { email: '', subject: '', body: '' } },
  { id: 'sms', title: 'Сообщение', detail: 'Номер и текст SMS', icon: 'chat', defaults: { phone: '', body: '' } },
  { id: 'location', title: 'Место', detail: 'Координаты для карты', icon: 'pin', defaults: { latitude: '', longitude: '' } },
  { id: 'json', title: 'JSON', detail: 'Данные для своего приложения', icon: 'code', defaults: { json: '' } },
  { id: 'binary', title: 'Свои данные', detail: 'MIME-тип и байты в HEX', icon: 'chip', defaults: { mediaType: 'application/octet-stream', hex: '' } },
]);
const fail = message => { throw new Error(message); };
const required = (value, label) => typeof value === 'string' && value.trim() ? value.trim() : fail('Заполните поле «' + label + '».');
const untrimmed = (value, label) => { required(value, label); return value; };
const email = value => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value) ? value : fail('Проверьте адрес электронной почты.');
const phone = value => { const number = required(value, 'Телефон').replace(/[\s()\-]/g, ''); return /^\+?[0-9]{3,20}$/.test(number) ? number : fail('Укажите номер телефона, например +7 999 123-45-67.'); };
export function normalUrl(value) {
  let text = required(value, 'Ссылка');
  if (!/^[a-z][a-z0-9+.-]*:/i.test(text)) text = 'https://' + text;
  let url; try { url = new URL(text); } catch { return fail('Проверьте ссылку. Например: https://example.com'); }
  if (!['https:', 'http:'].includes(url.protocol) || url.username || url.password) return fail('Используйте ссылку http или https без логина и пароля.');
  return url.href;
}
const cardText = value => String(value || '').replace(/\\/g, '\\\\').replace(/\r?\n/g, '\\n').replace(/,/g, '\\,').replace(/;/g, '\\;');
function tlv(type, bytes) { const result = new Uint8Array(4 + bytes.length); new DataView(result.buffer).setUint16(0, type); new DataView(result.buffer).setUint16(2, bytes.length); result.set(bytes, 4); return result; }
const u16 = value => new Uint8Array([value >> 8, value & 255]);
function concat(parts) { const result = new Uint8Array(parts.reduce((size, part) => size + part.length, 0)); let offset = 0; for (const part of parts) { result.set(part, offset); offset += part.length; } return result; }
export function wifiPayload(values) {
  const ssid = encoder.encode(untrimmed(values.ssid, 'Название сети'));
  if (ssid.length > 32) return fail('Название Wi-Fi должно занимать не больше 32 байт.');
  if (!['open', 'wpa2'].includes(values.security)) return fail('Выберите тип защиты Wi-Fi.');
  const password = values.security === 'open' ? '' : untrimmed(values.password, 'Пароль Wi-Fi');
  if (values.security !== 'open' && !(password.length >= 8 && password.length <= 63 && encoder.encode(password).length <= 63) && !/^[0-9a-f]{64}$/i.test(password)) return fail('Для WPA/WPA2 нужен пароль от 8 до 63 байт или 64 символа HEX.');
  return tlv(0x100e, concat([tlv(0x1026, new Uint8Array([1])), tlv(0x1045, ssid), tlv(0x1003, u16(values.security === 'open' ? 1 : 0x20)),
    tlv(0x100f, u16(values.security === 'open' ? 1 : 8)), tlv(0x1027, encoder.encode(password)), tlv(0x1020, new Uint8Array(6).fill(255))]));
}
export function makeRecord(kind = 'url') {
  const type = recordTypes.find(type => type.id === kind); if (!type) return fail('Неизвестный тип записи.');
  return { key: crypto.randomUUID(), kind, values: { ...type.defaults } };
}
export function compileRecord(record) {
  const v = record.values;
  switch (record.kind) {
    case 'url': return { recordType: 'url', data: normalUrl(v.url) };
    case 'text': return { recordType: 'text', lang: 'ru', data: untrimmed(v.text, 'Текст') };
    case 'phone': return { recordType: 'url', data: 'tel:' + phone(v.phone) };
    case 'email': return { recordType: 'url', data: 'mailto:' + encodeURIComponent(email(required(v.email, 'Кому'))).replace(/%40/gi, '@') + '?' + new URLSearchParams({ subject: v.subject || '', body: v.body || '' }).toString().replace(/\+/g, '%20') };
    case 'sms': return { recordType: 'url', data: 'sms:' + phone(v.phone) + '?body=' + encodeURIComponent(v.body || '') };
    case 'location': {
      const latitude = Number(required(v.latitude, 'Широта')), longitude = Number(required(v.longitude, 'Долгота'));
      if (!Number.isFinite(latitude) || latitude < -90 || latitude > 90 || !Number.isFinite(longitude) || longitude < -180 || longitude > 180) return fail('Широта: от −90 до 90. Долгота: от −180 до 180.');
      return { recordType: 'url', data: 'geo:' + latitude + ',' + longitude };
    }
    case 'contact': {
      const name = required(v.name, 'Имя');
      const lines = ['BEGIN:VCARD', 'VERSION:3.0', 'N:;' + cardText(name) + ';;;', 'FN:' + cardText(name)];
      if (v.company?.trim()) lines.push('ORG:' + cardText(v.company.trim()));
      if (v.phone?.trim()) lines.push('TEL;TYPE=CELL:' + phone(v.phone));
      if (v.email?.trim()) lines.push('EMAIL:' + cardText(email(v.email.trim())));
      if (v.url?.trim()) lines.push('URL:' + cardText(normalUrl(v.url)));
      lines.push('END:VCARD');
      return { recordType: 'mime', mediaType: 'text/vcard', data: encoder.encode(lines.join('\r\n') + '\r\n') };
    }
    case 'wifi': return { recordType: 'mime', mediaType: 'application/vnd.wfa.wsc', data: wifiPayload(v) };
    case 'json': {
      const json = required(v.json, 'JSON'); try { JSON.parse(json); } catch { return fail('Проверьте JSON: скобки, кавычки и запятые.'); }
      return { recordType: 'mime', mediaType: 'application/json', data: encoder.encode(json) };
    }
    case 'binary': {
      const mediaType = required(v.mediaType, 'MIME-тип').toLowerCase();
      if (!/^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+$/i.test(mediaType)) return fail('Укажите MIME-тип, например application/octet-stream.');
      const hex = (v.hex || '').replace(/\s/g, '');
      if (hex.length % 2 || !/^[a-f0-9]*$/i.test(hex)) return fail('HEX состоит из пар символов 0–9 и A–F.');
      return { recordType: 'mime', mediaType, data: Uint8Array.from(hex.match(/.{2}/g) || [], byte => parseInt(byte, 16)) };
    }
    case 'raw': return restoreRecord(record.snapshot);
    default: return fail('Эту запись пока нельзя записать.');
  }
}
const uriPrefixes = ['https://www.', 'http://www.', 'https://', 'http://', 'tel:', 'mailto:', 'ftp://'];
export function estimateBytes(message) {
  return message.records.reduce((sum, record) => {
    const data = record.data, bytes = typeof data === 'string' ? encoder.encode(data).length : data?.records ? estimateBytes(data) : byteView(data).length;
    const type = record.recordType;
    let payload = bytes, typeBytes = 1;
    if (type === 'text') payload += 1 + encoder.encode(record.lang || 'ru').length;
    else if (type === 'url') { const prefix = uriPrefixes.find(prefix => data.startsWith(prefix)); payload += 1 - (prefix ? encoder.encode(prefix).length : 0); }
    else if (type === 'mime') typeBytes = encoder.encode(record.mediaType).length;
    else if (type === 'empty') { payload = 0; typeBytes = 0; }
    else if (type === 'absolute-url') { payload = 0; typeBytes = bytes; }
    else if (type === 'unknown') typeBytes = 0;
    else typeBytes = encoder.encode(type).length;
    const id = encoder.encode(record.id || '').length;
    return sum + 3 + (payload > 255 ? 3 : 0) + typeBytes + payload + (id ? 1 + id : 0);
  }, 0);
}
export function estimateTagBytes(message) { const bytes = estimateBytes(message); return bytes + (bytes >= 255 ? 5 : 3); }
export function compileDraft(draft) {
  if (!draft.records.length || draft.records.length > 16) return fail('Добавьте от 1 до 16 записей.');
  const message = { records: draft.records.map(compileRecord) };
  if (estimateBytes(message) > 16_384) return fail('Данные слишком большие для обычной NFC-метки. Запишите ссылку на файл.');
  return message;
}
export function byteView(data) {
  if (!data) return new Uint8Array();
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  return fail('Некорректные байты записи.');
}
export const toBase64 = bytes => { let text = ''; for (const byte of bytes) text += String.fromCharCode(byte); return btoa(text); };
export function fromBase64(text) {
  if (typeof text !== 'string' || text.length > 32_768 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(text)) return fail('Некорректные данные записи.');
  return Uint8Array.from(atob(text), ch => ch.charCodeAt(0));
}
export function snapshotRecord(record, depth = 0) {
  if (depth > 8) return fail('Запись содержит слишком много вложенных уровней.');
  const result = { recordType: record.recordType, id: record.id || '' };
  if (record.recordType === 'text') { result.lang = record.lang || 'ru'; result.encoding = record.encoding || 'utf-8'; result.text = typeof record.data === 'string' ? record.data : new TextDecoder(result.encoding).decode(byteView(record.data)); }
  else if (['url', 'absolute-url'].includes(record.recordType)) result.text = typeof record.data === 'string' ? record.data : new TextDecoder().decode(byteView(record.data));
  else {
    if (record.mediaType) result.mediaType = record.mediaType;
    const nested = typeof record.toRecords === 'function' ? record.toRecords() : record.data?.records;
    if (nested?.length) result.records = [...nested].map(item => snapshotRecord(item, depth + 1));
    else result.base64 = toBase64(byteView(record.data));
  }
  return result;
}
export function restoreRecord(snapshot, depth = 0) {
  if (!snapshot || typeof snapshot !== 'object' || Array.isArray(snapshot) || depth > 8) return fail('Некорректная запись в шаблоне.');
  const type = snapshot.recordType;
  if (typeof type !== 'string' || !(['text', 'url', 'absolute-url', 'mime', 'empty', 'unknown', 'smart-poster'].includes(type) || /^[a-z0-9.-]+:[a-z0-9_.:-]+$/i.test(type))) return fail('Неподдерживаемый тип записи в шаблоне.');
  const record = { recordType: type };
  if (snapshot.id) { if (typeof snapshot.id !== 'string' || snapshot.id.length > 255) return fail('Некорректный идентификатор записи.'); record.id = snapshot.id; }
  if (type === 'text') {
    if (typeof snapshot.text !== 'string' || typeof snapshot.lang !== 'string' || !/^[a-z0-9-]{1,32}$/i.test(snapshot.lang)) return fail('Некорректная текстовая запись.');
    record.lang = snapshot.lang; record.data = snapshot.text; record.encoding = 'utf-8';
  } else if (type === 'url' || type === 'absolute-url') {
    if (typeof snapshot.text !== 'string' || snapshot.text.length > 8192) return fail('Некорректная ссылка в шаблоне.');
    let url; try { url = new URL(snapshot.text); } catch { return fail('Некорректная ссылка в шаблоне.'); }
    if (!['https:', 'http:', 'tel:', 'mailto:', 'sms:', 'geo:'].includes(url.protocol) || url.username || url.password) return fail('Эту ссылку нельзя использовать в шаблоне.');
    record.data = url.href;
  } else if (type === 'mime') {
    if (typeof snapshot.mediaType !== 'string' || !/^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+$/i.test(snapshot.mediaType)) return fail('Некорректный MIME-тип.');
    record.mediaType = snapshot.mediaType; record.data = fromBase64(snapshot.base64);
  } else if (type === 'empty') record.data = new Uint8Array();
  else if (snapshot.records) {
    if (!Array.isArray(snapshot.records) || snapshot.records.length > 16) return fail('Некорректные вложенные записи.');
    record.data = { records: snapshot.records.map(item => restoreRecord(item, depth + 1)) };
  } else record.data = fromBase64(snapshot.base64 || '');
  return record;
}
export const messageSignature = message => JSON.stringify([...message.records].map(record => {
  const value = snapshotRecord(record); delete value.encoding;
  if (['url', 'absolute-url'].includes(value.recordType)) { try { value.text = new URL(value.text).href; } catch { /* A readable nonstandard URI stays literal. */ } }
  if (value.mediaType) value.mediaType = value.mediaType.toLowerCase();
  return value;
}));
export function validateDraft(value) {
  if (!value || value.schema !== 'soty.nfc-template.v1' || !Array.isArray(value.records) || value.records.length < 1 || value.records.length > 16) return fail('Это не шаблон NFC из «Сот».');
  if (typeof value.name !== 'string' || value.name.length > 80) return fail('Проверьте название шаблона.');
  const records = value.records.map(record => {
    if (!record || typeof record !== 'object') return fail('Некорректная запись.');
    if (record.kind === 'raw') { restoreRecord(record.snapshot); return { key: crypto.randomUUID(), kind: 'raw', snapshot: record.snapshot }; }
    const type = recordTypes.find(type => type.id === record.kind); if (!type || !record.values || typeof record.values !== 'object' || Array.isArray(record.values)) return fail('Неизвестный тип записи.');
    const values = { ...type.defaults }; for (const key of Object.keys(values)) { if (typeof record.values[key] !== 'string' || record.values[key].length > 16_384) return fail('Некорректное поле шаблона.'); values[key] = record.values[key]; }
    return { key: crypto.randomUUID(), kind: type.id, values };
  });
  const result = { schema: value.schema, name: value.name, records }; compileDraft(result); return result;
}
