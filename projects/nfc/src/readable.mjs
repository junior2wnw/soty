import { fromBase64 } from './records.mjs';
const decoder = new TextDecoder('utf-8', { fatal: true });
function attributes(bytes) {
  const values = new Map(), view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  for (let offset = 0; offset < bytes.length;) {
    if (offset + 4 > bytes.length) throw new Error('truncated');
    const type = view.getUint16(offset), length = view.getUint16(offset + 2); offset += 4;
    if (offset + length > bytes.length || values.has(type)) throw new Error('invalid');
    values.set(type, bytes.subarray(offset, offset + length)); offset += length;
  }
  return values;
}
export function readableRecord(snapshot) {
  if (snapshot.recordType !== 'mime' || typeof snapshot.base64 !== 'string') return null;
  try {
    const bytes = fromBase64(snapshot.base64), mime = snapshot.mediaType?.toLowerCase();
    if (mime === 'application/vnd.wfa.wsc') {
      const envelope = attributes(bytes), credential = envelope.get(0x100e); if (!credential) return null;
      const values = attributes(credential), ssid = values.get(0x1045), auth = values.get(0x1003);
      if (!ssid || ssid.length > 32 || auth?.length !== 2) return null;
      const security = new DataView(auth.buffer, auth.byteOffset, auth.byteLength).getUint16(0);
      return { kind: 'wifi', fields: [['Сеть', decoder.decode(ssid)], ['Защита', security === 1 ? 'Открытая сеть' : security === 32 ? 'WPA2' : 'Тип ' + security]],
        password: decoder.decode(values.get(0x1027) || new Uint8Array()) };
    }
    if (mime === 'text/vcard' || mime === 'text/x-vcard') {
      const text = decoder.decode(bytes); if (!/^BEGIN:VCARD\r?\n/i.test(text) || !/\r?\nEND:VCARD\r?\n?$/i.test(text)) return null;
      const labels = { FN: 'Имя', ORG: 'Организация', TEL: 'Телефон', EMAIL: 'Почта', URL: 'Сайт' }, fields = [];
      for (const line of text.replace(/\r?\n[ \t]/g, '').split(/\r?\n/)) {
        const colon = line.indexOf(':'); if (colon < 0) continue;
        const property = line.slice(0, colon).split(';')[0].toUpperCase(); if (!labels[property]) continue;
        const value = line.slice(colon + 1).replace(/\\([nN,;\\])/g, (_, ch) => /n/i.test(ch) ? '\n' : ch);
        fields.push([labels[property], value]);
      }
      return fields.length ? { kind: 'contact', fields } : null;
    }
  } catch { /* An unfamiliar payload remains available as raw bytes. */ }
  return null;
}
