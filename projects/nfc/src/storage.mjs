import { validateDraft, toBase64, fromBase64 } from './records.mjs';

export const storageKeys = { draft: 'soty.nfc.draft.v1', templates: 'soty.nfc.templates.v1' };
export const emptyDraft = () => ({ schema: 'soty.nfc-template.v1', name: '', records: [] });
export function saveLocal(storage, key, value) {
  try { storage.setItem(key, JSON.stringify(value)); return true; } catch { return false; }
}
export function loadLocal(storage, key, fallback) {
  try { const text = storage.getItem(key); if (!text || text.length > 524_288) return fallback; return JSON.parse(text); } catch { return fallback; }
}
export function loadTemplates(storage) {
  const values = loadLocal(storage, storageKeys.templates, []);
  if (!Array.isArray(values)) return [];
  return values.slice(0, 40).flatMap(value => {
    try { return [{ ...validateDraft(value), key: typeof value.key === 'string' ? value.key : crypto.randomUUID(), updatedAt: Number.isFinite(value.updatedAt) ? value.updatedAt : 0 }]; }
    catch { return []; }
  });
}
export function templateFile(draft) {
  return { schema: 'soty.nfc-template.v1', name: draft.name || 'Моя метка', records: draft.records.map(({ key, ...record }) => record) };
}
export function draftLink(base, draft) {
  const url = new URL('/', base), bytes = new TextEncoder().encode(JSON.stringify(templateFile(draft)));
  validateDraft(templateFile(draft));
  const encoded = toBase64(bytes).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  url.hash = 'draft=' + encoded;
  if (url.href.length > 2300) throw new Error('Шаблон слишком большой для QR. Скачайте его файлом.');
  return url.href;
}
export function parseDraftLink(hash) {
  if (!hash.startsWith('#draft=')) return null;
  const match = /^#draft=([A-Za-z0-9_-]{1,2400})$/.exec(hash);
  if (!match) throw new Error('Ссылка на шаблон повреждена. Попросите отправить её ещё раз.');
  let text = match[1].replace(/-/g, '+').replace(/_/g, '/'); text += '='.repeat((4 - text.length % 4) % 4);
  try { return validateDraft(JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(fromBase64(text)))); }
  catch { throw new Error('Ссылка на шаблон повреждена. Попросите отправить её ещё раз.'); }
}
