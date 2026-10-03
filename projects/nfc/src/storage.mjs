import { validateDraft, makeRecord, restoreRecord, toBase64, fromBase64 } from './records.mjs';

export const storageKeys = { draft: 'soty.nfc.draft.v1', templates: 'soty.nfc.templates.v1' };
export const templateLimit = 40;
export function restoreLocalDraft(value) {
  if (!value || value.schema !== 'soty.nfc-template.v1' || typeof value.name !== 'string' || value.name.length > 80
    || !Array.isArray(value.records) || value.records.length < 1 || value.records.length > 16) throw new Error('invalid_draft');
  const records = value.records.map(record => {
    if (record?.kind === 'raw') { restoreRecord(record.snapshot); return { key: crypto.randomUUID(), kind: 'raw', snapshot: record.snapshot }; }
    if (typeof record?.kind !== 'string') throw new Error('invalid_draft');
    const fresh = makeRecord(record?.kind);
    if (!record?.values || typeof record.values !== 'object' || Array.isArray(record.values)) throw new Error('invalid_draft');
    for (const key of Object.keys(fresh.values)) {
      const field = record.values[key];
      if (typeof field !== 'string' || field.length > 16_384) throw new Error('invalid_draft');
      fresh.values[key] = field;
    }
    return fresh;
  });
  // Incomplete form fields are valid drafts; only saved templates must compile.
  return { schema: value.schema, name: value.name, records };
}
function intact(text, key) {
  try {
    if (text.length > 524_288) return false;
    const value = JSON.parse(text);
    if (key === storageKeys.draft) restoreLocalDraft(value);
    else {
      if (!Array.isArray(value) || value.length > templateLimit) return false;
      value.forEach(validateDraft);
    }
    return true;
  } catch { return false; }
}
export function saveLocal(storage, key, value) {
  try {
    const next = JSON.stringify(value);
    if (Object.values(storageKeys).includes(key) && next.length > 524_288) return false;
    if (Object.values(storageKeys).includes(key)) {
      const previous = storage.getItem(key);
      if (previous !== null && !intact(previous, key)) {
        const recoveryKey = key + '.recovery', recovery = storage.getItem(recoveryKey);
        // Never overwrite an earlier recovery or the original if its copy fails.
        if (recovery !== null && recovery !== previous) return false;
        if (recovery === null) storage.setItem(recoveryKey, previous);
      }
    }
    storage.setItem(key, next); return true;
  } catch { return false; }
}
export function loadLocal(storage, key, fallback) {
  try { const text = storage.getItem(key); if (!text || text.length > 524_288) return fallback; return JSON.parse(text); } catch { return fallback; }
}
export function loadTemplates(storage) {
  const values = loadLocal(storage, storageKeys.templates, []);
  if (!Array.isArray(values)) return [];
  return values.slice(0, templateLimit).flatMap((value, index) => {
    try { return [{ ...validateDraft(value), key: typeof value.key === 'string' ? value.key : 'legacy:' + index + ':' + JSON.stringify(value), updatedAt: Number.isFinite(value.updatedAt) ? value.updatedAt : 0 }]; }
    catch { return []; }
  });
}
export function saveTemplates(storage, values) {
  try {
    if (!Array.isArray(values) || values.length > templateLimit) return false;
    values.forEach(validateDraft);
    return saveLocal(storage, storageKeys.templates, values);
  } catch { return false; }
}
export function recoveryFile(storage) {
  const items = {};
  try {
    for (const key of Object.values(storageKeys)) {
      const archived = storage.getItem(key + '.recovery'), current = storage.getItem(key);
      if (archived !== null) items[key + '.recovery'] = archived;
      if (current !== null && !intact(current, key)) items[key] = current;
    }
  } catch { return null; }
  return Object.keys(items).length ? { schema: 'soty.nfc-recovery.v1', items } : null;
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
