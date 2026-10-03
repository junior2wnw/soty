import './style.css';
import QRCode from 'qrcode';
import { recordTypes, makeRecord, compileDraft, estimateBytes, estimateTagBytes, restoreRecord, validateDraft, fromBase64 } from './records.mjs';
import { createNfcController, nfcAvailability } from './nfc.mjs';
import { loadLocal, loadTemplates, saveLocal, storageKeys, templateFile, draftLink, parseDraftLink } from './storage.mjs';
import { readableRecord } from './readable.mjs';

const paths = {
  link: '<path d="m10 13 4-4m-6 8-1 1a4 4 0 0 1-6-6l4-4a4 4 0 0 1 6 0m2-1 1-1a4 4 0 0 1 6 6l-4 4a4 4 0 0 1-6 0"/>',
  text: '<path d="M4 5h16M4 10h16M4 15h10M4 20h7"/>',
  person: '<circle cx="12" cy="8" r="4"/><path d="M4 21v-2a8 8 0 0 1 16 0v2"/>',
  wifi: '<path d="M2 8a16 16 0 0 1 20 0M5 12a11 11 0 0 1 14 0m-11 4a6 6 0 0 1 8 0"/><circle cx="12" cy="20" r="1"/>',
  phone: '<rect x="6" y="2" width="12" height="20" rx="3"/><path d="M10 18h4"/>',
  mail: '<rect x="3" y="5" width="18" height="14" rx="2"/><path d="m3 6 9 7 9-7"/>',
  chat: '<path d="M21 11a9 9 0 0 1-9 9H3l2-5a9 9 0 1 1 16-4Z"/><path d="M8 9h8m-8 4h5"/>',
  pin: '<path d="M19 9c0 5-7 13-7 13S5 14 5 9a7 7 0 1 1 14 0Z"/><circle cx="12" cy="9" r="2"/>',
  code: '<path d="m8 6-6 6 6 6m8-12 6 6-6 6m-3-15-2 18"/>',
  chip: '<rect x="6" y="6" width="12" height="12" rx="2"/><path d="M9 2v4m6-4v4M9 18v4m6-4v4M2 9h4m-4 6h4m12-6h4m-4 6h4"/>',
  read: '<path d="M4 9V5a1 1 0 0 1 1-1h4m6 0h4a1 1 0 0 1 1 1v4m0 6v4a1 1 0 0 1-1 1h-4m-6 0H5a1 1 0 0 1-1-1v-4M2 12h20"/>',
  write: '<path d="m14 4 6 6M3 21l4-1L21 6a2 2 0 0 0-4-4L3 16v5Z"/>',
  templates: '<rect x="7" y="3" width="14" height="15" rx="2"/><path d="M17 18v3H3V6h4m4 2h6m-6 4h6"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  check: '<path d="m5 12 4 4L20 5"/>',
  close: '<path d="m6 6 12 12M6 18 18 6"/>',
  arrow: '<path d="M4 12h16m-6-6 6 6-6 6"/>',
  download: '<path d="M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5"/>',
  upload: '<path d="M12 16V4m-5 5 5-5 5 5M4 16v5h16v-5"/>',
  lock: '<rect x="5" y="10" width="14" height="11" rx="2"/><path d="M8 10V6a4 4 0 0 1 8 0v4"/><circle cx="12" cy="15" r="1"/>',
  trash: '<path d="M3 6h18M9 6V3h6v3M5 6l1 15h12l1-15M10 10v7m4-7v7"/>',
  copy: '<rect x="8" y="8" width="13" height="13" rx="2"/><path d="M16 8V3H3v13h5"/>',
  help: '<circle cx="12" cy="12" r="9"/><path d="M9 8a3 3 0 0 1 6 0c0 3-3 2-3 5m0 3v1"/>',
  nfc: '<path d="M7 8a6 6 0 0 1 0 8m5-12a12 12 0 0 1 0 16m5-18a16 16 0 0 1 0 20"/>',
  external: '<path d="M14 3h7v7m0-7L10 14M10 3H3v18h18v-7"/>',
};
function icon(name) { const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg'); svg.setAttribute('viewBox', '0 0 24 24'); svg.setAttribute('fill', 'none'); svg.setAttribute('stroke', 'currentColor'); svg.setAttribute('stroke-width', '1.7'); svg.setAttribute('stroke-linecap', 'round'); svg.setAttribute('stroke-linejoin', 'round'); svg.setAttribute('aria-hidden', 'true'); svg.innerHTML = paths[name] || paths.nfc; return svg; }
function el(tag, className = '', text = '') { const node = document.createElement(tag); if (className) node.className = className; if (text) node.textContent = text; return node; }
const recordCount = count => count + ' ' + (count === 1 ? 'запись' : count < 5 ? 'записи' : 'записей');
function button(label, symbol, className, action) { const node = el('button', className || 'button'); node.type = 'button'; node.setAttribute('aria-label', label); if (symbol) node.append(icon(symbol)); node.append(el('span', '', label)); if (action) node.addEventListener('click', action); return node; }
let storage; try { storage = window.localStorage; } catch { storage = { getItem: () => null, setItem: () => { throw new Error('unavailable'); } }; }
const embedded = window.top !== window;
const availability = nfcAvailability({ Reader: window.NDEFReader, secure: window.isSecureContext, embedded, userAgent: navigator.userAgent });
let draft = { schema: 'soty.nfc-template.v1', name: '', records: [makeRecord()] };
const recalled = loadLocal(storage, storageKeys.draft, null);
if (recalled?.schema === draft.schema && Array.isArray(recalled.records) && recalled.records.length > 0 && recalled.records.length <= 16) {
  try {
    const records = recalled.records.map(record => {
      if (record.kind === 'raw') { restoreRecord(record.snapshot); return { key: crypto.randomUUID(), kind: 'raw', snapshot: record.snapshot }; }
      const type = recordTypes.find(item => item.id === record.kind); if (!type || !record.values) throw new Error('invalid');
      const fresh = makeRecord(type.id); for (const key of Object.keys(fresh.values)) if (typeof record.values[key] === 'string' && record.values[key].length <= 16_384) fresh.values[key] = record.values[key]; return fresh;
    }); draft = { ...draft, name: typeof recalled.name === 'string' ? recalled.name.slice(0, 80) : '', records };
  } catch { /* Retain the invalid stored value; opening the app never erases it. */ }
}
let incomingDraft = null, initialError = '';
if (location.hash.startsWith('#draft=')) {
  try { incomingDraft = parseDraftLink(location.hash); } catch (error) { initialError = error.message; }
  history.replaceState(null, '', location.pathname + location.search);
}
let templates = loadTemplates(storage), selected = 0, view = 'write', capacity = 0, overwrite = false, lastRead = null, operation = { phase: 'idle' }, compiled = null;
let storageReady = true, noticeTimer;
const root = document.querySelector('#app');
const header = el('header', 'topbar'), brand = el('div', 'brand'), brandMark = el('span', 'brand-mark'); brandMark.append(icon('nfc'));
const brandCopy = el('div'); brandCopy.append(el('strong', '', 'Метки'), el('small', '', 'NFC · приложение в Сотах')); brand.append(brandMark, brandCopy);
const headerActions = el('div', 'header-actions'); headerActions.append(button('На телефон', 'phone', 'button button-small', () => void share()), button('Как это работает', 'help', 'icon-button help-button', help));
header.append(brand, headerActions);
const intro = el('section', 'intro'); intro.append(el('p', 'eyebrow', 'ИЗ ЦИФРОВОГО — В ФИЗИЧЕСКОЕ'), el('h1', '', 'Одно касание. Нужное действие.'), el('p', 'intro-detail', 'Ссылка, контакт или Wi-Fi на маленькой метке. Подготовьте данные, приложите телефон — готово.'));
const nav = el('nav', 'tabs'); nav.setAttribute('aria-label', 'Работа с NFC');
for (const [id, label, symbol] of [['write', 'Записать', 'write'], ['read', 'Прочитать', 'read'], ['templates', 'Шаблоны', 'templates']]) {
  const target = button(label, symbol, 'tab', () => { if (controller.busy) { toast('Сначала завершите или остановите текущую операцию.'); return; } view = id; render(); });
  target.dataset.view = id; nav.append(target);
}
const support = el('aside', 'support'), supportCopy = el('div', 'support-copy'); supportCopy.append(el('strong', '', availability.title), el('p', '', availability.detail)); support.append(icon(availability.supported ? 'check' : embedded ? 'external' : 'phone'), supportCopy);
if (embedded) support.append(button('Открыть отдельно', 'external', 'button button-primary button-small', openSeparately));
else if (!availability.supported) support.append(button('Открыть на телефоне', 'arrow', 'button button-small', () => void share()));
const content = el('main', 'content'); content.id = 'main'; content.tabIndex = -1;
const footer = el('footer', 'footer'); footer.append(el('span', '', 'Ваши данные остаются на этом устройстве.'), button('О метках и совместимости', 'help', 'text-button', help));
const toastNode = el('div', 'toast'); toastNode.setAttribute('role', 'status'); toastNode.hidden = true;
root.append(header, intro, nav, support, content, footer, toastNode);
const controller = createNfcController({ Reader: window.NDEFReader, onState: next => { operation = next; updateOperation(); if (next.phase === 'read') { lastRead = next.result; if (view === 'read') renderReadResult(); } } });
function persist() { storageReady = saveLocal(storage, storageKeys.draft, draft); const note = document.querySelector('[data-draft-status]'); if (note) { note.textContent = storageReady ? 'Черновик на этом устройстве' : 'Не удалось сохранить черновик. Скачайте его перед закрытием.'; note.classList.toggle('warning', !storageReady); } }
function toast(text) { clearTimeout(noticeTimer); toastNode.textContent = text; toastNode.hidden = false; noticeTimer = setTimeout(() => { toastNode.hidden = true; }, 4500); }
function modal(title) {
  const previous = document.activeElement, dialog = el('dialog', 'dialog'), head = el('div', 'dialog-head'), body = el('div', 'dialog-body');
  const heading = el('h2', '', title); heading.id = 'dialog-' + crypto.randomUUID(); dialog.setAttribute('aria-labelledby', heading.id);
  const close = button('Закрыть', 'close', 'icon-button', () => dialog.close()); close.setAttribute('aria-label', 'Закрыть'); head.append(heading, close); dialog.append(head, body); document.body.append(dialog);
  dialog.addEventListener('close', () => { dialog.remove(); if (previous?.isConnected) previous.focus(); }, { once: true }); dialog.addEventListener('click', event => { if (event.target === dialog) { const box = dialog.getBoundingClientRect(); if (event.clientX < box.left || event.clientX > box.right || event.clientY < box.top || event.clientY > box.bottom) dialog.close(); } });
  dialog.showModal(); return { dialog, body };
}
function confirm(title, text, label, action, dangerous = false) {
  const { dialog, body } = modal(title); body.append(el('p', 'muted', text)); const row = el('div', 'action-row');
  row.append(button('Отмена', undefined, 'button', () => dialog.close()), button(label, dangerous ? 'trash' : 'check', 'button ' + (dangerous ? 'button-danger' : 'button-primary'), () => { dialog.close(); action(); })); body.append(row);
}
function render() {
  nav.querySelectorAll('button').forEach(node => { node.setAttribute('aria-current', node.dataset.view === view ? 'page' : 'false'); });
  content.replaceChildren(); operation = controller.busy ? operation : { phase: 'idle' };
  if (view === 'write') renderWrite(); else if (view === 'read') renderRead(); else renderTemplates();
}
function addRecord(kind) {
  if (draft.records.length >= 16) { toast('На одной метке может быть до 16 записей.'); return; }
  const record = makeRecord(kind);
  const current = draft.records[0];
  if (draft.records.length === 1 && current.kind === 'url' && !current.values.url.trim()) draft.records = [record];
  else draft.records.push(record);
  selected = draft.records.length - 1; persist(); render();
}
function chooseRecord() {
  const { dialog, body } = modal('Что запишем на метку?'); const grid = el('div', 'type-grid');
  for (const type of recordTypes) {
    const choice = button(type.title, type.icon, 'type-choice', () => { dialog.close(); addRecord(type.id); });
    choice.append(el('small', '', type.detail)); grid.append(choice);
  }
  body.append(grid, el('p', 'muted small', 'Можно добавить несколько записей. Телефон обычно открывает первую подходящую.'));
}
const fieldDefinitions = {
  url: [['url', 'Ссылка', 'https://example.com', 'url']],
  text: [['text', 'Текст', 'Например, инструкция к прибору', 'textarea']],
  contact: [['name', 'Имя', 'Как представить контакт'], ['phone', 'Телефон', '+7 999 123-45-67', 'tel'], ['email', 'Электронная почта', 'name@example.com', 'email'], ['company', 'Организация', 'Необязательно'], ['url', 'Сайт', 'Необязательно', 'url']],
  wifi: [['ssid', 'Название сети', 'Например, Home Wi-Fi'], ['security', 'Защита', '', 'select'], ['password', 'Пароль Wi-Fi', 'Пароль сети', 'password']],
  phone: [['phone', 'Телефон', '+7 999 123-45-67', 'tel']],
  email: [['email', 'Кому', 'name@example.com', 'email'], ['subject', 'Тема', 'Необязательно'], ['body', 'Текст письма', 'Необязательно', 'textarea']],
  sms: [['phone', 'Телефон', '+7 999 123-45-67', 'tel'], ['body', 'Текст сообщения', 'Сообщение для получателя', 'textarea']],
  location: [['latitude', 'Широта', '55.751244'], ['longitude', 'Долгота', '37.618423']],
  json: [['json', 'JSON', '{"name":"Моя метка"}', 'textarea']],
  binary: [['mediaType', 'MIME-тип', 'application/octet-stream'], ['hex', 'Байты в HEX', '01 02 0A FF', 'textarea']],
};
function editor(record) {
  const host = el('div', 'editor-fields');
  if (record.kind === 'raw') { host.append(el('p', 'muted', 'Запись скопирована с метки. При записи сохранятся её тип и содержимое.'), snapshotCard(record.snapshot)); return host; }
  for (const [key, title, placeholder, inputType] of fieldDefinitions[record.kind]) {
    if (record.kind === 'wifi' && key === 'password' && record.values.security === 'open') continue;
    const field = el('label', 'field'), label = el('span', 'field-label', title); const input = el(inputType === 'textarea' ? 'textarea' : inputType === 'select' ? 'select' : 'input', 'input');
    input.name = key; input.value = record.values[key]; input.setAttribute('aria-label', title); input.autocomplete = 'off';
    if (inputType === 'select') { for (const [value, title] of [['wpa2', 'WPA2 — с паролем'], ['open', 'Открытая — без пароля']]) { const option = el('option', '', title); option.value = value; option.selected = record.values[key] === value; input.append(option); } }
    else { input.placeholder = placeholder || ''; input.maxLength = key === 'ssid' ? 64 : record.kind === 'text' || inputType === 'textarea' ? 16_384 : 2048; if (input.tagName === 'INPUT') input.type = inputType === 'url' ? 'text' : inputType || 'text'; else input.rows = ['text', 'json', 'binary'].includes(record.kind) ? 6 : 3; }
    input.addEventListener('input', () => { record.values[key] = input.value; persist(); updatePreview(); });
    if (inputType === 'select') input.addEventListener('change', () => { record.values[key] = input.value; persist(); render(); });
    field.append(label, input);
    if (inputType === 'password') { const toggle = button('Показать пароль', undefined, 'text-button field-toggle', () => { const visible = input.type === 'password'; input.type = visible ? 'text' : 'password'; const label = visible ? 'Скрыть пароль' : 'Показать пароль'; toggle.querySelector('span').textContent = label; toggle.setAttribute('aria-label', label); }); field.append(toggle); }
    host.append(field);
  }
  if (record.kind === 'wifi') host.append(el('p', 'field-hint', 'Подключение по NFC зависит от телефона. iPhone не подключается к Wi-Fi по такой записи. Пароль будет доступен тем, кто прочитает метку.'));
  if (record.kind === 'binary' || record.kind === 'json') host.append(el('p', 'field-hint', 'Эти данные прочитает приложение, которое знает их формат. Большие файлы лучше открывать по ссылке.'));
  if (record.kind === 'location') host.append(el('p', 'field-hint', 'Укажите десятичные координаты. Точка отделяет дробную часть.'));
  return host;
}
function renderWrite() {
  const layout = el('div', 'write-layout'), builder = el('section', 'card builder'), side = el('aside', 'write-side');
  const head = el('div', 'section-head'); head.append(el('div', '', 'Что будет на метке'), el('span', 'count-label', recordCount(draft.records.length)));
  const choices = el('div', 'record-tabs'); choices.setAttribute('aria-label', 'Записи на метке');
  draft.records.forEach((record, index) => {
    const type = recordTypes.find(type => type.id === record.kind); const target = button((index + 1) + '. ' + (type?.title || 'С метки'), type?.icon || 'chip', 'record-tab', () => { selected = index; render(); }); target.setAttribute('aria-pressed', String(selected === index)); choices.append(target);
  });
  const selectedRecord = draft.records[selected] || draft.records[0]; selected = Math.min(selected, draft.records.length - 1);
  const quick = el('div', 'quick-types');
  for (const kind of ['url', 'text', 'wifi']) { const type = recordTypes.find(type => type.id === kind); quick.append(button(type.title, type.icon, 'button button-small', () => addRecord(kind))); }
  quick.append(button('Другой тип', 'plus', 'button button-small', chooseRecord));
  const editing = el('div', 'editing-head'); editing.append(el('h2', '', recordTypes.find(type => type.id === selectedRecord.kind)?.title || 'Содержимое с метки'));
  if (draft.records.length > 1) editing.append(button('Убрать запись', 'trash', 'icon-button', () => { draft.records.splice(selected, 1); selected = Math.max(0, selected - 1); persist(); render(); }));
  builder.append(head, choices, editing, editor(selectedRecord), quick);
  const actions = el('div', 'builder-footer'), draftStatus = el('span', 'small muted'); draftStatus.dataset.draftStatus = ''; actions.append(draftStatus, button('Сохранить шаблон', 'templates', 'text-button', saveTemplate)); builder.append(actions);
  const preview = el('section', 'card preview'); preview.setAttribute('aria-label', 'Предпросмотр метки'); preview.append(el('p', 'eyebrow', 'ВАША МЕТКА'));
  const tag = el('div', 'tag-visual'); tag.append(el('span', 'tag-hole'), icon('nfc')); preview.append(tag);
  const previewText = el('div', 'preview-copy'); previewText.dataset.preview = ''; preview.append(previewText); side.append(preview);
  const writeCard = el('section', 'card write-actions'); const capacityField = el('label', 'capacity-field'); capacityField.append(el('span', '', 'Размер метки'));
  const sizes = el('select', 'capacity-select'); sizes.setAttribute('aria-label', 'Размер метки');
  for (const [value, label] of [[0, 'Не знаю'], [144, 'NTAG213 · 144 Б'], [504, 'NTAG215 · 504 Б'], [888, 'NTAG216 · 888 Б']]) { const option = el('option', '', label); option.value = String(value); option.selected = capacity === value; sizes.append(option); }
  sizes.addEventListener('change', () => { capacity = Number(sizes.value); updatePreview(); }); capacityField.append(sizes);
  const meter = el('div', 'memory'); meter.dataset.memory = ''; const validation = el('p', 'validation'); validation.dataset.validation = '';
  const permission = el('label', 'check-row'), check = el('input'); check.type = 'checkbox'; check.checked = overwrite; check.addEventListener('change', () => { overwrite = check.checked; }); permission.append(check, el('span', '', 'Разрешить замену прежнего содержимого'));
  const write = button('Записать на метку', 'nfc', 'button button-primary button-wide', () => void writeDraft()); write.dataset.write = '';
  const status = operationPanel(); writeCard.append(capacityField, meter, validation, permission, write, status, el('p', 'write-hint', 'Поднесите метку после нажатия. Держите её до конца записи и проверки.'));
  const advanced = el('details', 'advanced'); advanced.append(el('summary', '', 'Очистка и защита метки')); const extra = el('div', 'advanced-body');
  const erase = button('Очистить содержимое', 'trash', 'text-button', () => confirm('Очистить метку?', 'Прежнее NDEF-содержимое будет заменено пустой записью. Это не полное стирание памяти чипа.', 'Очистить', () => void controller.write({ records: [{ recordType: 'empty' }] }, { overwrite: true }).catch(() => {}), true)); erase.disabled = !availability.supported;
  const lock = button('Запретить запись навсегда', 'lock', 'text-button danger-text', lockDialog); lock.disabled = !availability.supported || !window.NDEFReader?.prototype?.makeReadOnly;
  extra.append(erase, lock); advanced.append(extra); writeCard.append(advanced); side.append(writeCard); layout.append(builder, side); content.append(layout); persist(); updatePreview(); updateOperation();
}
function previewLabel(record) {
  if (record.kind === 'raw') return { title: record.snapshot.recordType === 'url' ? 'Откроет ссылку' : 'Данные с метки', detail: record.snapshot.text || record.snapshot.mediaType || 'Содержимое сохранится без изменений' };
  const v = record.values;
  return ({
    url: { title: 'Откроет ссылку', detail: v.url || 'Адрес вашего сайта' },
    text: { title: 'Покажет текст', detail: v.text || 'Ваша короткая записка' },
    contact: { title: 'Предложит сохранить контакт', detail: v.name || 'Имя контакта' },
    wifi: { title: 'Поделится сетью Wi-Fi', detail: v.ssid || 'Название вашей сети' },
    phone: { title: 'Откроет номер телефона', detail: v.phone || 'Номер для звонка' },
    email: { title: 'Подготовит письмо', detail: v.email || 'Адрес получателя' },
    sms: { title: 'Подготовит сообщение', detail: v.body || v.phone || 'Номер и текст SMS' },
    location: { title: 'Откроет место на карте', detail: v.latitude && v.longitude ? v.latitude + ', ' + v.longitude : 'Координаты места' },
    json: { title: 'Передаст данные приложению', detail: v.json || 'Ваш объект JSON' },
    binary: { title: 'Сохранит ваши байты', detail: v.mediaType || 'Данные своего формата' },
  })[record.kind];
}
function updatePreview() {
  const preview = document.querySelector('[data-preview]'); if (!preview) return;
  preview.replaceChildren(); const info = previewLabel(draft.records[0]); preview.append(el('h3', '', info.title), el('p', '', info.detail));
  if (draft.records.length > 1) preview.append(el('small', 'muted', '+ ещё ' + recordCount(draft.records.length - 1)));
  const memory = document.querySelector('[data-memory]'), validation = document.querySelector('[data-validation]'), write = document.querySelector('[data-write]');
  compiled = null; let bytes = 0, error = '';
  try { compiled = compileDraft(draft); bytes = estimateTagBytes(compiled); if (capacity && bytes > capacity) { error = 'Содержимое больше выбранной памяти. Сократите данные или возьмите метку большего размера.'; compiled = null; } }
  catch (reason) { error = reason.message; }
  memory.replaceChildren(); const row = el('div', 'memory-caption'); row.append(el('strong', '', bytes ? bytes + ' Б' : '—'), el('span', '', capacity ? 'из ' + capacity + ' Б памяти' : 'объём NDEF · оценка')); memory.append(row);
  if (capacity) { const track = el('div', 'memory-track'), fill = el('div', bytes > capacity ? 'over' : ''); fill.style.width = Math.min(100, bytes / capacity * 100) + '%'; track.append(fill); memory.append(track, el('small', 'muted', 'Размер выбран вручную. Служебные данные тоже занимают память.')); }
  validation.textContent = error; validation.hidden = !error; write.disabled = !availability.supported || !compiled || controller.busy;
}
function operationPanel() {
  const host = el('div', 'operation'); host.dataset.operation = ''; host.setAttribute('aria-live', 'polite'); host.hidden = true;
  host.append(el('strong'), el('p'), button('Остановить', 'close', 'button button-small', () => controller.cancel())); return host;
}
function updateOperation() {
  const phase = operation.phase, busy = ['scanning', 'writing', 'verifying', 'locking'].includes(phase);
  const titles = { scanning: 'Ищем метку…', writing: 'Ждём метку для записи…', verifying: 'Записано. Проверяем содержимое…', verified: 'Записано и проверено', written: 'Записано · проверка не завершена', read: 'Метка прочитана', locking: 'Ждём метку для защиты…', locked: 'Запись на метку закрыта навсегда', cancelled: 'Операция остановлена', error: 'Не удалось закончить' };
  for (const host of document.querySelectorAll('[data-operation]')) {
    host.hidden = phase === 'idle'; host.dataset.phase = phase; host.classList.toggle('busy', busy);
    host.querySelector('strong').textContent = titles[phase] || '';
    host.querySelector('p').textContent = operation.message || operation.hint || (busy ? 'Держите эту метку у NFC-антенны телефона. Не убирайте её до завершения.' : phase === 'verified' ? 'Содержимое прочитано обратно и совпадает с вашим шаблоном.' : '');
    host.querySelector('button').hidden = !busy;
  }
  const write = document.querySelector('[data-write]'); if (write) write.disabled = !availability.supported || !compiled || busy;
  const read = document.querySelector('[data-read]'); if (read) read.disabled = !availability.supported || busy;
}
async function writeDraft() {
  if (!compiled || !availability.supported) return; persist();
  try { await controller.write(compileDraft(draft), { overwrite }); } catch { /* Typed state is shown by the controller. */ }
}
function renderRead() {
  const card = el('section', 'card read-card'), mark = el('div', 'read-mark'); mark.append(icon('read'));
  card.append(mark, el('h2', '', 'Что уже записано на метке?'), el('p', 'muted', 'Нажмите «Прочитать», затем приложите метку к телефону. Ссылки и действия сами не запускаются.'));
  const read = button('Прочитать метку', 'read', 'button button-primary', () => void controller.read().catch(() => {})); read.dataset.read = ''; read.disabled = !availability.supported;
  card.append(read, operationPanel()); content.append(card); const results = el('section', 'read-results'); results.dataset.readResults = ''; content.append(results); renderReadResult(); updateOperation();
}
function snapshotCard(snapshot) {
  const card = el('article', 'read-record'), header = el('div', 'read-record-head');
  const label = ({ text: 'Текст', url: 'Ссылка', 'absolute-url': 'Ссылка', mime: snapshot.mediaType === 'application/vnd.wfa.wsc' ? 'Wi-Fi · конфигурация сети' : snapshot.mediaType?.includes('vcard') ? 'Контакт' : 'Данные', empty: 'Пустая запись', 'smart-poster': 'Ссылка с описанием' })[snapshot.recordType] || 'Свои данные';
  header.append(icon(snapshot.recordType === 'url' ? 'link' : 'chip'), el('strong', '', label)); card.append(header);
  const interpreted = readableRecord(snapshot);
  if (interpreted) {
    const list = el('dl', 'read-fields');
    for (const [name, value] of interpreted.fields) list.append(el('dt', '', name), el('dd', '', value.slice(0, 4096)));
    card.append(list);
    if (interpreted.password) { const password = el('p', 'read-value'); password.hidden = true; const reveal = button('Показать пароль Wi-Fi', 'lock', 'text-button', () => { password.hidden = !password.hidden; password.textContent = password.hidden ? '' : interpreted.password; const label = password.hidden ? 'Показать пароль Wi-Fi' : 'Скрыть пароль Wi-Fi'; reveal.querySelector('span').textContent = label; reveal.setAttribute('aria-label', label); }); card.append(reveal, password); }
    const raw = el('details', 'byte-details'); raw.append(el('summary', '', 'Исходные данные'), el('pre', '', [...fromBase64(snapshot.base64).subarray(0, 512)].map(byte => byte.toString(16).padStart(2, '0')).join(' '))); card.append(raw);
  }
  else if (snapshot.text) card.append(el('p', 'read-value', snapshot.text.slice(0, 4096)));
  else if (snapshot.base64) {
    const bytes = fromBase64(snapshot.base64); card.append(el('p', 'muted small', (snapshot.mediaType || snapshot.recordType) + ' · ' + bytes.length + ' Б'));
    if (snapshot.mediaType?.startsWith('text/') || snapshot.mediaType === 'application/json') card.append(el('pre', 'read-value', new TextDecoder().decode(bytes).slice(0, 4096)));
    else { const details = el('details', 'byte-details'); details.append(el('summary', '', 'Посмотреть байты'), el('pre', '', [...bytes.subarray(0, 512)].map(byte => byte.toString(16).padStart(2, '0')).join(' ') + (bytes.length > 512 ? ' …' : ''))); card.append(details); }
  }
  if (snapshot.records) for (const child of snapshot.records) card.append(snapshotCard(child));
  if (snapshot.recordType === 'url' && /^https?:\/\//i.test(snapshot.text || '')) {
    try { const url = new URL(snapshot.text); if (!url.username && !url.password) { const link = el('a', 'text-button', 'Открыть ссылку'); link.href = url.href; link.target = '_blank'; link.rel = 'noopener noreferrer'; card.append(link); } } catch { /* Never launch malformed input. */ }
  }
  return card;
}
function renderReadResult() {
  const results = document.querySelector('[data-read-results]'); if (!results || !lastRead) return; results.replaceChildren();
  const heading = el('div', 'section-head'); heading.append(el('h2', '', 'Содержимое метки'), el('span', 'count-label', recordCount(lastRead.records.length))); results.append(heading);
  if (lastRead.serialNumber) results.append(el('p', 'muted small', 'Номер из NFC: ' + lastRead.serialNumber));
  if (!lastRead.records.length) results.append(el('p', 'muted', 'NDEF-содержимого нет.'));
  for (const record of lastRead.records) results.append(snapshotCard(record));
  if (lastRead.records.length) results.append(button('Использовать для записи', 'copy', 'button', () => {
    const replace = () => { const value = { schema: draft.schema, name: 'Скопированная метка', records: lastRead.records.map(snapshot => ({ key: crypto.randomUUID(), kind: 'raw', snapshot })) }; try { compileDraft(value); draft = value; selected = 0; view = 'write'; persist(); render(); } catch (error) { toast(error.message); } };
    confirm('Использовать содержимое метки?', 'Текущий черновик будет заменён. Сначала сохраните его как шаблон, если он вам нужен.', 'Использовать', replace);
  }));
}
function saveTemplate() {
  try { compileDraft(draft); } catch (error) { toast(error.message); return; }
  const { dialog, body } = modal('Сохранить шаблон'); const field = el('label', 'field'), input = el('input', 'input'); input.value = draft.name; input.maxLength = 80; input.placeholder = 'Например, Гостевой Wi-Fi'; field.append(el('span', 'field-label', 'Название шаблона'), input);
  const note = el('p', 'muted small', 'Шаблон хранится только в этом браузере. Его можно скачать и перенести на телефон.');
  body.append(field, note, button('Сохранить', 'check', 'button button-primary button-wide', () => {
    if (!input.value.trim()) { input.focus(); return; }
    const next = { ...templateFile(draft), name: input.value.trim(), key: crypto.randomUUID(), updatedAt: Date.now() };
    const values = [next, ...templates].slice(0, 40); if (!saveLocal(storage, storageKeys.templates, values)) { note.textContent = 'Браузер не смог сохранить шаблон. Скачайте его файлом.'; return; }
    templates = values; draft.name = next.name; persist(); dialog.close(); toast('Шаблон «' + next.name + '» сохранён.');
  })); input.focus();
}
function download(draftValue) {
  const blob = new Blob([JSON.stringify(templateFile(draftValue), null, 2)], { type: 'application/json' }), url = URL.createObjectURL(blob);
  const link = el('a'); link.href = url; link.download = 'nfc-template.json'; document.body.append(link); link.click(); link.remove(); setTimeout(() => URL.revokeObjectURL(url), 10_000);
}
function importTemplate() {
  const input = el('input'); input.type = 'file'; input.accept = '.json,application/json';
  input.addEventListener('change', async () => {
    const file = input.files?.[0]; if (!file) return;
    try { if (file.size > 100_000) throw new Error('Файл слишком большой. Выберите один NFC-шаблон до 100 КБ.'); const value = validateDraft(JSON.parse(await file.text()));
      acceptDraft(value); } catch (error) { toast(error.message || 'Не удалось открыть шаблон.'); }
  }); input.click();
}
function acceptDraft(value) {
  const use = () => { draft = value; selected = 0; view = 'write'; persist(); render(); toast('Шаблон открыт. Проверьте содержимое перед записью.'); };
  confirm('Открыть шаблон «' + (value.name || 'Моя метка') + '»?', 'Он заменит текущий черновик. Сохраните прежний как шаблон, если он вам нужен.', 'Открыть шаблон', use);
}
function renderTemplates() {
  const head = el('div', 'templates-head'); const copy = el('div'); copy.append(el('h2', '', 'Всегда под рукой'), el('p', 'muted', 'Ваши сохранённые метки на этом устройстве.'));
  const actions = el('div', 'action-row'); actions.append(button('Открыть файл', 'upload', 'button', importTemplate), button('Скачать черновик', 'download', 'button', () => { try { compileDraft(draft); download(draft); } catch (error) { toast(error.message); } })); head.append(copy, actions); content.append(head);
  if (!templates.length) { const empty = el('section', 'card templates-empty'); empty.append(icon('templates'), el('h3', '', 'Первая метка станет шаблоном'), el('p', 'muted', 'Подготовьте ссылку, контакт или Wi-Fi и нажмите «Сохранить шаблон». В следующий раз останется только приложить метку.'), button('Подготовить метку', 'plus', 'button button-primary', () => { view = 'write'; render(); })); content.append(empty); return; }
  const grid = el('div', 'templates-grid');
  for (const template of templates) {
    const card = el('article', 'card template-card'); const info = previewLabel(template.records[0]); card.append(el('span', 'template-symbol').appendChild(icon(recordTypes.find(type => type.id === template.records[0].kind)?.icon || 'chip')).parentNode, el('h3', '', template.name), el('p', 'template-detail', info.detail), el('small', 'muted', recordCount(template.records.length) + ' · ' + estimateBytes(compileDraft(template)) + ' Б'));
    const controls = el('div', 'template-controls'); controls.append(button('Открыть', 'arrow', 'button button-small', () => acceptDraft(validateDraft(template))),
      button('Скачать', 'download', 'icon-button', () => download(template)), button('Удалить', 'trash', 'icon-button', () => confirm('Удалить шаблон?', 'Шаблон «' + template.name + '» будет удалён с этого устройства. Скачайте его, если хотите сохранить копию.', 'Удалить', () => { const next = templates.filter(value => value.key !== template.key); if (!saveLocal(storage, storageKeys.templates, next)) { toast('Не удалось сохранить изменение. Шаблон сохранён.'); return; } templates = next; render(); }, true))); card.append(controls); grid.append(card);
  } content.append(grid);
}
async function share() {
  const { dialog, body } = modal('Откройте на телефоне'); body.append(el('p', 'muted', 'Наведите камеру Android на QR. Откройте ссылку в Chrome — затем можно записать метку.'));
  const qrHost = el('div', 'qr-host'), canvas = el('canvas'); canvas.setAttribute('aria-label', 'QR-код для открытия на телефоне'); qrHost.append(canvas);
  const checkRow = el('label', 'check-row'), check = el('input'); check.type = 'checkbox';
  try { compileDraft(draft); } catch { check.disabled = true; }
  checkRow.append(check, el('span', '', 'Передать подготовленное содержимое'));
  const privacy = el('p', 'muted small', 'По умолчанию QR содержит только адрес приложения.'), error = el('p', 'validation'); error.setAttribute('role', 'alert');
  const linkText = el('input', 'input share-link'); linkText.readOnly = true; linkText.setAttribute('aria-label', 'Ссылка для телефона');
  let link = new URL('/', location.href).href, revision = 0;
  const update = async () => {
    const request = ++revision; error.textContent = '';
    try { link = check.checked ? draftLink(location.href, draft) : new URL('/', location.href).href; linkText.value = link; privacy.textContent = check.checked ? 'В этой ссылке есть содержимое шаблона, включая пароль Wi-Fi, если вы его указали. Отправляйте её только нужному человеку.' : 'QR содержит только адрес приложения. Ваши данные с меток в него не входят.';
      const qr = await QRCode.toDataURL(link, { width: 240, margin: 2, errorCorrectionLevel: 'M', color: { dark: '#183e30', light: '#ffffff' } }); if (request !== revision || !dialog.isConnected) return;
      const image = el('img'); image.src = qr; image.alt = 'QR-код для открытия NFC-приложения на телефоне'; qrHost.replaceChildren(image);
    } catch (reason) { if (request !== revision || !dialog.isConnected) return; check.checked = false; link = new URL('/', location.href).href; linkText.value = link; privacy.textContent = 'QR содержит только адрес приложения. Шаблон можно скачать файлом.'; const qr = await QRCode.toDataURL(link, { width: 240, margin: 2, color: { dark: '#183e30', light: '#ffffff' } }); if (request === revision && dialog.isConnected) { const image = el('img'); image.src = qr; image.alt = 'QR-код приложения без содержимого шаблона'; qrHost.replaceChildren(image); error.textContent = reason.message; } }
  };
  check.addEventListener('change', () => void update());
  const controls = el('div', 'action-row'); controls.append(button('Скопировать ссылку', 'copy', 'button button-primary', () => void navigator.clipboard.writeText(link).then(() => toast('Ссылка скопирована.')).catch(() => { linkText.focus(); linkText.select(); toast('Выделили ссылку. Скопируйте её вручную.'); })), button('Скачать шаблон', 'download', 'button', () => { try { compileDraft(draft); download(draft); } catch (reason) { error.textContent = reason.message; } }));
  body.append(qrHost, checkRow, privacy, linkText, error, controls); await update();
}
function openSeparately() {
  if (!embedded) return;
  window.parent.postMessage({ schema: 'soty.app-action.v1', action: 'open-separately' }, '*');
  toast('Открываем отдельное окно. Если оно не появилось, в Сотах выберите «Действия с приложением» → «Открыть отдельно».');
}
function lockDialog() {
  const { dialog, body } = modal('Закрыть запись навсегда?'); body.append(el('p', 'danger-copy', 'После этого содержимое метки нельзя будет изменить. Отменить защиту невозможно.'), el('p', 'muted', 'Сначала прочитайте метку и проверьте, что на ней правильные данные.'));
  const field = el('label', 'field'), input = el('input', 'input'); input.placeholder = 'ЗАКРЫТЬ'; input.autocomplete = 'off'; field.append(el('span', 'field-label', 'Введите ЗАКРЫТЬ для подтверждения'), input);
  const lock = button('Запретить запись навсегда', 'lock', 'button button-danger button-wide', () => { dialog.close(); void controller.lock().catch(() => {}); }); lock.disabled = true; input.addEventListener('input', () => { lock.disabled = input.value.trim() !== 'ЗАКРЫТЬ'; }); body.append(field, lock);
}
function help() {
  const { body } = modal('Маленькая метка. Большие возможности.');
  const steps = el('ol', 'help-steps');
  for (const [title, text] of [['Подготовьте содержимое', 'Выберите ссылку, контакт, Wi-Fi или другой тип. Можно добавить несколько записей.'], ['Приложите метку к Android', 'Откройте это приложение отдельным окном в Chrome, включите NFC и нажмите «Записать».'], ['Дождитесь проверки', 'Не убирайте метку: приложение прочитает записанное обратно и сравнит данные.']]) { const item = el('li'); item.append(el('strong', '', title), el('p', 'muted', text)); steps.append(item); }
  body.append(steps, el('h3', '', 'Какие метки подходят'), el('p', 'muted', 'Обычные NDEF-метки с разрешённой записью, например NTAG213, NTAG215 или NTAG216. Пропуска, платёжные карты и защищённые секторы этим приложением не переписываются.'),
    el('h3', '', 'Что будет на iPhone'), el('p', 'muted', 'Поддерживаемый iPhone может открыть HTTPS-ссылку с уже записанной метки. Запись через браузер iPhone недоступна. Текст, контакты и Wi-Fi обрабатываются телефонами по-разному.'),
    el('h3', '', 'Ваши данные'), el('p', 'muted', 'Чтение и запись происходят на телефоне. Шаблоны хранятся в этом браузере; сервер не получает содержимое меток. Скачайте важные шаблоны: очистка данных браузера удалит локальные копии.'));
}
document.addEventListener('visibilitychange', () => { if (document.hidden) controller.cancel('hidden'); });
window.addEventListener('pagehide', () => controller.cancel('hidden'));
render();
if (initialError) toast(initialError);
if (incomingDraft) acceptDraft(incomingDraft);
