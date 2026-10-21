import { el, button, labeledField } from './dom';
import { pinDialogSubmit, errorText, type WorldDialog } from './dialogs';
import type { WorldApi } from './types';
import { SQRT3, roundedHexPath } from '../geometry/hex.mjs';
import { resolveFieldAppArt } from './field-art.mjs';
import type { AppArt } from './app-art.mjs';
import { createCoverApi, type CoverKind } from './cover-api.mjs';
import { prepareCoverFile, coverCropRect } from './cover-local-raster.mjs';
import { createCoverEditorState, createCoverJournal, type CoverEditorSnapshot, type CoverCommitted } from './cover-editor-state.mjs';
import './cover-editor.css';

export interface CoverEditorOptions {
  dialog: WorldDialog; api: WorldApi; kind: CoverKind; targetId: string; accountId: string;
  isCurrent(): boolean; onCommitted(result: CoverCommitted): void;
  targetLabel?: string; inheritedArt?: AppArt;
  storage?: Pick<Storage, 'getItem' | 'setItem'>;
}
const messages: Record<string, string> = {
  cover_file_limit: 'Выберите PNG, JPEG или WebP размером до 8 МБ.', cover_file_type: 'Подойдут PNG, JPEG и WebP.',
  cover_file_dimensions: 'Изображение слишком большое. Допустимо до 16 миллионов пикселей.', cover_file_invalid: 'Картинка не читается. Попробуйте другой файл.',
  cover_animation_unsupported: 'Нужна неподвижная картинка.', cover_input_limit: 'Не удалось уменьшить картинку. Попробуйте другой файл.',
  cover_encode_unavailable: 'Браузер не смог подготовить картинку. Попробуйте другой файл.', cover_encode_invalid: 'Браузер не смог подготовить картинку.',
  cover_decode_busy: 'Предыдущая картинка ещё обрабатывается.', cover_context_changed: 'Аккаунт изменился. Откройте редактор заново.',
  cover_target_changed: 'Приложение или доступ изменились. Обновите состояние и проверьте правки.',
  cover_revision_conflict: 'Картинку уже изменили. Обновите состояние и сделайте новый выбор.',
  cover_observation_changed: 'Картинка изменилась во время чтения. Обновите состояние.',
  cover_store_changed: 'Хранилище изменилось. Прежний запрос не будет отправлен в другое хранилище.',
  cover_journal_unavailable: 'Не удалось обновить сведения о запросе в этой вкладке. Если сохранение уже началось, проверьте результат. Новый запрос не будет отправлен.',
  cover_journal_invalid: 'Сведения о прежнем запросе повреждены. Запрос не отправлен.', cover_journal_quota: 'Есть незавершённые сохранения. Сначала проверьте их результат.',
  cover_intent_changed: 'Состояние сохранения изменилось. Откройте редактор заново.', cover_pending_intent: 'Сначала проверьте или отмените прежнюю подготовку.',
  cover_unsaved_changes: 'Сначала сохраните или отмените правки.', cover_not_ready: 'Выберите картинку или обновите состояние.',
  cover_network_unavailable: 'Нет подтверждения от сервера. Проверьте результат, когда появится связь.',
  cover_response_invalid: 'Сервер не подтвердил ожидаемый результат.', cover_image_invalid: 'Не удалось проверить изображение.',
  cover_target_unavailable: 'Сейчас нет доступа к этому изображению.', cover_version_expired: 'Эта версия уже недоступна.',
  cover_draft_unavailable: 'Подготовка уже недоступна. Проверьте результат и выберите картинку заново.', cover_draft_expired: 'Срок подготовки истёк. Проверьте результат и выберите картинку заново.',
  cover_request_expired: 'Срок запроса истёк. Проверьте актуальную версию перед новым выбором.',
  unsupported_operation: 'Редактор картинок ещё не подключён на этом источнике. Обновите состояние позже.',
  cover_owner_quota: 'В хранилище картинок недостаточно места.', cover_rate_limited: 'Слишком много изменений. Подождите немного.',
};
const phaseText: Record<string, string> = { idle: 'Загрузка…', loading: 'Загрузка…', decoding: 'Подготовка на устройстве…', preparing: 'Проверка изображения…', saving: 'Сохранение…',
  ready: 'Текущая версия', editing: 'Несохранённые правки', preview: 'Предпросмотр версии', prepared: 'Готово к сохранению', unknown: 'Результат пока не подтверждён', conflict: 'Данные изменились', error: 'Не удалось завершить действие' };

/** Host owns the dialog/account generation. This mount owns all listeners,
 * local rasters and pending-intent recovery, and never exposes bytes to agents. */
export function mountCoverEditor(options: CoverEditorOptions): { dispose(): void; requestClose(afterClose?: () => void): void; hasUnsavedChanges(): boolean } {
  const { dialog } = options; let disposed = false, closing = false, confirmation = false, imageEpoch = 0;
  let sourceImage: HTMLImageElement | null = null, sourceUrl = '', sourceReady = false, closeContinuation: (() => void) | null = null;
  const current = (): boolean => !disposed && options.isCurrent();
  const signalOwner = new AbortController(), signal = signalOwner.signal;
  const form = el('form', 'sw-cover-editor');
  const instruction = el('p', 'sw-muted', options.kind === 'app' ? 'Картинка будет видна в соте. Доступ к приложению останется прежним.' : 'Картинка станет вашим аватаром. Настройки видимости профиля останутся прежними.');
  const toolbar = el('div', 'sw-cover-toolbar'), file = el('input'); file.type = 'file'; file.accept = 'image/png,image/jpeg,image/webp'; file.hidden = true;
  const choose = button('Выбрать картинку', 'image'), refresh = button('Обновить', 'refresh'); toolbar.append(choose, refresh, file);
  const stage = el('div', 'sw-cover-stage'), mask = el('div', `sw-cover-mask ${options.kind === 'profile' ? 'is-profile' : 'is-app'}`);
  const canvas = el('canvas'); canvas.width = 320; canvas.height = 320; canvas.setAttribute('aria-hidden', 'true');
  const placeholder = el('span', 'sw-cover-placeholder', 'Без картинки'); mask.setAttribute('role', 'img'); mask.setAttribute('aria-label', 'Предпросмотр'); mask.append(canvas, placeholder);
  const caption = el('div', 'sw-cover-caption', options.targetLabel ?? (options.kind === 'app' ? 'Ваша сота' : 'Ваш профиль')); mask.append(caption);
  const inherited = options.kind === 'app' ? options.inheritedArt ?? resolveFieldAppArt({ appId: options.targetId }) : null;
  const inheritedImage = el('img', 'sw-cover-inherited'); inheritedImage.alt = ''; inheritedImage.hidden = true; inheritedImage.decoding = 'async'; inheritedImage.referrerPolicy = 'no-referrer'; mask.insertBefore(inheritedImage, caption);
  if (inherited?.src && /^\/app-art\/[a-z0-9/_\-.]+\.webp$/u.test(inherited.src)) { inheritedImage.src = inherited.src; inheritedImage.style.objectPosition = `${inherited.focalPoint.x * 100}% ${inherited.focalPoint.y * 100}%`; }
  inheritedImage.addEventListener('error', () => { inheritedImage.hidden = true; inheritedImage.removeAttribute('src'); if (!controller.read().source) placeholder.hidden = false; }, { signal });
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg'); svg.setAttribute('width', '0'); svg.setAttribute('height', '0'); svg.setAttribute('aria-hidden', 'true');
  const defs = document.createElementNS(svg.namespaceURI, 'defs'), clip = document.createElementNS(svg.namespaceURI, 'clipPath'), path = document.createElementNS(svg.namespaceURI, 'path');
  const clipId = `sw-cover-hex-${crypto.randomUUID()}`; clip.setAttribute('id', clipId); clip.setAttribute('clipPathUnits', 'objectBoundingBox');
  path.setAttribute('d', roundedHexPath(1, .12)); path.setAttribute('transform', `matrix(0 .5 ${-1 / SQRT3} 0 .5 .5)`); clip.append(path); defs.append(clip); svg.append(defs);
  mask.style.setProperty('--sw-cover-mask', `url(#${clipId})`);
  const small = el('div', 'sw-cover-small'), smallCanvas = el('canvas'); smallCanvas.width = 96; smallCanvas.height = 96;
  smallCanvas.style.setProperty('--sw-cover-mask', `url(#${clipId})`); smallCanvas.className = options.kind === 'profile' ? 'is-profile' : 'is-app'; smallCanvas.setAttribute('aria-hidden', 'true');
  small.append(smallCanvas, el('span', 'sw-muted', 'На маленьком экране')); stage.append(svg, mask, small);
  const cropFields = el('fieldset', 'sw-cover-crop'), legend = el('legend', '', 'Кадрирование'); cropFields.append(legend);
  function range(label: string, min: number, max: number, step: number): HTMLInputElement {
    const input = el('input'); input.type = 'range'; input.min = String(min); input.max = String(max); input.step = String(step); cropFields.append(labeledField(label, input)); return input;
  }
  const x = range('Горизонталь', 0, 100, 1), y = range('Вертикаль', 0, 100, 1), zoom = range('Масштаб', 1, 4, .05);
  const reset = button('Сбросить кадр'), cropHint = el('p', 'sw-muted', 'Передвигайте картинку или используйте ползунки. Стрелки клавиатуры меняют кадр.'); cropFields.append(reset, cropHint);
  const modes = el('div', 'sw-cover-modes'), inherit = button(options.kind === 'app' ? 'Стандартная обложка' : 'Без фото'), none = button('Без обложки');
  modes.append(inherit); if (options.kind === 'app') modes.append(none);
  const history = el('details', 'sw-cover-history'), summary = el('summary', '', 'История версий'), historyList = el('div', 'sw-cover-history-list'); history.append(summary, historyList);
  const statusLine = el('p', 'sw-cover-status'); statusLine.setAttribute('role', 'status'); statusLine.setAttribute('aria-live', 'polite');
  const errorLine = el('p', 'sw-cover-error'); errorLine.setAttribute('role', 'alert');
  const pendingActions = el('div', 'sw-cover-toolbar'), recheck = button('Проверить результат', 'refresh'), cancelDraft = button('Отменить подготовку'); pendingActions.append(recheck, cancelDraft);
  const generation = el('p', 'sw-muted sw-cover-generation', 'Генерация изображений пока не подключена. Можно выбрать свою картинку.');
  const leave = el('div', 'sw-cover-leave'); leave.hidden = true; leave.setAttribute('role', 'group'); leave.setAttribute('aria-label', 'Закрыть редактор');
  const leaveText = el('p'), stay = button('Продолжить редактирование'), leaveNow = button('Закрыть редактор'); leave.append(leaveText, stay, leaveNow);
  form.append(instruction, toolbar, stage, cropFields, modes, statusLine, errorLine, pendingActions, history, generation, leave); dialog.body.append(form);
  const save = button('Сохранить', 'check', 'sw-button-primary'); pinDialogSubmit(dialog, form, save);
  const footer = save.parentElement!, close = button('Закрыть'); footer.prepend(close);
  const sdk = createCoverApi({ api: options.api, kind: options.kind, targetId: options.targetId, accountId: options.accountId, isCurrent: current });
  const storage = options.storage ?? { getItem: (key: string) => sessionStorage.getItem(key), setItem: (key: string, value: string) => sessionStorage.setItem(key, value) };
  const controller = createCoverEditorState({ sdk, isCurrent: current, prepareFile: (selected, valid) => prepareCoverFile(selected, valid, options.kind), journal: createCoverJournal(storage), onChange: render,
    onCommitted: result => { if (current()) options.onCommitted(result); } });
  let historySignature = '';
  function draw(state: CoverEditorSnapshot): void {
    const context = canvas.getContext('2d'), miniature = smallCanvas.getContext('2d'); if (!context || !miniature) return;
    context.clearRect(0, 0, 320, 320); miniature.clearRect(0, 0, 96, 96);
    const shown = sourceReady && sourceImage && state.source && sourceUrl === state.source.url;
    const inheritedShown = !state.source && state.current?.mode === 'inherit' && !!inheritedImage.getAttribute('src'); inheritedImage.hidden = !inheritedShown;
    placeholder.hidden = !!shown || inheritedShown; canvas.hidden = !shown; smallCanvas.hidden = !shown;
    if (!shown || !sourceImage) return;
    const crop = coverCropRect(sourceImage.naturalWidth, sourceImage.naturalHeight, state.crop);
    context.drawImage(sourceImage, crop.left, crop.top, crop.size, crop.size, 0, 0, 320, 320); miniature.drawImage(canvas, 0, 0, 96, 96);
  }
  function setSource(state: CoverEditorSnapshot): void {
    if ((state.source?.url ?? '') === sourceUrl) { draw(state); return; }
    const ticket = ++imageEpoch; sourceUrl = state.source?.url ?? ''; sourceReady = false;
    if (sourceImage) { sourceImage.onload = null; sourceImage.onerror = null; sourceImage.removeAttribute('src'); } sourceImage = null;
    draw(state); if (!sourceUrl) return;
    const image = new Image(); sourceImage = image;
    image.onload = () => { if (!current() || ticket !== imageEpoch || sourceImage !== image) return; sourceReady = true; draw(controller.read()); };
    image.onerror = () => { if (current() && ticket === imageEpoch) { sourceReady = false; draw(controller.read()); } }; image.src = sourceUrl;
  }
  function render(state: CoverEditorSnapshot): void {
    if (!current()) return; form.setAttribute('aria-busy', String(state.busy)); setSource(state);
    x.value = String(state.crop.x * 100); y.value = String(state.crop.y * 100); zoom.value = String(state.crop.zoom);
    x.setAttribute('aria-valuetext', `${Math.round(state.crop.x * 100)}%`); y.setAttribute('aria-valuetext', `${Math.round(state.crop.y * 100)}%`); zoom.setAttribute('aria-valuetext', `${state.crop.zoom.toFixed(2)}×`);
    cropFields.disabled = state.busy || !!state.pending || state.phase === 'conflict' || !state.source;
    choose.disabled = state.busy || !!state.pending || state.phase === 'conflict' || !state.current; refresh.disabled = state.busy || !!state.pending;
    refresh.querySelector('span')!.textContent = state.dirty ? 'Отменить правки' : 'Обновить';
    inherit.disabled = none.disabled = state.busy || !!state.pending || state.dirty || !state.current;
    save.disabled = !state.canSave; pendingActions.hidden = !state.pending; recheck.disabled = state.busy;
    cancelDraft.hidden = !state.pending || !['prepare', 'prepareFromVersion'].includes(state.pending.method); cancelDraft.disabled = state.busy;
    statusLine.textContent = state.notice || phaseText[state.phase] || ''; errorLine.hidden = !state.error;
    errorLine.textContent = state.error ? messages[state.error] ?? errorText({ code: state.error }) : '';
    mask.setAttribute('aria-label', state.source ? `Предпросмотр. Масштаб ${state.crop.zoom.toFixed(2)}` : state.current?.mode === 'inherit' ? 'Стандартный вид без личной картинки' : 'Без картинки');
    const signature = JSON.stringify(state.versions); if (signature !== historySignature) {
      historySignature = signature; historyList.replaceChildren();
      if (!state.versions.length) historyList.append(el('p', 'sw-muted', 'Пока нет сохранённых версий.'));
      for (const version of state.versions) {
        const row = el('div', 'sw-cover-version'), label = el('div'); label.append(el('strong', '', `Версия ${version.version}`), el('small', 'sw-muted', `${new Date(version.createdAt).toLocaleString('ru-RU')} · ${version.mode === 'custom' ? 'Картинка' : version.mode === 'none' ? 'Без картинки' : 'Стандартная'}`));
        const preview = button('Посмотреть'), restore = button('Восстановить'); preview.dataset.restorable = restore.dataset.restorable = String(version.restorable);
        preview.addEventListener('click', () => { void controller.previewVersion(version.id); }, { signal }); restore.addEventListener('click', () => { void controller.restore(version.id); }, { signal });
        row.append(label, preview, restore); historyList.append(row);
      }
    }
    for (const action of historyList.querySelectorAll<HTMLButtonElement>('button')) action.disabled = state.busy || state.dirty || !!state.pending || action.dataset.restorable !== 'true';
  }
  function dispose(): void {
    if (disposed) return; disposed = true; closeContinuation = null; signalOwner.abort(); controller.dispose(); imageEpoch++;
    if (sourceImage) { sourceImage.onload = null; sourceImage.onerror = null; sourceImage.removeAttribute('src'); } sourceImage = null;
    canvas.width = smallCanvas.width = 1; form.remove(); footer.remove(); dialog.element.classList.remove('sw-cover-dialog');
  }
  function finishClose(): void {
    if (closing) return; closing = true; const continuation = closeContinuation, wasCurrent = options.isCurrent();
    endDrag(); dispose(); dialog.close({ restoreFocus: wasCurrent && !continuation }); if (wasCurrent) continuation?.();
  }
  function requestClose(afterClose?: () => void): void {
    if (afterClose) closeContinuation = afterClose;
    if (disposed) return; if (!options.isCurrent() || !controller.read().hasUnsavedChanges) { finishClose(); return; }
    confirmation = true; leave.hidden = false;
    leaveText.textContent = controller.read().pending ? 'Сохранение может завершиться на сервере. При следующем открытии проверим тот же запрос. Закрыть окно?' : 'Закрыть редактор и отменить несохранённые правки?';
    stay.focus(); leave.scrollIntoView({ block: 'nearest', behavior: 'instant' });
  }
  choose.addEventListener('click', () => { if (current()) { file.value = ''; file.click(); } }, { signal });
  file.addEventListener('change', () => { const selected = file.files?.[0]; file.value = ''; if (selected && current()) void controller.choose(selected); }, { signal });
  form.addEventListener('submit', event => { event.preventDefault(); if (current()) void controller.save(); }, { signal });
  refresh.addEventListener('click', () => { void controller.discard(); }, { signal }); reset.addEventListener('click', () => controller.setCrop({ x: .5, y: .5, zoom: 1 }), { signal });
  for (const input of [x, y, zoom]) input.addEventListener('input', () => controller.setCrop({ x: Number(x.value) / 100, y: Number(y.value) / 100, zoom: Number(zoom.value) }), { signal });
  inherit.addEventListener('click', () => { void controller.changeMode('inherit'); }, { signal }); none.addEventListener('click', () => { void controller.changeMode('none'); }, { signal });
  recheck.addEventListener('click', () => { void controller.recheck(); }, { signal }); cancelDraft.addEventListener('click', () => { void controller.cancelPreparation(); }, { signal });
  close.addEventListener('click', () => requestClose(), { signal }); stay.addEventListener('click', () => { confirmation = false; closeContinuation = null; leave.hidden = true; save.focus(); }, { signal }); leaveNow.addEventListener('click', finishClose, { signal });
  dialog.element.addEventListener('click', event => {
    if (!(event.target instanceof Element) || !event.target.closest('.sw-dialog-header button')) return;
    event.preventDefault(); event.stopImmediatePropagation(); requestClose();
  }, { capture: true, signal });
  dialog.element.addEventListener('cancel', event => { event.preventDefault(); requestClose(); }, { signal });
  dialog.element.addEventListener('close', dispose, { once: true, signal });
  window.addEventListener('beforeunload', event => { if (!current() || !controller.read().hasUnsavedChanges) return; event.preventDefault(); event.returnValue = ''; }, { signal });
  let drag: { id: number; x: number; y: number; crop: CoverEditorSnapshot['crop']; width: number } | null = null;
  mask.addEventListener('pointerdown', event => {
    const state = controller.read(); if (!current() || state.busy || state.pending || !state.source || event.button !== 0) return;
    drag = { id: event.pointerId, x: event.clientX, y: event.clientY, crop: state.crop, width: Math.max(1, mask.getBoundingClientRect().width) }; mask.setPointerCapture(event.pointerId); event.preventDefault();
  }, { signal });
  mask.addEventListener('pointermove', event => {
    if (!drag || event.pointerId !== drag.id) return;
    controller.setCrop({ x: Math.max(0, Math.min(1, drag.crop.x - (event.clientX - drag.x) / drag.width / drag.crop.zoom)),
      y: Math.max(0, Math.min(1, drag.crop.y - (event.clientY - drag.y) / drag.width / drag.crop.zoom)), zoom: drag.crop.zoom });
  }, { signal });
  const endDrag = (): void => { if (drag && mask.hasPointerCapture(drag.id)) mask.releasePointerCapture(drag.id); drag = null; };
  mask.addEventListener('pointerup', endDrag, { signal }); mask.addEventListener('pointercancel', endDrag, { signal }); mask.addEventListener('lostpointercapture', () => { drag = null; }, { signal });
  dialog.element.classList.add('sw-cover-dialog'); render(controller.read()); void controller.load();
  return { dispose() { endDrag(); dispose(); }, requestClose, hasUnsavedChanges: () => !disposed && (confirmation || controller.read().hasUnsavedChanges) };
}
