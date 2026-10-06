import './app-feedback.css';
import { button, el, labeledField } from './dom';
import type { WorldApi } from './types';
import { attachmentBytes, validateAttachmentBudget, rasterFile, captureSelectedDisplay, createFeedbackRecorder, editRaster,
  type FeedbackAttachment, type RasterRect } from './feedback-media.mjs';

interface FeedbackContext {
  installationId: string; appId: string; title: string; recipientLabel: string;
  ticketVisibility: 'reporter-and-support'; canSubmit: boolean; canManage: boolean;
  capabilities: { text: boolean; voice: boolean; screenshot: boolean; asr: boolean };
  limits: { bodyChars: number; totalAttachmentBytes: number; maxAttachments: number; maxAudioSeconds: number };
  readiness?: 'ready' | 'pending' | 'unavailable';
}
interface FeedbackMessage { id?: string; body: string; createdAt: string | number; kind?: string; label?: string; }
interface TicketAttachment { id?: string; kind: 'image' | 'audio'; name: string; mimeType: string; dataBase64?: string; byteLength?: number; }
interface FeedbackTicket {
  id: string; appId: string; installationId: string; body: string; status: string; revision: number;
  createdAt: string | number; updatedAt?: string | number; attachments?: TicketAttachment[];
  messages?: FeedbackMessage[]; canReply?: boolean; canManage?: boolean; canAccept?: boolean;
}
interface FeedbackReceipt { ticketId: string; createdAt?: string | number; revision?: number; }
interface PendingAction {
  kind: 'status' | 'accept'; ticketId: string; installationId: string; requestId: string; expectedRevision: number;
  status?: 'in_progress' | 'needs_action' | 'ready_to_check';
}
interface Draft {
  draftId: string;
  accountId: string; appId: string; installationId: string | null; body: string; attachments: FeedbackAttachment[];
  pending: { requestId: string; installationId: string } | null;
  replies: Record<string, string>; pendingReply: { ticketId: string; requestId: string; body: string; expectedRevision: number; installationId: string } | null;
  pendingAction: PendingAction | null;
  updatedAt: number;
}
export interface AppFeedbackOptions {
  api: WorldApi; accountId: string; appId: string; title: string; isCurrent(): boolean;
}
export interface AppFeedbackHandle { open(): void; flush(): Promise<void>; hasUnsavedChanges(): boolean; dispose(): void; }

const DEFAULT_LIMITS = { bodyChars: 8000, totalAttachmentBytes: 1048576, maxAttachments: 3, maxAudioSeconds: 120 };
const statuses: Record<string, string> = { received: 'Принято', in_progress: 'В работе', needs_action: 'Нужно ваше действие', needs_answer: 'Нужно ваше действие', ready_to_check: 'Готово для проверки', resolved: 'Решено' };
const mediaRefusals = new Set(['feedback_attachment_invalid', 'feedback_attachment_limit', 'feedback_audio_duration_limit', 'feedback_audio_duration_unknown',
  'feedback_image_dimensions_limit', 'feedback_media_invalid', 'feedback_media_unsupported', 'feedback_invalid_body']);
const errorCode = (error: unknown) => error && typeof error === 'object' && 'code' in error ? String(error.code) : '';
function failure(error: unknown): string {
  const code = error && typeof error === 'object' && 'code' in error ? String(error.code) : error instanceof Error ? error.message : '';
  if (['feedback_attachment_bytes', 'feedback_attachment_limit'].includes(code)) return 'Вложения вместе должны занимать не больше 1 МиБ, максимум три файла. Удалите вложение или сократите запись.';
  if (code === 'feedback_attachment_count') return 'Можно приложить не больше трёх файлов.';
  if (['feedback_image_format', 'feedback_image_dimensions', 'feedback_image_dimensions_limit'].includes(code)) return 'Выберите PNG, JPEG или WebP до 8 МиБ и 16 миллионов пикселей.';
  if (['feedback_audio_duration_limit', 'feedback_audio_duration_unknown'].includes(code)) return 'Сервер не подтвердил допустимую длительность аудио. Удалите запись и запишите короткую заново; текст сохранён.';
  if (['feedback_attachment_invalid', 'feedback_media_invalid', 'feedback_media_unsupported'].includes(code)) return 'Сервер не принял формат вложения. Удалите его или выберите другой снимок; текст сохранён.';
  if (code === 'feedback_invalid_body') return 'Проверьте содержание сообщения: оно не должно быть пустым или длиннее 8000 символов.';
  if (code === 'feedback_capture_unavailable') return 'Снимок экрана здесь недоступен. Выберите файл изображения.';
  if (error instanceof DOMException && ['NotAllowedError', 'NotFoundError'].includes(error.name)) return 'Доступ не предоставлен. Текст и выбранный файл остаются доступны.';
  if (['feedback_record_unavailable', 'feedback_record_failed', 'feedback_record_empty'].includes(code)) return 'Не удалось сохранить запись. Можно отправить текст или выбранный снимок.';
  if (['authentication_required', 'ACTIVE_PROFILE_CHANGED', 'apps_access_denied', 'feedback_access_denied', 'feedback_authentication_required', 'feedback_support_required', 'feedback_ticket_unavailable', 'app_unavailable'].includes(code)) return 'Доступ изменился. Откройте обращение с нужным аккаунтом.';
  if (['feedback_installation_mismatch', 'feedback_scope_mismatch'].includes(code)) return 'Очередь изменилась. Прежнее сообщение не переносится новому получателю автоматически.';
  if (code.endsWith('_capacity') || code === 'feedback_busy') return 'Очередь сейчас занята. Черновик и прежний запрос сохранены; проверьте отправку позже.';
  if (/conflict|revision/.test(code)) return 'Обращение изменилось. Обновите историю; ваш ответ сохранён.';
  return 'Не получили подтверждение. Проверьте соединение и повторите проверку.';
}
const abortError = (error: unknown) => error instanceof DOMException && error.name === 'AbortError';
const mediaUrl = (item: FeedbackAttachment) => `data:${item.mimeType};base64,${item.dataBase64}`;
const formatDate = (value: string | number) => { const date = new Date(value); return Number.isNaN(date.getTime()) ? '' : date.toLocaleString('ru-RU'); };

/** One account/app scope for the entire lifetime. Changing global navigation never moves its draft. */
export function mountAppFeedback(host: HTMLElement, options: AppFeedbackOptions): AppFeedbackHandle {
  let disposed = false, context: FeedbackContext | null = null, readiness = 'pending', saving = true, saved = true, busy = false, localConflict = false;
  let session = new AbortController(), generation = 0, recording: ReturnType<typeof createFeedbackRecorder> | null = null;
  const current = () => !disposed && options.isCurrent();
  const key = `soty.feedback.v1:${encodeURIComponent(options.accountId)}:${encodeURIComponent(options.appId)}`;
  const lockKey = `${key}:submit`;
  let draft: Draft = { draftId: crypto.randomUUID(), accountId: options.accountId, appId: options.appId, installationId: null, body: '', attachments: [], pending: null, replies: {}, pendingReply: null, pendingAction: null, updatedAt: Date.now() };
  let draftWarning = '';
  try {
    const raw = localStorage.getItem(key);
    if (raw && raw.length <= 3000000) {
      const value = JSON.parse(raw) as Draft;
      if (typeof value.draftId === 'string' && /^[a-zA-Z0-9:_-]{8,120}$/.test(value.draftId) && value.accountId === options.accountId && value.appId === options.appId && typeof value.body === 'string' && value.body.length <= 8000 &&
          typeof value.updatedAt === 'number' && Date.now() - value.updatedAt < 24 * 60 * 60 * 1000 && Array.isArray(value.attachments)) {
        validateAttachmentBudget(value.attachments);
        if (value.pending && (!/^[a-zA-Z0-9:_-]{8,120}$/.test(value.pending.requestId) || typeof value.pending.installationId !== 'string')) throw new Error('bad_pending');
        if (value.pendingReply && (typeof value.pendingReply.body !== 'string' || value.pendingReply.body.length > 8000 || !Number.isSafeInteger(value.pendingReply.expectedRevision))) throw new Error('bad_reply');
        if (value.pendingAction && (!['status', 'accept'].includes(value.pendingAction.kind) || !Number.isSafeInteger(value.pendingAction.expectedRevision) ||
            typeof value.pendingAction.requestId !== 'string' || typeof value.pendingAction.ticketId !== 'string' || typeof value.pendingAction.installationId !== 'string' ||
            value.pendingAction.kind === 'status' && !['in_progress', 'needs_action', 'ready_to_check'].includes(value.pendingAction.status ?? ''))) throw new Error('bad_action');
        const replies = Object.fromEntries(Object.entries(value.replies ?? {}).slice(0, 20).filter(([id, body]) => typeof id === 'string' && id.length <= 120 && typeof body === 'string' && body.length <= 8000));
        draft = { draftId: value.draftId, accountId: options.accountId, appId: options.appId, installationId: typeof value.installationId === 'string' ? value.installationId : null,
          body: value.body, attachments: value.attachments, pending: value.pending ? { requestId: value.pending.requestId, installationId: value.pending.installationId } : null,
          pendingReply: value.pendingReply ? { ticketId: value.pendingReply.ticketId, requestId: value.pendingReply.requestId, body: value.pendingReply.body, expectedRevision: value.pendingReply.expectedRevision, installationId: value.pendingReply.installationId } : null,
          pendingAction: value.pendingAction ? { kind: value.pendingAction.kind, ticketId: value.pendingAction.ticketId, installationId: value.pendingAction.installationId,
            requestId: value.pendingAction.requestId, expectedRevision: value.pendingAction.expectedRevision, ...(value.pendingAction.kind === 'status' ? { status: value.pendingAction.status } : {}) } : null,
          replies, updatedAt: value.updatedAt };
      } else draftWarning = 'Прежний локальный черновик устарел. Новое сообщение не отправлено.';
    }
  } catch { draftWarning = 'Не удалось восстановить локальный черновик. Новое сообщение не отправлено.'; }

  const dialog = el('dialog', 'sf-dialog'); dialog.setAttribute('aria-labelledby', `feedback-heading-${options.appId}`);
  const header = el('header', 'sf-header'), title = el('h2', '', 'Сообщить проблему'); title.id = `feedback-heading-${options.appId}`;
  const close = button('Закрыть', 'close', 'sw-button-quiet', () => closeDialog()); header.append(title, close);
  const subtitle = el('p', 'sf-subtitle', options.title), audience = el('p', 'sf-audience', 'Проверяем получателя…');
  const notice = el('p', 'sf-notice'); notice.setAttribute('role', 'status'); notice.setAttribute('aria-live', 'polite'); notice.hidden = true;
  const keepAsNew = button('Оставить текст как новый черновик', undefined, '', () => {
    if (!current() || busy || draft.pending) return; localConflict = false; draft.draftId = crypto.randomUUID(); keepAsNew.hidden = true; persist(); say('Новый черновик сохранён. Перед отправкой проверьте историю.');
  }); keepAsNew.hidden = true;
  const navigation = el('div', 'sf-navigation');
  const compose = el('section', 'sf-compose'), history = el('section', 'sf-history'); history.hidden = true;
  const composeTab = button('Написать', undefined, '', () => { compose.hidden = false; history.hidden = true; composeTab.setAttribute('aria-pressed', 'true'); historyTab.setAttribute('aria-pressed', 'false'); text.focus(); });
  const historyTab = button('История', 'history', '', () => { compose.hidden = true; history.hidden = false; composeTab.setAttribute('aria-pressed', 'false'); historyTab.setAttribute('aria-pressed', 'true'); void loadHistory(); });
  composeTab.setAttribute('aria-pressed', 'true'); historyTab.setAttribute('aria-pressed', 'false'); navigation.append(composeTab, historyTab);
  const form = el('form', 'sf-form'), text = el('textarea', 'sw-input'); text.rows = 5; text.maxLength = 8000; text.value = draft.body;
  text.placeholder = 'Что произошло? Можно написать, записать голос или приложить снимок.'; text.setAttribute('aria-label', 'Содержание обращения');
  const media = el('div', 'sf-media-actions'), previews = el('div', 'sf-previews');
  const recordButton = button('Записать голос');
  const captureButton = button('Выбрать экран', 'image');
  recordButton.addEventListener('click', event => { if (event.isTrusted) void record(); });
  captureButton.addEventListener('click', event => { if (event.isTrusted) void capture(); });
  const file = el('input'); file.type = 'file'; file.accept = 'image/png,image/jpeg,image/webp'; file.hidden = true;
  const fileButton = button('Выбрать снимок', 'image', '', () => file.click());
  media.append(recordButton, captureButton, fileButton, file);
  const noTranscript = el('p', 'sw-muted sf-hint', 'Голос отправляется как аудио. Автоматическая расшифровка пока недоступна.');
  const storage = el('input'); storage.type = 'checkbox'; storage.checked = true;
  const storageLabel = el('label', 'sf-storage'); storageLabel.append(storage, el('span', '', 'Сохранять черновик в этом браузере на сутки'));
  const submit = button('Отправить', undefined, 'sw-button-primary'); submit.type = 'submit';
  form.append(labeledField('Сообщение', text), media, noTranscript, previews, storageLabel, submit); compose.append(form);
  const historyHeading = el('h3', '', 'История обращений'), refresh = button('Обновить', 'refresh', '', () => void loadHistory());
  const historyList = el('div', 'sf-history-list'), detail = el('section', 'sf-ticket'); history.append(historyHeading, refresh, historyList, detail);
  dialog.append(header, subtitle, audience, notice, keepAsNew, navigation, compose, history); host.append(dialog);

  function say(value: string): void { if (!current()) return; notice.textContent = value; notice.hidden = !value; }
  function persist(): boolean {
    if (!current()) return false;
    if (localConflict) { saved = false; return false; }
    draft.updatedAt = Date.now();
    try { if (saving) localStorage.setItem(key, JSON.stringify(draft)); else localStorage.removeItem(key); saved = saving; return saved; }
    catch { saved = false; say('Браузер не сохранил черновик. Оставьте окно открытым или скопируйте текст.'); return false; }
  }
  const contents = () => !!(draft.body || draft.attachments.length || draft.pending || draft.pendingReply || draft.pendingAction || Object.values(draft.replies).some(Boolean));
  function controls(): void {
    const pending = !!draft.pending, available = !!context?.canSubmit && readiness === 'ready' && (!draft.installationId || draft.installationId === context.installationId);
    text.disabled = busy || pending; fileButton.disabled = captureButton.disabled = busy || pending || !available || context?.capabilities.screenshot === false;
    recordButton.disabled = busy || pending || !available || context?.capabilities.voice === false;
    submit.disabled = busy || !available;
    submit.querySelector('span')!.textContent = pending ? 'Проверить отправку' : 'Отправить';
    if (recording) { recordButton.disabled = false; submit.disabled = captureButton.disabled = fileButton.disabled = true; }
  }
  function closeDialog(): void {
    if (!current()) return;
    persist();
    if (contents() && !saved) { say('Черновик не сохранён. Скопируйте текст или включите сохранение перед закрытием.'); return; }
    session.abort(); recording?.cancel(); recording = null; dialog.close(); controls();
  }
  dialog.addEventListener('cancel', event => { event.preventDefault(); closeDialog(); });
  dialog.addEventListener('close', () => { session.abort(); recording?.cancel(); recording = null; });
  text.addEventListener('input', () => { draft.body = text.value; persist(); });
  storage.addEventListener('change', () => { saving = storage.checked; persist(); say(saving ? 'Черновик сохраняется в этом браузере. Он не зашифрован.' : 'Черновик хранится только в открытом окне.'); });

  async function loadContext(): Promise<void> {
    const stamp = ++generation; readiness = 'pending'; controls();
    try {
      const result = await options.api.request<{ context: FeedbackContext; readiness?: string }>('apps.feedback.context', { appId: options.appId });
      if (!current() || stamp !== generation) return;
      const value = result.context;
      if (!value || value.appId !== options.appId || typeof value.installationId !== 'string' || typeof value.recipientLabel !== 'string' || value.ticketVisibility !== 'reporter-and-support') throw new Error('invalid_feedback_context');
      context = { ...value, limits: Object.fromEntries(Object.entries(DEFAULT_LIMITS).map(([name, maximum]) => [name, Math.min(maximum, Number(value.limits?.[name as keyof typeof DEFAULT_LIMITS]) || maximum)])) as typeof DEFAULT_LIMITS }; readiness = result.readiness ?? value.readiness ?? 'ready';
      subtitle.textContent = context.title;
      if (!draft.installationId) { draft.installationId = context.installationId; persist(); }
      audience.textContent = `Получатель: ${context.recipientLabel}. Он получит текст и выбранные вложения. Обращение видно вам и допущенной поддержке; оно не публикуется как отзыв.`;
      historyHeading.textContent = context.canManage ? 'Обращения приложения' : 'Ваши обращения';
      if (draft.installationId !== context.installationId) say('Очередь изменилась. Прежний черновик не перенесён новому получателю.');
      else if (readiness !== 'ready' || !context.canSubmit) say('Очередь пока недоступна. Черновик можно сохранить и проверить позже.');
      else if (draft.pending) say('Отправка ожидает проверки. Повтор использует прежний запрос.');
      else if (draftWarning) say(draftWarning);
    } catch (error) { if (current() && stamp === generation) { readiness = 'unavailable'; audience.textContent = 'Получатель пока не подтверждён. Ничего не отправлено.'; say(failure(error)); } }
    finally { if (current() && stamp === generation) controls(); }
  }
  const usedBytes = () => validateAttachmentBudget(draft.attachments, context?.limits ?? DEFAULT_LIMITS);
  const remaining = () => (context?.limits.totalAttachmentBytes ?? DEFAULT_LIMITS.totalAttachmentBytes) - usedBytes();
  function addAttachment(item: FeedbackAttachment): void {
    if (!current()) return;
    validateAttachmentBudget([...draft.attachments, item], context?.limits ?? DEFAULT_LIMITS);
    draft.attachments.push(item); persist(); renderPreviews(); say('Проверьте вложение перед отправкой.');
  }
  async function capture(): Promise<void> {
    if (!current() || busy || draft.pending) return;
    busy = true; controls(); const signal = session.signal;
    try { const value = await captureSelectedDisplay(remaining(), signal); if (current() && !signal.aborted) addAttachment(value); }
    catch (error) { if (!abortError(error) && !signal.aborted) say(failure(error)); }
    finally { if (current()) { busy = false; controls(); } }
  }
  file.addEventListener('change', () => {
    const selected = file.files?.[0]; file.value = ''; if (!selected || !current() || busy || draft.pending) return;
    busy = true; controls(); const signal = session.signal;
    void rasterFile(selected, remaining(), signal).then(value => { if (current() && !signal.aborted) addAttachment(value); })
      .catch(error => { if (!abortError(error) && !signal.aborted) say(failure(error)); }).finally(() => { if (current()) { busy = false; controls(); } });
  });
  async function record(): Promise<void> {
    if (recording) { if (recording.active()) recording.stop(); else recording.cancel(); return; }
    if (!current() || busy || draft.pending) return;
    const signal = session.signal;
    const instance = createFeedbackRecorder({ maxBytes: remaining(), maxSeconds: context?.limits.maxAudioSeconds ?? 120, signal,
      onTick: seconds => { if (current() && !signal.aborted) recordButton.querySelector('span')!.textContent = `Остановить · ${seconds} с`; } });
    recording = instance; recordButton.querySelector('span')!.textContent = 'Отменить запрос микрофона'; controls();
    try { const result = await instance.start(); if (current() && !signal.aborted) addAttachment(result); }
    catch (error) { if (!abortError(error) && !signal.aborted) say(failure(error)); }
    finally { if (recording === instance) recording = null; if (current()) { recordButton.querySelector('span')!.textContent = 'Записать голос'; controls(); } }
  }

  function renderPreviews(): void {
    previews.replaceChildren();
    draft.attachments.forEach((item, index) => {
      const row = el('article', 'sf-preview');
      if (item.kind === 'image') {
        const image = el('img'); image.src = mediaUrl(item); image.alt = 'Предпросмотр выбранного снимка'; image.tabIndex = 0;
        image.setAttribute('aria-label', 'Предпросмотр снимка. Стрелки выделяют и перемещают участок; Shift со стрелками меняет его размер.'); row.append(image);
        const selection = el('div', 'sf-selection'); row.append(selection); selection.hidden = true;
        let from: { x: number; y: number } | null = null, rect: RasterRect | null = null;
        image.addEventListener('pointerdown', event => { if (draft.pending || busy) return; image.focus(); image.setPointerCapture(event.pointerId); const bounds = image.getBoundingClientRect(); from = { x: event.clientX - bounds.left, y: event.clientY - bounds.top }; event.preventDefault(); });
        image.addEventListener('pointermove', event => {
          if (!from) return; const bounds = image.getBoundingClientRect(), x = Math.max(0, Math.min(bounds.width, event.clientX - bounds.left)), y = Math.max(0, Math.min(bounds.height, event.clientY - bounds.top));
          const left = Math.min(from.x, x), top = Math.min(from.y, y), width = Math.abs(from.x - x), height = Math.abs(from.y - y);
          selection.hidden = false; Object.assign(selection.style, { left: `${image.offsetLeft + left}px`, top: `${image.offsetTop + top}px`, width: `${width}px`, height: `${height}px` });
          rect = { x: left / bounds.width * image.naturalWidth, y: top / bounds.height * image.naturalHeight, width: width / bounds.width * image.naturalWidth, height: height / bounds.height * image.naturalHeight };
        });
        image.addEventListener('pointerup', () => { from = null; }); image.addEventListener('pointercancel', () => { from = null; rect = null; selection.hidden = true; });
        image.addEventListener('keydown', event => {
          if (!['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown'].includes(event.key) || draft.pending || busy || !image.naturalWidth) return;
          event.preventDefault();
          rect ??= { x: 0, y: 0, width: Math.max(2, Math.round(image.naturalWidth / 3)), height: Math.max(2, Math.round(image.naturalHeight / 3)) };
          const dx = event.key === 'ArrowLeft' ? -10 : event.key === 'ArrowRight' ? 10 : 0, dy = event.key === 'ArrowUp' ? -10 : event.key === 'ArrowDown' ? 10 : 0;
          if (event.shiftKey) { rect.width = Math.max(2, Math.min(image.naturalWidth - rect.x, rect.width + dx)); rect.height = Math.max(2, Math.min(image.naturalHeight - rect.y, rect.height + dy)); }
          else { rect.x = Math.max(0, Math.min(image.naturalWidth - rect.width, rect.x + dx)); rect.y = Math.max(0, Math.min(image.naturalHeight - rect.height, rect.y + dy)); }
          const bounds = image.getBoundingClientRect(); selection.hidden = false; Object.assign(selection.style, { left: `${image.offsetLeft + rect.x / image.naturalWidth * bounds.width}px`, top: `${image.offsetTop + rect.y / image.naturalHeight * bounds.height}px`, width: `${rect.width / image.naturalWidth * bounds.width}px`, height: `${rect.height / image.naturalHeight * bounds.height}px` });
        });
        const actions = el('div', 'sf-row');
        const edit = (operation: 'crop' | 'redact') => {
          if (!rect || rect.width < 2 || rect.height < 2) { say('Выделите участок на снимке. Можно также подготовить файл заранее.'); return; }
          if (busy || draft.pending) return; busy = true; controls(); const signal = session.signal;
          void editRaster(item, rect, operation, remaining() + attachmentBytes(item), signal).then(value => {
            if (!current() || signal.aborted) return; const ownsFocus = row.contains(document.activeElement);
            draft.attachments[index] = value; persist(); renderPreviews();
            if (ownsFocus) previews.children[index]?.querySelector<HTMLImageElement>('img')?.focus();
            say(operation === 'redact' ? 'Сохранён новый снимок. Скрытая область удалена из отправляемых пикселей.' : 'Сохранён обрезанный снимок. Остальные пиксели не отправляются.');
          }).catch(error => { if (!abortError(error) && !signal.aborted) say(failure(error)); }).finally(() => { if (current()) { busy = false; controls(); } });
        };
        const crop = button('Обрезать выделение', undefined, '', () => edit('crop')), redact = button('Скрыть выделение', undefined, '', () => edit('redact'));
        crop.disabled = redact.disabled = !!draft.pending; actions.append(crop, redact); row.append(el('small', 'sw-muted', 'Выделите участок, чтобы обрезать или скрыть его. Изменения сохраняются в самом изображении.'), actions);
      } else { const audio = el('audio'); audio.controls = true; audio.preload = 'metadata'; audio.src = mediaUrl(item); row.append(audio); }
      const remove = button('Убрать вложение', 'close', 'sw-button-quiet', () => { if (busy || draft.pending) return; draft.attachments.splice(index, 1); persist(); renderPreviews(); }); remove.disabled = !!draft.pending;
      row.append(el('span', 'sf-file-size', `${Math.ceil(attachmentBytes(item) / 1024)} КиБ`), remove); previews.append(row);
    });
  }

  async function submission(): Promise<void> {
    if (!current() || busy || recording || !context?.canSubmit || readiness !== 'ready') return;
    const locks = navigator.locks;
    if (!locks) { say('Безопасная отправка между вкладками недоступна. Откройте Соты в современном браузере; черновик сохранён.'); return; }
    busy = true; controls();
    try {
      await locks.request(lockKey, async () => {
        if (!current() || !context) return;
        const latest = saving ? localStorage.getItem(key) : null;
        if (latest && saving) {
          const value = JSON.parse(latest) as Draft;
          if (value.accountId !== options.accountId || value.appId !== options.appId) throw new Error('invalid_feedback_draft');
          if (!draft.pending && value.draftId !== draft.draftId) { localConflict = true; saved = false; keepAsNew.hidden = false; say('Черновик изменён или отправлен в другой вкладке. Обновите историю; этот текст остаётся в окне.'); return; }
          if (value.pending && !draft.pending) { draft.pending = { requestId: value.pending.requestId, installationId: value.pending.installationId }; draft.body = value.body; draft.attachments = value.attachments; text.value = draft.body; renderPreviews(); }
        }
        if (!draft.pending) {
          if (!draft.body.trim() && !draft.attachments.length) { say('Напишите сообщение или добавьте голос либо снимок.'); return; }
          if (draft.body.length > context.limits.bodyChars || draft.installationId !== context.installationId) { say('Проверьте сообщение и получателя. Черновик не отправлен.'); return; }
          validateAttachmentBudget(draft.attachments, context.limits);
          draft.pending = { requestId: crypto.randomUUID(), installationId: context.installationId };
          if (!persist() && saving) { draft.pending = null; return; }
        }
        const pending = draft.pending;
        const result = await options.api.request<{ requestId: string; replayed: boolean; receipt: FeedbackReceipt; ticket: FeedbackTicket | null }>('apps.feedback.submit', {
          appId: options.appId, installationId: pending.installationId, requestId: pending.requestId,
          body: draft.body.trim() || (draft.attachments.some(item => item.kind === 'audio') ? 'Голосовое сообщение' : 'Сообщение со снимком'), attachments: draft.attachments,
        });
        if (!current()) return;
        if (result.requestId !== pending.requestId || !result.receipt?.ticketId) throw new Error('invalid_feedback_receipt');
        draft.draftId = crypto.randomUUID(); draft.body = ''; draft.attachments = []; draft.pending = null; text.value = ''; persist(); renderPreviews();
        say(`Принято сервером. Обращение ${result.receipt.ticketId}.`);
        if (result.ticket) renderTicket(result.ticket);
      });
    } catch (error) { if (current()) {
      // Native service validates these before entering its transaction. Unknown outcomes stay frozen.
      if (mediaRefusals.has(errorCode(error))) draft.pending = null;
      persist(); say(`${mediaRefusals.has(errorCode(error)) ? 'Не отправлено. ' : ''}${failure(error)}`);
    } }
    finally { if (current()) { busy = false; controls(); renderPreviews(); } }
  }
  form.addEventListener('submit', event => { event.preventDefault(); void submission(); });

  let historyGeneration = 0, selectedTicketId: string | null = null;
  async function loadHistory(cursor?: string): Promise<void> {
    if (!current() || !context) return; const stamp = ++historyGeneration;
    if (!cursor) historyList.replaceChildren(el('p', 'sw-muted', 'Загружаем разрешённые обращения…'));
    try {
      const result = await options.api.request<{ tickets: FeedbackTicket[]; nextCursor?: string | null }>('apps.feedback.list', { appId: options.appId, installationId: context.installationId, limit: 20, ...(cursor ? { cursor } : {}) });
      if (!current() || stamp !== historyGeneration) return;
      if (!cursor) historyList.replaceChildren(); else historyList.querySelector('[data-feedback-more]')?.remove();
      if (!result.tickets.length && !cursor) historyList.append(el('p', 'sw-muted', 'Обращений пока нет.'));
      for (const ticket of result.tickets) {
        const row = button(ticket.body.slice(0, 160) || 'Обращение с вложением', undefined, 'sf-ticket-row', () => void loadTicket(ticket.id));
        row.dataset.feedbackTicketId = ticket.id;
        row.append(el('small', 'sw-muted sf-ticket-row-status', `${statuses[ticket.status] ?? 'Обращение'} · ${formatDate(ticket.createdAt)}`)); historyList.append(row);
      }
      if (result.nextCursor) { const nextCursor = result.nextCursor; const more = button('Показать ещё', undefined, '', () => { more.disabled = true; void loadHistory(nextCursor).finally(() => { if (more.isConnected) more.disabled = false; }); }); more.dataset.feedbackMore = ''; historyList.append(more); }
      if (!cursor && selectedTicketId) void loadTicket(selectedTicketId);
    } catch (error) { if (current() && stamp === historyGeneration) historyList.replaceChildren(el('p', 'sf-error', failure(error))); }
  }
  let ticketGeneration = 0;
  async function loadTicket(ticketId: string): Promise<void> {
    if (!current() || !context) return; const stamp = ++ticketGeneration;
    detail.setAttribute('aria-busy', 'true');
    const status = detail.querySelector('.sf-ticket-status'); if (status) status.textContent = 'Проверяем актуальность…';
    try { const result = await options.api.request<{ ticket: FeedbackTicket }>('apps.feedback.get', { appId: options.appId, installationId: context.installationId, ticketId });
      if (current() && stamp === ticketGeneration) renderTicket(result.ticket);
    } catch (error) { if (current() && stamp === ticketGeneration) detail.replaceChildren(el('p', 'sf-error', failure(error))); }
    finally { if (current() && stamp === ticketGeneration) detail.removeAttribute('aria-busy'); }
  }
  function renderTicket(ticket: FeedbackTicket): void {
    if (!current() || ticket.appId !== options.appId || ticket.installationId !== context?.installationId) return;
    selectedTicketId = ticket.id;
    for (const row of historyList.querySelectorAll<HTMLElement>('[data-feedback-ticket-id]')) {
      if (row.dataset.feedbackTicketId === ticket.id) {
        const status = row.querySelector('.sf-ticket-row-status');
        if (status) status.textContent = `${statuses[ticket.status] ?? 'Обращение'} · ${formatDate(ticket.createdAt)}`;
      }
    }
    detail.replaceChildren(el('h3', '', `Обращение ${ticket.id}`), el('p', 'sf-ticket-status', statuses[ticket.status] ?? 'Обращение'), el('p', 'sf-ticket-body', ticket.body));
    for (const item of ticket.attachments ?? []) {
      if (typeof item.dataBase64 !== 'string') {
        detail.append(el('p', 'sw-muted', item.kind === 'audio' ? 'К обращению приложена голосовая запись.' : 'К обращению приложен снимок.'), button('Открыть вложения', undefined, '', () => void loadTicket(ticket.id))); continue;
      }
      const attachment = { ...item, dataBase64: item.dataBase64 };
      try { validateAttachmentBudget([attachment]); } catch { continue; }
      if (item.kind === 'image') { const image = el('img', 'sf-history-image'); image.src = mediaUrl(attachment); image.alt = 'Снимок из обращения'; detail.append(image); }
      else { const audio = el('audio'); audio.controls = true; audio.preload = 'metadata'; audio.src = mediaUrl(attachment); detail.append(audio); }
    }
    for (const message of ticket.messages ?? []) {
      const row = el('article', 'sf-thread-message'); row.append(el('strong', '', message.kind === 'support' ? 'Поддержка' : 'Отправитель'), el('p', '', message.body), el('small', 'sw-muted', formatDate(message.createdAt))); detail.append(row);
    }
    const actionRow = el('div', 'sf-row');
    const pendingAction = draft.pendingAction?.ticketId === ticket.id ? draft.pendingAction : null;
    if (pendingAction) actionRow.append(button('Проверить изменение', 'refresh', '', () => void changeTicket(ticket, pendingAction.kind, pendingAction.status)));
    else {
      if (ticket.canManage && ticket.status !== 'resolved') actionRow.append(button('Передать на проверку', undefined, '', () => void changeTicket(ticket, 'status', 'ready_to_check')));
      if (ticket.canAccept && ticket.status === 'ready_to_check') actionRow.append(button('Проблема решена', undefined, 'sw-button-primary', () => void changeTicket(ticket, 'accept')));
    }
    if (actionRow.childElementCount) detail.append(actionRow);
    if (ticket.canReply === false) return;
    const response = el('textarea', 'sw-input'); response.rows = 3; response.maxLength = context?.limits.bodyChars ?? 8000;
    response.setAttribute('aria-label', 'Ответ в обращении'); response.value = draft.pendingReply?.ticketId === ticket.id ? draft.pendingReply.body : draft.replies[ticket.id] ?? '';
    response.disabled = !!draft.pendingReply;
    response.addEventListener('input', () => {
      if (!response.value) delete draft.replies[ticket.id];
      else if (!(ticket.id in draft.replies) && Object.keys(draft.replies).length >= 20) { saved = false; say('Сохранено много черновиков ответов. Скопируйте новый текст или завершите прежний ответ.'); return; }
      else draft.replies[ticket.id] = response.value;
      persist();
    });
    const sendReply = button(draft.pendingReply?.ticketId === ticket.id ? 'Проверить ответ' : 'Ответить', undefined, 'sw-button-primary', () => {
      if (!current() || !context || busy || draft.pendingReply && draft.pendingReply.ticketId !== ticket.id) return;
      if (!draft.pendingReply && !response.value.trim()) return;
      const locks = navigator.locks; if (!locks) { say('Отправка недоступна в этом браузере. Текст сохранён.'); return; }
      busy = true; sendReply.disabled = true;
      void locks.request(lockKey, async () => {
        if (!current() || !context) return;
        if (!draft.pendingReply) { draft.pendingReply = { ticketId: ticket.id, installationId: ticket.installationId, requestId: crypto.randomUUID(), body: response.value.trim(), expectedRevision: ticket.revision }; if (!persist() && saving) { draft.pendingReply = null; return; } }
        const pending = draft.pendingReply;
        const result = await options.api.request<{ requestId: string; receipt: FeedbackReceipt; ticket: FeedbackTicket | null }>('apps.feedback.reply', { appId: options.appId,
          installationId: pending.installationId, ticketId: pending.ticketId, requestId: pending.requestId, body: pending.body, expectedRevision: pending.expectedRevision });
        if (!current()) return;
        if (result.requestId !== pending.requestId || result.receipt?.ticketId !== pending.ticketId) throw new Error('invalid_feedback_receipt');
        delete draft.replies[ticket.id]; draft.pendingReply = null; persist(); say('Ответ принят сервером.'); if (result.ticket) renderTicket(result.ticket); else void loadTicket(ticket.id);
      }).catch(error => {
        if (!current()) return;
        if (error && typeof error === 'object' && 'code' in error && error.code === 'feedback_revision_conflict') {
          // An explicit backend refusal created no reply. Preserve words, refresh revision, then allow a new intent.
          if (draft.pendingReply?.ticketId === ticket.id) { draft.replies[ticket.id] = draft.pendingReply.body; draft.pendingReply = null; }
          persist(); say('Обращение изменилось. Ответ не отправлен; текст сохранён. Проверьте свежую историю.'); void loadTicket(ticket.id);
        } else { persist(); say(failure(error)); }
      }).finally(() => { if (current()) { busy = false; sendReply.disabled = false; if (draft.pendingReply) { response.disabled = true; sendReply.querySelector('span')!.textContent = 'Проверить ответ'; } controls(); } });
    });
    detail.append(labeledField(context?.canManage ? 'Ответ поддержки' : 'Ваш ответ', response), sendReply);
  }

  async function changeTicket(ticket: FeedbackTicket, kind: PendingAction['kind'], status?: PendingAction['status']): Promise<void> {
    if (!current() || !context || busy || draft.pendingAction && draft.pendingAction.ticketId !== ticket.id) return;
    if (kind === 'status' && !status && !draft.pendingAction) return;
    if (!draft.pendingAction && (kind === 'accept' ? !ticket.canAccept || ticket.status !== 'ready_to_check' : !ticket.canManage)) return;
    const locks = navigator.locks; if (!locks) { say('Не удалось безопасно сохранить изменение в этом браузере.'); return; }
    busy = true; controls(); detail.querySelectorAll<HTMLButtonElement>('button').forEach(node => { node.disabled = true; });
    try {
      await locks.request(lockKey, async () => {
        if (!current() || !context) return;
        if (!draft.pendingAction) {
          draft.pendingAction = { kind, ticketId: ticket.id, installationId: ticket.installationId, requestId: crypto.randomUUID(), expectedRevision: ticket.revision, ...(kind === 'status' && status ? { status } : {}) };
          if (!persist() && saving) { draft.pendingAction = null; return; }
        }
        const pending = draft.pendingAction;
        if (!pending) return;
        const result = await options.api.request<{ requestId: string; receipt: FeedbackReceipt; ticket: FeedbackTicket | null }>(`apps.feedback.${pending.kind}`, {
          appId: options.appId, installationId: pending.installationId, ticketId: pending.ticketId, requestId: pending.requestId, expectedRevision: pending.expectedRevision,
          ...(pending.kind === 'status' ? { status: pending.status } : {}),
        });
        if (!current()) return;
        if (result.requestId !== pending.requestId || result.receipt?.ticketId !== pending.ticketId) throw new Error('invalid_feedback_receipt');
        draft.pendingAction = null; persist(); say(pending.kind === 'accept' ? 'Ваше подтверждение принято сервером.' : 'Обращение передано для проверки результата.');
        if (result.ticket) renderTicket(result.ticket); else void loadTicket(ticket.id);
      });
    } catch (error) {
      if (!current()) return;
      if (error && typeof error === 'object' && 'code' in error && error.code === 'feedback_revision_conflict') {
        draft.pendingAction = null; persist(); say('Обращение изменилось. Это изменение не применено. Проверьте свежий результат перед новым действием.'); void loadTicket(ticket.id);
      } else { persist(); say(failure(error)); void loadTicket(ticket.id); }
    } finally { if (current()) { busy = false; controls(); detail.querySelectorAll<HTMLButtonElement>('button').forEach(node => { node.disabled = false; }); } }
  }

  renderPreviews(); controls();
  return {
    open() { if (!current()) return; if (session.signal.aborted) session = new AbortController(); if (!dialog.open) dialog.showModal(); compose.hidden = false; history.hidden = true; text.focus(); void loadContext(); },
    async flush() { persist(); },
    hasUnsavedChanges: () => contents() && !saved,
    dispose() { if (disposed) return; if (current()) persist(); disposed = true; generation++; historyGeneration++; ticketGeneration++; session.abort(); recording?.cancel(); dialog.remove(); },
  };
}
