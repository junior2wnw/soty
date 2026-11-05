import {button, el} from './dom';
import {createFeedbackRecorder, type FeedbackRecorder} from './feedback-media.mjs';
import './canvas-voice-capture.css';

export interface CanvasVoiceCaptureOptions {
  isCurrent(): boolean;
  signal?: AbortSignal;
  /** Receives a RAM file, not a claim that the Source or canvas has saved it. */
  onFile(file: File): void | Promise<void>;
  /** True while permission, recording, or finalization is pending; false before onFile. */
  onRecordingChange?(busy: boolean): void;
}

export interface CanvasVoiceCapture {
  dispose(): void;
  setDisabled(disabled: boolean): void;
}

const MAX_BYTES = 2 * 1024 * 1024, MAX_SECONDS = 120;
const timeLabel = (seconds: number): string => `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')}`;

function captureError(error: unknown): string {
  const name = error instanceof Error ? error.name : '', code = error instanceof Error ? error.message : '';
  if (['NotAllowedError', 'SecurityError'].includes(name)) return 'Разрешите доступ к микрофону в настройках браузера и повторите.';
  if (name === 'NotFoundError') return 'Микрофон не найден. Подключите его и повторите.';
  if (name === 'NotReadableError') return 'Микрофон занят или недоступен. Освободите его и повторите.';
  if (code === 'feedback_record_unavailable') return 'Этот браузер не поддерживает запись Opus. Можно выбрать готовый аудиофайл.';
  if (code === 'feedback_attachment_bytes') return 'Запись превысила 2 МБ. Запишите более короткую голосовую.';
  if (code === 'feedback_record_empty') return 'Запись пустая. Попробуйте записать ещё раз.';
  return 'Не удалось завершить запись. Попробуйте ещё раз.';
}

/** Mounting never asks for a microphone. Only the explicit record button does. */
export function mountCanvasVoiceCapture(host: HTMLElement, options: CanvasVoiceCaptureOptions): CanvasVoiceCapture {
  const root = el('div', 'cv-capture'), row = el('div', 'cv-controls');
  const record = button('Записать голос', undefined, 'cv-record');
  const cancel = button('Отменить', undefined, 'cv-cancel');
  const time = el('span', 'cv-time', 'до 2:00'), status = el('p', 'cv-status', '');
  const dot = el('span', 'cv-dot'); dot.setAttribute('aria-hidden', 'true');
  record.prepend(dot); root.setAttribute('role', 'group'); root.setAttribute('aria-label', 'Голосовая запись');
  status.setAttribute('role', 'status'); status.setAttribute('aria-live', 'polite');
  time.setAttribute('aria-label', 'Длительность записи');
  row.append(record, time, cancel); root.append(row, status); host.append(root);
  let disposed = false, disabled = false, sequence = 0, active = false;
  let session: {id: number; controller: AbortController; recorder: FeedbackRecorder<Blob>; guard: ReturnType<typeof setInterval>} | null = null;
  let state: 'idle' | 'permission' | 'recording' | 'finishing' | 'delivery' = 'idle';
  const live = (): boolean => !disposed && !disabled && !options.signal?.aborted && options.isCurrent();
  const label = (value: string): void => { record.querySelector('span:not(.cv-dot)')!.textContent = value; };
  const activity = (value: boolean): void => { if (active === value) return; active = value; options.onRecordingChange?.(value); };
  function render(): void {
    root.dataset.state = state;
    record.disabled = !live() || ['permission', 'finishing', 'delivery'].includes(state);
    cancel.hidden = state === 'idle'; cancel.disabled = state === 'delivery';
    label(state === 'permission' ? 'Ожидание микрофона…' : state === 'recording' ? 'Остановить' : state === 'finishing' ? 'Завершение…' : state === 'delivery' ? 'Добавление…' : 'Записать голос');
  }
  function abandon(message = ''): void {
    const previous = session; session = null; sequence++;
    if (previous) { clearInterval(previous.guard); previous.controller.abort(); previous.recorder.cancel(); }
    state = 'idle'; time.textContent = 'до 2:00'; status.textContent = message; activity(false); render();
  }
  async function begin(): Promise<void> {
    if (!live()) return;
    if (state === 'recording') { state = 'finishing'; render(); session?.recorder.stop(); return; }
    if (state !== 'idle') return;
    if (globalThis.isSecureContext === false) {
      status.textContent = 'Для записи откройте Соты через HTTPS или через localhost на этом устройстве.'; return;
    }
    const id = ++sequence, controller = new AbortController();
    const recorder = createFeedbackRecorder({maxBytes: MAX_BYTES, maxSeconds: MAX_SECONDS, output: 'blob', signal: controller.signal,
      onTick: seconds => {
        if (session?.id !== id) return;
        if (!live()) { abandon(); return; }
        state = 'recording'; time.textContent = timeLabel(seconds); status.textContent = 'Идёт запись · максимум 2 минуты'; render();
      }});
    const guard = setInterval(() => { if (session?.id === id && !live()) abandon(); }, 100);
    session = {id, controller, recorder, guard}; state = 'permission'; status.textContent = 'Разрешите доступ к микрофону.'; render();
    try {
      activity(true);
      if (!live() || session?.id !== id) { abandon(); return; }
      const blob = await recorder.start();
      if (!live() || session?.id !== id) return;
      if (!['audio/webm', 'audio/ogg'].includes(blob.type) || blob.size === 0 || blob.size > MAX_BYTES) throw new Error('feedback_record_failed');
      const file = new File([blob], `Голосовая запись.${blob.type === 'audio/ogg' ? 'ogg' : 'webm'}`, {type: blob.type});
      state = 'delivery'; status.textContent = ''; activity(false); render();
      if (!live() || session?.id !== id) return;
      await options.onFile(file);
      if (live() && session?.id === id) status.textContent = 'Голосовая запись готова.';
    } catch (error) {
      if (live() && session?.id === id && !(error instanceof Error && error.name === 'AbortError')) status.textContent = captureError(error);
    } finally {
      clearInterval(guard); controller.abort(); recorder.cancel();
      if (session?.id === id) { session = null; state = 'idle'; activity(false); if (!disposed) render(); }
    }
  }
  const clicked = (): void => { void begin(); };
  const cancelled = (): void => { if (state !== 'delivery') abandon('Запись отменена.'); };
  record.addEventListener('click', clicked); cancel.addEventListener('click', cancelled);
  function dispose(): void {
    if (disposed) return;
    disposed = true; abandon(); root.remove();
    options.signal?.removeEventListener('abort', dispose);
    record.removeEventListener('click', clicked); cancel.removeEventListener('click', cancelled);
  }
  options.signal?.addEventListener('abort', dispose, {once: true});
  render(); if (options.signal?.aborted) dispose();
  return {dispose, setDisabled(value: boolean): void { disabled = value; if (value) abandon(); else if (!disposed) render(); }};
}
