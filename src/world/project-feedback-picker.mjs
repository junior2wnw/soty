import { captureSelectedDisplay, createFeedbackRecorder, rasterFile, validateAttachmentBudget } from './feedback-media.mjs';

/** Root-only picker. A Source message opens this dialog; only a separate user
 * gesture starts capture. Selected bytes stay in RAM and are not submitted. */
export function pickProjectFeedbackMedia({ appId, title, request, signal, document: doc = document } = {}) {
  if (!appId || !['image', 'audio'].includes(request?.kind) || signal?.aborted) return Promise.reject(new DOMException('Cancelled', 'AbortError'));
  return new Promise((resolve, reject) => {
    const dialog = doc.createElement('dialog'); dialog.className = 'soty-project-capture';
    const heading = doc.createElement('h2'); heading.id = `project-capture-${crypto.randomUUID()}`;
    heading.textContent = request.kind === 'audio' ? 'Голосовое сообщение' : 'Скриншот для обращения';
    dialog.setAttribute('aria-labelledby', heading.id);
    const recipient = doc.createElement('p'); recipient.textContent = `Для приложения «${typeof title === 'string' ? title.slice(0, 200) : 'Проект'}». Отправку обращения вы подтвердите в проекте.`;
    const status = doc.createElement('p'); status.setAttribute('role', 'status');
    const preview = doc.createElement('div'), actions = doc.createElement('div'); actions.className = 'soty-project-capture-actions';
    const button = text => { const element = doc.createElement('button'); element.type = 'button'; element.textContent = text; return element; };
    const begin = button(request.kind === 'audio' ? 'Записать голос' : 'Выбрать окно для снимка');
    const stop = button('Остановить запись'); stop.hidden = true;
    const choose = button(request.kind === 'audio' ? 'Выбрать готовую запись' : 'Выбрать файл');
    const use = button('Использовать вложение'); use.disabled = true;
    const cancel = button('Отмена');
    const file = doc.createElement('input'); file.type = 'file'; file.hidden = true;
    file.accept = request.kind === 'audio' ? 'audio/webm,audio/ogg' : 'image/png,image/jpeg,image/webp';
    actions.append(begin, stop, choose, use, cancel); dialog.append(heading, recipient, status, preview, actions, file);
    let closed = false, busy = false, selected = null, recorder = null;
    const lifetime = new AbortController();
    const cleanup = () => {
      lifetime.abort(); recorder?.cancel(); signal?.removeEventListener('abort', cancelled); dialog.close(); dialog.remove();
      selected = null; preview.replaceChildren();
    };
    const cancelled = () => { if (closed) return; closed = true; cleanup(); reject(new DOMException('Cancelled', 'AbortError')); };
    const show = attachment => {
      if (closed || lifetime.signal.aborted) return;
      validateAttachmentBudget([attachment]); selected = attachment; preview.replaceChildren();
      const media = doc.createElement(attachment.kind === 'image' ? 'img' : 'audio');
      media.src = `data:${attachment.mimeType};base64,${attachment.dataBase64}`;
      if (attachment.kind === 'image') media.alt = 'Выбранный скриншот'; else media.controls = true;
      preview.append(media); use.disabled = false; status.textContent = 'Проверьте вложение перед передачей в проект.';
    };
    const perform = async operation => {
      if (busy || closed) return;
      busy = true; begin.disabled = true; choose.disabled = true; use.disabled = true; status.textContent = '';
      try { show(await operation()); }
      catch (error) { if (!closed && error?.name !== 'AbortError') status.textContent = error?.message === 'feedback_attachment_bytes'
        ? 'Вложение должно быть не больше 1 МиБ. Выберите меньший файл или короткую запись.'
        : 'Не удалось подготовить вложение. Можно выбрать файл или отменить.'; }
      finally { busy = false; begin.disabled = false; choose.disabled = false; stop.hidden = true; use.disabled = !selected; }
    };
    begin.onclick = () => {
      if (request.kind === 'image') void perform(() => captureSelectedDisplay(1048576, lifetime.signal));
      else void perform(() => {
        recorder = createFeedbackRecorder({ maxBytes: 1048576, maxSeconds: 120, signal: lifetime.signal,
          onTick: seconds => { if (!closed) status.textContent = `Запись: ${seconds} сек. Можно остановить в любой момент.`; } });
        stop.hidden = false; return recorder.start();
      });
    };
    stop.onclick = () => recorder?.stop(); choose.onclick = () => file.click();
    file.onchange = () => {
      const selectedFile = file.files?.[0]; file.value = ''; if (!selectedFile) return;
      void perform(async () => {
        if (selectedFile.size > 8 * 1024 * 1024) throw new Error('feedback_attachment_bytes');
        if (request.kind === 'image') return rasterFile(selectedFile, 1048576, lifetime.signal);
        if (selectedFile.size > 1048576) throw new Error('feedback_attachment_bytes');
        // Windows commonly classifies an audio-only .webm as video/webm.
        // This is only a proposed container type; the Source's packet parser
        // still rejects video/non-Opus/truncation/duration before its commit.
        const mimeType = ['audio/webm', 'video/webm'].includes(selectedFile.type) ? 'audio/webm'
          : selectedFile.type === 'audio/ogg' ? 'audio/ogg' : null;
        if (!mimeType) throw new Error('feedback_media_unsupported');
        const { blobAttachment } = await import('./feedback-media.mjs');
        return blobAttachment(new Blob([selectedFile], { type: mimeType }), 'audio', selectedFile.name, lifetime.signal);
      });
    };
    use.onclick = () => {
      if (!selected || busy || closed || signal?.aborted) return;
      const result = selected; closed = true; cleanup(); resolve([result]);
    };
    cancel.onclick = cancelled; dialog.addEventListener('cancel', event => { event.preventDefault(); cancelled(); });
    signal?.addEventListener('abort', cancelled, { once: true });
    doc.body.append(dialog); dialog.showModal(); begin.focus();
  });
}
