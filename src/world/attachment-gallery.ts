import { button, el, iconButton } from './dom';
import type { WorldApi, WorldAttachment } from './types';
import type { WorldDialog } from './dialogs';
import { createAttachmentPreviewPool, isRasterAttachment, type AttachmentImageLease } from './attachment-preview.mjs';
import { bindGallerySwipe } from './gallery-swipe.mjs';

interface Options {
  api: WorldApi; accountId: string; communityId: string; viewport: HTMLElement;
  current(): boolean; error(reason: unknown): void;
  dialog(title: string, onClose: () => void): WorldDialog;
}
interface Thumbnail { refresh(): void; dispose(): void; }
interface Entry { id: string; refs: WorldAttachment[]; current(): boolean; dispose(): void; }

/** Message photos only, maximum the existing attachment batch. A signed read
 * creates each new view; offscreen/hidden/removed/context changes release it. */
export function createChatAttachmentMedia(options: Options) {
  let closed = false;
  const lifetime = new AbortController(), entries = new Map<string, Entry>(), thumbnails = new Map<Element, Thumbnail>();
  const downloadTimers = new Map<ReturnType<typeof setTimeout>, AttachmentImageLease>();
  const visible = (): boolean => !closed && options.current() && document.visibilityState !== 'hidden';
  const pool = createAttachmentPreviewPool({ api: options.api, accountId: options.accountId, communityId: options.communityId, current: visible });
  let gallery: { entryId: string; close(): void } | undefined;
  const size = (bytes: number): string => bytes < 1024 ? `${bytes} Б` : `${Math.ceil(bytes / 1024)} КБ`;
  const report = (reason: unknown): void => { if (visible()) options.error(reason); };
  const explanation = (reason: unknown): string => {
    const code = typeof reason === 'object' && reason && 'code' in reason ? reason.code : '';
    if (code === 'attachment_preview_dimensions') return 'Большое разрешение. Можно скачать оригинал.';
    if (code === 'attachment_preview_busy') return 'Много открытых фото. Откройте это фото отдельно.';
    if (code === 'attachment_preview_format' || code === 'attachment_preview_unsupported' || code === 'attachment_preview_capacity') return 'Превью недоступно. Можно скачать оригинал.';
    return 'Не удалось открыть фото. Повторите или скачайте файл.';
  };
  const accessError = (reason: unknown): void => {
    const code = typeof reason === 'object' && reason && 'code' in reason ? reason.code : '';
    if (['community_membership_required', 'community_not_found', 'community_banned', 'attachment_account_mismatch', 'ACTIVE_PROFILE_CHANGED'].includes(String(code))) report(reason);
  };
  async function download(ref: WorldAttachment, current: () => boolean): Promise<void> {
    const lease = await pool.load(ref, { kind: 'download', current });
    if (!visible() || !current()) { lease.release(); return; }
    try { const link = el('a'); link.href = lease.url; link.download = lease.name; link.click(); }
    catch (reason) { lease.release(); throw reason; }
    const timer = setTimeout(() => { downloadTimers.delete(timer); lease.release(); }, 1000);
    downloadTimers.set(timer, lease);
  }
  const observer = typeof IntersectionObserver === 'function' ? new IntersectionObserver(items => {
    for (const item of items) {
      const thumb = thumbnails.get(item.target); if (!thumb) continue;
      (item.target as HTMLElement).dataset.inView = String(item.isIntersecting && document.visibilityState !== 'hidden'); thumb.refresh();
    }
  }, { root: options.viewport, rootMargin: '160px 0px', threshold: 0 }) : undefined;

  function openGallery(entry: Entry, initial: number): void {
    if (!visible() || !entry.current()) return;
    gallery?.close(); const refs = entry.refs.filter(isRasterAttachment); if (!refs.length) return;
    let disposed = false, version = 0, selected = initial, lease: AttachmentImageLease | undefined, stopSwipe: ReturnType<typeof bindGallerySwipe> | undefined;
    const events = new AbortController();
    const dispose = (): void => { if (disposed) return; disposed = true; version++; events.abort(); stopSwipe?.(); lease?.release(); lease = undefined; image.removeAttribute('src'); if (gallery?.entryId === entry.id) gallery = undefined; };
    const dialog = options.dialog('Фотографии', dispose);
    dialog.element.classList.add('sw-photo-gallery');
    const stage = el('div', 'sw-photo-stage'), image = el('img'); image.decoding = 'async'; image.hidden = true;
    const status = el('p', 'sw-photo-status'); status.setAttribute('role', 'status'); status.setAttribute('aria-live', 'polite');
    const fallback = el('div', 'sw-photo-fallback');
    const retry = button('Повторить просмотр', 'refresh', 'sw-button-quiet', () => show(selected, true)); retry.hidden = true; fallback.append(status, retry);
    const name = el('p', 'sw-photo-name'), counter = el('span', 'sw-photo-counter'); counter.setAttribute('aria-live', 'polite');
    const current = (): boolean => !disposed && visible() && entry.current() && dialog.element.open;
    const previous = iconButton('Предыдущее фото', 'back', () => show(selected - 1));
    const next = iconButton('Следующее фото', 'next', () => show(selected + 1));
    const save = button('Скачать оригинал', 'download', 'sw-button-quiet', () => {
      if (save.disabled || !current()) return; save.disabled = true; const request = version;
      void download(refs[selected]!, () => current() && request === version).catch(reason => { if (current() && request === version) { status.textContent = 'Не удалось скачать файл. Повторите действие.'; report(reason); } })
        .finally(() => { if (current() && request === version) save.disabled = false; });
    });
    const controls = el('div', 'sw-photo-controls'); controls.append(previous, counter, next, save);
    stage.append(image, fallback); dialog.body.append(stage, name, controls);
    function show(index: number, force = false): void {
      if (!current()) { dialog.close({ restoreFocus: false }); return; }
      const target = Math.max(0, Math.min(refs.length - 1, index));
      if (!force && target === selected && (lease || stage.getAttribute('aria-busy') === 'true')) return;
      stopSwipe?.cancel();
      selected = target; const request = ++version, ref = refs[selected]!;
      lease?.release(); lease = undefined; image.removeAttribute('src'); image.hidden = true; image.alt = ref.name;
      name.textContent = ref.name; name.title = ref.name; name.setAttribute('aria-label', ref.name); counter.textContent = `${selected + 1} из ${refs.length}`;
      previous.disabled = selected === 0; next.disabled = selected === refs.length - 1; save.disabled = false;
      fallback.hidden = false; status.hidden = false; retry.hidden = true; status.textContent = 'Загружаем фото…'; stage.setAttribute('aria-busy', 'true');
      void pool.load(ref, { kind: 'gallery', current: () => current() && request === version, released: () => { if (request === version) {
        lease = undefined; image.removeAttribute('src'); image.hidden = true; fallback.hidden = false; status.hidden = false; retry.hidden = false;
        status.textContent = 'Фото закрыто. Повторите просмотр или скачайте оригинал.'; stage.setAttribute('aria-busy', 'false');
      } } })
        .then(result => { if (!current() || request !== version) { result.release(); return; } lease = result; image.src = result.url; image.hidden = false; status.hidden = true; fallback.hidden = true; })
        .catch(reason => { if (!current() || request !== version) return; lease = undefined; fallback.hidden = false; status.hidden = false; retry.hidden = false; status.textContent = explanation(reason); accessError(reason); })
        .finally(() => { if (current() && request === version) stage.setAttribute('aria-busy', 'false'); });
    }
    dialog.element.addEventListener('keydown', event => {
      if (event.isComposing || event.altKey || event.ctrlKey || event.metaKey) return;
      if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); dialog.close(); }
      else if (['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) {
        event.preventDefault(); event.stopPropagation(); show(event.key === 'Home' ? 0 : event.key === 'End' ? refs.length - 1 : selected + (event.key === 'ArrowLeft' ? -1 : 1));
      }
    }, { signal: events.signal });
    image.addEventListener('error', () => { if (current()) { lease?.release(); lease = undefined; image.hidden = true; fallback.hidden = false; status.hidden = false; retry.hidden = false; status.textContent = 'Превью недоступно. Можно скачать оригинал.'; } }, { signal: events.signal });
    stopSwipe = bindGallerySwipe(stage, { current, selectionEpoch: () => version, previous: () => show(selected - 1), next: () => show(selected + 1) });
    gallery = { entryId: entry.id, close: () => dialog.close({ restoreFocus: false }) }; show(initial);
  }

  function render(id: string, attachments: WorldAttachment[], guard: () => boolean): HTMLElement {
    remove(id); const block = el('div', 'sw-message-files'), disposers: (() => void)[] = [];
    const entry: Entry = { id, refs: attachments.map(ref => ({ ...ref })), current: () => !closed && entries.get(id) === entry && options.current() && guard(), dispose: () => { for (const dispose of disposers) dispose(); } };
    entries.set(id, entry);
    const photos = el('div', 'sw-message-photos'); photos.setAttribute('role', 'group'); photos.setAttribute('aria-label', 'Фотографии сообщения');
    const rasterRefs = entry.refs.filter(isRasterAttachment); if (rasterRefs.length) block.append(photos);
    for (const ref of entry.refs) {
      const file = el('div', 'sw-message-file-item');
      if (isRasterAttachment(ref)) {
        const opener = el('button', 'sw-message-photo'); opener.type = 'button'; opener.setAttribute('aria-label', `Открыть фото ${ref.name}`);
        opener.setAttribute('aria-haspopup', 'dialog'); opener.dataset.inView = 'false';
        const image = el('img'); image.alt = ''; image.loading = 'lazy'; image.decoding = 'async'; image.hidden = true;
        const label = el('span', 'sw-message-photo-label', 'Открыть фото'); opener.append(image, label);
        let permanentFallback = false;
        opener.addEventListener('click', () => { if (permanentFallback) save.click(); else openGallery(entry, rasterRefs.indexOf(ref)); });
        let version = 0, busy = false, disposed = false, lease: AttachmentImageLease | undefined;
        const fallbackLabel = (): void => {
          label.hidden = false; label.textContent = 'Превью недоступно · Скачать файл'; opener.setAttribute('aria-label', `Скачать файл ${ref.name}`); opener.removeAttribute('aria-haspopup');
        };
        const reset = (): void => { version++; busy = false; lease?.release(); lease = undefined; image.removeAttribute('src'); image.hidden = true; label.hidden = false; if (permanentFallback) fallbackLabel(); else label.textContent = 'Открыть фото'; opener.removeAttribute('aria-busy'); };
        const thumbnail: Thumbnail = {
          refresh() {
            const inView = opener.dataset.inView === 'true';
            if (!entry.current() || !visible() || !inView) { reset(); return; }
            if (disposed || busy || lease || permanentFallback) return; busy = true; const request = ++version;
            label.textContent = 'Загружаем…'; opener.setAttribute('aria-busy', 'true');
            void pool.load(ref, { current: () => !disposed && entry.current() && visible() && opener.dataset.inView === 'true' && request === version,
              released: () => { if (request === version) { lease = undefined; image.removeAttribute('src'); image.hidden = true; label.hidden = false; label.textContent = 'Открыть фото'; } } })
              .then(result => { if (disposed || !entry.current() || !visible() || request !== version) { result.release(); return; } lease = result; image.src = result.url; image.hidden = false; label.hidden = true; })
              .catch(reason => { if (disposed || !entry.current() || request !== version) return;
                const code = typeof reason === 'object' && reason && 'code' in reason ? reason.code : '';
                permanentFallback = ['attachment_preview_format', 'attachment_preview_dimensions', 'attachment_preview_unsupported'].includes(String(code));
                if (permanentFallback) fallbackLabel(); else label.textContent = 'Не удалось открыть · Повторить'; opener.title = explanation(reason); accessError(reason);
              })
              .finally(() => { if (request === version) { busy = false; opener.removeAttribute('aria-busy'); } });
          },
          dispose() { disposed = true; reset(); observer?.unobserve(opener); thumbnails.delete(opener); },
        };
        image.addEventListener('error', () => { permanentFallback = true; reset(); opener.title = 'Превью недоступно. Можно скачать оригинал.'; });
        thumbnails.set(opener, thumbnail); disposers.push(() => thumbnail.dispose()); observer?.observe(opener); file.append(opener); photos.append(file);
      } else block.append(file);
      const save = button(ref.name, 'download', 'sw-message-file', () => {
        if (!entry.current() || save.disabled) return; save.disabled = true; save.setAttribute('aria-busy', 'true');
        void download(ref, entry.current).catch(reason => { if (entry.current()) report(reason); }).finally(() => { save.disabled = false; save.removeAttribute('aria-busy'); });
      });
      save.setAttribute('aria-label', `Скачать ${ref.name}`); save.append(el('small', '', size(ref.size))); file.append(save);
    }
    return block;
  }
  function remove(id: string): void { if (gallery?.entryId === id) gallery.close(); const entry = entries.get(id); entries.delete(id); entry?.dispose(); }
  function clear(): void {
    gallery?.close(); pool.clear(); for (const thumbnail of thumbnails.values()) thumbnail.refresh();
    for (const [timer, lease] of downloadTimers) { clearTimeout(timer); lease.release(); } downloadTimers.clear();
  }
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') clear();
    else for (const element of thumbnails.keys()) { observer?.unobserve(element); observer?.observe(element); }
  }, { signal: lifetime.signal });
  return { render, remove, clear, close() { if (closed) return; closed = true; lifetime.abort(); observer?.disconnect(); for (const id of [...entries.keys()]) remove(id); clear(); pool.close(); } };
}
