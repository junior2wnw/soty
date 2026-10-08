import './app-reviews.css';
import { button, el, nounCount } from './dom';
import type { WorldApi } from './types';
import { parseReviewsContext, fetchPublicReviewJson, reviewSubjectKey, type ReviewBinding, type PublicReviewSubject, type PublicReviewPage } from './reviews-public.mjs';

export interface AppReviewsOptions {
  api: WorldApi; appId: string; title: string; isCurrent(): boolean;
  entry(): { domainId: string; path: string } | null;
  onClose?(): void;
}
export interface AppReviewsHandle { open(): void; hasUnsavedChanges(): boolean; dispose(): void; }
const labels = { app: 'Приложение', project: 'Проект', person: 'Человек' };
function failure(error: unknown): string {
  const code = error && typeof error === 'object' && 'code' in error ? String(error.code) : '';
  if (code === 'reviews_subject_unavailable') return 'Объект сейчас не опубликован у источника. Пустой список отзывов не подтверждён.';
  if (code === 'reviews_rate_limited') return 'Источник просит подождать. Обновите позже или откройте его страницу.';
  if (['reviews_response_invalid', 'reviews_context_invalid', 'reviews_redirect_denied', 'reviews_response_limit'].includes(code)) return 'Не удалось подтвердить данные этого объекта. Оценки и отзывы не показаны.';
  if (code === 'reviews_mode_unsupported') return 'Этот режим отзывов пока недоступен в Сотах.';
  if (['ACTIVE_PROFILE_CHANGED', 'apps_access_denied', 'reviews_authentication_required', 'app_unavailable'].includes(code)) return 'Доступ изменился. Откройте приложение с нужным аккаунтом.';
  return 'Источник сейчас не отвечает. Отзывы не загружены; это не означает, что их нет.';
}
function publicLink(binding: ReviewBinding): HTMLAnchorElement {
  const link = el('a', 'sr-public-link', 'Открыть в Поведай'); link.href = binding.publicPageUrl;
  link.target = '_blank'; link.rel = 'noopener noreferrer'; link.referrerPolicy = 'no-referrer'; return link;
}

/** Immutable per-stage account/entry lifetime. No provider script, cookies,
 * author identity matching or review write authority is introduced here. */
export function mountAppReviews(host: HTMLElement, options: AppReviewsOptions): AppReviewsHandle {
  let disposed = false, generation = 0, controller = new AbortController();
  const current = () => !disposed && options.isCurrent();
  type NativeDraft = { body: string; authorLabel: string; rating: string; requestId: string | null; pending: boolean; unknown: boolean };
  const drafts = new Map<string, NativeDraft>();
  let draftEntry: string | null = null;
  const entryKey = () => JSON.stringify(options.entry());
  const unsaved = () => [...drafts.values()].some(draft => draft.pending || draft.unknown || draft.body.trim() || draft.authorLabel.trim() || draft.rating);
  const dialog = el('dialog', 'sr-dialog'); dialog.setAttribute('aria-labelledby', `reviews-heading-${options.appId}`);
  const header = el('header', 'sr-header'), heading = el('h2', '', 'Отзывы'); heading.id = `reviews-heading-${options.appId}`;
  const close = button('Закрыть', 'close', 'sw-button-quiet', () => closeDialog()); header.append(heading, close);
  const appTitle = el('p', 'sr-app-title', options.title), notice = el('p', 'sr-notice'); notice.setAttribute('role', 'status'); notice.setAttribute('aria-live', 'polite');
  const tabs = el('nav', 'sr-subject-tabs'); tabs.setAttribute('aria-label', 'Объект отзывов');
  const content = el('section', 'sr-content'), retry = button('Обновить отзывы', 'refresh', '', () => void loadContext());
  dialog.append(header, appTitle, notice, tabs, content, retry); host.append(dialog);
  function cancel(): void { generation++; controller.abort(); controller = new AbortController(); }
  function closeDialog(): void {
    if (!current()) return;
    if (unsaved() && !window.confirm('Закрыть и удалить черновик? Если подтверждение не получено, запись могла сохраниться в Поведай.')) return;
    drafts.clear(); draftEntry = null; cancel(); dialog.close(); options.onClose?.();
  }
  dialog.addEventListener('cancel', event => { event.preventDefault(); closeDialog(); });
  function say(value: string): void { if (current()) notice.textContent = value; }
  async function loadContext(): Promise<void> {
    if (!current()) return; cancel(); const stamp = generation;
    tabs.replaceChildren(); content.replaceChildren(); retry.disabled = true; say('Проверяем, к каким объектам привязаны отзывы…');
    try {
      const entry = options.entry();
      const raw = await options.api.request<unknown>('apps.reviews.context', { appId: options.appId, ...(entry ? { domainId: entry.domainId, path: entry.path } : {}) });
      if (!current() || stamp !== generation || !dialog.open) return;
      const context = parseReviewsContext(raw, { allowFixtureOrigins: import.meta.env.DEV });
      if (context.mode === 'disabled') { say('Публичные отзывы к этому приложению пока не подключены.'); return; }
      for (const binding of context.subjects) {
        const tab = button(labels[binding.subjectKind], undefined, '', () => void select(binding));
        tab.dataset.reviewSubject = reviewSubjectKey(binding); tab.setAttribute('aria-pressed', 'false'); tabs.append(tab);
      }
      if (context.subjects.length === 1) tabs.hidden = true; else tabs.hidden = false;
      await select(context.subjects[0]!);
    } catch (error) { if (current() && stamp === generation) say(failure(error)); }
    finally { if (current()) retry.disabled = false; }
  }
  async function select(binding: ReviewBinding): Promise<void> {
    if (!current() || !dialog.open) return; cancel(); const stamp = generation, signal = controller.signal;
    for (const tab of tabs.querySelectorAll<HTMLButtonElement>('button')) tab.setAttribute('aria-pressed', String(tab.dataset.reviewSubject === reviewSubjectKey(binding)));
    const source = el('p', 'sr-source', `Источник: ${new URL(binding.origin).hostname}`);
    const rights = el('p', 'sr-rights', 'Здесь показаны опубликованные отзывы источника. Чтобы оставить отзыв, открыть обсуждение или сообщить о публикации, перейдите к источнику: там действуют его правила и вход.');
    const summary = el('section', 'sr-summary'), reviews = el('section', 'sr-list'), pageControls = el('div', 'sr-page-controls');
    const native = el('section', 'sr-list');
    content.replaceChildren(source, publicLink(binding), rights, summary, reviews, pageControls, native); retry.disabled = true; say('Загружаем опубликованные данные…');
    let nextCursor: string | null = null, pages = 0, loading = false;
    const seenItems = new Set<string>(), seenCursors = new Set<string>();
    const active = () => current() && stamp === generation && dialog.open && !signal.aborted;
    const capturedEntry = entryKey(), draftKey = reviewSubjectKey(binding);
    const selectedArgs = { appId: options.appId, subjectKind: binding.subjectKind, providerRef: binding.providerRef,
      subjectRef: binding.subjectRef, providerSubjectId: binding.providerSubjectId };
    const nativeStatus = el('p', 'sr-detail'); nativeStatus.setAttribute('role', 'status');
    const nativeOpen = button('Отзывы через вход Сот', undefined, '', () => void openNative());
    native.append(nativeOpen, nativeStatus);
    let nativePending = false;
    const nativeCurrent = () => active() && capturedEntry === entryKey();
    function nativeFailure(error: unknown): string {
      const code = error && typeof error === 'object' && 'code' in error ? String(error.code) : '';
      if (['managed_reviews_unavailable', 'unsupported_operation'].includes(code)) return 'Вход Сот для Поведай пока не подключён. Откройте отзывы у источника.';
      if (['managed_source_grant_required', 'managed_principal_revoked'].includes(code)) return 'Источник не разрешил это действие.';
      if (['managed_reviews_authentication_required', 'managed_actor_invalid', 'managed_identity_expired'].includes(code)) return 'Подтвердите вход в Поведай.';
      return 'Не удалось подтвердить вход или объект у источника.';
    }
    async function openNative(): Promise<void> {
      if (!nativeCurrent() || nativePending) return;
      if (draftEntry !== null && draftEntry !== capturedEntry && unsaved()) { nativeStatus.textContent = 'Сначала сохраните или закройте прежний черновик.'; return; }
      nativePending = true; nativeOpen.disabled = true; nativeStatus.textContent = 'Проверяем вход…';
      try {
        const result = await options.api.request<{ available: boolean; subjectKind?: string; providerSubjectId?: string;
          providerRef?: ReviewBinding['providerRef']; subjectRef?: ReviewBinding['subjectRef']; code?: string }>('apps.reviews.managed.context', selectedArgs);
        if (!nativeCurrent()) return;
        if (result.available !== true) { nativeStatus.textContent = 'Вход Сот для Поведай пока не подключён. Откройте отзывы у источника.'; return; }
        const equalRef = (value: ReviewBinding['providerRef'] | undefined, expected: ReviewBinding['providerRef']) => value?.id === expected.id && value.version === expected.version && value.digest === expected.digest;
        if (result.subjectKind !== binding.subjectKind || result.providerSubjectId !== binding.providerSubjectId
          || !equalRef(result.providerRef, binding.providerRef) || !equalRef(result.subjectRef, binding.subjectRef)) throw new TypeError('Native subject changed');
        if (draftEntry !== capturedEntry) { drafts.clear(); draftEntry = capturedEntry; }
        const draft = drafts.get(draftKey) ?? { body: '', authorLabel: '', rating: '', requestId: null, pending: false, unknown: false };
        drafts.set(draftKey, draft); showComposer(draft);
      } catch (error) { if (nativeCurrent()) nativeStatus.textContent = nativeFailure(error); }
      finally { if (nativeCurrent()) { nativePending = false; nativeOpen.disabled = false; } }
    }
    function showComposer(draft: NativeDraft): void {
      nativeOpen.hidden = true; nativeStatus.textContent = draft.unknown ? 'Подтверждение не получено. Запись могла сохраниться; автоматического повтора нет.' : 'Право отправки проверит источник. Новые записи сначала проходят проверку.';
      const form = el('form', 'sr-list'), bodyLabel = el('label', '', 'Текст отзыва'), body = el('textarea'); body.maxLength = 10000; body.rows = 4; body.value = draft.body;
      const aliasLabel = el('label', '', 'Имя для публикации (необязательно)'), alias = el('input'); alias.maxLength = 120; alias.value = draft.authorLabel; alias.autocomplete = 'off';
      const ratingLabel = el('label', '', 'Оценка (необязательно)'), ratingInput = el('select');
      for (const value of ['', '1', '2', '3', '4', '5']) { const option = el('option', '', value || 'Без оценки'); option.value = value; ratingInput.append(option); } ratingInput.value = draft.rating;
      bodyLabel.append(body); aliasLabel.append(alias); ratingLabel.append(ratingInput);
      const submit = button('Отправить на проверку', undefined, '', () => {}); submit.type = 'submit';
      const check = button('Обновить записи', 'refresh', '', () => void readNative()), list = el('section', 'sr-list');
      form.append(bodyLabel, aliasLabel, ratingLabel, submit, check); native.append(el('h3', '', 'Записи этого входа'), form, list);
      const update = () => { body.readOnly = alias.readOnly = draft.pending || draft.unknown; ratingInput.disabled = draft.pending || draft.unknown;
        submit.disabled = draft.pending || draft.unknown || !draft.body.trim(); };
      for (const input of [body, alias, ratingInput]) input.addEventListener('input', () => {
        if (!nativeCurrent() || draft.pending || draft.unknown) return;
        draft.body = body.value; draft.authorLabel = alias.value; draft.rating = ratingInput.value; draft.requestId = null; update();
      }); update();
      form.addEventListener('submit', event => { event.preventDefault(); void collect(); });
      async function collect(): Promise<void> {
        if (!nativeCurrent() || draft.pending || draft.unknown || !draft.body.trim()) return;
        draft.requestId ??= crypto.randomUUID(); const requestId = draft.requestId;
        const input = { ...selectedArgs, requestId, body: draft.body,
          ...(draft.authorLabel.trim() ? { authorLabel: draft.authorLabel.trim() } : {}), ...(draft.rating ? { rating: Number(draft.rating) } : {}) };
        draft.pending = true; update(); nativeStatus.textContent = 'Отправляем…';
        try {
          const reply = await options.api.request<{ status: string; reviewId: string; requestId: string }>('apps.reviews.managed.collect', input);
          if (!current() || capturedEntry !== entryKey() || drafts.get(draftKey) !== draft || draft.requestId !== requestId) return;
          if (reply.status !== 'pending' || reply.requestId !== requestId || typeof reply.reviewId !== 'string' || !reply.reviewId) throw new TypeError('Unconfirmed Native ACK');
          draft.body = ''; draft.authorLabel = ''; draft.rating = ''; draft.requestId = null; draft.unknown = false;
          if (nativeCurrent()) { body.value = ''; alias.value = ''; ratingInput.value = ''; nativeStatus.textContent = 'Запись сохранена и ожидает проверки источника.'; }
        } catch {
          if (current() && capturedEntry === entryKey() && drafts.get(draftKey) === draft && draft.requestId === requestId) {
            draft.unknown = true;
            if (nativeCurrent()) nativeStatus.textContent = 'Подтверждение не получено. Текст сохранён; запись могла дойти до источника. Автоматического повтора нет.';
          }
        } finally { draft.pending = false; if (nativeCurrent()) update(); }
      }
      async function readNative(): Promise<void> {
        if (!nativeCurrent() || check.disabled) return; check.disabled = true;
        try {
          const result = await options.api.request<{ items: { id: string; body: string; status: string }[]; nextCursor: string | null }>('apps.reviews.managed.list', { ...selectedArgs, limit: 20 });
          if (!nativeCurrent()) return;
          if (!Array.isArray(result.items) || result.items.length > 20) throw new TypeError('Invalid Native list');
          list.replaceChildren();
          for (const item of result.items) { if (typeof item.body !== 'string' || typeof item.status !== 'string') throw new TypeError('Invalid Native item');
            const row = el('article', 'sr-review'); row.append(el('small', 'sr-detail', item.status === 'pending' ? 'Ожидает проверки источника' : 'Запись источника'), el('p', 'sr-body', item.body)); list.append(row); }
          if (!result.items.length) list.append(el('p', 'sr-detail', 'В этом списке записей нет.'));
          if (draft.unknown) nativeStatus.textContent = 'Список обновлён. Он не подтверждает точный результат прежней отправки.';
        } catch (error) { if (nativeCurrent()) { list.replaceChildren(); nativeStatus.textContent = nativeFailure(error); } }
        finally { if (nativeCurrent()) check.disabled = false; }
      }
    }
    const more = button('Показать ещё отзывы', undefined, '', () => void loadMore()); more.hidden = true; pageControls.append(more);
    const pageStatus = el('p', 'sr-page-status'); pageStatus.setAttribute('role', 'status'); pageControls.append(pageStatus);
    function renderSummary(subject: PublicReviewSubject): void {
      summary.replaceChildren(el('h3', '', `${labels[binding.subjectKind]}: ${subject.title}`));
      const rating = subject.rating;
      summary.append(el('p', 'sr-rating', rating.count && rating.average !== null ? `${rating.average.toLocaleString('ru-RU', { maximumFractionDigits: 2 })} из 5 · ${nounCount(rating.count, 'оценка', 'оценки', 'оценок')}` : 'Оценок пока нет.'));
      summary.append(el('p', 'sr-counts', `${nounCount(subject.counts.publishedReviews, 'опубликованный отзыв', 'опубликованных отзыва', 'опубликованных отзывов')} · ${nounCount(subject.counts.discussionItems, 'комментарий у источника', 'комментария у источника', 'комментариев у источника')}`));
      if (rating.provenance.kind === 'imported') summary.append(el('p', 'sr-detail', 'Агрегат оценки импортирован источником. Он не является оценкой человека или всей системы Сот.'));
      if (!rating.distributionComplete) summary.append(el('p', 'sr-detail', 'Распределение оценок у источника неполное. Полный агрегат и отдельные отзывы могут иметь разный охват.'));
      if (rating.updatedAt) summary.append(el('small', 'sr-detail', `Обновление агрегата: ${new Date(rating.updatedAt).toLocaleString('ru-RU')}`));
    }
    function renderPage(page: PublicReviewPage): boolean {
      let added = 0;
      for (const item of page.items) {
        if (seenItems.has(item.id)) continue; seenItems.add(item.id); added++;
        const article = el('article', 'sr-review'), byline = el('header', 'sr-review-byline');
        byline.append(el('strong', '', item.author.name || 'Гость'), el('small', '', new Date(item.createdAt).toLocaleDateString('ru-RU'))); article.append(byline);
        if (item.author.verified) article.append(el('small', 'sr-provider-verification', 'Источник отмечает автора как подтверждённого. Это не подтверждение опыта или истинности отзыва.'));
        if (item.imported) article.append(el('small', 'sr-detail', 'Импортированный отзыв'));
        if (item.rating !== undefined) article.append(el('p', 'sr-item-rating', `Оценка: ${item.rating} из 5`));
        if (item.title) article.append(el('h4', '', item.title));
        article.append(el('p', 'sr-body', item.body));
        if (item.pros) article.append(el('p', 'sr-body', `Плюсы: ${item.pros}`));
        if (item.cons) article.append(el('p', 'sr-body', `Минусы: ${item.cons}`));
        reviews.append(article);
      }
      pages++; nextCursor = page.nextCursor;
      if (!seenItems.size && !page.nextCursor) reviews.append(el('p', 'sr-empty', 'Опубликованных отзывов пока нет.'));
      const repeated = !!nextCursor && seenCursors.has(nextCursor);
      if (nextCursor) seenCursors.add(nextCursor);
      more.hidden = !nextCursor || repeated || pages >= 10 || !added;
      if (repeated || nextCursor && !added) pageStatus.textContent = 'Источник вернул повтор страницы. Обновите список перед продолжением.';
      else if (pages >= 10 && nextCursor) pageStatus.textContent = 'В этой панели показаны первые 100 отзывов. Остальные доступны на странице источника.';
      else pageStatus.textContent = `${nounCount(seenItems.size, 'отзыв показан', 'отзыва показаны', 'отзывов показано')}.`;
      return added > 0;
    }
    async function loadMore(): Promise<void> {
      if (!active() || loading || !nextCursor || pages >= 10) return; const cursor = nextCursor;
      loading = true; more.disabled = true; pageStatus.textContent = 'Загружаем следующую страницу…';
      try { const page = await fetchPublicReviewJson(binding, 'reviews', { cursor, signal }); if (active()) renderPage(page); }
      catch (error) { if (active()) pageStatus.textContent = failure(error); }
      finally { if (active()) { loading = false; more.disabled = false; } }
    }
    try {
      const [subject, page] = await Promise.allSettled([fetchPublicReviewJson(binding, 'subject', { signal }), fetchPublicReviewJson(binding, 'reviews', { signal })]);
      if (!active()) return;
      if (subject.status === 'rejected') { say(failure(subject.reason)); return; }
      renderSummary(subject.value);
      if (page.status === 'fulfilled') { renderPage(page.value); say('Опубликованные данные загружены. Объекты и их оценки показаны отдельно.'); }
      else { say('Данные объекта загружены. Отзывы сейчас недоступны.'); reviews.append(el('p', 'sr-error', failure(page.reason))); }
    } finally { if (active()) retry.disabled = false; }
  }
  return {
    open() { if (!current()) return; if (!dialog.open) dialog.showModal(); close.focus(); void loadContext(); },
    hasUnsavedChanges: unsaved,
    dispose() { if (disposed) return; disposed = true; drafts.clear(); cancel(); dialog.remove(); },
  };
}
