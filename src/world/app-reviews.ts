import './app-reviews.css';
import { button, el, nounCount } from './dom';
import type { WorldApi } from './types';
import { parseReviewsContext, fetchPublicReviewJson, reviewSubjectKey, type ReviewBinding, type PublicReviewSubject, type PublicReviewPage } from './reviews-public.mjs';

export interface AppReviewsOptions {
  api: WorldApi; appId: string; title: string; isCurrent(): boolean;
  entry(): { domainId: string; path: string } | null;
  onClose?(): void;
}
export interface AppReviewsHandle { open(): void; dispose(): void; }
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
  const link = el('a', 'sr-public-link', 'Открыть отзывы у источника'); link.href = binding.publicPageUrl;
  link.target = '_blank'; link.rel = 'noopener noreferrer'; link.referrerPolicy = 'no-referrer'; return link;
}

/** Immutable per-stage account/entry lifetime. No provider script, cookies,
 * author identity matching or review write authority is introduced here. */
export function mountAppReviews(host: HTMLElement, options: AppReviewsOptions): AppReviewsHandle {
  let disposed = false, generation = 0, controller = new AbortController();
  const current = () => !disposed && options.isCurrent();
  const dialog = el('dialog', 'sr-dialog'); dialog.setAttribute('aria-labelledby', `reviews-heading-${options.appId}`);
  const header = el('header', 'sr-header'), heading = el('h2', '', 'Отзывы'); heading.id = `reviews-heading-${options.appId}`;
  const close = button('Закрыть', 'close', 'sw-button-quiet', () => closeDialog()); header.append(heading, close);
  const appTitle = el('p', 'sr-app-title', options.title), notice = el('p', 'sr-notice'); notice.setAttribute('role', 'status'); notice.setAttribute('aria-live', 'polite');
  const tabs = el('nav', 'sr-subject-tabs'); tabs.setAttribute('aria-label', 'Объект отзывов');
  const content = el('section', 'sr-content'), retry = button('Обновить отзывы', 'refresh', '', () => void loadContext());
  dialog.append(header, appTitle, notice, tabs, content, retry); host.append(dialog);
  function cancel(): void { generation++; controller.abort(); controller = new AbortController(); }
  function closeDialog(): void { if (!current()) return; cancel(); dialog.close(); options.onClose?.(); }
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
    content.replaceChildren(source, publicLink(binding), rights, summary, reviews, pageControls); retry.disabled = true; say('Загружаем опубликованные данные…');
    let nextCursor: string | null = null, pages = 0, loading = false;
    const seenItems = new Set<string>(), seenCursors = new Set<string>();
    const active = () => current() && stamp === generation && dialog.open && !signal.aborted;
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
    dispose() { if (disposed) return; disposed = true; cancel(); dialog.remove(); },
  };
}
