import { avatar, button, el, iconButton } from './dom';
import { icon } from './icons';
import { appendHexSurface } from './brand';
import { resolveAppArt, installAppArtFallback } from './app-art.mjs';
import './experience.css';
export interface AppCardData {
    id: string;
    title: string;
    description: string;
    coverKey?: string;
    status?: string;
    community?: string;
    symbol: string;
    featured?: boolean;
    own?: boolean;
    shared?: boolean;
    actionLabel?: string;
    people?: {
        name: string;
        avatarUrl?: string;
        profileId?: string;
        avatarRevision?: number | null;
    }[];
    open(): void;
    settings?: () => void;
    menuLabel?: string;
}
export type HomeFilter = 'all' | 'mine' | 'together';
export interface HomeOptions {
    cards: AppCardData[];
    query: string;
    filter: HomeFilter;
    presentation: 'list' | 'field';
    communities: {
        id: string;
        name: string;
        unread: number;
        open(): void;
        people: AppCardData['people'];
    }[];
    loading: boolean;
    errors: string[];
    create(): void;
    discover(): void;
    assistant(): void;
    retry(): void;
    searchWorld?(query: string): void;
    saved?(): void;
    changeFilter(filter: HomeFilter): void;
    changeQuery(query: string): void;
    changePresentation(view: 'list' | 'field'): void;
    mountField?(host: HTMLElement, cards: AppCardData[]): void;
    unmountField?(): void;
}
function appMark(name: string): HTMLElement { const mark = el('span', 'sx-app-mark'); appendHexSurface(mark); mark.append(icon(name)); return mark; }
export function createAppCard(card: AppCardData): HTMLElement {
    const shell = el('article', `sx-app-card${card.featured ? ' is-featured' : ''}`);
    const open = el('button', 'sx-app-open');
    open.type = 'button';
    open.dataset.entityId = `app:${card.id}`;
    open.setAttribute('aria-label', `Открыть: ${card.title}`);
    open.addEventListener('click', card.open);
    if (card.actionLabel)
        open.setAttribute('aria-label', `${card.actionLabel}: ${card.title}`);
    const art = resolveAppArt(card.coverKey ?? { appId: card.id });
    const visual = el('span', 'sx-app-visual');
    visual.style.setProperty('--app-accent', art.palette.accent);
    visual.style.setProperty('--app-base', art.palette.base);
    visual.style.setProperty('--app-ink', art.palette.ink);
    visual.style.setProperty('--art-position', `${art.focalPoint.x * 100}% ${art.focalPoint.y * 100}%`);
    visual.style.setProperty('--art-compact-position', `${art.compactFocalPoint.x * 100}% ${art.compactFocalPoint.y * 100}%`);
    if (art.src) {
        const image = el('img', 'sx-app-art');
        image.sizes = card.featured ? '(min-width:1100px) 60vw, (min-width:720px) 50vw, 100vw' : '(min-width:1100px) 25vw, (min-width:720px) 33vw, 50vw';
        image.alt = '';
        image.width = art.width;
        image.height = art.height;
        image.decoding = 'async';
        image.loading = card.featured ? 'eager' : 'lazy';
        installAppArtFallback(image, () => { visual.classList.add('is-fallback'); const fallback = el('span', 'sx-fallback-art'); fallback.append(icon(card.symbol)); visual.prepend(fallback); });
        image.srcset = art.srcset;
        image.src = art.src;
        visual.append(image);
    }
    else {
        visual.classList.add('is-fallback');
        const fallback = el('span', 'sx-fallback-art');
        fallback.append(icon(card.symbol));
        visual.append(fallback);
    }
    const copy = el('span', 'sx-app-copy');
    copy.append(appMark(card.symbol));
    const text = el('span', 'sx-app-text');
    text.append(el('strong', '', card.title), el('small', '', card.description));
    copy.append(text);
    if (card.featured) {
        const resume = el('span', 'sx-app-resume');
        resume.append(icon('arrow'), el('span', '', card.actionLabel || 'Открыть'));
        text.append(resume);
    }
    visual.append(copy);
    open.append(visual);
    shell.append(open);
    const details = el('div', 'sx-card-details');
    if (card.status) {
        const state = el('span', 'sx-app-status', card.status);
        state.classList.toggle('is-offline', card.status === 'Не в сети');
        details.append(state);
    }
    if (card.people?.length) {
        const faces = el('span', 'sx-face-stack');
        for (const person of card.people.slice(0, 3))
            faces.append(avatar(person.name, person.avatarUrl, 'sage', person.profileId, person.avatarRevision));
        details.append(faces);
    }
    if (card.settings) {
        const action = iconButton(card.menuLabel || `Настроить: ${card.title}`, 'more', card.settings);
        action.dataset.appAction = 'inspect'; action.dataset.appId = card.id;
        details.append(action);
    }
    if (details.childNodes.length)
        shell.append(details);
    return shell;
}
export function createHome(options: HomeOptions): HTMLElement {
    const home = el('section', 'sx-home');
    home.setAttribute('aria-label', 'Мои соты');
    const heading = el('header', 'sx-home-heading'), copy = el('div');
    copy.append(el('h1', '', 'Мои соты'), el('p', 'sx-subtitle', 'Приложения, люди и идеи — рядом'));
    const presentation = el('div', 'sx-presentation');
    presentation.setAttribute('role', 'group');
    presentation.setAttribute('aria-label', 'Вид моих сот');
    for (const [value, label, symbol] of [['list', 'Карточки', 'grid'], ['field', 'Поле', 'cells']] as const) {
        const choice = button(label, symbol, '', () => options.changePresentation(value));
        choice.setAttribute('aria-label', label);
        choice.setAttribute('aria-pressed', String(value === options.presentation));
        presentation.append(choice);
    }
    heading.append(copy, presentation);
    const search = el('label', 'sx-mobile-search');
    const input = el('input');
    input.type = 'search';
    input.value = options.query;
    input.placeholder = 'Мои приложения';
    input.setAttribute('aria-label', 'Поиск моих приложений');
    search.append(icon('search'), input);
    input.addEventListener('compositionstart', () => { input.dataset.composing = 'true'; });
    input.addEventListener('compositionend', () => { delete input.dataset.composing; });
    const toolbar = el('div', 'sx-home-toolbar'), tabs = el('div', 'sx-home-tabs');
    tabs.setAttribute('role', 'group');
    tabs.setAttribute('aria-label', 'Приложения');
    let filter = options.filter, query = options.query;
    const stage = el('div', options.presentation === 'field' ? 'sx-home-field' : 'sx-home-grid');
    const render = () => {
        options.unmountField?.();
        stage.replaceChildren();
        const cards = options.cards.filter(card => (filter === 'all' || filter === 'mine' && card.own || filter === 'together' && card.shared) && `${card.title} ${card.description}`.toLocaleLowerCase('ru').includes(query.trim().toLocaleLowerCase('ru')));
        const fieldMode = options.presentation === 'field' && Boolean(options.mountField) && cards.length > 0;
        stage.classList.toggle('is-card-results', options.presentation === 'field' && !fieldMode);
        if (fieldMode) {
            options.mountField!(stage, cards);
            return;
        }
        if (!cards.length && options.loading) {
            const skeleton = el('div', 'sx-card-skeleton');
            skeleton.setAttribute('role', 'status');
            skeleton.setAttribute('aria-label', 'Обновляем приложения');
            stage.append(skeleton);
            return;
        }
        if (!cards.length) {
            const empty = el('div', 'sx-home-empty');
            empty.append(appMark('search'), el('h2', '', query ? 'Ничего не нашлось' : filter === 'together' ? 'Ваши приложения вместе' : 'Здесь будут ваши проекты'), button(query ? 'Сбросить поиск' : 'Добавить приложение', query ? 'close' : 'plus', 'sw-button-primary', () => { if (query) {
                query = '';
                input.value = '';
                options.changeQuery('');
                render();
                if (input.getClientRects().length)
                    input.focus();
                else
                    tabs.querySelector<HTMLButtonElement>('button[aria-pressed="true"]')?.focus();
            }
            else
                options.create(); }));
            if (query && options.searchWorld)
                empty.append(button('Искать в общем мире', 'world', 'sw-button-quiet', () => options.searchWorld?.(query)));
            stage.append(empty);
            return;
        }
        const featured = cards.filter(card => card.featured).slice(0, 2);
        const normal = cards.filter(card => !featured.includes(card));
        if (featured.length) {
            const row = el('div', 'sx-feature-row');
            featured.forEach((card, index) => { const node = createAppCard(card); node.classList.add(index === 0 ? 'sx-feature-primary' : 'sx-feature-secondary'); row.append(node); });
            stage.append(row);
        }
        normal.forEach(card => stage.append(createAppCard(card)));
        if (options.loading) {
            const skeleton = el('div', 'sx-card-skeleton');
            skeleton.setAttribute('role', 'status');
            skeleton.setAttribute('aria-label', 'Обновляем приложения');
            stage.append(skeleton);
        }
    };
    for (const [value, label] of [['all', 'Все'], ['mine', 'Мои проекты'], ['together', 'Вместе']] as const) {
        const tab = button(label, undefined, '', () => { filter = value; options.changeFilter(value); tabs.querySelectorAll('button').forEach(node => node.setAttribute('aria-pressed', String(node === tab))); render(); });
        tab.setAttribute('aria-pressed', String(filter === value));
        tabs.append(tab);
    }
    tabs.append(button('Открытия', undefined, 'sx-discover-tab', options.discover));
    if (options.saved) tabs.append(button('Сохранённые', 'folder', 'sx-discover-tab', options.saved));
    const circles = el('div', 'sx-community-pills');
    for (const community of options.communities.slice(0, 2))
        circles.append(button(community.name, 'people', '', community.open));
    toolbar.append(tabs, circles);
    input.addEventListener('input', () => { query = input.value; options.changeQuery(query); render(); });
    render();
    const circle = el('section', 'sx-your-circle');
    circle.append(el('h2', '', 'Ваш круг'));
    const compact = el('div', 'sx-circle-content');
    for (const community of options.communities.slice(0, 3)) {
        const chip = button(community.name, 'people', 'sx-circle-chip', community.open);
        if (community.unread)
            chip.append(el('span', 'sx-unread', String(community.unread)));
        compact.append(chip);
        if (community.people?.length) {
            const faces = el('span', 'sx-face-stack');
            for (const person of community.people.slice(0, 3))
                faces.append(avatar(person.name, person.avatarUrl, 'sage', person.profileId, person.avatarRevision));
            compact.append(faces);
        }
    }
    compact.append(button('Найти своих', 'world', 'sx-circle-chip', options.discover));
    circle.append(compact, button('Создать с ИИ', 'sparkle', 'sx-circle-assistant', options.assistant));
    home.append(heading, search, toolbar, stage);
    if (options.errors.length) {
        const error = el('div', 'sx-home-errors');
        error.setAttribute('role', 'status');
        error.append(el('span', '', options.errors.join(' · ')), button('Повторить', 'refresh', 'sw-button-quiet', options.retry));
        home.append(error);
    }
    home.append(circle);
    return home;
}
