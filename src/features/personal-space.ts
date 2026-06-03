import QRCode from "qrcode";
import { icon } from "../icons";
import type { IconName } from "../icons";

export type PersonalSpaceRoute = {
  readonly handle: string;
  readonly slug: string;
};

export type PersonalSpaceInstallResult = {
  readonly ok: boolean;
  readonly message: string;
};

export type PersonalSpaceProfileUpdate = {
  readonly displayName: string;
  readonly about: string;
  readonly contact: string;
};

export type PersonalSpacePostDraft = {
  readonly text: string;
};

export type PersonalSpacePageOptions = {
  readonly route: PersonalSpaceRoute;
  readonly canInstall: () => boolean;
  readonly canNotify: () => boolean;
  readonly install: () => Promise<PersonalSpaceInstallResult>;
  readonly enableNotifications: () => Promise<PersonalSpaceInstallResult>;
  readonly updateProfile: (route: PersonalSpaceRoute, update: PersonalSpaceProfileUpdate) => Promise<PersonalSpaceInstallResult>;
  readonly savePost: (route: PersonalSpaceRoute, draft: PersonalSpacePostDraft) => Promise<PersonalSpaceInstallResult>;
  readonly uploadPhoto: (route: PersonalSpaceRoute, file: File) => Promise<string>;
  readonly exportBackup: () => void;
  readonly importBackup: (file: File) => Promise<PersonalSpaceInstallResult>;
  readonly openMessage: (profile: PersonalSpaceProfile, fromHandle: string) => void;
  readonly openRuntime: (profile: PersonalSpaceProfile) => void;
};

type PersonalSpaceContact = {
  readonly label: string;
  readonly value: string;
  readonly href?: string;
};

type PersonalSpacePost = {
  readonly id: string;
  readonly title: string;
  readonly text: string;
  readonly meta: string;
};

type PersonalSpaceReview = {
  readonly id: string;
  readonly author: string;
  readonly text: string;
  readonly rating: number;
};

type PersonalSpaceLink = {
  readonly slug: string;
  readonly title: string;
  readonly summary: string;
  readonly href: string;
  readonly active: boolean;
};

export type PersonalSpaceProfile = {
  readonly kind: "entity" | "space";
  readonly handle: string;
  readonly slug: string;
  readonly url: string;
  readonly displayName: string;
  readonly shortName: string;
  readonly accountName: string;
  readonly photoUrl: string;
  readonly title: string;
  readonly headline: string;
  readonly about: string;
  readonly accent: string;
  readonly contacts: readonly PersonalSpaceContact[];
  readonly posts: readonly PersonalSpacePost[];
  readonly reviews: readonly PersonalSpaceReview[];
  readonly spaces: readonly PersonalSpaceLink[];
  readonly actions: {
    readonly messageUrl: string;
    readonly runtimeUrl: string;
  };
};

const fallbackProfile: PersonalSpaceProfile = {
  kind: "entity",
  handle: "guest",
  slug: "",
  url: "/@guest",
  displayName: "Соты",
  shortName: "Соты",
  accountName: "Соты",
  photoUrl: "",
  title: "страница",
  headline: "контакт, записи, отзывы, связь",
  about: "Контакт, записи, отзывы, связь.",
  accent: "#f1f1f1",
  contacts: [],
  posts: [],
  reviews: [],
  spaces: [],
  actions: {
    messageUrl: "/?pwa=1&bare=1",
    runtimeUrl: "/?pwa=1"
  }
};

const layers = [
  { id: "card", label: "Контакт", title: "Контакт", icon: "person" },
  { id: "personal", label: "Я", title: "Страница", icon: "hexagon" },
  { id: "reviews", label: "Отзывы", title: "Отзывы", icon: "heart" },
  { id: "messages", label: "Связь", title: "Связь", icon: "mail" },
  { id: "place", label: "Соты", title: "Соты", icon: "hexagon" }
] as const satisfies readonly {
  readonly id: string;
  readonly label: string;
  readonly title: string;
  readonly icon: IconName;
}[];

type PersonalSpaceLayer = typeof layers[number]["id"];
type EntityActionId = "edit" | "install" | "message" | "note" | "notifications" | "review" | "share" | "runtime";
type EntityActionSurface = "hero" | "reviews" | "messages" | "place";
type EntityAction = {
  readonly id: EntityActionId;
  readonly label: string;
  readonly icon: IconName;
  readonly tone: "primary" | "secondary";
};
const localHandleKey = "soty:personal-handle:v1";

export function personalSpaceRouteFromLocation(location: Location = window.location): PersonalSpaceRoute | null {
  const parts = location.pathname.split("/").filter(Boolean);
  const first = parts[0] || "";
  if (!first.startsWith("@")) {
    return null;
  }
  const handle = cleanRoutePart(first.slice(1));
  if (!handle) {
    return null;
  }
  return {
    handle,
    slug: cleanRoutePart(parts[1] || "")
  };
}

export function personalSpaceManifestHref(route: PersonalSpaceRoute): string {
  const handle = encodeURIComponent(route.handle);
  return route.slug
    ? `/manifest/space/${handle}/${encodeURIComponent(route.slug)}.json`
    : `/manifest/space/${handle}.json`;
}

export async function renderPersonalSpacePage(root: HTMLElement, options: PersonalSpacePageOptions): Promise<void> {
  root.innerHTML = renderLoading(options.route);
  const profile = await loadPersonalSpaceProfile(options.route);
  const activeLayer: PersonalSpaceLayer = options.route.slug ? "place" : "card";
  const localHandle = loadLocalHandle();
  root.innerHTML = renderPage(profile, activeLayer, options.canInstall(), options.canNotify(), localHandle);
  bindPersonalSpace(root, profile, options);
}

export async function uploadPersonalSpacePhoto(route: PersonalSpaceRoute, file: File): Promise<string> {
  const dataUrl = await fileToAvatarDataUrl(file);
  const handle = encodeURIComponent(route.handle);
  const url = route.slug
    ? `/api/spaces/${handle}/${encodeURIComponent(route.slug)}/photo`
    : `/api/spaces/${handle}/photo`;
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json"
    },
    body: JSON.stringify({ dataUrl })
  });
  if (!response.ok) {
    throw new Error("profile photo upload failed");
  }
  const payload = await response.json() as unknown;
  if (!isRecord(payload) || payload.ok !== true) {
    throw new Error("profile photo upload rejected");
  }
  return cleanUrlPath(payload.photoUrl);
}

export async function updatePersonalSpaceProfile(route: PersonalSpaceRoute, update: PersonalSpaceProfileUpdate): Promise<PersonalSpaceInstallResult> {
  const handle = encodeURIComponent(route.handle);
  const response = await fetch(`/api/spaces/${handle}/profile`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json"
    },
    body: JSON.stringify(update)
  });
  if (!response.ok) {
    return { ok: false, message: "Не удалось сохранить." };
  }
  const payload = await response.json() as unknown;
  return isRecord(payload) && payload.ok === true
    ? { ok: true, message: "Сохранено." }
    : { ok: false, message: "Не удалось сохранить." };
}

export async function savePersonalSpacePost(route: PersonalSpaceRoute, draft: PersonalSpacePostDraft): Promise<PersonalSpaceInstallResult> {
  const handle = encodeURIComponent(route.handle);
  const response = await fetch(`/api/spaces/${handle}/posts`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json"
    },
    body: JSON.stringify(draft)
  });
  if (!response.ok) {
    return { ok: false, message: "Не удалось сохранить." };
  }
  const payload = await response.json() as unknown;
  return isRecord(payload) && payload.ok === true
    ? { ok: true, message: "Сохранено." }
    : { ok: false, message: "Не удалось сохранить." };
}

async function loadPersonalSpaceProfile(route: PersonalSpaceRoute): Promise<PersonalSpaceProfile> {
  const handle = encodeURIComponent(route.handle);
  const url = route.slug
    ? `/api/spaces/${handle}/${encodeURIComponent(route.slug)}`
    : `/api/spaces/${handle}`;
  try {
    const response = await fetch(url, {
      cache: "no-store",
      headers: { Accept: "application/json" }
    });
    if (!response.ok) {
      return fallbackFor(route);
    }
    return normalizeProfile(await response.json(), route);
  } catch {
    return fallbackFor(route);
  }
}

function renderLoading(route: PersonalSpaceRoute): string {
  return `
    <section class="personal-space-shell is-loading">
      <main class="personal-loading">
        <span>@${escapeHtml(route.handle)}</span>
        <b>Открываю пространство</b>
      </main>
    </section>
  `;
}

function renderPage(profile: PersonalSpaceProfile, activeLayer: PersonalSpaceLayer, canInstall: boolean, canNotify: boolean, localHandle: string): string {
  const ownSpace = isOwnProfile(profile, localHandle);
  const initialsText = initials(profile.shortName || profile.displayName);
  const avatar = profile.photoUrl
    ? `<img src="${escapeAttr(profile.photoUrl)}" alt="" />`
    : escapeHtml(initialsText);
  const heroActions = entityActionsFor({ surface: "hero", ownSpace, canInstall });
  return `
    <section class="personal-space-shell" style="--personal-accent:${escapeAttr(profile.accent)}" data-active-layer="${activeLayer}">
      <header class="personal-topbar">
        <a href="/" class="personal-brand">соты</a>
        ${ownSpace ? "" : `<nav aria-label="пространство"><button type="button" data-action="self">${icon("person")} ${localHandle ? "Я" : "Создать"}</button></nav>`}
        ${ownSpace ? `<input data-backup-import type="file" accept="application/json,.json" hidden />` : ""}
      </header>
      <main class="personal-main">
        <section class="personal-hero" aria-label="визитная карточка">
          ${ownSpace ? `<button class="personal-avatar${profile.photoUrl ? " has-photo" : ""}" type="button" data-action="photo" aria-label="Сделать фото страницы">` : `<div class="personal-avatar${profile.photoUrl ? " has-photo" : ""}" aria-hidden="true">`}
            ${avatar}
            ${ownSpace ? `<span>${icon("scan")} Фото</span>` : ""}
          ${ownSpace ? "</button>" : "</div>"}
          ${ownSpace ? `<input data-profile-photo type="file" accept="image/*" capture="user" hidden />` : ""}
          <div class="personal-identity">
            <span>@${escapeHtml(profile.handle)}${profile.slug ? ` / ${escapeHtml(profile.slug)}` : ""}</span>
            <h1>${escapeHtml(profile.displayName)}</h1>
            <p>${escapeHtml(personalSpaceCopy(profile.headline, ownSpace))}</p>
            ${renderQuickContacts(profile, ownSpace)}
            ${renderEntityActions(heroActions)}
            <div class="personal-note" data-install-note></div>
          </div>
        </section>
        <div class="personal-layerbar" role="tablist" aria-label="слои пространства">
          ${layers.map((display) => {
            return `
            <button type="button" role="tab" data-layer="${display.id}" aria-label="${escapeAttr(display.title)}" title="${escapeAttr(display.title)}" aria-selected="${display.id === activeLayer ? "true" : "false"}">
              ${icon(display.icon)}
              ${escapeHtml(display.label)}
            </button>
          `;
          }).join("")}
        </div>
        <section class="personal-panels">
          ${layers.map((layer) => renderLayerPanel(profile, layer.id, ownSpace, activeLayer, canNotify)).join("")}
        </section>
      </main>
    </section>
  `;
}

function renderQuickContacts(profile: PersonalSpaceProfile, ownSpace: boolean): string {
  const contacts = visibleContacts(profile, ownSpace).slice(0, 3);
  if (contacts.length === 0) {
    return "";
  }
  return `
    <div class="personal-quick-contacts" aria-label="контакты">
      ${contacts.map((contact) => `
        ${contact.href ? `<a href="${escapeAttr(contact.href)}">` : "<span>"}
          <small>${escapeHtml(contact.label)}</small>
          <b>${escapeHtml(contact.value)}</b>
        ${contact.href ? "</a>" : "</span>"}
      `).join("")}
    </div>
  `;
}

type PersonalPanelView = {
  readonly eyebrow: string;
  readonly title: string;
  readonly text?: string;
  readonly action?: EntityAction;
  readonly body: string;
};

function renderLayerPanel(
  profile: PersonalSpaceProfile,
  layer: PersonalSpaceLayer,
  ownSpace: boolean,
  activeLayer: PersonalSpaceLayer,
  canNotify: boolean
): string {
  const view = panelView(profile, layer, ownSpace, canNotify);
  return `
    <article class="personal-panel${layer === activeLayer ? " is-active" : ""}" data-panel="${layer}">
      <div class="personal-panel-copy">
        <span>${escapeHtml(view.eyebrow)}</span>
        <h2>${escapeHtml(view.title)}</h2>
        ${view.text ? `<p>${escapeHtml(view.text)}</p>` : ""}
        ${view.action ? renderEntityAction(view.action, view.action.tone === "secondary" ? "personal-panel-action personal-secondary" : "personal-panel-action") : ""}
      </div>
      ${view.body}
    </article>
  `;
}

function panelView(profile: PersonalSpaceProfile, layer: PersonalSpaceLayer, ownSpace: boolean, canNotify: boolean): PersonalPanelView {
  if (layer === "personal") {
    return {
      eyebrow: "я",
      title: "Страница",
      ...(ownSpace ? { action: { id: "note", label: "Записать", icon: "hexagon", tone: "primary" } as const } : {}),
      body: renderPanelList(
        "personal-feed",
        profile.posts.map((post) => `
          <section class="personal-post">
            <small>${escapeHtml(post.meta)}</small>
            <h3>${escapeHtml(post.title)}</h3>
            <p>${escapeHtml(personalSpaceCopy(post.text, ownSpace))}</p>
          </section>
        `),
        "hexagon",
        "Пока пусто."
      )
    };
  }
  if (layer === "reviews") {
    const [reviewAction] = entityActionsFor({ surface: "reviews", ownSpace, canInstall: false });
    return {
      eyebrow: "отзывы",
      title: "Отзывы",
      ...(reviewAction ? { action: reviewAction } : {}),
      body: renderPanelList(
        "personal-reviews",
        profile.reviews.map((review) => `
          <section class="personal-review">
            <div>${"★".repeat(Math.max(1, Math.min(5, review.rating)))}</div>
            <p>${escapeHtml(review.text)}</p>
            <b>${escapeHtml(review.author)}</b>
          </section>
        `),
        "heart",
        ownSpace ? "Отзывы появятся здесь." : "Пока нет отзывов."
      )
    };
  }
  if (layer === "messages") {
    const messageActions = entityActionsFor({ surface: "messages", ownSpace, canInstall: false, canNotify });
    if (ownSpace) {
      return {
        eyebrow: "связь",
        title: "Связь",
        body: `
          <div class="personal-message-preview">
            <div class="personal-message-copy"><b>${escapeHtml(profile.shortName)}</b><p>На странице.</p></div>
            ${renderInlineEntityActions(messageActions)}
            <small data-action-note></small>
          </div>
        `
      };
    }
    return {
      eyebrow: "связь",
      title: "Сообщения",
      body: `
        <div class="personal-message-preview">
          <div class="personal-message-copy"><b>${escapeHtml(profile.shortName)}</b><p>Личный чат.</p></div>
          ${renderInlineEntityActions(messageActions)}
          <small data-action-note></small>
        </div>
      `
    };
  }
  if (layer === "place") {
    return {
      eyebrow: "соты",
      title: "Соты",
      text: "Модули страницы.",
      body: renderSpaceModules(profile)
    };
  }
  return {
    eyebrow: "контакт",
    title: "Контакт",
    text: personalSpaceCopy(profile.about, ownSpace),
    ...(ownSpace ? { action: { id: "edit", label: "Править", icon: "person", tone: "secondary" } as const } : {}),
    body: `
      <div class="personal-contact-list">
        ${visibleContacts(profile, ownSpace).map((contact) => `
          ${contact.href ? `<a href="${escapeAttr(contact.href)}">` : "<div>"}
            <span>${escapeHtml(contact.label)}</span>
            <b>${escapeHtml(contact.value)}</b>
          ${contact.href ? "</a>" : "</div>"}
        `).join("")}
      </div>
    `
  };
}

function entityActionsFor(options: { readonly surface: EntityActionSurface; readonly ownSpace: boolean; readonly canInstall: boolean; readonly canNotify?: boolean }): readonly EntityAction[] {
  if (options.surface === "hero") {
    return options.ownSpace
      ? [
        { id: "share", label: "Поделиться", icon: "qr", tone: "primary" },
        { id: "install", label: options.canInstall ? "Установить" : "Как установить", icon: "install", tone: "secondary" }
      ]
      : [
        { id: "install", label: "Сохранить", icon: "install", tone: "primary" },
        { id: "message", label: "Написать", icon: "mail", tone: "secondary" },
        { id: "share", label: "Поделиться", icon: "qr", tone: "secondary" }
      ];
  }
  if (options.surface === "reviews") {
    return options.ownSpace ? [] : [{ id: "review", label: "Отзыв", icon: "heart", tone: "primary" }];
  }
  if (options.surface === "messages") {
    return [
      { id: options.ownSpace ? "note" : "message", label: options.ownSpace ? "Записать" : "Написать", icon: options.ownSpace ? "hexagon" : "send", tone: "primary" },
      ...(!options.ownSpace && options.canNotify ? [{ id: "notifications", label: "Оповещения", icon: "bell", tone: "secondary" } as const] : [])
    ];
  }
  return [{ id: "runtime", label: "Открыть", icon: "hexagon", tone: "primary" }];
}

function renderEntityActions(actions: readonly EntityAction[]): string {
  if (actions.length === 0) {
    return "";
  }
  return `<div class="personal-actions">${actions.map((action) => renderEntityAction(action)).join("")}</div>`;
}

function renderInlineEntityActions(actions: readonly EntityAction[]): string {
  if (actions.length === 0) {
    return "";
  }
  return `<div class="personal-message-actions">${actions.map((action) => renderEntityAction(action)).join("")}</div>`;
}

function renderEntityAction(action: EntityAction, className?: string): string {
  const buttonClass = className || (action.tone === "secondary" ? "personal-secondary" : "personal-primary");
  return `<button class="${escapeAttr(buttonClass)}" type="button" data-action="${action.id}">${icon(action.icon)} ${escapeHtml(action.label)}</button>`;
}

function renderSpaceModules(profile: PersonalSpaceProfile): string {
  const coreModules = [
    { title: "Агент", summary: "помощь" },
    { title: "Приложения", summary: "инструменты" },
    { title: "Доступ", summary: "устройства" }
  ];
  return `
    <div class="personal-spaces">
      ${coreModules.map((module) => `
        <button type="button" data-action="runtime">
          <span>${escapeHtml(module.title)}</span>
          <p>${escapeHtml(module.summary)}</p>
        </button>
      `).join("")}
      ${profile.spaces.map((space) => `
        <a href="${escapeAttr(space.href)}" data-space-link class="${space.active ? "is-active" : ""}">
          <span>${escapeHtml(space.title)}</span>
          <p>${escapeHtml(space.summary)}</p>
        </a>
      `).join("")}
    </div>
  `;
}

function visibleContacts(profile: PersonalSpaceProfile, ownSpace: boolean): readonly PersonalSpaceContact[] {
  return ownSpace
    ? profile.contacts.filter((contact) => contact.label !== "чат")
    : profile.contacts;
}

function personalSpaceCopy(text: string, ownSpace: boolean): string {
  if (!ownSpace) {
    return text;
  }
  return text
    .replaceAll("сообщений", "заметок")
    .replaceAll("сообщения", "заметки")
    .replaceAll("сообщение", "заметку")
    .replaceAll("Чат", "Связь")
    .replaceAll("чат", "связь");
}

function renderPanelList(className: string, items: readonly string[], emptyIcon: IconName, emptyText: string): string {
  return `
    <div class="${className}">
      ${items.length ? items.join("") : `<section class="personal-empty">${icon(emptyIcon)} <span>${escapeHtml(emptyText)}</span></section>`}
    </div>
  `;
}

function bindPersonalSpace(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): void {
  const shell = root.querySelector<HTMLElement>(".personal-space-shell");
  const photoInput = root.querySelector<HTMLInputElement>("[data-profile-photo]");
  const backupInput = root.querySelector<HTMLInputElement>("[data-backup-import]");
  shell?.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const layerButton = target?.closest<HTMLButtonElement>("[data-layer]");
    if (layerButton && shell.contains(layerButton)) {
      const layer = layerButton.dataset.layer as PersonalSpaceLayer | undefined;
      if (layer) {
        setActiveLayer(root, layer);
      }
      return;
    }
    const spaceLink = target?.closest<HTMLAnchorElement>("[data-space-link]");
    if (spaceLink && shell.contains(spaceLink)) {
      event.preventDefault();
      window.history.pushState({}, "", spaceLink.href);
      window.dispatchEvent(new CustomEvent("soty-personal-routechange"));
      return;
    }
    const actionNode = target?.closest<HTMLElement>("[data-action]");
    if (actionNode && shell.contains(actionNode)) {
      handleEntityAction(root, actionNode, profile, options);
    }
  });
  backupInput?.addEventListener("change", () => {
    const file = backupInput.files?.[0];
    if (file) {
      void importPersonalBackup(root, file, options);
    }
    backupInput.value = "";
  });
  photoInput?.addEventListener("change", () => {
    const file = photoInput.files?.[0];
    if (file) {
      void updateProfilePhoto(root, file, options);
    }
    photoInput.value = "";
  });
}

function handleEntityAction(root: HTMLElement, node: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): void {
  const action = node.dataset.action;
  if (action === "message") {
    const handle = loadLocalHandle();
    if (handle) {
      options.openMessage(profile, handle);
      return;
    }
    showNicknameSheet(root, {
      title: "Как подписать?",
      description: "Короткое имя для сообщений.",
      action: "Написать",
      onDone: (nextHandle) => options.openMessage(profile, nextHandle)
    });
    return;
  }
  if (action === "review") {
    const handle = loadLocalHandle();
    if (handle) {
      showReviewSheet(root, profile, handle, options);
      return;
    }
    showNicknameSheet(root, {
      title: "Как подписать?",
      description: "Короткое имя для отзыва.",
      action: "Отзыв",
      onDone: (nextHandle) => showReviewSheet(root, profile, nextHandle, options)
    });
    return;
  }
  if (action === "runtime") {
    options.openRuntime(profile);
    return;
  }
  if (action === "notifications" && node instanceof HTMLButtonElement) {
    void enablePersonalNotifications(root, node, options);
    return;
  }
  if (action === "note") {
    showPostSheet(root, options);
    return;
  }
  if (action === "edit") {
    showProfileSheet(root, profile, options);
    return;
  }
  if (action === "self") {
    const handle = loadLocalHandle();
    if (handle) {
      openPersonalRoute(handle);
      return;
    }
    showNicknameSheet(root, {
      title: "Имя страницы",
      description: "Оно станет ссылкой и QR.",
      action: "Создать Я",
      onDone: openPersonalRoute
    });
    return;
  }
  if (action === "share") {
    void showShareSheet(root, profile, options);
    return;
  }
  if (action === "backup-export") {
    options.exportBackup();
    return;
  }
  if (action === "backup-import") {
    root.querySelector<HTMLInputElement>("[data-backup-import]")?.click();
    return;
  }
  if (action === "photo") {
    root.querySelector<HTMLInputElement>("[data-profile-photo]")?.click();
    return;
  }
  if (action === "install" && node instanceof HTMLButtonElement) {
    void installPersonalSpace(root, node, options);
  }
}

async function importPersonalBackup(root: HTMLElement, file: File, options: PersonalSpacePageOptions): Promise<void> {
  const note = root.querySelector<HTMLElement>("[data-install-note]");
  if (note) {
    note.textContent = "Импортирую...";
  }
  const result = await options.importBackup(file);
  if (note) {
    note.textContent = result.message;
  }
}

async function enablePersonalNotifications(root: HTMLElement, button: HTMLButtonElement, options: PersonalSpacePageOptions): Promise<void> {
  const note = button.closest<HTMLElement>(".personal-message-preview")?.querySelector<HTMLElement>("[data-action-note]")
    || root.querySelector<HTMLElement>("[data-install-note]");
  button.disabled = true;
  const result = await options.enableNotifications();
  if (note) {
    note.textContent = result.message;
  }
  if (result.ok) {
    button.textContent = "Включено";
    return;
  }
  button.disabled = false;
}

function showPostSheet(root: HTMLElement, options: PersonalSpacePageOptions): void {
  closePersonalOverlay(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet" role="dialog" aria-modal="true" aria-label="Записать">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <h2>Запись</h2>
      <form data-post-form>
        <textarea name="text" maxlength="420" required placeholder="Что важно?"></textarea>
        <button type="submit">${icon("check")} Сохранить</button>
      </form>
      <small data-error></small>
    </section>
  `;
  root.append(overlay);
  const textarea = overlay.querySelector<HTMLTextAreaElement>("textarea[name='text']");
  textarea?.focus();
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", () => overlay.remove());
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      overlay.remove();
    }
  });
  overlay.querySelector<HTMLFormElement>("[data-post-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    const text = cleanText(textarea?.value || "", 420);
    const error = overlay.querySelector<HTMLElement>("[data-error]");
    const button = overlay.querySelector<HTMLButtonElement>("button[type='submit']");
    if (!text) {
      if (error) {
        error.textContent = "Напишите пару слов.";
      }
      return;
    }
    if (button) {
      button.disabled = true;
    }
    void options.savePost(options.route, { text })
      .then(async (result) => {
        if (!result.ok) {
          throw new Error(result.message);
        }
        overlay.remove();
        await renderPersonalSpacePage(root, options);
        setActiveLayer(root, "personal");
      })
      .catch((err) => {
        if (error) {
          error.textContent = err instanceof Error ? err.message : "Не удалось сохранить.";
        }
        if (button) {
          button.disabled = false;
        }
      });
  });
}

function showProfileSheet(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): void {
  closePersonalOverlay(root);
  const contact = visibleContacts(profile, true).find((item) => item.label === "контакт")?.value || "";
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet" role="dialog" aria-modal="true" aria-label="Править страницу">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <h2>Страница</h2>
      <form data-profile-form>
        <input name="displayName" autocomplete="name" maxlength="100" aria-label="имя" placeholder="Имя или название" value="${escapeAttr(profile.displayName)}" />
        <textarea name="about" maxlength="420" aria-label="о странице" placeholder="О себе, проекте или месте">${escapeHtml(profile.about)}</textarea>
        <input name="contact" autocomplete="url" maxlength="160" aria-label="контакт" placeholder="Сайт, email или контакт" value="${escapeAttr(contact)}" />
        <button type="submit">${icon("check")} Сохранить</button>
      </form>
      <small data-error></small>
    </section>
  `;
  root.append(overlay);
  const nameInput = overlay.querySelector<HTMLInputElement>("input[name='displayName']");
  const aboutInput = overlay.querySelector<HTMLTextAreaElement>("textarea[name='about']");
  const contactInput = overlay.querySelector<HTMLInputElement>("input[name='contact']");
  nameInput?.focus();
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", () => overlay.remove());
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      overlay.remove();
    }
  });
  overlay.querySelector<HTMLFormElement>("[data-profile-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    const button = overlay.querySelector<HTMLButtonElement>("button[type='submit']");
    const error = overlay.querySelector<HTMLElement>("[data-error]");
    const update = {
      displayName: cleanText(nameInput?.value || "", 100),
      about: cleanText(aboutInput?.value || "", 420),
      contact: cleanText(contactInput?.value || "", 160)
    };
    if (!update.displayName && !update.about && !update.contact) {
      if (error) {
        error.textContent = "Добавьте хотя бы одно поле.";
      }
      return;
    }
    if (button) {
      button.disabled = true;
    }
    void options.updateProfile(options.route, update)
      .then(async (result) => {
        if (!result.ok) {
          throw new Error(result.message);
        }
        overlay.remove();
        await renderPersonalSpacePage(root, options);
        setActiveLayer(root, "card");
      })
      .catch((err) => {
        if (error) {
          error.textContent = err instanceof Error ? err.message : "Не удалось сохранить.";
        }
        if (button) {
          button.disabled = false;
        }
      });
  });
}

function showNicknameSheet(
  root: HTMLElement,
  options: { readonly title: string; readonly description: string; readonly action: string; readonly onDone: (handle: string) => void }
): void {
  closePersonalOverlay(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet" role="dialog" aria-modal="true" aria-label="${escapeAttr(options.title)}">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <h2>${escapeHtml(options.title)}</h2>
      <p>${escapeHtml(options.description)}</p>
      <form data-nick-form>
        <input name="handle" autocomplete="nickname" inputmode="text" maxlength="32" placeholder="например, anna" />
        <button type="submit">${escapeHtml(options.action)}</button>
      </form>
      <small data-error></small>
    </section>
  `;
  root.append(overlay);
  const input = overlay.querySelector<HTMLInputElement>("input[name='handle']");
  input?.focus();
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", () => overlay.remove());
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      overlay.remove();
    }
  });
  overlay.querySelector<HTMLFormElement>("[data-nick-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    const handle = cleanRoutePart(input?.value || "");
    const error = overlay.querySelector<HTMLElement>("[data-error]");
    if (!handle) {
      if (error) {
        error.textContent = "Введите короткий ник.";
      }
      return;
    }
    saveLocalHandle(handle);
    overlay.remove();
    options.onDone(handle);
  });
}

function showReviewSheet(root: HTMLElement, profile: PersonalSpaceProfile, author: string, options: PersonalSpacePageOptions): void {
  closePersonalOverlay(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet" role="dialog" aria-modal="true" aria-label="Отзыв">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <h2>Отзыв</h2>
      <form data-review-form>
        <textarea name="text" maxlength="320" required placeholder="Что важно?"></textarea>
        <button type="submit">${icon("heart")} Сохранить</button>
      </form>
      <small data-error></small>
    </section>
  `;
  root.append(overlay);
  const textarea = overlay.querySelector<HTMLTextAreaElement>("textarea[name='text']");
  textarea?.focus();
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", () => overlay.remove());
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      overlay.remove();
    }
  });
  overlay.querySelector<HTMLFormElement>("[data-review-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    const text = cleanText(textarea?.value || "", 320);
    const error = overlay.querySelector<HTMLElement>("[data-error]");
    const button = overlay.querySelector<HTMLButtonElement>("button[type='submit']");
    if (!text) {
      if (error) {
        error.textContent = "Напишите пару слов.";
      }
      return;
    }
    if (button) {
      button.disabled = true;
    }
    void submitPersonalReview(profile, author, text)
      .then(async () => {
        overlay.remove();
        await renderPersonalSpacePage(root, options);
        setActiveLayer(root, "reviews");
      })
      .catch(() => {
        if (error) {
          error.textContent = "Не удалось сохранить.";
        }
        if (button) {
          button.disabled = false;
        }
      });
  });
}

async function submitPersonalReview(profile: PersonalSpaceProfile, author: string, text: string): Promise<void> {
  const handle = encodeURIComponent(profile.handle);
  const url = profile.slug
    ? `/api/spaces/${handle}/${encodeURIComponent(profile.slug)}/reviews`
    : `/api/spaces/${handle}/reviews`;
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json"
    },
    body: JSON.stringify({ author, text, rating: 5 })
  });
  if (!response.ok) {
    throw new Error("review save failed");
  }
  const payload = await response.json() as unknown;
  if (!isRecord(payload) || payload.ok !== true) {
    throw new Error("review save rejected");
  }
}

async function showShareSheet(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): Promise<void> {
  closePersonalOverlay(root);
  const shareUrl = new URL(profile.url, window.location.origin).toString();
  const ownSpace = isOwnProfile(profile, loadLocalHandle());
  const qr = await QRCode.toDataURL(shareUrl, {
    margin: 1,
    scale: 8,
    color: {
      dark: "#171717",
      light: "#ffffff"
    }
  });
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet personal-share-sheet" role="dialog" aria-modal="true" aria-label="Поделиться">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <h2>Поделиться</h2>
      <p>QR или ссылка.</p>
      <img src="${escapeAttr(qr)}" alt="QR-код ${escapeAttr(profile.displayName)}" />
      <div class="personal-share-actions">
        <button type="button" data-copy>${icon("copy")} Скопировать</button>
        <button type="button" data-native>${icon("send")} Отправить</button>
      </div>
      ${ownSpace ? `
      <div class="personal-share-actions is-secondary">
        <button type="button" data-share-import>${icon("upload")} Импорт</button>
        <button type="button" data-share-export>${icon("download")} Экспорт</button>
      </div>
      ` : ""}
      <small data-share-note>${escapeHtml(shareUrl)}</small>
    </section>
  `;
  root.append(overlay);
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", () => overlay.remove());
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      overlay.remove();
    }
  });
  overlay.querySelector<HTMLElement>("[data-copy]")?.addEventListener("click", () => {
    void navigator.clipboard?.writeText(shareUrl).then(() => {
      const note = overlay.querySelector<HTMLElement>("[data-share-note]");
      if (note) {
        note.textContent = "Ссылка скопирована.";
      }
    });
  });
  overlay.querySelector<HTMLElement>("[data-native]")?.addEventListener("click", () => {
    if (navigator.share) {
      void navigator.share({ title: profile.displayName, url: shareUrl }).catch(() => undefined);
      return;
    }
    void navigator.clipboard?.writeText(shareUrl);
  });
  overlay.querySelector<HTMLElement>("[data-share-import]")?.addEventListener("click", () => {
    overlay.remove();
    root.querySelector<HTMLInputElement>("[data-backup-import]")?.click();
  });
  overlay.querySelector<HTMLElement>("[data-share-export]")?.addEventListener("click", () => {
    options.exportBackup();
    const note = overlay.querySelector<HTMLElement>("[data-share-note]");
    if (note) {
      note.textContent = "Экспорт готов.";
    }
  });
}

function closePersonalOverlay(root: HTMLElement): void {
  root.querySelector(".personal-overlay")?.remove();
}

function openPersonalRoute(handle: string): void {
  window.history.pushState({}, "", `/@${encodeURIComponent(handle)}`);
  window.dispatchEvent(new CustomEvent("soty-personal-routechange"));
}

async function updateProfilePhoto(root: HTMLElement, file: File, options: PersonalSpacePageOptions): Promise<void> {
  const note = root.querySelector<HTMLElement>("[data-install-note]");
  if (note) {
    note.textContent = "Готовлю фото страницы...";
  }
  try {
    await options.uploadPhoto(options.route, file);
    await renderPersonalSpacePage(root, options);
    const nextNote = root.querySelector<HTMLElement>("[data-install-note]");
    if (nextNote) {
      nextNote.textContent = "Фото сохранено. Оно станет иконкой PWA при установке или переустановке.";
    }
  } catch {
    if (note) {
      note.textContent = "Не получилось сохранить фото. Попробуйте другой снимок.";
    }
  }
}

function setActiveLayer(root: HTMLElement, layer: PersonalSpaceLayer): void {
  root.querySelector<HTMLElement>(".personal-space-shell")?.setAttribute("data-active-layer", layer);
  root.querySelectorAll<HTMLButtonElement>("[data-layer]").forEach((button) => {
    button.setAttribute("aria-selected", button.dataset.layer === layer ? "true" : "false");
  });
  root.querySelectorAll<HTMLElement>("[data-panel]").forEach((panel) => {
    panel.classList.toggle("is-active", panel.dataset.panel === layer);
  });
}

async function installPersonalSpace(root: HTMLElement, button: HTMLButtonElement, options: PersonalSpacePageOptions): Promise<void> {
  const note = root.querySelector<HTMLElement>("[data-install-note]");
  button.disabled = true;
  const result = await options.install();
  if (note) {
    note.textContent = result.message;
  }
  button.disabled = false;
}

function normalizeProfile(value: unknown, route: PersonalSpaceRoute): PersonalSpaceProfile {
  const record = isRecord(value) ? value : {};
  const actions = isRecord(record.actions) ? record.actions : {};
  return {
    kind: record.kind === "space" ? "space" : "entity",
    handle: cleanText(record.handle, 64) || route.handle,
    slug: cleanText(record.slug, 64) || route.slug,
    url: cleanUrlPath(record.url) || routeUrl(route),
    displayName: cleanText(record.displayName, 100) || route.handle,
    shortName: cleanText(record.shortName, 32) || route.handle,
    accountName: cleanText(record.accountName, 100) || cleanText(record.displayName, 100) || route.handle,
    photoUrl: cleanUrlPath(record.photoUrl),
    title: cleanText(record.title, 80) || fallbackProfile.title,
    headline: cleanText(record.headline, 180) || fallbackProfile.headline,
    about: cleanText(record.about, 420) || fallbackProfile.about,
    accent: cleanColor(record.accent) || fallbackProfile.accent,
    contacts: list(record.contacts).map(normalizeContact).filter(isContact).slice(0, 6),
    posts: list(record.posts).map(normalizePost).filter(isPost).slice(0, 8),
    reviews: list(record.reviews).map(normalizeReview).filter(isReview).slice(0, 8),
    spaces: list(record.spaces).map(normalizeSpaceLink).filter(isSpaceLink).slice(0, 12),
    actions: {
      messageUrl: cleanUrlPath(actions.messageUrl) || `/?pwa=1&bare=1&to=${encodeURIComponent(`@${route.handle}`)}`,
      runtimeUrl: cleanUrlPath(actions.runtimeUrl) || "/?pwa=1"
    }
  };
}

function normalizeContact(value: unknown): PersonalSpaceContact | null {
  if (!isRecord(value)) {
    return null;
  }
  const label = cleanText(value.label, 32);
  const contactValue = cleanText(value.value, 120);
  if (!label || !contactValue) {
    return null;
  }
  return {
    label,
    value: contactValue,
    ...(cleanUrlPath(value.href) ? { href: cleanUrlPath(value.href) } : {})
  };
}

function normalizePost(value: unknown): PersonalSpacePost | null {
  if (!isRecord(value)) {
    return null;
  }
  const title = cleanText(value.title, 120);
  const text = cleanText(value.text, 420);
  if (!title || !text) {
    return null;
  }
  return {
    id: cleanText(value.id, 80) || title,
    title,
    text,
    meta: cleanText(value.meta, 80)
  };
}

function normalizeReview(value: unknown): PersonalSpaceReview | null {
  if (!isRecord(value)) {
    return null;
  }
  const text = cleanText(value.text, 320);
  if (!text) {
    return null;
  }
  return {
    id: cleanText(value.id, 80) || text,
    author: cleanText(value.author, 80) || "Гость",
    text,
    rating: Number.isFinite(value.rating) ? Number(value.rating) : 5
  };
}

function normalizeSpaceLink(value: unknown): PersonalSpaceLink | null {
  if (!isRecord(value)) {
    return null;
  }
  const slug = cleanText(value.slug, 64);
  const title = cleanText(value.title, 80);
  if (!slug || !title) {
    return null;
  }
  return {
    slug,
    title,
    summary: cleanText(value.summary, 140),
    href: cleanUrlPath(value.href) || "#",
    active: value.active === true
  };
}

function fallbackFor(route: PersonalSpaceRoute): PersonalSpaceProfile {
  return {
    ...fallbackProfile,
    handle: route.handle,
    slug: route.slug,
    url: routeUrl(route),
    displayName: route.slug ? `${route.slug} · ${route.handle}` : route.handle,
    shortName: route.slug || route.handle,
    accountName: route.handle,
    photoUrl: "",
    actions: {
      messageUrl: `/?pwa=1&bare=1&to=${encodeURIComponent(`@${route.handle}`)}`,
      runtimeUrl: `/?pwa=1&space=${encodeURIComponent(routeUrl(route))}`
    }
  };
}

function isOwnProfile(profile: PersonalSpaceProfile, localHandle: string): boolean {
  return Boolean(localHandle) && profile.handle === localHandle;
}

function loadLocalHandle(): string {
  try {
    return cleanRoutePart(window.localStorage.getItem(localHandleKey) || "");
  } catch {
    return "";
  }
}

function saveLocalHandle(handle: string): void {
  try {
    window.localStorage.setItem(localHandleKey, handle);
  } catch {
    // Local storage is a convenience only; the current action can continue.
  }
}

function routeUrl(route: PersonalSpaceRoute): string {
  return route.slug ? `/@${route.handle}/${route.slug}` : `/@${route.handle}`;
}

function cleanRoutePart(value: string): string {
  try {
    return decodeURIComponent(value)
      .normalize("NFKC")
      .replace(/^@/u, "")
      .replace(/[^\p{L}\p{N}._-]+/gu, "-")
      .replace(/^-+|-+$/gu, "")
      .slice(0, 64)
      .toLowerCase();
  } catch {
    return "";
  }
}

function cleanText(value: unknown, max: number): string {
  return String(typeof value === "string" || typeof value === "number" ? value : "")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, max);
}

function cleanUrlPath(value: unknown): string {
  const text = cleanText(value, 240);
  if (!text) {
    return "";
  }
  if (/^(?:\/|https?:\/\/)/u.test(text) && !/[<>"']/u.test(text)) {
    return text;
  }
  return "";
}

function cleanColor(value: unknown): string {
  const text = cleanText(value, 24);
  return /^#[0-9a-f]{6}$/iu.test(text) ? text : "";
}

function list(value: unknown): readonly unknown[] {
  return Array.isArray(value) ? value : [];
}

async function fileToAvatarDataUrl(file: File): Promise<string> {
  if (!file.type.startsWith("image/")) {
    throw new Error("not an image");
  }
  const imageUrl = URL.createObjectURL(file);
  try {
    const image = await loadImage(imageUrl);
    const canvas = document.createElement("canvas");
    const size = 512;
    canvas.width = size;
    canvas.height = size;
    const context = canvas.getContext("2d");
    if (!context) {
      throw new Error("canvas unavailable");
    }
    context.fillStyle = "#f6f8fb";
    context.fillRect(0, 0, size, size);
    const sourceSize = Math.min(image.naturalWidth || image.width, image.naturalHeight || image.height);
    const sourceX = ((image.naturalWidth || image.width) - sourceSize) / 2;
    const sourceY = ((image.naturalHeight || image.height) - sourceSize) / 2;
    context.drawImage(image, sourceX, sourceY, sourceSize, sourceSize, 0, 0, size, size);
    return canvas.toDataURL("image/jpeg", 0.88);
  } finally {
    URL.revokeObjectURL(imageUrl);
  }
}

function loadImage(url: string): Promise<HTMLImageElement> {
  return new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () => resolve(image);
    image.onerror = () => reject(new Error("image load failed"));
    image.src = url;
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isContact(value: PersonalSpaceContact | null): value is PersonalSpaceContact {
  return Boolean(value);
}

function isPost(value: PersonalSpacePost | null): value is PersonalSpacePost {
  return Boolean(value);
}

function isReview(value: PersonalSpaceReview | null): value is PersonalSpaceReview {
  return Boolean(value);
}

function isSpaceLink(value: PersonalSpaceLink | null): value is PersonalSpaceLink {
  return Boolean(value);
}

function initials(value: string): string {
  const parts = value.trim().split(/\s+/u).filter(Boolean);
  const source = parts.length > 1 ? `${parts[0]?.[0] || ""}${parts[1]?.[0] || ""}` : value.slice(0, 2);
  return source.toLocaleUpperCase("ru-RU") || "С";
}

function escapeAttr(value: string): string {
  return escapeHtml(value).replace(/"/gu, "&quot;");
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/gu, "&amp;")
    .replace(/</gu, "&lt;")
    .replace(/>/gu, "&gt;")
    .replace(/"/gu, "&quot;")
    .replace(/'/gu, "&#39;");
}
