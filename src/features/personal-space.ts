import QRCode from "qrcode";
import { icon } from "../icons";

export type PersonalSpaceRoute = {
  readonly handle: string;
  readonly slug: string;
};

export type PersonalSpaceInstallResult = {
  readonly ok: boolean;
  readonly message: string;
};

export type PersonalSpacePageOptions = {
  readonly route: PersonalSpaceRoute;
  readonly canInstall: () => boolean;
  readonly install: () => Promise<PersonalSpaceInstallResult>;
  readonly uploadPhoto: (route: PersonalSpaceRoute, file: File) => Promise<string>;
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
  readonly kind: "person" | "space";
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
  kind: "person",
  handle: "guest",
  slug: "",
  url: "/@guest",
  displayName: "Соты",
  shortName: "Соты",
  accountName: "Соты",
  photoUrl: "",
  title: "личное пространство",
  headline: "визитка, личное место и приватная сота в одном простом экране",
  about: "Сначала QR открывает понятную карточку. Потом она растет в личное место, отзывы, сообщения и большое пространство.",
  accent: "#78e08f",
  contacts: [],
  posts: [],
  reviews: [],
  spaces: [],
  actions: {
    messageUrl: "/?pwa=1&bare=1",
    runtimeUrl: "/?pwa=1"
  }
};

const layerLabels = [
  ["card", "Визитка"],
  ["personal", "Личное"],
  ["reviews", "Отзывы"],
  ["messages", "Сообщения"],
  ["place", "Место"]
] as const;

type PersonalSpaceLayer = typeof layerLabels[number][0];
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
  root.innerHTML = renderPage(profile, activeLayer, options.canInstall(), localHandle);
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
      <div class="personal-orbit"></div>
      <main class="personal-loading">
        <span>@${escapeHtml(route.handle)}</span>
        <b>Открываю пространство</b>
      </main>
    </section>
  `;
}

function renderPage(profile: PersonalSpaceProfile, activeLayer: PersonalSpaceLayer, canInstall: boolean, localHandle: string): string {
  const ownSpace = isOwnProfile(profile, localHandle);
  const initialsText = initials(profile.shortName || profile.displayName);
  const avatar = profile.photoUrl
    ? `<img src="${escapeAttr(profile.photoUrl)}" alt="" />`
    : escapeHtml(initialsText);
  return `
    <section class="personal-space-shell" style="--personal-accent:${escapeAttr(profile.accent)}" data-active-layer="${activeLayer}">
      <div class="personal-orbit"></div>
      <header class="personal-topbar">
        <a href="/" class="personal-brand">соты</a>
        <nav aria-label="пространство">
          <button type="button" data-action="self">${localHandle ? "Я" : "Создать Я"}</button>
        </nav>
      </header>
      <main class="personal-main">
        <section class="personal-hero" aria-label="визитная карточка">
          ${ownSpace ? `<button class="personal-avatar${profile.photoUrl ? " has-photo" : ""}" type="button" data-action="photo" aria-label="Сделать фото профиля">` : `<div class="personal-avatar${profile.photoUrl ? " has-photo" : ""}" aria-hidden="true">`}
            ${avatar}
            ${ownSpace ? `<span>${icon("scan")} Фото</span>` : ""}
          ${ownSpace ? "</button>" : "</div>"}
          ${ownSpace ? `<input data-profile-photo type="file" accept="image/*" capture="user" hidden />` : ""}
          <div class="personal-identity">
            <span>@${escapeHtml(profile.handle)}${profile.slug ? ` / ${escapeHtml(profile.slug)}` : ""}</span>
            <h1>${escapeHtml(profile.displayName)}</h1>
            <p>${escapeHtml(profile.headline)}</p>
            <div class="personal-actions">
              ${ownSpace
                ? `<button class="personal-primary" type="button" data-action="share">${icon("qr")} Поделиться</button>
                   <button class="personal-secondary" type="button" data-action="install">${icon("install")} ${canInstall ? "Установить" : "Как установить"}</button>`
                : `<button class="personal-primary" type="button" data-action="install">${icon("install")} Сохранить контакт</button>
                   <button class="personal-secondary" type="button" data-action="message">${icon("mail")} Написать</button>`}
            </div>
            <div class="personal-note" data-install-note>${ownSpace
              ? "Поделитесь QR или ссылкой. Фото профиля станет иконкой PWA."
              : "Сохраните контакт: страница станет отдельной PWA, а переписка останется рядом."}</div>
          </div>
        </section>
        <div class="personal-layerbar" role="tablist" aria-label="слои пространства">
          ${layerLabels.map(([id, label]) => `
            <button type="button" role="tab" data-layer="${id}" aria-selected="${id === activeLayer ? "true" : "false"}">
              ${escapeHtml(label)}
            </button>
          `).join("")}
        </div>
        <section class="personal-panels">
          ${renderCardPanel(profile)}
          ${renderPersonalPanel(profile)}
          ${renderReviewsPanel(profile)}
          ${renderMessagesPanel(profile)}
          ${renderPlacePanel(profile)}
        </section>
      </main>
    </section>
  `;
}

function renderCardPanel(profile: PersonalSpaceProfile): string {
  return `
    <article class="personal-panel is-active" data-panel="card">
      <div class="personal-panel-copy">
        <span>первое касание</span>
        <h2>Сначала просто понятно, кто перед тобой.</h2>
        <p>${escapeHtml(profile.about)}</p>
      </div>
      <div class="personal-contact-list">
        ${profile.contacts.map((contact) => `
          ${contact.href ? `<a href="${escapeAttr(contact.href)}">` : "<div>"}
            <span>${escapeHtml(contact.label)}</span>
            <b>${escapeHtml(contact.value)}</b>
          ${contact.href ? "</a>" : "</div>"}
        `).join("")}
      </div>
    </article>
  `;
}

function renderPersonalPanel(profile: PersonalSpaceProfile): string {
  return `
    <article class="personal-panel" data-panel="personal">
      <div class="personal-panel-copy">
        <span>личное</span>
        <h2>Записи появляются только когда человеку есть что показать.</h2>
      </div>
      <div class="personal-feed">
        ${profile.posts.map((post) => `
          <section class="personal-post">
            <small>${escapeHtml(post.meta)}</small>
            <h3>${escapeHtml(post.title)}</h3>
            <p>${escapeHtml(post.text)}</p>
          </section>
        `).join("")}
      </div>
    </article>
  `;
}

function renderReviewsPanel(profile: PersonalSpaceProfile): string {
  return `
    <article class="personal-panel" data-panel="reviews">
      <div class="personal-panel-copy">
        <span>доверие</span>
        <h2>Отзывы живут рядом с визиткой, а не где-то в чужом сервисе.</h2>
      </div>
      <div class="personal-reviews">
        ${profile.reviews.map((review) => `
          <section class="personal-review">
            <div>${"★".repeat(Math.max(1, Math.min(5, review.rating)))}</div>
            <p>${escapeHtml(review.text)}</p>
            <b>${escapeHtml(review.author)}</b>
          </section>
        `).join("")}
      </div>
    </article>
  `;
}

function renderMessagesPanel(profile: PersonalSpaceProfile): string {
  return `
    <article class="personal-panel" data-panel="messages">
      <div class="personal-panel-copy">
        <span>личные сообщения</span>
        <h2>Кнопка "Написать" открывает простую личку, а под ней уже работает сота.</h2>
        <p>Файлы, доступы, агент и мини-приложения не торчат в визитке. Они появляются в диалоге, когда действительно нужны.</p>
      </div>
      <div class="personal-message-preview">
        <div><b>${escapeHtml(profile.shortName)}</b><p>Здравствуйте. Чем могу помочь?</p></div>
        <div><b>Вы</b><p>Хочу написать лично и при необходимости отправить файл.</p></div>
        <button type="button" data-action="message">${icon("send")} Открыть сообщения</button>
      </div>
    </article>
  `;
}

function renderPlacePanel(profile: PersonalSpaceProfile): string {
  return `
    <article class="personal-panel" data-panel="place">
      <div class="personal-panel-copy">
        <span>большое место</span>
        <h2>Из одной страницы можно вырастить магазин, клуб, сервис или команду.</h2>
      </div>
      <div class="personal-spaces">
        ${profile.spaces.map((space) => `
          <a href="${escapeAttr(space.href)}" data-space-link class="${space.active ? "is-active" : ""}">
            <span>${escapeHtml(space.title)}</span>
            <p>${escapeHtml(space.summary)}</p>
          </a>
        `).join("")}
      </div>
    </article>
  `;
}

function bindPersonalSpace(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): void {
  root.querySelectorAll<HTMLButtonElement>("[data-layer]").forEach((button) => {
    button.addEventListener("click", () => {
      const layer = button.dataset.layer as PersonalSpaceLayer | undefined;
      if (layer) {
        setActiveLayer(root, layer);
      }
    });
  });
  root.querySelectorAll<HTMLElement>("[data-action='message']").forEach((node) => {
    node.addEventListener("click", () => {
      const handle = loadLocalHandle();
      if (handle) {
        options.openMessage(profile, handle);
        return;
      }
      showNicknameSheet(root, {
        title: "Как вас подписать?",
        description: "Только ник. Без анкеты.",
        action: "Написать",
        onDone: (nextHandle) => options.openMessage(profile, nextHandle)
      });
    });
  });
  root.querySelectorAll<HTMLElement>("[data-action='runtime']").forEach((node) => {
    node.addEventListener("click", () => options.openRuntime(profile));
  });
  root.querySelectorAll<HTMLElement>("[data-action='self']").forEach((node) => {
    node.addEventListener("click", () => {
      const handle = loadLocalHandle();
      if (handle) {
        openPersonalRoute(handle);
        return;
      }
      showNicknameSheet(root, {
        title: "Ваш ник",
        description: "Он станет вашей ссылкой и QR.",
        action: "Создать Я",
        onDone: openPersonalRoute
      });
    });
  });
  root.querySelectorAll<HTMLElement>("[data-action='share']").forEach((node) => {
    node.addEventListener("click", () => {
      void showShareSheet(root, profile);
    });
  });
  const photoInput = root.querySelector<HTMLInputElement>("[data-profile-photo]");
  root.querySelector<HTMLElement>("[data-action='photo']")?.addEventListener("click", () => {
    photoInput?.click();
  });
  photoInput?.addEventListener("change", () => {
    const file = photoInput.files?.[0];
    if (file) {
      void updateProfilePhoto(root, file, options);
    }
    photoInput.value = "";
  });
  root.querySelectorAll<HTMLButtonElement>("[data-action='install']").forEach((button) => {
    button.addEventListener("click", () => {
      void installPersonalSpace(root, button, options);
    });
  });
  root.querySelectorAll<HTMLAnchorElement>("[data-space-link]").forEach((link) => {
    link.addEventListener("click", (event) => {
      event.preventDefault();
      window.history.pushState({}, "", link.href);
      window.dispatchEvent(new CustomEvent("soty-personal-routechange"));
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

async function showShareSheet(root: HTMLElement, profile: PersonalSpaceProfile): Promise<void> {
  closePersonalOverlay(root);
  const shareUrl = new URL(profile.url, window.location.origin).toString();
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
      <p>Покажите QR или отправьте ссылку.</p>
      <img src="${escapeAttr(qr)}" alt="QR-код ${escapeAttr(profile.displayName)}" />
      <div class="personal-share-actions">
        <button type="button" data-copy>${icon("copy")} Скопировать</button>
        <button type="button" data-native>${icon("send")} Отправить</button>
      </div>
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
    note.textContent = "Готовлю фото профиля...";
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
    kind: record.kind === "space" ? "space" : "person",
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
