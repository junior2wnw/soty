import { icon } from "../icons";
import type { IconName } from "../icons";
import { miniAppDefaultHeight, miniAppDefaultWidth, normalizeMiniAppLayout, safeMiniAppUrl } from "./mini-apps";
import type { MiniAppWindowLayout } from "./mini-apps";
import { runtimeModuleDefinitions } from "./runtime-modules";
import { showLinkShareSheet } from "./share-sheet";

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

export type PersonalSpaceMessageDraft = {
  readonly author: string;
  readonly text: string;
  readonly clientId: string;
};

export type PersonalSpaceInboxRequest = {
  readonly limit: number;
};

export type PersonalSpaceInboxMessage = {
  readonly id: string;
  readonly author: string;
  readonly text: string;
  readonly createdAt: string;
};

export type PersonalSpaceAgentRequest = {
  readonly text: string;
  readonly intent: "miniapp" | "page";
};

export type PersonalSpaceAgentResult = {
  readonly ok: boolean;
  readonly message: string;
  readonly reply: string;
};

export type PersonalOwnerAction = "profile" | "post" | "photo" | "module" | "messages";

export type PersonalOwnerActionPayload = {
  readonly v: 1;
  readonly kind: "soty.personal-space.owner-action";
  readonly action: PersonalOwnerAction;
  readonly handle: string;
  readonly slug: string;
  readonly bodyHash: string;
  readonly deviceId: string;
  readonly publicJwk: JsonWebKey;
  readonly createdAt: string;
  readonly nonce: string;
};

export type PersonalOwnerProof = {
  readonly payload: PersonalOwnerActionPayload;
  readonly signature: string;
};

export type PersonalOwnerProofFactory<TData> = (data: TData) => Promise<PersonalOwnerProof | null>;

export type PersonalSpacePageOptions = {
  readonly route: PersonalSpaceRoute;
  readonly canInstall: () => boolean;
  readonly canNotify: () => boolean;
  readonly install: () => Promise<PersonalSpaceInstallResult>;
  readonly enableNotifications: () => Promise<PersonalSpaceInstallResult>;
  readonly updateProfile: (route: PersonalSpaceRoute, update: PersonalSpaceProfileUpdate) => Promise<PersonalSpaceInstallResult>;
  readonly savePost: (route: PersonalSpaceRoute, draft: PersonalSpacePostDraft) => Promise<PersonalSpaceInstallResult>;
  readonly saveModule: (route: PersonalSpaceRoute, draft: PersonalSpaceModuleDraft) => Promise<PersonalSpaceInstallResult>;
  readonly loadInbox: (route: PersonalSpaceRoute) => Promise<readonly PersonalSpaceInboxMessage[]>;
  readonly askAgent: (profile: PersonalSpaceProfile, request: PersonalSpaceAgentRequest) => Promise<PersonalSpaceAgentResult>;
  readonly uploadPhoto: (route: PersonalSpaceRoute, file: File) => Promise<string>;
  readonly exportBackup: () => void;
  readonly importBackup: (file: File) => Promise<PersonalSpaceInstallResult>;
  readonly applyManifest: (profile: PersonalSpaceProfile) => void;
  readonly isOwned: (profile: PersonalSpaceProfile) => boolean | Promise<boolean>;
  readonly openRuntime: (profile: PersonalSpaceProfile, target?: string) => void;
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

type PersonalSpaceReactions = {
  readonly likes: number;
};

type PersonalSpaceLink = {
  readonly slug: string;
  readonly title: string;
  readonly summary: string;
  readonly href: string;
  readonly active: boolean;
};

type PersonalModuleVisibility = "public" | "trusted";
type PersonalSpaceCardModuleKind = "link" | "miniapp";

type PersonalSpaceCardModule = {
  readonly id: string;
  readonly kind: PersonalSpaceCardModuleKind;
  readonly title: string;
  readonly summary: string;
  readonly href: string;
  readonly visibility: PersonalModuleVisibility;
  readonly layout?: MiniAppWindowLayout;
};

export type PersonalSpaceModuleDraft = {
  readonly kind: PersonalSpaceCardModuleKind;
  readonly title: string;
  readonly summary: string;
  readonly href: string;
  readonly visibility: PersonalModuleVisibility;
  readonly layout?: MiniAppWindowLayout;
};

type PersonalThreadLine = {
  readonly id: string;
  readonly author: string;
  readonly text: string;
  readonly createdAt: string;
  readonly mine: boolean;
};

export type PersonalSpaceProfile = {
  readonly kind: "entity" | "space";
  readonly handle: string;
  readonly slug: string;
  readonly url: string;
  readonly ownerDeviceId: string;
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
  readonly reactions: PersonalSpaceReactions;
  readonly spaces: readonly PersonalSpaceLink[];
  readonly modules: readonly PersonalSpaceCardModule[];
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
  ownerDeviceId: "",
  displayName: "Соты",
  shortName: "Соты",
  accountName: "Соты",
  photoUrl: "",
  title: "страница",
  headline: "Визитка, отзывы, связь.",
  about: "Описание не указано.",
  accent: "#f1f1f1",
  contacts: [],
  posts: [],
  reviews: [],
  reactions: { likes: 0 },
  spaces: [],
  modules: [],
  actions: {
    messageUrl: "/?pwa=1&bare=1",
    runtimeUrl: "/?pwa=1"
  }
};

const layers = [
  { id: "card", label: "Визитка", title: "Визитка", icon: "person" },
  { id: "personal", label: "Я", title: "Я", icon: "hexagon" },
  { id: "reviews", label: "Отзывы", title: "Отзывы", icon: "heart" },
  { id: "messages", label: "Связь", title: "Связь", icon: "mail" },
  { id: "place", label: "Место", title: "Место", icon: "apps" }
] as const satisfies readonly {
  readonly id: string;
  readonly label: string;
  readonly title: string;
  readonly icon: IconName;
}[];

type PersonalSpaceLayer = typeof layers[number]["id"];
type PersonalLayerDisplay = {
  readonly id: PersonalSpaceLayer;
  readonly label: string;
  readonly title: string;
  readonly icon: IconName;
};
type EntityActionId = "edit" | "install" | "message" | "note" | "notifications" | "review" | "share" | "runtime";
type EntityActionSurface = "card" | "personal" | "reviews" | "messages" | "place";
type EntityAction = {
  readonly id: EntityActionId;
  readonly label: string;
  readonly icon: IconName;
  readonly tone: "primary" | "secondary";
};
type PersonalModule = {
  readonly id: string;
  readonly title: string;
  readonly summary: string;
  readonly icon: IconName;
  readonly kind: "action" | "runtime" | "space" | "custom" | "miniapp";
  readonly target: string;
  readonly layout?: MiniAppWindowLayout;
  readonly active?: boolean;
  readonly priority?: boolean;
  readonly visibility?: PersonalModuleVisibility;
};
type PersonalModuleGroup = {
  readonly title: string;
  readonly modules: readonly PersonalModule[];
  readonly priority?: boolean;
};
const localHandleKey = "soty:personal-handle:v1";
const legacyLocalHandleKeys = ["soty:self-start-handle:v1", "soty:handle:v1"];
const profileCachePrefix = "soty:personal-profile:v1:";
const personalThreadPrefix = "soty:personal-thread:v1:";
const personalMessageClientKey = "soty:personal-message-client:v1";
const personalReactionPrefix = "soty:personal-reaction:v1:";
const personalReactionClientKey = "soty:personal-reaction-client:v1";
const internalContactLabels = new Set(["чат", "страница"]);

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
  const previousLayer = previousActiveLayer(root, routeUrl(options.route));
  root.innerHTML = renderLoading(options.route);
  const profile = await loadPersonalSpaceProfile(options.route);
  options.applyManifest(profile);
  const activeLayer: PersonalSpaceLayer = requestedActiveLayer() || previousLayer || (options.route.slug ? "place" : "card");
  const localHandle = loadLocalHandle();
  const ownSpace = await options.isOwned(profile);
  root.innerHTML = renderPage(profile, activeLayer, options.canInstall(), options.canNotify(), localHandle, ownSpace);
  bindPersonalSpace(root, profile, options);
}

export async function uploadPersonalSpacePhoto(
  route: PersonalSpaceRoute,
  file: File,
  ownerProofForData?: PersonalOwnerProofFactory<{ readonly dataUrl: string }>
): Promise<string> {
  const dataUrl = await fileToAvatarDataUrl(file);
  const data = { dataUrl };
  const owner = await ownerProofForData?.(data) ?? null;
  const handle = encodeURIComponent(route.handle);
  const url = route.slug
    ? `/api/spaces/${handle}/${encodeURIComponent(route.slug)}/photo`
    : `/api/spaces/${handle}/photo`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify(owner ? { data, owner } : data)
    });
    if (!response.ok) {
      saveCachedPersonalProfile(applyLocalProfilePhoto(route, dataUrl));
      return dataUrl;
    }
    const payload = await response.json() as unknown;
    if (!isRecord(payload) || payload.ok !== true) {
      saveCachedPersonalProfile(applyLocalProfilePhoto(route, dataUrl));
      return dataUrl;
    }
    const photoUrl = cleanUrlPath(payload.photoUrl) || dataUrl;
    saveCachedPersonalProfile(applyLocalProfilePhoto(route, photoUrl));
    return photoUrl;
  } catch {
    saveCachedPersonalProfile(applyLocalProfilePhoto(route, dataUrl));
    return dataUrl;
  }
}

export function cleanPersonalHandle(value: string): string {
  return cleanRoutePart(value);
}

export function loadPersonalHandle(): string {
  const primary = readStoredHandle(localHandleKey);
  if (primary) {
    return primary;
  }
  for (const key of legacyLocalHandleKeys) {
    const legacy = readStoredHandle(key);
    if (legacy) {
      savePersonalHandle(legacy);
      return legacy;
    }
  }
  return "";
}

export function savePersonalHandle(handle: string): void {
  const clean = cleanPersonalHandle(handle);
  if (!clean) {
    return;
  }
  try {
    window.localStorage.setItem(localHandleKey, clean);
    window.localStorage.setItem("soty:self-start-handle:v1", clean);
  } catch {
    // Local storage is a convenience only; the current action can continue.
  }
}

export async function updatePersonalSpaceProfile(
  route: PersonalSpaceRoute,
  update: PersonalSpaceProfileUpdate,
  owner?: PersonalOwnerProof | null
): Promise<PersonalSpaceInstallResult> {
  saveCachedPersonalProfile(applyLocalProfileUpdate(route, update));
  const handle = encodeURIComponent(route.handle);
  try {
    const response = await fetch(`/api/spaces/${handle}/profile`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify(owner ? { data: update, owner } : update)
    });
    if (!response.ok) {
      return { ok: true, message: "Сохранено в браузере." };
    }
    const payload = await response.json() as unknown;
    return isRecord(payload) && payload.ok === true
      ? { ok: true, message: "Сохранено." }
      : { ok: true, message: "Сохранено в браузере." };
  } catch {
    return { ok: true, message: "Сохранено в браузере." };
  }
}

export async function savePersonalSpacePost(
  route: PersonalSpaceRoute,
  draft: PersonalSpacePostDraft,
  owner?: PersonalOwnerProof | null
): Promise<PersonalSpaceInstallResult> {
  saveCachedPersonalProfile(applyLocalPost(route, draft));
  const handle = encodeURIComponent(route.handle);
  try {
    const response = await fetch(`/api/spaces/${handle}/posts`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify(owner ? { data: draft, owner } : draft)
    });
    if (!response.ok) {
      return { ok: true, message: "Сохранено в браузере." };
    }
    const payload = await response.json() as unknown;
    return isRecord(payload) && payload.ok === true
      ? { ok: true, message: "Сохранено." }
      : { ok: true, message: "Сохранено в браузере." };
  } catch {
    return { ok: true, message: "Сохранено в браузере." };
  }
}

export async function savePersonalSpaceModule(
  route: PersonalSpaceRoute,
  draft: PersonalSpaceModuleDraft,
  owner?: PersonalOwnerProof | null
): Promise<PersonalSpaceInstallResult> {
  saveCachedPersonalProfile(applyLocalModule(route, draft));
  const handle = encodeURIComponent(route.handle);
  const url = route.slug
    ? `/api/spaces/${handle}/${encodeURIComponent(route.slug)}/modules`
    : `/api/spaces/${handle}/modules`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify(owner ? { data: draft, owner } : draft)
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => null) as unknown;
      if (isRecord(payload) && payload.error === "server_hosted_mini_apps_disabled") {
        return { ok: false, message: "Mini-app разместите вне сот." };
      }
      return { ok: true, message: "Сохранено в браузере." };
    }
    const payload = await response.json() as unknown;
    return isRecord(payload) && payload.ok === true
      ? { ok: true, message: "Сохранено." }
      : { ok: true, message: "Сохранено в браузере." };
  } catch {
    return { ok: true, message: "Сохранено в браузере." };
  }
}

export async function loadPersonalSpaceInbox(
  route: PersonalSpaceRoute,
  request: PersonalSpaceInboxRequest,
  owner?: PersonalOwnerProof | null
): Promise<readonly PersonalSpaceInboxMessage[]> {
  if (!owner) {
    return [];
  }
  const handle = encodeURIComponent(route.handle);
  const url = route.slug
    ? `/api/spaces/${handle}/${encodeURIComponent(route.slug)}/messages/inbox`
    : `/api/spaces/${handle}/messages/inbox`;
  try {
    const response = await fetch(url, {
      method: "POST",
      cache: "no-store",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify({ data: request, owner })
    });
    if (!response.ok) {
      return [];
    }
    const payload = await response.json() as unknown;
    if (!isRecord(payload) || payload.ok !== true) {
      return [];
    }
    return list(payload.messages).map(normalizeInboxMessage).filter(isInboxMessage).slice(0, 50);
  } catch {
    return [];
  }
}

async function sendPersonalSpaceMessage(profile: PersonalSpaceProfile, draft: PersonalSpaceMessageDraft): Promise<PersonalSpaceInstallResult> {
  const author = cleanText(draft.author, 80) || "guest";
  const text = cleanText(draft.text, 420);
  const clientId = cleanMessageClientId(draft.clientId);
  if (!text || !clientId) {
    return { ok: false, message: "Напишите пару слов." };
  }
  const handle = encodeURIComponent(profile.handle);
  const url = profile.slug
    ? `/api/spaces/${handle}/${encodeURIComponent(profile.slug)}/messages`
    : `/api/spaces/${handle}/messages`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify({ author, text, clientId })
    });
    if (!response.ok) {
      return { ok: false, message: "Не получилось отправить." };
    }
    const payload = await response.json() as unknown;
    return isRecord(payload) && payload.ok === true
      ? { ok: true, message: "Отправлено." }
      : { ok: false, message: "Не получилось отправить." };
  } catch {
    return { ok: false, message: "Не получилось отправить." };
  }
}

async function savePersonalReaction(profile: PersonalSpaceProfile): Promise<PersonalSpaceReactions> {
  const clientId = personalReactionClientId();
  const handle = encodeURIComponent(profile.handle);
  const url = profile.slug
    ? `/api/spaces/${handle}/${encodeURIComponent(profile.slug)}/reactions`
    : `/api/spaces/${handle}/reactions`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify({ type: "like", clientId })
    });
    if (!response.ok) {
      return profile.reactions;
    }
    const payload = await response.json() as unknown;
    if (!isRecord(payload) || payload.ok !== true) {
      return profile.reactions;
    }
    return normalizeReactions(payload.reactions);
  } catch {
    return profile.reactions;
  }
}

async function loadPersonalSpaceProfile(route: PersonalSpaceRoute): Promise<PersonalSpaceProfile> {
  const handle = encodeURIComponent(route.handle);
  const url = route.slug
    ? `/api/spaces/${handle}/${encodeURIComponent(route.slug)}`
    : `/api/spaces/${handle}`;
  const cached = loadCachedPersonalProfile(route);
  if (cached) {
    void refreshPersonalSpaceProfile(route, url);
    return cached;
  }
  const fetched = await fetchPersonalSpaceProfile(route, url);
  if (fetched) {
    return fetched;
  }
  const fallback = fallbackFor(route);
  if (route.handle === loadLocalHandle()) {
    saveCachedPersonalProfile(fallback);
  }
  return fallback;
}

async function refreshPersonalSpaceProfile(route: PersonalSpaceRoute, url: string): Promise<void> {
  await fetchPersonalSpaceProfile(route, url);
}

async function fetchPersonalSpaceProfile(route: PersonalSpaceRoute, url: string): Promise<PersonalSpaceProfile | null> {
  try {
    const response = await fetch(url, {
      cache: "no-store",
      headers: { Accept: "application/json" }
    });
    if (!response.ok) {
      return null;
    }
    const profile = mergeLocalProfile(route, normalizeProfile(await response.json(), route));
    saveCachedPersonalProfile(profile);
    return profile;
  } catch {
    return null;
  }
}

function loadCachedPersonalProfile(route: PersonalSpaceRoute): PersonalSpaceProfile | null {
  try {
    const text = window.localStorage.getItem(profileCacheKey(route));
    if (!text) {
      return null;
    }
    const profile = normalizeProfile(JSON.parse(text) as unknown, route);
    return profile.handle === route.handle && profile.slug === route.slug ? profile : null;
  } catch {
    return null;
  }
}

function saveCachedPersonalProfile(profile: PersonalSpaceProfile): void {
  try {
    window.localStorage.setItem(profileCacheKey(profile), JSON.stringify(profile));
  } catch {
    // Local storage only improves continuity; failing to cache must not block the page.
  }
}

function profileCacheKey(route: PersonalSpaceRoute): string {
  return `${profileCachePrefix}${routeUrl(route)}`;
}

function localBaseProfile(route: PersonalSpaceRoute): PersonalSpaceProfile {
  return loadCachedPersonalProfile(route) || fallbackFor(route);
}

function mergeLocalProfile(route: PersonalSpaceRoute, fetched: PersonalSpaceProfile): PersonalSpaceProfile {
  const cached = loadCachedPersonalProfile(route);
  if (!cached) {
    return fetched;
  }
  const defaultDisplayName = defaultPersonalDisplayName(route);
  const fetchedUsesDefaultName = fetched.displayName === route.handle || fetched.displayName === defaultDisplayName;
  const cachedUsesDefaultName = cached.displayName === route.handle || cached.displayName === defaultDisplayName;
  const displayName = fetchedUsesDefaultName && !cachedUsesDefaultName
    ? cached.displayName
    : fetched.displayName;
  const about = isDefaultPersonalText(fetched.about) && !isDefaultPersonalText(cached.about)
    ? cached.about
    : fetched.about;
  const headline = isDefaultPersonalText(fetched.headline) && !isDefaultPersonalText(cached.headline)
    ? cached.headline
    : fetched.headline;
  return {
    ...fetched,
    ownerDeviceId: fetched.ownerDeviceId || cached.ownerDeviceId,
    displayName,
    shortName: displayName,
    accountName: displayName,
    photoUrl: fetched.photoUrl || cached.photoUrl,
    headline,
    about,
    contacts: mergeByKey(fetched.contacts, cached.contacts, contactKey).slice(0, 6),
    posts: mergeByKey(fetched.posts, cached.posts, (post) => post.id).slice(0, 8),
    reviews: mergeByKey(fetched.reviews, cached.reviews, (review) => review.id).slice(0, 8),
    reactions: { likes: Math.max(fetched.reactions.likes, cached.reactions.likes) },
    modules: mergeByKey(fetched.modules, cached.modules, (module) => module.id).slice(0, 16)
  };
}

function applyLocalProfileUpdate(route: PersonalSpaceRoute, update: PersonalSpaceProfileUpdate): PersonalSpaceProfile {
  const profile = localBaseProfile(route);
  const displayName = cleanText(update.displayName, 100) || profile.displayName;
  const about = cleanText(update.about, 420) || profile.about;
  const contact = cleanText(update.contact, 160);
  const contacts = contact
    ? [
        { label: "контакт", value: contact, ...(contactHref(contact) ? { href: contactHref(contact) } : {}) },
        ...profile.contacts.filter((item) => item.label !== "контакт")
      ].slice(0, 6)
    : profile.contacts.filter((item) => item.label !== "контакт");
  return {
    ...profile,
    displayName,
    shortName: displayName,
    accountName: displayName,
    about,
    contacts
  };
}

function applyLocalProfilePhoto(route: PersonalSpaceRoute, photoUrl: string): PersonalSpaceProfile {
  return {
    ...localBaseProfile(route),
    photoUrl
  };
}

function applyLocalPost(route: PersonalSpaceRoute, draft: PersonalSpacePostDraft): PersonalSpaceProfile {
  const profile = localBaseProfile(route);
  const text = cleanText(draft.text, 420);
  if (!text) {
    return profile;
  }
  const post = {
    id: localItemId("post"),
    title: text.slice(0, 72),
    text,
    meta: localMeta()
  };
  return {
    ...profile,
    posts: [post, ...profile.posts].slice(0, 8)
  };
}

function applyLocalReview(profile: PersonalSpaceProfile, author: string, text: string): PersonalSpaceProfile {
  const review = {
    id: localItemId("review"),
    author: cleanText(author, 80) || "Гость",
    text: cleanText(text, 320),
    rating: 5
  };
  if (!review.text) {
    return profile;
  }
  return {
    ...profile,
    reviews: [review, ...profile.reviews].slice(0, 8)
  };
}

function applyLocalReaction(profile: PersonalSpaceProfile): PersonalSpaceProfile {
  return {
    ...profile,
    reactions: {
      likes: Math.max(0, profile.reactions.likes) + 1
    }
  };
}

function applyLocalModule(route: PersonalSpaceRoute, draft: PersonalSpaceModuleDraft): PersonalSpaceProfile {
  const profile = localBaseProfile(route);
  const title = cleanText(draft.title, 80);
  if (!title) {
    return profile;
  }
  const module = normalizeCardModule({
    id: localItemId("module"),
    kind: draft.kind,
    title,
    summary: draft.summary,
    href: draft.href,
    visibility: draft.visibility,
    layout: draft.layout
  });
  if (!module) {
    return profile;
  }
  return {
    ...profile,
    modules: [module, ...profile.modules].slice(0, 16)
  };
}

function mergeByKey<T>(primary: readonly T[], secondary: readonly T[], keyFor: (value: T) => string): readonly T[] {
  const seen = new Set<string>();
  const next: T[] = [];
  for (const item of [...primary, ...secondary]) {
    const key = keyFor(item);
    if (!key || seen.has(key)) {
      continue;
    }
    seen.add(key);
    next.push(item);
  }
  return next;
}

function contactKey(contact: PersonalSpaceContact): string {
  return `${contact.label}:${contact.value}:${contact.href || ""}`;
}

function contactHref(value: string): string {
  if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/u.test(value)) {
    return `mailto:${value}`;
  }
  if (/^https?:\/\//iu.test(value)) {
    return value;
  }
  return "";
}

function localItemId(kind: string): string {
  return `${kind}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function localMeta(): string {
  return new Date().toLocaleDateString("ru-RU", { day: "2-digit", month: "short", year: "numeric" });
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

function renderPage(profile: PersonalSpaceProfile, activeLayer: PersonalSpaceLayer, canInstall: boolean, canNotify: boolean, localHandle: string, ownSpace: boolean): string {
  const initialsText = initials(profile.shortName || profile.displayName);
  const avatar = profile.photoUrl
    ? `<img src="${escapeAttr(profile.photoUrl)}" alt="" />`
    : escapeHtml(initialsText);
  const heroText = heroLine(profile, ownSpace);
  return `
    <section class="personal-space-shell" style="--personal-accent:${escapeAttr(profile.accent)}" data-active-layer="${activeLayer}" data-owned="${ownSpace ? "true" : "false"}" data-route="${escapeAttr(profile.url)}">
      <header class="personal-topbar">
        <a href="/" class="personal-brand">соты</a>
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
            <p>${escapeHtml(heroText)}</p>
            ${renderReactionPulse(profile, ownSpace)}
            ${renderQuickContacts(profile, ownSpace)}
            <div class="personal-note" data-install-note></div>
          </div>
        </section>
        <div class="personal-layerbar" role="tablist" aria-label="слои инфо-карты">
          ${layers.map((display) => {
            const view = layerDisplay(display, ownSpace);
            return `
            <button type="button" role="tab" data-layer="${display.id}" data-tooltip="off" aria-label="${escapeAttr(view.title)}" aria-selected="${display.id === activeLayer ? "true" : "false"}">
              ${icon(view.icon)}
              <span>${escapeHtml(view.label)}</span>
            </button>
          `;
          }).join("")}
        </div>
        <section class="personal-panels" aria-label="инфо-карта">
          ${layers.map((layer) => renderLayerPanel(profile, layer.id, ownSpace, activeLayer, canInstall, canNotify, localHandle)).join("")}
        </section>
      </main>
    </section>
  `;
}

function renderReactionPulse(profile: PersonalSpaceProfile, ownSpace: boolean): string {
  const liked = hasLocalReaction(profile);
  const likes = reactionCountForDisplay(profile);
  if (ownSpace) {
    return likes > 0
      ? `<div class="personal-reaction is-static" aria-label="Нравится ${likes}">${icon("heart")} <span>${escapeHtml(formatReactionCount(likes))}</span></div>`
      : "";
  }
  return `
    <button class="personal-reaction${liked ? " is-liked" : ""}" type="button" data-action="react" aria-pressed="${liked ? "true" : "false"}" aria-label="${liked ? "Уже нравится" : "Нравится"}">
      ${icon("heart")}
      <span>${escapeHtml(likes > 0 ? formatReactionCount(likes) : "Нравится")}</span>
    </button>
  `;
}

function renderQuickContacts(profile: PersonalSpaceProfile, ownSpace: boolean): string {
  const contacts = visibleContacts(profile, ownSpace)
    .filter((contact) => !internalContactLabels.has(contact.label))
    .slice(0, 2);
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
  readonly actions?: readonly EntityAction[];
  readonly body: string;
};

function renderLayerPanel(
  profile: PersonalSpaceProfile,
  layer: PersonalSpaceLayer,
  ownSpace: boolean,
  activeLayer: PersonalSpaceLayer,
  canInstall: boolean,
  canNotify: boolean,
  localHandle: string
): string {
  const view = panelView(profile, layer, ownSpace, canInstall, canNotify, localHandle);
  return `
    <article class="personal-panel${layer === activeLayer ? " is-active" : ""}" data-panel="${layer}">
      <div class="personal-panel-copy">
        <span>${escapeHtml(view.eyebrow)}</span>
        <h2>${escapeHtml(view.title)}</h2>
        ${view.text ? `<p>${escapeHtml(view.text)}</p>` : ""}
        ${renderEntityActions(view.actions || [])}
      </div>
      ${view.body}
    </article>
  `;
}

function panelView(
  profile: PersonalSpaceProfile,
  layer: PersonalSpaceLayer,
  ownSpace: boolean,
  canInstall: boolean,
  canNotify: boolean,
  localHandle: string
): PersonalPanelView {
  if (layer === "personal") {
    const actions = entityActionsFor({ surface: "personal", ownSpace, canInstall: false });
    return {
      eyebrow: ownSpace ? "я" : "страница",
      title: "Записи",
      ...(actions.length ? { actions } : {}),
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
        ownSpace ? "Пока пусто." : "Пока нет записей."
      )
    };
  }
  if (layer === "reviews") {
    const actions = entityActionsFor({ surface: "reviews", ownSpace, canInstall: false });
    return {
      eyebrow: "отзывы",
      title: "Отзывы",
      ...(actions.length ? { actions } : {}),
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
    const actions = entityActionsFor({ surface: "messages", ownSpace, canInstall: false, canNotify });
    return {
      eyebrow: "связь",
      title: ownSpace ? "Заметки" : "Связь",
      ...(actions.length ? { actions } : {}),
      body: renderMessagePreview(profile, ownSpace, localHandle)
    };
  }
  if (layer === "place") {
    return {
      eyebrow: "место",
      title: "Место",
      body: renderSpaceModules(profile, ownSpace)
    };
  }
  return {
    eyebrow: "визитка",
    title: "Визитка",
    text: cardText(profile, ownSpace),
    actions: entityActionsFor({ surface: "card", ownSpace, canInstall }),
    body: renderContactList(profile, ownSpace)
  };
}

function renderContactList(profile: PersonalSpaceProfile, ownSpace: boolean): string {
  return renderPanelList(
    "personal-contact-list",
    visibleContacts(profile, ownSpace).map((contact) => `
      ${contact.href ? `<a href="${escapeAttr(contact.href)}">` : "<div>"}
        <span>${escapeHtml(contact.label)}</span>
        <b>${escapeHtml(contact.value)}</b>
      ${contact.href ? "</a>" : "</div>"}
    `),
    "person",
    ownSpace ? "Добавьте контакт." : "Контакт не указан."
  );
}

function renderMessagePreview(profile: PersonalSpaceProfile, ownSpace: boolean, localHandle: string): string {
  const actor = cleanRoutePart(localHandle || profile.handle) || "guest";
  const lines = loadPersonalThread(profile, actor).slice(-3);
  return `
    <div class="personal-message-preview">
      <div class="personal-message-copy">
        <b>${escapeHtml(ownSpace ? profile.shortName : profile.displayName)}</b>
        <p>${escapeHtml(ownSpace ? "Личные заметки в этой карточке." : "Личная переписка по этой карточке.")}</p>
      </div>
      ${ownSpace ? renderOwnerInboxShell() : ""}
      ${renderThreadLines(lines, ownSpace, true)}
      <small data-action-note></small>
    </div>
  `;
}

function renderOwnerInboxShell(): string {
  return `
    <section class="personal-inbox" data-personal-inbox aria-label="Входящие">
      <h3>Входящие</h3>
      <div class="personal-inbox-list">
        <section class="personal-empty">${icon("mail")} <span>Проверяю...</span></section>
      </div>
    </section>
  `;
}

function entityActionsFor(options: { readonly surface: EntityActionSurface; readonly ownSpace: boolean; readonly canInstall: boolean; readonly canNotify?: boolean }): readonly EntityAction[] {
  if (options.surface === "card") {
    if (options.ownSpace) {
      return [
        { id: "share", label: "Поделиться", icon: "qr", tone: "primary" },
        ...(options.canInstall ? [{ id: "install", label: "Сохранить", icon: "install", tone: "secondary" } as const] : []),
        { id: "edit", label: "Править", icon: "person", tone: "secondary" }
      ];
    }
    return [
      ...(options.canInstall ? [{ id: "install", label: "Сохранить", icon: "install", tone: "primary" } as const] : []),
      { id: "message", label: "Написать", icon: "mail", tone: options.canInstall ? "secondary" : "primary" },
      { id: "share", label: "Поделиться", icon: "qr", tone: "secondary" }
    ];
  }
  if (options.surface === "personal") {
    return options.ownSpace ? [{ id: "note", label: "Записать", icon: "hexagon", tone: "primary" }] : [];
  }
  if (options.surface === "reviews") {
    return options.ownSpace ? [] : [{ id: "review", label: "Отзыв", icon: "heart", tone: "primary" }];
  }
  if (options.surface === "messages") {
    return [
      { id: "message", label: options.ownSpace ? "Заметка" : "Написать", icon: options.ownSpace ? "hexagon" : "send", tone: "primary" }
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

function renderEntityAction(action: EntityAction, className?: string): string {
  const buttonClass = className || (action.tone === "secondary" ? "personal-secondary" : "personal-primary");
  return `<button class="${escapeAttr(buttonClass)}" type="button" data-action="${action.id}">${icon(action.icon)} ${escapeHtml(action.label)}</button>`;
}

function renderSpaceModules(profile: PersonalSpaceProfile, ownSpace: boolean): string {
  const groups = personalModuleGroups(profile, ownSpace);
  return `
    <div class="personal-spaces">
      ${groups.map(renderPersonalModuleGroup).join("")}
    </div>
  `;
}

function personalModuleGroups(profile: PersonalSpaceProfile, ownSpace: boolean): readonly PersonalModuleGroup[] {
  const runtimeModules: readonly PersonalModule[] = runtimeModuleDefinitions.map((module) => ({
    ...module,
    kind: "runtime" as const,
    priority: module.id === "agent"
  }));
  const runtimeById = (id: string): PersonalModule[] => runtimeModules.filter((module) => module.id === id);
  const customModules: readonly PersonalModule[] = profile.modules
    .filter((module) => ownSpace || module.visibility === "public")
    .map((module) => ({
      id: `custom:${module.id}`,
      title: module.title,
      summary: module.summary || visibilityLabel(module.visibility),
      icon: module.kind === "miniapp" ? "apps" as const : module.visibility === "trusted" ? "shield" as const : "apps" as const,
      kind: module.kind === "miniapp" ? "miniapp" as const : "custom" as const,
      target: module.href,
      ...(module.kind === "miniapp" ? { layout: module.layout || "large" as const } : {}),
      visibility: module.visibility
    }));
  const addModule: PersonalModule = {
    id: "module:add",
    title: "Модуль",
    summary: "добавить",
    icon: "apps",
    kind: "action",
    target: "module"
  };
  const childSpaces: readonly PersonalModule[] = profile.spaces.map((space) => ({
    id: `space:${space.slug}`,
    title: space.title,
    summary: space.summary,
    icon: "hexagon" as const,
    kind: "space" as const,
    target: space.href,
    active: space.active
  }));
  const dataModules: readonly PersonalModule[] = ownSpace
    ? [{
      id: "data",
      title: "Данные",
      summary: "импорт / экспорт",
      icon: "download" as const,
      kind: "action" as const,
      target: "data"
    }]
    : [];
  const priorityModules = ownSpace ? [...runtimeById("agent"), addModule, ...dataModules] : [];
  return [
    ...(priorityModules.length ? [{ title: "Главное", modules: priorityModules, priority: true }] : []),
    ...(customModules.length ? [{ title: "Модули", modules: customModules }] : []),
    ...(childSpaces.length ? [{ title: "Пространства", modules: childSpaces }] : [])
  ];
}

function renderPersonalModuleGroup(group: PersonalModuleGroup): string {
  return `
    <section class="personal-module-group${group.priority ? " is-priority" : ""}" aria-label="${escapeAttr(group.title)}">
      <h3>${escapeHtml(group.title)}</h3>
      <div class="personal-module-grid">
        ${group.modules.map(renderPersonalModule).join("")}
      </div>
    </section>
  `;
}

function layerDisplay(layer: typeof layers[number], ownSpace: boolean): PersonalLayerDisplay {
  if (!ownSpace && layer.id === "personal") {
    return { ...layer, label: "Страница", title: "Страница" };
  }
  return layer;
}

function renderPersonalModule(module: PersonalModule): string {
  return `
    <button type="button" data-module-kind="${module.kind}" data-module-target="${escapeAttr(module.target)}" data-module-layout="${escapeAttr(module.layout || "")}" class="${[module.active ? "is-active" : "", module.priority ? "is-priority" : ""].filter(Boolean).join(" ")}">
      ${icon(module.icon)}
      <span>${escapeHtml(module.title)}</span>
      <p>${escapeHtml(module.summary)}</p>
      ${module.visibility ? `<small>${escapeHtml(visibilityLabel(module.visibility))}</small>` : ""}
    </button>
  `;
}

function visibilityLabel(visibility: PersonalModuleVisibility): string {
  return visibility === "trusted" ? "по списку" : "всем";
}

function visibleContacts(profile: PersonalSpaceProfile, ownSpace: boolean): readonly PersonalSpaceContact[] {
  void ownSpace;
  return profile.contacts.filter((contact) => !internalContactLabels.has(contact.label));
}

function personalSpaceCopy(text: string, ownSpace: boolean): string {
  void ownSpace;
  return text
    .replaceAll("Чат", "Связь")
    .replaceAll("чат", "связь");
}

function heroLine(profile: PersonalSpaceProfile, ownSpace: boolean): string {
  const text = personalSpaceCopy(profile.headline, ownSpace);
  if (isDefaultPersonalText(text)) {
    return ownSpace ? "Визитка, записи, отзывы, связь." : "Контакты, отзывы, связь.";
  }
  return text;
}

function cardText(profile: PersonalSpaceProfile, ownSpace: boolean): string {
  const text = personalSpaceCopy(profile.about, ownSpace);
  if (isDefaultPersonalText(text)) {
    return ownSpace ? "Добавьте описание и контакт." : "Описание не указано.";
  }
  return text;
}

function isDefaultPersonalText(value: string): boolean {
  const text = value.trim().toLowerCase();
  return !text
    || text === "визитка, записи, отзывы, связь"
    || text === "визитка, отзывы, связь."
    || text === "описание не указано."
    || text === "пока без описания."
    || text === "контактная страница.";
}

function defaultPersonalDisplayName(route: PersonalSpaceRoute): string {
  const ownerName = titleFromRoutePart(route.handle);
  if (!route.slug) {
    return ownerName;
  }
  return `${defaultSpaceTitle(route.slug)} · ${ownerName}`;
}

function defaultSpaceTitle(slug: string): string {
  if (slug === "work") {
    return "Работа";
  }
  if (slug === "home") {
    return "Дом";
  }
  if (slug === "club") {
    return "Клуб";
  }
  return titleFromRoutePart(slug);
}

function titleFromRoutePart(value: string): string {
  return value
    .replace(/[-_.]+/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .replace(/^\p{Ll}/u, (char) => char.toLocaleUpperCase("ru-RU")) || "Соты";
}

function previousActiveLayer(root: HTMLElement, routeUrl: string): PersonalSpaceLayer | null {
  const shell = root.querySelector<HTMLElement>(".personal-space-shell");
  if (!shell || shell.dataset.route !== routeUrl) {
    return null;
  }
  const layer = shell.dataset.activeLayer || "";
  return isPersonalLayer(layer) ? layer : null;
}

function requestedActiveLayer(): PersonalSpaceLayer | null {
  try {
    const layer = new URL(window.location.href).searchParams.get("layer") || "";
    return isPersonalLayer(layer) ? layer : null;
  } catch {
    return null;
  }
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
      const layer = layerButton.dataset.layer || "";
      if (isPersonalLayer(layer)) {
        setActiveLayer(root, layer);
      }
      return;
    }
    const moduleNode = target?.closest<HTMLElement>("[data-module-kind]");
    if (moduleNode && shell.contains(moduleNode)) {
      openPersonalModule(root, moduleNode, profile, options);
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
  if (isRenderedOwnSpace(root)) {
    void hydrateOwnerInbox(root, options);
  }
}

function openPersonalModule(root: HTMLElement, node: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): void {
  const kind = node.dataset.moduleKind;
  const target = node.dataset.moduleTarget || "";
  if (kind === "action" && target === "data") {
    showDataSheet(root, options);
    return;
  }
  if (kind === "action" && target === "module") {
    showModuleSheet(root, options);
    return;
  }
  if (kind === "space" && target) {
    window.history.pushState({}, "", target);
    window.dispatchEvent(new CustomEvent("soty-personal-routechange"));
    return;
  }
  if (kind === "miniapp") {
    showPersonalMiniAppSheet(root, {
      title: node.querySelector("span")?.textContent || "Mini-app",
      target,
      layout: normalizeMiniAppLayout(node.dataset.moduleLayout || "large")
    });
    return;
  }
  if (kind === "custom") {
    if (target) {
      window.location.assign(target);
    }
    return;
  }
  if (kind === "runtime" && target === "qr") {
    void showShareSheet(root, profile);
    return;
  }
  if (kind === "runtime" && target === "agent") {
    showAgentSheet(root, profile, options);
    return;
  }
  if (kind === "runtime") {
    showRuntimeModuleSheet(root, profile, target, options);
  }
}

function showPersonalMiniAppSheet(
  root: HTMLElement,
  module: { readonly title: string; readonly target: string; readonly layout: MiniAppWindowLayout }
): void {
  closePersonalOverlay(root);
  const url = safeMiniAppUrl(module.target, { baseUrl: window.location.href });
  if (!url || isServerHostedMiniAppUrl(url)) {
    const overlay = document.createElement("div");
    overlay.className = "personal-overlay";
    overlay.innerHTML = `
      <section class="personal-sheet" role="dialog" aria-modal="true" aria-label="Mini-app">
        <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
        <h2>Mini-app</h2>
        <p>Разместите mini-app вне сот.</p>
      </section>
    `;
    root.append(overlay);
    overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", () => overlay.remove());
    overlay.addEventListener("click", (event) => {
      if (event.target === overlay) {
        overlay.remove();
      }
    });
    return;
  }
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay personal-mini-app-overlay";
  const height = miniAppDefaultHeight(module.layout);
  const width = miniAppDefaultWidth(module.layout);
  overlay.innerHTML = `
    <section class="personal-sheet personal-mini-app-sheet" data-mini-app-layout="${escapeAttr(module.layout)}" role="dialog" aria-modal="true" aria-label="${escapeAttr(module.title)}">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <h2>${escapeHtml(module.title)}</h2>
      <div class="personal-mini-app-frame" style="--mini-app-height:${escapeAttr(height)};--mini-app-width:${escapeAttr(width)}">
        <iframe title="${escapeAttr(module.title)}" src="${escapeAttr(url)}" loading="lazy" referrerpolicy="no-referrer" sandbox="allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox allow-downloads allow-same-origin"></iframe>
      </div>
    </section>
  `;
  root.append(overlay);
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", () => overlay.remove());
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      overlay.remove();
    }
  });
}

function handleEntityAction(root: HTMLElement, node: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): void {
  const action = node.dataset.action;
  if (action === "react" && node instanceof HTMLButtonElement) {
    void reactToPersonalSpace(root, node, profile);
    return;
  }
  if (action === "message") {
    const handle = loadLocalHandle();
    if (handle) {
      showMessageSheet(root, profile, handle, options);
      return;
    }
    showNicknameSheet(root, {
      title: "Ваше имя",
      description: "",
      action: "Написать",
      onDone: (nextHandle) => showMessageSheet(root, profile, nextHandle, options)
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
      title: "Ваше имя",
      description: "",
      action: "Оставить",
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
      description: "",
      action: "Открыть",
      onDone: openPersonalRoute
    });
    return;
  }
  if (action === "share") {
    void showShareSheet(root, profile);
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

async function reactToPersonalSpace(root: HTMLElement, button: HTMLButtonElement, profile: PersonalSpaceProfile): Promise<void> {
  if (hasLocalReaction(profile)) {
    updateReactionButton(button, reactionCountForDisplay(profile), true);
    return;
  }
  const nextProfile = applyLocalReaction(profile);
  markLocalReaction(profile);
  saveCachedPersonalProfile(nextProfile);
  updateReactionButton(button, reactionCountForDisplay(nextProfile), true);
  const reactions = await savePersonalReaction(nextProfile);
  const syncedProfile = {
    ...nextProfile,
    reactions: {
      likes: Math.max(reactionCountForDisplay(nextProfile), reactions.likes)
    }
  };
  saveCachedPersonalProfile(syncedProfile);
  const currentButton = root.querySelector<HTMLButtonElement>("[data-action='react']");
  if (currentButton) {
    updateReactionButton(currentButton, reactionCountForDisplay(syncedProfile), true);
  }
}

function updateReactionButton(button: HTMLButtonElement, likes: number, liked: boolean): void {
  button.classList.toggle("is-liked", liked);
  button.setAttribute("aria-pressed", liked ? "true" : "false");
  button.setAttribute("aria-label", liked ? "Уже нравится" : "Нравится");
  button.innerHTML = `${icon("heart")} <span>${escapeHtml(formatReactionCount(likes))}</span>`;
}

async function importPersonalBackup(root: HTMLElement, file: File, options: PersonalSpacePageOptions): Promise<void> {
  const note = root.querySelector<HTMLElement>("[data-data-note]")
    || root.querySelector<HTMLElement>("[data-install-note]");
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

function showMessageSheet(root: HTMLElement, profile: PersonalSpaceProfile, author: string, options: PersonalSpacePageOptions): void {
  closePersonalOverlay(root);
  const actor = cleanRoutePart(author) || "guest";
  const ownSpace = isRenderedOwnSpace(root);
  let lines = loadPersonalThread(profile, actor);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet personal-message-sheet" role="dialog" aria-modal="true" aria-label="${escapeAttr(ownSpace ? "Заметки" : "Связь")}">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <h2>${escapeHtml(ownSpace ? "Заметки" : profile.shortName)}</h2>
      <p>${escapeHtml(ownSpace ? "Только для себя." : "Личная связь.")}</p>
      ${renderThreadLines(lines, ownSpace, false)}
      <form data-message-form>
        <textarea name="text" maxlength="420" required placeholder="${escapeAttr(ownSpace ? "Короткая заметка" : "Сообщение")}"></textarea>
        <button type="submit">${icon("send")} ${escapeHtml(ownSpace ? "Записать" : "Отправить")}</button>
      </form>
      <small data-error></small>
    </section>
  `;
  root.append(overlay);
  const textarea = overlay.querySelector<HTMLTextAreaElement>("textarea[name='text']");
  textarea?.focus();
  const close = () => {
    overlay.remove();
    void renderPersonalSpacePage(root, options).then(() => setActiveLayer(root, "messages"));
  };
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", close);
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      close();
    }
  });
  overlay.querySelector<HTMLFormElement>("[data-message-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    void submitPersonalMessage();
  });
  async function submitPersonalMessage(): Promise<void> {
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
    if (!ownSpace) {
      const result = await sendPersonalSpaceMessage(profile, {
        author: actor,
        text,
        clientId: personalMessageClientId()
      });
      if (!result.ok) {
        if (error) {
          error.textContent = result.message;
        }
        if (button) {
          button.disabled = false;
        }
        return;
      }
    }
    lines = appendPersonalThreadLine(profile, actor, text);
    const thread = overlay.querySelector<HTMLElement>("[data-personal-thread]");
    if (thread) {
      thread.innerHTML = renderThreadLineItems(lines, ownSpace);
      thread.scrollTop = thread.scrollHeight;
    }
    if (textarea) {
      textarea.value = "";
      textarea.focus();
    }
    if (error) {
      error.textContent = ownSpace ? "" : "Отправлено.";
    }
    if (button) {
      button.disabled = false;
    }
  }
}

function renderThreadLines(lines: readonly PersonalThreadLine[], ownSpace: boolean, compact: boolean): string {
  return `
    <div class="personal-thread${compact ? " is-compact" : ""}" data-personal-thread>
      ${renderThreadLineItems(lines, ownSpace)}
    </div>
  `;
}

async function hydrateOwnerInbox(root: HTMLElement, options: PersonalSpacePageOptions): Promise<void> {
  const inbox = root.querySelector<HTMLElement>("[data-personal-inbox] .personal-inbox-list");
  if (!inbox) {
    return;
  }
  const messages = await options.loadInbox(options.route);
  if (!inbox.isConnected) {
    return;
  }
  inbox.innerHTML = renderInboxItems(messages);
}

function renderInboxItems(messages: readonly PersonalSpaceInboxMessage[]): string {
  if (messages.length === 0) {
    return `<section class="personal-empty">${icon("mail")} <span>Сообщений пока нет.</span></section>`;
  }
  return messages.slice(0, 8).map((message) => `
    <article class="personal-inbox-item">
      <span>${escapeHtml(message.author)}</span>
      <p>${escapeHtml(message.text)}</p>
      <time>${escapeHtml(threadTime(message.createdAt))}</time>
    </article>
  `).join("");
}

function renderThreadLineItems(lines: readonly PersonalThreadLine[], ownSpace: boolean): string {
  if (lines.length === 0) {
    return `<section class="personal-empty personal-thread-empty">${icon(ownSpace ? "hexagon" : "mail")} <span>${escapeHtml(ownSpace ? "Заметок пока нет." : "Переписка начнется здесь.")}</span></section>`;
  }
  return lines.map((line) => `
    <article class="personal-thread-line${line.mine ? " is-mine" : ""}">
      <span>${escapeHtml(line.author)}</span>
      <p>${escapeHtml(line.text)}</p>
      <time>${escapeHtml(threadTime(line.createdAt))}</time>
    </article>
  `).join("");
}

function appendPersonalThreadLine(profile: PersonalSpaceProfile, author: string, text: string): readonly PersonalThreadLine[] {
  const line = {
    id: localItemId("message"),
    author: cleanText(author, 80) || "guest",
    text: cleanText(text, 420),
    createdAt: new Date().toISOString(),
    mine: true
  };
  const next = [...loadPersonalThread(profile, author), line].filter((item) => item.text).slice(-80);
  savePersonalThread(profile, author, next);
  return next;
}

function loadPersonalThread(profile: PersonalSpaceProfile, author: string): readonly PersonalThreadLine[] {
  try {
    const parsed = JSON.parse(window.localStorage.getItem(personalThreadKey(profile, author)) || "[]") as unknown;
    return list(parsed).map(normalizeThreadLine).filter(isThreadLine).slice(-80);
  } catch {
    return [];
  }
}

function savePersonalThread(profile: PersonalSpaceProfile, author: string, lines: readonly PersonalThreadLine[]): void {
  try {
    window.localStorage.setItem(personalThreadKey(profile, author), JSON.stringify(lines));
  } catch {
    // Local-first continuity is helpful, but a blocked storage write must not trap the user.
  }
}

function personalThreadKey(profile: PersonalSpaceProfile, author: string): string {
  return `${personalThreadPrefix}${routeUrl(profile)}:${cleanRoutePart(author) || "guest"}`;
}

function personalMessageClientId(): string {
  try {
    const stored = cleanMessageClientId(window.localStorage.getItem(personalMessageClientKey) || "");
    if (stored) {
      return stored;
    }
    const bytes = new Uint8Array(18);
    crypto.getRandomValues(bytes);
    const id = `mc_${Array.from(bytes, (byte) => byte.toString(36).padStart(2, "0")).join("").slice(0, 48)}`;
    window.localStorage.setItem(personalMessageClientKey, id);
    return id;
  } catch {
    return `mc_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 12)}`;
  }
}

function cleanMessageClientId(value: string): string {
  const text = cleanText(value, 80);
  return /^mc_[a-z0-9_-]{8,76}$/iu.test(text) ? text : "";
}

function reactionCountForDisplay(profile: PersonalSpaceProfile): number {
  return Math.max(0, profile.reactions.likes, hasLocalReaction(profile) ? 1 : 0);
}

function formatReactionCount(value: number): string {
  const count = Math.max(0, Math.round(value));
  if (count < 1000) {
    return String(count);
  }
  if (count < 1_000_000) {
    return `${Math.floor(count / 100) / 10}k`;
  }
  return `${Math.floor(count / 100_000) / 10}m`;
}

function hasLocalReaction(profile: PersonalSpaceProfile): boolean {
  try {
    return window.localStorage.getItem(personalReactionKey(profile)) === "like";
  } catch {
    return false;
  }
}

function markLocalReaction(profile: PersonalSpaceProfile): void {
  try {
    window.localStorage.setItem(personalReactionKey(profile), "like");
  } catch {
    // A reaction still feels instant even if storage is unavailable.
  }
}

function personalReactionKey(profile: PersonalSpaceProfile): string {
  return `${personalReactionPrefix}${routeUrl(profile)}`;
}

function personalReactionClientId(): string {
  try {
    const stored = cleanReactionClientId(window.localStorage.getItem(personalReactionClientKey) || "");
    if (stored) {
      return stored;
    }
    const bytes = new Uint8Array(18);
    crypto.getRandomValues(bytes);
    const id = `rc_${Array.from(bytes, (byte) => byte.toString(36).padStart(2, "0")).join("").slice(0, 48)}`;
    window.localStorage.setItem(personalReactionClientKey, id);
    return id;
  } catch {
    return `rc_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 12)}`;
  }
}

function cleanReactionClientId(value: string): string {
  const text = cleanText(value, 80);
  return /^rc_[a-z0-9_-]{8,76}$/iu.test(text) ? text : "";
}

function normalizeThreadLine(value: unknown): PersonalThreadLine | null {
  if (!isRecord(value)) {
    return null;
  }
  const text = cleanText(value.text, 420);
  if (!text) {
    return null;
  }
  return {
    id: cleanText(value.id, 80) || localItemId("message"),
    author: cleanText(value.author, 80) || "guest",
    text,
    createdAt: cleanText(value.createdAt, 40) || new Date().toISOString(),
    mine: value.mine !== false
  };
}

function normalizeInboxMessage(value: unknown): PersonalSpaceInboxMessage | null {
  if (!isRecord(value)) {
    return null;
  }
  const text = cleanText(value.text, 420);
  if (!text) {
    return null;
  }
  return {
    id: cleanText(value.id, 80) || localItemId("message"),
    author: cleanText(value.author, 80) || "guest",
    text,
    createdAt: cleanText(value.createdAt, 40) || new Date().toISOString()
  };
}

function isInboxMessage(value: PersonalSpaceInboxMessage | null): value is PersonalSpaceInboxMessage {
  return Boolean(value);
}

function isThreadLine(value: PersonalThreadLine | null): value is PersonalThreadLine {
  return Boolean(value);
}

function threadTime(value: string): string {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) {
    return "";
  }
  return date.toLocaleString("ru-RU", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" });
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
        <textarea name="about" maxlength="420" aria-label="о странице" placeholder="О странице, проекте или месте">${escapeHtml(profile.about)}</textarea>
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

function showDataSheet(root: HTMLElement, options: PersonalSpacePageOptions): void {
  closePersonalOverlay(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet" role="dialog" aria-modal="true" aria-label="Данные">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <h2>Данные</h2>
      <div class="personal-data-actions" aria-label="импорт и экспорт">
        <button type="button" data-backup-import-sheet>${icon("upload")} <span>Импорт</span></button>
        <button type="button" data-backup-export-sheet>${icon("download")} <span>Экспорт</span></button>
      </div>
      <small data-data-note></small>
    </section>
  `;
  root.append(overlay);
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", () => overlay.remove());
  overlay.querySelector<HTMLButtonElement>("[data-backup-import-sheet]")?.addEventListener("click", () => {
    root.querySelector<HTMLInputElement>("[data-backup-import]")?.click();
  });
  overlay.querySelector<HTMLButtonElement>("[data-backup-export-sheet]")?.addEventListener("click", () => {
    options.exportBackup();
    const note = overlay.querySelector<HTMLElement>("[data-data-note]");
    if (note) {
      note.textContent = "Экспорт готов.";
    }
  });
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      overlay.remove();
    }
  });
}

function showModuleSheet(root: HTMLElement, options: PersonalSpacePageOptions): void {
  closePersonalOverlay(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet" role="dialog" aria-modal="true" aria-label="Модуль">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <h2>Модуль</h2>
      <form data-module-form>
        <select name="kind" aria-label="тип">
          <option value="miniapp">Mini-app</option>
          <option value="link">Ссылка</option>
        </select>
        <input name="title" maxlength="80" required placeholder="Название" />
        <input name="summary" maxlength="140" placeholder="Коротко" />
        <input name="href" autocomplete="url" maxlength="360" required placeholder="URL" />
        <select name="layout" aria-label="размер">
          <option value="large">Большой</option>
          <option value="half">Средний</option>
          <option value="compact">Компактный</option>
          <option value="full">Весь экран</option>
        </select>
        <select name="visibility" aria-label="видимость">
          <option value="public">Всем</option>
          <option value="trusted">По списку</option>
        </select>
        <button type="submit">${icon("check")} Сохранить</button>
      </form>
      <small data-error></small>
    </section>
  `;
  root.append(overlay);
  const kindInput = overlay.querySelector<HTMLSelectElement>("select[name='kind']");
  const titleInput = overlay.querySelector<HTMLInputElement>("input[name='title']");
  const summaryInput = overlay.querySelector<HTMLInputElement>("input[name='summary']");
  const hrefInput = overlay.querySelector<HTMLInputElement>("input[name='href']");
  const layoutInput = overlay.querySelector<HTMLSelectElement>("select[name='layout']");
  const visibilityInput = overlay.querySelector<HTMLSelectElement>("select[name='visibility']");
  const submitButton = overlay.querySelector<HTMLButtonElement>("button[type='submit']");
  const syncModuleKind = () => {
    const miniApp = kindInput?.value !== "link";
    if (layoutInput) {
      layoutInput.hidden = !miniApp;
    }
    if (hrefInput) {
      hrefInput.placeholder = miniApp ? "https://... / http://127.0.0.1:..." : "https://... / /...";
    }
  };
  syncModuleKind();
  kindInput?.addEventListener("change", syncModuleKind);
  titleInput?.focus();
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", () => overlay.remove());
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      overlay.remove();
    }
  });
  overlay.querySelector<HTMLFormElement>("[data-module-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    void saveModuleFromSheet();
  });

  async function saveModuleFromSheet(): Promise<void> {
    const title = cleanText(titleInput?.value || "", 80);
    const error = overlay.querySelector<HTMLElement>("[data-error]");
    const kind: PersonalSpaceCardModuleKind = kindInput?.value === "link" ? "link" : "miniapp";
    if (!title) {
      if (error) {
        error.textContent = "Добавьте название.";
      }
      return;
    }
    const href = kind === "miniapp"
      ? safeMiniAppUrl(hrefInput?.value || "", { baseUrl: window.location.href })
      : cleanUrlPath(hrefInput?.value || "");
    if (!href || (kind === "miniapp" && isServerHostedMiniAppUrl(href))) {
      if (error) {
        error.textContent = kind === "miniapp" ? "Mini-app разместите вне сот." : "Добавьте ссылку.";
      }
      return;
    }
    const draft = {
      kind,
      title,
      summary: cleanText(summaryInput?.value || "", 140),
      href,
      visibility: visibilityInput?.value === "trusted" ? "trusted" as const : "public" as const,
      ...(kind === "miniapp" ? { layout: normalizeMiniAppLayout(layoutInput?.value || "large") } : {})
    };
    if (submitButton) {
      submitButton.disabled = true;
    }
    const result = await options.saveModule(options.route, draft);
    if (!result.ok) {
      if (error) {
        error.textContent = result.message;
      }
      if (submitButton) {
        submitButton.disabled = false;
      }
      return;
    }
    overlay.remove();
    await renderPersonalSpacePage(root, options);
    setActiveLayer(root, "place");
  }
}

function showAgentSheet(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): void {
  closePersonalOverlay(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet personal-agent-sheet" role="dialog" aria-modal="true" aria-label="ИИ">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <div class="personal-module-mark">${icon("agent")}</div>
      <h2>ИИ</h2>
      <form data-agent-form>
        <select name="intent" aria-label="тип задачи">
          <option value="miniapp">Mini-app</option>
          <option value="page">Страница</option>
        </select>
        <textarea name="text" maxlength="720" required placeholder="Что сделать для этой карточки?"></textarea>
        <button type="submit">${icon("check")} Сделать</button>
      </form>
      <div class="personal-agent-reply" data-agent-reply hidden></div>
      <small data-error></small>
    </section>
  `;
  root.append(overlay);
  const textarea = overlay.querySelector<HTMLTextAreaElement>("textarea[name='text']");
  const intentInput = overlay.querySelector<HTMLSelectElement>("select[name='intent']");
  const submitButton = overlay.querySelector<HTMLButtonElement>("button[type='submit']");
  const replyBox = overlay.querySelector<HTMLElement>("[data-agent-reply]");
  const error = overlay.querySelector<HTMLElement>("[data-error]");
  textarea?.focus();
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", () => overlay.remove());
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      overlay.remove();
    }
  });
  overlay.querySelector<HTMLFormElement>("[data-agent-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    void askAgentFromSheet();
  });

  async function askAgentFromSheet(): Promise<void> {
    const text = cleanText(textarea?.value || "", 720);
    if (!text) {
      if (error) {
        error.textContent = "Напишите короткую задачу.";
      }
      return;
    }
    if (submitButton) {
      submitButton.disabled = true;
    }
    if (error) {
      error.textContent = "";
    }
    if (replyBox) {
      replyBox.hidden = false;
      replyBox.textContent = "Думаю...";
    }
    try {
      const result = await options.askAgent(profile, {
        text,
        intent: intentInput?.value === "page" ? "page" : "miniapp"
      });
      const visibleReply = replyWithoutCardModule(result.reply) || result.message;
      if (replyBox) {
        replyBox.textContent = visibleReply;
      }
      if (!result.ok) {
        return;
      }
      const draft = agentModuleDraftFromReply(result.reply);
      if (!draft) {
        return;
      }
      const saved = await options.saveModule(options.route, draft);
      if (!saved.ok) {
        if (error) {
          error.textContent = saved.message;
        }
        return;
      }
      overlay.remove();
      await renderPersonalSpacePage(root, options);
      setActiveLayer(root, "place");
    } catch {
      if (replyBox) {
        replyBox.textContent = "ИИ сейчас недоступен.";
      }
      if (error) {
        error.textContent = "Можно повторить позже.";
      }
    } finally {
      if (submitButton && overlay.isConnected) {
        submitButton.disabled = false;
      }
    }
  }
}

function agentModuleDraftFromReply(reply: string): PersonalSpaceModuleDraft | null {
  const json = cardModuleJsonFromReply(reply);
  if (!json) {
    return null;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(json) as unknown;
  } catch {
    return null;
  }
  if (!isRecord(parsed)) {
    return null;
  }
  const kind: PersonalSpaceCardModuleKind = parsed.kind === "link" ? "link" : "miniapp";
  const title = cleanText(parsed.title || parsed.name, 80);
  const summary = cleanText(parsed.summary || parsed.description, 140);
  const rawHref = typeof parsed.href === "string"
    ? parsed.href
    : typeof parsed.url === "string"
      ? parsed.url
      : typeof parsed.target === "string"
        ? parsed.target
        : "";
  const href = kind === "miniapp"
    ? safeMiniAppUrl(rawHref, { baseUrl: window.location.href })
    : cleanUrlPath(rawHref);
  if (!title || !href || (kind === "miniapp" && isServerHostedMiniAppUrl(href))) {
    return null;
  }
  return {
    kind,
    title,
    summary,
    href,
    visibility: parsed.visibility === "trusted" ? "trusted" : "public",
    ...(kind === "miniapp" ? { layout: normalizeMiniAppLayout(cleanText(parsed.layout, 24) || "large") } : {})
  };
}

function cardModuleJsonFromReply(reply: string): string {
  for (const line of reply.split(/\r?\n/u)) {
    const text = line.trim().replace(/^`+/u, "").replace(/`+$/u, "");
    const match = text.match(/^SOTY_CARD_MODULE:\s*(\{.*\})$/u);
    if (match?.[1]) {
      return match[1];
    }
  }
  return "";
}

function replyWithoutCardModule(reply: string): string {
  return reply
    .split(/\r?\n/u)
    .filter((line) => !line.trim().replace(/^`+/u, "").startsWith("SOTY_CARD_MODULE:"))
    .join("\n")
    .trim();
}

function showRuntimeModuleSheet(root: HTMLElement, profile: PersonalSpaceProfile, target: string, options: PersonalSpacePageOptions): void {
  const module = runtimeModuleDefinitions.find((item) => item.target === target);
  if (!module) {
    options.openRuntime(profile, target);
    return;
  }
  closePersonalOverlay(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet personal-module-sheet" role="dialog" aria-modal="true" aria-label="${escapeAttr(module.title)}">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <div class="personal-module-mark">${icon(module.icon)}</div>
      <h2>${escapeHtml(module.title)}</h2>
      <p>${escapeHtml(module.summary)}</p>
      <button class="personal-primary" type="button" data-runtime-open>${icon("expand")} Открыть</button>
    </section>
  `;
  root.append(overlay);
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", () => overlay.remove());
  overlay.querySelector<HTMLButtonElement>("[data-runtime-open]")?.addEventListener("click", () => options.openRuntime(profile, target));
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      overlay.remove();
    }
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
      ${options.description ? `<p>${escapeHtml(options.description)}</p>` : ""}
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
  saveCachedPersonalProfile(applyLocalReview(profile, author, text));
  const handle = encodeURIComponent(profile.handle);
  const url = profile.slug
    ? `/api/spaces/${handle}/${encodeURIComponent(profile.slug)}/reviews`
    : `/api/spaces/${handle}/reviews`;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify({ author, text, rating: 5 })
    });
    if (!response.ok) {
      return;
    }
    const payload = await response.json() as unknown;
    if (!isRecord(payload) || payload.ok !== true) {
      return;
    }
  } catch {
    return;
  }
}

async function showShareSheet(root: HTMLElement, profile: PersonalSpaceProfile): Promise<void> {
  closePersonalOverlay(root);
  const shareUrl = new URL(profile.url, window.location.origin).toString();
  await showLinkShareSheet({
    title: profile.displayName,
    url: shareUrl
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
      nextNote.textContent = "Фото сохранено.";
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

function isPersonalLayer(value: string): value is PersonalSpaceLayer {
  return layers.some((layer) => layer.id === value);
}

async function installPersonalSpace(root: HTMLElement, button: HTMLButtonElement, options: PersonalSpacePageOptions): Promise<void> {
  const note = root.querySelector<HTMLElement>("[data-install-note]");
  button.disabled = true;
  try {
    const result = await options.install();
    if (note) {
      note.textContent = result.message;
    }
  } catch {
    if (note) {
      note.textContent = "Можно повторить позже.";
    }
  } finally {
    button.disabled = false;
  }
}

function normalizeProfile(value: unknown, route: PersonalSpaceRoute): PersonalSpaceProfile {
  const record = isRecord(value) ? value : {};
  const actions = isRecord(record.actions) ? record.actions : {};
  return {
    kind: record.kind === "space" ? "space" : "entity",
    handle: cleanText(record.handle, 64) || route.handle,
    slug: cleanText(record.slug, 64) || route.slug,
    url: cleanUrlPath(record.url) || routeUrl(route),
    ownerDeviceId: cleanOwnerDeviceId(record.ownerDeviceId),
    displayName: cleanText(record.displayName, 100) || route.handle,
    shortName: cleanText(record.shortName, 32) || route.handle,
    accountName: cleanText(record.accountName, 100) || cleanText(record.displayName, 100) || route.handle,
    photoUrl: cleanProfilePhotoUrl(record.photoUrl),
    title: cleanText(record.title, 80) || fallbackProfile.title,
    headline: cleanText(record.headline, 180) || fallbackProfile.headline,
    about: cleanText(record.about, 420) || fallbackProfile.about,
    accent: cleanColor(record.accent) || fallbackProfile.accent,
    contacts: list(record.contacts).map(normalizeContact).filter(isContact).slice(0, 6),
    posts: list(record.posts).map(normalizePost).filter(isPost).slice(0, 8),
    reviews: list(record.reviews).map(normalizeReview).filter(isReview).slice(0, 8),
    reactions: normalizeReactions(record.reactions),
    spaces: list(record.spaces).map(normalizeSpaceLink).filter(isSpaceLink).slice(0, 12),
    modules: list(record.modules).map(normalizeCardModule).filter(isCardModule).slice(0, 16),
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

function normalizeReactions(value: unknown): PersonalSpaceReactions {
  const record = isRecord(value) ? value : {};
  const likes = Number(record.likes);
  return {
    likes: Number.isFinite(likes) ? Math.max(0, Math.round(likes)) : 0
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

function normalizeCardModule(value: unknown): PersonalSpaceCardModule | null {
  if (!isRecord(value)) {
    return null;
  }
  const title = cleanText(value.title, 80);
  if (!title) {
    return null;
  }
  const kind: PersonalSpaceCardModuleKind = value.kind === "miniapp" ? "miniapp" : "link";
  const rawHref = typeof value.href === "string" ? value.href : typeof value.target === "string" ? value.target : "";
  const href = kind === "miniapp"
    ? safeMiniAppUrl(rawHref, { baseUrl: window.location.href })
    : cleanUrlPath(rawHref);
  if (!href || (kind === "miniapp" && isServerHostedMiniAppUrl(href))) {
    return null;
  }
  return {
    id: cleanText(value.id, 80) || localItemId("module"),
    kind,
    title,
    summary: cleanText(value.summary, 140),
    href,
    visibility: value.visibility === "trusted" ? "trusted" : "public",
    ...(kind === "miniapp" ? { layout: normalizeMiniAppLayout(cleanText(value.layout, 24) || "large") } : {})
  };
}

function fallbackFor(route: PersonalSpaceRoute): PersonalSpaceProfile {
  return {
    ...fallbackProfile,
    handle: route.handle,
    slug: route.slug,
    url: routeUrl(route),
    ownerDeviceId: "",
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

function loadLocalHandle(): string {
  return loadPersonalHandle();
}

function saveLocalHandle(handle: string): void {
  savePersonalHandle(handle);
}

function isRenderedOwnSpace(root: HTMLElement): boolean {
  return root.querySelector<HTMLElement>(".personal-space-shell")?.dataset.owned === "true";
}

function readStoredHandle(key: string): string {
  try {
    return cleanPersonalHandle(window.localStorage.getItem(key) || "");
  } catch {
    return "";
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

function cleanOwnerDeviceId(value: unknown): string {
  const text = cleanText(value, 120);
  return /^dev_[A-Za-z0-9_-]{16,80}$/u.test(text) ? text : "";
}

function cleanUrlPath(value: unknown): string {
  const text = cleanText(value, 240);
  if (!text) {
    return "";
  }
  if (/^(?:\/|https?:\/\/|mailto:|tel:)/u.test(text) && !/[<>"']/u.test(text)) {
    return text;
  }
  return "";
}

function isServerHostedMiniAppUrl(value: string): boolean {
  const text = cleanText(value, 500);
  if (!text) {
    return false;
  }
  const currentOrigin = typeof window === "undefined" ? "" : window.location.origin;
  try {
    const url = new URL(text, currentOrigin || "https://xn--n1afe0b.online");
    const host = url.hostname.toLowerCase();
    // Generated mini-apps stay URL-based for now: no hosting under the Soty web origin until the sandbox/deploy model is separate.
    return url.origin === currentOrigin || host === "xn--n1afe0b.online";
  } catch {
    return text.startsWith("/") && !text.startsWith("//");
  }
}

function cleanProfilePhotoUrl(value: unknown): string {
  const text = typeof value === "string" ? value.trim() : "";
  if (!text) {
    return "";
  }
  if (/^(?:\/|https?:\/\/)/u.test(text) && !/[<>"']/u.test(text)) {
    return text.slice(0, 240);
  }
  if (
    text.length < 900000
    && /^data:image\/(?:png|jpe?g|webp);base64,[a-z0-9+/=]+$/iu.test(text)
  ) {
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

function isCardModule(value: PersonalSpaceCardModule | null): value is PersonalSpaceCardModule {
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
