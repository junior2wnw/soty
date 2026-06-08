import { icon } from "../icons";
import type { IconName } from "../icons";
import { showAgentWorkspace } from "../agent-layer/workspace";
import type { AgentWorkspaceController } from "../agent-layer/workspace";
import { applyChessMove, boardSquares, chessFromSnapshot, chooseAgentMove, createChessSnapshot, isAgentTurn, isSquare, legalMovesForSquare, normalizeChessSnapshot, pieceGlyph, promotionChoices } from "./chess";
import type { ChessSnapshot } from "./chess";
import { miniAppDefaultHeight, miniAppDefaultWidth, normalizeMiniAppLayout, safeMiniAppInlineHtml, safeMiniAppUrl } from "./mini-apps";
import type { MiniAppWindowLayout } from "./mini-apps";
import { hasPushNotificationSubscription, showSystemAttentionNotice } from "./notifications";
import { runtimeModuleDefinitions, runtimeModuleTargetFromString } from "./runtime-modules";
import type { RuntimeModuleTarget } from "./runtime-modules";
import { copyText, showLinkShareSheet } from "./share-sheet";
import type { Color, PieceSymbol, Square } from "chess.js";

export type PersonalSpaceRoute = {
  readonly handle: string;
  readonly slug: string;
};

export type PersonalSpaceInstallResult = {
  readonly ok: boolean;
  readonly message: string;
  readonly guide?: PersonalSpaceInstallGuide;
};

export type PersonalSpaceInstallGuide = {
  readonly title: string;
  readonly steps: readonly string[];
  readonly hint?: string;
};

export type PersonalSpaceProfileUpdate = {
  readonly displayName: string;
  readonly about: string;
  readonly contact: string;
  readonly spaces?: readonly PersonalSpaceLinkUpdate[];
  readonly trustedHandles?: readonly string[];
};

export type PersonalSpaceLinkUpdate = {
  readonly slug: string;
  readonly title: string;
  readonly summary: string;
};

export type PersonalSpacePostDraft = {
  readonly text: string;
};

export type PersonalSpaceMessageDraft = {
  readonly author: string;
  readonly text: string;
  readonly clientId: string;
  readonly replyTo?: PersonalSpaceMessageReplyRef;
  readonly attachment?: PersonalSpaceMessageAttachment;
};

export type PersonalSpaceInboxRequest = {
  readonly limit: number;
};

export type PersonalSpaceThreadRequest = {
  readonly clientId: string;
  readonly limit: number;
  readonly peek?: boolean;
};

export type PersonalSpaceMessageReplyDraft = {
  readonly clientId: string;
  readonly text: string;
  readonly replyTo?: PersonalSpaceMessageReplyRef;
  readonly attachment?: PersonalSpaceMessageAttachment;
};

export type PersonalSpaceMessageReactionDraft = {
  readonly messageId: string;
  readonly clientId: string;
  readonly actorId: string;
  readonly key: "heart" | "check" | "like";
};

export type PersonalSpaceNotificationRequest = {
  readonly route: PersonalSpaceRoute;
  readonly scope: "owner" | "visitor";
  readonly clientId: string;
  readonly title: string;
  readonly url: string;
  readonly icon?: string;
};

type PersonalSpaceMessageSender = "visitor" | "owner";

export type PersonalSpaceMessageAttachment = {
  readonly name: string;
  readonly type: string;
  readonly size: number;
  readonly dataUrl: string;
};

export type PersonalSpaceMessageReplyRef = {
  readonly id: string;
  readonly author: string;
  readonly text: string;
};

type PersonalSpaceMessageReactions = Record<string, number>;

export type PersonalSpaceInboxMessage = {
  readonly id: string;
  readonly author: string;
  readonly text: string;
  readonly clientId: string;
  readonly sender: PersonalSpaceMessageSender;
  readonly replyTo?: PersonalSpaceMessageReplyRef;
  readonly attachment?: PersonalSpaceMessageAttachment;
  readonly reactions: PersonalSpaceMessageReactions;
  readonly readByOwnerAt?: string;
  readonly readByVisitorAt?: string;
  readonly createdAt: string;
};

export type PersonalSpaceThreadMessage = PersonalSpaceInboxMessage;

export type PersonalSpaceAgentRequest = {
  readonly text: string;
  readonly intent: "miniapp" | "page";
};

export type PersonalSpaceAgentDeviceState = "ready" | "update" | "missing";

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
  readonly enableNotifications: (request: PersonalSpaceNotificationRequest) => Promise<PersonalSpaceInstallResult>;
  readonly updateProfile: (route: PersonalSpaceRoute, update: PersonalSpaceProfileUpdate) => Promise<PersonalSpaceInstallResult>;
  readonly savePost: (route: PersonalSpaceRoute, draft: PersonalSpacePostDraft) => Promise<PersonalSpaceInstallResult>;
  readonly saveModule: (route: PersonalSpaceRoute, draft: PersonalSpaceModuleDraft) => Promise<PersonalSpaceInstallResult>;
  readonly loadInbox: (route: PersonalSpaceRoute) => Promise<readonly PersonalSpaceInboxMessage[]>;
  readonly loadThread: (route: PersonalSpaceRoute, request: PersonalSpaceThreadRequest) => Promise<readonly PersonalSpaceThreadMessage[]>;
  readonly replyMessage: (route: PersonalSpaceRoute, draft: PersonalSpaceMessageReplyDraft) => Promise<PersonalSpaceInstallResult>;
  readonly reactMessage: (route: PersonalSpaceRoute, draft: PersonalSpaceMessageReactionDraft) => Promise<PersonalSpaceInstallResult>;
  readonly syncNotifications: (request: PersonalSpaceNotificationRequest) => Promise<void>;
  readonly askAgent: (profile: PersonalSpaceProfile, request: PersonalSpaceAgentRequest) => Promise<PersonalSpaceAgentResult>;
  readonly uploadPhoto: (route: PersonalSpaceRoute, file: File) => Promise<string>;
  readonly exportBackup: () => void;
  readonly importBackup: (file: File) => Promise<PersonalSpaceInstallResult>;
  readonly applyManifest: (profile: PersonalSpaceProfile) => void;
  readonly isOwned: (profile: PersonalSpaceProfile) => boolean | Promise<boolean>;
  readonly agentDeviceState?: () => PersonalSpaceAgentDeviceState;
  readonly installAgent: (profile: PersonalSpaceProfile) => void;
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

type PersonalSocialContact = {
  readonly handle: string;
  readonly slug: string;
  readonly displayName: string;
  readonly headline: string;
  readonly photoUrl: string;
  readonly url: string;
  readonly lastSeenAt: string;
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

type PersonalAgentPreset = {
  readonly mode?: "miniapp" | "page" | "style";
  readonly text?: string;
};
type PersonalSpaceCardModuleKind = "link" | "miniapp" | "runtime";

type PersonalSpaceCardModule = {
  readonly id: string;
  readonly kind: PersonalSpaceCardModuleKind;
  readonly title: string;
  readonly summary: string;
  readonly href: string;
  readonly visibility: PersonalModuleVisibility;
  readonly layout?: MiniAppWindowLayout;
  readonly inlineHtml?: string;
};

export type PersonalSpaceModuleDraft = {
  readonly kind: PersonalSpaceCardModuleKind;
  readonly title: string;
  readonly summary: string;
  readonly href: string;
  readonly visibility: PersonalModuleVisibility;
  readonly layout?: MiniAppWindowLayout;
  readonly inlineHtml?: string;
};

type PersonalThreadLine = {
  readonly id: string;
  readonly author: string;
  readonly text: string;
  readonly createdAt: string;
  readonly mine: boolean;
  readonly replyTo?: PersonalSpaceMessageReplyRef;
  readonly attachment?: PersonalSpaceMessageAttachment;
  readonly reactions: PersonalSpaceMessageReactions;
  readonly status?: "sending" | "sent" | "seen" | "failed" | "saved";
};

type PersonalFileItem = {
  readonly id: string;
  readonly name: string;
  readonly type: string;
  readonly size: number;
  readonly createdAt: string;
  readonly dataUrl: string;
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
  readonly trustedViewer: boolean;
  readonly trustedHandles: readonly string[];
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
  title: "карточка",
  headline: "Живая инфо-карта.",
  about: "Описание не указано.",
  accent: "#f1f1f1",
  contacts: [],
  posts: [],
  reviews: [],
  reactions: { likes: 0 },
  trustedViewer: false,
  trustedHandles: [],
  spaces: [],
  modules: [],
  actions: {
    messageUrl: "/@guest?layer=messages",
    runtimeUrl: "/@guest?layer=place"
  }
};

const layers = [
  { id: "card", label: "Визитка", title: "Визитка", icon: "person" },
  { id: "personal", label: "Записи", title: "Записи", icon: "hexagon" },
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
type EntityActionId = "agent" | "agent-install" | "agent-studio" | "edit" | "install" | "message" | "note" | "notifications" | "review" | "share" | "runtime";
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
const personalChessPrefix = "soty:personal-chess:v1:";
const personalFilesPrefix = "soty:personal-files:v1:";
const personalThreadPrefix = "soty:personal-thread:v1:";
const personalMessageClientKey = "soty:personal-message-client:v1";
const personalNotificationPromptPrefix = "soty:personal-notification-prompt:v1:";
const personalSocialContactsKey = "soty:personal-social-contacts:v1";
const personalReactionPrefix = "soty:personal-reaction:v1:";
const personalReactionClientKey = "soty:personal-reaction-client:v1";
let personalLayerKeysBound = false;
let personalInboxRefreshTimer = 0;
let personalInboxRefreshKey = "";
let personalThreadRefreshTimer = 0;
let personalThreadRefreshKey = "";
const personalMessageNoticeLatest = new Map<string, string>();
const personalMessageNoticeGroups = new Set<string>();
const personalMessengerAttachments = new WeakMap<HTMLElement, PersonalSpaceMessageAttachment>();
const personalMessengerReplies = new WeakMap<HTMLElement, PersonalSpaceMessageReplyRef>();
const personalFileLimit = 8;
const personalFileMaxBytes = 8_000_000;
const personalFileDataUrlMaxChars = 11_200_000;
const personalSocialContactLimit = 24;
const internalContactLabels = new Set(["чат", "страница"]);

export function personalSpaceRouteFromLocation(location: Location = window.location): PersonalSpaceRoute | null {
  const rawParts = location.pathname.split("/").filter(Boolean);
  const parts = rawParts[0] === "pwa" ? rawParts.slice(1) : rawParts;
  const first = parts[0] || "";
  if (!first.startsWith("@") || parts.length > 2) {
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

export function personalSpacePwaStartUrl(route: PersonalSpaceRoute): string {
  const handle = encodeURIComponent(route.handle);
  return route.slug
    ? `/pwa/@${handle}/${encodeURIComponent(route.slug)}/`
    : `/pwa/@${handle}/`;
}

export function personalSpaceManifestHref(route: PersonalSpaceRoute): string {
  const handle = encodeURIComponent(route.handle);
  return route.slug
    ? `/manifest/space/${handle}/${encodeURIComponent(route.slug)}.json`
    : `/manifest/space/${handle}.json`;
}

export async function renderPersonalSpacePage(root: HTMLElement, options: PersonalSpacePageOptions): Promise<void> {
  const previousLayer = previousActiveLayer(root, routeUrl(options.route));
  delete document.body.dataset.sotyReady;
  root.innerHTML = renderLoading(options.route);
  const profile = await loadPersonalSpaceProfile(options.route, (freshProfile) => {
    if (freshProfile.handle === options.route.handle && freshProfile.slug === options.route.slug) {
      options.applyManifest(freshProfile);
    }
  });
  options.applyManifest(profile);
  const activeLayer: PersonalSpaceLayer = requestedActiveLayer() || (requestedRuntimeModule() ? "place" : null) || previousLayer || (options.route.slug ? "place" : "card");
  const localHandle = loadLocalHandle();
  const ownSpace = await options.isOwned(profile);
  root.innerHTML = renderPage(profile, activeLayer, localHandle, ownSpace, options.canNotify(), options.agentDeviceState?.() || "missing");
  bindPersonalSpace(root, profile, options);
  openRequestedPersonalRuntimeModule(root, profile, options);
  document.body.dataset.sotyReady = "1";
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

export async function loadPersonalSpaceThread(
  route: PersonalSpaceRoute,
  request: PersonalSpaceThreadRequest,
  owner?: PersonalOwnerProof | null
): Promise<readonly PersonalSpaceThreadMessage[]> {
  const clientId = cleanMessageClientId(request.clientId);
  if (!clientId) {
    return [];
  }
  const handle = encodeURIComponent(route.handle);
  const url = route.slug
    ? `/api/spaces/${handle}/${encodeURIComponent(route.slug)}/messages/thread`
    : `/api/spaces/${handle}/messages/thread`;
  try {
    const data = {
      clientId,
      limit: Math.max(1, Math.min(100, Math.round(request.limit || 80))),
      ...(request.peek ? { peek: true } : {})
    };
    const response = await fetch(url, {
      method: "POST",
      cache: "no-store",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify(owner ? { data, owner } : data)
    });
    if (!response.ok) {
      return [];
    }
    const payload = await response.json() as unknown;
    if (!isRecord(payload) || payload.ok !== true) {
      return [];
    }
    return list(payload.messages).map(normalizeInboxMessage).filter(isInboxMessage).slice(-100);
  } catch {
    return [];
  }
}

export async function replyPersonalSpaceMessage(
  route: PersonalSpaceRoute,
  draft: PersonalSpaceMessageReplyDraft,
  owner?: PersonalOwnerProof | null
): Promise<PersonalSpaceInstallResult> {
  const clientId = cleanMessageClientId(draft.clientId);
  const text = cleanText(draft.text, 420);
  const replyTo = normalizeMessageReplyRef(draft.replyTo);
  const attachment = normalizeMessageAttachment(draft.attachment);
  if (!clientId || (!text && !attachment)) {
    return { ok: false, message: "Напишите ответ." };
  }
  if (!owner) {
    return { ok: false, message: "Нужна подпись владельца." };
  }
  const handle = encodeURIComponent(route.handle);
  const url = route.slug
    ? `/api/spaces/${handle}/${encodeURIComponent(route.slug)}/messages/reply`
    : `/api/spaces/${handle}/messages/reply`;
  const data = {
    clientId,
    text,
    ...(replyTo ? { replyTo } : {}),
    ...(attachment ? { attachment } : {})
  };
  try {
    const response = await fetch(url, {
      method: "POST",
      cache: "no-store",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify({ data, owner })
    });
    if (!response.ok) {
      return { ok: false, message: "Не получилось ответить." };
    }
    const payload = await response.json() as unknown;
    return isRecord(payload) && payload.ok === true
      ? { ok: true, message: "Ответ отправлен." }
      : { ok: false, message: "Не получилось ответить." };
  } catch {
    return { ok: false, message: "Не получилось ответить." };
  }
}

async function sendPersonalSpaceMessage(profile: PersonalSpaceProfile, draft: PersonalSpaceMessageDraft): Promise<PersonalSpaceInstallResult> {
  const author = cleanText(draft.author, 80) || "guest";
  const text = cleanText(draft.text, 420);
  const clientId = cleanMessageClientId(draft.clientId);
  const replyTo = normalizeMessageReplyRef(draft.replyTo);
  const attachment = normalizeMessageAttachment(draft.attachment);
  if ((!text && !attachment) || !clientId) {
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
      body: JSON.stringify({
        author,
        text,
        clientId,
        ...(replyTo ? { replyTo } : {}),
        ...(attachment ? { attachment } : {})
      })
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

export async function reactPersonalSpaceMessage(
  route: PersonalSpaceRoute,
  draft: PersonalSpaceMessageReactionDraft,
  owner?: PersonalOwnerProof | null
): Promise<PersonalSpaceInstallResult> {
  const messageId = cleanText(draft.messageId, 80);
  const clientId = cleanMessageClientId(draft.clientId);
  const actorId = cleanMessageReactionActorId(draft.actorId);
  const key = normalizeMessageReactionKey(draft.key);
  if (!messageId || !clientId || !actorId || !key) {
    return { ok: false, message: "Реакция не сохранилась." };
  }
  const handle = encodeURIComponent(route.handle);
  const url = route.slug
    ? `/api/spaces/${handle}/${encodeURIComponent(route.slug)}/messages/react`
    : `/api/spaces/${handle}/messages/react`;
  const data = { messageId, clientId, actorId, key };
  try {
    const response = await fetch(url, {
      method: "POST",
      cache: "no-store",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify(owner ? { data, owner } : data)
    });
    if (!response.ok) {
      return { ok: false, message: "Реакция не сохранилась." };
    }
    const payload = await response.json() as unknown;
    return isRecord(payload) && payload.ok === true
      ? { ok: true, message: "Готово." }
      : { ok: false, message: "Реакция не сохранилась." };
  } catch {
    return { ok: false, message: "Реакция не сохранилась." };
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

async function loadPersonalSpaceProfile(
  route: PersonalSpaceRoute,
  onFreshProfile?: (profile: PersonalSpaceProfile) => void
): Promise<PersonalSpaceProfile> {
  const viewer = loadLocalHandle();
  const url = personalProfileApiUrl(route, viewer);
  const cached = loadCachedPersonalProfile(route);
  if (cached) {
    void refreshPersonalSpaceProfile(route, url, onFreshProfile);
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

function personalProfileApiUrl(route: PersonalSpaceRoute, viewer = ""): string {
  const handle = encodeURIComponent(route.handle);
  const path = route.slug
    ? `/api/spaces/${handle}/${encodeURIComponent(route.slug)}`
    : `/api/spaces/${handle}`;
  const cleanViewer = cleanRoutePart(viewer);
  return cleanViewer ? `${path}?viewer=${encodeURIComponent(cleanViewer)}` : path;
}

async function refreshPersonalSpaceProfile(
  route: PersonalSpaceRoute,
  url: string,
  onFreshProfile?: (profile: PersonalSpaceProfile) => void
): Promise<void> {
  const profile = await fetchPersonalSpaceProfile(route, url);
  if (profile) {
    onFreshProfile?.(profile);
  }
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
    trustedViewer: fetched.trustedViewer || cached.trustedViewer,
    trustedHandles: fetched.trustedHandles.length ? fetched.trustedHandles : cached.trustedHandles,
    spaces: mergeByKey(fetched.spaces, cached.spaces, (space) => space.slug).slice(0, 12),
    modules: mergeByKey(fetched.modules, cached.modules, cardModuleKey).slice(0, 16)
  };
}

function applyLocalProfileUpdate(route: PersonalSpaceRoute, update: PersonalSpaceProfileUpdate): PersonalSpaceProfile {
  const profile = localBaseProfile(route);
  const displayName = cleanText(update.displayName, 100) || profile.displayName;
  const about = cleanText(update.about, 420) || profile.about;
  const contact = cleanText(update.contact, 160);
  const trustedHandles = update.trustedHandles ? normalizeTrustedHandles(update.trustedHandles) : profile.trustedHandles;
  const spaces = update.spaces ? normalizeSpaceUpdates(update.spaces, profile.handle) : profile.spaces;
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
    trustedHandles,
    spaces,
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
    layout: draft.layout,
    inlineHtml: draft.inlineHtml
  });
  if (!module) {
    return profile;
  }
  return {
    ...profile,
    modules: [module, ...profile.modules.filter((item) => cardModuleKey(item) !== cardModuleKey(module))].slice(0, 16)
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

function normalizeTrustedHandles(value: unknown): readonly string[] {
  const source = Array.isArray(value)
    ? value
    : typeof value === "string"
      ? value.split(/[\s,;]+/u)
      : [];
  const seen = new Set<string>();
  const handles: string[] = [];
  for (const item of source) {
    const handle = cleanRoutePart(String(item || ""));
    if (!handle || seen.has(handle)) {
      continue;
    }
    seen.add(handle);
    handles.push(handle);
    if (handles.length >= 50) {
      break;
    }
  }
  return handles;
}

function normalizeSpaceUpdates(value: unknown, handle = ""): readonly PersonalSpaceLink[] {
  const cleanHandle = cleanRoutePart(handle);
  return list(value)
    .map((item) => {
      if (!isRecord(item)) {
        return null;
      }
      const slug = cleanRoutePart(String(item.slug || ""));
      const title = cleanText(item.title, 80);
      if (!slug || !title) {
        return null;
      }
      return {
        slug,
        title,
        summary: cleanText(item.summary, 140),
        href: cleanUrlPath(item.href) || (cleanHandle ? `/@${cleanHandle}/${slug}` : "#"),
        active: item.active === true
      };
    })
    .filter(isSpaceLink)
    .slice(0, 12);
}

function spaceUpdatesWith(
  current: readonly PersonalSpaceLink[],
  next?: PersonalSpaceLinkUpdate
): readonly PersonalSpaceLinkUpdate[] {
  const seen = new Set<string>();
  const spaces: PersonalSpaceLinkUpdate[] = [];
  const push = (space: PersonalSpaceLinkUpdate): void => {
    const slug = cleanRoutePart(space.slug);
    const title = cleanText(space.title, 80);
    if (!slug || !title || seen.has(slug)) {
      return;
    }
    seen.add(slug);
    spaces.push({
      slug,
      title,
      summary: cleanText(space.summary, 140)
    });
  };
  if (next) {
    push(next);
  }
  current.forEach((space) => push(space));
  return spaces.slice(0, 12);
}

function localSpaceProfile(
  profile: PersonalSpaceProfile,
  space: PersonalSpaceLinkUpdate,
  spaces: readonly PersonalSpaceLinkUpdate[]
): PersonalSpaceProfile {
  const slug = cleanRoutePart(space.slug);
  const title = cleanText(space.title, 80) || slug;
  const summary = cleanText(space.summary, 140) || "пространство";
  const route = { handle: profile.handle, slug };
  const links = normalizeSpaceUpdates(spaces, profile.handle).map((item) => ({
    ...item,
    active: item.slug === slug
  }));
  return {
    ...profile,
    kind: "space",
    slug,
    url: routeUrl(route),
    displayName: `${title} · ${profile.accountName || profile.displayName}`,
    shortName: title,
    title: "пространство",
    headline: summary,
    about: `${title}: записи, отзывы, связь.`,
    posts: [],
    spaces: links,
    modules: [],
    actions: {
      messageUrl: personalMessageUrl(route),
      runtimeUrl: personalRuntimeUrl(route)
    }
  };
}

function contactValueForProfile(profile: PersonalSpaceProfile): string {
  return visibleContacts(profile, true)
    .find((item) => !internalContactLabels.has(item.label) && item.value !== profile.url && !item.value.startsWith("@"))
    ?.value || "";
}

function cardModuleKey(module: PersonalSpaceCardModule): string {
  if (module.kind === "miniapp" && module.inlineHtml) {
    return `${module.kind}:inline:${module.title}`.toLowerCase();
  }
  return `${module.kind}:${module.href}`.toLowerCase();
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
        <b>Открываю карточку</b>
      </main>
    </section>
  `;
}

function renderPage(
  profile: PersonalSpaceProfile,
  activeLayer: PersonalSpaceLayer,
  localHandle: string,
  ownSpace: boolean,
  canNotify: boolean,
  agentDeviceState: PersonalSpaceAgentDeviceState
): string {
  const initialsText = initials(profile.shortName || profile.displayName);
  const avatar = profile.photoUrl
    ? `<img src="${escapeAttr(profile.photoUrl)}" alt="" />`
    : escapeHtml(initialsText);
  const heroText = heroLine(profile, ownSpace);
  return `
    <section class="personal-space-shell" style="--personal-accent:${escapeAttr(profile.accent)}" data-active-layer="${activeLayer}" data-owned="${ownSpace ? "true" : "false"}" data-route="${escapeAttr(profile.url)}">
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
            ${renderHeroActions(ownSpace)}
            ${ownSpace ? renderOwnerAgentDock(agentDeviceState) : ""}
            <div class="personal-note" data-install-note></div>
          </div>
        </section>
        <div class="personal-layerbar" role="tablist" aria-label="слои инфо-карты">
          ${layers.map((display) => {
            return `
            <button type="button" role="tab" data-layer="${display.id}" data-tooltip="off" aria-label="${escapeAttr(display.title)}" aria-selected="${display.id === activeLayer ? "true" : "false"}" aria-controls="personal-panel-${display.id}" tabindex="${display.id === activeLayer ? "0" : "-1"}">
              ${icon(display.icon)}
              <span>${escapeHtml(display.label)}</span>
            </button>
          `;
          }).join("")}
        </div>
        <section class="personal-panels" aria-label="инфо-карта">
          ${layers.map((layer) => renderLayerPanel(profile, layer.id, ownSpace, activeLayer, localHandle, canNotify)).join("")}
        </section>
      </main>
      ${ownSpace ? `<input data-backup-import type="file" accept="application/json,.json" hidden />` : ""}
    </section>
  `;
}

function renderHeroActions(ownSpace: boolean): string {
  return renderEntityActions(entityActionsFor({ surface: "card", ownSpace }));
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
  localHandle: string,
  canNotify: boolean
): string {
  const view = panelView(profile, layer, ownSpace, localHandle, canNotify);
  const active = layer === activeLayer;
  return `
    <article id="personal-panel-${layer}" class="personal-panel${active ? " is-active" : ""}" data-panel="${layer}" role="tabpanel" aria-hidden="${active ? "false" : "true"}"${active ? "" : " hidden"}>
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
  localHandle: string,
  canNotify: boolean
): PersonalPanelView {
  if (layer === "personal") {
    const actions = entityActionsFor({ surface: "personal", ownSpace });
    return {
      eyebrow: "записи",
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
    const actions = entityActionsFor({ surface: "reviews", ownSpace });
    return {
      eyebrow: "отзывы",
      title: "Отзывы",
      ...(actions.length ? { actions } : {}),
      body: renderReviewsPanel(profile, ownSpace)
    };
  }
  if (layer === "messages") {
    const actions = entityActionsFor({ surface: "messages", ownSpace, canNotify });
    return {
      eyebrow: "сообщения",
      title: ownSpace ? "Мессенджер" : "Сообщения",
      ...(actions.length ? { actions } : {}),
      body: renderMessagePreview(profile, ownSpace, localHandle)
    };
  }
  if (layer === "place") {
    return {
      eyebrow: "место",
      title: "Место",
      body: renderSpaceModules(profile, ownSpace, localHandle)
    };
  }
  return {
    eyebrow: "визитка",
    title: "Визитка",
    text: cardText(profile, ownSpace),
    body: renderContactList(profile, ownSpace)
  };
}

function renderReviewsPanel(profile: PersonalSpaceProfile, ownSpace: boolean): string {
  return `
    ${!ownSpace ? `<div class="personal-review-pulse">${renderReactionPulse(profile, ownSpace)}</div>` : ""}
    ${renderPanelList(
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
    )}
  `;
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
  const actor = cleanRoutePart(localHandle || (ownSpace ? profile.handle : "")) || "guest";
  const clientId = ownSpace ? "" : personalMessageClientIdFor(profile, actor);
  const lines = ownSpace ? [] : loadPersonalThread(profile, actor).slice(-32);
  const roomTitle = ownSpace ? "Диалог" : profile.displayName;
  const roomStatus = ownSpace ? "Выберите человека" : profile.accountName || profile.headline || "Личный диалог";
  return `
    <div class="personal-message-preview personal-messenger${ownSpace ? " is-owner" : ""}" data-personal-messenger data-actor="${escapeAttr(actor)}" data-client-id="${escapeAttr(clientId)}">
      <div class="personal-messenger-side">
        ${ownSpace ? renderOwnerInboxShell(profile) : renderMessengerSocialPanel(profile)}
      </div>
      <section class="personal-messenger-room" aria-label="${escapeAttr(roomTitle)}">
        <header class="personal-messenger-head">
          <div class="personal-messenger-avatar" aria-hidden="true" data-room-avatar>${escapeHtml(initials(profile.shortName || profile.displayName))}</div>
          <div>
            <b data-room-title>${escapeHtml(roomTitle)}</b>
            <p data-room-status>${escapeHtml(roomStatus)}</p>
          </div>
          <span data-room-badge>${escapeHtml(ownSpace ? "live" : "dm")}</span>
        </header>
        <div class="personal-thread-tools">
          <input type="search" maxlength="80" data-thread-search placeholder="${escapeAttr("Поиск")}" aria-label="${escapeAttr("Поиск по диалогу")}">
        </div>
        ${renderThreadLines(lines, ownSpace, false)}
        ${renderNotificationPrompt()}
        ${renderMessageComposer(ownSpace)}
        <small data-action-note data-error></small>
      </section>
    </div>
  `;
}

function renderNotificationPrompt(): string {
  return `
    <div class="personal-notification-prompt" data-notification-prompt hidden>
      <span aria-hidden="true">${icon("bell")}</span>
      <b>Не пропустить ответ?</b>
      <button type="button" data-notification-enable>Включить</button>
      <button type="button" data-notification-dismiss aria-label="${escapeAttr("Не сейчас")}">${icon("close")}</button>
    </div>
  `;
}

function renderOwnerInboxShell(profile: PersonalSpaceProfile): string {
  return `
    <section class="personal-inbox" data-personal-inbox aria-label="Входящие">
      <header class="personal-sidebar-head">
        <h3>Диалоги</h3>
        <span>${escapeHtml(profile.handle)}</span>
      </header>
      <div class="personal-inbox-search">
        <input type="search" maxlength="80" data-inbox-search placeholder="${escapeAttr("Люди и сообщения")}" aria-label="${escapeAttr("Поиск диалогов")}">
      </div>
      <div class="personal-inbox-list">
        <section class="personal-empty">${icon("mail")} <span>Проверяю...</span></section>
      </div>
    </section>
    <section class="personal-social-card" aria-label="Профиль">
      <div class="personal-social-stats">
        <a href="${escapeAttr(profile.url)}?layer=personal"><b>${escapeHtml(String(profile.posts.length))}</b><span>записи</span></a>
        <a href="${escapeAttr(profile.url)}?layer=reviews"><b>${escapeHtml(String(profile.reviews.length))}</b><span>отзывы</span></a>
        <a href="${escapeAttr(profile.url)}?layer=place"><b>${escapeHtml(String(visiblePersonalPlaceCount(profile, true, profile.handle)))}</b><span>место</span></a>
      </div>
    </section>
  `;
}

function renderMessengerSocialPanel(profile: PersonalSpaceProfile): string {
  const recent = loadPersonalSocialContacts()
    .filter((contact) => contact.url !== profile.url)
    .slice(0, 5);
  const feedItems = messengerSocialFeedItems(profile);
  return `
    <section class="personal-social-card" aria-label="Профиль">
      <div class="personal-social-profile">
        <div class="personal-social-avatar">${profile.photoUrl ? `<img src="${escapeAttr(profile.photoUrl)}" alt="">` : `<span>${escapeHtml(initials(profile.shortName || profile.displayName))}</span>`}</div>
        <div>
          <h3>${escapeHtml(profile.displayName)}</h3>
          <p>${escapeHtml(profile.headline || profile.about || profile.accountName)}</p>
        </div>
      </div>
      <div class="personal-social-stats">
        <a href="${escapeAttr(profile.url)}?layer=personal"><b>${escapeHtml(String(profile.posts.length))}</b><span>записи</span></a>
        <a href="${escapeAttr(profile.url)}?layer=reviews"><b>${escapeHtml(String(profile.reviews.length))}</b><span>отзывы</span></a>
        <a href="${escapeAttr(profile.url)}?layer=place"><b>${escapeHtml(String(visiblePersonalPlaceCount(profile, false, loadLocalHandle())))}</b><span>место</span></a>
      </div>
      <nav class="personal-social-actions" aria-label="Переходы">
        <a href="${escapeAttr(profile.url)}">${icon("person")}<span>Профиль</span></a>
        <a href="${escapeAttr(profile.url)}?layer=personal">${icon("hexagon")}<span>Лента</span></a>
        <a href="${escapeAttr(profile.url)}?layer=place">${icon("apps")}<span>Место</span></a>
      </nav>
    </section>
    ${feedItems.length ? `
      <section class="personal-social-card personal-social-feed" aria-label="Лента">
        <header class="personal-sidebar-head">
          <h3>Лента</h3>
          <span>${escapeHtml(threadTime(new Date().toISOString()))}</span>
        </header>
        ${feedItems.join("")}
      </section>
    ` : ""}
    ${recent.length ? `
      <section class="personal-social-card personal-social-people" aria-label="Люди">
        <header class="personal-sidebar-head">
          <h3>Люди</h3>
          <span>${escapeHtml(String(recent.length))}</span>
        </header>
        ${recent.map(renderPersonalSocialContact).join("")}
      </section>
    ` : ""}
  `;
}

function messengerSocialFeedItems(profile: PersonalSpaceProfile): readonly string[] {
  const postItems = profile.posts.slice(0, 2).map((post) => {
    const text = personalSpaceCopy(post.text, false);
    return `
      <a class="personal-social-feed-item" href="${escapeAttr(profile.url)}?layer=personal">
        <span>${icon("hexagon")}</span>
        <div>
          <b>${escapeHtml(post.title)}</b>
          <p>${escapeHtml(text)}</p>
          <small>${escapeHtml(post.meta || "запись")}</small>
        </div>
      </a>
    `;
  });
  const reviewItems = profile.reviews.slice(0, 1).map((review) => `
    <a class="personal-social-feed-item" href="${escapeAttr(profile.url)}?layer=reviews">
      <span>${icon("heart")}</span>
      <div>
        <b>${escapeHtml(review.author)}</b>
        <p>${escapeHtml(review.text)}</p>
        <small>${escapeHtml("отзыв")}</small>
      </div>
    </a>
  `);
  return [...postItems, ...reviewItems].slice(0, 3);
}

function renderPersonalSocialContact(contact: PersonalSocialContact): string {
  const title = contact.displayName || contact.handle;
  const subtitle = contact.headline || `@${contact.handle}`;
  return `
    <a class="personal-social-person" href="${escapeAttr(contact.url)}">
      <span class="personal-social-person-avatar">${contact.photoUrl ? `<img src="${escapeAttr(contact.photoUrl)}" alt="">` : `<b>${escapeHtml(initials(title))}</b>`}</span>
      <span>
        <b>${escapeHtml(title)}</b>
        <small>${escapeHtml(subtitle)}</small>
      </span>
    </a>
  `;
}

function rememberPersonalSocialContact(profile: PersonalSpaceProfile): void {
  const contact = socialContactFromProfile(profile);
  if (!contact.handle || !contact.url) {
    return;
  }
  try {
    const current = loadPersonalSocialContacts();
    const contactKey = personalSocialContactKey(contact);
    const next = [
      contact,
      ...current.filter((item) => personalSocialContactKey(item) !== contactKey)
    ].slice(0, personalSocialContactLimit);
    window.localStorage.setItem(personalSocialContactsKey, JSON.stringify(next));
  } catch {
    // Social memory only improves repeated visits; the card must work without it.
  }
}

function loadPersonalSocialContacts(): readonly PersonalSocialContact[] {
  try {
    const parsed = JSON.parse(window.localStorage.getItem(personalSocialContactsKey) || "[]") as unknown;
    return list(parsed)
      .map(normalizePersonalSocialContact)
      .filter(isPersonalSocialContact)
      .sort((left, right) => right.lastSeenAt.localeCompare(left.lastSeenAt))
      .slice(0, personalSocialContactLimit);
  } catch {
    return [];
  }
}

function normalizePersonalSocialContact(value: unknown): PersonalSocialContact | null {
  if (!isRecord(value)) {
    return null;
  }
  const handle = cleanRoutePart(String(value.handle || ""));
  const slug = cleanRoutePart(String(value.slug || ""));
  const url = cleanUrlPath(value.url) || (handle ? routeUrl({ handle, slug }) : "");
  const displayName = cleanText(value.displayName, 80) || titleFromRoutePart(handle);
  const lastSeenAt = cleanText(value.lastSeenAt, 40) || new Date(0).toISOString();
  if (!handle || !url) {
    return null;
  }
  return {
    handle,
    slug,
    displayName,
    headline: cleanText(value.headline, 120),
    photoUrl: cleanProfilePhotoUrl(value.photoUrl),
    url,
    lastSeenAt
  };
}

function isPersonalSocialContact(value: PersonalSocialContact | null): value is PersonalSocialContact {
  return Boolean(value);
}

function socialContactFromProfile(profile: PersonalSpaceProfile): PersonalSocialContact {
  return {
    handle: cleanRoutePart(profile.handle),
    slug: cleanRoutePart(profile.slug),
    displayName: cleanText(profile.displayName, 80) || titleFromRoutePart(profile.handle),
    headline: cleanText(profile.headline || profile.about || profile.accountName, 120),
    photoUrl: cleanProfilePhotoUrl(profile.photoUrl),
    url: cleanUrlPath(profile.url) || routeUrl(profile),
    lastSeenAt: new Date().toISOString()
  };
}

function personalSocialContactKey(contact: Pick<PersonalSocialContact, "handle" | "slug" | "url">): string {
  return `${cleanRoutePart(contact.handle)}:${cleanRoutePart(contact.slug)}:${cleanUrlPath(contact.url)}`;
}

function renderMessageComposer(ownSpace: boolean): string {
  return `
    <form class="personal-message-composer" data-message-form>
      <div class="personal-reply-draft" data-reply-draft hidden>
        <span></span>
        <button type="button" data-reply-clear aria-label="${escapeAttr("Убрать ответ")}">${icon("close")}</button>
      </div>
      <div class="personal-attachment-chip" data-attachment-chip hidden>
        <span></span>
        <button type="button" data-attachment-clear aria-label="${escapeAttr("Убрать файл")}">${icon("close")}</button>
      </div>
      <input type="file" data-message-file hidden>
      <button class="personal-message-tool" type="button" data-message-attach aria-label="${escapeAttr("Прикрепить файл")}">${icon("clip")}</button>
      <textarea name="text" maxlength="420" rows="2" placeholder="${escapeAttr(ownSpace ? "Ответ" : "Сообщение")}"></textarea>
      <button class="personal-message-submit" type="submit" aria-label="${escapeAttr("Отправить")}">${icon("send")}</button>
    </form>
  `;
}

function entityActionsFor(options: { readonly surface: EntityActionSurface; readonly ownSpace: boolean; readonly canNotify?: boolean }): readonly EntityAction[] {
  if (options.surface === "card") {
    if (options.ownSpace) {
      return [
        { id: "edit", label: "Править", icon: "person", tone: "secondary" },
        { id: "share", label: "Поделиться", icon: "qr", tone: "secondary" }
      ];
    }
    return [
      { id: "message", label: "Написать", icon: "mail", tone: "primary" },
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
      ...(options.canNotify ? [{ id: "notifications" as const, label: "Оповещения", icon: "bell" as const, tone: "secondary" as const }] : []),
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

function renderOwnerAgentDock(agentDeviceState: PersonalSpaceAgentDeviceState): string {
  const status = agentDeviceState === "ready"
    ? {
      label: "Агент готов",
      detail: "Можно писать задачи в чат",
      installDetail: "проверить / обновить"
    }
    : agentDeviceState === "update"
      ? {
        label: "Нужно обновление",
        detail: "Скачайте новую версию агента",
        installDetail: "обновить агент"
      }
      : {
        label: "Сначала устройство",
        detail: "Установите агента один раз",
        installDetail: "установить агент"
      };
  const actions: readonly { readonly action: EntityActionId; readonly label: string; readonly detail: string; readonly icon: IconName; readonly primary?: boolean }[] = [
    {
      action: "agent",
      label: "Задача ИИ",
      detail: "открыть чат агента",
      icon: "agent",
      primary: true
    },
    {
      action: "agent-install",
      label: "Устройство",
      detail: status.installDetail,
      icon: "download"
    },
    {
      action: "agent-studio",
      label: "Мастерская",
      detail: "mini-app / стиль",
      icon: "apps"
    }
  ];
  return `
    <section class="personal-agent-guide" data-agent-guide data-agent-device-state="${escapeAttr(agentDeviceState)}" aria-label="${escapeAttr("Агент карточки")}">
      <div class="personal-agent-status">
        <span>${icon("agent")}</span>
        <div>
          <b>${escapeHtml(status.label)}</b>
          <small>${escapeHtml(status.detail)}</small>
        </div>
      </div>
      <div class="personal-agent-path" aria-label="${escapeAttr("Как работает агент")}">
        <span><b>1</b> устройство</span>
        <span><b>2</b> чат</span>
        <span><b>3</b> задача</span>
      </div>
      <div class="personal-agent-dock" aria-label="${escapeAttr("Действия агента")}">
        ${actions.map((action) => `
          <button type="button" data-action="${escapeAttr(action.action)}"${action.primary ? ` class="is-primary"` : ""}>
            ${icon(action.icon)}
            <span>
              <b>${escapeHtml(action.label)}</b>
              <small>${escapeHtml(action.detail)}</small>
            </span>
          </button>
        `).join("")}
      </div>
      <div class="personal-agent-presets" aria-label="${escapeAttr("Быстрые задачи агента")}">
        <button type="button" data-agent-preset="style" data-agent-text="${escapeAttr("Наведи красоту в моей карточке: сделай аккуратный визуальный блок без лишнего шума.")}">${icon("agent")} <span>Красота</span></button>
        <button type="button" data-agent-preset="miniapp" data-agent-text="${escapeAttr("Создай полезный mini-app для этой карточки.")}">${icon("apps")} <span>Mini-app</span></button>
        <button type="button" data-agent-preset="page" data-agent-text="${escapeAttr("Добавь полезную страницу или ссылку для этой карточки.")}">${icon("qr")} <span>Страница</span></button>
      </div>
    </section>
  `;
}

function renderSpaceModules(profile: PersonalSpaceProfile, ownSpace: boolean, localHandle: string): string {
  const groups = personalModuleGroups(profile, ownSpace, localHandle);
  if (groups.length === 0) {
    return renderPanelList(
      "personal-spaces",
      [],
      "apps",
      ownSpace ? "Добавьте пространство или модуль." : "Открытых модулей пока нет."
    );
  }
  return `
    <div class="personal-spaces">
      ${groups.map(renderPersonalModuleGroup).join("")}
    </div>
  `;
}

function personalModuleGroups(profile: PersonalSpaceProfile, ownSpace: boolean, localHandle: string): readonly PersonalModuleGroup[] {
  const runtimeModules: readonly PersonalModule[] = runtimeModuleDefinitions.map((module) => ({
    ...module,
    kind: "runtime" as const,
    priority: module.id === "agent"
  }));
  const runtimeById = (id: string): PersonalModule[] => runtimeModules.filter((module) => module.id === id);
  const customModules: readonly PersonalModule[] = profile.modules
    .filter((module) => canSeePersonalModule(module, profile, ownSpace, localHandle))
    .map(cardModuleToPersonalModule);
  const addModule: PersonalModule = {
    id: "module:add",
    title: "Модуль",
    summary: "добавить",
    icon: "apps",
    kind: "action",
    target: "module"
  };
  const addSpace: PersonalModule = {
    id: "space:add",
    title: "Пространство",
    summary: "создать",
    icon: "hexagon",
    kind: "action",
    target: "space"
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
  const priorityModules = ownSpace ? [...runtimeById("agent"), addSpace, addModule, ...runtimeById("access"), ...dataModules] : [];
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

function canSeePersonalModule(
  module: PersonalSpaceCardModule,
  profile: PersonalSpaceProfile,
  ownSpace: boolean,
  localHandle: string
): boolean {
  if (ownSpace || module.visibility === "public") {
    return true;
  }
  const handle = cleanRoutePart(localHandle);
  return Boolean(handle && (profile.trustedViewer || profile.trustedHandles.includes(handle)));
}

function visiblePersonalModules(profile: PersonalSpaceProfile, ownSpace: boolean, localHandle: string): readonly PersonalSpaceCardModule[] {
  return profile.modules.filter((module) => canSeePersonalModule(module, profile, ownSpace, localHandle));
}

function visiblePersonalPlaceCount(profile: PersonalSpaceProfile, ownSpace: boolean, localHandle: string): number {
  const customCount = visiblePersonalModules(profile, ownSpace, localHandle).length;
  const runtimeCount = ownSpace ? runtimeModuleDefinitions.length : 0;
  return customCount + profile.spaces.length + runtimeCount;
}

function cardModuleToPersonalModule(module: PersonalSpaceCardModule): PersonalModule {
  if (module.kind === "runtime") {
    const runtime = runtimeModuleDefinitions.find((item) => item.target === module.href);
    return {
      id: `custom:${module.id}`,
      title: module.title || runtime?.title || "Модуль",
      summary: module.summary || runtime?.summary || visibilityLabel(module.visibility),
      icon: runtime?.icon || (module.visibility === "trusted" ? "shield" as const : "apps" as const),
      kind: "runtime",
      target: runtime?.target || module.href,
      visibility: module.visibility
    };
  }
  return {
    id: `custom:${module.id}`,
    title: module.title,
    summary: module.summary || visibilityLabel(module.visibility),
    icon: module.kind === "miniapp" ? "apps" as const : module.visibility === "trusted" ? "shield" as const : "apps" as const,
    kind: module.kind === "miniapp" ? "miniapp" : "custom",
    target: module.href,
    ...(module.kind === "miniapp" ? { layout: module.layout || "large" as const } : {}),
    visibility: module.visibility
  };
}

function renderPersonalModule(module: PersonalModule): string {
  return `
    <button type="button" data-module-id="${escapeAttr(module.id)}" data-module-kind="${module.kind}" data-module-target="${escapeAttr(module.target)}" data-module-layout="${escapeAttr(module.layout || "")}" class="${[module.active ? "is-active" : "", module.priority ? "is-priority" : ""].filter(Boolean).join(" ")}">
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
    return ownSpace ? "Визитка, записи, отзывы, связь." : "Контакты и личная связь.";
  }
  return text;
}

function cardText(profile: PersonalSpaceProfile, ownSpace: boolean): string {
  const text = personalSpaceCopy(profile.about, ownSpace);
  if (isDefaultPersonalText(text)) {
    return ownSpace ? "Добавьте описание и контакт." : "Контактная карточка.";
  }
  return text;
}

function isDefaultPersonalText(value: string): boolean {
  const text = value.trim().toLowerCase();
  return !text
    || text === "визитка, записи, отзывы, связь"
    || text === "живая инфо-карта."
    || text === "контакты и личная связь."
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

function requestedRuntimeModule(): RuntimeModuleTarget | "" {
  try {
    return runtimeModuleTargetFromString(new URL(window.location.href).searchParams.get("module") || "");
  } catch {
    return "";
  }
}

function openRequestedPersonalRuntimeModule(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): void {
  const target = requestedRuntimeModule();
  if (!target) {
    return;
  }
  setActiveLayer(root, "place");
  try {
    const url = new URL(window.location.href);
    url.searchParams.delete("module");
    if (!url.searchParams.has("layer")) {
      url.searchParams.set("layer", "place");
    }
    window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  } catch {
    // The module can still open even if the URL cannot be cleaned.
  }
  window.setTimeout(() => {
    openPersonalRuntimeModule(root, profile, target, options);
  }, 0);
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
  bindPersonalLayerKeys();
  rememberPersonalSocialContact(profile);
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
    const agentPresetButton = target?.closest<HTMLButtonElement>("[data-agent-preset]");
    if (agentPresetButton && shell.contains(agentPresetButton)) {
      const mode = agentPresetMode(agentPresetButton.dataset.agentPreset || "");
      const text = agentPresetButton.dataset.agentText || "";
      showAgentSheet(root, profile, options, {
        ...(mode ? { mode } : {}),
        ...(text ? { text } : {})
      });
      return;
    }
    const moduleNode = target?.closest<HTMLElement>("[data-module-kind]");
    if (moduleNode && shell.contains(moduleNode)) {
      openPersonalModule(root, moduleNode, profile, options);
      return;
    }
    const inboxNode = target?.closest<HTMLElement>("[data-inbox-client-id]");
    if (inboxNode && shell.contains(inboxNode)) {
      void openOwnerConversation(root, inboxNode, profile, options);
      return;
    }
    const actionNode = target?.closest<HTMLElement>("[data-action]");
    if (actionNode && shell.contains(actionNode)) {
      handleEntityAction(root, actionNode, profile, options);
    }
  });
  bindPersonalMessenger(root, profile, options);
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
    void hydrateOwnerInbox(root, profile, options);
    startOwnerInboxRefresh(root, profile, options);
    stopThreadRefresh();
  } else {
    stopOwnerInboxRefresh();
    startThreadRefresh(root, profile, options, personalMessageClientIdFor(profile, loadLocalHandle()), false);
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
  if (kind === "action" && target === "space") {
    showSpaceSheet(root, profile, options);
    return;
  }
  if (kind === "space" && target) {
    window.history.pushState({}, "", personalNavigationTarget(target));
    window.dispatchEvent(new CustomEvent("soty-personal-routechange"));
    return;
  }
  if (kind === "miniapp") {
    const moduleId = (node.dataset.moduleId || "").replace(/^custom:/u, "");
    const record = profile.modules.find((module) => module.id === moduleId);
    showPersonalMiniAppSheet(root, {
      title: node.querySelector("span")?.textContent || "Mini-app",
      target,
      layout: normalizeMiniAppLayout(node.dataset.moduleLayout || "large"),
      inlineHtml: record?.inlineHtml || ""
    });
    return;
  }
  if (kind === "custom") {
    if (target) {
      window.location.assign(target);
    }
    return;
  }
  if (kind === "runtime") {
    openPersonalRuntimeModule(root, profile, target, options);
  }
}

function openPersonalRuntimeModule(root: HTMLElement, profile: PersonalSpaceProfile, target: string, options: PersonalSpacePageOptions): void {
  if (target === "qr") {
    void showShareSheet(root, profile);
    return;
  }
  if (target === "agent") {
    options.openRuntime(profile, "agent");
    return;
  }
  showRuntimeModuleSheet(root, profile, target, options);
}

function showPersonalMiniAppSheet(
  root: HTMLElement,
  module: { readonly title: string; readonly target: string; readonly layout: MiniAppWindowLayout; readonly inlineHtml?: string }
): void {
  closePersonalOverlay(root);
  const inlineHtml = safeMiniAppInlineHtml(module.inlineHtml || "");
  const url = safeMiniAppUrl(module.target, { baseUrl: window.location.href });
  if (!inlineHtml && (!url || isServerHostedMiniAppUrl(url))) {
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
  const frame = inlineHtml
    ? `<iframe title="${escapeAttr(module.title)}" srcdoc="${escapeAttr(inlineHtml)}" loading="lazy" referrerpolicy="no-referrer" sandbox="allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox allow-downloads"></iframe>`
    : `<iframe title="${escapeAttr(module.title)}" src="${escapeAttr(url)}" loading="lazy" referrerpolicy="no-referrer" sandbox="allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox allow-downloads allow-same-origin"></iframe>`;
  overlay.innerHTML = `
    <section class="personal-sheet personal-mini-app-sheet" data-mini-app-layout="${escapeAttr(module.layout)}" role="dialog" aria-modal="true" aria-label="${escapeAttr(module.title)}">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <h2>${escapeHtml(module.title)}</h2>
      <div class="personal-mini-app-frame" style="--mini-app-height:${escapeAttr(height)};--mini-app-width:${escapeAttr(width)}">
        ${frame}
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
    setActiveLayer(root, "messages");
    const handle = loadLocalHandle();
    if (handle || isRenderedOwnSpace(root)) {
      focusPersonalMessenger(root, handle || profile.handle);
      return;
    }
    showNicknameSheet(root, {
      title: "Ваше имя",
      description: "",
      action: "Написать",
      onDone: (nextHandle) => focusPersonalMessenger(root, nextHandle)
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
  if (action === "agent") {
    options.openRuntime(profile, "agent");
    return;
  }
  if (action === "agent-install") {
    options.installAgent(profile);
    return;
  }
  if (action === "agent-studio") {
    showAgentSheet(root, profile, options);
    return;
  }
  if (action === "notifications" && node instanceof HTMLButtonElement) {
    void enablePersonalNotifications(root, node, profile, options);
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

function agentPresetMode(value: string): PersonalAgentPreset["mode"] {
  return value === "page" || value === "style" ? value : "miniapp";
}

function focusPersonalMessenger(root: HTMLElement, actor: string): void {
  const messenger = root.querySelector<HTMLElement>("[data-personal-messenger]");
  if (messenger) {
    const cleanActor = cleanRoutePart(actor);
    if (cleanActor) {
      messenger.dataset.actor = cleanActor;
    }
  }
  window.setTimeout(() => {
    const textarea = root.querySelector<HTMLTextAreaElement>("[data-personal-messenger] textarea[name='text']");
    textarea?.focus();
    textarea?.scrollIntoView({ block: "nearest", inline: "nearest" });
  }, 0);
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

async function enablePersonalNotifications(root: HTMLElement, button: HTMLButtonElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): Promise<void> {
  const note = button.closest<HTMLElement>(".personal-message-preview")?.querySelector<HTMLElement>("[data-action-note]")
    || root.querySelector<HTMLElement>("[data-install-note]");
  const request = personalNotificationRequest(root, profile, options);
  if (!request) {
    if (note) {
      note.textContent = "Откройте диалог.";
    }
    return;
  }
  button.disabled = true;
  const result = await options.enableNotifications(request);
  if (note) {
    note.textContent = result.message;
  }
  if (result.ok) {
    button.textContent = "Включено";
    hidePersonalNotificationPrompt(root);
    return;
  }
  button.disabled = false;
  updatePersonalNotificationPrompt(root, profile, options);
}

function personalNotificationRequest(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): PersonalSpaceNotificationRequest | null {
  const messenger = root.querySelector<HTMLElement>("[data-personal-messenger]");
  if (!messenger) {
    return null;
  }
  const ownSpace = isRenderedOwnSpace(root);
  if (ownSpace) {
    return {
      route: options.route,
      scope: "owner",
      clientId: "",
      title: profile.displayName || profile.handle || "Соты",
      url: personalMessageUrl(options.route),
      icon: personalProfileIconUrl(profile)
    };
  }
  const actor = cleanRoutePart(messenger.dataset.actor || loadLocalHandle() || "");
  const clientId = personalMessageClientIdFor(profile, actor);
  if (!clientId) {
    return null;
  }
  messenger.dataset.clientId = clientId;
  return {
    route: options.route,
    scope: "visitor",
    clientId,
    title: profile.displayName || profile.handle || "Соты",
    url: personalMessageUrl(options.route),
    icon: personalProfileIconUrl(profile)
  };
}

function personalNotificationsAlreadyGranted(): boolean {
  return "Notification" in window && Notification.permission === "granted";
}

function updatePersonalNotificationPrompt(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions, hasConversationActivity = false): void {
  const messenger = root.querySelector<HTMLElement>("[data-personal-messenger]");
  const prompt = messenger?.querySelector<HTMLElement>("[data-notification-prompt]");
  if (!messenger || !prompt) {
    return;
  }
  if (hasConversationActivity) {
    messenger.dataset.notificationPromptArmed = "true";
  }
  const request = personalNotificationRequest(root, profile, options);
  const shouldShow = messenger.dataset.notificationPromptArmed === "true"
    && Boolean(request)
    && options.canNotify()
    && !personalNotificationsAlreadyGranted()
    && !personalNotificationPromptSnoozed(messenger, options);
  prompt.hidden = !shouldShow;
}

function hidePersonalNotificationPrompt(root: HTMLElement): void {
  root.querySelector<HTMLElement>("[data-notification-prompt]")?.setAttribute("hidden", "");
}

function dismissPersonalNotificationPrompt(messenger: HTMLElement, options: PersonalSpacePageOptions): void {
  try {
    window.localStorage.setItem(personalNotificationPromptKey(messenger, options), String(Date.now()));
  } catch {
    // A blocked storage write should not trap the messenger UI.
  }
  messenger.querySelector<HTMLElement>("[data-notification-prompt]")?.setAttribute("hidden", "");
  setMessengerNote(messenger, "Хорошо.");
}

function personalNotificationPromptSnoozed(messenger: HTMLElement, options: PersonalSpacePageOptions, now = Date.now()): boolean {
  try {
    const timestamp = Number(window.localStorage.getItem(personalNotificationPromptKey(messenger, options)) || 0);
    return Number.isFinite(timestamp) && now - timestamp < 7 * 24 * 60 * 60 * 1000;
  } catch {
    return false;
  }
}

function personalNotificationPromptKey(messenger: HTMLElement, options: PersonalSpacePageOptions): string {
  const owner = messenger.classList.contains("is-owner");
  const peer = owner
    ? cleanMessageClientId(messenger.dataset.clientId || "") || "owner"
    : cleanRoutePart(messenger.dataset.actor || loadLocalHandle() || "guest") || "guest";
  return `${personalNotificationPromptPrefix}${routeUrl(options.route)}:${owner ? "owner" : "visitor"}:${peer}`;
}

function bindPersonalMessenger(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): void {
  const messenger = root.querySelector<HTMLElement>("[data-personal-messenger]");
  const form = messenger?.querySelector<HTMLFormElement>("[data-message-form]");
  const textarea = form?.querySelector<HTMLTextAreaElement>("textarea[name='text']");
  const fileInput = form?.querySelector<HTMLInputElement>("[data-message-file]");
  const attachButton = form?.querySelector<HTMLButtonElement>("[data-message-attach]");
  const searchInput = messenger?.querySelector<HTMLInputElement>("[data-thread-search]");
  const inboxSearch = messenger?.querySelector<HTMLInputElement>("[data-inbox-search]");
  if (!messenger || !form || !textarea) {
    return;
  }
  textarea.required = false;
  const ownSpace = isRenderedOwnSpace(root);
  let holdTimer = 0;
  let holdX = 0;
  let holdY = 0;
  let holdLine: HTMLElement | null = null;
  let skipNextLineClick = false;
  const cancelHold = () => {
    if (holdTimer) {
      window.clearTimeout(holdTimer);
      holdTimer = 0;
    }
    holdLine = null;
  };
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const text = cleanText(textarea.value || "", 420);
    const attachment = personalMessengerAttachments.get(messenger);
    const replyTo = personalMessengerReplies.get(messenger);
    if (!text && !attachment) {
      setMessengerNote(messenger, "Напишите пару слов.");
      textarea.focus();
      return;
    }
    const currentActor = cleanRoutePart(messenger.dataset.actor || loadLocalHandle() || profile.handle);
    if (!ownSpace && (!currentActor || currentActor === "guest") && !loadLocalHandle()) {
      showNicknameSheet(root, {
        title: "Ваше имя",
        description: "",
        action: "Продолжить",
        onDone: (nextHandle) => {
          messenger.dataset.actor = nextHandle;
          void submitMessengerMessage(messenger, profile, nextHandle, text, ownSpace, options, attachment, replyTo);
        }
      });
      return;
    }
    void submitMessengerMessage(messenger, profile, currentActor || "guest", text, ownSpace, options, attachment, replyTo);
  });
  attachButton?.addEventListener("click", () => {
    fileInput?.click();
  });
  fileInput?.addEventListener("change", () => {
    const file = fileInput.files?.[0];
    fileInput.value = "";
    if (!file) {
      return;
    }
    void messageAttachmentFromFile(file)
      .then((attachment) => {
        if (!attachment) {
          setMessengerNote(messenger, messageAttachmentRejectText(file));
          return;
        }
        setMessengerAttachment(messenger, attachment);
        setMessengerNote(messenger, personalFileMeta({
          id: "attachment",
          name: attachment.name,
          type: attachment.type,
          size: attachment.size,
          dataUrl: attachment.dataUrl,
          createdAt: new Date().toISOString()
        }));
        textarea.focus();
      })
      .catch(() => {
        setMessengerNote(messenger, messageAttachmentRejectText(file));
      });
  });
  messenger.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const clearReply = target?.closest<HTMLElement>("[data-reply-clear]");
    if (clearReply && messenger.contains(clearReply)) {
      event.preventDefault();
      clearMessengerReply(messenger);
      textarea.focus();
      return;
    }
    const clearAttachment = target?.closest<HTMLElement>("[data-attachment-clear]");
    if (clearAttachment && messenger.contains(clearAttachment)) {
      event.preventDefault();
      clearMessengerAttachment(messenger);
      textarea.focus();
      return;
    }
    const replyButton = target?.closest<HTMLButtonElement>("[data-thread-reply]");
    if (replyButton && messenger.contains(replyButton)) {
      event.preventDefault();
      const reply = threadLineReplyRefFromNode(replyButton.closest<HTMLElement>("[data-thread-line]"));
      if (reply) {
        setMessengerReply(messenger, reply);
        textarea.focus();
      }
      return;
    }
    const copyButton = target?.closest<HTMLButtonElement>("[data-thread-copy]");
    if (copyButton && messenger.contains(copyButton)) {
      event.preventDefault();
      const line = copyButton.closest<HTMLElement>("[data-thread-line]");
      const text = cleanText(line?.dataset.threadText || "", 420);
      void copyText(text);
      setMessengerNote(messenger, text ? "Скопировано." : "Нечего копировать.");
      clearThreadSelection(messenger);
      return;
    }
    const reactButton = target?.closest<HTMLButtonElement>("[data-thread-react]");
    if (reactButton && messenger.contains(reactButton)) {
      event.preventDefault();
      void submitMessengerReaction(messenger, profile, reactButton, ownSpace, options);
      selectThreadLine(messenger, reactButton.closest<HTMLElement>("[data-thread-line]"), true);
      return;
    }
    const notificationEnable = target?.closest<HTMLButtonElement>("[data-notification-enable]");
    if (notificationEnable && messenger.contains(notificationEnable)) {
      event.preventDefault();
      void enablePersonalNotifications(root, notificationEnable, profile, options);
      return;
    }
    const notificationDismiss = target?.closest<HTMLButtonElement>("[data-notification-dismiss]");
    if (notificationDismiss && messenger.contains(notificationDismiss)) {
      event.preventDefault();
      dismissPersonalNotificationPrompt(messenger, options);
      return;
    }
    const line = target?.closest<HTMLElement>("[data-thread-line]");
    if (line && messenger.contains(line) && !isMessengerInteractiveTarget(target)) {
      if (skipNextLineClick) {
        skipNextLineClick = false;
        return;
      }
      if (messenger.querySelector(".personal-thread-line.is-selected")) {
        event.preventDefault();
        toggleThreadLineSelection(messenger, line);
      }
    }
  });
  messenger.addEventListener("pointerdown", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const line = target?.closest<HTMLElement>("[data-thread-line]");
    if (!line || !messenger.contains(line) || isMessengerInteractiveTarget(target)) {
      return;
    }
    cancelHold();
    holdLine = line;
    holdX = event.clientX;
    holdY = event.clientY;
    holdTimer = window.setTimeout(() => {
      if (!holdLine) {
        return;
      }
      skipNextLineClick = true;
      selectThreadLine(messenger, holdLine, true);
      holdLine.focus({ preventScroll: true });
      navigator.vibrate?.(18);
      holdTimer = 0;
    }, 420);
  });
  messenger.addEventListener("pointermove", (event) => {
    if (!holdTimer) {
      return;
    }
    if (Math.abs(event.clientX - holdX) > 8 || Math.abs(event.clientY - holdY) > 8) {
      cancelHold();
    }
  });
  messenger.addEventListener("pointerup", cancelHold);
  messenger.addEventListener("pointercancel", cancelHold);
  messenger.addEventListener("contextmenu", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const line = target?.closest<HTMLElement>("[data-thread-line]");
    if (!line || !messenger.contains(line) || isMessengerInteractiveTarget(target)) {
      return;
    }
    event.preventDefault();
    selectThreadLine(messenger, line, true);
    line.focus({ preventScroll: true });
  });
  messenger.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      clearThreadSelection(messenger);
    }
  });
  searchInput?.addEventListener("input", () => {
    filterMessengerThread(messenger, searchInput.value);
  });
  inboxSearch?.addEventListener("input", () => {
    filterOwnerInbox(messenger, inboxSearch.value);
  });
  textarea.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !event.shiftKey && !event.ctrlKey && !event.metaKey && !event.altKey) {
      clearThreadSelection(messenger);
      clearMessengerReply(messenger);
      return;
    }
    if (event.key === "Enter" && (event.ctrlKey || event.metaKey) && !event.shiftKey && !event.altKey && !event.isComposing) {
      event.preventDefault();
      form.requestSubmit();
    }
  });
  const notificationRequest = personalNotificationRequest(root, profile, options);
  if (notificationRequest && personalNotificationsAlreadyGranted()) {
    void options.syncNotifications(notificationRequest);
  }
  updatePersonalNotificationPrompt(root, profile, options, !ownSpace && loadPersonalThread(profile, cleanRoutePart(messenger.dataset.actor || "") || "guest").length > 0);
}

async function submitMessengerMessage(
  messenger: HTMLElement,
  profile: PersonalSpaceProfile,
  actor: string,
  text: string,
  ownSpace: boolean,
  options: PersonalSpacePageOptions,
  attachment?: PersonalSpaceMessageAttachment,
  replyTo?: PersonalSpaceMessageReplyRef
): Promise<void> {
  const cleanActor = cleanRoutePart(actor) || "guest";
  const clientId = ownSpace
    ? cleanMessageClientId(messenger.dataset.clientId || "")
    : personalMessageClientIdFor(profile, cleanActor);
  if (!ownSpace) {
    messenger.dataset.clientId = clientId;
  }
  const textarea = messenger.querySelector<HTMLTextAreaElement>("textarea[name='text']");
  const button = messenger.querySelector<HTMLButtonElement>("button[type='submit']");
  const thread = messenger.querySelector<HTMLElement>("[data-personal-thread]");
  if (ownSpace && !clientId) {
    setMessengerNote(messenger, "Выберите диалог.");
    return;
  }
  const storageKey = ownSpace ? clientId : cleanActor;
  let lines = loadPersonalThread(profile, storageKey);
  const optimisticId = localItemId("message");
  const optimisticLine: PersonalThreadLine = {
    id: optimisticId,
    author: ownSpace ? profile.handle : cleanActor,
    text,
    createdAt: new Date().toISOString(),
    mine: true,
    ...(replyTo ? { replyTo } : {}),
    ...(attachment ? { attachment } : {}),
    reactions: {},
    status: "sending"
  };
  lines = [...lines, optimisticLine].slice(-80);
  if (button) {
    button.disabled = true;
  }
  if (textarea) {
    textarea.value = "";
    textarea.focus();
  }
  renderMessengerThread(thread, lines, ownSpace);
  setMessengerNote(messenger, "Отправляю...");
  const result = ownSpace
    ? await options.replyMessage(options.route, {
      clientId,
      text,
      ...(replyTo ? { replyTo } : {}),
      ...(attachment ? { attachment } : {})
    })
    : await sendPersonalSpaceMessage(profile, {
      author: cleanActor,
      text,
      clientId,
      ...(replyTo ? { replyTo } : {}),
      ...(attachment ? { attachment } : {})
    });
  if (!result.ok) {
    lines = lines.map((line) => line.id === optimisticId ? { ...line, status: "failed" } : line);
    renderMessengerThread(thread, lines, ownSpace);
    setMessengerNote(messenger, result.message);
    if (button) {
      button.disabled = false;
    }
    return;
  }
  clearMessengerAttachment(messenger);
  clearMessengerReply(messenger);
  lines = lines.map((line) => line.id === optimisticId ? { ...line, status: "sent" } : line);
  savePersonalThread(profile, storageKey, lines);
  renderMessengerThread(thread, lines, ownSpace);
  setMessengerNote(messenger, result.message);
  updatePersonalNotificationPrompt(rootForMessenger(messenger), profile, options, true);
  await hydrateMessengerThread(rootForMessenger(messenger), profile, options, clientId, ownSpace);
  startThreadRefresh(rootForMessenger(messenger), profile, options, clientId, ownSpace);
  if (button) {
    button.disabled = false;
  }
}

function renderMessengerThread(thread: HTMLElement | null, lines: readonly PersonalThreadLine[], ownSpace: boolean): void {
  if (!thread) {
    return;
  }
  const signature = personalThreadSignature(lines);
  if (thread.dataset.threadSignature === signature) {
    return;
  }
  const shouldStickToBottom = !thread.dataset.threadSignature
    || thread.scrollHeight - thread.scrollTop - thread.clientHeight < 72;
  thread.innerHTML = renderThreadLineItems(lines, ownSpace);
  thread.dataset.threadSignature = signature;
  const messenger = thread.closest<HTMLElement>("[data-personal-messenger]");
  const search = messenger?.querySelector<HTMLInputElement>("[data-thread-search]");
  if (messenger && search?.value) {
    filterMessengerThread(messenger, search.value);
  }
  if (shouldStickToBottom) {
    thread.scrollTop = thread.scrollHeight;
  }
}

function personalThreadSignature(lines: readonly PersonalThreadLine[]): string {
  return lines.map((line) => [
    line.id,
    line.text,
    line.status || "",
    line.createdAt,
    line.replyTo ? `${line.replyTo.id}:${line.replyTo.text}` : "",
    line.attachment ? `${line.attachment.name}:${line.attachment.size}` : "",
    Object.entries(line.reactions || {}).sort(([left], [right]) => left.localeCompare(right)).map(([key, count]) => `${key}:${count}`).join(",")
  ].join("|")).join("~");
}

function setMessengerNote(messenger: HTMLElement, message: string): void {
  const note = messenger.querySelector<HTMLElement>("[data-action-note]");
  if (note) {
    note.textContent = message;
  }
}

function setMessengerAttachment(messenger: HTMLElement, attachment: PersonalSpaceMessageAttachment): void {
  personalMessengerAttachments.set(messenger, attachment);
  const chip = messenger.querySelector<HTMLElement>("[data-attachment-chip]");
  const label = chip?.querySelector<HTMLElement>("span");
  if (chip) {
    chip.hidden = false;
  }
  if (label) {
    label.textContent = `${attachment.name} · ${personalFileSize(attachment.size)}`;
  }
}

function clearMessengerAttachment(messenger: HTMLElement): void {
  personalMessengerAttachments.delete(messenger);
  const chip = messenger.querySelector<HTMLElement>("[data-attachment-chip]");
  const label = chip?.querySelector<HTMLElement>("span");
  if (chip) {
    chip.hidden = true;
  }
  if (label) {
    label.textContent = "";
  }
}

function setMessengerReply(messenger: HTMLElement, reply: PersonalSpaceMessageReplyRef): void {
  personalMessengerReplies.set(messenger, reply);
  const preview = messenger.querySelector<HTMLElement>("[data-reply-draft]");
  const label = preview?.querySelector<HTMLElement>("span");
  if (preview) {
    preview.hidden = false;
  }
  if (label) {
    label.textContent = `${reply.author}: ${reply.text}`;
  }
}

function clearMessengerReply(messenger: HTMLElement): void {
  personalMessengerReplies.delete(messenger);
  const preview = messenger.querySelector<HTMLElement>("[data-reply-draft]");
  const label = preview?.querySelector<HTMLElement>("span");
  if (preview) {
    preview.hidden = true;
  }
  if (label) {
    label.textContent = "";
  }
}

async function messageAttachmentFromFile(file: File): Promise<PersonalSpaceMessageAttachment | null> {
  if (file.size <= 0 || file.size > personalFileMaxBytes) {
    return null;
  }
  const dataUrl = await fileToDataUrl(file);
  return normalizeMessageAttachment({
    name: file.name || "file",
    type: file.type || "application/octet-stream",
    size: file.size,
    dataUrl
  });
}

function messageAttachmentRejectText(file: File): string {
  if (file.size <= 0) {
    return "Файл пустой.";
  }
  if (file.size > personalFileMaxBytes) {
    return `Файл больше ${personalFileSize(personalFileMaxBytes)}.`;
  }
  return "Файл не прочитался.";
}

function filterMessengerThread(messenger: HTMLElement, query: string): void {
  const needle = cleanText(query, 80).toLocaleLowerCase("ru-RU");
  messenger.querySelectorAll<HTMLElement>("[data-thread-line]").forEach((line) => {
    if (!needle) {
      line.hidden = false;
      return;
    }
    const haystack = `${line.dataset.threadAuthor || ""} ${line.dataset.threadText || ""}`.toLocaleLowerCase("ru-RU");
    line.hidden = !haystack.includes(needle);
  });
}

function filterOwnerInbox(root: ParentNode, query: string): void {
  const needle = cleanText(query, 80).toLocaleLowerCase("ru-RU");
  root.querySelectorAll<HTMLElement>("[data-inbox-client-id]").forEach((item) => {
    if (!needle) {
      item.hidden = false;
      return;
    }
    const haystack = `${item.dataset.inboxAuthor || ""} ${item.dataset.inboxSearch || ""}`.toLocaleLowerCase("ru-RU");
    item.hidden = !haystack.includes(needle);
  });
}

function isMessengerInteractiveTarget(target: Element | null): boolean {
  return Boolean(target?.closest("button,a,input,textarea,label,select,[contenteditable='true']"));
}

function setThreadLineSelected(line: HTMLElement, selected: boolean): void {
  line.classList.toggle("is-selected", selected);
  line.setAttribute("aria-selected", selected ? "true" : "false");
}

function clearThreadSelection(messenger: HTMLElement): void {
  messenger.querySelectorAll<HTMLElement>(".personal-thread-line.is-selected").forEach((line) => {
    setThreadLineSelected(line, false);
  });
}

function selectThreadLine(messenger: HTMLElement, line: HTMLElement | null, single: boolean): void {
  if (!line || !messenger.contains(line)) {
    return;
  }
  if (single) {
    clearThreadSelection(messenger);
  }
  setThreadLineSelected(line, true);
  setMessengerNote(messenger, "Сообщение выбрано.");
}

function toggleThreadLineSelection(messenger: HTMLElement, line: HTMLElement): void {
  const selected = line.classList.contains("is-selected");
  setThreadLineSelected(line, !selected);
  if (!messenger.querySelector(".personal-thread-line.is-selected")) {
    setMessengerNote(messenger, "");
  } else {
    setMessengerNote(messenger, "Сообщение выбрано.");
  }
}

function threadLineReplyRefFromNode(node: HTMLElement | null): PersonalSpaceMessageReplyRef | null {
  if (!node) {
    return null;
  }
  return normalizeMessageReplyRef({
    id: node.dataset.threadId || "",
    author: node.dataset.threadAuthor || "",
    text: node.dataset.threadText || ""
  });
}

async function submitMessengerReaction(
  messenger: HTMLElement,
  profile: PersonalSpaceProfile,
  button: HTMLButtonElement,
  ownSpace: boolean,
  options: PersonalSpacePageOptions
): Promise<void> {
  const line = button.closest<HTMLElement>("[data-thread-line]");
  const messageId = cleanText(line?.dataset.threadId || "", 80);
  const key = normalizeMessageReactionKey(button.dataset.reactionKey);
  const clientId = ownSpace
    ? cleanMessageClientId(messenger.dataset.clientId || "")
    : personalMessageClientIdFor(profile, cleanRoutePart(messenger.dataset.actor || loadLocalHandle() || ""));
  if (!ownSpace) {
    messenger.dataset.clientId = clientId;
  }
  const actorId = ownSpace ? `owner:${cleanRoutePart(profile.handle) || "owner"}` : clientId;
  if (!messageId || !key || !clientId || !cleanMessageReactionActorId(actorId)) {
    setMessengerNote(messenger, "Реакция не сохранилась.");
    return;
  }
  button.disabled = true;
  try {
    const result = await options.reactMessage(options.route, { messageId, clientId, actorId, key });
    setMessengerNote(messenger, result.message);
    if (result.ok) {
      await hydrateMessengerThread(rootForMessenger(messenger), profile, options, clientId, ownSpace);
    }
  } catch {
    setMessengerNote(messenger, "Реакция не сохранилась.");
  } finally {
    button.disabled = false;
  }
}

function renderThreadLines(lines: readonly PersonalThreadLine[], ownSpace: boolean, compact: boolean): string {
  return `
    <div class="personal-thread${compact ? " is-compact" : ""}" data-personal-thread>
      ${renderThreadLineItems(lines, ownSpace)}
    </div>
  `;
}

async function hydrateOwnerInbox(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): Promise<void> {
  const inbox = root.querySelector<HTMLElement>("[data-personal-inbox] .personal-inbox-list");
  if (!inbox) {
    return;
  }
  const messenger = root.querySelector<HTMLElement>("[data-personal-messenger]");
  const selectedClientId = cleanMessageClientId(messenger?.dataset.clientId || "");
  const messages = await options.loadInbox(options.route);
  if (!inbox.isConnected) {
    return;
  }
  noticePersonalOwnerInbox(profile, options.route, messages);
  const signature = personalInboxSignature(messages, selectedClientId);
  if (inbox.dataset.inboxSignature !== signature) {
    inbox.innerHTML = renderInboxItems(messages, selectedClientId);
    inbox.dataset.inboxSignature = signature;
  }
  filterOwnerInbox(root, root.querySelector<HTMLInputElement>("[data-inbox-search]")?.value || "");
  if (!selectedClientId && messages[0]?.clientId && document.visibilityState === "visible") {
    const first = inbox.querySelector<HTMLElement>("[data-inbox-client-id]");
    if (first) {
      await openOwnerConversation(root, first, profile, options);
    }
  }
}

function personalInboxSignature(messages: readonly PersonalSpaceInboxMessage[], selectedClientId: string): string {
  return [
    selectedClientId,
    messages.slice(0, 8).map((message) => [
      message.id,
      message.clientId,
      message.author,
      message.sender,
      message.text,
      message.readByOwnerAt || "",
      message.readByVisitorAt || "",
      message.createdAt,
      message.attachment ? `${message.attachment.name}:${message.attachment.size}` : ""
    ].join("|")).join("~")
  ].join("::");
}

function startOwnerInboxRefresh(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): void {
  const key = routeUrl(options.route);
  if (personalInboxRefreshTimer && personalInboxRefreshKey === key) {
    return;
  }
  stopOwnerInboxRefresh();
  personalInboxRefreshKey = key;
  personalInboxRefreshTimer = window.setInterval(() => {
    const inbox = root.querySelector<HTMLElement>("[data-personal-inbox] .personal-inbox-list");
    if (!inbox || !inbox.isConnected) {
      stopOwnerInboxRefresh();
      return;
    }
    void hydrateOwnerInbox(root, profile, options);
  }, 3500);
}

function stopOwnerInboxRefresh(): void {
  if (personalInboxRefreshTimer) {
    window.clearInterval(personalInboxRefreshTimer);
    personalInboxRefreshTimer = 0;
  }
  personalInboxRefreshKey = "";
}

async function openOwnerConversation(root: HTMLElement, node: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): Promise<void> {
  const clientId = cleanMessageClientId(node.dataset.inboxClientId || "");
  if (!clientId) {
    return;
  }
  const actor = cleanRoutePart(node.dataset.inboxAuthor || "") || "guest";
  const messenger = root.querySelector<HTMLElement>("[data-personal-messenger]");
  if (!messenger) {
    return;
  }
  messenger.dataset.clientId = clientId;
  messenger.dataset.actor = actor;
  setMessengerRoom(messenger, actor, "Диалог открыт", "live");
  root.querySelectorAll<HTMLElement>("[data-inbox-client-id]").forEach((item) => {
    item.classList.toggle("is-active", item === node);
  });
  await hydrateMessengerThread(root, profile, options, clientId, true);
  startThreadRefresh(root, profile, options, clientId, true);
  messenger.querySelector<HTMLTextAreaElement>("textarea[name='text']")?.focus();
}

async function hydrateMessengerThread(
  root: HTMLElement,
  profile: PersonalSpaceProfile,
  options: PersonalSpacePageOptions,
  clientId: string,
  ownSpace: boolean,
  refreshOptions: { readonly peek?: boolean } = {}
): Promise<void> {
  const cleanClientId = cleanMessageClientId(clientId);
  const messenger = root.querySelector<HTMLElement>("[data-personal-messenger]");
  if (!cleanClientId || !messenger) {
    return;
  }
  if (ownSpace && cleanMessageClientId(messenger.dataset.clientId || "") !== cleanClientId) {
    return;
  }
  const messages = await options.loadThread(options.route, {
    clientId: cleanClientId,
    limit: 80,
    ...(refreshOptions.peek ? { peek: true } : {})
  });
  if (!messenger.isConnected || (ownSpace && cleanMessageClientId(messenger.dataset.clientId || "") !== cleanClientId)) {
    return;
  }
  noticePersonalThreadMessages(profile, options.route, messages, cleanClientId, ownSpace);
  if (messages.length === 0) {
    updatePersonalNotificationPrompt(root, profile, options, false);
    return;
  }
  const actor = cleanRoutePart(messenger.dataset.actor || loadLocalHandle() || "guest") || "guest";
  const lines = threadMessagesToLines(messages, profile, actor, ownSpace);
  savePersonalThread(profile, ownSpace ? cleanClientId : actor, lines);
  renderMessengerThread(messenger.querySelector<HTMLElement>("[data-personal-thread]"), lines, ownSpace);
  updatePersonalNotificationPrompt(root, profile, options, true);
}

function startThreadRefresh(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions, clientId: string, ownSpace: boolean): void {
  const cleanClientId = cleanMessageClientId(clientId);
  if (!cleanClientId) {
    return;
  }
  const key = `${routeUrl(options.route)}:${cleanClientId}:${ownSpace ? "owner" : "visitor"}`;
  if (personalThreadRefreshTimer && personalThreadRefreshKey === key) {
    return;
  }
  stopThreadRefresh();
  personalThreadRefreshKey = key;
  personalThreadRefreshTimer = window.setInterval(() => {
    const messenger = root.querySelector<HTMLElement>("[data-personal-messenger]");
    if (!messenger || !messenger.isConnected) {
      stopThreadRefresh();
      return;
    }
    if (ownSpace && cleanMessageClientId(messenger.dataset.clientId || "") !== cleanClientId) {
      return;
    }
    void hydrateMessengerThread(root, profile, options, cleanClientId, ownSpace, { peek: document.visibilityState !== "visible" });
  }, 3000);
  void hydrateMessengerThread(root, profile, options, cleanClientId, ownSpace);
}

function stopThreadRefresh(): void {
  if (personalThreadRefreshTimer) {
    window.clearInterval(personalThreadRefreshTimer);
    personalThreadRefreshTimer = 0;
  }
  personalThreadRefreshKey = "";
}

function noticePersonalOwnerInbox(
  profile: PersonalSpaceProfile,
  route: PersonalSpaceRoute,
  messages: readonly PersonalSpaceInboxMessage[]
): void {
  const group = `owner:${routeUrl(route)}`;
  const initialized = personalMessageNoticeGroups.has(group);
  for (const message of messages) {
    const key = `${group}:${message.clientId}`;
    const previous = personalMessageNoticeLatest.get(key) || "";
    personalMessageNoticeLatest.set(key, message.id);
    if (!initialized || previous === message.id || message.sender === "owner" || message.readByOwnerAt) {
      continue;
    }
    showPersonalMessageNotice(profile, message.author, messagePreviewText(message), routeUrl(route), personalActorIconUrl(message.author));
  }
  personalMessageNoticeGroups.add(group);
}

function noticePersonalThreadMessages(
  profile: PersonalSpaceProfile,
  route: PersonalSpaceRoute,
  messages: readonly PersonalSpaceThreadMessage[],
  clientId: string,
  ownSpace: boolean
): void {
  const latest = messages[messages.length - 1];
  const group = `thread:${routeUrl(route)}:${clientId}:${ownSpace ? "owner" : "visitor"}`;
  if (!latest) {
    personalMessageNoticeGroups.add(group);
    return;
  }
  const initialized = personalMessageNoticeGroups.has(group);
  const previous = personalMessageNoticeLatest.get(group) || "";
  personalMessageNoticeLatest.set(group, latest.id);
  personalMessageNoticeGroups.add(group);
  if (ownSpace) {
    return;
  }
  const incoming = latest.sender === "owner";
  if (!initialized || previous === latest.id || !incoming) {
    return;
  }
  if (!latest.readByVisitorAt) {
    showPersonalMessageNotice(profile, profile.displayName || latest.author, messagePreviewText(latest), routeUrl(route), personalProfileIconUrl(profile));
  }
}

async function showPersonalMessageNotice(profile: PersonalSpaceProfile, title: string, body: string, url: string, iconUrl = ""): Promise<void> {
  if (document.visibilityState === "visible") {
    return;
  }
  const noticeTitle = cleanText(title, 80) || profile.displayName || "Соты";
  const vibration = [24, 36, 24] as const;
  navigator.vibrate?.(vibration);
  if (await hasPushNotificationSubscription()) {
    return;
  }
  void showSystemAttentionNotice(url, {
    title: noticeTitle,
    body: cleanText(body, 160) || "Новое сообщение",
    icon: iconUrl || personalProfileIconUrl(profile),
    badge: "/icon.svg",
    tag: personalNoticeTag(url, noticeTitle),
    silent: false,
    vibrate: vibration,
    timestamp: Date.now()
  });
}

function rootForMessenger(messenger: HTMLElement): HTMLElement {
  return messenger.closest<HTMLElement>("#app") || document.querySelector<HTMLElement>("#app") || messenger;
}

function setMessengerRoom(messenger: HTMLElement, title: string, status: string, badge: string): void {
  const cleanTitle = cleanText(title, 80) || "Диалог";
  const titleNode = messenger.querySelector<HTMLElement>("[data-room-title]");
  const statusNode = messenger.querySelector<HTMLElement>("[data-room-status]");
  const badgeNode = messenger.querySelector<HTMLElement>("[data-room-badge]");
  const avatarNode = messenger.querySelector<HTMLElement>("[data-room-avatar]");
  if (titleNode) {
    titleNode.textContent = cleanTitle;
  }
  if (statusNode) {
    statusNode.textContent = status;
  }
  if (badgeNode) {
    badgeNode.textContent = badge;
  }
  if (avatarNode) {
    avatarNode.textContent = initials(cleanTitle);
  }
}

function threadMessagesToLines(
  messages: readonly PersonalSpaceThreadMessage[],
  profile: PersonalSpaceProfile,
  actor: string,
  ownSpace: boolean
): readonly PersonalThreadLine[] {
  const ownerName = profile.shortName || profile.displayName || profile.handle;
  return messages.map((message) => {
    const fromOwner = message.sender === "owner";
    const mine = ownSpace ? fromOwner : !fromOwner;
    const seen = ownSpace
      ? fromOwner && Boolean(message.readByVisitorAt)
      : !fromOwner && Boolean(message.readByOwnerAt);
    return {
      id: message.id,
      author: fromOwner ? ownerName : cleanText(message.author, 80) || actor || "guest",
      text: message.text,
      createdAt: message.createdAt,
      mine,
      ...(message.replyTo ? { replyTo: message.replyTo } : {}),
      ...(message.attachment ? { attachment: message.attachment } : {}),
      reactions: message.reactions || {},
      ...(mine ? { status: seen ? "seen" as const : "sent" as const } : {})
    };
  }).slice(-80);
}

function renderInboxItems(messages: readonly PersonalSpaceInboxMessage[], selectedClientId = ""): string {
  if (messages.length === 0) {
    return `<section class="personal-empty">${icon("mail")} <span>Сообщений пока нет.</span></section>`;
  }
  return messages.slice(0, 8).map((message) => {
    const unread = message.sender !== "owner" && !message.readByOwnerAt;
    const state = unread ? "новое" : message.sender === "owner" ? "вы ответили" : "прочитано";
    return `
    <button class="personal-inbox-item${message.clientId === selectedClientId ? " is-active" : ""}${unread ? " has-unread" : ""}" type="button" data-inbox-client-id="${escapeAttr(message.clientId)}" data-inbox-author="${escapeAttr(message.author)}" data-inbox-search="${escapeAttr(messagePreviewText(message))}">
      <span>${escapeHtml(message.author)}</span>
      <time>${escapeHtml(threadTime(message.createdAt))}</time>
      <p>${escapeHtml(messagePreviewText(message))}</p>
      <small>${escapeHtml(state)}</small>
    </button>
  `;
  }).join("");
}

function renderThreadLineItems(lines: readonly PersonalThreadLine[], ownSpace: boolean): string {
  if (lines.length === 0) {
    return `<section class="personal-empty personal-thread-empty">${icon("mail")} <span>${escapeHtml(ownSpace ? "Выберите диалог справа." : "Переписка начнется здесь.")}</span></section>`;
  }
  return lines.map((line) => `
    <article class="personal-thread-line${line.mine ? " is-mine" : ""}${line.status ? ` is-${line.status}` : ""}" data-thread-line data-thread-id="${escapeAttr(line.id)}" data-thread-author="${escapeAttr(line.author)}" data-thread-text="${escapeAttr(messagePreviewText(line))}" tabindex="0" aria-selected="false">
      <span>${escapeHtml(line.author)}</span>
      ${line.replyTo ? renderMessageReplyRef(line.replyTo) : ""}
      ${line.text ? `<p>${escapeHtml(line.text)}</p>` : ""}
      ${line.attachment ? renderMessageAttachment(line.attachment) : ""}
      ${renderMessageReactions(line.reactions)}
      ${line.status === "sending" ? "" : `
        <div class="personal-thread-actions" aria-label="${escapeAttr("Действия сообщения")}">
          <button type="button" data-thread-reply aria-label="${escapeAttr("Ответить")}">${icon("reply")}</button>
          <button type="button" data-thread-copy aria-label="${escapeAttr("Копировать")}">${icon("copy")}</button>
          <button type="button" data-thread-react data-reaction-key="heart" aria-label="${escapeAttr("Сердце")}">${icon("heart")}</button>
          <button type="button" data-thread-react data-reaction-key="check" aria-label="${escapeAttr("Готово")}">${icon("check")}</button>
        </div>
      `}
      ${renderThreadLineMeta(line)}
    </article>
  `).join("");
}

function messagePreviewText(message: Pick<PersonalSpaceInboxMessage, "text" | "attachment"> | Pick<PersonalThreadLine, "text" | "attachment">): string {
  const text = cleanText(message.text, 140);
  if (text) {
    return text;
  }
  if (message.attachment?.name) {
    return `Файл: ${message.attachment.name}`;
  }
  return "Сообщение";
}

function renderMessageReplyRef(reply: PersonalSpaceMessageReplyRef): string {
  return `
    <blockquote class="personal-thread-reply">
      <b>${escapeHtml(reply.author)}</b>
      <span>${escapeHtml(reply.text)}</span>
    </blockquote>
  `;
}

function renderMessageAttachment(attachment: PersonalSpaceMessageAttachment): string {
  const isImage = attachment.type.startsWith("image/");
  return `
    <a class="personal-thread-attachment${isImage ? " is-image" : ""}" href="${escapeAttr(attachment.dataUrl)}" download="${escapeAttr(attachment.name)}" target="_blank" rel="noopener">
      ${isImage ? `<img src="${escapeAttr(attachment.dataUrl)}" alt="${escapeAttr(attachment.name)}" loading="lazy">` : icon("clip")}
      <span>
        <b>${escapeHtml(attachment.name)}</b>
        <small>${escapeHtml(personalFileSize(attachment.size))}</small>
      </span>
    </a>
  `;
}

function renderMessageReactions(reactions: PersonalSpaceMessageReactions): string {
  const entries = Object.entries(reactions)
    .map(([key, count]) => [normalizeMessageReactionKey(key), Math.max(0, Math.round(Number(count) || 0))] as const)
    .filter(([key, count]) => Boolean(key) && count > 0)
    .sort(([left], [right]) => messageReactionOrder(left) - messageReactionOrder(right));
  if (entries.length === 0) {
    return "";
  }
  return `
    <div class="personal-thread-reactions">
      ${entries.map(([key, count]) => `<span aria-label="${escapeAttr(messageReactionLabel(key))}">${icon(messageReactionIcon(key))}<b>${escapeHtml(formatReactionCount(count))}</b></span>`).join("")}
    </div>
  `;
}

function renderThreadLineMeta(line: PersonalThreadLine): string {
  const label = threadLineTimeText(line);
  return `
    <time class="personal-thread-meta" datetime="${escapeAttr(line.createdAt)}" aria-label="${escapeAttr(label)}">
      <span class="personal-thread-clock">${escapeHtml(threadTime(line.createdAt))}</span>
      ${renderThreadDelivery(line)}
    </time>
  `;
}

function renderThreadDelivery(line: PersonalThreadLine): string {
  if (!line.mine || !line.status) {
    return "";
  }
  const label = messageDeliveryLabel(line.status);
  if (line.status === "seen") {
    return `<span class="personal-thread-delivery is-seen" aria-label="${escapeAttr(label)}">${icon("check")}${icon("check")}</span>`;
  }
  if (line.status === "sent" || line.status === "saved") {
    return `<span class="personal-thread-delivery is-sent" aria-label="${escapeAttr(label)}">${icon("check")}</span>`;
  }
  if (line.status === "sending") {
    return `<span class="personal-thread-delivery is-sending" aria-label="${escapeAttr(label)}">...</span>`;
  }
  return `<span class="personal-thread-delivery is-failed" aria-label="${escapeAttr(label)}">!</span>`;
}

function messageDeliveryLabel(status: PersonalThreadLine["status"] | undefined): string {
  if (status === "seen") {
    return "прочитано";
  }
  if (status === "sending") {
    return "отправляю";
  }
  if (status === "sent" || status === "saved") {
    return "отправлено";
  }
  if (status === "failed") {
    return "не отправлено";
  }
  return "";
}

function threadLineTimeText(line: PersonalThreadLine): string {
  const time = threadTime(line.createdAt);
  const delivery = line.mine ? messageDeliveryLabel(line.status) : "";
  return delivery ? `${time}, ${delivery}` : time;
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

function personalMessageClientIdFor(profile: PersonalSpaceProfile, actor: string): string {
  const local = cleanRoutePart(actor);
  const remote = cleanRoutePart(profile.handle);
  if (!local || local === "guest" || !remote || local === remote) {
    return personalMessageClientId();
  }
  const pair = local < remote ? `${local}:${remote}` : `${remote}:${local}`;
  return cleanMessageClientId(`mc_dm_${stableMessageToken(pair)}`) || personalMessageClientId();
}

function stableMessageToken(value: string): string {
  let left = 0x811c9dc5;
  let right = 0x45d9f3b;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    left = Math.imul(left ^ code, 0x01000193) >>> 0;
    right = Math.imul(right ^ code, 0x27d4eb2d) >>> 0;
  }
  return `${left.toString(36).padStart(7, "0")}_${right.toString(36).padStart(7, "0")}`;
}

function cleanMessageClientId(value: string): string {
  const text = cleanText(value, 80);
  return /^mc_[a-z0-9_-]{8,76}$/iu.test(text) ? text : "";
}

function cleanMessageReactionActorId(value: unknown): string {
  const text = cleanText(value, 96);
  return /^(mc_[a-z0-9_-]{8,76}|owner:[\p{L}\p{N}._-]{1,64})$/iu.test(text) ? text : "";
}

function normalizeMessageReactionKey(value: unknown): PersonalSpaceMessageReactionDraft["key"] | "" {
  const text = cleanText(value, 16);
  return text === "heart" || text === "check" || text === "like" ? text : "";
}

function messageReactionOrder(value: string): number {
  if (value === "heart") {
    return 1;
  }
  if (value === "check") {
    return 2;
  }
  if (value === "like") {
    return 3;
  }
  return 9;
}

function messageReactionLabel(value: string): string {
  if (value === "check") {
    return "Готово";
  }
  if (value === "like") {
    return "Нравится";
  }
  return "Сердце";
}

function messageReactionIcon(value: string): IconName {
  return value === "check" ? "check" : "heart";
}

function normalizeMessageSender(value: unknown): PersonalSpaceMessageSender {
  return cleanText(value, 24).toLowerCase() === "owner" ? "owner" : "visitor";
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

function normalizeMessageReplyRef(value: unknown): PersonalSpaceMessageReplyRef | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = cleanText(value.id, 80);
  const text = cleanText(value.text, 140);
  if (!id || !text) {
    return null;
  }
  return {
    id,
    author: cleanText(value.author, 80) || "guest",
    text
  };
}

function normalizeMessageAttachment(value: unknown): PersonalSpaceMessageAttachment | null {
  if (!isRecord(value)) {
    return null;
  }
  const name = cleanText(value.name, 120) || "file";
  const type = cleanText(value.type, 80).toLowerCase() || "application/octet-stream";
  const size = Math.round(Number(value.size));
  const dataUrl = cleanPersonalFileDataUrl(value.dataUrl);
  if (!Number.isSafeInteger(size) || size <= 0 || size > personalFileMaxBytes || !dataUrl) {
    return null;
  }
  return {
    name,
    type,
    size,
    dataUrl
  };
}

function normalizeMessageReactions(value: unknown): PersonalSpaceMessageReactions {
  if (!isRecord(value)) {
    return {};
  }
  const result: Record<string, number> = {};
  for (const [rawKey, rawCount] of Object.entries(value)) {
    const key = normalizeMessageReactionKey(rawKey);
    const count = Math.max(0, Math.round(Number(rawCount) || 0));
    if (key && count > 0) {
      result[key] = Math.min(999, count);
    }
  }
  return result;
}

function normalizeThreadLineStatus(value: unknown): PersonalThreadLine["status"] | "" {
  const text = cleanText(value, 16);
  return text === "sending" || text === "sent" || text === "seen" || text === "failed" || text === "saved" ? text : "";
}

function normalizeThreadLine(value: unknown): PersonalThreadLine | null {
  if (!isRecord(value)) {
    return null;
  }
  const text = cleanText(value.text, 420);
  const attachment = normalizeMessageAttachment(value.attachment);
  if (!text && !attachment) {
    return null;
  }
  const replyTo = normalizeMessageReplyRef(value.replyTo);
  const status = normalizeThreadLineStatus(value.status);
  return {
    id: cleanText(value.id, 80) || localItemId("message"),
    author: cleanText(value.author, 80) || "guest",
    text,
    createdAt: cleanText(value.createdAt, 40) || new Date().toISOString(),
    mine: value.mine !== false,
    ...(replyTo ? { replyTo } : {}),
    ...(attachment ? { attachment } : {}),
    reactions: normalizeMessageReactions(value.reactions),
    ...(status ? { status } : {})
  };
}

function normalizeInboxMessage(value: unknown): PersonalSpaceInboxMessage | null {
  if (!isRecord(value)) {
    return null;
  }
  const text = cleanText(value.text, 420);
  const attachment = normalizeMessageAttachment(value.attachment);
  const clientId = cleanMessageClientId(String(value.clientId || ""));
  if ((!text && !attachment) || !clientId) {
    return null;
  }
  const replyTo = normalizeMessageReplyRef(value.replyTo);
  const readByOwnerAt = cleanText(value.readByOwnerAt, 40);
  const readByVisitorAt = cleanText(value.readByVisitorAt, 40);
  return {
    id: cleanText(value.id, 80) || localItemId("message"),
    author: cleanText(value.author, 80) || "guest",
    text,
    clientId,
    sender: normalizeMessageSender(value.sender),
    ...(replyTo ? { replyTo } : {}),
    ...(attachment ? { attachment } : {}),
    reactions: normalizeMessageReactions(value.reactions),
    ...(readByOwnerAt ? { readByOwnerAt } : {}),
    ...(readByVisitorAt ? { readByVisitorAt } : {}),
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
    <section class="personal-sheet" role="dialog" aria-modal="true" aria-label="Править карточку">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <h2>Карточка</h2>
      <form data-profile-form>
        <input name="displayName" autocomplete="name" maxlength="100" aria-label="имя" placeholder="Имя или название" value="${escapeAttr(profile.displayName)}" />
        <textarea name="about" maxlength="420" aria-label="о карточке" placeholder="Кто это и чем полезно">${escapeHtml(profile.about)}</textarea>
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
      contact: cleanText(contactInput?.value || "", 160),
      spaces: spaceUpdatesWith(profile.spaces),
      trustedHandles: profile.trustedHandles
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

function showSpaceSheet(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): void {
  closePersonalOverlay(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet" role="dialog" aria-modal="true" aria-label="Пространство">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <h2>Пространство</h2>
      <form data-space-form>
        <input name="title" maxlength="80" required placeholder="Название" />
        <input name="summary" maxlength="140" placeholder="Коротко" />
        <button type="submit">${icon("check")} Создать</button>
      </form>
      <small data-error></small>
    </section>
  `;
  root.append(overlay);
  const titleInput = overlay.querySelector<HTMLInputElement>("input[name='title']");
  const summaryInput = overlay.querySelector<HTMLInputElement>("input[name='summary']");
  titleInput?.focus();
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", () => overlay.remove());
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      overlay.remove();
    }
  });
  overlay.querySelector<HTMLFormElement>("[data-space-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    const button = overlay.querySelector<HTMLButtonElement>("button[type='submit']");
    const error = overlay.querySelector<HTMLElement>("[data-error]");
    const title = cleanText(titleInput?.value || "", 80);
    const slug = cleanRoutePart(title);
    const summary = cleanText(summaryInput?.value || "", 140) || "пространство";
    if (!title || !slug) {
      if (error) {
        error.textContent = "Добавьте название.";
      }
      return;
    }
    const nextSpace = {
      slug,
      title,
      summary
    };
    const nextSpaces = spaceUpdatesWith(profile.spaces, nextSpace);
    const update: PersonalSpaceProfileUpdate = {
      displayName: profile.displayName,
      about: profile.about,
      contact: contactValueForProfile(profile),
      trustedHandles: profile.trustedHandles,
      spaces: nextSpaces
    };
    if (button) {
      button.disabled = true;
    }
    void options.updateProfile(options.route, update)
      .then((result) => {
        if (!result.ok) {
          throw new Error(result.message);
        }
        saveCachedPersonalProfile(localSpaceProfile(profile, nextSpace, nextSpaces));
        overlay.remove();
        const href = personalNavigationTarget(`/@${encodeURIComponent(profile.handle)}/${encodeURIComponent(slug)}?layer=place`);
        window.history.pushState({}, "", href);
        window.dispatchEvent(new CustomEvent("soty-personal-routechange"));
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

function showModuleSheet(root: HTMLElement, options: PersonalSpacePageOptions): void {
  closePersonalOverlay(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet" role="dialog" aria-modal="true" aria-label="Модуль">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <h2>Модуль</h2>
      ${renderRuntimeModulePresets()}
      <form data-module-form>
        <select name="kind" aria-label="тип">
          <option value="runtime">Возможность</option>
          <option value="miniapp" selected>Mini-app</option>
          <option value="link">Ссылка</option>
        </select>
        <select name="runtime" aria-label="возможность">
          ${runtimeModuleDefinitions
            .filter((module) => module.id !== "agent")
            .map((module) => `<option value="${escapeAttr(module.target)}">${escapeHtml(module.title)}</option>`)
            .join("")}
        </select>
        <input name="title" maxlength="80" required placeholder="Название" />
        <input name="summary" maxlength="140" placeholder="Коротко" />
        <input name="href" autocomplete="url" maxlength="360" required placeholder="URL" />
        <textarea name="inlineHtml" maxlength="240000" placeholder="HTML mini-app, если нет URL"></textarea>
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
  const runtimeInput = overlay.querySelector<HTMLSelectElement>("select[name='runtime']");
  const titleInput = overlay.querySelector<HTMLInputElement>("input[name='title']");
  const summaryInput = overlay.querySelector<HTMLInputElement>("input[name='summary']");
  const hrefInput = overlay.querySelector<HTMLInputElement>("input[name='href']");
  const inlineHtmlInput = overlay.querySelector<HTMLTextAreaElement>("textarea[name='inlineHtml']");
  const layoutInput = overlay.querySelector<HTMLSelectElement>("select[name='layout']");
  const visibilityInput = overlay.querySelector<HTMLSelectElement>("select[name='visibility']");
  const submitButton = overlay.querySelector<HTMLButtonElement>("button[type='submit']");
  const syncModuleKind = () => {
    const kind = kindInput?.value === "runtime" ? "runtime" : kindInput?.value === "link" ? "link" : "miniapp";
    const miniApp = kind === "miniapp";
    if (runtimeInput) {
      runtimeInput.hidden = kind !== "runtime";
      runtimeInput.required = kind === "runtime";
    }
    if (layoutInput) {
      layoutInput.hidden = !miniApp;
    }
    if (inlineHtmlInput) {
      inlineHtmlInput.hidden = !miniApp;
    }
    if (hrefInput) {
      hrefInput.hidden = kind === "runtime";
      hrefInput.required = kind === "link";
      hrefInput.placeholder = miniApp ? "https://... / http://127.0.0.1:... / или HTML ниже" : "https://... / /...";
    }
  };
  syncModuleKind();
  kindInput?.addEventListener("change", syncModuleKind);
  runtimeInput?.addEventListener("change", () => fillRuntimeModuleDraft(runtimeInput.value, titleInput, summaryInput));
  overlay.querySelectorAll<HTMLButtonElement>("[data-module-preset]").forEach((button) => {
    button.addEventListener("click", () => {
      const target = cleanRuntimeModuleTarget(button.dataset.modulePreset || "");
      if (!target) {
        return;
      }
      if (kindInput) {
        kindInput.value = "runtime";
      }
      if (runtimeInput) {
        runtimeInput.value = target;
      }
      fillRuntimeModuleDraft(target, titleInput, summaryInput);
      syncModuleKind();
      titleInput?.focus();
    });
  });
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
    const kind: PersonalSpaceCardModuleKind = kindInput?.value === "runtime"
      ? "runtime"
      : kindInput?.value === "link"
        ? "link"
        : "miniapp";
    if (!title) {
      if (error) {
        error.textContent = "Добавьте название.";
      }
      return;
    }
    const inlineHtml = kind === "miniapp" ? safeMiniAppInlineHtml(inlineHtmlInput?.value || "") : "";
    const href = kind === "miniapp"
      ? inlineHtml
        ? "about:srcdoc"
        : safeMiniAppUrl(hrefInput?.value || "", { baseUrl: window.location.href })
      : kind === "runtime"
        ? cleanRuntimeModuleTarget(runtimeInput?.value || "")
        : cleanUrlPath(hrefInput?.value || "");
    if (!href || (!inlineHtml && kind === "miniapp" && isServerHostedMiniAppUrl(href))) {
      if (error) {
        error.textContent = kind === "miniapp"
          ? "Добавьте внешний URL, локальный URL или HTML mini-app."
          : kind === "runtime"
            ? "Выберите возможность."
            : "Добавьте ссылку.";
      }
      return;
    }
    const draft = {
      kind,
      title,
      summary: cleanText(summaryInput?.value || "", 140),
      href,
      visibility: visibilityInput?.value === "trusted" ? "trusted" as const : "public" as const,
      ...(kind === "miniapp" ? { layout: normalizeMiniAppLayout(layoutInput?.value || "large") } : {}),
      ...(inlineHtml ? { inlineHtml } : {})
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

function renderRuntimeModulePresets(): string {
  const presets = runtimeModuleDefinitions.filter((module) => module.id !== "agent");
  if (presets.length === 0) {
    return "";
  }
  return `
    <div class="personal-module-presets" aria-label="готовые возможности">
      ${presets.map((module) => `
        <button type="button" data-module-preset="${escapeAttr(module.target)}">
          ${icon(module.icon)}
          <span>${escapeHtml(module.title)}</span>
        </button>
      `).join("")}
    </div>
  `;
}

function fillRuntimeModuleDraft(
  target: string,
  titleInput: HTMLInputElement | null,
  summaryInput: HTMLInputElement | null
): void {
  const module = runtimeModuleDefinitions.find((item) => item.target === cleanRuntimeModuleTarget(target));
  if (!module) {
    return;
  }
  if (titleInput) {
    titleInput.value = module.title;
  }
  if (summaryInput) {
    summaryInput.value = module.summary;
  }
}

function showAgentSheet(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions, preset: PersonalAgentPreset = {}): void {
  closePersonalOverlay(root);
  let savedModule = false;
  let rawReply = "";
  let controller: AgentWorkspaceController | null = null;
  const presetMode = preset.mode === "page" ? "page" : preset.mode === "style" ? "style" : "miniapp";
  const presetText = cleanText(preset.text || "", 900);
  controller = showAgentWorkspace(root, {
    title: "Agent",
    subtitle: profile.displayName || `@${profile.handle}`,
    userId: profile.handle,
    projectId: `${profile.handle}/${profile.slug}`,
    surface: "personal-card",
    providers: [{
      id: "soty-codex",
      kind: "soty-codex",
      label: "Soty Codex",
      summary: "Server Codex executor with Soty context.",
      capabilities: ["card-module", "miniapp", "page"]
    }],
    modes: [
      {
        id: "miniapp",
        label: "Mini-app",
        intentPrefix: "Create or improve a mini-app module for this personal Soty card.",
        placeholder: "Какой mini-app сделать для этой карточки?"
      },
      {
        id: "page",
        label: "Ссылка",
        intentPrefix: "Create or improve a useful link/page module for this personal Soty card.",
        placeholder: "Какую ссылку или страницу добавить?"
      },
      {
        id: "style",
        label: "Красота",
        intentPrefix: "Create a polished visual mini-app or card module that improves this Soty card without clutter.",
        placeholder: "Какой визуальный результат нужен для этой соты?"
      }
    ],
    defaultModeId: presetMode,
    initialText: presetText,
    contextItems: [
      { label: "handle", value: `@${profile.handle}` },
      { label: "space", value: profile.slug || "home" },
      { label: "owner", value: profile.displayName }
    ],
    async onRun(request) {
      savedModule = false;
      rawReply = "";
      request.emit({ kind: "status", text: "personal-card agent request" });
      const result = await options.askAgent(profile, {
        text: request.text,
        intent: request.mode.id === "page" ? "page" : "miniapp"
      });
      rawReply = result.reply;
      const visibleReply = replyWithoutCardModule(result.reply) || result.message;
      if (!result.ok) {
        return {
          ok: false,
          message: result.message,
          reply: visibleReply,
          proof: ["agent returned non-ok result"]
        };
      }
      const draft = agentModuleDraftFromReply(rawReply);
      if (!draft) {
        return {
          ok: true,
          message: result.message,
          reply: visibleReply,
          proof: ["agent reply received", "no card module marker"]
        };
      }
      const saved = await options.saveModule(options.route, draft);
      savedModule = saved.ok;
      return {
        ok: saved.ok,
        message: saved.message,
        reply: visibleReply,
        proof: saved.ok ? ["agent reply received", "card module saved"] : ["agent reply received", saved.message]
      };
    },
    async onResult(result) {
      if (!result.ok || !savedModule) {
        return;
      }
      controller?.close();
      await renderPersonalSpacePage(root, options);
      setActiveLayer(root, "place");
    }
  });
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
  const kind: PersonalSpaceCardModuleKind = parsed.kind === "link" ? "link" : parsed.kind === "runtime" ? "runtime" : "miniapp";
  const rawHref = typeof parsed.href === "string"
    ? parsed.href
    : typeof parsed.url === "string"
      ? parsed.url
      : typeof parsed.target === "string"
        ? parsed.target
        : "";
  const rawInlineHtml = typeof parsed.inlineHtml === "string"
    ? parsed.inlineHtml
    : typeof parsed.html === "string"
      ? parsed.html
      : "";
  const inlineHtml = kind === "miniapp" ? safeMiniAppInlineHtml(rawInlineHtml) : "";
  const href = kind === "miniapp"
    ? inlineHtml
      ? "about:srcdoc"
      : safeMiniAppUrl(rawHref, { baseUrl: window.location.href })
    : kind === "runtime"
      ? cleanRuntimeModuleTarget(rawHref)
      : cleanUrlPath(rawHref);
  const runtime = kind === "runtime" ? runtimeModuleDefinitions.find((module) => module.target === href) : null;
  const title = cleanText(parsed.title || parsed.name, 80) || runtime?.title || "";
  const summary = cleanText(parsed.summary || parsed.description, 140) || runtime?.summary || "";
  if (!title || !href || (!inlineHtml && kind === "miniapp" && isServerHostedMiniAppUrl(href))) {
    return null;
  }
  return {
    kind,
    title,
    summary,
    href,
    visibility: parsed.visibility === "trusted" ? "trusted" : "public",
    ...(kind === "miniapp" ? { layout: normalizeMiniAppLayout(cleanText(parsed.layout, 24) || "large") } : {}),
    ...(inlineHtml ? { inlineHtml } : {})
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

async function runPersonalAgentCommand(
  root: HTMLElement,
  profile: PersonalSpaceProfile,
  options: PersonalSpacePageOptions,
  request: PersonalSpaceAgentRequest,
  replyBox: HTMLElement | null,
  error: HTMLElement | null,
  overlay: HTMLElement
): Promise<void> {
  if (error) {
    error.textContent = "";
  }
  if (replyBox) {
    replyBox.hidden = false;
    replyBox.textContent = "Думаю...";
  }
  try {
    const result = await options.askAgent(profile, request);
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
  }
}

function showPersonalActionsSheet(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): void {
  closePersonalOverlay(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet personal-actions-sheet" role="dialog" aria-modal="true" aria-label="Команды">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <div class="personal-module-mark">${icon("check")}</div>
      <h2>Команды</h2>
      <p>${escapeHtml(profile.displayName)}</p>
      <div class="personal-command-presets" aria-label="быстрые команды">
        <button type="button" data-command-intent="miniapp" data-command-text="Создай простой mini-app для этой карточки.">${icon("apps")} <span>Mini-app</span></button>
        <button type="button" data-command-intent="page" data-command-text="Добавь полезную ссылку для этой карточки.">${icon("qr")} <span>Ссылка</span></button>
        <button type="button" data-command-intent="miniapp" data-command-text="Создай небольшой инструмент для этой карточки.">${icon("check")} <span>Инструмент</span></button>
      </div>
      <form data-command-form>
        <select name="intent" aria-label="тип команды">
          <option value="miniapp">Mini-app</option>
          <option value="page">Ссылка</option>
        </select>
        <textarea name="text" maxlength="720" required placeholder="Что сделать?"></textarea>
        <button type="submit">${icon("check")} Выполнить</button>
      </form>
      <div class="personal-agent-reply" data-command-reply hidden></div>
      <small data-error></small>
    </section>
  `;
  root.append(overlay);
  const textarea = overlay.querySelector<HTMLTextAreaElement>("textarea[name='text']");
  const intentInput = overlay.querySelector<HTMLSelectElement>("select[name='intent']");
  const submitButton = overlay.querySelector<HTMLButtonElement>("button[type='submit']");
  const replyBox = overlay.querySelector<HTMLElement>("[data-command-reply]");
  const error = overlay.querySelector<HTMLElement>("[data-error]");
  const close = () => overlay.remove();
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", close);
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      close();
      return;
    }
    const preset = event.target instanceof Element ? event.target.closest<HTMLButtonElement>("[data-command-text]") : null;
    if (preset && overlay.contains(preset)) {
      if (intentInput) {
        intentInput.value = preset.dataset.commandIntent === "page" ? "page" : "miniapp";
      }
      if (textarea) {
        textarea.value = preset.dataset.commandText || "";
        textarea.focus();
      }
    }
  });
  overlay.querySelector<HTMLFormElement>("[data-command-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    void runCommandFromSheet();
  });
  textarea?.focus();

  async function runCommandFromSheet(): Promise<void> {
    const text = cleanText(textarea?.value || "", 720);
    if (!text) {
      if (error) {
        error.textContent = "Напишите короткую команду.";
      }
      return;
    }
    if (submitButton) {
      submitButton.disabled = true;
    }
    try {
      await runPersonalAgentCommand(
        root,
        profile,
        options,
        {
          text,
          intent: intentInput?.value === "page" ? "page" : "miniapp"
        },
        replyBox,
        error,
        overlay
      );
    } finally {
      if (submitButton && overlay.isConnected) {
        submitButton.disabled = false;
      }
    }
  }
}

function showRuntimeModuleSheet(root: HTMLElement, profile: PersonalSpaceProfile, target: string, options: PersonalSpacePageOptions): void {
  if (target === "access") {
    showTrustedAccessSheet(root, profile, options);
    return;
  }
  if (target === "apps") {
    showCardAppsSheet(root, profile, options);
    return;
  }
  if (target === "actions") {
    showPersonalActionsSheet(root, profile, options);
    return;
  }
  if (target === "chess") {
    showPersonalChessSheet(root, profile);
    return;
  }
  if (target === "files") {
    showPersonalFilesSheet(root, profile);
    return;
  }
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

function showPersonalFilesSheet(root: HTMLElement, profile: PersonalSpaceProfile): void {
  closePersonalOverlay(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  const files = loadPersonalFiles(profile);
  overlay.innerHTML = `
    <section class="personal-sheet personal-files-sheet" role="dialog" aria-modal="true" aria-label="Файлы">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <div class="personal-module-mark">${icon("clip")}</div>
      <h2>Файлы</h2>
      <p>Локально для этой карточки.</p>
      <input type="file" multiple hidden data-personal-files-input />
      <div class="personal-file-actions">
        <button type="button" data-personal-files-add>${icon("clip")} <span>Добавить</span></button>
        <button type="button" data-personal-files-clear${files.length ? "" : " hidden"}>${icon("close")} <span>Очистить</span></button>
      </div>
      <div class="personal-file-list" data-personal-files-list>
        ${renderPersonalFiles(files)}
      </div>
      <small data-personal-files-note></small>
    </section>
  `;
  root.append(overlay);
  const close = () => overlay.remove();
  const render = () => {
    const list = overlay.querySelector<HTMLElement>("[data-personal-files-list]");
    if (list) {
      list.innerHTML = renderPersonalFiles(loadPersonalFiles(profile));
    }
    const clear = overlay.querySelector<HTMLButtonElement>("[data-personal-files-clear]");
    clear?.toggleAttribute("hidden", loadPersonalFiles(profile).length === 0);
  };
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", close);
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      close();
      return;
    }
    const target = event.target instanceof Element ? event.target : null;
    const remove = target?.closest<HTMLButtonElement>("[data-personal-file-remove]");
    if (remove && overlay.contains(remove)) {
      removePersonalFile(profile, remove.dataset.personalFileRemove || "");
      render();
    }
  });
  const input = overlay.querySelector<HTMLInputElement>("[data-personal-files-input]");
  const note = overlay.querySelector<HTMLElement>("[data-personal-files-note]");
  overlay.querySelector<HTMLButtonElement>("[data-personal-files-add]")?.addEventListener("click", () => input?.click());
  overlay.querySelector<HTMLButtonElement>("[data-personal-files-clear]")?.addEventListener("click", () => {
    savePersonalFiles(profile, []);
    if (note) {
      note.textContent = "Очищено.";
    }
    render();
  });
  input?.addEventListener("change", () => {
    const selected = Array.from(input.files || []);
    input.value = "";
    if (selected.length === 0) {
      return;
    }
    void addPersonalFiles(profile, selected).then((result) => {
      if (note) {
        note.textContent = result.skipped > 0
          ? `Добавлено: ${result.added}. Не подошло: ${result.skipped}.`
          : result.added > 0
            ? `Добавлено: ${result.added}.`
            : "Файл слишком большой.";
      }
      render();
    });
  });
}

function renderPersonalFiles(files: readonly PersonalFileItem[]): string {
  if (files.length === 0) {
    return `<section class="personal-empty">${icon("clip")} <span>Файлов пока нет.</span></section>`;
  }
  return files.map((file) => `
    <article class="personal-file-item">
      <a href="${escapeAttr(file.dataUrl)}" download="${escapeAttr(file.name)}">
        ${icon("clip")}
        <span>${escapeHtml(file.name)}</span>
        <p>${escapeHtml(personalFileMeta(file))}</p>
      </a>
      <button type="button" data-personal-file-remove="${escapeAttr(file.id)}" aria-label="Удалить">${icon("close")}</button>
    </article>
  `).join("");
}

async function addPersonalFiles(profile: PersonalSpaceProfile, selected: readonly File[]): Promise<{ readonly added: number; readonly skipped: number }> {
  const current = loadPersonalFiles(profile);
  const next: PersonalFileItem[] = [];
  let skipped = 0;
  for (let index = 0; index < selected.length; index += 1) {
    const file = selected[index];
    if (!file) {
      skipped += 1;
      continue;
    }
    if (file.size <= 0 || file.size > personalFileMaxBytes) {
      skipped += 1;
      continue;
    }
    try {
      next.push({
        id: localItemId("file"),
        name: cleanText(file.name, 120) || "file",
        type: cleanText(file.type || "file", 80) || "file",
        size: file.size,
        createdAt: new Date().toISOString(),
        dataUrl: await fileToDataUrl(file)
      });
    } catch {
      skipped += 1;
    }
    if (next.length + current.length >= personalFileLimit) {
      skipped += Math.max(0, selected.length - index - 1);
      break;
    }
  }
  savePersonalFiles(profile, [...next, ...current].slice(0, personalFileLimit));
  return { added: next.length, skipped };
}

function loadPersonalFiles(profile: PersonalSpaceProfile): readonly PersonalFileItem[] {
  try {
    const parsed = JSON.parse(window.localStorage.getItem(personalFilesKey(profile)) || "[]") as unknown;
    return list(parsed).map(normalizePersonalFile).filter(isPersonalFile).slice(0, personalFileLimit);
  } catch {
    return [];
  }
}

function savePersonalFiles(profile: PersonalSpaceProfile, files: readonly PersonalFileItem[]): void {
  try {
    window.localStorage.setItem(personalFilesKey(profile), JSON.stringify(files.slice(0, personalFileLimit)));
  } catch {
    // Local file shelf is best-effort; storage pressure should not block the card UI.
  }
}

function removePersonalFile(profile: PersonalSpaceProfile, id: string): void {
  const clean = cleanText(id, 80);
  if (!clean) {
    return;
  }
  savePersonalFiles(profile, loadPersonalFiles(profile).filter((file) => file.id !== clean));
}

function normalizePersonalFile(value: unknown): PersonalFileItem | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = cleanText(value.id, 80);
  const name = cleanText(value.name, 120);
  const type = cleanText(value.type, 80) || "file";
  const size = Number(value.size);
  const createdAt = cleanText(value.createdAt, 40);
  const dataUrl = cleanPersonalFileDataUrl(value.dataUrl);
  if (!id || !name || !Number.isFinite(size) || size < 0 || size > personalFileMaxBytes || !createdAt || !dataUrl) {
    return null;
  }
  return { id, name, type, size: Math.round(size), createdAt, dataUrl };
}

function isPersonalFile(value: PersonalFileItem | null): value is PersonalFileItem {
  return value !== null;
}

function cleanPersonalFileDataUrl(value: unknown): string {
  const text = typeof value === "string" ? value.trim() : "";
  if (text.length > personalFileDataUrlMaxChars) {
    return "";
  }
  return /^data:[a-z0-9.+/-]{1,100};base64,[a-z0-9+/=]+$/iu.test(text) ? text : "";
}

function personalFilesKey(profile: PersonalSpaceProfile): string {
  return `${personalFilesPrefix}${routeUrl(profile)}`;
}

function personalFileMeta(file: PersonalFileItem): string {
  return `${personalFileSize(file.size)} · ${threadTime(file.createdAt)}`;
}

function personalFileSize(size: number): string {
  if (size >= 1_000_000) {
    return `${(size / 1_000_000).toFixed(1)} MB`;
  }
  if (size >= 1000) {
    return `${Math.round(size / 1000)} KB`;
  }
  return `${Math.max(0, Math.round(size))} B`;
}

function fileToDataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.addEventListener("load", () => {
      const result = typeof reader.result === "string" ? reader.result : "";
      const dataUrl = cleanPersonalFileDataUrl(result);
      if (!dataUrl) {
        reject(new Error("invalid file data"));
        return;
      }
      resolve(dataUrl);
    });
    reader.addEventListener("error", () => reject(reader.error || new Error("file read failed")));
    reader.readAsDataURL(file);
  });
}

function showCardAppsSheet(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): void {
  closePersonalOverlay(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet personal-card-apps-sheet" role="dialog" aria-modal="true" aria-label="Mini-app">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <div class="personal-module-mark">${icon("apps")}</div>
      <h2>Mini-app</h2>
      <p>Маленький инструмент внутри этой карточки.</p>
      <div class="personal-app-actions">
        <button type="button" data-app-agent>${icon("agent")} <span>ИИ</span></button>
        <button type="button" data-app-module>${icon("apps")} <span>Добавить</span></button>
      </div>
      <small>URL может быть внешним или локальным. Наш сервер mini-app пока не хостит.</small>
    </section>
  `;
  root.append(overlay);
  const close = () => overlay.remove();
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", close);
  overlay.querySelector<HTMLButtonElement>("[data-app-agent]")?.addEventListener("click", () => {
    overlay.remove();
    showAgentSheet(root, profile, options);
  });
  overlay.querySelector<HTMLButtonElement>("[data-app-module]")?.addEventListener("click", () => {
    overlay.remove();
    showModuleSheet(root, options);
  });
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      close();
    }
  });
}

function showTrustedAccessSheet(root: HTMLElement, profile: PersonalSpaceProfile, options: PersonalSpacePageOptions): void {
  closePersonalOverlay(root);
  const ownSpace = isRenderedOwnSpace(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  const localHandle = loadLocalHandle();
  overlay.innerHTML = `
    <section class="personal-sheet personal-access-sheet" role="dialog" aria-modal="true" aria-label="Доступ">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <div class="personal-module-mark">${icon("shield")}</div>
      <h2>Доступ</h2>
      ${ownSpace ? `
        <p>Кто видит модули «По списку».</p>
        <form data-access-form>
          <textarea name="handles" maxlength="520" aria-label="ники" placeholder="anna, team, device-1">${escapeHtml(profile.trustedHandles.join(", "))}</textarea>
          <button type="submit">${icon("check")} Сохранить</button>
        </form>
        <small data-error></small>
      ` : `
        <p>${escapeHtml(profile.trustedViewer ? "Вы в списке этой карточки." : localHandle ? "Доступ по списку закрыт." : "Для доступа нужен ваш ник.")}</p>
      `}
    </section>
  `;
  root.append(overlay);
  const close = () => overlay.remove();
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", close);
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      close();
    }
  });
  if (!ownSpace) {
    return;
  }
  const input = overlay.querySelector<HTMLTextAreaElement>("textarea[name='handles']");
  input?.focus();
  overlay.querySelector<HTMLFormElement>("[data-access-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    const button = overlay.querySelector<HTMLButtonElement>("button[type='submit']");
    const error = overlay.querySelector<HTMLElement>("[data-error]");
    const trustedHandles = normalizeTrustedHandles(input?.value || "");
    const update: PersonalSpaceProfileUpdate = {
      displayName: profile.displayName,
      about: profile.about,
      contact: contactValueForProfile(profile),
      spaces: spaceUpdatesWith(profile.spaces),
      trustedHandles
    };
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
        setActiveLayer(root, "place");
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

function showPersonalChessSheet(root: HTMLElement, profile: PersonalSpaceProfile): void {
  closePersonalOverlay(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  let snapshot = loadPersonalChessSnapshot(profile);
  let selected: Square | "" = "";
  let orientation: Color = "w";
  let promotion: { readonly from: Square; readonly to: Square; readonly choices: readonly PieceSymbol[] } | null = null;
  let pendingAgent = false;
  overlay.innerHTML = `
    <section class="personal-sheet personal-chess-sheet" role="dialog" aria-modal="true" aria-label="Шахматы">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <div data-personal-chess></div>
    </section>
  `;
  root.append(overlay);
  const shell = overlay.querySelector<HTMLElement>("[data-personal-chess]");
  const save = () => savePersonalChessSnapshot(profile, snapshot);
  const render = () => {
    if (shell) {
      shell.innerHTML = renderPersonalChess(snapshot, selected, orientation, promotion, pendingAgent);
    }
  };
  const queueAgentMove = () => {
    if (!isAgentTurn(snapshot) || pendingAgent || snapshot.result) {
      return;
    }
    pendingAgent = true;
    render();
    window.setTimeout(() => {
      if (!overlay.isConnected) {
        return;
      }
      const agentMove = chooseAgentMove(snapshot);
      pendingAgent = false;
      if (agentMove) {
        const applied = applyChessMove(snapshot, agentMove.from, agentMove.to, agentMove.promotion ?? "q");
        if (applied) {
          snapshot = applied.snapshot;
          save();
        }
      }
      render();
    }, 180);
  };
  render();
  queueAgentMove();
  overlay.querySelector<HTMLElement>("[data-close]")?.addEventListener("click", () => overlay.remove());
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      overlay.remove();
      return;
    }
    const target = event.target instanceof Element ? event.target : null;
    if (!target) {
      return;
    }
    if (target.closest("[data-chess-new]")) {
      snapshot = createPersonalChessSnapshot(profile);
      selected = "";
      promotion = null;
      pendingAgent = false;
      save();
      render();
      return;
    }
    if (target.closest("[data-chess-flip]")) {
      orientation = orientation === "w" ? "b" : "w";
      render();
      return;
    }
    const promotionNode = target.closest<HTMLButtonElement>("[data-promotion]");
    if (promotionNode && promotion) {
      const piece = personalChessPromotionPiece(promotionNode.dataset.promotion || "");
      if (!piece) {
        return;
      }
      const applied = applyChessMove(snapshot, promotion.from, promotion.to, piece);
      if (applied) {
        snapshot = applied.snapshot;
        selected = "";
        promotion = null;
        save();
        render();
        queueAgentMove();
      }
      return;
    }
    const squareNode = target.closest<HTMLButtonElement>("[data-square]");
    if (!squareNode || pendingAgent || snapshot.result) {
      return;
    }
    const squareValue = squareNode.dataset.square || "";
    if (!isSquare(squareValue)) {
      return;
    }
    const game = chessFromSnapshot(snapshot);
    if (selected) {
      if (selected === squareValue) {
        selected = "";
        promotion = null;
        render();
        return;
      }
      const choices = promotionChoices(snapshot, selected, squareValue);
      if (choices.length > 0) {
        promotion = { from: selected, to: squareValue, choices };
        render();
        return;
      }
      const applied = applyChessMove(snapshot, selected, squareValue);
      if (applied) {
        snapshot = applied.snapshot;
        selected = "";
        promotion = null;
        save();
        render();
        queueAgentMove();
        return;
      }
    }
    const piece = game.get(squareValue);
    if (!piece || piece.color !== game.turn() || isAgentTurn(snapshot)) {
      return;
    }
    selected = squareValue;
    promotion = null;
    render();
  });
}

function renderPersonalChess(
  snapshot: ChessSnapshot,
  selected: Square | "",
  orientation: Color,
  promotion: { readonly from: Square; readonly to: Square; readonly choices: readonly PieceSymbol[] } | null,
  pendingAgent: boolean
): string {
  const game = chessFromSnapshot(snapshot);
  const legalMoves = selected && !snapshot.result ? legalMovesForSquare(snapshot, selected) : [];
  const legalTargets = new Set(legalMoves.map((move) => move.to));
  const captureTargets = new Set(legalMoves.filter((move) => Boolean(move.captured)).map((move) => move.to));
  const status = personalChessStatus(snapshot, pendingAgent);
  const moves = snapshot.history.slice(-10);
  return `
    <div class="personal-chess-head">
      <div class="personal-module-mark">${icon("chess")}</div>
      <div>
        <h2>Шахматы</h2>
        <p>${escapeHtml(status)}</p>
      </div>
      <div class="personal-chess-actions" aria-label="шахматы">
        <button type="button" data-chess-flip aria-label="Развернуть доску" title="Развернуть доску">${icon("refresh")}</button>
        <button type="button" data-chess-new aria-label="Новая партия" title="Новая партия">${icon("check")}</button>
      </div>
    </div>
    <div class="personal-chess-layout">
      <div class="personal-chess-board" aria-label="шахматная доска">
        ${boardSquares(orientation).map((square) => {
          const piece = game.get(square);
          const classes = [
            "personal-chess-square",
            personalChessSquareTone(square),
            selected === square ? "is-selected" : "",
            snapshot.lastMove?.from === square || snapshot.lastMove?.to === square ? "is-last" : "",
            legalTargets.has(square) ? "is-legal" : "",
            captureTargets.has(square) ? "is-capture" : ""
          ].filter(Boolean).join(" ");
          return `<button type="button" class="${classes}" data-square="${square}" aria-label="${escapeAttr(personalChessSquareLabel(square, piece?.color, piece?.type))}"${pendingAgent || Boolean(snapshot.result) ? " disabled" : ""}>${pieceGlyph(piece)}</button>`;
        }).join("")}
      </div>
      <aside class="personal-chess-desk">
        <b>${escapeHtml(personalChessTurn(snapshot, pendingAgent))}</b>
        <div class="personal-chess-moves" aria-label="последние ходы">
          ${moves.length ? moves.map((move) => `<span>${escapeHtml(move)}</span>`).join("") : `<small>Новая партия.</small>`}
        </div>
        ${promotion ? `
          <div class="personal-chess-promotion" aria-label="превращение пешки">
            ${promotion.choices.map((piece) => `<button type="button" data-promotion="${piece}">${escapeHtml(personalChessPieceName(piece))}</button>`).join("")}
          </div>
        ` : ""}
      </aside>
    </div>
  `;
}

function createPersonalChessSnapshot(profile: PersonalSpaceProfile): ChessSnapshot {
  return createChessSnapshot({
    mode: "agent",
    localNick: loadLocalHandle() || "Я",
    opponentNick: profile.shortName || profile.displayName || "Гений"
  });
}

function loadPersonalChessSnapshot(profile: PersonalSpaceProfile): ChessSnapshot {
  const fallback = {
    mode: "agent" as const,
    localNick: loadLocalHandle() || "Я",
    opponentNick: profile.shortName || profile.displayName || "Гений"
  };
  try {
    const raw = window.localStorage.getItem(personalChessKey(profile));
    return raw ? normalizeChessSnapshot(JSON.parse(raw) as unknown, fallback) : createChessSnapshot(fallback);
  } catch {
    return createChessSnapshot(fallback);
  }
}

function savePersonalChessSnapshot(profile: PersonalSpaceProfile, snapshot: ChessSnapshot): void {
  try {
    window.localStorage.setItem(personalChessKey(profile), JSON.stringify(snapshot));
  } catch {
    // Local chess state must not block the module UI.
  }
}

function personalChessKey(profile: PersonalSpaceProfile): string {
  return `${personalChessPrefix}${routeUrl(profile)}`;
}

function personalChessStatus(snapshot: ChessSnapshot, pendingAgent: boolean): string {
  if (snapshot.result === "white") {
    return "Белые выиграли.";
  }
  if (snapshot.result === "black") {
    return "Чёрные выиграли.";
  }
  if (snapshot.result === "draw") {
    return snapshot.resultReason || "Ничья.";
  }
  if (pendingAgent || isAgentTurn(snapshot)) {
    return "Гений думает.";
  }
  const game = chessFromSnapshot(snapshot);
  return game.isCheck() ? "Шах. Ваш ход." : "Ваш ход.";
}

function personalChessTurn(snapshot: ChessSnapshot, pendingAgent: boolean): string {
  if (snapshot.result) {
    return personalChessStatus(snapshot, pendingAgent);
  }
  return pendingAgent || isAgentTurn(snapshot) ? "Ход соперника" : "Ваш ход";
}

function personalChessSquareTone(square: Square): "light" | "dark" {
  const file = square.charCodeAt(0) - 97;
  const rank = Number(square[1]);
  return (file + rank) % 2 === 0 ? "dark" : "light";
}

function personalChessSquareLabel(square: Square, color?: Color, piece?: PieceSymbol): string {
  if (!color || !piece) {
    return square;
  }
  return `${square} ${color === "w" ? "белые" : "чёрные"} ${personalChessPieceName(piece)}`;
}

function personalChessPieceName(piece: PieceSymbol): string {
  const names: Record<PieceSymbol, string> = {
    p: "пешка",
    n: "конь",
    b: "слон",
    r: "ладья",
    q: "ферзь",
    k: "король"
  };
  return names[piece];
}

function personalChessPromotionPiece(value: string): PieceSymbol | "" {
  return value === "q" || value === "r" || value === "b" || value === "n" ? value : "";
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

function showInstallGuideSheet(root: HTMLElement, guide: PersonalSpaceInstallGuide): void {
  closePersonalOverlay(root);
  const overlay = document.createElement("div");
  overlay.className = "personal-overlay";
  overlay.innerHTML = `
    <section class="personal-sheet personal-install-sheet" role="dialog" aria-modal="true" aria-label="${escapeAttr(guide.title)}">
      <button class="personal-sheet-close" type="button" data-close>${icon("close")}</button>
      <div class="personal-module-mark">${icon("install")}</div>
      <h2>${escapeHtml(guide.title)}</h2>
      <ol class="personal-install-steps">
        ${guide.steps.map((step) => `<li>${escapeHtml(step)}</li>`).join("")}
      </ol>
      ${guide.hint ? `<p>${escapeHtml(guide.hint)}</p>` : ""}
      <div class="personal-install-actions">
        <button type="button" data-copy-install-link>${icon("copy")} <span>Ссылка</span></button>
        <button type="button" data-close>Готово</button>
      </div>
      <small data-error></small>
    </section>
  `;
  root.append(overlay);
  const close = () => overlay.remove();
  overlay.querySelectorAll<HTMLElement>("[data-close]").forEach((button) => button.addEventListener("click", close));
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      close();
    }
  });
  overlay.querySelector<HTMLButtonElement>("[data-copy-install-link]")?.addEventListener("click", () => {
    const note = overlay.querySelector<HTMLElement>("[data-error]");
    void copyText(window.location.href)
      .then(() => {
        if (note) {
          note.textContent = "Скопировано.";
        }
      })
      .catch(() => undefined);
  });
}

function closePersonalOverlay(root: HTMLElement): void {
  root.querySelector(".personal-overlay")?.remove();
}

function openPersonalRoute(handle: string): void {
  window.location.assign(`/@${encodeURIComponent(handle)}`);
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
    const active = button.dataset.layer === layer;
    button.setAttribute("aria-selected", active ? "true" : "false");
    button.tabIndex = active ? 0 : -1;
  });
  root.querySelectorAll<HTMLElement>("[data-panel]").forEach((panel) => {
    const active = panel.dataset.panel === layer;
    panel.classList.toggle("is-active", active);
    panel.hidden = !active;
    panel.setAttribute("aria-hidden", active ? "false" : "true");
  });
  rememberPersonalLayerInUrl(layer);
}

function rememberPersonalLayerInUrl(layer: PersonalSpaceLayer): void {
  try {
    const url = new URL(window.location.href);
    const parts = url.pathname.split("/").filter(Boolean);
    const routeFirst = parts[0] === "pwa" ? parts[1] : parts[0];
    if (!routeFirst?.startsWith("@")) {
      return;
    }
    if (layer === "card") {
      url.searchParams.delete("layer");
    } else {
      url.searchParams.set("layer", layer);
    }
    window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  } catch {
    // Layer navigation still works when History API is unavailable.
  }
}

function bindPersonalLayerKeys(): void {
  if (personalLayerKeysBound) {
    return;
  }
  personalLayerKeysBound = true;
  window.addEventListener("keydown", (event) => {
    const root = document.querySelector<HTMLElement>("#app");
    const shell = root?.querySelector<HTMLElement>(".personal-space-shell");
    if (!root || !shell) {
      return;
    }
    switchPersonalLayerByKey(root, shell, event);
  });
}

function switchPersonalLayerByKey(root: HTMLElement, shell: HTMLElement, event: KeyboardEvent): void {
  const target = event.target instanceof HTMLElement ? event.target : null;
  if (event.defaultPrevented || event.altKey || event.ctrlKey || event.metaKey || root.querySelector(".personal-overlay") || isPersonalShortcutTargetLocked(target)) {
    return;
  }
  const layerButton = target?.closest<HTMLButtonElement>("[data-layer]");
  const scopedToLayerbar = Boolean(layerButton && shell.contains(layerButton));
  const currentLayer = scopedToLayerbar
    ? layerButton?.dataset.layer || ""
    : shell.dataset.activeLayer || "";
  const nextLayer = nextPersonalLayer(currentLayer, event.key, scopedToLayerbar);
  if (!nextLayer) {
    return;
  }
  event.preventDefault();
  setActiveLayer(root, nextLayer);
  if (scopedToLayerbar) {
    shell.querySelector<HTMLButtonElement>(`[data-layer="${nextLayer}"]`)?.focus();
  }
}

function nextPersonalLayer(currentLayer: string, key: string, scopedToLayerbar: boolean): PersonalSpaceLayer | null {
  const directIndex = /^[1-5]$/u.test(key) ? Number(key) - 1 : -1;
  if (directIndex >= 0) {
    return layers[directIndex]?.id || null;
  }
  const currentIndex = layers.findIndex((layer) => layer.id === currentLayer);
  if (currentIndex < 0) {
    return null;
  }
  if (key === "ArrowRight" || key === "PageDown" || (scopedToLayerbar && key === "ArrowDown")) {
    return layers[(currentIndex + 1) % layers.length]?.id || null;
  }
  if (key === "ArrowLeft" || key === "PageUp" || (scopedToLayerbar && key === "ArrowUp")) {
    return layers[(currentIndex - 1 + layers.length) % layers.length]?.id || null;
  }
  if (scopedToLayerbar && key === "Home") {
    return layers[0]?.id || null;
  }
  if (scopedToLayerbar && key === "End") {
    return layers[layers.length - 1]?.id || null;
  }
  return null;
}

function isPersonalShortcutTargetLocked(target: HTMLElement | null): boolean {
  if (!target) {
    return false;
  }
  return target.closest("input, textarea, select, [contenteditable='true']") !== null;
}

function isPersonalLayer(value: string): value is PersonalSpaceLayer {
  return layers.some((layer) => layer.id === value);
}

async function installPersonalSpace(root: HTMLElement, button: HTMLButtonElement, options: PersonalSpacePageOptions): Promise<void> {
  const note = root.querySelector<HTMLElement>("[data-install-note]");
  button.disabled = true;
  try {
    const result = await options.install();
    if (result.guide) {
      showInstallGuideSheet(root, result.guide);
      return;
    }
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
    trustedViewer: record.trustedViewer === true,
    trustedHandles: normalizeTrustedHandles(record.trustedHandles),
    spaces: list(record.spaces).map(normalizeSpaceLink).filter(isSpaceLink).slice(0, 12),
    modules: list(record.modules).map(normalizeCardModule).filter(isCardModule).slice(0, 16),
    actions: {
      messageUrl: cleanUrlPath(actions.messageUrl) || personalMessageUrl(route),
      runtimeUrl: cleanRuntimeUrl(actions.runtimeUrl, route)
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
  const kind: PersonalSpaceCardModuleKind = value.kind === "runtime" ? "runtime" : value.kind === "miniapp" ? "miniapp" : "link";
  const rawHref = typeof value.href === "string" ? value.href : typeof value.target === "string" ? value.target : "";
  const rawInlineHtml = typeof value.inlineHtml === "string"
    ? value.inlineHtml
    : typeof value.html === "string"
      ? value.html
      : "";
  const inlineHtml = kind === "miniapp" ? safeMiniAppInlineHtml(rawInlineHtml) : "";
  const href = kind === "miniapp"
    ? inlineHtml
      ? "about:srcdoc"
      : safeMiniAppUrl(rawHref, { baseUrl: window.location.href })
    : kind === "runtime"
      ? cleanRuntimeModuleTarget(rawHref)
      : cleanUrlPath(rawHref);
  const runtime = kind === "runtime" ? runtimeModuleDefinitions.find((module) => module.target === href) : null;
  const title = cleanText(value.title, 80) || runtime?.title || "";
  if (!href || (!inlineHtml && kind === "miniapp" && isServerHostedMiniAppUrl(href))) {
    return null;
  }
  if (!title) {
    return null;
  }
  return {
    id: cleanText(value.id, 80) || localItemId("module"),
    kind,
    title,
    summary: cleanText(value.summary, 140) || runtime?.summary || "",
    href,
    visibility: value.visibility === "trusted" ? "trusted" : "public",
    ...(kind === "miniapp" ? { layout: normalizeMiniAppLayout(cleanText(value.layout, 24) || "large") } : {}),
    ...(inlineHtml ? { inlineHtml } : {})
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
      messageUrl: personalMessageUrl(route),
      runtimeUrl: personalRuntimeUrl(route)
    }
  };
}

function personalMessageUrl(route: PersonalSpaceRoute): string {
  return `${routeUrl(route)}?layer=messages`;
}

function personalProfileIconUrl(profile: Pick<PersonalSpaceProfile, "handle" | "slug"> & { readonly photoUrl?: string }): string {
  const handle = encodeURIComponent(cleanRoutePart(profile.handle) || "guest");
  const slug = encodeURIComponent(cleanRoutePart(profile.slug || ""));
  const baseUrl = slug
    ? `/icon/space/${handle}/${slug}.svg`
    : `/icon/space/${handle}.svg`;
  const version = personalPhotoVersion(profile.photoUrl || "");
  return version ? `${baseUrl}?v=${encodeURIComponent(version)}` : baseUrl;
}

function personalActorIconUrl(handle: string): string {
  const cleanHandle = cleanRoutePart(handle);
  return cleanHandle && cleanHandle !== "guest"
    ? `/icon/space/${encodeURIComponent(cleanHandle)}.svg`
    : "";
}

function personalPhotoVersion(photoUrl: string): string {
  try {
    const url = new URL(photoUrl, window.location.origin);
    return cleanText(url.searchParams.get("v") || "", 32).replace(/[^a-z0-9_-]/giu, "");
  } catch {
    return "";
  }
}

function personalNoticeTag(url: string, title: string): string {
  const route = cleanText(url, 120)
    .replace(/[^\p{L}\p{N}:._/?=@-]+/gu, "-")
    .replace(/-+/gu, "-");
  const peer = cleanRoutePart(title) || "thread";
  return `soty:messages:${route}:${peer}`.slice(0, 140);
}

function personalRuntimeUrl(route: PersonalSpaceRoute): string {
  return `${routeUrl(route)}?layer=place`;
}

function cleanRuntimeUrl(value: unknown, route: PersonalSpaceRoute): string {
  const clean = cleanUrlPath(value);
  if (!clean || clean.startsWith("/?pwa=1")) {
    return personalRuntimeUrl(route);
  }
  return clean;
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

function personalNavigationTarget(href: string): string {
  const currentParts = window.location.pathname.split("/").filter(Boolean);
  if (currentParts[0] !== "pwa") {
    return href;
  }
  try {
    const url = new URL(href, window.location.origin);
    if (url.origin !== window.location.origin) {
      return href;
    }
    const route = personalSpaceRouteFromPath(url.pathname);
    if (!route) {
      return href;
    }
    url.pathname = personalSpacePwaStartUrl(route);
    return `${url.pathname}${url.search}${url.hash}`;
  } catch {
    return href;
  }
}

function personalSpaceRouteFromPath(pathname: string): PersonalSpaceRoute | null {
  const rawParts = pathname.split("/").filter(Boolean);
  const parts = rawParts[0] === "pwa" ? rawParts.slice(1) : rawParts;
  const first = parts[0] || "";
  if (!first.startsWith("@") || parts.length > 2) {
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

function cleanRuntimeModuleTarget(value: string): string {
  return runtimeModuleTargetFromString(cleanText(value, 40));
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
