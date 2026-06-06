export type AttentionNotice = {
  readonly title: string;
  readonly body?: string;
  readonly icon?: string;
  readonly badge?: string;
  readonly tag?: string;
  readonly renotify?: boolean;
  readonly silent?: boolean;
  readonly vibrate?: readonly number[];
  readonly timestamp?: number;
};

type AttentionOptions = {
  readonly reason: string;
  readonly tunnelId: string;
  readonly hadNotice: boolean;
  readonly hidden: boolean;
  readonly url: string;
  readonly notice?: AttentionNotice | undefined;
};

const activeNoticeKeys = new Set<string>();
const lastTypingNoticeAt = new Map<string, number>();

export async function requestNotificationPermission(): Promise<NotificationPermission | "unsupported"> {
  if (!("Notification" in window)) {
    return "unsupported";
  }
  if (Notification.permission !== "default") {
    return Notification.permission;
  }
  try {
    return await Notification.requestPermission();
  } catch {
    return Notification.permission;
  }
}

export function notificationPermissionNote(permission: NotificationPermission | "unsupported"): string {
  if (permission === "granted") {
    return " Оповещения включены.";
  }
  if (permission === "denied") {
    return " Оповещения можно включить в настройках браузера.";
  }
  return "";
}

export function shouldOfferNotifications(): boolean {
  return "Notification" in window && Notification.permission !== "denied";
}

export function hasNotificationPermission(): boolean {
  return "Notification" in window && Notification.permission === "granted";
}

export function supportsPushNotifications(): boolean {
  return "serviceWorker" in navigator && "PushManager" in window;
}

export async function subscribeToPushNotifications(publicKey: string): Promise<PushSubscription | null> {
  if (!supportsPushNotifications()) {
    return null;
  }
  const cleanKey = publicKey.trim();
  if (!cleanKey) {
    return null;
  }
  const keyBuffer = base64UrlToArrayBuffer(cleanKey);
  const registration = await navigator.serviceWorker.ready;
  const current = await registration.pushManager.getSubscription();
  if (current) {
    const currentKey = current.options?.applicationServerKey;
    if (currentKey && arrayBufferEquals(currentKey, keyBuffer)) {
      return current;
    }
    await current.unsubscribe().catch(() => undefined);
  }
  return await registration.pushManager.subscribe({
    userVisibleOnly: true,
    applicationServerKey: keyBuffer
  });
}

export async function hasPushNotificationSubscription(): Promise<boolean> {
  if (!supportsPushNotifications()) {
    return false;
  }
  try {
    const registration = await navigator.serviceWorker.ready;
    return Boolean(await registration.pushManager.getSubscription());
  } catch {
    return false;
  }
}

export async function showSystemAttentionNotice(url: string, notice: AttentionNotice): Promise<void> {
  if (!("Notification" in window) || Notification.permission !== "granted") {
    return;
  }
  const title = notice.title.trim() || "соты";
  const tag = notice.tag?.trim() || `soty:${url}`;
  const options: NotificationOptions & {
    badge?: string;
    renotify?: boolean;
    silent?: boolean;
    timestamp?: number;
    vibrate?: readonly number[];
  } = {
    body: notice.body || "Новое событие",
    icon: notice.icon || "/icon.svg",
    badge: notice.badge || "/icon.svg",
    tag,
    renotify: notice.renotify === true,
    silent: notice.silent === true,
    data: { url, tag }
  };
  if (notice.vibrate?.length) {
    options.vibrate = notice.vibrate
      .map((item) => Math.max(0, Math.min(220, Math.round(Number(item) || 0))))
      .slice(0, 7);
  }
  if (notice.timestamp && Number.isFinite(notice.timestamp)) {
    options.timestamp = notice.timestamp;
  }
  try {
    const registration = "serviceWorker" in navigator
      ? await navigator.serviceWorker.ready
      : null;
    if (registration?.showNotification) {
      await registration.showNotification(title, options);
      return;
    }
  } catch {
    // Fall through to the page-level notification API.
  }
  try {
    new Notification(title, options);
  } catch {
    // Some browsers only allow ServiceWorkerRegistration.showNotification.
  }
}

export function notifyHiddenOnce(options: AttentionOptions): void {
  if (!options.hidden || options.hadNotice) {
    return;
  }
  const key = `${options.tunnelId}:${options.reason}`;
  if (activeNoticeKeys.has(key)) {
    return;
  }
  activeNoticeKeys.add(key);
  navigator.vibrate?.([24, 36, 24]);
  if (options.notice) {
    void showSystemAttentionNotice(options.url, options.notice);
  }
}

export function clearAttentionNotices(tunnelId: string): void {
  for (const key of [...activeNoticeKeys]) {
    if (key.startsWith(`${tunnelId}:`)) {
      activeNoticeKeys.delete(key);
    }
  }
}

export function shouldNotifyTyping(key: string, now = Date.now()): boolean {
  const last = lastTypingNoticeAt.get(key) || 0;
  if (now - last < 60_000) {
    return false;
  }
  lastTypingNoticeAt.set(key, now);
  return true;
}

function base64UrlToArrayBuffer(value: string): ArrayBuffer {
  const padding = "=".repeat((4 - value.length % 4) % 4);
  const base64 = `${value}${padding}`.replace(/-/gu, "+").replace(/_/gu, "/");
  const raw = window.atob(base64);
  const bytes = new Uint8Array(raw.length);
  for (let index = 0; index < raw.length; index += 1) {
    bytes[index] = raw.charCodeAt(index);
  }
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
}

function arrayBufferEquals(left: ArrayBuffer, right: ArrayBuffer): boolean {
  if (left.byteLength !== right.byteLength) {
    return false;
  }
  const leftBytes = new Uint8Array(left);
  const rightBytes = new Uint8Array(right);
  for (let index = 0; index < leftBytes.length; index += 1) {
    if (leftBytes[index] !== rightBytes[index]) {
      return false;
    }
  }
  return true;
}
