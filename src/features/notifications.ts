export type AttentionNotice = {
  readonly title: string;
  readonly body?: string;
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
  return "Notification" in window && Notification.permission === "default";
}

export async function showSystemAttentionNotice(url: string, notice: AttentionNotice): Promise<void> {
  if (!("Notification" in window) || Notification.permission !== "granted") {
    return;
  }
  const title = notice.title.trim() || "соты";
  const options: NotificationOptions = {
    body: notice.body || "Новое событие",
    icon: "/icon.svg",
    badge: "/icon.svg",
    tag: `soty:${url}`,
    data: { url }
  };
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
  navigator.vibrate?.([45, 70, 45]);
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
