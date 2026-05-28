export type QuickAction = {
  readonly schema?: string;
  readonly id: string;
  readonly title: string;
  readonly label: string;
  readonly summary: string;
  readonly tags: readonly string[];
  readonly source?: string;
  readonly kind?: string;
  readonly runtime?: Record<string, unknown>;
  readonly hidden?: boolean;
};

export const quickActions: readonly QuickAction[] = [
  {
    id: "windows-reinstall",
    title: "Переустановка Windows",
    label: "ОС",
    summary: "Подготовить, проверить, подтвердить и сопровождать установку.",
    tags: ["windows", "винда", "переустановка", "usb", "флешка", "драйверы"]
  },
  {
    id: "wallpaper",
    title: "Поставить обои",
    label: "ФОН",
    summary: "Создать или взять картинку и поставить на нужный рабочий стол.",
    tags: ["обои", "wallpaper", "рабочий стол", "картинка", "image"]
  },
  {
    id: "copy-file",
    title: "Скопировать файл",
    label: "ФАЙЛ",
    summary: "Перенести файл между текущим и подключенным устройством.",
    tags: ["копировать", "файл", "download", "upload", "передать", "скачать"]
  },
  {
    id: "check-device",
    title: "Проверить устройство",
    label: "ПРОВ",
    summary: "Понять состояние агента, сети, диска, процессов и доступа.",
    tags: ["проверить", "статус", "диагностика", "агент", "сеть", "диск"]
  },
  {
    id: "agent-repair",
    title: "Починить Клаву",
    label: "КЛАВА",
    summary: "Проверить установку, обновление, автозапуск и связь Клавы.",
    tags: ["агент", "установить", "обновить", "починить", "bridge", "relay"]
  },
  {
    id: "native-window-chrome",
    hidden: true,
    title: "Окно без хедера",
    label: "HDR",
    summary: "Спрятать системную полосу PWA, закрепить watcher и проверить, что окно не уходит под Пуск.",
    tags: ["окно", "хедер", "titlebar", "pwa", "chrome", "frameless", "свернуть", "двигать", "resize"]
  },
  {
    id: "soty-export",
    title: "Экспорт Сот",
    label: "СОХР",
    summary: "Собрать перенос состояния в один файл и восстановить из него.",
    tags: ["экспорт", "импорт", "backup", "перенос", "флешка", "соты"]
  }
];

export async function fetchFrontendQuickActions(timeoutMs = 1400): Promise<readonly QuickAction[]> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch("/api/frontend/capabilities", {
      cache: "no-store",
      signal: controller.signal
    });
    if (!response.ok) {
      return [];
    }
    const payload = await response.json() as { readonly actions?: readonly unknown[] };
    return Array.isArray(payload.actions)
      ? payload.actions
        .map((item) => normalizeFrontendQuickAction(item))
        .filter((item): item is QuickAction => Boolean(item))
      : [];
  } catch {
    return [];
  } finally {
    window.clearTimeout(timer);
  }
}

export function mergeQuickActions(
  curated: readonly QuickAction[],
  generated: readonly QuickAction[]
): readonly QuickAction[] {
  const seen = new Set<string>();
  const result: QuickAction[] = [];
  for (const action of [...curated, ...generated]) {
    if (!action.id || action.hidden || seen.has(action.id)) {
      continue;
    }
    seen.add(action.id);
    result.push(action);
  }
  return result;
}

function normalizeFrontendQuickAction(value: unknown): QuickAction | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = cleanId(recordString(value, "id"));
  const title = cleanText(recordString(value, "title"), 100);
  if (!id || !title) {
    return null;
  }
  return {
    schema: cleanText(recordString(value, "schema"), 80),
    id,
    title,
    label: cleanText(recordString(value, "label"), 16) || labelFor(title),
    summary: cleanText(recordString(value, "summary"), 220),
    tags: cleanList(value.tags, 32, 80),
    source: cleanText(recordString(value, "source"), 60),
    kind: cleanText(recordString(value, "kind"), 60),
    runtime: cleanRuntime(value.runtime)
  };
}

function cleanRuntime(value: unknown): Record<string, unknown> {
  if (!isRecord(value)) {
    return {};
  }
  const result: Record<string, unknown> = {};
  for (const [key, raw] of Object.entries(value).slice(0, 32)) {
    const cleanKey = cleanId(key).slice(0, 80);
    if (!cleanKey) {
      continue;
    }
    if (typeof raw === "boolean" || typeof raw === "number") {
      result[cleanKey] = raw;
    } else if (typeof raw === "string") {
      result[cleanKey] = cleanText(raw, 240);
    } else if (Array.isArray(raw)) {
      result[cleanKey] = cleanList(raw, 32, 120);
    }
  }
  return result;
}

function cleanList(value: unknown, maxItems: number, maxLength: number): readonly string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => cleanText(String(item || ""), maxLength))
    .filter(Boolean)
    .slice(0, maxItems);
}

function cleanId(value: string): string {
  return value
    .trim()
    .replace(/[^A-Za-z0-9._:-]+/gu, "-")
    .replace(/-+/gu, "-")
    .replace(/^-|-$/gu, "")
    .slice(0, 180);
}

function cleanText(value: string, maxLength: number): string {
  return String(value || "")
    .replace(/[\u0000-\u001F\u007F]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, maxLength);
}

function labelFor(value: string): string {
  const words = cleanText(value, 80).split(/[^A-Za-z0-9]+/u).filter(Boolean);
  if (words.length >= 2) {
    return words.slice(0, 2).map((word) => word[0]).join("").toUpperCase();
  }
  return (words[0] || "ACT").slice(0, 4).toUpperCase();
}

function recordString(value: Record<string, unknown>, key: string): string {
  const item = value[key];
  return typeof item === "string" ? item : "";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
