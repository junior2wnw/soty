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
  readonly agentCard: {
    readonly intent: string;
    readonly targetPolicy?: string;
    readonly firstMoves?: readonly string[];
    readonly confirmBefore: readonly string[];
    readonly successProof: readonly string[];
    readonly avoid?: readonly string[];
  };
};

export const quickActions: readonly QuickAction[] = [
  {
    id: "windows-reinstall",
    title: "Переустановка Windows",
    label: "ОС",
    summary: "Подготовить, проверить, подтвердить и сопровождать установку.",
    tags: ["windows", "винда", "переустановка", "usb", "флешка", "драйверы"],
    agentCard: {
      intent: "Safely prepare and guide a Windows reinstall/reset on the selected/current device.",
      targetPolicy: "Use the current dialog/source device unless the user explicitly names another linked device.",
      firstMoves: [
        "Ask only for missing install mode, USB presence, and USB erase permission.",
        "Use managed reinstall capability for prepare/status/repair/arm when available.",
        "Keep long work durable and continue polling until completed, blocked, or waiting for final confirmation."
      ],
      confirmBefore: ["erasing USB media", "starting the final reboot/install step"],
      successProof: ["fresh reinstall status", "prepared media proof", "final user confirmation before destructive step"],
      avoid: ["manual ISO path requests when managed media download is available", "starting duplicate prepare jobs", "treating a healthy running job as a blocker"]
    }
  },
  {
    id: "wallpaper",
    title: "Поставить обои",
    label: "ФОН",
    summary: "Создать или взять картинку и поставить на нужный рабочий стол.",
    tags: ["обои", "wallpaper", "рабочий стол", "картинка", "image"],
    agentCard: {
      intent: "Create or use an image and set it as wallpaper on the correct target desktop.",
      targetPolicy: "Never switch to an unnamed linked device. Use the current/source computer unless the current dialog or comment names another device.",
      firstMoves: [
        "Resolve image source: generate, use attached file, or use named existing file.",
        "Resolve target device from current dialog and user wording.",
        "Apply wallpaper through the best available interactive/user route and verify the actual wallpaper path or visible state."
      ],
      confirmBefore: [],
      successProof: ["target device identity", "file path/hash or generated image proof", "wallpaper readback on the same target"],
      avoid: ["claiming success without readback", "using the only linked device as implicit target", "hiding a failed apply behind a generic done message"]
    }
  },
  {
    id: "copy-file",
    title: "Скопировать файл",
    label: "ФАЙЛ",
    summary: "Перенести файл между текущим и подключенным устройством.",
    tags: ["копировать", "файл", "download", "upload", "передать", "скачать"],
    agentCard: {
      intent: "Copy a file between the current computer and a selected/mentioned linked device.",
      targetPolicy: "Infer direction from wording and current dialog: in a device chat, source is that device and destination is the user's current computer unless stated otherwise.",
      firstMoves: [
        "Identify exact source file and destination.",
        "Use the native Soty artifact/file route for cross-device transfer.",
        "Verify size/hash or destination listing."
      ],
      confirmBefore: ["overwriting an existing file", "copying large/sensitive folders"],
      successProof: ["source path", "destination path", "size/hash readback"],
      avoid: ["printing raw secrets from files", "copying ambiguous paths", "claiming transfer before destination proof"]
    }
  },
  {
    id: "check-device",
    title: "Проверить устройство",
    label: "ПРОВ",
    summary: "Понять состояние агента, сети, диска, процессов и доступа.",
    tags: ["проверить", "статус", "диагностика", "агент", "сеть", "диск"],
    agentCard: {
      intent: "Run a compact health/status diagnostic on the current or named device.",
      targetPolicy: "Probe the selected/current device first; only inspect another device if the user names it.",
      firstMoves: [
        "Collect identity, agent/link status, OS, disk, network, and recent task status.",
        "Keep probes short and non-destructive.",
        "Summarize one concrete blocker or the next useful action."
      ],
      confirmBefore: [],
      successProof: ["device identity", "fresh timestamped status", "specific failed component if any"],
      avoid: ["large inventories before a focused probe", "raw transport jargon in user-facing answer", "mixing statuses from different devices"]
    }
  },
  {
    id: "agent-repair",
    title: "Починить Клаву",
    label: "КЛАВА",
    summary: "Проверить установку, обновление, автозапуск и связь Клавы.",
    tags: ["агент", "установить", "обновить", "починить", "bridge", "relay"],
    agentCard: {
      intent: "Repair or update the Soty agent on the current/named device with proof.",
      targetPolicy: "Prefer the current device context; for linked devices use only explicit current dialog target or named target.",
      firstMoves: [
        "Check agent health/version/autostart before reinstalling.",
        "Use the official Soty installer/update path.",
        "Verify local health and relay/source status after changes."
      ],
      confirmBefore: ["privileged install/update prompts", "stopping user-visible active work"],
      successProof: ["agent version", "health endpoint or relay status", "source worker/machine link readiness when relevant"],
      avoid: ["installing duplicate agents", "masking PATH/runtime issues as missing Codex", "leaving the user without a clear next action"]
    }
  },
  {
    id: "native-window-chrome",
    hidden: true,
    title: "Окно без хедера",
    label: "HDR",
    summary: "Спрятать системную полосу PWA, закрепить watcher и проверить, что окно не уходит под Пуск.",
    tags: ["окно", "хедер", "titlebar", "pwa", "chrome", "frameless", "свернуть", "двигать", "resize"],
    agentCard: {
      intent: "Enable and verify the frameless Soty PWA window on the selected/current device.",
      targetPolicy: "Prefer the current/source device. Use another linked device only when the current dialog or user wording names it.",
      firstMoves: [
        "Run the native-window-chrome computer operation in install/apply mode with ClientTitlebarHeight=32.",
        "Verify caption=false, frameless=true, clientTitlebar.hidden=true, and watcher persistence.",
        "Check that the visible window stays inside the working area and can still be resized from the side edges."
      ],
      confirmBefore: [],
      successProof: ["caption=false", "frameless=true", "clientTitlebar.hidden=true", "clientTitlebar.bottomInsideWorkingArea=true", "persistence includes scheduled-task or hkcu-run"],
      avoid: ["changing unrelated Chrome windows", "leaving duplicate watcher processes", "claiming success without fresh status JSON"]
    }
  },
  {
    id: "soty-export",
    title: "Экспорт Сот",
    label: "СОХР",
    summary: "Собрать перенос состояния в один файл и восстановить из него.",
    tags: ["экспорт", "импорт", "backup", "перенос", "флешка", "соты"],
    agentCard: {
      intent: "Help the user export/import Soty state as a single portable file.",
      targetPolicy: "This is normally a current-browser/current-device action unless the user names another device.",
      firstMoves: [
        "Find the current available export/import mechanism.",
        "Guide or perform the export/import with one file.",
        "Verify dialogs/devices restored after import."
      ],
      confirmBefore: ["overwriting current local Soty state during import"],
      successProof: ["export file exists or import count", "selected device/dialog state after import"],
      avoid: ["automatic hidden backup during OS reinstall", "splitting state across many files", "pretending an import happened without count/readback"]
    }
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
  const agentCard = isRecord(value.agentCard) ? value.agentCard : {};
  const intent = cleanText(recordString(agentCard, "intent") || title, 260);
  const successProof = cleanList(agentCard.successProof, 10, 140);
  return {
    schema: cleanText(recordString(value, "schema"), 80),
    id,
    title,
    label: cleanText(recordString(value, "label"), 16) || labelFor(title),
    summary: cleanText(recordString(value, "summary"), 220),
    tags: cleanList(value.tags, 32, 80),
    source: cleanText(recordString(value, "source"), 60),
    kind: cleanText(recordString(value, "kind"), 60),
    runtime: cleanRuntime(value.runtime),
    agentCard: {
      intent,
      targetPolicy: cleanText(recordString(agentCard, "targetPolicy"), 260),
      firstMoves: cleanList(agentCard.firstMoves, 10, 220),
      confirmBefore: cleanList(agentCard.confirmBefore, 10, 180),
      successProof: successProof.length > 0 ? successProof : ["status", "result"],
      avoid: cleanList(agentCard.avoid, 10, 180)
    }
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
