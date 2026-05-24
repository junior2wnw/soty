export type QuickAction = {
  readonly id: string;
  readonly title: string;
  readonly label: string;
  readonly summary: string;
  readonly tags: readonly string[];
  readonly hidden?: boolean;
  readonly agentCard: {
    readonly intent: string;
    readonly targetPolicy: string;
    readonly firstMoves: readonly string[];
    readonly confirmBefore: readonly string[];
    readonly successProof: readonly string[];
    readonly avoid: readonly string[];
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
    title: "Починить агент",
    label: "АЛИК",
    summary: "Проверить установку, обновление, автозапуск и связь агента.",
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
