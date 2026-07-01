export function createSourceTaskClassifier(dependencies = {}) {
  const {
    cleanActionToken = (value, fallback = "generic") => String(value || fallback || "").trim().toLowerCase(),
    hasWallpaperIntent = () => false,
    isGeneratedImageIntent = () => false,
    hasScreenshotIntent = () => false,
    hasBrowserPageIntent = () => false,
    hasAppWindowIntent = () => false
  } = dependencies;

  function classifyRoutineSourceTask(value) {
    const text = normalizeRoutineIntentText(value);
    if (hasWallpaperIntent(text) && isGeneratedImageIntent(text)) {
      return "generated-image-wallpaper";
    }
    if (hasWallpaperIntent(text) || /wallpaper|desktop background|обои|рабоч\w*\s+стол/u.test(text)) {
      return "download-image-wallpaper";
    }
    if (hasBrowserAutomationIntent(text)) {
      return "browser";
    }
    if (hasSecurityCheckIntent(text)) {
      return "security-check";
    }
    if (hasDriverCheckIntent(text)) {
      return "driver-check";
    }
    if (/battery|powercfg|sleep|lid|power plan|заряд|батаре|питани|сон|крышк/u.test(text)) {
      return "power-check";
    }
    if (/(?:\bport\b|listener|listen|tcp|udp|netstat|порт|слуша|соединен)/u.test(text)) {
      return "system-check";
    }
    if (/(?:system\s+(?:time|date)|set-date|get-date|timezone|time\s+zone|date\/time|системн\w*\s+врем|системн\w*\s+дат|текущ\w*\s+врем|измен\w*\s+врем|часов\w*\s+пояс|дата\s+и\s+врем)/iu.test(text)) {
      return "system-time";
    }
    if (hasExplicitEventLogIntent(text)) {
      return "system-check";
    }
    if (hasAppAutomationIntent(text) || /notepad|calc|calculator|paint|process|pid|start-process|stop-process/u.test(text)) {
      return "program-control";
    }
    if (/script|powershell-скрипт|\.ps1|скрипт/u.test(text)) {
      return "script-task";
    }
    if (/https?:\/\/|www\.|интернет|веб|браузер|сайт|ссылк|заголов/iu.test(text)) {
      return "web-lookup";
    }
    if (/internet|web|browser|curl|invoke-webrequest|официальн|сайт|ссылк|релиз|lts|github|node\.js|powershell/u.test(text)
      && /(official|официальн|релиз|release|lts|stable|стабиль|ссылк|link|github)/u.test(text)) {
      return "web-lookup";
    }
    if (/(?:winget|where\.exe|where\s+|which\s+|installed|version|версии?|установлен[аоы]?|наличи|программ|приложени|git|node|npm|python|pwsh|powershell)\b/u.test(text)
      && !/\b(?:install|upgrade|uninstall|remove)\b|установи|обнови|удали/u.test(text)) {
      return "software-check";
    }
    if (/internet|web|browser|curl|invoke-webrequest|официальн|сайт|ссылк|релиз|lts|github|node\.js|powershell/u.test(text)) {
      return "web-lookup";
    }
    if (/uptime|ram|memory|disk|cpu|bits|windows update|ipv4|ip address|gateway|dns|defender|firewall|памят|диск|шлюз|сеть|сетев|защит|брандмауэр/u.test(text)) {
      return "system-check";
    }
    if (/temp|file|folder|directory|report\.txt|hash|checksum|zip|archive|compress|файл|папк|архив|отчет|отчёт|создай папку|удали скрипт/u.test(text)) {
      return "file-work";
    }
    if (/notepad|calc|calculator|paint|process|pid|start-process|stop-process|блокнот|калькулятор|процесс|запусти|закрой/u.test(text)) {
      return "program-control";
    }
    return "";
  }

  function hasBrowserAutomationIntent(text) {
    const value = String(text || "");
    const hasTarget = hasBrowserPageIntent(value) || /(?:https?:\/\/|www\.|\bvk\b|vk\.com|\bsite\b|\bpage\b|\bbrowser\b|(?:^|[^\p{L}\p{N}_])вк(?:$|[^\p{L}\p{N}_])|вконтакте|браузер|сайт|страниц)/iu.test(value);
    if (!hasTarget) {
      return false;
    }
    return hasScreenshotIntent(value)
      || /(?:\bopen\b|\bgoto\b|\bvisit\b|\bnavigate\b|\bclick\b|\bpress\b|\bread\b|\bfind\b|\bsearch\b|\bdownload\b|\bsave\b|зайди|открой|перейди|нажми|клик|прочитай|прочти|найди|поищи|скачай|сохрани|сделай\s+скрин|скрин)/iu.test(value);
  }

  function hasAppAutomationIntent(text) {
    const value = String(text || "");
    if (!hasAppWindowIntent(value)) {
      return false;
    }
    return hasScreenshotIntent(value)
      || /(?:\bopen\b|\blaunch\b|\bstart\b|\btype\b|\bwrite\b|\binput\b|\bclick\b|\bpress\b|\bread\b|\binspect\b|\bsnapshot\b|открой|запусти|напиши|введи|напечатай|нажми|клик|прочитай|посмотри|снимок|скрин)/iu.test(value);
  }

  function hasExplicitEventLogIntent(text) {
    const value = String(text || "").toLowerCase();
    if (/(?:event\s*log|eventlog|winlog|eventvwr|журнал\s+событи|событи[яй]?\s+windows|windows\s+events|системн\w*\s+журнал)/iu.test(value)) {
      return true;
    }
    const hasErrorWord = /\b(?:errors?|critical|criticals?)\b|ошиб|критич/iu.test(value);
    if (!hasErrorWord) {
      return false;
    }
    const hasSystemAnchor = /\b(?:windows|system|win)\b|винд|систем|журнал|событ|event|за\s+\d{1,3}\s*(?:h|ч|час)|24\s*(?:h|ч|час)|последн|last\s+\d/iu.test(value);
    const hasProbeVerb = /\b(?:check|show|list|find|diagnos|inspect)\b|проверь|проверить|посмотри|покажи|найди|выведи|диагност|последн/iu.test(value);
    return hasSystemAnchor && hasProbeVerb;
  }

  function normalizeRoutineIntentText(text) {
    return String(text || "")
      .toLowerCase()
      .replace(/\b[a-z]:[\\/][^\s"'`<>|]+/giu, " windows-path ")
      .replace(/\bwindows[\\/]+system32[\\/]+drivers[\\/]+etc[\\/]+hosts\b/giu, " windows-hosts-file ");
  }

  function hasDriverCheckIntent(text) {
    const value = String(text || "").toLowerCase();
    return /(?:\bdriver\b|\bdrivers\b|pnputil|devmgmt|device manager|problem device|pnp|драйвер|диспетчер\s+устройств|проблемн\w*\s+устройств|устройств\w*\s+с\s+ошиб)/iu.test(value);
  }

  function hasSecurityCheckIntent(text) {
    const value = String(text || "").toLowerCase();
    return /(?:defender|microsoft\s+defender|windows\s+security|anti-?virus|antivirus|malware|virus|threat|pua|mpcomputerstatus|start-mpscan|get-mpthreat|security\s+center|\u0431\u0435\u0437\u043e\u043f\u0430\u0441\u043d|\u0437\u0430\u0449\u0438\u0442|\u0430\u043d\u0442\u0438\u0432\u0438\u0440\u0443\u0441|\u0432\u0438\u0440\u0443\u0441|\u0443\u0433\u0440\u043e\u0437|\u0432\u0440\u0435\u0434\u043e\u043d\u043e\u0441|\u0437\u0430\u0449\u0438\u0442\u043d\u0438\u043a)/iu.test(value);
  }

  function isRoutineAgentTaskFamily(family) {
    return [
      "program-control",
      "file-work",
      "system-check",
      "system-time",
      "service-check",
      "identity-probe",
      "script-task",
      "web-lookup",
      "power-check",
      "security-check",
      "driver-check",
      "software-check",
      "audio-volume",
      "audio-mute",
      "download-image-wallpaper",
      "generated-image-wallpaper",
      "wallpaper",
      "desktop"
    ].includes(cleanActionToken(family, ""));
  }

  function classifySourceCommand(command) {
    const lower = String(command || "").toLowerCase();
    const routineFamily = classifyRoutineSourceTask(lower);
    if (routineFamily) {
      return routineFamily;
    }
    if (/utf-?8|unicode|codepage|chcp|outputencoding|inputencoding|windowsidentity|text\.encoding|кракозябр|кодиров/u.test(lower)) {
      return "encoding-identity";
    }
    if (/\b(whoami|hostname)\b|computername|username/u.test(lower)) {
      return "identity-probe";
    }
    if (hasDriverCheckIntent(normalizeRoutineIntentText(lower))) {
      return "driver-check";
    }
    if (hasSecurityCheckIntent(normalizeRoutineIntentText(lower))) {
      return "security-check";
    }
    if (/volume|mute|audio|sound|endpointvolume|nircmd|sndvol|speaker|mic|микрофон|звук|громк/u.test(lower)) {
      return /mute|muted|выключ/u.test(lower) ? "audio-mute" : "audio-volume";
    }
    if (/(?:system\s+(?:time|date)|set-date|get-date|timezone|time\s+zone|date\/time|системн\w*\s+врем|системн\w*\s+дат|текущ\w*\s+врем|измен\w*\s+врем|часов\w*\s+пояс|дата\s+и\s+врем)/iu.test(lower)) {
      return "system-time";
    }
    if (hasDriverCheckIntent(normalizeRoutineIntentText(lower))) {
      return "driver-check";
    }
    if (hasSecurityCheckIntent(normalizeRoutineIntentText(lower))) {
      return "security-check";
    }
    if (/systemreset|reagentc\s+\/boottore/u.test(lower)) {
      return "windows-reinstall";
    }
    if (/reinstall|reset this pc|windows reset|winre|recovery|bcd|boot\.wim|setupcomplete|переустанов|сброс|восстановлен|вернуть компьютер|удалить всё|удалить все/u.test(lower)) {
      return "windows-reinstall";
    }
    if (/battery|powercfg|sleep|lid|заряд|питан/u.test(lower)) {
      return "power-check";
    }
    if (/winget|choco|scoop|msiexec|install|установ/u.test(lower)) {
      return "package-install";
    }
    if (/get-service|systemctl|service|служб/u.test(lower)) {
      return "service-check";
    }
    return "generic";
  }

  function isPlainNonDeviceTask(text) {
    const lower = String(text || "").toLowerCase();
    return /без компьютера|не используй компьютер|не трогай компьютер|no computer|without computer/iu.test(lower)
      || (/(омлет|рецепт|готовк|сковород|яичниц|разминк|тренировк|зарядк|workout|warm-?up|exercise)/iu.test(lower) && !/(файл|папк|windows|powershell|cmd|браузер|интернет|сайт|программ|служб|процесс|pid|диск|сеть)/iu.test(lower));
  }

  return Object.freeze({
    classifyRoutineSourceTask,
    hasBrowserAutomationIntent,
    hasAppAutomationIntent,
    hasExplicitEventLogIntent,
    normalizeRoutineIntentText,
    hasDriverCheckIntent,
    hasSecurityCheckIntent,
    isRoutineAgentTaskFamily,
    classifySourceCommand,
    isPlainNonDeviceTask
  });
}
