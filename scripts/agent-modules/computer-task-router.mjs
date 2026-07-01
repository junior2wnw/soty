export function createComputerTaskRouter(dependencies = {}) {
  const {
    firstKeyValue = () => "",
    hasScreenshotIntent = () => false,
    hasBrowserPageIntent = () => false,
    hasWallpaperIntent = () => false,
    isGeneratedImageIntent = () => false
  } = dependencies;

  const appAliasRules = Object.freeze([
    { name: "notepad", pattern: /notepad|\u0431\u043b\u043e\u043a\u043d\u043e\u0442/iu },
    { name: "calculator", pattern: /calc(?:ulator)?|\u043a\u0430\u043b\u044c\u043a\u0443\u043b\u044f\u0442/iu },
    { name: "paint", pattern: /mspaint|paint|\u043f\u0435\u0439\u043d\u0442/iu },
    { name: "explorer", pattern: /explorer|\u043f\u0440\u043e\u0432\u043e\u0434\u043d\u0438\u043a/iu },
    { name: "codex", pattern: /codex|\u043a\u043e\u0434(?:\u0435|\u0436)\u043a\u0441/iu },
    { name: "chrome", pattern: /chrome|\u0445\u0440\u043e\u043c/iu },
    { name: "edge", pattern: /edge|\u044d\u0434\u0436/iu }
  ]);

  const computerIntentPatterns = Object.freeze({
    appWindow: /(?:\bapp(?:lication)?s?\b|\bwindow(?:s)?\b|\bgui\b|\bui\b|\bnotepad\b|\bcalc(?:ulator)?\b|\bmspaint\b|\bpaint\b|\bexplorer\b|\bcodex\b|\u043a\u043e\u0434(?:\u0435|\u0436)\u043a\u0441|\u043e\u043a\u043d|\u043f\u0440\u0438\u043b\u043e\u0436|\u043f\u0440\u043e\u0433\u0440\u0430\u043c|\u0431\u043b\u043e\u043a\u043d\u043e\u0442|\u043a\u0430\u043b\u044c\u043a\u0443\u043b\u044f\u0442|\u043f\u0440\u043e\u0432\u043e\u0434\u043d\u0438\u043a|\u043f\u0435\u0439\u043d\u0442)/iu,
    explicitScript: /(?:powershell|cmd(?:\.exe)?|\bterminal\b|\bconsole\b|\bshell\b|\bscript\b|\bcommand\b|get-process|\bprocess(?:es)?\b|\u043f\u0440\u043e\u0446\u0435\u0441|\u0442\u0435\u0440\u043c\u0438\u043d\u0430\u043b|\u043a\u043e\u043d\u0441\u043e\u043b|\u043a\u043e\u043c\u0430\u043d\u0434|\u0441\u043a\u0440\u0438\u043f\u0442)/iu,
    appLaunch: /(?:launch|start|open\s+(?:app|program|window)|\u0437\u0430\u043f\u0443\u0441\u0442|\u043e\u0442\u043a\u0440)/iu,
    appType: /(?:type|input|enter|write|send|submit|message|\u0432\u0432\u0435\u0434|\u043d\u0430\u043f\u0435\u0447|\u043d\u0430\u043f\u0438\u0448|\u043e\u0442\u043f\u0440\u0430\u0432|\u0441\u043e\u043e\u0431\u0449)/iu,
    appClick: /(?:click|press|invoke|\u043d\u0430\u0436\u043c|\u043a\u043b\u0438\u043a)/iu,
    appSnapshot: /(?:inspect|snapshot|read|elements|controls|\u044d\u043b\u0435\u043c|\u043f\u0440\u043e\u0447\u0438\u0442|\u043f\u043e\u0441\u043c\u043e\u0442\u0440)/iu,
    appList: /(?:\blist\b|\bwindows\b|\bapps\b|\u0441\u043f\u0438\u0441|\u043e\u043a\u043d\u0430|\u043f\u0440\u0438\u043b\u043e\u0436\u0435\u043d)/iu,
    appSubmit: /(?:\bsend\b|\bsubmit\b|\bmessage\b|\bchat\b|\bdialog\b|\u043e\u0442\u043f\u0440\u0430\u0432|\u0441\u043e\u043e\u0431\u0449\u0435\u043d|\u0434\u0438\u0430\u043b\u043e\u0433|\u0447\u0430\u0442|\u043d\u0430\u043f\u0438\u0448\u0438\s+(?:\u0435\u043c\u0443|\u0435\u0439|\u0438\u043c|\u0432\s+(?:\u0447\u0430\u0442|\u0434\u0438\u0430\u043b\u043e\u0433)))/iu,
    wallpaperFamily: /(?:wallpaper|desktop|download-image-wallpaper)/iu,
    download: /(?:\u0441\u043a\u0430\u0447\u0430\u0439|\u0437\u0430\u0433\u0440\u0443\u0437\u0438|download|save\s+(?:it|file)|\u0441\u043e\u0445\u0440\u0430\u043d\u0438)/iu,
    audio: /(?:\u0433\u0440\u043e\u043c\u043a|\u0437\u0432\u0443\u043a|volume|mute|unmute)/iu,
    systemResources: /(?:\u0440\u0435\u0441\u0443\u0440\u0441|cpu|ram|memory|\u043f\u0430\u043c\u044f\u0442|\u0434\u0438\u0441\u043a|disk|\u043d\u0430\u0433\u0440\u0443\u0437)/iu,
    time: /(?:\u0432\u0440\u0435\u043c\u044f|\u0434\u0430\u0442[\u0430\u0443]|time|date)/iu,
    openUrl: /(?:\u043e\u0442\u043a\u0440\u043e\u0439|open|browser|\u0431\u0440\u0430\u0443\u0437\u0435\u0440)/iu,
    web: /(?:\u0438\u043d\u0442\u0435\u0440\u043d\u0435\u0442|\u0441\u0430\u0439\u0442|url|fetch|search|\u043d\u0430\u0439\u0434\u0438|\u043f\u043e\u0438\u0449\u0438|\u0437\u0430\u0433\u0443\u0433\u043b|web)/iu,
    file: /(?:\u0444\u0430\u0439\u043b|\u043f\u0430\u043f\u043a|desktop|\u0440\u0430\u0431\u043e\u0447|read file|write file|create file|delete file|list files)/iu,
    downloadCycle: /(?:\u0443\u0434\u0430\u043b\u0438|\u0443\u0434\u0430\u043b\u0438\u0442\u044c|delete|remove|cleanup|clean up)/iu,
    fileWrite: /(?:\u0441\u043e\u0437\u0434\u0430[\u0439\u0442\u044c]|\u0437\u0430\u043f\u0438\u0448\u0438|\u043d\u0430\u043f\u0438\u0448\u0438|write|create)/iu,
    fileRead: /(?:\u043f\u0440\u043e\u0447\u0438\u0442\u0430[\u0439\u0442\u044c]|\u0441\u0447\u0438\u0442\u0430\u0439|\u043f\u0440\u043e\u0432\u0435\u0440\u044c|verify|read|show)/iu,
    fileDelete: /(?:\u0443\u0434\u0430\u043b\u0438|\u0443\u0434\u0430\u043b\u0438\u0442\u044c|delete|remove)/iu,
    fileAppend: /(?:\u0434\u043e\u0431\u0430\u0432\u044c|append)/iu,
    fileList: /(?:\u0441\u043f\u0438\u0441\u043e\u043a|list|ls|\u043f\u043e\u043a\u0430\u0436\u0438\s+\u0444\u0430\u0439\u043b\u044b)/iu,
    fileStat: /(?:\u0441\u0442\u0430\u0442\u0443\u0441|stat|exists|\u0441\u0443\u0449\u0435\u0441\u0442\u0432)/iu,
    timeSet: /(?:\u0443\u0441\u0442\u0430\u043d\u043e\u0432|set|\u0438\u0437\u043c\u0435\u043d)/iu,
    script: /(?:powershell|cmd|\u043a\u043e\u043c\u0430\u043d\u0434|\u0441\u043a\u0440\u0438\u043f\u0442|terminal|console|\u0437\u0430\u043f\u0443\u0441\u0442\u0438)/iu
  });

  const appTypeActionAliases = Object.freeze(["type", "write", "input", "enter", "send", "submit"]);
  const appClickActionAliases = Object.freeze(["click", "press", "invoke"]);

  function hasComputerIntent(name, value) {
    const pattern = computerIntentPatterns[name];
    return Boolean(pattern && pattern.test(String(value || "")));
  }

  function hasAppWindowIntent(value) {
    return hasComputerIntent("appWindow", value);
  }

  function hasExplicitScriptIntent(value) {
    return hasComputerIntent("explicitScript", value);
  }

  function inferAppNameFromText(text) {
    const value = String(text || "");
    const keyed = firstKeyValue(value, ["app", "application", "window", "title"]);
    if (keyed) return keyed;
    for (const alias of appAliasRules) {
      if (alias.pattern.test(value)) {
        return alias.name;
      }
    }
    return "";
  }

  const computerOperationRules = Object.freeze([
    { operation: ({ lower }) => hasBrowserPageIntent(lower) ? "browser" : "desktop", when: ({ lower }) => hasScreenshotIntent(lower) },
    { operation: "wallpaper", when: ({ lower, family }) => (hasWallpaperIntent(lower) || hasComputerIntent("wallpaperFamily", family)) && !isGeneratedImageIntent(lower) },
    { operation: "image", when: ({ lower }) => isGeneratedImageIntent(lower) && hasWallpaperIntent(lower) },
    { operation: "browser", when: ({ args }) => Boolean(args.url && args.text) },
    { operation: "download", when: ({ args, lower }) => Boolean(args.url && hasComputerIntent("download", lower)) },
    { operation: "audio", when: ({ args, lower }) => args.volumePercent !== undefined || hasComputerIntent("audio", lower) },
    { operation: "system-resources", when: ({ lower, family }) => hasComputerIntent("systemResources", lower) || family === "system-check" },
    { operation: "time", when: ({ lower, family }) => hasComputerIntent("time", lower) || family === "system-time" },
    { operation: "open-url", when: ({ args, lower }) => Boolean(args.url && hasComputerIntent("openUrl", lower)) },
    { operation: "web", when: ({ args, lower, family }) => Boolean(args.url || args.query || hasComputerIntent("web", lower) || family === "web-lookup") },
    { operation: "app", when: ({ text, family }) => hasAppWindowIntent(text) || family === "app" || family === "computer-use" },
    { operation: "file", when: ({ args, lower, family }) => Boolean(args.path || hasComputerIntent("file", lower) || family === "file-work") },
    { operation: "script", when: ({ args, lower, family }) => Boolean(args.script || hasComputerIntent("script", lower) || family === "script-task") }
  ]);

  const computerActionResolvers = Object.freeze({
    browser: ({ lower, args }) => {
      if (hasScreenshotIntent(lower)) return "screenshot";
      if (args.text || args.linkText || args.selector || args.target || hasComputerIntent("appClick", lower)) return "click_text";
      return args.url ? "text" : "status";
    },
    desktop: ({ lower }) => hasScreenshotIntent(lower) ? "screenshot" : "status",
    wallpaper: () => "wallpaper",
    web: ({ args }) => args.url && !args.query ? "fetch" : "search",
    download: ({ lower }) => hasComputerIntent("downloadCycle", lower) ? "cycle" : "save",
    app: ({ text, lower, args }) => {
      if (hasComputerIntent("appLaunch", lower) && (args.app || inferAppNameFromText(text))) return "launch";
      if (hasComputerIntent("appType", lower) && (args.content || args.value || args.input || (!args.target && args.text))) return "type";
      if (hasComputerIntent("appClick", lower) || args.target || args.text) return "click";
      if (hasComputerIntent("appSnapshot", lower) || args.app || args.window || args.title) return "snapshot";
      return "list";
    },
    file: ({ lower, args }) => {
      const wantsWrite = args.content !== undefined || hasComputerIntent("fileWrite", lower);
      const wantsRead = hasComputerIntent("fileRead", lower);
      const wantsDelete = hasComputerIntent("fileDelete", lower);
      if (args.path && args.content !== undefined && wantsWrite && wantsRead && wantsDelete) return "cycle";
      if (wantsDelete) return "delete";
      if (hasComputerIntent("fileAppend", lower)) return "append";
      if (hasComputerIntent("fileRead", lower) && !args.content) return "read";
      if (hasComputerIntent("fileList", lower)) return "list";
      if (hasComputerIntent("fileStat", lower)) return "stat";
      return args.content !== undefined ? "write" : "stat";
    },
    time: ({ lower }) => hasComputerIntent("timeSet", lower) ? "set" : "status",
    audio: ({ args }) => args.volumePercent !== undefined ? "set" : "status"
  });

  function inferGonkaComputerOperationFromText(text, family, args = {}) {
    const context = {
      text: String(text || ""),
      lower: String(text || "").toLowerCase(),
      family: String(family || ""),
      args: args || {}
    };
    for (const rule of computerOperationRules) {
      if (rule.when(context)) {
        return typeof rule.operation === "function" ? rule.operation(context) : rule.operation;
      }
    }
    return "system-resources";
  }

  function inferGonkaComputerActionFromText(text, operation, args) {
    const actionArgs = args || {};
    const normalizedOperation = String(operation || "");
    const resolver = computerActionResolvers[normalizedOperation];
    if (!resolver) {
      return actionArgs.action || "status";
    }
    return resolver({
      text: String(text || ""),
      lower: String(text || "").toLowerCase(),
      operation: normalizedOperation,
      args: actionArgs
    });
  }

  return Object.freeze({
    appAliasRules,
    computerIntentPatterns,
    appTypeActionAliases,
    appClickActionAliases,
    computerOperationRules,
    computerActionResolvers,
    hasComputerIntent,
    hasAppWindowIntent,
    hasExplicitScriptIntent,
    inferAppNameFromText,
    inferGonkaComputerOperationFromText,
    inferGonkaComputerActionFromText
  });
}
