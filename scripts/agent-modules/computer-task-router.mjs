export function createComputerTaskRouter() {
  const computerActionPattern = /(?:\b(?:set|download|install|create|write|delete|remove|open|click|press|change|run|start|stop|check|scan|repair|update|save|find|read|copy|move|rename|type|send|submit|launch|close|screenshot|capture|browse|search|fetch)\b|\u0441\u043a\u0430\u0447|\u0437\u0430\u0433\u0440\u0443\u0437|\u0443\u0441\u0442\u0430\u043d\u043e\u0432|\u0441\u043e\u0437\u0434|\u0437\u0430\u043f\u0438\u0448|\u0437\u0430\u043f\u0443\u0441\u0442|\u0443\u0434\u0430\u043b|\u0441\u043e\u0445\u0440\u0430\u043d|\u043e\u0442\u043a\u0440|\u043d\u0430\u0436\u043c|\u043a\u043b\u0438\u043a|\u0438\u0437\u043c\u0435\u043d|\u0441\u0434\u0435\u043b|\u043f\u0440\u043e\u0432\u0435\u0440|\u0441\u043a\u0430\u043d|\u043f\u043e\u0447\u0438\u043d|\u043e\u0431\u043d\u043e\u0432|\u043d\u0430\u0439\u0434|\u043f\u0440\u043e\u0447\u0438\u0442|\u0441\u043a\u043e\u043f\u0438\u0440|\u043f\u0435\u0440\u0435\u043c\u0435\u0441\u0442|\u043f\u0435\u0440\u0435\u0438\u043c\u0435\u043d|\u043d\u0430\u043f\u0435\u0447|\u0432\u0432\u0435\u0434|\u043e\u0442\u043f\u0440\u0430\u0432|\u0437\u0430\u0439\u0434|\u0441\u043a\u0440\u0438\u043d)/iu;
  const appWindowPattern = /(?:\b(?:app|application|window|gui|ui|notepad|calculator|paint|explorer|codex)\b|\u043e\u043a\u043d|\u043f\u0440\u0438\u043b\u043e\u0436|\u043f\u0440\u043e\u0433\u0440\u0430\u043c|\u0431\u043b\u043e\u043a\u043d\u043e\u0442|\u043a\u0430\u043b\u044c\u043a\u0443\u043b\u044f\u0442|\u043f\u0440\u043e\u0432\u043e\u0434\u043d\u0438\u043a|\u043a\u043e\u0434(?:\u0435|\u0436)\u043a\u0441)/iu;
  const explicitScriptPattern = /(?:powershell|cmd(?:\.exe)?|\bterminal\b|\bconsole\b|\bshell\b|\bscript\b|\bcommand\b|\u043f\u0440\u043e\u0446\u0435\u0441|\u0442\u0435\u0440\u043c\u0438\u043d\u0430\u043b|\u043a\u043e\u043d\u0441\u043e\u043b|\u043a\u043e\u043c\u0430\u043d\u0434|\u0441\u043a\u0440\u0438\u043f\u0442)/iu;

  function hasComputerIntent(name, value) {
    if (name === "appWindow") {
      return appWindowPattern.test(String(value || ""));
    }
    if (name === "explicitScript") {
      return explicitScriptPattern.test(String(value || ""));
    }
    return computerActionPattern.test(String(value || ""));
  }

  function hasAppWindowIntent(value) {
    return hasComputerIntent("appWindow", value);
  }

  function hasExplicitScriptIntent(value) {
    return hasComputerIntent("explicitScript", value);
  }

  return Object.freeze({
    hasComputerIntent,
    hasAppWindowIntent,
    hasExplicitScriptIntent
  });
}
