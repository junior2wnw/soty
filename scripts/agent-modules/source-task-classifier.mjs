export function createSourceTaskClassifier() {
  function normalizeRoutineIntentText(text) {
    return String(text || "").toLowerCase();
  }

  function isRoutineAgentTaskFamily() {
    return false;
  }

  function classifySourceCommand(command) {
    const lower = normalizeRoutineIntentText(command);
    if (/\b(?:systemreset|reagentc\s+\/boottore)\b/u.test(lower)
      || /\b(?:reinstall|reset this pc|windows reset|winre|recovery|bcd|boot\.wim|setupcomplete)\b/u.test(lower)
      || /(?:\u043f\u0435\u0440\u0435\u0443\u0441\u0442\u0430\u043d\u043e\u0432|\u0441\u0431\u0440\u043e\u0441|\u0432\u043e\u0441\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d|\u0432\u0435\u0440\u043d\u0443\u0442\u044c\s+\u043a\u043e\u043c\u043f|\u0443\u0434\u0430\u043b\u0438\u0442\u044c\s+\u0432\u0441[её])/iu.test(lower)) {
      return "windows-reinstall";
    }
    return "generic";
  }

  function isPlainNonDeviceTask(text) {
    return /(?:\bno\s+computer\b|\bwithout\s+(?:the\s+)?computer\b|\bdo\s+not\s+use\s+(?:the\s+)?computer\b|\u0431\u0435\u0437\s+\u043a\u043e\u043c\u043f\u044c\u044e\u0442\u0435\u0440\u0430|\u043d\u0435\s+\u0438\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439\s+\u043a\u043e\u043c\u043f|\u043d\u0435\s+\u0442\u0440\u043e\u0433\u0430\u0439\s+\u043a\u043e\u043c\u043f)/iu.test(String(text || ""));
  }

  return Object.freeze({
    isRoutineAgentTaskFamily,
    classifySourceCommand,
    isPlainNonDeviceTask
  });
}
