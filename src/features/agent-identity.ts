export const agentIdentity = Object.freeze({
  displayName: "\u041a\u043b\u0430\u0432\u0430",
  legacyDisplayNames: ["\u0410\u0433\u0435\u043d\u0442"] as const,
  operatorHeaderNames: ["\u041a\u043b\u0430\u0432\u0430", "\u0410\u0433\u0435\u043d\u0442", "Codex", "\u041e\u043f\u0435\u0440\u0430\u0442\u043e\u0440", "Operator"] as const
});

export const agentDialogLabel = agentIdentity.displayName;

const operatorHeaderRegex = new RegExp(
  `^(?:${agentIdentity.operatorHeaderNames.map(escapeRegExp).join("|")})\\s+\\u00b7\\s+\\d{1,2}:\\d{2}$`,
  "u"
);

export function isOperatorHeaderText(line: string): boolean {
  return operatorHeaderRegex.test(line);
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}
