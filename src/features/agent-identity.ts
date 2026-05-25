export const agentIdentity = Object.freeze({
  displayName: "\u041a\u043b\u0430\u0432\u0430",
  legacyDisplayNames: ["\u0410\u0433\u0435\u043d\u0442"] as const,
  invocationWords: ["\u043a\u043b\u0430\u0432\u0430", "klava"] as const,
  operatorHeaderNames: ["\u041a\u043b\u0430\u0432\u0430", "\u0410\u0433\u0435\u043d\u0442", "Codex", "\u041e\u043f\u0435\u0440\u0430\u0442\u043e\u0440", "Operator"] as const
});

export const agentDialogLabel = agentIdentity.displayName;

const invocationAlternates = agentIdentity.invocationWords.map(escapeRegExp).join("|");
const agentInvocationRegex = new RegExp(`(^|[^\\p{L}\\p{N}_])(?:${invocationAlternates})(?=$|[^\\p{L}\\p{N}_])`, "iu");
const leadingAgentInvocationRegex = new RegExp(`^\\s*(?:${invocationAlternates})\\s*[,.:;!?-]*\\s*`, "iu");
const operatorHeaderRegex = new RegExp(
  `^(?:${agentIdentity.operatorHeaderNames.map(escapeRegExp).join("|")})\\s+\\u00b7\\s+\\d{1,2}:\\d{2}$`,
  "u"
);

export function containsAgentInvocationText(text: string): boolean {
  return agentInvocationRegex.test(text);
}

export function stripAgentInvocationText(text: string): string {
  const stripped = text.replace(leadingAgentInvocationRegex, "").trim();
  return stripped || text;
}

export function isOperatorHeaderText(line: string): boolean {
  return operatorHeaderRegex.test(line);
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}
