/** Operational text bounds, not a claimed tokenizer-specific 500-token guarantee. */
export const LIVE_ANSWER_MAX_CHARACTERS = 9000;
export const LIVE_SPEECH_MAX_CHARACTERS = 650;

export const LIVE_ANSWER_VALIDATION_ERRORS = [
  "missing_answer", "answer_too_long", "missing_speech", "speech_too_long",
  "speech_lead_mismatch", "incomplete_response",
] as const;

export function firstAnswerParagraph(answer: string): string {
  return answer.trim().split(/(?:\r?\n[\t ]*){2,}|\u2029/u, 1)[0].trim();
}

export function liveAnswerTextIssues(value: unknown): (typeof LIVE_ANSWER_VALIDATION_ERRORS)[number][] {
  const candidate = value && typeof value === "object" ? value as Record<string, unknown> : {};
  const issues: (typeof LIVE_ANSWER_VALIDATION_ERRORS)[number][] = [];
  const hasAnswer = typeof candidate.answer === "string" && Boolean(candidate.answer.trim());
  const hasSpeech = typeof candidate.speech === "string" && Boolean(candidate.speech.trim());
  if (!hasAnswer) issues.push("missing_answer");
  if (typeof candidate.answer === "string" && candidate.answer.length > LIVE_ANSWER_MAX_CHARACTERS) issues.push("answer_too_long");
  if (!hasSpeech) issues.push("missing_speech");
  if (typeof candidate.speech === "string" && candidate.speech.length > LIVE_SPEECH_MAX_CHARACTERS) issues.push("speech_too_long");
  if (hasAnswer && hasSpeech && (candidate.speech as string).trim() !== firstAnswerParagraph(candidate.answer as string)) issues.push("speech_lead_mismatch");
  return issues;
}

/** Both channels come from one generation. Never turn a rejected answer into a partial one. */
export function validateLiveAnswerText(value: unknown): { answer: string; speech: string } {
  const issues = liveAnswerTextIssues(value);
  if (issues.length) throw new Error(issues[0]);
  const candidate = value as { answer: string; speech: string };
  return { answer: candidate.answer.trim(), speech: candidate.speech.trim() };
}
