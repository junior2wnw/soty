import { createHash } from "node:crypto";

const signatureMaxSource = 2000;

export function cleanEnum(value, allowed, fallback) {
  const text = String(value || "").trim();
  return allowed.includes(text) ? text : fallback;
}

export function cleanText(value, max) {
  return String(value || "")
    .replace(/[\u0000-\u001F\u007F]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, max);
}

export function cleanHash(value) {
  const text = String(value || "").trim().toLowerCase();
  return /^[a-f0-9]{8,32}$/u.test(text) ? text.slice(0, 32) : "";
}

export function cleanSignature(value, family = "") {
  const text = cleanText(value, 160);
  if (/^[a-z][a-z0-9_-]{0,39}:[a-f0-9]{16,64}$/iu.test(text)) {
    return text.toLowerCase();
  }
  const normalized = redactLearningText(value).toLowerCase().slice(0, signatureMaxSource);
  const prefix = cleanText(family, 32).toLowerCase().replace(/[^a-z0-9_-]+/gu, "") || "generic";
  return `${prefix}:${hashShort(normalized).slice(0, 16)}`;
}

export function cleanTaskSignature(value) {
  const text = cleanText(value, 160);
  if (!text) {
    return "";
  }
  if (/^task:[a-f0-9]{16,64}$/iu.test(text)) {
    return text.toLowerCase();
  }
  const normalized = redactLearningText(value).toLowerCase().slice(0, signatureMaxSource);
  return `task:${hashShort(normalized).slice(0, 16)}`;
}

export function redactLearningText(value) {
  return cleanText(value, signatureMaxSource)
    .replace(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/giu, "<email>")
    .replace(/\b(?:\d{1,3}\.){3}\d{1,3}\b/gu, "<ip>")
    .replace(/\b[0-9A-F]{2}(?::[0-9A-F]{2}){5}\b/giu, "<mac>")
    .replace(/[A-Za-z]:\\[^\s'"]+/gu, "<path>")
    .replace(/\/(?:Users|home)\/[^\s'"]+/giu, "<path>")
    .replace(/\b[A-Za-z0-9_-]{48,}\b/gu, "<id>")
    .replace(/\b(?:sk|sess|key|token(?!s\b)|secret|password|pwd)[-_A-Za-z0-9]*\b\s*[:=]\s*['"]?[^'"\s]+/giu, "<secret>")
    .replace(/\s+/gu, " ")
    .trim();
}

export function cleanIso(value) {
  const text = cleanText(value, 80);
  if (!text) {
    return "";
  }
  const time = Date.parse(text);
  return Number.isFinite(time) ? new Date(time).toISOString() : "";
}

export function hashShort(value) {
  return createHash("sha256").update(String(value || "")).digest("hex").slice(0, 24);
}
