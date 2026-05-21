export function normalizeRelayId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{32,192}$/u.test(text) ? text : "";
}

export function cleanText(value, max) {
  return typeof value === "string" ? value.slice(0, max) : "";
}

export function safeRelayLimit(value, fallback, max) {
  const limit = Number.parseInt(String(value || ""), 10);
  return Number.isSafeInteger(limit) ? Math.max(1000, Math.min(limit, max)) : fallback;
}

export function cleanArtifactToken(value) {
  const text = String(value || "").trim();
  return /^[0-9a-z]+_[0-9a-f]{32}$/u.test(text) ? text : "";
}

export function cleanDownloadName(value) {
  return String(Array.isArray(value) ? value[0] : value || "")
    .replace(/[\\/:*?"<>|]/gu, "_")
    .trim()
    .slice(0, 160) || "artifact.bin";
}

export function cleanMimeType(value) {
  const text = String(Array.isArray(value) ? value[0] : value || "").trim().slice(0, 160);
  return /^[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*\/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*$/u.test(text)
    ? text
    : "application/octet-stream";
}

export function cleanHex(value, length) {
  const text = String(Array.isArray(value) ? value[0] : value || "").trim().toLowerCase();
  return new RegExp(`^[0-9a-f]{${length}}$`, "u").test(text) ? text : "";
}

export function safeRunAs(value) {
  const text = String(value || "").trim().toLowerCase();
  return text === "system" || text === "machine" || text === "elevated" ? "system" : "user";
}

export function cleanStringList(value, maxItems, maxChars) {
  if (!Array.isArray(value)) {
    return [];
  }
  return [...new Set(value.map((item) => cleanText(item, maxChars)).filter(Boolean))].slice(0, maxItems);
}
