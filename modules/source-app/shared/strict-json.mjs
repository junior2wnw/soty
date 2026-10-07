/** Small protocol grammar guard, not an identity/permission parser. Decoded
 * duplicate keys and excessive nesting are rejected before JSON.parse. */
export function parseSourceJson(text, { depth = 18, nodes = 4096, bytes = 1500000 } = {}) {
  const invalid = () => { throw Object.assign(new Error('source_app_input_invalid'), { code: 'source_app_input_invalid', status: 400 }); };
  if (typeof text !== 'string' || new TextEncoder().encode(text).length > bytes) invalid();
  let at = 0, count = 0;
  const space = () => { while (at < text.length && ' \t\n\r'.includes(text[at])) at++; };
  function string() {
    if (text[at] !== '"') invalid(); const start = at++;
    while (at < text.length) {
      const char = text[at++];
      if (char === '\\') { at++; continue; }
      if (char === '"') { try { return JSON.parse(text.slice(start, at)); } catch { invalid(); } }
    }
    invalid();
  }
  function value(level) {
    if (level > depth || ++count > nodes) invalid(); space();
    if (text[at] === '{') {
      at++; space(); const keys = new Set(); if (text[at] === '}') { at++; return; }
      while (at < text.length) {
        const key = string(); if (keys.has(key)) invalid(); keys.add(key);
        if (++count > nodes) invalid(); space(); if (text[at++] !== ':') invalid(); value(level + 1); space();
        if (text[at] === '}') { at++; return; } if (text[at++] !== ',') invalid(); space();
      }
    } else if (text[at] === '[') {
      at++; space(); if (text[at] === ']') { at++; return; }
      while (at < text.length) { value(level + 1); space(); if (text[at] === ']') { at++; return; } if (text[at++] !== ',') invalid(); space(); }
    } else if (text[at] === '"') { string(); return; }
    else { const start = at; while (at < text.length && !' \t\n\r,]}'.includes(text[at])) at++; if (start === at) invalid(); return; }
    invalid();
  }
  value(0); space(); if (at !== text.length) invalid();
  try { return JSON.parse(text); } catch { invalid(); }
}
