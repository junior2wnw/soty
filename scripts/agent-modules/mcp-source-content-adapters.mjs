export function createMcpSourceContentAdapters() {
  function sourceOpenUrlScript(url) {
    const payload = Buffer.from(JSON.stringify({ url: String(url || "").slice(0, 4000) }), "utf8").toString("base64");
    return `
const { spawn } = await import("node:child_process");
const req = JSON.parse(Buffer.from("${payload}", "base64").toString("utf8"));
const url = String(req.url || "");
if (!/^https?:\\/\\//i.test(url)) {
  console.error("invalid url");
  process.exit(2);
}
const command = process.platform === "win32"
  ? { file: "cmd.exe", args: ["/d", "/s", "/c", "start", "", url] }
  : process.platform === "darwin"
    ? { file: "open", args: [url] }
    : { file: "xdg-open", args: [url] };
const child = spawn(command.file, command.args, { detached: true, stdio: "ignore", windowsHide: false });
child.unref();
console.log(JSON.stringify({ ok: true, action: "open", url, platform: process.platform }));
`.trim();
  }

  function sourceFileScript(args) {
    const payload = Buffer.from(JSON.stringify({
      action: String(args.action || "").slice(0, 40),
      path: String(args.path || "").slice(0, 2000),
      toPath: String(args.toPath || "").slice(0, 2000),
      content: String(args.content || "").slice(0, 300_000),
      downloadName: String(args.downloadName || "").slice(0, 240),
      mimeType: String(args.mimeType || "").slice(0, 160),
      pattern: String(args.pattern || "").slice(0, 2000),
      glob: String(args.glob || "").slice(0, 200),
      regex: args.regex === true,
      recursive: args.recursive === true,
      maxResults: Number.isSafeInteger(args.maxResults) ? Math.max(1, Math.min(args.maxResults, 500)) : 80,
      maxChars: Number.isSafeInteger(args.maxChars) ? Math.max(1000, Math.min(args.maxChars, 12000)) : 9000,
      maxBytes: Number.isSafeInteger(args.maxBytes) ? Math.max(1, Math.min(args.maxBytes, 512_000_000)) : 512_000_000
    }), "utf8").toString("base64");
    return `
const fs = await import("node:fs");
const path = await import("node:path");
const os = await import("node:os");
const crypto = await import("node:crypto");
const req = JSON.parse(Buffer.from("${payload}", "base64").toString("utf8"));
const emit = (value) => console.log(JSON.stringify(value));
function expandPath(value) {
  let text = String(value || "").trim();
  if (!text) throw new Error("empty path");
  if (text === "~" || text.startsWith("~/") || text.startsWith("~\\\\")) {
    text = path.join(os.homedir(), text.slice(2));
  }
  text = text
    .replace(/%([^%]+)%/g, (_, name) => process.env[name] || "")
    .replace(/\\$\\{([^}]+)\\}|\\$([A-Za-z_][A-Za-z0-9_]*)/g, (_, braced, plain) => process.env[braced || plain] || "");
  return path.resolve(text);
}
function itemInfo(fullPath, name = path.basename(fullPath)) {
  const stat = fs.statSync(fullPath);
  return {
    name,
    path: fullPath,
    type: stat.isDirectory() ? "directory" : "file",
    length: stat.isDirectory() ? 0 : stat.size,
    updated: stat.mtime.toISOString()
  };
}
function wildcardToRegExp(glob) {
  const escaped = String(glob || "*").replace(/[.+^$(){}|[\\]\\\\]/g, "\\\\$&").replace(/\\*/g, ".*").replace(/\\?/g, ".");
  return new RegExp("^" + escaped + "$", "i");
}
function cleanFileName(value) {
  return String(value || "file").replace(/[\\\\/:*?"<>|]/g, "_").slice(0, 120) || "file";
}
function mimeFromName(name) {
  const ext = path.extname(name).toLowerCase();
  return ({
    ".txt": "text/plain",
    ".json": "application/json",
    ".csv": "text/csv",
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif": "image/gif",
    ".zip": "application/zip",
    ".7z": "application/x-7z-compressed"
  })[ext] || "application/octet-stream";
}
function emitSotyFileControl(kind, value) {
  console.log("SOTY_FILE_" + kind + " " + Buffer.from(JSON.stringify(value), "utf8").toString("base64"));
}
function publishFile(fullPath) {
  const stat = fs.statSync(fullPath);
  if (!stat.isFile()) throw new Error("download path is not a file");
  const maxBytes = Math.max(1, Math.min(Number(req.maxBytes) || 512000000, 512000000));
  if (stat.size > maxBytes) throw new Error("file too large for Soty room file transfer");
  const name = cleanFileName(req.downloadName || path.basename(fullPath));
  const type = String(req.mimeType || mimeFromName(name)).slice(0, 160) || "application/octet-stream";
  const autoDownload = String(req.action || "").toLowerCase() === "download";
  const delivery = autoDownload ? "controller-browser-downloads" : "room-file-rail";
  const fileId = "file_" + (crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString("hex"));
  const chunkSize = 256000;
  const total = Math.max(1, Math.ceil(stat.size / chunkSize));
  const hash = crypto.createHash("sha256");
  const buffer = Buffer.allocUnsafe(chunkSize);
  const fd = fs.openSync(fullPath, "r");
  let index = 0;
  try {
    emitSotyFileControl("BEGIN", { id: fileId, name, type, size: stat.size, total, autoDownload, delivery });
    for (;;) {
      const bytesRead = fs.readSync(fd, buffer, 0, chunkSize, null);
      if (bytesRead <= 0) break;
      const chunk = buffer.subarray(0, bytesRead);
      hash.update(chunk);
      console.log("SOTY_FILE_CHUNK " + fileId + " " + index + " " + chunk.toString("base64"));
      index += 1;
    }
  } finally {
    fs.closeSync(fd);
  }
  const sha256 = hash.digest("hex");
  emitSotyFileControl("END", { id: fileId, sha256 });
  return {
    name,
    type,
    bytes: stat.size,
    chunks: total,
    sha256,
    delivery,
    autoDownload,
    controllerDownloads: autoDownload,
    controllerPath: autoDownload ? "browser-default-downloads" : ""
  };
}
function listFiles(root, recursive, limit, out = []) {
  if (out.length >= limit) return out;
  const stat = fs.statSync(root);
  if (!stat.isDirectory()) {
    out.push(root);
    return out;
  }
  for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
    if (out.length >= limit) break;
    const full = path.join(root, entry.name);
    if (entry.isDirectory()) {
      if (recursive) listFiles(full, true, limit, out);
    } else if (entry.isFile()) {
      out.push(full);
    }
  }
  return out;
}
let action = "";
let fullPath = "";
try {
  action = String(req.action || "").trim().toLowerCase();
  fullPath = expandPath(req.path);
  const maxResults = Math.max(1, Math.min(500, Number(req.maxResults) || 80));
  const maxChars = Math.max(1000, Math.min(12000, Number(req.maxChars) || 9000));
  if (action === "stat") {
    emit({ ok: true, action, ...itemInfo(fullPath) });
  } else if (action === "list") {
    const stat = fs.statSync(fullPath);
    const entries = stat.isDirectory()
      ? (req.recursive ? listFiles(fullPath, true, maxResults) : fs.readdirSync(fullPath).slice(0, maxResults).map((name) => path.join(fullPath, name)))
      : [fullPath];
    emit({ ok: true, action, path: fullPath, items: entries.map((entry) => itemInfo(entry)) });
  } else if (action === "read") {
    const text = fs.readFileSync(fullPath, "utf8").slice(0, maxChars);
    emit({ ok: true, action, path: fullPath, text });
  } else if (action === "write" || action === "append") {
    fs.mkdirSync(path.dirname(fullPath), { recursive: true });
    if (action === "write") fs.writeFileSync(fullPath, String(req.content || ""), "utf8");
    else fs.appendFileSync(fullPath, String(req.content || ""), "utf8");
    emit({ ok: true, action, path: fullPath, bytes: Buffer.byteLength(String(req.content || ""), "utf8") });
  } else if (action === "cycle") {
    fs.mkdirSync(path.dirname(fullPath), { recursive: true });
    const content = String(req.content || "");
    fs.writeFileSync(fullPath, content, "utf8");
    const text = fs.readFileSync(fullPath, "utf8").slice(0, maxChars);
    fs.rmSync(fullPath, { force: true });
    emit({ ok: true, action, path: fullPath, bytes: Buffer.byteLength(content, "utf8"), text, written: true, read: true, deleted: true });
  } else if (action === "mkdir") {
    fs.mkdirSync(fullPath, { recursive: true });
    emit({ ok: true, action, path: fullPath });
  } else if (action === "move" || action === "copy") {
    const toPath = expandPath(req.toPath);
    fs.mkdirSync(path.dirname(toPath), { recursive: true });
    if (action === "move") fs.renameSync(fullPath, toPath);
    else fs.cpSync(fullPath, toPath, { recursive: req.recursive === true, force: true });
    emit({ ok: true, action, path: fullPath, toPath });
  } else if (action === "delete") {
    fs.rmSync(fullPath, { recursive: req.recursive === true, force: true });
    emit({ ok: true, action, path: fullPath });
  } else if (action === "download" || action === "publish") {
    const published = publishFile(fullPath);
    emit({ ok: true, action, path: fullPath, sentTo: published.delivery, ...published });
  } else if (action === "search") {
    const pattern = String(req.pattern || "");
    if (!pattern.trim()) throw new Error("empty pattern");
    const glob = wildcardToRegExp(req.glob || "*");
    const matcher = req.regex ? new RegExp(pattern, "iu") : null;
    const files = listFiles(fullPath, true, maxResults * 20).filter((file) => glob.test(path.basename(file)));
    const matches = [];
    for (const file of files) {
      if (matches.length >= maxResults) break;
      let text = "";
      try { text = fs.readFileSync(file, "utf8"); } catch { continue; }
      const lines = text.split(/\\r?\\n/u);
      for (let index = 0; index < lines.length && matches.length < maxResults; index += 1) {
        const line = lines[index];
        if (matcher ? matcher.test(line) : line.includes(pattern)) {
          matches.push({ path: file, line: index + 1, text: line.trim().slice(0, 1000) });
        }
      }
    }
    emit({ ok: true, action, path: fullPath, pattern, matches });
  } else {
    throw new Error("unsupported file action: " + action);
  }
} catch (error) {
  emit({ ok: false, action, path: fullPath, error: error && error.message ? error.message : String(error) });
  process.exit(1);
}
`.trim();
  }

  function sourceWebScript(args) {
    const payload = Buffer.from(JSON.stringify({
      action: String(args.action || args.operation || "").slice(0, 40),
      url: String(args.url || "").slice(0, 4000),
      query: String(args.query || args.text || args.pattern || "").slice(0, 500),
      maxChars: Number.isSafeInteger(args.maxChars) ? Math.max(1000, Math.min(args.maxChars, 12000)) : 9000,
      timeoutMs: Number.isSafeInteger(args.timeoutMs) ? Math.max(1000, Math.min(args.timeoutMs, 120000)) : 30000
    }), "utf8").toString("base64");
    return `
const req = JSON.parse(Buffer.from("${payload}", "base64").toString("utf8"));
const emit = (value) => console.log(JSON.stringify(value));
const maxChars = Math.max(1000, Math.min(Number(req.maxChars) || 9000, 12000));
const timeoutMs = Math.max(1000, Math.min(Number(req.timeoutMs) || 30000, 120000));
const userAgent = "Mozilla/5.0 (compatible; SotyAgent/1.0; +https://xn--n1afe0b.online)";
function decodeEntities(value) {
  return String(value || "")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#(\\d+);/g, (_, code) => String.fromCodePoint(Number(code) || 32));
}
function stripHtml(value) {
  return decodeEntities(String(value || "")
    .replace(/<script[\\s\\S]*?<\\/script>/gi, " ")
    .replace(/<style[\\s\\S]*?<\\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\\s+/g, " ")
    .trim());
}
function titleFromHtml(value) {
  const match = String(value || "").match(/<title[^>]*>([\\s\\S]*?)<\\/title>/i);
  return match ? stripHtml(match[1]).slice(0, 240) : "";
}
function absoluteHttpUrl(value, base) {
  const url = new URL(String(value || ""), base);
  if (!/^https?:$/i.test(url.protocol)) throw new Error("unsupported url protocol");
  return url.toString();
}
async function getText(url) {
  const res = await fetch(url, {
    headers: { "user-agent": userAgent, accept: "text/html,application/xhtml+xml,application/xml,text/plain;q=0.9,*/*;q=0.8" },
    signal: AbortSignal.timeout(timeoutMs)
  });
  const text = await res.text();
  return { res, text };
}
function duckDuckGoResults(html, baseUrl) {
  const results = [];
  const re = /<a[^>]+class=["'][^"']*result__a[^"']*["'][^>]+href=["']([^"']+)["'][^>]*>([\\s\\S]*?)<\\/a>/gi;
  let match;
  while ((match = re.exec(html)) && results.length < 8) {
    let href = decodeEntities(match[1]);
    try {
      const parsed = new URL(href, baseUrl);
      href = parsed.hostname.includes("duckduckgo.com") && parsed.searchParams.get("uddg")
        ? parsed.searchParams.get("uddg")
        : parsed.toString();
    } catch {}
    results.push({ title: stripHtml(match[2]).slice(0, 180), url: href });
  }
  return results;
}
const action = String(req.action || "").toLowerCase();
if (action === "search" || action === "web-search" || action === "web_search" || (!req.url && req.query)) {
  const query = String(req.query || "").trim();
  if (!query) throw new Error("empty query");
  const searchUrl = "https://duckduckgo.com/html/?q=" + encodeURIComponent(query);
  const { res, text } = await getText(searchUrl);
  emit({
    ok: true,
    action: "search",
    query,
    status: res.status,
    url: searchUrl,
    results: duckDuckGoResults(text, searchUrl),
    text: stripHtml(text).slice(0, maxChars)
  });
} else {
  const url = absoluteHttpUrl(req.url, "https://example.com/");
  const { res, text } = await getText(url);
  const type = String(res.headers.get("content-type") || "");
  const body = type.includes("html") ? stripHtml(text) : text.replace(/\\s+/g, " ").trim();
  emit({
    ok: res.ok,
    action: "fetch",
    url: res.url || url,
    status: res.status,
    contentType: type,
    title: type.includes("html") ? titleFromHtml(text) : "",
    text: body.slice(0, maxChars)
  });
  if (!res.ok) process.exitCode = 1;
}
`.trim();
  }

  return Object.freeze({
    sourceOpenUrlScript,
    sourceFileScript,
    sourceWebScript
  });
}
