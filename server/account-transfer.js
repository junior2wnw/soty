import express from "express";
import { createHash, randomUUID } from "node:crypto";
import { chmod, lstat, mkdir, open, opendir, readFile, rename, rm, statfs } from "node:fs/promises";
import path from "node:path";

const jsonParser = express.json({ limit: process.env.SOTY_ACCOUNT_TRANSFER_JSON_LIMIT || "8mb", type: "application/json" });
const lookupPattern = /^[A-Za-z0-9_-]{32,96}$/u;
const base64UrlPattern = /^[A-Za-z0-9_-]+$/u;
const maxCiphertextChars = 8_000_000;
const defaults = Object.freeze({ maxFiles: 10_000, maxTotalBytes: 1024 * 1024 * 1024, minFreeBytes: 64 * 1024 * 1024,
  maxConcurrentWrites: 4, maxConcurrentReads: 8, peerWritesPerHour: 12, globalWritesPerMinute: 120,
  peerReadsPerMinute: 60, globalReadsPerMinute: 1200, maxRateEntries: 10_000 });

export function attachAccountTransfer(app, { dataDir, transferLimits = {}, clock = Date.now } = {}) {
  const root = path.join(dataDir || process.cwd(), "account-transfer");
  const configured = {};
  for (const [key, name] of [['maxFiles', 'SOTY_ACCOUNT_TRANSFER_MAX_FILES'], ['maxTotalBytes', 'SOTY_ACCOUNT_TRANSFER_MAX_BYTES'], ['minFreeBytes', 'SOTY_ACCOUNT_TRANSFER_MIN_FREE_BYTES']]) {
    if (process.env[name] !== undefined) configured[key] = Number(process.env[name]);
  }
  const limits = { ...defaults, ...configured, ...transferLimits };
  for (const [key, value] of Object.entries(limits)) {
    if (!Object.hasOwn(defaults, key) || !Number.isSafeInteger(value) || value < (key === 'minFreeBytes' ? 0 : 1)) throw new Error('invalid_account_transfer_limits');
  }
  const rates = new Map();
  const active = { read: 0, write: 0 };
  let writes = Promise.resolve();

  function admit(kind) {
    return (req, res, next) => {
      res.setHeader('Cache-Control', 'no-store');
      const now = clock();
      for (const [key, value] of rates) if (value.expiresAt <= now) rates.delete(key);
      const peer = createHash('sha256').update(req.ip || req.socket?.remoteAddress || 'unknown').digest('hex');
      const write = kind === 'write';
      const budgets = [
        [`${kind}:global`, write ? limits.globalWritesPerMinute : limits.globalReadsPerMinute, 60_000],
        [`${kind}:${peer}`, write ? limits.peerWritesPerHour : limits.peerReadsPerMinute, write ? 3_600_000 : 60_000]
      ];
      const exhausted = budgets.filter(([key, maximum]) => (rates.get(key)?.count || 0) >= maximum);
      if (exhausted.length > 0
        || rates.size + budgets.filter(([key]) => !rates.has(key)).length > limits.maxRateEntries) {
        res.setHeader('Retry-After', String(Math.ceil(Math.max(60_000, ...exhausted.map(([key]) => rates.get(key).expiresAt - now)) / 1000)));
        res.status(429).json({ ok: false, error: 'account_transfer_rate_limited' }); return;
      }
      for (const [key, , interval] of budgets) {
        const value = rates.get(key) || { count: 0, expiresAt: now + interval };
        value.count++; rates.set(key, value);
      }
      if (active[kind] >= (write ? limits.maxConcurrentWrites : limits.maxConcurrentReads)) {
        res.setHeader('Retry-After', '5');
        res.status(503).json({ ok: false, error: 'account_transfer_busy' }); return;
      }
      active[kind]++;
      let released = false;
      const release = () => { if (!released) { released = true; active[kind]--; } };
      res.locals.accountTransferDone = release;
      const releaseIdle = () => { if (!res.locals.accountTransferRunning) release(); };
      res.once('finish', releaseIdle); res.once('close', releaseIdle);
      next();
    };
  }

  const parseJson = (req, res, next) => jsonParser(req, res, error => {
    if (!error) return next();
    res.status(error.type === 'entity.too.large' ? 413 : 400).json({ ok: false, error: 'bad_account_transfer' });
  });

  app.put("/api/account-transfer/:lookup", admit('write'), parseJson, async (req, res) => {
    const lookup = cleanLookup(req.params.lookup);
    const envelope = cleanEnvelope(req.body);
    if (!lookup || !envelope) {
      res.status(400).json({ ok: false, error: "bad_account_transfer" });
      return;
    }
    res.locals.accountTransferRunning = true;
    try {
      // Serialize quota observation and atomic replacement. The host runs one
      // application process per data volume; a restart recounts the real files.
      const run = writes.then(() => storeEnvelope(root, lookup, envelope, limits, clock()));
      writes = run.catch(() => undefined);
      await run;
      res.json({ ok: true });
    } catch (error) {
      const full = error.code === 'account_transfer_storage_full' || error.code === 'ENOSPC';
      res.status(full ? 507 : 500).json({ ok: false, error: full ? 'account_transfer_storage_full' : 'account_transfer_write_failed' });
    } finally { res.locals.accountTransferDone(); }
  });

  app.get("/api/account-transfer/:lookup", admit('read'), async (req, res) => {
    const lookup = cleanLookup(req.params.lookup);
    if (!lookup) {
      res.status(400).json({ ok: false, error: "bad_account_transfer" });
      return;
    }
    res.locals.accountTransferRunning = true;
    try {
      const file = transferFile(root, lookup);
      const info = await fileInfo(file);
      if (!info) {
        res.status(404).json({ ok: false, error: "account_transfer_not_found" });
        return;
      }
      if (!info.isFile() || info.size > maxCiphertextChars + 4096) {
        res.status(410).json({ ok: false, error: 'account_transfer_invalid' }); return;
      }
      const parsed = JSON.parse(await readFile(file, "utf8"));
      const envelope = cleanEnvelope(parsed);
      if (!envelope) {
        res.status(410).json({ ok: false, error: "account_transfer_invalid" });
        return;
      }
      res.setHeader("Cache-Control", "no-store");
      res.json(envelope);
    } catch {
      res.status(500).json({ ok: false, error: "account_transfer_read_failed" });
    } finally { res.locals.accountTransferDone(); }
  });
}

async function fileInfo(file) {
  try { return await lstat(file); }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
}

async function storeEnvelope(root, lookup, envelope, limits, now) {
  await mkdir(root, { recursive: true, mode: 0o700 });
  const file = transferFile(root, lookup);
  const previous = await fileInfo(file);
  if (previous && !previous.isFile()) throw new Error('account_transfer_invalid_file');
  const contents = `${JSON.stringify(envelope)}\n`, bytes = Buffer.byteLength(contents);
  let storedBytes = 0, storedFiles = 0;
  for await (const entry of await opendir(root)) {
    if (!entry.isFile()) continue;
    if (/^[A-Za-z0-9_-]{32,96}\.json$/u.test(entry.name)) {
      const info = await fileInfo(path.join(root, entry.name));
      if (info?.isFile()) { storedFiles++; storedBytes += info.size; }
    } else if (/^[A-Za-z0-9_-]{32,96}\.json\.[A-Za-z0-9_.-]+\.tmp$/u.test(entry.name)) {
      const temporary = path.join(root, entry.name), info = await fileInfo(temporary);
      // Only abandoned staging files expire. A valid recovery envelope has no TTL.
      if (info?.isFile() && info.mtimeMs < now - 24 * 3_600_000) await rm(temporary, { force: true });
    }
  }
  const growth = bytes - (previous?.size || 0);
  if ((!previous && storedFiles >= limits.maxFiles) || (growth > 0 && storedBytes + growth > limits.maxTotalBytes)) {
    throw Object.assign(new Error('account_transfer_storage_full'), { code: 'account_transfer_storage_full' });
  }
  const disk = await statfs(root);
  if (disk.bavail * disk.bsize < limits.minFreeBytes + bytes) {
    throw Object.assign(new Error('account_transfer_storage_full'), { code: 'account_transfer_storage_full' });
  }
  const tmp = `${file}.${randomUUID()}.tmp`;
  try {
    const handle = await open(tmp, 'wx', 0o600);
    try { await handle.writeFile(contents, 'utf8'); await handle.sync(); }
    finally { await handle.close(); }
    await rename(tmp, file);
    await chmod(file, 0o600).catch(() => undefined);
    if (process.platform !== 'win32') {
      const directory = await open(root, 'r');
      try { await directory.sync(); } finally { await directory.close(); }
    }
  } finally { await rm(tmp, { force: true }).catch(() => undefined); }
}

function cleanLookup(value) {
  const text = typeof value === "string" ? value : "";
  return lookupPattern.test(text) ? text : "";
}

function transferFile(root, lookup) {
  return path.join(root, `${lookup}.json`);
}

function cleanEnvelope(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const kdf = value.kdf && typeof value.kdf === "object" && !Array.isArray(value.kdf) ? value.kdf : null;
  const cipher = value.cipher && typeof value.cipher === "object" && !Array.isArray(value.cipher) ? value.cipher : null;
  if (
    value.schema !== "soty.account-phrase-backup.v1"
    || !kdf
    || !cipher
    || kdf.name !== "PBKDF2"
    || kdf.hash !== "SHA-256"
    || !isSafeIterations(kdf.iterations)
    || !isBase64Url(kdf.salt, 16, 128)
    || cipher.name !== "AES-GCM"
    || !isBase64Url(cipher.nonce, 12, 64)
    || !isBase64Url(cipher.ciphertext, 1, maxCiphertextChars)
  ) {
    return null;
  }
  return {
    schema: "soty.account-phrase-backup.v1",
    createdAt: typeof value.createdAt === "string" ? value.createdAt.slice(0, 80) : new Date().toISOString(),
    kdf: {
      name: "PBKDF2",
      hash: "SHA-256",
      iterations: Math.trunc(kdf.iterations),
      salt: kdf.salt
    },
    cipher: {
      name: "AES-GCM",
      nonce: cipher.nonce,
      ciphertext: cipher.ciphertext
    }
  };
}

function isSafeIterations(value) {
  return Number.isSafeInteger(value) && value >= 100_000 && value <= 1_000_000;
}

function isBase64Url(value, min, max) {
  return typeof value === "string"
    && value.length >= min
    && value.length <= max
    && base64UrlPattern.test(value);
}
