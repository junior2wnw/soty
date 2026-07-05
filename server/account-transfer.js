import express from "express";
import { existsSync } from "node:fs";
import { chmod, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";

const jsonParser = express.json({ limit: process.env.SOTY_ACCOUNT_TRANSFER_JSON_LIMIT || "8mb", type: "application/json" });
const lookupPattern = /^[A-Za-z0-9_-]{32,96}$/u;
const base64UrlPattern = /^[A-Za-z0-9_-]+$/u;
const maxCiphertextChars = 8_000_000;

export function attachAccountTransfer(app, { dataDir } = {}) {
  const root = path.join(dataDir || process.cwd(), "account-transfer");

  app.put("/api/account-transfer/:lookup", jsonParser, async (req, res) => {
    const lookup = cleanLookup(req.params.lookup);
    const envelope = cleanEnvelope(req.body);
    if (!lookup || !envelope) {
      res.status(400).json({ ok: false, error: "bad_account_transfer" });
      return;
    }
    try {
      await mkdir(root, { recursive: true, mode: 0o700 });
      const file = transferFile(root, lookup);
      const tmp = `${file}.${process.pid}.${Date.now()}.tmp`;
      await writeFile(tmp, `${JSON.stringify(envelope)}\n`, { encoding: "utf8", mode: 0o600 });
      await rename(tmp, file);
      await chmod(file, 0o600).catch(() => undefined);
      res.json({ ok: true });
    } catch {
      res.status(500).json({ ok: false, error: "account_transfer_write_failed" });
    }
  });

  app.get("/api/account-transfer/:lookup", async (req, res) => {
    const lookup = cleanLookup(req.params.lookup);
    if (!lookup) {
      res.status(400).json({ ok: false, error: "bad_account_transfer" });
      return;
    }
    try {
      const file = transferFile(root, lookup);
      if (!existsSync(file)) {
        res.status(404).json({ ok: false, error: "account_transfer_not_found" });
        return;
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
    }
  });
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
