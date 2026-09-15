#!/usr/bin/env node
import { createPrivateKey, sign } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { canonicalSpreadExRelease, spreadExMlReleaseSchema } from "./agent-modules/spreadex-ml.mjs";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const sourcePath = join(root, "release", "spreadex-ml.unsigned.json");
const outputPath = join(root, "release", "spreadex-ml.json");
const privateKeyText = String(process.env.SOTY_SPREADEX_ML_RELEASE_PRIVATE_KEY || "").trim();
if (!privateKeyText) throw new Error("SOTY_SPREADEX_ML_RELEASE_PRIVATE_KEY is required");

const release = JSON.parse(await readFile(sourcePath, "utf8"));
if (release?.schema !== spreadExMlReleaseSchema || release.available !== true) throw new Error("Invalid unsigned SpreadEx ML release");
delete release.signature;
const key = privateKeyText.includes("BEGIN PRIVATE KEY")
  ? createPrivateKey(privateKeyText.replace(/\\n/gu, "\n"))
  : createPrivateKey({ key: Buffer.from(privateKeyText, "base64"), format: "der", type: "pkcs8" });
const signature = sign(null, Buffer.from(canonicalSpreadExRelease(release), "utf8"), key).toString("base64url");
await mkdir(dirname(outputPath), { recursive: true });
await writeFile(outputPath, `${JSON.stringify({ ...release, signature }, null, 2)}\n`, { mode: 0o644 });
process.stdout.write(`spreadex-ml:${release.version}:${release.modelVersion}:${signature.slice(0, 12)}\n`);
