import { readFile, writeFile } from "node:fs/promises";

export function unixInstallerBytes(source) {
  // latin1 preserves every byte, including the existing UTF-8 script text.
  const next = Buffer.from(source.toString("latin1").replace(/\r\n/g, "\n"), "latin1");
  if (next.includes(13)) throw new Error("unix-installer-bare-cr");
  return next;
}

export async function publishUnixInstaller(file) {
  const source = await readFile(file), next = unixInstallerBytes(source);
  if (!next.equals(source)) await writeFile(file, next);
}
