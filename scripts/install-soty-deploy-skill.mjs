#!/usr/bin/env node
import { readdir, mkdir, readFile, writeFile, realpath, open, unlink, rename } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, basename, join, relative, resolve, isAbsolute } from 'node:path';
import { createHash, randomBytes } from 'node:crypto';
import { fileURLToPath, pathToFileURL } from 'node:url';

const source = fileURLToPath(new URL('../skills/soty-app-deploy/', import.meta.url));
const hash = value => createHash('sha256').update(value).digest('hex');
const fail = code => { throw Object.assign(new Error(code), { code }); };
const inside = (root, path) => { const part = relative(root, path); return !isAbsolute(part) && part !== '..' && !part.startsWith('..' + (process.platform === 'win32' ? '\\' : '/')); };
async function existsFile(path) { try { return await readFile(path); } catch (error) { if (error.code !== 'ENOENT') throw error; return null; } }
async function bundle(root, directory = root) {
  const result = {};
  for (const item of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, item.name);
    if (item.isSymbolicLink()) fail('skill_source_symlink');
    if (item.isDirectory()) Object.assign(result, await bundle(root, path));
    else if (item.isFile()) result[relative(root, path).replaceAll('\\', '/')] = await readFile(path);
  }
  return result;
}
export async function installSotyDeploySkill(target) {
  const root = resolve(target);
  if (basename(root) !== 'soty-app-deploy') fail('skill_target_name_required');
  await mkdir(root, { recursive: true });
  if (await realpath(root) !== root) fail('skill_target_symlink');
  const files = await bundle(source), digests = Object.fromEntries(Object.entries(files).map(([name, bytes]) => [name, hash(bytes)]));
  const lockPath = join(root, '.install.lock'); const lock = await open(lockPath, 'wx', 0o600);
  try {
    const receiptPath = join(root, 'agents', 'install-receipt.json'), bytes = await existsFile(receiptPath);
    let prior = null;
    if (bytes) {
      try { prior = JSON.parse(bytes); } catch { fail('skill_receipt_invalid'); }
      if (prior.schema !== 'soty.skill-install.v1' || !prior.files || typeof prior.files !== 'object') fail('skill_receipt_invalid');
    }
    for (const [name, value] of Object.entries(files)) {
      const destination = join(root, name), existing = await existsFile(destination);
      try { if (!inside(root, await realpath(destination))) fail('skill_target_symlink'); }
      catch (error) { if (error.code !== 'ENOENT') throw error; }
      if (existing && hash(existing) !== hash(value) && (!prior || prior.files[name] !== hash(existing))) fail('skill_local_changes');
    }
    for (const [name, value] of Object.entries(files)) {
      const destination = join(root, name); await mkdir(dirname(destination), { recursive: true });
      if (!inside(root, await realpath(dirname(destination)))) fail('skill_target_symlink');
      const temporary = destination + '.next-' + randomBytes(6).toString('hex');
      await writeFile(temporary, value, { flag: 'wx', mode: 0o600 }); await rename(temporary, destination);
    }
    const receipt = { schema: 'soty.skill-install.v1', installedAt: Date.now(),
      sourceRepository: fileURLToPath(new URL('../', import.meta.url)), files: digests };
    const next = receiptPath + '.next-' + randomBytes(6).toString('hex');
    await writeFile(next, JSON.stringify(receipt, null, 2) + '\n', { flag: 'wx', mode: 0o600 }); await rename(next, receiptPath);
    return { ok: true, skill: 'soty-app-deploy', root, files: Object.keys(files).length };
  } finally { await lock.close(); await unlink(lockPath); }
}
if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  try {
    const args = process.argv.slice(2);
    if (args.length && (args.length !== 2 || args[0] !== '--target')) fail('skill_install_arguments');
    const target = args[1] ?? join(process.env.CODEX_HOME || join(homedir(), '.codex'), 'skills', 'soty-app-deploy');
    process.stdout.write(JSON.stringify(await installSotyDeploySkill(target)) + '\n');
  } catch (error) {
    process.stdout.write(JSON.stringify({ ok: false, code: /^skill_[a-z_]+$/u.test(error.code || '') ? error.code : 'skill_install_failed' }) + '\n');
    process.exitCode = 1;
  }
}
