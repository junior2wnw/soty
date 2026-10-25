#!/usr/bin/env node
import { existsSync, lstatSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync } from 'node:fs';
import { resolve, join, posix } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { PUBLIC_SKILL_FILES } from '../deploy/ui/release.mjs';

const prefix = 'skills/soty-app-deploy/';
export const deploySkillFiles = Object.freeze(PUBLIC_SKILL_FILES.filter(name => name.startsWith(prefix)));
const fail = code => { throw Object.assign(new Error(code), { code }); };
function regularPath(root, relative, create = false) {
  const parts = relative.split('/'); let parent = root;
  for (const part of parts.slice(0, -1)) {
    parent = join(parent, part);
    if (!existsSync(parent)) { if (!create) fail('public_skill_missing'); mkdirSync(parent); }
    const info = lstatSync(parent); if (info.isSymbolicLink() || !info.isDirectory()) fail('public_skill_symlink');
  }
  const file = join(parent, parts.at(-1));
  if (existsSync(file)) { const info = lstatSync(file); if (info.isSymbolicLink() || !info.isFile()) fail('public_skill_symlink'); }
  return file;
}
function linksArePackaged(source, name) {
  for (const match of source.toString('utf8').matchAll(/\]\(([^\s)]+)\)/gu)) {
    const target = match[1].split('#')[0];
    if (!target || /^https?:\/\//u.test(target)) continue;
    if (target.startsWith('/') || /^[a-z][a-z0-9+.-]*:/iu.test(target)
      || !deploySkillFiles.includes(posix.normalize(posix.join(posix.dirname(name), target)))) fail('public_skill_reference_missing');
  }
}
export function syncPublicDeploySkill(repository = fileURLToPath(new URL('../', import.meta.url)), { check = false } = {}) {
  const root = resolve(repository);
  if (realpathSync(root) !== root) fail('public_skill_root_symlink');
  const entries = deploySkillFiles.map(name => {
    const file = regularPath(root, name);
    if (!existsSync(file)) fail('public_skill_missing');
    const bytes = readFileSync(file); linksArePackaged(bytes, name); return { name, bytes };
  });
  const publicRoot = join(root, 'public', 'skills', 'soty-app-deploy');
  if (existsSync(publicRoot)) {
    regularPath(root, 'public/skills/soty-app-deploy/SKILL.md');
    const visit = directory => {
      for (const entry of readdirSync(directory, { withFileTypes: true })) {
        const file = join(directory, entry.name);
        if (entry.isSymbolicLink()) fail('public_skill_symlink');
        if (entry.isDirectory()) visit(file);
        else {
          const name = file.slice(join(root, 'public').length + 1).replaceAll('\\', '/');
          if (!deploySkillFiles.includes(name)) fail('public_skill_unexpected_file');
        }
      }
    }; visit(publicRoot);
  }
  for (const { name, bytes } of entries) {
    const target = regularPath(root, 'public/' + name, !check);
    if (check) {
      if (!existsSync(target) || !readFileSync(target).equals(bytes)) fail('public_skill_out_of_sync');
    } else writeFileSync(target, bytes);
  }
  return { ok: true, mode: check ? 'check' : 'sync', skill: 'soty-app-deploy', files: entries.length };
}
if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  try {
    const args = process.argv.slice(2);
    if (args.length > 1 || args.length === 1 && args[0] !== '--check') fail('public_skill_arguments');
    process.stdout.write(JSON.stringify(syncPublicDeploySkill(undefined, { check: args[0] === '--check' })) + '\n');
  } catch (error) {
    process.stdout.write(JSON.stringify({ ok: false, code: /^public_skill_[a-z_]+$/u.test(error.code ?? '') ? error.code : 'public_skill_failed' }) + '\n');
    process.exitCode = 1;
  }
}
