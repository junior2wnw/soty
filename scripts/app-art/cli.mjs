#!/usr/bin/env node
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { REPO_ROOT, prepareArtwork, importArtwork, bindArtwork, aliasFieldArtwork, configureFieldIcon, rollbackArtwork, validateArtwork, validatePublicArtwork, listArtwork, queueArtwork, migrateArtworkHistory, readJson, definitionsFromCatalog } from './pipeline.mjs';

const HELP = `Soty application artwork
  prepare --all [--refresh]
  prepare --key notes [--app-id app-<32 hex>] [--prompt-file prompt.txt] [--reference-image output/app-art-history/.../source....png]
  prepare --definition app-art.json
  prepare --catalog app-catalog.json
  import --key notes --source C:\\path\\image.png [--job scripts/app-art/jobs/notes/job-....json] --visual-approved [--reviewer codex] [--generation-id id]
  bind --app-id app-<32 hex> --key cover-key [--profile field]
  profile --key field-hive --cover-key hive
  icon --key field-hive --icon cells
  rollback --key notes --version 1
  validate [--public]
  list
  queue [--all]
  migrate-history

Generation runs through Codex built-in image_gen using prepared job.prompt.
This local CLI prepares, versions, validates, imports and binds images; it does not call an image API.
`;
const FLAG_SETS = {
  prepare: ['root', 'all', 'refresh', 'key', 'definition', 'catalog', 'prompt-file', 'app-id', 'reference-image'],
  import: ['root', 'key', 'source', 'job', 'visual-approved', 'reviewer', 'generation-id'],
  bind: ['root', 'app-id', 'key', 'profile'], profile: ['root','cover-key','key'], icon: ['root','key','icon'], rollback: ['root', 'key', 'version'], validate: ['root', 'public'], list: ['root'], queue: ['root', 'all'], 'migrate-history': ['root'],
};
const BOOLEANS = new Set(['all', 'refresh', 'visual-approved', 'public']);
function flags(args, command) {
  const result = {};
  for (let i = 0; i < args.length; i++) {
    const token = args[i];
    if (!token.startsWith('--') || !FLAG_SETS[command]?.includes(token.slice(2))) throw new Error('Unknown artwork argument; run --help');
    const key = token.slice(2);
    if (Object.hasOwn(result, key)) throw new Error(`Duplicate --${key}`);
    if (BOOLEANS.has(key)) result[key] = true;
    else { const value = args[++i]; if (!value || value.startsWith('--')) throw new Error(`Missing value for --${key}`); result[key] = value; }
  }
  return result;
}

try {
  const [command = 'help', ...args] = process.argv.slice(2);
  if (command === 'help' || command === '--help' || command === '-h') { process.stdout.write(HELP); }
  else {
    if (!FLAG_SETS[command]) throw new Error('Unknown artwork command; run --help');
    const options = flags(args, command), root = resolve(options.root || REPO_ROOT);
    let result;
    if (command === 'prepare') {
      if (options.definition && options.catalog) throw new Error('Choose one definition or catalog input');
      const definitions = options.definition ? [await readJson(resolve(root, options.definition))] : options.catalog ? definitionsFromCatalog(await readJson(resolve(root, options.catalog))) : [];
      const prompt = options['prompt-file'] ? await readFile(resolve(root, options['prompt-file']), 'utf8') : undefined;
      result = await prepareArtwork(root, { definitions, keys: options.key ? [options.key] : [], all: !!options.all, refresh: !!options.refresh, ...(prompt !== undefined ? { prompt } : {}), ...(options['app-id'] ? { appId: options['app-id'] } : {}), ...(options['reference-image'] ? { referenceImage: options['reference-image'] } : {}) });
    } else if (command === 'import') {
      result = await importArtwork(root, { key: options.key, source: options.source, visualApproved: !!options['visual-approved'], reviewer: options.reviewer || 'local-review', ...(options.job ? { job: options.job } : {}), ...(options['generation-id'] ? { generationId: options['generation-id'] } : {}) });
    } else if (command === 'bind') result = await bindArtwork(root, { key: options.key, appId: options['app-id'], ...(options.profile ? {profile:options.profile} : {}) });
    else if (command === 'profile') result = await aliasFieldArtwork(root,{key:options.key,coverKey:options['cover-key']});
    else if (command === 'icon') result = await configureFieldIcon(root,{key:options.key,icon:options.icon});
    else if (command === 'rollback') result = await rollbackArtwork(root, { key: options.key, version: Number(options.version) });
    else if (command === 'validate') result = options.public ? await validatePublicArtwork(root) : await validateArtwork(root);
    else if (command === 'queue') result = await queueArtwork(root, { includeAccepted: !!options.all });
    else if (command === 'migrate-history') result = await migrateArtworkHistory(root);
    else result = await listArtwork(root);
    process.stdout.write(JSON.stringify(result, null, 2) + '\n');
  }
} catch (error) {
  // No env/config dump or raw catalog data; print only the bounded diagnostic.
  process.stderr.write(`app-art: ${String(error.message).slice(0, 500)}\n`);
  process.exitCode = 1;
}
