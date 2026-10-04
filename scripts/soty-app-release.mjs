#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { initAppManifest, readAppManifest, readReleaseJson, createReleasePlan, writeReleasePlan, verifyRelease, namedZoneIngress } from '../deploy/apps/release.mjs';
import { AppDeploymentError } from '../src/world/app-deployment.mjs';

// No config dump, arbitrary shell execution, credentials or automatic publication.
const usage = 'init --project PATH --name NAME --port N [--entry-path /]\n'
  + 'plan --project PATH --deployment FILE --output NEW_DIR [--mode isolated|native] [--domain-id ID] [--gateway-port N] [--frame-origin HTTPS_ORIGIN ...]\n'
  + 'verify --plan FILE [--probes FILE] --output NEW_FILE\n'
  + 'zone --origin HTTPS_ZONE [--gateway-port N] --output NEW_FILE\n';
export async function main(argv) {
  const action = argv[0];
  if (action === 'help' || !action) { process.stdout.write(usage); return; }
  const allowed = { init: ['project', 'name', 'port', 'entry-path'], plan: ['project', 'deployment', 'output', 'mode', 'domain-id', 'gateway-port', 'frame-origin'], verify: ['plan', 'probes', 'output'], zone: ['origin', 'gateway-port', 'output'] };
  if (!allowed[action]) throw new AppDeploymentError('invalid_release_command');
  const args = {}, frameOrigins = [];
  for (let i = 1; i < argv.length; i += 2) {
    const key = argv[i]?.slice(2), value = argv[i + 1];
    if (!argv[i]?.startsWith('--') || !allowed[action].includes(key) || !value || (key !== 'frame-origin' && Object.hasOwn(args, key))) throw new AppDeploymentError('invalid_release_arguments');
    if (key === 'frame-origin') frameOrigins.push(value); else args[key] = value;
  }
  const required = { init: ['project', 'name', 'port'], plan: ['project', 'deployment', 'output'], verify: ['plan', 'output'], zone: ['origin', 'output'] };
  if (required[action].some(key => !args[key])) throw new AppDeploymentError('missing_release_argument');
  let result;
  if (action === 'init') {
    result = await initAppManifest({ project: args.project, name: args.name, port: Number(args.port), entryPath: args['entry-path'] ?? '/' });
    result = { ok: true, file: result.file, unchanged: result.unchanged };
  } else if (action === 'plan') {
    const plan = createReleasePlan({ manifest: await readAppManifest(args.project), deployment: await readReleaseJson(args.deployment),
      domainId: args['domain-id'], mode: args.mode ?? 'isolated', gatewayPort: Number(args['gateway-port'] ?? 18182), frameOrigins });
    const written = await writeReleasePlan(args.output, plan);
    result = { ok: true, file: written.file, mode: plan.mode, origin: plan.origin, shellLaunchUrl: plan.shellLaunchUrl, applied: false };
  } else if (action === 'zone') {
    const text = namedZoneIngress({ origin: args.origin, gatewayPort: Number(args['gateway-port'] ?? 18182) });
    await writeFile(resolve(args.output), text, { flag: 'wx', mode: 0o600 });
    result = { ok: true, file: resolve(args.output), applied: false, registryTlsPermissionRequired: true };
  } else {
    const plan = await readReleaseJson(args.plan), probes = args.probes ? await readReleaseJson(args.probes) : [];
    if (!Array.isArray(probes)) throw new AppDeploymentError('invalid_release_probes');
    result = await verifyRelease(plan, { probes });
    await writeFile(resolve(args.output), JSON.stringify(result, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
    if (!result.ok) process.exitCode = 1;
  }
  process.stdout.write(JSON.stringify(result) + '\n');
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try { await main(process.argv.slice(2)); }
  catch (error) {
    // Only explicit codes from our validators; OS/network errors may contain secrets.
    const filesystemCodes = { EEXIST: 'release_output_exists', ENOENT: 'release_file_missing', EACCES: 'release_file_denied' };
    process.stdout.write(JSON.stringify({ ok: false, code: error instanceof AppDeploymentError ? error.code : filesystemCodes[error.code] ?? 'app_release_failed' }) + '\n');
    process.exitCode = 1;
  }
}
