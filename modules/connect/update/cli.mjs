import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { createRelease, fetchRelease, applyRelease, runValidation } from './index.mjs';

const [command, configPath] = process.argv.slice(2);
function failure(error) {
  const code = typeof error?.code === 'string' && /^[A-Za-z0-9_-]{1,100}$/.test(error.code) ? error.code : 'update_failed';
  return { status: error?.interventionRequired ? 'intervention_required' : 'failed', error: code,
    ...(error?.interventionRequired ? { interventionRequired: true, recovery: error.recovery } : {}) };
}
try {
  if (!['pack', 'sync', 'watch'].includes(command) || !configPath) throw new Error('Usage: node update/cli.mjs pack|sync|watch <config.json>');
  const absolute = path.resolve(configPath);
  const config = JSON.parse(await readFile(absolute, 'utf8'));
  const base = path.dirname(absolute);
  const resolve = p => path.resolve(base, p);
  if (command === 'pack') {
    const privateKey = await readFile(resolve(config.signingKeyFile), 'utf8');
    const release = await createRelease({ directory: resolve(config.directory), privateKey, keyId: config.keyId,
      sequence: config.sequence, expiresAt: config.expiresAt, channel: config.channel, rollout: config.rollout });
    await writeFile(resolve(config.output), `${JSON.stringify(release)}\n`, { flag: 'wx' });
    console.log(JSON.stringify({ status: 'packed', version: release.signed.version, sequence: release.signed.sequence }));
  } else {
    if (!config.source || !config.trustFile || !config.target || !config.stateDir || !config.validate?.length) throw new Error('update_configuration_required');
    async function sync() {
      const trust = JSON.parse(await readFile(resolve(config.trustFile), 'utf8'));
      const release = await fetchRelease(config.source);
      const result = await applyRelease({ target: resolve(config.target), stateDir: resolve(config.stateDir), release, trust,
        deploymentId: config.deploymentId, channel: config.channel,
        activate: config.activate?.length ? () => runValidation(config.activate, resolve(config.projectDirectory || '.')) : undefined,
        validate: () => runValidation(config.validate, resolve(config.projectDirectory || '.')) });
      console.log(JSON.stringify(result));
    }
    if (command === 'sync') await sync();
    else {
      const interval = Math.max(60_000, Number(config.intervalMs) || 6 * 60 * 60 * 1000);
      while (true) {
        try { await sync(); } catch (e) { console.error(JSON.stringify(failure(e))); }
        await new Promise(r => setTimeout(r, interval));
      }
    }
  }
} catch (e) {
  console.error(JSON.stringify(failure(e)));
  process.exitCode = 1;
}
