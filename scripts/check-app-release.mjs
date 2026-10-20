#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { statSync, readdirSync } from 'node:fs';
import { resolve, isAbsolute } from 'node:path';
import { fileURLToPath } from 'node:url';

// Local qualification only. Closed commands; never reads live hosting secrets,
// changes grants, deploys, starts production writers or attaches to a browser.
const root = fileURLToPath(new URL('../', import.meta.url));
const fail = (code, stage) => {
  process.stdout.write(JSON.stringify({ schema: 'soty.app-pipeline-check.v1', ok: false, code, ...(stage ? { stage } : {}), deployed: false }) + '\n');
  process.exit(1);
};
if (process.argv.length !== 2) fail('pipeline_arguments_invalid');
const [major, minor] = process.versions.node.split('.').map(Number);
if (major < 24 || major === 24 && minor < 15) fail('pipeline_node_unsupported');
const caddy = process.env.SOTY_CADDY_BIN;
if (!caddy || !isAbsolute(caddy)) fail('pipeline_caddy_required');
try { if (!statSync(caddy).isFile()) fail('pipeline_caddy_required'); } catch { fail('pipeline_caddy_required'); }
const version = spawnSync(caddy, ['version'], { timeout: 5000, encoding: 'utf8', maxBuffer: 65536 });
if (version.status !== 0) fail('pipeline_caddy_unavailable');

const tests = [
  ...readdirSync(resolve(root, 'deploy/apps')).filter(name => name.endsWith('.test.mjs')).sort().map(name => 'deploy/apps/' + name),
  'src/world/app-runtime-display.test.mjs', 'src/world/app-runtime-phase-trace.test.mjs', 'src/world/app-runtime-phase-stage.test.mjs',
  'server/test/capabilities-mcp.test.mjs', 'server/test/capabilities-mcp-codex-compat.test.mjs', 'server/test/capabilities-mcp-independent.test.mjs',
  'server/test/capabilities-mcp-ingress.test.mjs', 'server/test/capabilities-oauth-native-flow.test.mjs',
  'modules/human-identity/test/http.test.mjs', 'modules/human-identity/test/https-cookie.test.mjs',
  'modules/source-rp/test/protocol-http.test.mjs', 'modules/source-rp/test/session-service.test.mjs', 'modules/source-rp/test/multiprocess.test.mjs',
];
const checks = [
  ['tests', ['--test', '--test-concurrency=1', ...tests]],
  ['typecheck', ['node_modules/typescript/bin/tsc', '-p', 'tsconfig.json', '--noEmit']],
  ['prebuild', ['scripts/build-agent-release.mjs']],
  ['build', ['node_modules/vite/bin/vite.js', 'build']],
];
for (const [stage, args] of checks) {
  process.stdout.write(JSON.stringify({ stage, state: 'running' }) + '\n');
  const result = spawnSync(process.execPath, args, { cwd: root, stdio: 'inherit', timeout: 180000 });
  if (result.status !== 0) fail('pipeline_check_failed', stage);
}
process.stdout.write(JSON.stringify({ schema: 'soty.app-pipeline-check.v1', ok: true,
  scope: 'app_deployment_pipeline', productionAndDataGatesRequired: true, deployed: false, stages: checks.map(([stage]) => stage) }) + '\n');
