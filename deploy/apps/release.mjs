import { createHash } from 'node:crypto';
import { mkdir, readFile, realpath, writeFile, stat } from 'node:fs/promises';
import { dirname, join, relative, resolve, isAbsolute } from 'node:path';
import { normalizeLocalAppManifest } from '../../scripts/agent-modules/local-apps.mjs';
import { validateAppDeployment, deploymentOrigin, AppDeploymentError } from '../../src/world/app-deployment.mjs';
import { formatAppLaunchRoute, validateAppLaunchPath } from '../../src/world/app-launch.mjs';

export const releaseHash = value => createHash('sha256').update(value).digest('hex');
const check = (value, code) => { if (!value) throw new AppDeploymentError(code); };
const contained = (base, target) => { const path = relative(base, target); return !isAbsolute(path) && path !== '..' && !path.startsWith('..' + (process.platform === 'win32' ? '\\' : '/')); };
export async function readReleaseJson(path, maximum = 131072) {
  const info = await stat(path); check(info.isFile() && info.size <= maximum, 'release_file_too_large');
  try { return JSON.parse(await readFile(path, 'utf8')); } catch { throw new AppDeploymentError('invalid_release_json'); }
}
async function manifestPath(project, create) {
  const base = await realpath(resolve(project));
  const folder = join(base, '.soty');
  if (create) await mkdir(folder, { recursive: true });
  check(contained(base, await realpath(folder)), 'manifest_workspace_escape');
  const file = join(folder, 'app.json');
  try { check(contained(base, await realpath(file)), 'manifest_workspace_escape'); }
  catch (error) { if (error.code !== 'ENOENT' || !create) throw error; }
  return file;
}
export async function initAppManifest({ project, name, port, entryPath = '/' }) {
  const value = normalizeLocalAppManifest({ schema: 'soty.local-app.v1', name, port, entryPath });
  const file = await manifestPath(project, true);
  try {
    const existing = normalizeLocalAppManifest(await readReleaseJson(file, 4096));
    check(JSON.stringify(existing) === JSON.stringify(value), 'manifest_already_exists_different');
    return { file, manifest: value, unchanged: true };
  } catch (error) { if (error.code !== 'ENOENT') throw error; }
  await writeFile(file, JSON.stringify(value, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  return { file, manifest: value, unchanged: false };
}
export async function readAppManifest(project) {
  return normalizeLocalAppManifest(await readReleaseJson(await manifestPath(project, false), 4096));
}
export function createReleasePlan({ manifest, deployment, domainId, mode = 'isolated', gatewayPort = 18182, frameOrigins = [], now = Date.now() }) {
  const app = validateAppDeployment(deployment), local = normalizeLocalAppManifest(manifest);
  check(app.app.state === 'enabled', 'app_revoked');
  check(now - app.checkedAt <= 600000 && app.checkedAt <= now + 30000, 'deployment_snapshot_stale');
  check(local.port === app.source.port && local.entryPath === app.source.entryPath, 'manifest_source_mismatch');
  check(['isolated', 'native'].includes(mode), 'invalid_ingress_mode');
  check(Number.isInteger(gatewayPort) && gatewayPort >= 1024 && gatewayPort <= 65535 && gatewayPort !== app.source.port, 'invalid_gateway_port');
  const aliases = app.addresses.aliases.filter(item => item.active && item.state === 'bound');
  const selected = domainId ? aliases.find(item => item.id === domainId) : aliases.length === 1 ? aliases[0] : null;
  check(Boolean(selected), domainId ? 'active_named_address_required' : 'select_named_address');
  const origins = [...new Set([app.shellOrigin, ...frameOrigins.map(deploymentOrigin)])];
  check(origins.length <= 8 && !origins.includes(selected.origin), 'invalid_frame_origins');
  const origin = new URL(selected.origin);
  check(origin.port === '' && !['localhost', '127.0.0.1', '[::1]'].includes(origin.hostname), 'invalid_public_app_origin');
  const plan = { schema: 'soty.app-release.v1', createdAt: now, mode,
    appId: app.app.id, domainId: selected.id, origin: selected.origin, shellOrigin: app.shellOrigin,
    shellLaunchUrl: app.shellOrigin + '/#' + formatAppLaunchRoute({ appId: app.app.id, domainId: selected.id, path: app.source.entryPath }),
    source: app.source, publication: app.publication, gatewayPort, frameOrigins: origins,
    deploymentSha256: releaseHash(JSON.stringify(app)),
    probes: [
      { path: app.source.entryPath, status: 200 },
      { path: queriedPath(app.source.entryPath), status: 200 },
      { path: '/_soty/boot?path=' + encodeURIComponent(app.source.entryPath), status: 200 },
    ] };
  return plan;
}
function queriedPath(path) {
  const hashAt = path.indexOf('#'), base = hashAt < 0 ? path : path.slice(0, hashAt);
  return base + (base.includes('?') ? '&' : '?') + 'soty_release_probe=1';
}
export function nativeIngress(plan) {
  check(plan.mode === 'native', 'native_ingress_required');
  check(/^app-[a-f0-9]{32}$/u.test(plan.appId) && /^[a-f0-9]{64}$/u.test(plan.source.digest), 'invalid_native_pin');
  const host = new URL(deploymentOrigin(plan.origin)).hostname;
  check(!/[{}"\s]/u.test(host) && Number.isInteger(plan.source.port) && plan.source.port >= 1024 && plan.source.port <= 65535
    && Number.isInteger(plan.gatewayPort) && plan.gatewayPort >= 1024 && plan.gatewayPort <= 65535
    && plan.source.port !== plan.gatewayPort, 'invalid_native_upstream');
  const parents = plan.frameOrigins.map(deploymentOrigin).join(' ');
  check(plan.frameOrigins.length > 0 && plan.frameOrigins.length <= 8 && !plan.frameOrigins.includes(plan.origin), 'invalid_frame_origins');
  return [
    '# Soty native app ' + plan.appId + '; reviewed fixed upstream, current source pin.',
    host + ' {',
    '    header {',
    '        Strict-Transport-Security "max-age=31536000; includeSubDomains"',
    '        X-Content-Type-Options "nosniff"',
    '        Referrer-Policy "no-referrer"',
    '        +Content-Security-Policy "frame-ancestors \'self\' ' + parents + '"',
    '        -Server',
    '        defer',
    '    }',
    '    @soty_boot path /_soty/*',
    '    route {',
    '        handle @soty_boot { reverse_proxy 127.0.0.1:' + plan.gatewayPort + ' }',
    '        handle {',
    '            forward_auth 127.0.0.1:' + plan.gatewayPort + ' {',
    '                uri /_soty/ingress-check?',
    '                header_up X-Soty-Ingress-App ' + plan.appId,
    '                header_up X-Soty-Ingress-Target ' + plan.source.digest,
    '            }',
    '            reverse_proxy 127.0.0.1:' + plan.source.port + ' {',
    '                header_up Cookie "(^|; *)__Host-soty_app_session=[^;]*" ""',
    '                header_up -X-Soty-Ingress-App',
    '                header_up -X-Soty-Ingress-Target',
    '            }',
    '        }',
    '    }',
    '}',
    '',
  ].join('\n');
}
/** One shared named zone serves future isolated apps; exact native hosts win. */
export function namedZoneIngress({ origin, gatewayPort = 18182 }) {
  const zone = new URL(deploymentOrigin(origin));
  check(!zone.port && zone.hostname.includes('.') && !/^[\d.]+$/u.test(zone.hostname)
    && zone.hostname !== 'localhost' && !/[{}"\s]/u.test(zone.hostname), 'invalid_named_zone');
  check(Number.isInteger(gatewayPort) && gatewayPort >= 1024 && gatewayPort <= 65535, 'invalid_gateway_port');
  return '# Soty named applications in ' + zone.hostname + '\nhttps://*.' + zone.hostname + ' {\n'
    + '    tls { on_demand }\n    reverse_proxy 127.0.0.1:' + gatewayPort + '\n}\n';
}
export async function writeReleasePlan(output, plan) {
  // A fresh run directory prevents confusing a new intent with an old receipt.
  const directory = resolve(output); await mkdir(directory, { mode: 0o700 });
  const files = {};
  if (plan.mode === 'native') {
    const snippet = nativeIngress(plan);
    await writeFile(join(directory, 'native-ingress.caddy'), snippet, { flag: 'wx', mode: 0o600 });
    files['native-ingress.caddy'] = releaseHash(snippet);
  }
  const value = { ...plan, files };
  await writeFile(join(directory, 'release-plan.json'), JSON.stringify(value, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  return { file: join(directory, 'release-plan.json'), plan: value };
}
/** HTTP/TLS is one gate; browser login, read/write and data retention are separate. */
export async function verifyRelease(plan, { probes = [], fetcher = fetch } = {}) {
  check(plan.schema === 'soty.app-release.v1' && /^app-[a-f0-9]{32}$/u.test(plan.appId), 'invalid_release_plan');
  deploymentOrigin(plan.origin); deploymentOrigin(plan.shellOrigin);
  check(plan.publication.launchPolicy === 'anyone', 'authenticated_browser_verification_required');
  const checks = [...plan.probes, ...probes];
  check(checks.length >= 3 && checks.length <= 32, 'invalid_release_probes');
  const results = [];
  for (const item of checks) {
    check(item && Object.keys(item).every(key => ['path', 'status', 'contains'].includes(key))
      && typeof item.path === 'string' && item.path.startsWith('/') && !item.path.startsWith('//')
      && !/[\u0000-\u0020\u007f\\]/u.test(item.path)
      && Number.isInteger(item.status) && item.status >= 200 && item.status <= 499
      && (item.contains === undefined || typeof item.contains === 'string' && item.contains.length <= 256), 'invalid_release_probe');
    const url = new URL(item.path, plan.origin);
    check(url.origin === plan.origin, 'probe_origin_escape');
    const response = await fetcher(url, { redirect: 'manual', credentials: 'omit', signal: AbortSignal.timeout(10000) });
    let contains = true;
    if (item.contains !== undefined) {
      const reader = response.body.getReader(); let length = 0; const chunks = [];
      try {
        for (;;) { const { done, value } = await reader.read(); if (done) break; length += value.byteLength; check(length <= 2097152, 'probe_response_too_large'); chunks.push(Buffer.from(value)); }
        contains = Buffer.concat(chunks).toString('utf8').includes(item.contains);
      } finally { await reader.cancel(); }
    } else await response.body?.cancel();
    results.push({ path: item.path, expectedStatus: item.status, status: response.status, contentMatches: contains,
      ok: response.status === item.status && contains });
  }
  return { schema: 'soty.app-release-check.v1', checkedAt: Date.now(), appId: plan.appId, domainId: plan.domainId,
    origin: plan.origin, planSha256: releaseHash(JSON.stringify(plan)),
    ok: results.every(item => item.ok), httpOnly: true, browserAndDataChecksRequired: true, results };
}
