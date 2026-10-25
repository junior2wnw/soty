import { createHash } from 'node:crypto';
import { lstatSync, readdirSync, readFileSync, mkdirSync, writeFileSync, realpathSync } from 'node:fs';
import path from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import {SOURCE_SDK_INDEX,SOURCE_SDK_LIMITS,sourceSdkArchiveName} from '../../modules/source-app/package-contract.mjs';
import {readSourceSdkArchive,verifySourceSdkIndex} from '../../modules/source-app/public-release.mjs';
import {parseSourceJson} from '../../modules/source-app/shared/strict-json.mjs';

const fail = code => { throw Object.assign(new Error(code), { code }); };
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
export const PUBLIC_SKILL_FILES = Object.freeze([
  'skills/index.json', 'skills/index.html',
  'skills/soty-cover-art/SKILL.md', 'skills/soty-cover-art/references/runtime.md', 'skills/soty-cover-art/references/operator.md',
  'skills/soty-app-deploy/SKILL.md',
  'skills/soty-app-deploy/references/authentication.md', 'skills/soty-app-deploy/references/capacity.md',
  'skills/soty-app-deploy/references/contract.md', 'skills/soty-app-deploy/references/existing-data.md',
  'skills/soty-app-deploy/references/native-bindings.md', 'skills/soty-app-deploy/references/operations.md',
  'skills/soty-app-deploy/references/recovery.md', 'skills/soty-app-deploy/references/standard-modules.md',
  'skills/soty-app-deploy/references/troubleshooting.md',
]);
const fixed = new Set(['index.html', 'sw.js', 'manifest.webmanifest', 'icon.svg', 'capability-docs-update.js',
  'app-art/manifest.json', 'app-art/field-profile.json', 'fonts/manrope.woff2', 'fonts/manrope-OFL.txt',
  'icons/soty.svg', 'icons/soty-180.png', 'icons/soty-192.png', 'icons/soty-512.png',
  'help/interaction.html', SOURCE_SDK_INDEX, ...PUBLIC_SKILL_FILES]);
const hashed = name => /^assets\/[A-Za-z0-9_-]+-[A-Za-z0-9_-]{8}\.(?:js|css|woff2?|png|jpe?g|webp|svg|avif)$/u.test(name)
  || /^app-art\/[a-z0-9-]+\/v\d{3,}-[a-f0-9]{12}\/cover-(?:160|320|640|960|1440)\.[a-f0-9]{12}\.webp$/u.test(name)
  || sourceSdkArchiveName(name);
const publicDirectory = name => ['assets', 'app-art', 'fonts', 'icons'].includes(name)
  || ['help', 'sdk', 'sdk/source-app', 'skills', 'skills/soty-cover-art', 'skills/soty-cover-art/references',
    'skills/soty-app-deploy', 'skills/soty-app-deploy/references'].includes(name)
  || /^app-art\/[a-z0-9-]+(?:\/v\d{3,}-[a-f0-9]{12})?$/u.test(name);
const securityHeaders = ['Content-Security-Policy', 'Cross-Origin-Opener-Policy', 'Cross-Origin-Resource-Policy',
  'Origin-Agent-Cluster', 'Permissions-Policy', 'Referrer-Policy', 'Strict-Transport-Security', 'X-Content-Type-Options', 'X-Frame-Options'];
function validateRelease(release) {
  if (!Array.isArray(release?.manifest) || !release.manifest.length || release.manifest.length > 4096
    || release.manifest.some(file => !file || !(fixed.has(file.name) || hashed(file.name)) || !/^[a-f0-9]{64}$/u.test(file.sha256)
      || !Number.isSafeInteger(file.bytes) || file.bytes < 0 || file.immutable !== hashed(file.name))
    || new Set(release.manifest.map(file => file.name)).size !== release.manifest.length
    || !['index.html', 'sw.js', 'manifest.webmanifest'].every(name => release.manifest.some(file => file.name === name))
    || release.id !== hash(JSON.stringify(release.manifest))) fail('ui_release_manifest_invalid');
}

function readDist(directory) {
  const root = path.resolve(directory), files = new Map();let sdkArchives=0;
  if (realpathSync(root) !== root) fail('ui_dist_symlink');
  function visit(relative = '') {
    for (const entry of readdirSync(path.join(root, relative), { withFileTypes: true })) {
      const name = relative ? `${relative}/${entry.name}` : entry.name;
      if (relative==='sdk' && entry.name!=='source-app' || relative==='sdk/source-app' && name!==SOURCE_SDK_INDEX && !sourceSdkArchiveName(name)) fail('ui_sdk_entry_denied');
      if (entry.isSymbolicLink() && (fixed.has(name) || hashed(name) || publicDirectory(name))) fail('ui_dist_not_regular');
      if (entry.isDirectory()) { if (publicDirectory(name)) visit(name); continue; }
      if (!fixed.has(name) && !hashed(name)) continue;
      const file = path.join(root, name), stat = lstatSync(file);
      if (!stat.isFile() || stat.isSymbolicLink()) fail('ui_dist_not_regular');
      if (stat.size > 16 * 1024 * 1024 || files.size >= 4096) fail('ui_dist_limit');
      if (sourceSdkArchiveName(name) && stat.size > SOURCE_SDK_LIMITS.archiveBytes || name === SOURCE_SDK_INDEX && stat.size > 4096) fail('ui_dist_limit');
      const bytes = readFileSync(file), sha256 = hash(bytes);
      if (sourceSdkArchiveName(name)) {
        if (++sdkArchives>SOURCE_SDK_LIMITS.archives) fail('ui_dist_limit');
        if (name !== `sdk/source-app/${sha256}.tgz`) fail('ui_sdk_hash_name_mismatch');
        readSourceSdkArchive(bytes);
      }
      files.set(name, { bytes, sha256, immutable: hashed(name) });
    }
  }
  visit(); return files;
}

/** Public UI files only. Retained releases contribute immutable URLs, never old installers or documents. */
export function createUiRelease(dist, retained = []) {
  const files = readDist(dist);
  for (const required of ['index.html', 'sw.js', 'manifest.webmanifest']) if (!files.has(required)) fail('ui_required_file_missing');
  if ([...files.keys()].some(sourceSdkArchiveName) || files.has(SOURCE_SDK_INDEX)) {
    if (!files.has(SOURCE_SDK_INDEX)) fail('ui_sdk_index_missing');
    const index = parseSourceJson(new TextDecoder('utf-8',{fatal:true}).decode(files.get(SOURCE_SDK_INDEX).bytes),{bytes:4096});
    const archive = typeof index?.artifact?.url === 'string' ? files.get(index.artifact.url.slice(1)) : null;
    if (!archive || !sourceSdkArchiveName(index.artifact.url.slice(1))) fail('ui_sdk_archive_missing');
    verifySourceSdkIndex(index, archive.bytes);
  }
  for (const directory of retained) for (const [name, file] of readDist(directory)) {
    if (!file.immutable) continue;
    if (files.has(name) && files.get(name).sha256 !== file.sha256) fail('ui_immutable_url_conflict');
    files.set(name, file);
  }
  const requireUrl = url => {
    if (typeof url !== 'string' || !/^\/[A-Za-z0-9_./-]*$/u.test(url) || !files.has(url === '/' ? 'index.html' : url.slice(1))) fail('ui_reference_not_in_release');
  };
  for (const match of files.get('index.html').bytes.toString().matchAll(/(?:src|href)="(\/[^"#?]*)"/gu)) requireUrl(match[1]);
  const worker = files.get('sw.js').bytes.toString();
  for (const name of ['buildAssets', 'classicAssets', 'shell']) {
    const matches = [...worker.matchAll(new RegExp(`const ${name} = (\\[[^\\n]*\\]);`, 'gu'))];
    if (matches.length !== 1) fail('ui_worker_graph_missing');
    const urls = JSON.parse(matches[0][1]); if (!Array.isArray(urls)) fail('ui_worker_graph_invalid');
    urls.forEach(requireUrl);
  }
  for (const icon of JSON.parse(files.get('manifest.webmanifest').bytes).icons ?? []) requireUrl(icon.src);
  if (files.has('skills/index.json')) for (const skill of JSON.parse(files.get('skills/index.json').bytes).skills ?? []) {
    requireUrl(skill.entrypoint); (skill.references ?? []).forEach(requireUrl);
  }
  for (const document of ['skills/index.html', 'help/interaction.html']) if (files.has(document)) {
    for (const match of files.get(document).bytes.toString().matchAll(/(?:src|href)="(\/[^"#?]*)"/gu)) requireUrl(match[1]);
  }
  const manifest = [...files].map(([name, file]) => ({ name, sha256: file.sha256, bytes: file.bytes.length, immutable: file.immutable })).sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
  return { id: hash(JSON.stringify(manifest)), manifest, files };
}

/** Creates a fresh local staging directory; installation, ownership and transport are the caller's job. */
export function writeUiRelease(release, directory) {
  validateRelease(release);
  const root = path.resolve(directory), parent = path.dirname(root);
  if (realpathSync(parent) !== parent) fail('ui_release_parent_symlink');
  mkdirSync(root, { mode: 0o755 });
  for (const { name, sha256 } of release.manifest) {
    const file = release.files.get(name);
    if (!file || hash(file.bytes) !== sha256 || name.split('/').includes('..') || !(fixed.has(name) || hashed(name))) fail('ui_release_changed');
    mkdirSync(path.dirname(path.join(root, name)), { recursive: true, mode: 0o755 });
    writeFileSync(path.join(root, name), file.bytes, { flag: 'wx', mode: 0o444 });
  }
}

function locateProxy(config, host) {
  const found = [];
  function visit(value, pointer = [], hosts = [], boundaries = []) {
    if (Array.isArray(value)) { value.forEach((child, index) => visit(child, [...pointer, index], hosts, boundaries)); return; }
    if (!value || typeof value !== 'object') return;
    const matches = Array.isArray(value.match) ? value.match : [];
    const inherited = matches.flatMap(match => match.host ?? []); if (inherited.length) hosts = inherited;
    if (matches.some(match => match.path)) boundaries = [...boundaries, { pointer: [...pointer, 'match'], matches }];
    if (hosts.length === 1 && hosts[0] === host && value.handler === 'reverse_proxy'
      && value.upstreams?.length === 1 && value.upstreams[0].dial === '127.0.0.1:18182') found.push({ pointer, value, boundaries });
    Object.entries(value).forEach(([key, child]) => visit(child, [...pointer, key], hosts, boundaries));
  }
  visit(config);
  if (found.length !== 1 || found[0].value.handle_response || found[0].value.headers || found[0].value.rewrite
    || Object.keys(found[0].value).some(key => !['handler', 'upstreams', 'transport', 'flush_interval'].includes(key))) fail('ui_fallback_unknown_or_ambiguous');
  let leaf = config; found[0].pointer.slice(0, -2).forEach(key => { leaf = leaf[key]; });
  if (found[0].pointer.at(-2) !== 'handle' || found[0].pointer.at(-1) !== 0 || !isDeepStrictEqual(leaf, { handle: [found[0].value] })) fail('ui_fallback_unknown_or_ambiguous');
  return found[0];
}

// Keep the old native path matcher as the first OR branch, including all its
// methods/queries. Only new, exact inventory URLs gain a GET/HEAD-only branch.
function coverInventory(caddyfile, config, selected, release) {
  if (!selected.boundaries.length) return { caddyfile, config };
  const boundary = selected.boundaries[0], paths = boundary.matches[0]?.path;
  if (selected.boundaries.length !== 1 || !Array.isArray(paths)
    || !isDeepStrictEqual(boundary.matches, [{ path: paths }])
    || !['/', '/assets/*'].every(name => paths.includes(name))
    || paths.some(name => typeof name !== 'string' || !/^\/[A-Za-z0-9_./-]*\*?$/u.test(name))) fail('ui_fallback_unknown_or_ambiguous');
  const missing = release.manifest.map(file => '/' + file.name).filter(url => !paths.some(pattern => pattern.endsWith('*')
    ? url.toLowerCase().startsWith(pattern.slice(0, -1).toLowerCase()) : url.toLowerCase() === pattern.toLowerCase()));
  if (!missing.length) return { caddyfile, config };
  const definitions = [...caddyfile.matchAll(/^([\t ]*)(@[A-Za-z0-9_]+)[\t ]+path[\t ]+([^\r\n]+)\r?$/gmu)]
    .filter(match => isDeepStrictEqual(match[3].trim().split(/[\t ]+/u), paths));
  if (definitions.length !== 1) fail('ui_fallback_unknown_or_ambiguous');
  const definition = definitions[0], args = values => values.map(value => JSON.stringify(value)).join(', ');
  const expression = `path(${args(paths)}) || (method("GET", "HEAD") && path(${args(missing)}) && !query({"project": "*"}) && !query({"hive": "*"}))`;
  const text = `${definition[1]}${definition[2]} expression \`${expression}\``;
  const expected = structuredClone(config); let owner = expected;
  boundary.pointer.slice(0, -1).forEach(key => { owner = owner[key]; });
  owner[boundary.pointer.at(-1)] = [{ expression: { expr: expression, name: definition[2].slice(1) } }];
  return { caddyfile: caddyfile.slice(0, definition.index) + text + caddyfile.slice(definition.index + definition[0].length), config: expected };
}

// Caddy renumbers generated handle groups while adapting. Preserve their exact
// equality partition; only the arbitrary groupN labels may differ.
function normalizedGroups(config) {
  const result = structuredClone(config), names = new Map();
  const visit = routes => {
    for (const route of routes ?? []) {
      if (Array.isArray(route.handle) && /^group\d+$/u.test(route.group)) {
        if (!names.has(route.group)) names.set(route.group, `group${names.size}`);
        route.group = names.get(route.group);
      }
      for (const handler of route.handle ?? []) {
        if (handler.handler === 'subroute') { visit(handler.routes); visit(handler.errors?.routes); }
        if (handler.handler === 'reverse_proxy') for (const response of handler.handle_response ?? []) visit(response.routes);
      }
    }
  };
  for (const server of Object.values(result.apps?.http?.servers ?? {})) {
    visit(server.routes); visit(server.errors?.routes);
    for (const named of Object.values(server.named_routes ?? {})) visit(named.routes);
  }
  return result;
}

export function verifyStructuralDelta(expected, actual) {
  if (!isDeepStrictEqual(normalizedGroups(expected), normalizedGroups(actual))) fail('ui_structure_changed');
  return true;
}

// Recognize only the installed literal proxy + plain transport block. Caddy,
// not indentation or a homegrown config parser, validates the complete candidate.
function proxyBlocks(text) {
  const blocks = [];
  for (const start of text.matchAll(/^[\t ]*reverse_proxy 127\.0\.0\.1:18182 \{[\t ]*\r?$/gmu)) {
    let depth = 1, lines = 0, offset = start.index + start[0].length;
    for (const item of text.slice(offset).matchAll(/[^\n]*(?:\n|$)/gu)) {
      const line = item[0].replace(/\r?\n$/u, ''), value = line.trim();
      if (++lines > 64 || item.index > 8192) break;
      if (value === '}') depth--;
      else if (value === 'transport http {' && depth === 1) depth++;
      else if (!value.startsWith('#') && /[{}]/u.test(value)) break;
      if (depth === 0) { blocks.push({ index: start.index, text: text.slice(start.index, offset + item.index + line.length) }); break; }
    }
  }
  return blocks;
}

function replacement(original, release, root) {
  const urls = ['/', ...release.manifest.map(file => '/' + file.name)], immutable = release.manifest.filter(file => file.immutable).map(file => '/' + file.name);
  const body = original.slice(original.indexOf('{') + 1, original.lastIndexOf('}'));
  return `@soty_ui_release {
    method GET HEAD
    path ${urls.join(' ')}
    not {\n query project=*\n }
    not {\n query hive=*\n }
  }
  @soty_ui_immutable path ${immutable.join(' ')}
  reverse_proxy @soty_ui_release 127.0.0.1:18182 {${body}
    header_up -If-None-Match
    header_up -If-Modified-Since
    header_up -If-Match
    header_up -If-Unmodified-Since
    header_up -If-Range
    header_up -Range
    header_up Accept-Encoding identity
    @soty_ui_ok status 200
    handle_response @soty_ui_ok {
      route {
        copy_response_headers {
          include ${securityHeaders.join(' ')}
        }
        header Cache-Control no-store
        header @soty_ui_immutable Cache-Control "public, max-age=31536000, immutable"
        root * ${root}
        file_server
      }
    }
  }
  ${original}`;
}

/** No Caddyfile grammar implementation: Caddy proves one exact JSON-node replacement.
 * adapt must keep stdout/stderr private; its output can contain existing secrets. */
export async function createCaddyOverlay({ caddyfile, release, root, adapt, host = '4-2.xn--p1ai' }) {
  validateRelease(release);
  if (!/^[a-z0-9.-]+$/u.test(host) || !/^[a-f0-9]{64}$/u.test(release.id)
    || !/^\/[A-Za-z0-9_/-]+$/u.test(root) || path.posix.normalize(root) !== root || root.split('/').some(part => ['.', '..', 'current'].includes(part))
    || !root.endsWith('/' + release.id) || !release.manifest.some(file => file.immutable)) fail('ui_release_root_invalid');
  const before = await adapt(caddyfile), selected = locateProxy(before, host), accepted = [];
  const covered = coverInventory(caddyfile, before, selected, release);
  const matches = proxyBlocks(covered.caddyfile); if (matches.length > 64) fail('ui_fallback_unknown_or_ambiguous');
  for (const match of matches) {
    const text = replacement(match.text.trimStart(), release, root);
    let standalone, after;
    try {
      standalone = await adapt(`{\n auto_https off\n}\nhttp://127.0.0.1:19001 {\n${text}\n}`);
      const candidate = covered.caddyfile.slice(0, match.index) + text + covered.caddyfile.slice(match.index + match.text.length);
      after = await adapt(candidate);
      const wrappers = [];
      const scan = value => {
        if (!value || typeof value !== 'object') return;
        if (value.handler === 'subroute' && value.routes?.length === 2 && isDeepStrictEqual(value.routes[1], { handle: [selected.value] })) wrappers.push(value);
        Object.values(value).forEach(child => Array.isArray(child) ? child.forEach(scan) : scan(child));
      };
      scan(standalone); if (wrappers.length !== 1) continue;
      const expected = structuredClone(covered.config); let owner = expected;
      selected.pointer.slice(0, -3).forEach(key => { owner = owner[key]; }); owner.splice(selected.pointer.at(-3), 1, ...wrappers[0].routes);
      verifyStructuralDelta(expected, after); accepted.push({ caddyfile: candidate, config: after });
    } catch { /* Unknown syntax/config changes are a refusal, never a best-effort patch. */ }
  }
  if (accepted.length !== 1) fail('ui_fallback_unknown_or_ambiguous');
  return { ...accepted[0], baselineSha256: hash(caddyfile), candidateSha256: hash(accepted[0].caddyfile), releaseId: release.id };
}
