import { createHash, randomUUID } from 'node:crypto';
import { readFile, writeFile, rename, mkdir, rmdir, readdir, stat, lstat, realpath, unlink } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isDeepStrictEqual } from 'node:util';
import { appArtBindingKey } from '../../src/world/app-art-identity.mjs';

export const REPO_ROOT = fileURLToPath(new URL('../../', import.meta.url));
export const HISTORY_PARTS = Object.freeze(['output', 'app-art-history']);
export const ART_RECIPE = Object.freeze({ version: 'webp-1', widths: [320, 640, 960, 1440], quality: 82, effort: 5, byteLimits: [60000, 140000, 250000, 420000] });
export const FIELD_ART_RECIPE = Object.freeze({ version: 'webp-field-1', widths: [160, 320, 640, 960], quality: 82, effort: 5, byteLimits: [20000, 60000, 140000, 250000] });
const KEY = /^[a-z][a-z0-9-]{1,63}$/u;
const APP_ID = /^(?:app-[a-f0-9]{32}|notes|chess)$/u;
const COLOR = /^#[0-9a-f]{6}$/iu;
const HASH = /^[a-f0-9]{64}$/u;
const VERSION_DIR = /^v(\d{3,6})-([a-f0-9]{12})$/u;
const ART_PATH = /^\/app-art\/[a-z][a-z0-9-]{1,63}\/v\d{3,6}-[a-f0-9]{12}\/[a-z0-9.-]+$/u;
const PUBLIC_KEY = /^art-[a-f0-9]{32}$/u;
export const FIELD_ART_ICONS = Object.freeze(['cells','brush','bars','activity','note','chess','music','app']);
const PRIVATE_PATH = /^output\/app-art-history\/[a-z][a-z0-9-]{1,63}\/v\d{3,6}-[a-f0-9]{12}\/(?:source\.[a-f0-9]{12}\.(?:png|jpg|webp)|provenance\.json|layout\.[a-f0-9]{16}\.json)$/u;
const own = (object, key) => object && Object.hasOwn(object, key) ? object[key] : undefined;
const assert = (condition, message) => { if (!condition) throw new Error(message); };
export const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const jsonBytes = value => Buffer.from(JSON.stringify(value, null, 2) + '\n');
const sorted = object => Object.fromEntries(Object.entries(object).sort(([a], [b]) => a.localeCompare(b, 'en')));

function text(value, label, max = 400) {
  assert(typeof value === 'string' && value.trim().length > 0 && value.length <= max && !/[\u0000-\u001f\u007f]/u.test(value), `${label}: expected plain text, 1-${max} characters`);
  return value.trim();
}
export function validateKey(value) { assert(typeof value === 'string' && KEY.test(value), 'Invalid cover key: use 2-64 lowercase letters, digits and hyphens'); return value; }
export function validateAppId(value) { assert(typeof value === 'string' && APP_ID.test(value), 'Invalid stable app id'); return value; }
export function validateDefinition(value) {
  assert(value && typeof value === 'object' && !Array.isArray(value), 'Artwork definition must be an object');
  const key = validateKey(value.key);
  const name = text(value.name, 'name', 120);
  const purpose = text(value.purpose, 'purpose', 400);
  const subject = text(value.subject, 'subject', 1800);
  const alt = text(value.alt, 'alt', 300);
  assert(value.palette && ['base', 'accent', 'ink'].every(field => COLOR.test(value.palette[field] || '')), 'palette requires base/accent/ink in #RRGGBB');
  const focalPoint = value.focalPoint || { x: 0.5, y: 0.45 };
  assert(['x', 'y'].every(field => typeof focalPoint[field] === 'number' && Number.isFinite(focalPoint[field]) && focalPoint[field] >= 0 && focalPoint[field] <= 1), 'focalPoint coordinates must be between 0 and 1');
  const kind = value.kind || 'app';
  assert(['builtin', 'example', 'app'].includes(kind), 'Unknown artwork kind');
  const result = { key, name, purpose, subject, alt, palette: { base: value.palette.base.toUpperCase(), accent: value.palette.accent.toUpperCase(), ink: value.palette.ink.toUpperCase() }, focalPoint: { x: focalPoint.x, y: focalPoint.y }, kind };
  if (value.compactFocalPoint !== undefined) {
    assert(['x', 'y'].every(field => typeof value.compactFocalPoint?.[field] === 'number' && Number.isFinite(value.compactFocalPoint[field]) && value.compactFocalPoint[field] >= 0 && value.compactFocalPoint[field] <= 1), 'compactFocalPoint coordinates must be between 0 and 1');
    result.compactFocalPoint = { x: value.compactFocalPoint.x, y: value.compactFocalPoint.y };
  }
  if (value.publicKey !== undefined) { assert(kind === 'app' && PUBLIC_KEY.test(value.publicKey), 'App publicKey must be an opaque art identifier'); result.publicKey = value.publicKey; }
  if (value.appId !== undefined) result.appId = validateAppId(value.appId);
  if (value.profile !== undefined) { assert(value.profile === 'field', 'Unknown artwork presentation profile'); result.profile = 'field'; }
  if (value.icon !== undefined) { assert(result.profile === 'field' && FIELD_ART_ICONS.includes(value.icon), 'Field icon must be an approved local glyph'); result.icon = value.icon; }
  if (value.styleReference !== undefined) {
    assert(typeof value.styleReference==='string'&&/^output\/[a-z0-9/-]+\.png$/u.test(value.styleReference)&&!value.styleReference.includes('..'),'Style reference must be a known local output PNG');
    result.styleReference=value.styleReference;
  }
  return result;
}

/** Never accept arbitrary filesystem destinations from registry metadata. */
export function inside(base, ...parts) {
  const target = resolve(base, ...parts), relation = relative(resolve(base), target);
  assert(relation === '' || (!relation.startsWith(`..${sep}`) && relation !== '..' && !isAbsolute(relation)), 'Path escapes the artwork workspace');
  return target;
}
async function safeOutputPath(root, ...parts) {
  const base = await realpath(resolve(root));
  const target = inside(base, ...parts);
  let ancestor = dirname(target);
  while (true) {
    try { const actual = await realpath(ancestor); inside(base, actual); break; }
    catch (error) { if (error.code !== 'ENOENT') throw error; const parent = dirname(ancestor); assert(parent !== ancestor, 'No workspace parent'); ancestor = parent; }
  }
  return target;
}
export async function readJson(path, maxBytes = 2 * 1024 * 1024) {
  const info = await stat(path);
  assert(info.isFile() && info.size <= maxBytes, 'JSON input is not a bounded regular file');
  try { return JSON.parse(await readFile(path, 'utf8')); } catch { throw new Error(`Invalid JSON: ${basename(path)}`); }
}
async function atomicJson(root, parts, value) {
  const path = await safeOutputPath(root, ...parts);
  await mkdir(dirname(path), { recursive: true });
  const bytes=jsonBytes(value);
  try {
    const existing=await lstat(path);
    assert(existing.isFile()&&!existing.isSymbolicLink(),'JSON output must be a regular local file');
    if(existing.size===bytes.length&&(await readFile(path)).equals(bytes))return;
  } catch(error) {if(error.code!=='ENOENT')throw error;}
  const temporary = path + `.tmp-${randomUUID()}`;
  await writeFile(temporary, bytes, { flag: 'wx' });
  try {
    for(let attempt=0;;attempt++){
      try {await rename(temporary,path);break;}
      catch(error){if(process.platform!=='win32'||!['EPERM','EACCES','EBUSY'].includes(error.code)||attempt>=4)throw error;await new Promise(done=>setTimeout(done,40*(attempt+1)));}
    }
  } finally {await unlink(temporary).catch(error=>{if(error.code!=='ENOENT')throw error;});}
}
async function writeDisplayManifests(root, manifest, fieldProfile) {
  // Source import is bundled by Vite; public JSON remains a safe standalone artifact.
  await atomicJson(root, ['public', 'app-art', 'manifest.json'], manifest);
  await atomicJson(root, ['src', 'world', 'app-art-manifest.json'], manifest);
  if (fieldProfile) {
    await atomicJson(root, ['public', 'app-art', 'field-profile.json'], fieldProfile);
    await atomicJson(root, ['src', 'world', 'app-art-field-profile.json'], fieldProfile);
  }
}
async function immutable(root, parts, bytes) {
  const path = await safeOutputPath(root, ...parts);
  await mkdir(dirname(path), { recursive: true });
  try { await writeFile(path, bytes, { flag: 'wx' }); }
  catch (error) {
    if (error.code !== 'EEXIST') throw error;
    const existing = await lstat(path);
    assert(existing.isFile() && !existing.isSymbolicLink() && existing.nlink === 1, 'Immutable output is not an unlinked regular file');
    assert(sha256(await readFile(path)) === sha256(bytes), 'Immutable artwork collision');
  }
  return path;
}
export async function withArtLock(root, operation) {
  await mkdir(await safeOutputPath(root, 'scripts', 'app-art'), { recursive: true });
  const lock = await safeOutputPath(root, 'scripts', 'app-art', '.pipeline-lock');
  try { await mkdir(lock); } catch (error) { if (error.code === 'EEXIST') throw new Error('Another artwork operation is active; retry when it finishes'); throw error; }
  try { return await operation(); } finally { await rmdir(lock); }
}
async function state(root, { allowLegacy = false } = {}) {
  let registry;
  try { registry = await readJson(inside(root, 'scripts', 'app-art', 'registry.json')); }
  catch (error) { if (error.code !== 'ENOENT') throw error; throw new Error('Local artwork registry missing; restore private operator registry/history before modifying shipped covers. A fresh checkout can run validate --public.'); }
  assert(registry.schemaVersion === 1 && registry.assets && registry.bindings, 'Unsupported artwork registry');
  text(registry.styleVersion, 'styleVersion', 80);
  for (const [key, spec] of Object.entries(registry.assets)) assert(validateDefinition(spec).key === validateKey(key), 'Registry key does not match definition');
  assertPublicIdentities(registry);
  for (const [id, key] of Object.entries(registry.bindings)) { validateAppId(id); validateKey(key); assert(own(registry.assets, key), 'Binding points to unknown artwork'); }
  validateLocalFieldProfile(registry);
  let manifest;
  try { manifest = await readJson(inside(root, 'public', 'app-art', 'manifest.json')); }
  catch (error) { if (error.code !== 'ENOENT') throw error; manifest = { schemaVersion: 1, styleVersion: registry.styleVersion, bindings: {}, pending: {}, assets: {} }; }
  assert(manifest.schemaVersion === 1 && manifest.assets, 'Unsupported artwork manifest');
  let history;
  try { history = await readJson(inside(root, ...HISTORY_PARTS, 'index.json')); }
  catch (error) { if (error.code !== 'ENOENT') throw error; history = { schemaVersion: 1, assets: {} }; }
  assert(history.schemaVersion === 1 && history.assets, 'Unsupported private artwork history');
  const acceptedPublicKeys = new Set(Object.values(history.assets).map(asset => asset.publicKey || asset.key));
  const withoutHistory = Object.entries(manifest.assets).filter(([key]) => !acceptedPublicKeys.has(key));
  assert(withoutHistory.length === 0 || allowLegacy && withoutHistory.every(([,asset]) => asset.original && asset.provenance?.url), 'Private artwork history missing; restore local accepted history before modifying shipped covers');
  const fieldProfile = await optionalFieldProfile(root);
  if (fieldProfile) {
    assert(Object.keys(fieldProfile.assets || {}).every(key => acceptedPublicKeys.has(key)), 'Private field artwork history missing; restore local history before modifying shipped covers');
    assert(!Object.keys(fieldProfile.aliases || {}).length && !Object.keys(fieldProfile.bindings || {}).length || registry.profiles?.field, 'Private field artwork bindings missing; restore operator registry before modifying shipped covers');
  }
  return { registry, manifest, history };
}
function assertPublicIdentities(registry) {
  const owners = new Map();
  for (const [key, spec] of Object.entries(registry.assets)) {
    const publicKey = spec.publicKey || key;
    assert(!owners.has(publicKey), 'Public artwork key already belongs to another definition; use bind to share one cover');
    owners.set(publicKey, key);
  }
}
function displayAsset(asset) {
  assert(['builtin', 'example', 'app'].includes(asset.kind), 'Unknown accepted artwork kind');
  assert(asset.kind !== 'app' || PUBLIC_KEY.test(asset.publicKey || ''), 'Private app artwork has no opaque public key');
  const palette = Object.fromEntries(['base','accent','ink'].map(field => { assert(COLOR.test(asset.palette?.[field] || ''), 'Invalid display palette'); return [field, asset.palette[field]]; }));
  const focalPoint = Object.fromEntries(['x','y'].map(field => { const value = asset.focalPoint?.[field]; assert(typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1, 'Invalid display focal point'); return [field,value]; }));
  const compactSource = asset.layout?.compactFocalPoint || asset.compactFocalPoint || focalPoint;
  const compactFocalPoint = Object.fromEntries(['x','y'].map(field => { const value = compactSource[field]; assert(typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1, 'Invalid compact focal point'); return [field,value]; }));
  return {
    key: asset.publicKey || asset.key, version: asset.version,
    alt: asset.kind === 'app' ? '' : asset.alt,
    palette, focalPoint, compactFocalPoint,
    renditions: asset.renditions.map(({ url, width, height }) => ({ url, width, height })),
  };
}
function syncManifest(registry, history) {
  const assets = Object.fromEntries(Object.entries(history.assets).filter(([,asset]) => asset.profile !== 'field').map(([key, asset]) => {
    assert(own(registry.assets, key), 'Accepted artwork is absent from local registry');
    return [asset.publicKey || key, displayAsset(asset)];
  }));
  const bindings = Object.fromEntries(Object.entries(registry.bindings).filter(([,key]) => own(history.assets, key) && own(history.assets, key).profile !== 'field').map(([id,key]) => [appArtBindingKey(id), own(history.assets, key).publicKey || key]));
  return { schemaVersion: 1, bindings: sorted(bindings), assets: sorted(assets) };
}
const emptyFieldProfile = () => ({ schemaVersion: 1, profile: 'field', aliases: {}, bindings: {}, assets: {} });
async function optionalFieldProfile(root) {
  try { return await readJson(inside(root, 'public', 'app-art', 'field-profile.json')); }
  catch (error) { if (error.code !== 'ENOENT') throw error; return null; }
}
function validateLocalFieldProfile(registry) {
  if (registry.profiles === undefined) return;
  exactFields(registry.profiles, ['field'], 'Local artwork profiles');
  const profile = registry.profiles.field; exactFields(profile, ['aliases','bindings'], 'Local field artwork profile');
  for (const collection of [profile.aliases, profile.bindings]) assert(collection && typeof collection === 'object' && !Array.isArray(collection), 'Invalid field artwork mappings');
  for (const [alias, key] of Object.entries(profile.aliases)) {
    validateKey(alias); validateKey(key);
    const target = own(registry.assets,key), base = own(registry.assets,alias);
    assert(target?.profile === 'field', 'Field alias points to another profile');
    assert(PUBLIC_KEY.test(alias) || base && base.kind !== 'app' || !base && target.kind !== 'app', 'Private field aliases require an opaque public key');
  }
  for (const [id,key] of Object.entries(profile.bindings)) { validateAppId(id); assert(own(registry.assets,validateKey(key))?.profile === 'field', 'Field binding points to another profile'); }
}
function syncFieldProfile(registry, history) {
  validateLocalFieldProfile(registry);
  const result = emptyFieldProfile(), local = registry.profiles?.field || {aliases:{},bindings:{}};
  for (const [key,asset] of Object.entries(history.assets)) if (asset.profile === 'field') {
    assert(own(registry.assets,key)?.profile === 'field', 'Field artwork definition changed profile');
    result.assets[asset.publicKey || key] = { ...displayAsset(asset), ...(asset.layout?.icon ? {icon:asset.layout.icon} : {}) };
  }
  for (const [alias,key] of Object.entries(local.aliases)) if (own(history.assets,key)) result.aliases[alias] = own(history.assets,key).publicKey || key;
  for (const [id,key] of Object.entries(local.bindings)) if (own(history.assets,key)) result.bindings[appArtBindingKey(id)] = own(history.assets,key).publicKey || key;
  return {...result, aliases:sorted(result.aliases), bindings:sorted(result.bindings), assets:sorted(result.assets)};
}
function profileBindings(registry, profile) {
  if (profile === undefined) return registry.bindings;
  assert(profile === 'field', 'Unknown artwork presentation profile');
  registry.profiles ||= { field: { aliases:{}, bindings:{} } };
  return registry.profiles.field.bindings;
}
// Compact framing is display metadata; adjusting it must not change generation jobs.
const specHash = spec => { const definition = validateDefinition(spec); delete definition.compactFocalPoint; delete definition.icon; return sha256(jsonBytes(definition)); };

function layoutIdentity(key, asset, compactFocalPoint, icon) {
  assert(icon === undefined || asset.profile === 'field' && FIELD_ART_ICONS.includes(icon),'Layout icon must be an approved field glyph');
  return { schemaVersion: 1, type: 'layout-metadata', key, artVersion: asset.version, sourceSha256: asset.original.sha256, generationProvenanceSha256: asset.provenance.sha256, compactFocalPoint, ...(icon ? {icon} : {}) };
}
async function applyCompactLayout(root, registry, history, key) {
  const asset = own(history.assets, key); if (!asset) return null;
  const spec = validateDefinition(own(registry.assets, key));
  const compactFocalPoint = spec.compactFocalPoint || { x: asset.focalPoint.x, y: asset.focalPoint.y };
  const icon = spec.icon;
  const current = asset.layout?.compactFocalPoint || asset.compactFocalPoint || asset.focalPoint;
  if (isDeepStrictEqual(current, compactFocalPoint) && asset.layout?.icon === icon) return null;
  const identity = layoutIdentity(key, asset, compactFocalPoint, icon), layoutHash = sha256(jsonBytes(identity));
  const path = asset.provenance.path.split('/').slice(0,-1).join('/') + `/layout.${layoutHash.slice(0,16)}.json`;
  let bytes;
  try {
    bytes = await readFile(privateFile(root, path));
    const existing = JSON.parse(bytes);
    assert(existing.layoutHash === layoutHash && isDeepStrictEqual(Object.fromEntries(Object.entries(existing).filter(([field]) => !['layoutHash','createdAt'].includes(field))), identity), 'Existing layout receipt differs');
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
    bytes = jsonBytes({ ...identity, layoutHash, createdAt: new Date().toISOString() });
    await immutable(root, path.split('/'), bytes);
  }
  asset.layout = { compactFocalPoint, ...(icon ? {icon} : {}), provenance: { path, sha256: sha256(bytes) } };
  return { key, version: asset.version, compactFocalPoint, ...(icon ? {icon} : {}) };
}

/** A trusted display glyph is layout metadata, never an inferred app identity. */
export async function configureFieldIcon(root = REPO_ROOT, {key,icon} = {}) {
  validateKey(key); assert(FIELD_ART_ICONS.includes(icon),'Field icon must be an approved local glyph');
  return withArtLock(root,async()=>{
    const {registry,history}=await state(root),spec=own(registry.assets,key),asset=own(history.assets,key);
    assert(spec?.profile==='field'&&asset?.profile==='field','A display icon requires accepted field artwork');
    await validateAcceptedAsset(root,key,asset,await loadSharp());
    registry.assets[key]=validateDefinition({...spec,icon});
    const changed=await applyCompactLayout(root,registry,history,key);
    await atomicJson(root,['scripts','app-art','registry.json'],registry);
    await atomicJson(root,[...HISTORY_PARTS,'index.json'],history);
    await writeDisplayManifests(root,syncManifest(registry,history),syncFieldProfile(registry,history));
    return {key,icon,version:asset.version,unchanged:!changed};
  });
}

export function buildPrompt(definition) {
  const spec = validateDefinition(definition);
  return [
    'Use case: stylized-concept.',
    spec.profile === 'field'
      ? 'Asset: text-free premium Soty field cover, square 1:1, at least 960 by 960 pixels.'
      : 'Asset: text-free premium Soty application cover, landscape 3:2.',
    `Purpose of the application: ${spec.purpose}. Convey this with the object and material, never with words.`,
    `Subject: ${spec.subject}`,
    `Style: extraordinary photoreal studio still-life; graphite backdrop, restrained contemporary material craft, soft large light, realistic highlights and shadows. Palette base ${spec.palette.base}, accent ${spec.palette.accent}.`,
    spec.profile === 'field'
      ? 'Composition: one compact object centered slightly above the middle, with generous background around its silhouette. Keep every important extremity inside the central 70% of the square. It must fit a rounded point-up hexagon crop; leave a quiet lower third for a separate real HTML title.'
      : 'Composition: subject centered in upper two thirds, calm lower quarter for a separate real HTML title. All objects remain readable with a center crop into a 4:3 card.',
    'Constraints: no text, digits, logos, watermarks, screenshot, UI, buttons, frame or canvas border. No decorative extra objects. Serious, beautiful, quiet and precise.',
  ].join('\n');
}

/** A catalog snapshot uses actual app ids; it does not create or publish applications. */
export function definitionsFromCatalog(catalog) {
  const apps = Array.isArray(catalog) ? catalog : catalog?.apps;
  assert(Array.isArray(apps) && apps.length <= 500, 'Catalog needs at most 500 app records');
  return apps.map(app => {
    assert(app && typeof app === 'object', 'Invalid app catalog record');
    const appId = validateAppId(app.appId || app.id);
    const art = app.art && typeof app.art === 'object' ? app.art : {};
    const purpose = art.purpose || app.description || `Работа с приложением ${text(app.name, 'name', 120)}`;
    return validateDefinition({
      ...art, appId, key: art.key || app.coverKey || `app-${sha256(appId).slice(0,16)}`, name: app.name,
      purpose, subject: art.subject || `One restrained sculptural glass object expressing this purpose: ${text(purpose, 'purpose')}. Use a simple coherent form with tactile charcoal and warm ivory materials.`,
      alt: art.alt || `Арт-обложка приложения ${app.name}`,
      palette: art.palette || { base: '#2C2C2A', accent: '#E8CFAB', ink: '#F5F0E9' },
      focalPoint: art.focalPoint || { x: 0.5, y: 0.42 }, kind: 'app',
    });
  });
}

export async function prepareArtwork(root = REPO_ROOT, { definitions = [], keys = [], all = false, refresh = false, prompt, appId, referenceImage } = {}) {
  return withArtLock(root, async () => {
    const { registry, history } = await state(root);
    for (const value of definitions) {
      const previous = own(registry.assets, value?.key);
      assert(!previous?.publicKey || value.publicKey === undefined || value.publicKey === previous.publicKey, 'The opaque public key is stable; use the existing key for new versions');
      const kind = value.kind || previous?.kind || 'app';
      const spec = validateDefinition({ ...value, kind, ...(kind === 'app' ? { publicKey: value.publicKey || previous?.publicKey || `art-${randomUUID().replaceAll('-', '')}` } : {}) });
      registry.assets[spec.key] = spec;
      if (spec.appId) profileBindings(registry, spec.profile)[spec.appId] = spec.key;
      keys.push(spec.key);
    }
    assertPublicIdentities(registry);
    const requested = [...new Set(all ? Object.keys(registry.assets) : keys)].map(validateKey);
    assert(requested.length > 0, 'Choose --key, --all, --definition or --catalog');
    assert(prompt === undefined || requested.length === 1, 'A prompt file belongs to exactly one cover key');
    const inputImages = [];
    if (referenceImage !== undefined) {
      assert(requested.length === 1 && typeof referenceImage === 'string' && PRIVATE_PATH.test(referenceImage) && /\/source\.[a-f0-9]{12}\.(?:png|jpg|webp)$/u.test(referenceImage), 'An edit reference must be one accepted local history source');
      const privateDirectory = referenceImage.split('/').slice(0,-1).join('/');
      const receipt = await readJson(privateFile(root, `${privateDirectory}/provenance.json`));
      const accepted = receiptAsset(receipt.asset, privateDirectory);
      assert(accepted.key === requested[0] && accepted.original.path === referenceImage, 'Edit reference does not belong to this accepted artwork');
      assertAcceptance(receipt, accepted);
      await verifyFile(root, accepted.original);
      inputImages.push({ path: referenceImage, sha256: accepted.original.sha256 });
    }
    if (appId !== undefined) {
      validateAppId(appId);
      assert(requested.length === 1 && own(registry.assets, requested[0]), '--app-id requires exactly one defined cover key');
      profileBindings(registry, own(registry.assets,requested[0]).profile)[appId] = requested[0];
    }
    const prepared = [], ready = [], layoutUpdated = [];
    for (const key of requested) {
      const spec = own(registry.assets, key);
      assert(spec, 'Unknown cover key; prepare a definition first');
      if (spec.kind === 'app' && !spec.publicKey) spec.publicKey = `art-${randomUUID().replaceAll('-', '')}`;
      const layoutChange = await applyCompactLayout(root, registry, history, key); if (layoutChange) layoutUpdated.push(layoutChange);
      const alreadyAccepted = own(history.assets, key)?.styleVersion === registry.styleVersion && own(history.assets, key)?.definitionHash === specHash(spec);
      const metadataOnly = definitions.some(value => value.key === key) && prompt === undefined && referenceImage === undefined;
      if (!refresh && alreadyAccepted && (all || metadataOnly)) { ready.push(key); continue; }
      const finalPrompt = prompt === undefined ? buildPrompt(spec) : prompt.trim().replaceAll('\r\n', '\n');
      assert(typeof finalPrompt === 'string' && finalPrompt.length > 0 && finalPrompt.length <= 6000 && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(finalPrompt), 'prompt: expected at most 6000 characters of plain text');
      // Multiline built-in prompts are immutable data, never executable command text.
      const digest = sha256(finalPrompt);
      const jobIdentity = sha256(jsonBytes({ key, definitionHash: specHash(spec), styleVersion: registry.styleVersion, promptHash: digest, ...(inputImages.length ? { inputImages } : {}) }));
      const jobPath = inside(root, 'scripts', 'app-art', 'jobs', key, `job-${jobIdentity.slice(0,16)}.json`);
      let job;
      try { job = await readJson(jobPath); }
      catch (error) {
        if (error.code !== 'ENOENT') throw error;
        job = { schemaVersion: 1, key, status: 'prepared', preparedAt: new Date().toISOString(), provider: 'codex-builtin-image-gen', definitionHash: specHash(spec), styleVersion: registry.styleVersion, prompt: finalPrompt, promptHash: digest, reference: spec.styleReference || registry.reference || null, ...(inputImages.length ? { inputImages } : {}), jobHash: jobIdentity };
        await immutable(root, ['scripts', 'app-art', 'jobs', key, basename(jobPath)], jsonBytes(job));
      }
      prepared.push({ key, publicKey: spec.publicKey || key, job: relative(root, jobPath).split(sep).join('/'), promptHash: digest });
    }
    await atomicJson(root, ['scripts', 'app-art', 'registry.json'], registry);
    if (layoutUpdated.length) await atomicJson(root, [...HISTORY_PARTS, 'index.json'], history);
    // New private names, purpose, alt, bindings and pending jobs stay entirely local.
    await writeDisplayManifests(root, syncManifest(registry, history), syncFieldProfile(registry, history));
    return { prepared, ready, layoutUpdated, generation: 'Use Codex built-in image_gen with each job.prompt; CLI does not generate images.' };
  });
}

async function loadSharp() {
  try { return (await import('sharp')).default; }
  catch { throw new Error('WebP import requires sharp: install the project devDependencies with pnpm install'); }
}
async function resolveJob(root, key, jobPath) {
  const directory = inside(root, 'scripts', 'app-art', 'jobs', key);
  if (jobPath) return readJson(inside(root, relative(root, resolve(root, jobPath))));
  const files = (await readdir(directory)).filter(name => /^job-[a-f0-9]{16}\.json$/u.test(name));
  const jobs = await Promise.all(files.map(name => readJson(join(directory, name))));
  assert(jobs.length > 0, 'Prepare the artwork job before import');
  jobs.sort((a, b) => String(b.preparedAt).localeCompare(String(a.preparedAt)));
  return jobs[0];
}
function assertJob(job, spec, registry) {
  assert(job.schemaVersion === 1 && job.key === spec.key && job.status === 'prepared' && job.provider === 'codex-builtin-image-gen', 'Invalid prepared artwork job');
  assert(job.definitionHash === specHash(spec) && job.styleVersion === registry.styleVersion, 'Artwork definition changed after preparation; prepare a fresh job');
  assert(typeof job.prompt === 'string' && job.prompt.length <= 6000 && HASH.test(job.promptHash) && sha256(job.prompt) === job.promptHash, 'Artwork prompt hash mismatch');
  const inputImages = job.inputImages || [];
  assert(Array.isArray(inputImages) && inputImages.length <= 1 && inputImages.every(item => PRIVATE_PATH.test(item?.path || '') && /\/source\./u.test(item.path) && HASH.test(item.sha256 || '')), 'Invalid local edit reference');
  assert(job.jobHash === sha256(jsonBytes({ key: job.key, definitionHash: job.definitionHash, styleVersion: job.styleVersion, promptHash: job.promptHash, ...(inputImages.length ? { inputImages } : {}) })), 'Artwork job identity mismatch');
}
async function nextVersion(root, key, publicKey) {
  let names = [];
  for (const parts of [[...HISTORY_PARTS, key], ['public', 'app-art', publicKey]]) {
    try { names.push(...await readdir(inside(root, ...parts))); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  return Math.max(0, ...names.map(name => Number(VERSION_DIR.exec(name)?.[1]) || 0)) + 1;
}

export async function importArtwork(root = REPO_ROOT, { key, source, job: jobPath, visualApproved = false, reviewer = 'local-review', generationId } = {}) {
  validateKey(key);
  assert(visualApproved === true, 'Import needs --visual-approved after checking no text/UI, subject and responsive crops');
  text(reviewer, 'reviewer', 80);
  assert(typeof source === 'string' && !source.includes('://') && source.length <= 4096, 'Source must be a local image path');
  return withArtLock(root, async () => {
    const { registry, history } = await state(root), spec = own(registry.assets, key);
    assert(spec, 'Unknown artwork key');
    assert(spec.kind !== 'app' || PUBLIC_KEY.test(spec.publicKey || ''), 'Prepare an opaque public key before accepting app artwork');
    const job = await resolveJob(root, key, jobPath); assertJob(job, spec, registry);
    for (const reference of job.inputImages || []) await verifyFile(root, reference);
    const input = resolve(root, source), info = await stat(input);
    assert(info.isFile() && info.size > 0 && info.size <= 32 * 1024 * 1024, 'Source must be a regular image smaller than 32 MiB');
    const original = await readFile(input), originalHash = sha256(original), current = own(history.assets, key);
    const isPng = original.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]));
    const isJpeg = original[0] === 255 && original[1] === 216 && original[2] === 255;
    const isWebp = original.subarray(0, 4).toString('ascii') === 'RIFF' && original.subarray(8, 12).toString('ascii') === 'WEBP';
    assert(isPng || isJpeg || isWebp, 'Source signature must be PNG, JPEG or WebP');
    if (current?.original?.sha256 === originalHash && current?.promptHash === job.promptHash && current?.definitionHash === job.definitionHash && current?.styleVersion === job.styleVersion) {
      await validateAcceptedAsset(root, key, current, await loadSharp());
      const acceptedReceipt = JSON.parse(await verifyFile(root, current.provenance));
      if (acceptedReceipt.job.jobHash === job.jobHash)
        return { key, version: current.version, unchanged: true, src: current.renditions.find(item => item.width === 640)?.url || current.renditions[0].url };
    }
    const sharp = await loadSharp();
    const metadata = await sharp(original, { limitInputPixels: 32_000_000, failOn: 'error' }).metadata();
    assert(['png', 'jpeg', 'webp'].includes(metadata.format) && (!metadata.pages || metadata.pages === 1), 'Only still PNG, JPEG and WebP images are accepted');
    if (spec.profile === 'field') assert(metadata.width >= 960 && metadata.height >= 960 && metadata.width / metadata.height >= .98 && metadata.width / metadata.height <= 1.02, 'Field cover must be square, at least 960 × 960');
    else assert(metadata.width >= 960 && metadata.height >= 640 && metadata.width / metadata.height >= 1.2 && metadata.width / metadata.height <= 2.0, 'Cover must be landscape, at least 960 × 640, aspect 1.2–2.0');
    assert(!metadata.orientation || metadata.orientation === 1, 'Normalize EXIF orientation before accepting artwork');
    const publicKey = spec.publicKey || key;
    const version = await nextVersion(root, key, publicKey), directory = `v${String(version).padStart(3, '0')}-${originalHash.slice(0,12)}`;
    const urlBase = `/app-art/${publicKey}/${directory}`, parts = ['public', 'app-art', publicKey, directory], privateParts = [...HISTORY_PARTS, key, directory];
    const recipe = spec.profile === 'field' ? FIELD_ART_RECIPE : ART_RECIPE;
    const renditions = [], outputs = [];
    for (const [index, desiredWidth] of recipe.widths.entries()) {
      const width = Math.min(desiredWidth, metadata.width);
      if (renditions.some(item => item.width === width)) continue;
      const { data, info: rendered } = await sharp(original, { limitInputPixels: 32_000_000, failOn: 'error' }).resize({ width, withoutEnlargement: true }).webp({ quality: recipe.quality, effort: recipe.effort }).toBuffer({ resolveWithObject: true });
      assert(data.length <= recipe.byteLimits[index], `WebP ${width}px exceeds size budget; simplify the image or update the reviewed recipe`);
      const hash = sha256(data), filename = `cover-${rendered.width}.${hash.slice(0,12)}.webp`;
      renditions.push({ url: `${urlBase}/${filename}`, width: rendered.width, height: rendered.height, bytes: data.length, sha256: hash });
      outputs.push([filename, data]);
    }
    const sourceName = `source.${originalHash.slice(0,12)}.${metadata.format === 'jpeg' ? 'jpg' : metadata.format}`;
    const asset = { key, publicKey, version, styleVersion: job.styleVersion, definitionHash: specHash(spec), promptHash: job.promptHash, alt: spec.alt, palette: spec.palette, focalPoint: spec.focalPoint, ...(spec.compactFocalPoint ? { compactFocalPoint: { x: spec.compactFocalPoint.x, y: spec.compactFocalPoint.y } } : {}), ...(spec.profile ? {profile:spec.profile} : {}), kind: spec.kind, original: { path: [...privateParts, sourceName].join('/'), width: metadata.width, height: metadata.height, bytes: original.length, sha256: originalHash }, renditions };
    const receipt = {
      schemaVersion: 1, acceptedAt: new Date().toISOString(),
      generation: { provider: 'codex-builtin-image-gen', outputBasename: basename(input), ...(generationId ? { id: text(generationId, 'generationId', 160) } : {}), inputImages: job.inputImages || [], referenceMode: job.inputImages?.length ? 'precise-object-edit' : 'described-from-approved-render' },
      job: { key, jobHash: job.jobHash, prompt: job.prompt, promptHash: job.promptHash, definitionHash: job.definitionHash, styleVersion: job.styleVersion, styleReference: job.reference },
      recipe: { ...recipe, sharp: sharp.versions.sharp, libvips: sharp.versions.vips },
      review: { reviewer, visualApproved: true, noTextOrUi: true, subjectApproved: true, cropApproved: true, automatedChecks: ['local-source', 'still-image', 'dimensions', 'aspect', 'WebP-size-budgets', 'SHA-256'] },
      asset,
    };
    const receiptBytes = jsonBytes(receipt);
    // Commit files before the pointer. Interrupted imports leave the old UI usable.
    await immutable(root, [...privateParts, sourceName], original);
    for (const [filename, bytes] of outputs) await immutable(root, [...parts, filename], bytes);
    await immutable(root, [...privateParts, 'provenance.json'], receiptBytes);
    history.assets[key] = { ...asset, provenance: { path: [...privateParts, 'provenance.json'].join('/'), sha256: sha256(receiptBytes) } };
    await applyCompactLayout(root,registry,history,key);
    await atomicJson(root, [...HISTORY_PARTS, 'index.json'], history);
    await writeDisplayManifests(root, syncManifest(registry, history), syncFieldProfile(registry, history));
    return { key, publicKey, version, unchanged: false, src: renditions.find(item => item.width === 640)?.url || renditions[0].url, renditionBytes: renditions.map(item => ({ width: item.width, bytes: item.bytes })), provenanceHash: sha256(receiptBytes) };
  });
}

export async function bindArtwork(root = REPO_ROOT, { appId, key, profile } = {}) {
  validateAppId(appId); validateKey(key);
  return withArtLock(root, async () => {
    const { registry, history } = await state(root); assert(own(registry.assets, key), 'Cannot bind an unknown cover key');
    assert(own(registry.assets,key).profile === profile, 'Binding profile must match artwork definition');
    profileBindings(registry,profile)[appId] = key;
    await atomicJson(root, ['scripts', 'app-art', 'registry.json'], registry);
    await writeDisplayManifests(root, syncManifest(registry, history), syncFieldProfile(registry, history));
    return { appId, coverKey: key, publicKey: own(registry.assets, key).publicKey || key, ...(profile ? {profile} : {}), status: own(history.assets, key) ? 'ready' : 'pending' };
  });
}
/** Only explicit approved public aliases; editable application names never bind. */
export async function aliasFieldArtwork(root = REPO_ROOT, { coverKey, key } = {}) {
  validateKey(coverKey); validateKey(key);
  return withArtLock(root,async()=>{
    const {registry,history}=await state(root),spec=own(registry.assets,key),base=own(registry.assets,coverKey);
    assert(spec?.profile==='field','A field alias requires a field artwork definition');
    const alias=base?.publicKey || coverKey;
    assert(PUBLIC_KEY.test(alias)||base&&base.kind!=='app'||!base&&spec.kind!=='app','Private field aliases require an opaque public key');
    profileBindings(registry,'field');registry.profiles.field.aliases[alias]=key;
    validateLocalFieldProfile(registry);
    await atomicJson(root,['scripts','app-art','registry.json'],registry);
    await writeDisplayManifests(root,syncManifest(registry,history),syncFieldProfile(registry,history));
    return {profile:'field',coverKey:alias,publicKey:spec.publicKey||key,status:own(history.assets,key)?'ready':'pending'};
  });
}
function assetFile(root, url) {
  assert(typeof url === 'string' && ART_PATH.test(url) && !url.includes('..'), 'Manifest contains a nonlocal artwork URL');
  return inside(root, 'public', url.slice(1));
}
function privateFile(root, path) {
  assert(typeof path === 'string' && PRIVATE_PATH.test(path), 'Invalid private artwork history path');
  return inside(root, ...path.split('/'));
}
function receiptAsset(value, privateDirectory) {
  const original = value.original.path ? value.original : { path: `${privateDirectory}/${basename(value.original.url)}`, ...Object.fromEntries(Object.entries(value.original).filter(([field]) => field !== 'url')) };
  return { ...value, publicKey: value.publicKey || value.key, original };
}
async function verifyFile(root, descriptor) {
  assert(HASH.test(descriptor?.sha256 || ''), 'Missing SHA-256 in artwork descriptor');
  const path = descriptor.path ? privateFile(root, descriptor.path) : assetFile(root, descriptor.url), real = await realpath(path);
  inside(descriptor.path ? inside(root, ...HISTORY_PARTS) : inside(root, 'public', 'app-art'), real);
  const bytes = await readFile(real);
  assert(sha256(bytes) === descriptor.sha256, `Artwork file hash mismatch: ${basename(path)}`);
  if (descriptor.bytes !== undefined) assert(bytes.length === descriptor.bytes, 'Artwork byte count mismatch');
  return bytes;
}
async function validatePublicTree(root) {
  let count = 0;
  const visit = async (path, depth = 0) => {
    assert(depth <= 3, 'Unexpected public artwork tree depth');
    for (const entry of await readdir(path, { withFileTypes: true })) {
      count++; assert(count <= 10000 && !entry.isSymbolicLink(), 'Unexpected public artwork tree entry');
      if (entry.isDirectory()) await visit(join(path, entry.name), depth + 1);
      else assert(entry.isFile() && (depth === 0 && ['manifest.json','field-profile.json'].includes(entry.name) || /^cover-\d{2,4}\.[a-f0-9]{12}\.webp$/u.test(entry.name)), 'Public artwork contains source, provenance or other private files');
    }
  };
  const publicRoot = await realpath(inside(root, 'public', 'app-art')); inside(root, publicRoot);
  await visit(publicRoot);
  return count;
}
function assertAcceptance(receipt, asset) {
  assert(receipt.review?.visualApproved === true && receipt.review.noTextOrUi === true && receipt.generation?.provider === 'codex-builtin-image-gen', 'Artwork has no recorded visual acceptance');
  assert(receipt.job?.promptHash === sha256(receipt.job?.prompt || '') && asset.promptHash === receipt.job.promptHash, 'Provenance prompt mismatch');
  assert(receipt.job.key === asset.key && receipt.job.definitionHash === asset.definitionHash && receipt.job.styleVersion === asset.styleVersion, 'Provenance definition mismatch');
  const inputImages = receipt.generation.inputImages || [];
  assert(Array.isArray(inputImages) && inputImages.length <= 1 && inputImages.every(item => PRIVATE_PATH.test(item?.path || '') && /\/source\./u.test(item.path) && HASH.test(item.sha256 || '')), 'Invalid local edit reference');
  assert(receipt.job.jobHash === sha256(jsonBytes({ key: asset.key, definitionHash: asset.definitionHash, styleVersion: asset.styleVersion, promptHash: asset.promptHash, ...(inputImages.length ? { inputImages } : {}) })), 'Provenance job identity mismatch');
}
async function validateAcceptedAsset(root, key, asset, sharp) {
  validateKey(key); assert(asset.key === key, 'Invalid accepted asset key');
  const receiptBytes = await verifyFile(root, asset.provenance);
  let receipt; try { receipt = JSON.parse(receiptBytes); } catch { throw new Error('Invalid artwork provenance JSON'); }
  assertAcceptance(receipt, asset);
  const privateDirectory = asset.provenance.path.split('/').slice(0, -1).join('/');
  assert(isDeepStrictEqual(receiptAsset(receipt.asset, privateDirectory), Object.fromEntries(Object.entries(asset).filter(([field]) => !['provenance','layout'].includes(field)))), 'Private history and immutable receipt disagree');
  if (asset.layout) {
    const layoutBytes = await verifyFile(root, asset.layout.provenance), layout = JSON.parse(layoutBytes);
    const identity = layoutIdentity(key, asset, asset.layout.compactFocalPoint, asset.layout.icon);
    assert(layout.layoutHash === sha256(jsonBytes(identity)) && isDeepStrictEqual(Object.fromEntries(Object.entries(layout).filter(([field]) => !['layoutHash','createdAt'].includes(field))), identity), 'Layout receipt does not match the selected artwork version');
  }
  const original = await verifyFile(root, asset.original), metadata = await sharp(original, { limitInputPixels: 32_000_000 }).metadata();
  assert(metadata.width === asset.original.width && metadata.height === asset.original.height, 'Source dimensions mismatch');
  const recipe = asset.profile === 'field' ? FIELD_ART_RECIPE : ART_RECIPE;
  assert(receipt.recipe?.version===recipe.version,'Artwork recipe does not match its presentation profile');
  let totalBytes = 0;
  for (const item of asset.renditions) {
    assert(item.url.startsWith(`/app-art/${asset.publicKey || key}/`), 'Rendition belongs to another key');
    const bytes = await verifyFile(root, item), rendered = await sharp(bytes).metadata();
    assert(rendered.format === 'webp' && rendered.width === item.width && rendered.height === item.height, 'WebP dimensions mismatch');
    assert(!rendered.exif && !rendered.icc && !rendered.xmp && !rendered.iptc, 'WebP rendition contains embedded metadata');
    const index = recipe.widths.findIndex(width => width >= item.width);
    assert(index >= 0 && bytes.length <= recipe.byteLimits[index], 'Rendition exceeds current byte budget');
    totalBytes += bytes.length;
  }
  assert(asset.renditions.length >= 3, 'Responsive renditions are incomplete');
  return { key, version: asset.version, renditions: asset.renditions.length, renditionBytes: totalBytes };
}
const exactFields = (object, fields, label) => {
  assert(object && typeof object === 'object' && !Array.isArray(object) && isDeepStrictEqual(Object.keys(object).sort(), [...fields].sort()), `${label} contains private or unexpected fields`);
};
async function validatePublicFieldProfile(root, sharp) {
  const profile = await optionalFieldProfile(root);
  const sourcePath = inside(root,'src','world','app-art-field-profile.json');
  if (!profile) {
    try { await readFile(sourcePath); throw new Error('Public field profile is missing'); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    return {checked:[],bindings:0,aliases:0};
  }
  exactFields(profile,['schemaVersion','profile','aliases','bindings','assets'],'Public field artwork profile');
  assert(profile.schemaVersion===1&&profile.profile==='field','Unsupported field artwork profile');
  for(const collection of [profile.assets,profile.aliases,profile.bindings]) assert(collection&&typeof collection==='object'&&!Array.isArray(collection),'Invalid field display collection');
  assert(Object.keys(profile.assets).length<=500&&Object.keys(profile.aliases).length<=500&&Object.keys(profile.bindings).length<=10000,'Public field profile is too large');
  const bytes=await readFile(inside(root,'public','app-art','field-profile.json'));
  assert(bytes.equals(await readFile(sourcePath)),'Source and public field profiles differ');
  const checked=[];
  for(const [key,asset]of Object.entries(profile.assets)){
    validateKey(key);exactFields(asset,['key','version','alt','palette','focalPoint','compactFocalPoint','renditions',...(Object.hasOwn(asset,'icon')?['icon']:[])],'Public field artwork asset');
    assert(!Object.hasOwn(asset,'icon')||FIELD_ART_ICONS.includes(asset.icon),'Invalid field display icon');
    assert(asset.key===key&&Number.isInteger(asset.version)&&asset.version>0,'Invalid field artwork identity');
    assert(typeof asset.alt==='string'&&asset.alt.length<=300&&!/[\u0000-\u001f\u007f]/u.test(asset.alt)&&(!PUBLIC_KEY.test(key)||asset.alt===''),'Invalid field artwork alt');
    exactFields(asset.palette,['base','accent','ink'],'Field display palette');
    assert(Object.values(asset.palette).every(v=>typeof v==='string'&&COLOR.test(v)),'Invalid field display palette');
    for(const point of [asset.focalPoint,asset.compactFocalPoint]){exactFields(point,['x','y'],'Field display geometry');assert(Object.values(point).every(v=>typeof v==='number'&&Number.isFinite(v)&&v>=0&&v<=1),'Invalid field display focal point');}
    assert(Array.isArray(asset.renditions)&&asset.renditions.length>=3&&asset.renditions.length<=4,'Field renditions are incomplete');
    let previous=0;
    for(const item of asset.renditions){
      exactFields(item,['url','width','height'],'Field display rendition');
      assert(typeof item.url==='string'&&item.url.startsWith(`/app-art/${key}/v${String(asset.version).padStart(3,'0')}-`)&&ART_PATH.test(item.url),'Field rendition belongs to another artwork');
      assert(Number.isInteger(item.width)&&item.width>previous&&Number.isInteger(item.height)&&item.height>0&&item.width/item.height>=.98&&item.width/item.height<=1.02,'Invalid square field rendition');previous=item.width;
      const rendered=await sharp(await readFile(assetFile(root,item.url))).metadata();
      assert(rendered.format==='webp'&&rendered.width===item.width&&rendered.height===item.height&&!rendered.exif&&!rendered.icc&&!rendered.xmp&&!rendered.iptc,'Invalid field WebP dimensions or metadata');
    }
    checked.push({key,version:asset.version,renditions:asset.renditions.length});
  }
  for(const [alias,key]of Object.entries(profile.aliases)){validateKey(alias);assert(typeof key==='string'&&own(profile.assets,key),'Invalid field public alias');}
  for(const [id,key]of Object.entries(profile.bindings)) assert(/^(?:notes|chess|bind-[a-f0-9]{64})$/u.test(id)&&typeof key==='string'&&own(profile.assets,key),'Invalid field public binding');
  return {checked,bindings:Object.keys(profile.bindings).length,aliases:Object.keys(profile.aliases).length,sha256:sha256(bytes)};
}
/** A cold checkout can verify shipped display assets without recovering private operator data. */
export async function validatePublicArtwork(root = REPO_ROOT) {
  const manifest = await readJson(inside(root, 'public', 'app-art', 'manifest.json'));
  exactFields(manifest, ['schemaVersion','bindings','assets'], 'Public artwork manifest');
  assert(manifest.schemaVersion === 1 && manifest.assets && typeof manifest.assets === 'object' && !Array.isArray(manifest.assets)
    && manifest.bindings && typeof manifest.bindings === 'object' && !Array.isArray(manifest.bindings), 'Unsupported public artwork manifest');
  const [publicBytes, sourceBytes] = await Promise.all([
    readFile(inside(root, 'public', 'app-art', 'manifest.json')),
    readFile(inside(root, 'src', 'world', 'app-art-manifest.json')),
  ]);
  assert(publicBytes.equals(sourceBytes), 'Source and public artwork manifests differ');
  assert(Object.keys(manifest.assets).length <= 500 && Object.keys(manifest.bindings).length <= 10000, 'Public artwork manifest is too large');
  const sharp = await loadSharp(), checked = [];
  for (const [key, asset] of Object.entries(manifest.assets)) {
    validateKey(key); exactFields(asset, ['key','version','alt','palette','focalPoint','compactFocalPoint','renditions'], 'Public artwork asset');
    assert(asset.key === key && Number.isInteger(asset.version) && asset.version > 0, 'Invalid public artwork identity');
    assert(typeof asset.alt === 'string' && asset.alt.length <= 300 && !/[\u0000-\u001f\u007f]/u.test(asset.alt) && (!PUBLIC_KEY.test(key) || asset.alt === ''), 'Invalid public artwork alt');
    exactFields(asset.palette, ['base','accent','ink'], 'Display palette');
    assert(Object.values(asset.palette).every(value => typeof value === 'string' && COLOR.test(value)), 'Invalid display palette');
    for (const point of [asset.focalPoint, asset.compactFocalPoint]) {
      exactFields(point, ['x','y'], 'Display geometry');
      assert(Object.values(point).every(value => typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1), 'Invalid display focal point');
    }
    assert(Array.isArray(asset.renditions) && asset.renditions.length >= 3 && asset.renditions.length <= 4, 'Responsive renditions are incomplete');
    let previous = 0;
    for (const item of asset.renditions) {
      exactFields(item, ['url','width','height'], 'Display rendition');
      assert(typeof item.url === 'string' && item.url.startsWith(`/app-art/${key}/v${String(asset.version).padStart(3,'0')}-`) && ART_PATH.test(item.url), 'Rendition belongs to another artwork version');
      assert(Number.isInteger(item.width) && item.width > previous && Number.isInteger(item.height) && item.height > 0 && item.width / item.height >= 1.2 && item.width / item.height <= 2.0, 'Invalid responsive dimensions');
      previous = item.width;
      const rendered = await sharp(await readFile(assetFile(root, item.url))).metadata();
      assert(rendered.format === 'webp' && rendered.width === item.width && rendered.height === item.height, 'WebP dimensions mismatch');
    }
    checked.push({ key, version: asset.version, renditions: asset.renditions.length });
  }
  for (const [id, key] of Object.entries(manifest.bindings)) assert(/^(?:notes|chess|bind-[a-f0-9]{64})$/u.test(id) && typeof key === 'string' && own(manifest.assets, key), 'Invalid public artwork binding');
  const fieldProfile = await validatePublicFieldProfile(root,sharp);
  await validatePublicTree(root);
  const publicRoot = inside(root, 'public', 'app-art');
  let filesChecked = 0;
  for (const name of await readdir(publicRoot, { recursive: true })) {
    if (!name.endsWith('.webp')) continue;
    const parts = name.split(/[\\/]/u);
    assert(parts.length === 3 && KEY.test(parts[0]) && VERSION_DIR.test(parts[1]), 'Unexpected public rendition path');
    const match = /^cover-(\d{2,4})\.([a-f0-9]{12})\.webp$/u.exec(parts[2]);
    assert(match, 'Invalid public rendition filename');
    const bytes = await readFile(assetFile(root, `/app-art/${parts.join('/')}`));
    assert(sha256(bytes).startsWith(match[2]), 'Public rendition filename hash mismatch');
    const rendered = await sharp(bytes).metadata(), width = Number(match[1]);
    assert(rendered.format === 'webp' && rendered.width === width && !rendered.exif && !rendered.icc && !rendered.xmp && !rendered.iptc, 'Invalid public WebP or embedded metadata');
    const recipe = rendered.width===rendered.height ? FIELD_ART_RECIPE : ART_RECIPE;
    const index = recipe.widths.findIndex(value => value >= width);
    assert(index >= 0 && bytes.length <= recipe.byteLimits[index], 'Rendition exceeds current byte budget');
    filesChecked++;
  }
  return { valid: true, scope: 'public-display-only', manifestSha256: sha256(publicBytes), checked, fieldProfile, filesChecked, operatorHistoryVerified: false };
}
export async function validateArtwork(root = REPO_ROOT) {
  const { registry, manifest, history } = await state(root), sharp = await loadSharp();
  assert(isDeepStrictEqual(manifest, syncManifest(registry, history)), 'Public artwork manifest has private or stale fields');
  const fieldProfile=await optionalFieldProfile(root);
  assert(!fieldProfile&&Object.values(history.assets).every(a=>a.profile!=='field')||isDeepStrictEqual(fieldProfile,syncFieldProfile(registry,history)),'Field artwork profile has private or stale fields');
  const [publicBytes, sourceBytes] = await Promise.all([
    readFile(inside(root, 'public', 'app-art', 'manifest.json')),
    readFile(inside(root, 'src', 'world', 'app-art-manifest.json')),
  ]);
  const manifestSha256 = sha256(publicBytes);
  assert(publicBytes.equals(sourceBytes) && manifestSha256 === sha256(sourceBytes), 'Source and public artwork manifests differ');
  await validatePublicTree(root);
  const checked = [];
  for (const [key, asset] of Object.entries(history.assets)) {
    validateKey(key); assert(asset.key === key && own(registry.assets, key), 'Invalid manifest asset key');
    checked.push(await validateAcceptedAsset(root, key, asset, sharp));
  }
  return { valid: true, manifestSha256, checked, pending: Object.keys(registry.assets).filter(key => !own(history.assets, key)) };
}

export async function rollbackArtwork(root = REPO_ROOT, { key, version } = {}) {
  validateKey(key); assert(Number.isInteger(version) && version > 0, 'Rollback needs a positive version number');
  return withArtLock(root, async () => {
    const { registry, history } = await state(root);
    const names = await readdir(inside(root, ...HISTORY_PARTS, key)), directory = names.find(name => Number(VERSION_DIR.exec(name)?.[1]) === version);
    assert(directory, 'Artwork version does not exist');
    const privateDirectory = [...HISTORY_PARTS, key, directory].join('/'), provenancePath = `${privateDirectory}/provenance.json`, receiptBytes = await readFile(privateFile(root, provenancePath));
    let receipt; try { receipt = JSON.parse(receiptBytes); } catch { throw new Error('Invalid rollback receipt'); }
    assert(receipt.asset?.key === key && receipt.asset.version === version && receipt.review?.visualApproved === true, 'Invalid rollback acceptance');
    const asset = receiptAsset(receipt.asset, privateDirectory);
    const accepted = { ...asset, provenance: { path: provenancePath, sha256: sha256(receiptBytes) } };
    await validateAcceptedAsset(root, key, accepted, await loadSharp());
    history.assets[key] = accepted;
    await applyCompactLayout(root, registry, history, key);
    await atomicJson(root, [...HISTORY_PARTS, 'index.json'], history);
    await writeDisplayManifests(root, syncManifest(registry, history), syncFieldProfile(registry, history));
    return { key, version, restored: true };
  });
}

export async function listArtwork(root = REPO_ROOT) {
  const { registry, history } = await state(root);
  return { styleVersion: registry.styleVersion, assets: Object.entries(registry.assets).map(([key, spec]) => ({ key, publicKey: spec.publicKey || key, kind: spec.kind, status: own(history.assets, key) ? 'ready' : 'pending', version: own(history.assets, key)?.version || null })), bindings: sorted(registry.bindings) };
}

/** Pending generation requests; accepted jobs stay immutable but leave this queue. */
export async function queueArtwork(root = REPO_ROOT, { includeAccepted = false } = {}) {
  const { registry, history } = await state(root), queue = [];
  for (const [key, spec] of Object.entries(registry.assets)) {
    const directory = inside(root, 'scripts', 'app-art', 'jobs', key);
    let names;
    try { names = await readdir(directory); } catch (error) { if (error.code !== 'ENOENT') throw error; names = []; }
    const jobs = [];
    for (const name of names.filter(value => /^job-[a-f0-9]{16}\.json$/u.test(value))) {
      const job = await readJson(join(directory, name));
      try { assertJob(job, spec, registry); jobs.push({ job, path: relative(root, join(directory, name)).split(sep).join('/') }); }
      catch { /* A stale job remains evidence; it cannot authorize a new import. */ }
    }
    jobs.sort((a, b) => String(b.job.preparedAt).localeCompare(String(a.job.preparedAt)));
    const latest = jobs[0], current = own(history.assets, key);
    const matchesMetadata = latest && current?.promptHash === latest.job.promptHash && current?.definitionHash === latest.job.definitionHash && current?.styleVersion === latest.job.styleVersion;
    const accepted = matchesMetadata && JSON.parse(await verifyFile(root, current.provenance)).job?.jobHash === latest.job.jobHash;
    const status = accepted ? 'accepted' : latest ? 'needs-generation' : 'needs-prepare';
    if (!accepted || includeAccepted) queue.push({ key, status, ...(latest ? { job: latest.path, promptHash: latest.job.promptHash } : {}), ...(accepted ? { version: current.version } : {}) });
  }
  return { queue, pending: queue.filter(item => item.status !== 'accepted').length, generation: 'Codex built-in image_gen; no automatic API calls.' };
}

/** Native same-workspace moves preserve existing URLs and exact accepted source bytes. */
export async function migrateArtworkHistory(root = REPO_ROOT) {
  return withArtLock(root, async () => {
    const { registry, manifest, history } = await state(root, { allowLegacy: true }), plans = [], assets = [];
    for (const [key, legacy] of Object.entries(manifest.assets)) {
      if (!legacy.original || !legacy.provenance?.url) continue;
      validateKey(key); assert(legacy.key === key && own(registry.assets, key), 'Unknown legacy artwork key');
      assert(legacy.kind !== 'app', 'Private app artwork needs a reviewed opaque public key; automatic legacy publication is disabled');
      const directory = legacy.provenance.url.split('/')[3];
      assert(VERSION_DIR.test(directory || '') && Number(VERSION_DIR.exec(directory)[1]) === legacy.version, 'Invalid legacy version directory');
      const privateDirectory = [...HISTORY_PARTS, key, directory].join('/');
      const receiptPath = `${privateDirectory}/provenance.json`, originalPath = `${privateDirectory}/${basename(legacy.original.url)}`;
      const movedOrPublic = async (descriptor, privatePath) => {
        try { return await verifyFile(root, descriptor); }
        catch (error) { if (error.code !== 'ENOENT') throw error; return verifyFile(root, { ...descriptor, path: privatePath }); }
      };
      const receiptBytes = await movedOrPublic(legacy.provenance, receiptPath);
      let receipt; try { receipt = JSON.parse(receiptBytes); } catch { throw new Error('Invalid legacy acceptance receipt'); }
      const rawAsset = Object.fromEntries(Object.entries(legacy).filter(([field]) => field !== 'provenance'));
      assert(isDeepStrictEqual(receipt.asset, rawAsset), 'Legacy manifest and accepted receipt disagree');
      await movedOrPublic(legacy.original, originalPath);
      for (const descriptor of legacy.renditions) await verifyFile(root, descriptor);
      for (const [descriptor, privatePath] of [[legacy.original, originalPath], [legacy.provenance, receiptPath]]) {
        const source = assetFile(root, descriptor.url), target = await safeOutputPath(root, ...privatePath.split('/'));
        inside(root, source); inside(root, target);
        // Verify every absolute target, parent and known filename before any move.
        privateFile(root, privatePath);
        assert(descriptor.url.startsWith(`/app-art/${key}/${directory}/`), 'Legacy source is outside its known artwork directory');
        plans.push({ source, target, sha256: descriptor.sha256 });
      }
      assets.push([key, { ...receiptAsset(receipt.asset, privateDirectory), provenance: { path: receiptPath, sha256: legacy.provenance.sha256 } }]);
    }
    let moved = 0;
    for (const plan of plans) {
      let sourceInfo;
      try { sourceInfo = await lstat(plan.source); }
      catch (error) { if (error.code !== 'ENOENT') throw error; assert(sha256(await readFile(plan.target)) === plan.sha256, 'Interrupted migration lost an accepted file'); continue; }
      assert(sourceInfo.isFile() && !sourceInfo.isSymbolicLink() && sourceInfo.nlink === 1, 'Legacy source is not a regular file');
      const actualSource = await realpath(plan.source); inside(inside(root, 'public', 'app-art'), actualSource);
      assert(sha256(await readFile(actualSource)) === plan.sha256, 'Legacy source changed before move');
      try { const existing = await lstat(plan.target); assert(existing.isFile() && !existing.isSymbolicLink() && sha256(await readFile(plan.target)) === plan.sha256, 'Private history destination already differs'); }
      catch (error) { if (error.code !== 'ENOENT') throw error; }
      await mkdir(dirname(plan.target), { recursive: true });
      await rename(actualSource, plan.target);
      moved++;
    }
    for (const [key, asset] of assets) history.assets[key] = asset;
    await atomicJson(root, [...HISTORY_PARTS, 'index.json'], history);
    await writeDisplayManifests(root, syncManifest(registry, history), syncFieldProfile(registry, history));
    return { moved, migratedAssets: assets.length, history: 'output/app-art-history', responsiveUrlsPreserved: true };
  });
}
