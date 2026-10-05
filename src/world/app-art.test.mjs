import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { appArtBindingKey } from './app-art-identity.mjs';
import { resolveAppArt, createAppArtResolver, installAppArtFallback } from './app-art.mjs';

const local = '/app-art/notes/v001-59804a3b2eba/cover-640.03021a91e7f1.webp';
const valid = { schemaVersion: 1, bindings: { [appArtBindingKey('app-00000000000000000000000000000000')]: 'notes' }, assets: { notes: { key: 'notes', version: 1, alt: 'Светлая бумага', palette: { base: '#413B33', accent: '#E8CFAB', ink: '#F5F0E9' }, focalPoint: { x: 0.5, y: 0.4 }, renditions: [{ url: local, width: 640, height: 427 }] } } };

test('browser binding hash matches independent native SHA-256 and rejects malformed ids', () => {
  for (const appId of ['app-00000000000000000000000000000000', ...Array.from({ length: 50 }, () => `app-${randomBytes(16).toString('hex')}`)]) {
    assert.equal(appArtBindingKey(appId), 'bind-' + createHash('sha256').update('soty-app-art-binding-v1:' + appId).digest('hex'));
  }
  for (const appId of ['../notes', 'https://tracker.invalid/', '', 'app-' + 'a'.repeat(100000)]) assert.equal(appArtBindingKey(appId), null);
});

test('all shipped covers have responsive local artwork; records need explicit ids or keys', () => {
  for (const key of ['notes', 'chess', 'tavysh', 'hive', 'focus', 'pulse']) {
    const art = resolveAppArt(key);
    assert.equal(art.status, 'ready');
    assert.match(art.src, new RegExp(`^/app-art/${key}/v\\d{3}-[a-f0-9]{12}/cover-640\\.[a-f0-9]{12}\\.webp$`, 'u'));
    assert.equal(art.srcset.split(', ').length, 4);
    assert.ok(Object.isFrozen(art));
  }
  assert.equal(resolveAppArt({ name: 'HIVE' }).status, 'fallback');
  assert.equal(resolveAppArt({ appId: 'notes' }).key, 'notes');
});

test('stable-id mapping survives a changed app name and ignores unrelated artwork URLs', () => {
  const resolve = createAppArtResolver(valid);
  assert.equal(resolve({ appId: 'app-00000000000000000000000000000000', name: 'Переименовано' }).src, local);
  assert.equal(resolve({ coverKey: 'notes', coverUrl: 'https://tracker.invalid/collect' }).src, local);
  assert.equal(resolve({ name: 'notes' }).src, null);
  for (const value of ['https://tracker.invalid/x', '../notes', '" onerror="alert(1)', '__proto__']) assert.equal(resolve(value).status, 'fallback');
});

test('malformed/remote/traversal rendition metadata cannot become src or srcset', () => {
  for (const url of ['https://tracker.invalid/x.webp', '//tracker.invalid/x.webp', '/app-art/notes/../../collect.webp', local + '?id=tracking', local.replace('/notes/', '/hive/')]) {
    const source = structuredClone(valid);
    source.assets.notes.renditions[0].url = url;
    const art = createAppArtResolver(source)('notes');
    assert.equal(art.src, null);
    assert.equal(art.srcset, '');
  }
  const inherited = Object.create({ notes: valid.assets.notes });
  assert.equal(createAppArtResolver({ assets: inherited, bindings: {} })('notes').src, null);
});

test('missing cover retains deterministic generic palette without exposing private pending metadata', () => {
  const resolve = createAppArtResolver({ assets: {}, bindings: {}, pending: { waiting: { palette: { base: 'url(https://tracker.invalid)', accent: '#DDAABB', ink: null }, focalPoint: { x: -1, y: NaN }, alt: 'PRIVATE_PENDING_ALT' } } });
  const first = resolve({ appId: 'app-00000000000000000000000000000000' });
  assert.equal(first.status, 'fallback');
  assert.equal(first.alt, '');
  assert.match(first.palette.base, /^#[0-9A-F]{6}$/iu);
  assert.match(first.palette.accent, /^#[0-9A-F]{6}$/iu);
  assert.deepEqual(first.focalPoint, { x: 0.5, y: 0.5 });
  assert.deepEqual(first, resolve({ appId: 'app-00000000000000000000000000000000' }));
});

test('compact framing defaults to base framing, supports an override and clamps untrusted geometry', () => {
  const source = structuredClone(valid), resolve = createAppArtResolver(source);
  assert.deepEqual(resolve('notes').compactFocalPoint, resolve('notes').focalPoint);
  source.assets.notes.compactFocalPoint = { x: 0.745, y: 0.45, privateCaption: 'PRIVATE_COMPACT_LABEL' };
  assert.deepEqual(resolve('notes').compactFocalPoint, { x: 0.745, y: 0.45 });
  assert.ok(Object.isFrozen(resolve('notes').compactFocalPoint));
  source.assets.notes.compactFocalPoint = { x: 9, y: -3 };
  assert.deepEqual(resolve('notes').compactFocalPoint, { x: 1, y: 0 });
  source.assets.notes.compactFocalPoint = { x: 'url(https://tracker.invalid)', y: NaN };
  assert.deepEqual(resolve('notes').compactFocalPoint, { x: 0.5, y: 0.4 });
  assert.deepEqual(resolve(null).compactFocalPoint, resolve(null).focalPoint);
});

class FakeImage extends EventTarget {
  constructor() { super(); this.attributes = new Map([['src', local], ['srcset', local + ' 640w']]); this.dataset = {}; this.hidden = false; this.complete = false; this.naturalWidth = 640; }
  hasAttribute(name) { return this.attributes.has(name); }
  removeAttribute(name) { this.attributes.delete(name); }
}
test('broken cover hides only the image and removes tracking-capable retry sources once', () => {
  const image = new FakeImage(); let fallbacks = 0;
  installAppArtFallback(image, () => fallbacks++);
  image.dispatchEvent(new Event('error')); image.dispatchEvent(new Event('error'));
  assert.equal(image.hidden, true); assert.equal(image.dataset.artState, 'fallback');
  assert.equal(image.attributes.size, 0); assert.equal(fallbacks, 1);
});
test('disposed fallback listener leaves later renders alone; already failed images are handled', async () => {
  const disposed = new FakeImage(), cleanup = installAppArtFallback(disposed);
  cleanup(); disposed.dispatchEvent(new Event('error')); assert.equal(disposed.hidden, false);
  const broken = new FakeImage(); broken.complete = true; broken.naturalWidth = 0;
  installAppArtFallback(broken); await Promise.resolve(); assert.equal(broken.hidden, true);
});
