import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import { resolveAppArt } from './app-art.mjs';
import { fieldEntityKey } from '../../modules/field/contract.mjs';

// Execute the actual node renderer and URL policy. These are DOM-port tests,
// not a claim that browser typography or the visual composition was reviewed.
const text = readFileSync(process.env.SOTY_FIELD_ART_SOURCE ?? new URL('./unified-field.ts', import.meta.url), 'utf8');
const parsed = ts.createSourceFile('unified-field.ts', text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const printer = ts.createPrinter(), print = node => printer.printNode(ts.EmitHint.Unspecified, node, parsed);
const factory = parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'createUnifiedField');
const renderNode = factory.body.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'renderNode');
const localImage = parsed.statements.find(node => ts.isVariableStatement(node)
  && node.declarationList.declarations.some(item => ts.isIdentifier(item.name) && item.name.text === 'localImage'));
assert.ok(renderNode && localImage, 'Current production renderer and local image policy must exist');
const code = ts.transpileModule([print(localImage), print(renderNode), 'exports.renderNode = renderNode;'].join('\n'), {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
}).outputText;

class Port {
  parentElement = null; children = []; dataset = {}; attributes = new Map(); listeners = new Map(); complete = false; naturalWidth = 0;
  style = { setProperty(name, value) { this[name] = value; } };
  constructor(tag = 'div', className = '', textContent = '') { Object.assign(this, { tag, className, textContent }); }
  get classList() {
    return { add: name => { const names = new Set(this.className.split(/\s+/)); names.add(name); this.className = [...names].join(' '); },
      remove: name => { this.className = this.className.split(/\s+/).filter(value => value !== name).join(' '); },
      contains: name => this.className.split(/\s+/).includes(name) };
  }
  append(...children) { for (const child of children) { child.parentElement = this; this.children.push(child); } }
  replaceChildren(...children) { for (const child of this.children) child.parentElement = null; this.children = []; this.append(...children); }
  remove() { if (this.parentElement) this.parentElement.children = this.parentElement.children.filter(child => child !== this); this.parentElement = null; }
  setAttribute(name, value) { this.attributes.set(name, value); }
  addEventListener(type, fn) { this.listeners.set(type, fn); }
  dispatch(type) { this.listeners.get(type)?.(); }
  querySelector(selector) {
    for (const child of this.children) {
      if (selector.startsWith('.') ? child.classList.contains(selector.slice(1)) : child.tag === selector) return child;
      const nested = child.querySelector(selector); if (nested) return nested;
    }
    return null;
  }
}
const art = (extra = {}) => ({ key: null, status: 'fallback', src: null, srcset: '', sizes: '208px',
  palette: { base: '#263728', accent: '#C9E7A5', ink: '#F5FFE9' }, focalPoint: { x: .25, y: .4 }, ...extra });
const entity = (id = 'app-unknown-001', title = 'Приложение без обложки', extra = {}) => ({ entity: { kind: 'app', id }, title, symbol: 'music', source: 'owner', ...extra });
function harness(resolveArt = () => art(), cachedImage) {
  const target = new Port('button');
  const env = { exports: {}, options: { resolveArt }, resolveAppArt, fieldEntityKey, metrics: { radius: 120 },
    document: { baseURI: 'https://soty.fixture.invalid/' }, location: { origin: 'https://soty.fixture.invalid' }, URL, queueMicrotask,
    el(tag, className, copy) { const node = new Port(tag, className, copy); if (tag === 'img' && cachedImage) Object.assign(node, cachedImage); return node; },
    icon(name, className) { const node = new Port('svg', className); node.symbol = name; node.setAttribute('aria-hidden', 'true'); return node; },
    hexFrame: () => new Port('svg', 'uf-hex-frame'),
  };
  vm.createContext(env); new vm.Script(code).runInContext(env);
  return { target, env, render: value => env.exports.renderNode(target, { entity: value }) };
}

test('an app with no image uses its resolver palette, a large existing symbol and its real caption', () => {
  const h = harness(); h.render(entity());
  assert.equal(h.target.style['--uf-art-base'], '#263728'); assert.equal(h.target.style['--uf-art-accent'], '#C9E7A5'); assert.equal(h.target.style['--uf-art-ink'], '#F5FFE9');
  assert.equal(h.target.querySelector('.uf-fallback-mark').symbol, 'music');
  assert.equal(h.target.querySelector('.uf-label-text').textContent, 'Приложение без обложки');
  assert.ok(h.target.querySelector('.uf-hex-frame')); assert.equal(h.target.querySelector('img'), null);
  assert.equal(h.target.dataset.artState, 'fallback'); assert.equal(h.target.classList.contains('has-art'), false);
});

test('renaming an unbound app cannot select an unrelated cover or change its deterministic identity palette', () => {
  const h = harness(() => null), record = entity(); h.render(record);
  const palette = ['base', 'accent', 'ink'].map(name => h.target.style[`--uf-art-${name}`]);
  h.render({ ...record, title: 'HIVE Canvas Шахматы Кадушка' });
  assert.deepEqual(['base', 'accent', 'ink'].map(name => h.target.style[`--uf-art-${name}`]), palette);
  assert.equal(h.target.querySelector('img'), null);
  assert.equal(h.target.querySelector('.uf-label-text').textContent, 'HIVE Canvas Шахматы Кадушка');
  assert.deepEqual(palette, Object.values(resolveAppArt({ appId: record.entity.id }).palette));
});

test('a valid existing cover keeps its source, focal point, frame and normal loaded-art state', () => {
  const source = '/app-art/hive/v001-0123456789ab/cover-640.0123456789ab.webp';
  const h = harness(() => art({ src: source, srcset: source + ' 640w', icon: 'brush' })); h.render(entity());
  const image = h.target.querySelector('img');
  assert.equal(image.src, 'https://soty.fixture.invalid' + source); assert.equal(image.srcset, source + ' 640w');
  assert.equal(image.style.objectPosition, '25% 40%'); assert.equal(h.target.dataset.artState, 'fallback');
  image.dispatch('load'); assert.equal(h.target.dataset.artState, 'ready'); assert.equal(h.target.classList.contains('has-art'), true);
  assert.ok(h.target.querySelector('.uf-hex-frame')); assert.equal(h.target.querySelector('.uf-fallback-mark').symbol, 'brush');
});

test('a failed or cached-broken image restores the same palette and symbol without an empty contour', async () => {
  for (const cached of [false, true]) {
    const h = harness(() => art({ src: '/missing-cover.webp' }), cached ? { complete: true, naturalWidth: 0 } : undefined); h.render(entity());
    const image = h.target.querySelector('img'); if (!cached) image.dispatch('error'); else await new Promise(resolve => queueMicrotask(resolve));
    assert.equal(h.target.dataset.artState, 'fallback'); assert.equal(h.target.classList.contains('has-art'), false);
    assert.equal(h.target.querySelector('img'), null); assert.equal(h.target.querySelector('.uf-fallback-mark').symbol, 'music');
    assert.equal(h.target.style['--uf-art-base'], '#263728');
  }
});

test('late load/error from a replaced cover cannot change the next rendered app appearance', () => {
  const h = harness(() => art({ src: '/first.webp' })); h.render(entity()); const old = h.target.querySelector('img');
  h.env.options.resolveArt = () => art({ src: '/second.webp' }); h.render(entity('app-other-002')); const current = h.target.querySelector('img');
  old.dispatch('load'); assert.equal(h.target.classList.contains('has-art'), false);
  current.dispatch('load'); assert.equal(h.target.classList.contains('has-art'), true);
  old.dispatch('error'); assert.equal(h.target.classList.contains('has-art'), true); assert.equal(h.target.querySelector('img'), current);
});

test('invalid CSS values, a remote cover or a resolver failure leave a safe deterministic fallback', () => {
  const record = entity(), fallback = resolveAppArt({ appId: record.entity.id });
  const h = harness(() => art({ src: 'https://wrong-app.invalid/unverified.webp', palette: { base: 'url(https://wrong.invalid)', accent: '#12345', ink: 'var(--private)' } }));
  h.render(record);
  for (const name of ['base', 'accent', 'ink']) assert.equal(h.target.style[`--uf-art-${name}`], fallback.palette[name]);
  assert.equal(h.target.querySelector('img'), null);
  h.env.options.resolveArt = () => { throw new Error('Unavailable art resolver'); }; h.render(record);
  assert.equal(h.target.style['--uf-art-base'], fallback.palette.base); assert.equal(h.target.querySelector('.uf-fallback-mark').symbol, 'music');
});

for (const kind of ['app', 'builtin', 'community']) test('the ' + kind + ' renderer adds one inert hit surface inside the same native button', () => {
  const h = harness(), record = entity('hit-fixture', 'Проверка названия', { entity: { kind, id: 'hit-fixture' } });
  h.render(record);
  const hit = h.target.querySelector('.uf-hit-shape');
  assert.ok(hit); assert.equal(hit.tag, 'span'); assert.equal(hit.attributes.get('aria-hidden'), 'true');
  assert.equal(hit.parentElement, h.target); assert.equal(hit.children.length, 0); assert.equal(hit.listeners.size, 0);
  assert.equal(h.target.querySelector('.uf-label-text').textContent, 'Проверка названия');
  h.render(record);
  assert.equal(h.target.children.filter(child => child.classList.contains('uf-hit-shape')).length, 1);
});


test('device renderer preserves the full name and symbol and adds exactly one inert hex hit child', () => {
  let artCalls = 0; const h = harness(() => { artCalls++; return art(); });
  const title = 'Мой ноутбук · длинное проверочное название устройства';
  const record = entity('device-caption-fixture', title, { entity: { kind: 'device', id: 'device-caption-fixture' }, symbol: 'desktop' });
  h.render(record);
  const hit = h.target.querySelector('.uf-hit-shape'), inner = h.target.querySelector('.uf-device-inner');
  assert.equal(h.target.tag, 'button'); assert.equal(h.target.dataset.entityId, 'device:device-caption-fixture');
  assert.equal(h.target.querySelector('.uf-label-text').textContent, title);
  assert.equal(h.target.querySelector('.uf-label').parentElement, inner);
  assert.equal(h.target.querySelector('.uf-device-icon').symbol, 'desktop');
  assert.equal(h.target.querySelector('img'), null); assert.equal(artCalls, 0);
  assert.ok(h.target.querySelector('.uf-hex-frame'));
  assert.ok(hit); assert.equal(hit.tag, 'span'); assert.equal(hit.attributes.get('aria-hidden'), 'true');
  assert.equal(hit.parentElement, h.target); assert.equal(hit.children.length, 0); assert.equal(hit.listeners.size, 0);
  assert.deepEqual(h.target.children.map(child => child.className), ['uf-device-inner', 'uf-hex-frame', 'uf-hit-shape']);
});

for (const online of [true, false]) test('device ' + (online ? 'online' : 'offline') + ' witness keeps the real forty-five-second boundary', () => {
  const h = harness(); h.env.Date = { now: () => 50_000 };
  h.render(entity('device-fresh', 'Мой ноутбук', { entity: { kind: 'device', id: 'device-fresh' }, updatedAt: 5_000, online }));
  const status = h.target.querySelector('.uf-status'); assert.ok(status);
  assert.equal(status.classList.contains(online ? 'is-online' : 'is-offline'), true);
  assert.equal(status.attributes.get('aria-label'), online ? 'Устройство в сети' : 'Устройство не в сети');
  assert.equal(status.parentElement, h.target.querySelector('.uf-device-inner'));
  assert.equal(h.target.children.filter(child => child.classList.contains('uf-hit-shape')).length, 1);
});

test('stale, future, absent or unknown device observations do not invent presence after the paint split', () => {
  for (const observation of [{ updatedAt: 4_999, online: true }, { updatedAt: 50_001, online: true }, { online: false }, { updatedAt: 50_000 }]) {
    const h = harness(); h.env.Date = { now: () => 50_000 };
    h.render(entity('device-unknown', 'Ноутбук', { entity: { kind: 'device', id: 'device-unknown' }, ...observation }));
    assert.equal(h.target.querySelector('.uf-status'), null);
    assert.equal(h.target.querySelector('.uf-label-text').textContent, 'Ноутбук');
    assert.equal(h.target.children.filter(child => child.classList.contains('uf-hit-shape')).length, 1);
  }
});

test('device rerender retires the previous hit and presence without duplicating the next button surface', () => {
  const h = harness(); h.env.Date = { now: () => 50_000 };
  h.render(entity('device-rerender', 'Первое имя', { entity: { kind: 'device', id: 'device-rerender' }, updatedAt: 50_000, online: true }));
  const oldHit = h.target.querySelector('.uf-hit-shape'), oldInner = h.target.querySelector('.uf-device-inner');
  assert.ok(oldHit); assert.ok(oldInner);
  h.render(entity('device-rerender', 'Новое полное имя', { entity: { kind: 'device', id: 'device-rerender' } }));
  assert.equal(oldHit.parentElement, null); assert.equal(oldInner.parentElement, null);
  assert.equal(h.target.querySelector('.uf-status'), null);
  assert.equal(h.target.querySelector('.uf-label-text').textContent, 'Новое полное имя');
  assert.notEqual(h.target.querySelector('.uf-hit-shape'), oldHit);
  assert.equal(h.target.children.filter(child => child.classList.contains('uf-hit-shape')).length, 1);
  h.render(entity());
  assert.equal(h.target.querySelector('.uf-device-inner'), null);
  assert.equal(h.target.querySelector('.uf-fallback-mark').symbol, 'music');
  assert.equal(h.target.children.filter(child => child.classList.contains('uf-hit-shape')).length, 1);
});
