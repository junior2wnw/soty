import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import { parseReviewsContext, reviewSubjectKey } from './reviews-public.mjs';

const tick = () => new Promise(resolve => setImmediate(resolve));
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
class Element extends EventTarget {
  children = []; attributes = {}; dataset = {}; value = ''; hidden = false; disabled = false; open = false;
  constructor(tag, className = '', textContent = '') { super(); Object.assign(this, { tag, className, textContent }); }
  append(...nodes) { for (const node of nodes) { node.parent = this; this.children.push(node); } }
  replaceChildren(...nodes) { this.children = []; this.append(...nodes); }
  setAttribute(name, value) { this.attributes[name] = value; }
  showModal() { this.open = true; } close() { this.open = false; } focus() {}
  remove() { if (this.parent) this.parent.children = this.parent.children.filter(value => value !== this); }
  querySelectorAll(selector) { return all(this).filter(node => node !== this && node.tag === selector); }
}
function all(root) { return [root, ...root.children.flatMap(all)]; }
const binding = { subjectKind: 'app', providerRef: { id: 'povedai:fixture', version: 1, digest: 'a'.repeat(64) },
  subjectRef: { id: 'povedai:subject-app', version: 1, digest: 'b'.repeat(64) }, providerSubjectId: 'subject_' + '1'.repeat(20), providerEntityType: 'product',
  api: { subject: 'https://reviews.example/api/public/v1/subjects/subject_' + '1'.repeat(20),
    rating: 'https://reviews.example/api/public/v1/subjects/subject_' + '1'.repeat(20) + '/rating', reviews: 'https://reviews.example/api/public/v1/subjects/subject_' + '1'.repeat(20) + '/reviews' },
  publicPageUrl: 'https://reviews.example/subjects/subject_' + '1'.repeat(20) };
async function fixture(t, { collect, nativeContext, list } = {}) {
  const source = await readFile(new URL('./app-reviews.ts', import.meta.url), 'utf8');
  // Syntax seam only: use the production non-DEV branch in a Node VM.
  const code = ts.transpileModule(source.replace('import.meta.env.DEV', 'false'), { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  const module = { exports: {} }, host = new Element('host'), calls = []; let current = true, confirmedDiscard = false;
  const el = (tag, className, text) => new Element(tag, className, text);
  const ports = { './app-reviews.css': {}, './dom': { el, nounCount: count => String(count),
    button(label, _icon, className, callback) { const value = el('button', className, label); value.addEventListener('click', callback); return value; } },
    './reviews-public.mjs': { parseReviewsContext, reviewSubjectKey,
      async fetchPublicReviewJson(_binding, type) { if (type === 'reviews') return { items: [], nextCursor: null };
        return { title: 'Synthetic subject', rating: { count: 0, average: null, provenance: { kind: 'native' }, distributionComplete: true, updatedAt: null }, counts: { publishedReviews: 0, discussionItems: 0 } }; } } };
  vm.runInNewContext(code, { module, exports: module.exports, require: name => ports[name], AbortController, URL, crypto, window: { confirm: () => confirmedDiscard } });
  const api = { async request(method, args) {
    calls.push({ method, args });
    if (method === 'apps.reviews.context') return { mode: 'public-read', availability: 'unprobed', subjects: [binding] };
    if (method === 'apps.reviews.managed.context') return nativeContext ? nativeContext(args) : { available: true, subjectKind: binding.subjectKind,
      providerSubjectId: binding.providerSubjectId, providerRef: binding.providerRef, subjectRef: binding.subjectRef };
    if (method === 'apps.reviews.managed.collect') return collect ? collect(args) : { status: 'pending', reviewId: 'native-review', requestId: args.requestId };
    return list ? list(args) : { items: [], nextCursor: null };
  } };
  const handle = module.exports.mountAppReviews(host, { api, appId: 'app-one', title: 'Synthetic app', isCurrent: () => current, entry: () => ({ domainId: 'domain-one', path: '/' }) });
  t.after(() => handle.dispose());
  const button = label => all(host).find(value => value.tag === 'button' && value.textContent === label);
  const click = async label => { const control = button(label); assert.ok(control, 'actual control exists'); control.dispatchEvent(new Event('click')); await tick(); await tick(); };
  const draft = value => { const body = all(host).find(node => node.tag === 'textarea'); assert.ok(body); body.value = value; body.dispatchEvent(new Event('input')); return body; };
  const submit = async () => { all(host).find(node => node.tag === 'form').dispatchEvent(new Event('submit', { cancelable: true })); await tick(); await tick(); };
  const open = async () => { handle.open(); await tick(); await tick(); await click('Отзывы через вход Сот'); };
  return { handle, host, calls, draft, submit, open, click, button, discard() { confirmedDiscard = true; }, retire() { current = false; handle.dispose(); } };
}

test('Native-unavailable exposes the existing Source page and no fake working composer', async t => {
  const f = await fixture(t, { nativeContext: () => ({ available: false, code: 'managed_reviews_unavailable' }) }); await f.open();
  assert.equal(all(f.host).some(node => node.tag === 'textarea'), false);
  assert.equal(all(f.host).some(node => node.tag === 'a' && node.href === binding.publicPageUrl), true);
  assert.equal(f.calls.filter(value => value.method.includes('.collect')).length, 0);
});
test('unsaved Native draft stays on rejected dialog close; explicit ordinary discard releases it without a write', async t => {
  const f = await fixture(t); await f.open(); const body = f.draft('Synthetic draft');
  assert.equal(f.handle.hasUnsavedChanges(), true); await f.click('Закрыть');
  assert.equal(all(f.host).find(node => node.tag === 'dialog').open, true); assert.equal(body.value, 'Synthetic draft');
  f.discard(); await f.click('Закрыть'); assert.equal(f.handle.hasUnsavedChanges(), false);
  assert.equal(f.calls.filter(value => value.method.includes('.collect')).length, 0);
});
test('unknown Source ACK retains text and nonce; read-only list/text match cannot clear or repeat it', async t => {
  const f = await fixture(t, { collect() { throw new Error('Synthetic unknown ACK'); }, list: () => ({ items: [{ id: 'matching-text', body: 'Synthetic draft', status: 'pending' }], nextCursor: null }) });
  await f.open(); const body = f.draft('Synthetic draft'); await f.submit();
  const original = f.calls.find(value => value.method.includes('.collect'));
  assert.ok(original.args.requestId); assert.equal(body.value, 'Synthetic draft'); assert.equal(body.readOnly, true); assert.equal(f.handle.hasUnsavedChanges(), true);
  await f.click('Обновить записи'); await f.submit();
  assert.equal(f.calls.filter(value => value.method.includes('.collect')).length, 1); assert.equal(body.value, 'Synthetic draft'); assert.equal(f.handle.hasUnsavedChanges(), true);
});
test('only current matching pending ACK clears Native draft; a stale account cannot reuse or revive it', async t => {
  const gate = deferred(), f = await fixture(t, { collect: () => gate.promise }); await f.open();
  const body = f.draft('Synthetic draft'); await f.submit(); const request = f.calls.find(value => value.method.includes('.collect'));
  assert.equal(f.handle.hasUnsavedChanges(), true); f.retire();
  gate.resolve({ status: 'pending', reviewId: 'native-review', requestId: request.args.requestId }); await tick(); await tick();
  assert.equal(f.host.children.length, 0); assert.equal(f.calls.filter(value => value.method.includes('.collect')).length, 1);
});
test('confirmed pending ACK clears the draft, while foreign context reference never enables the form', async t => {
  const f = await fixture(t); await f.open(); f.draft('Synthetic draft'); await f.submit();
  assert.equal(f.handle.hasUnsavedChanges(), false); assert.equal(all(f.host).find(node => node.tag === 'textarea').value, '');
  const g = await fixture(t, { nativeContext: () => ({ available: true, subjectKind: binding.subjectKind, providerSubjectId: binding.providerSubjectId,
    providerRef: binding.providerRef, subjectRef: { ...binding.subjectRef, digest: 'f'.repeat(64) } }) }); await g.open();
  assert.equal(all(g.host).some(node => node.tag === 'textarea'), false);
});
test('current Source grant denial retires previously displayed private Native history', async t => {
  let reads = 0;
  const privateText = 'Synthetic private Native record';
  const f = await fixture(t, { list() {
    reads++;
    if (reads === 1) return { items: [{ id: 'private-history-one', body: privateText, status: 'pending' }], nextCursor: null };
    throw Object.assign(new Error('Synthetic current Source denial'), { code: 'managed_source_grant_required', status: 403 });
  } });
  await f.open(); await f.click('Обновить записи');
  assert.equal(all(f.host).some(node => node.textContent === privateText), true, 'control: permitted private history is actually rendered');
  await f.click('Обновить записи');
  assert.equal(all(f.host).some(node => node.textContent === privateText), false, 'current Native denial must retire earlier private rows');
  assert.equal(all(f.host).some(node => node.textContent === 'Недостаточно прав в Поведай. Проверьте доступ на странице Поведай.'), true);
  assert.equal(f.calls.filter(value => value.method.includes('.collect')).length, 0);
});
