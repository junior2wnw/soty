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
async function fixture(t, { collect, nativeContext, list, subjects = [binding] } = {}) {
  const source = await readFile(process.env.SOTY_APP_REVIEWS_SOURCE ?? new URL('./app-reviews.ts', import.meta.url), 'utf8');
  // Syntax seam only: use the production non-DEV branch in a Node VM.
  const code = ts.transpileModule(source.replace('import.meta.env.DEV', 'false'), { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  const module = { exports: {} }, host = new Element('host'), calls = []; let current = true, confirmedDiscard = false, entry = { domainId: 'domain-one', path: '/' };
  const el = (tag, className, text) => new Element(tag, className, text);
  const ports = { './app-reviews.css': {}, './dom': { el, nounCount: count => String(count),
    button(label, _icon, className, callback) { const value = el('button', className, label); value.addEventListener('click', callback); return value; } },
    './reviews-public.mjs': { parseReviewsContext, reviewSubjectKey,
      async fetchPublicReviewJson(_binding, type) { if (type === 'reviews') return { items: [], nextCursor: null };
        return { title: 'Synthetic subject', rating: { count: 0, average: null, provenance: { kind: 'native' }, distributionComplete: true, updatedAt: null }, counts: { publishedReviews: 0, discussionItems: 0 } }; } } };
  vm.runInNewContext(code, { module, exports: module.exports, require: name => ports[name], AbortController, URL, crypto, window: { confirm: () => confirmedDiscard } });
  const api = { async request(method, args) {
    calls.push({ method, args });
    if (method === 'apps.reviews.context') return { mode: 'public-read', availability: 'unprobed', subjects };
    const selected = subjects.find(value => value.subjectKind === args.subjectKind) ?? binding;
    if (method === 'apps.reviews.managed.context') return nativeContext ? nativeContext(args) : { available: true, subjectKind: selected.subjectKind,
      providerSubjectId: selected.providerSubjectId, providerRef: selected.providerRef, subjectRef: selected.subjectRef };
    if (method === 'apps.reviews.managed.collect') return collect ? collect(args) : { status: 'pending', reviewId: 'native-review', requestId: args.requestId };
    return list ? list(args) : { items: [], nextCursor: null };
  } };
  const handle = module.exports.mountAppReviews(host, { api, appId: 'app-one', title: 'Synthetic app', isCurrent: () => current, entry: () => entry });
  t.after(() => handle.dispose());
  const button = label => all(host).find(value => value.tag === 'button' && value.textContent === label);
  const click = async (label, index = 0) => { const control = all(host).filter(value => value.tag === 'button' && value.textContent === label)[index]; assert.ok(control, 'actual control exists'); control.dispatchEvent(new Event('click')); await tick(); await tick(); };
  const draft = value => { const body = all(host).find(node => node.tag === 'textarea'); assert.ok(body); body.value = value; body.dispatchEvent(new Event('input')); return body; };
  const submit = async () => { all(host).find(node => node.tag === 'form').dispatchEvent(new Event('submit', { cancelable: true })); await tick(); await tick(); };
  const open = async () => { handle.open(); await tick(); await tick(); await click('Отзывы через вход Сот'); };
  return { handle, host, calls, draft, submit, open, click, button, discard() { confirmedDiscard = true; }, moveEntry() { entry = { domainId: 'domain-two', path: '/another' }; }, retire() { current = false; handle.dispose(); } };
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

const approved = (id = 'approved-parent', body = 'Synthetic approved parent', depth = 0) => ({ id, body, status: 'approved', depth });
const collectCalls = f => f.calls.filter(value => value.method === 'apps.reviews.managed.collect');

test('ordinary approved-row reply sends exactly its current parent and selector; old detached form cannot dispatch', async t => {
  const f = await fixture(t, { list: () => ({ items: [approved()], nextCursor: null }) }); await f.open();
  const oldForm = all(f.host).find(node => node.tag === 'form'); f.draft('Held root draft');
  await f.click('Обновить записи'); await f.click('Ответить'); f.draft('Synthetic reply');
  assert.ok(all(f.host).some(node => node.textContent === 'Приложение · Ответ выбранной записи: Synthetic approved parent'));
  oldForm.dispatchEvent(new Event('submit', { cancelable: true })); await tick(); await tick();
  assert.equal(collectCalls(f).length, 0, 'retained obsolete UI cannot submit the previous root composer');
  await f.submit(); const request = collectCalls(f)[0];
  assert.equal(collectCalls(f).length, 1); assert.equal(request.args.parentId, 'approved-parent');
  assert.equal(request.args.subjectKind, 'app'); assert.deepEqual(request.args.subjectRef, binding.subjectRef);
  assert.equal(request.args.body, 'Synthetic reply'); assert.ok(request.args.requestId);
  await f.click('Вернуться к отзыву'); assert.equal(all(f.host).find(node => node.tag === 'textarea').value, 'Held root draft');
});

test('pending, hidden, invalid-depth and maximum-depth rows never offer a reply override', async t => {
  const rows = [{ id: 'pending-parent', body: 'Pending source row', status: 'pending', depth: 0 },
    { ...approved('deep-parent'), depth: 64 }, { ...approved('invalid-parent'), depth: -1 },
    { ...approved('unknown-depth'), depth: undefined }, { ...approved('deleted-parent'), status: 'deleted' },
    { ...approved('spam-parent'), status: 'spam' }, { ...approved('rejected-parent'), status: 'rejected' }];
  const f = await fixture(t, { list: () => ({ items: rows, nextCursor: null }) }); await f.open(); await f.click('Обновить записи');
  assert.equal(f.button('Ответить'), undefined); assert.ok(all(f.host).some(node => node.textContent === 'Ответ после проверки'));
  assert.equal(collectCalls(f).length, 0);
});

test('reply cancellation retains its draft and nonce; another parent cannot silently replace it', async t => {
  const f = await fixture(t, { collect: () => { throw new Error('Synthetic unknown reply ACK'); },
    list: () => ({ items: [approved(), approved('second-parent')], nextCursor: null }) });
  await f.open(); await f.click('Обновить записи'); await f.click('Ответить'); f.draft('Unknown reply'); await f.submit();
  const request = collectCalls(f)[0]; await f.click('Вернуться к отзыву');
  await f.click('Обновить записи'); await f.click('Ответить', 1);
  assert.ok(all(f.host).some(node => node.textContent === 'Сначала сохраните или удалите прежний черновик ответа этому объекту.'));
  await f.click('Вернуться к черновику ответа'); const body = all(f.host).find(node => node.tag === 'textarea');
  assert.equal(body.value, 'Unknown reply'); assert.equal(body.readOnly, true); await f.submit();
  assert.equal(collectCalls(f).length, 1); assert.equal(request.args.parentId, 'approved-parent'); assert.ok(request.args.requestId);
});

test('current parent withdrawal retires reply eligibility without discarding typed text or attempting a write', async t => {
  let reads = 0;
  const f = await fixture(t, { list: () => ({ items: reads++ === 0 ? [approved()] : [], nextCursor: null }) });
  await f.open(); await f.click('Обновить записи'); await f.click('Ответить'); const body = f.draft('Keep reply draft');
  await f.click('Обновить записи'); await f.submit();
  assert.equal(body.value, 'Keep reply draft'); assert.equal(f.button('Отправить ответ на проверку').disabled, true);
  assert.equal(all(f.host).some(node => node.textContent.includes('Synthetic approved parent')), false, 'a withdrawn row is not retained as composer context');
  assert.equal(collectCalls(f).length, 0);
});

test('unique managed-collect pre-effect proof_missing retains an editable draft and performs no automatic resend', async t => {
  let denied = true;
  const f = await fixture(t, { collect(args) { if (denied) throw Object.assign(new Error('Synthetic resolver denial'), { code: 'managed_source_app_native_proof_missing' });
    return { status: 'pending', reviewId: 'new-confirmed-review', requestId: args.requestId }; } });
  await f.open(); const body = f.draft('Known undispatched draft'); await f.submit();
  assert.equal(body.value, 'Known undispatched draft'); assert.equal(body.readOnly, false);
  assert.ok(all(f.host).some(node => node.textContent.startsWith('Запись не отправлена:')));
  denied = false; await f.click('Проверить доступ'); await f.click('Обновить записи');
  assert.equal(collectCalls(f).length, 1, 'fresh context/list never dispatches the prior intent');
  await f.submit(); assert.equal(collectCalls(f).length, 2, 'a separate deliberate ordinary submit is required');
  assert.equal(all(f.host).find(node => node.tag === 'textarea').value, '');
});

test('post-effect/fence errors and hostile diagnostics stay unknown, even after a fresh permitted context', async t => {
  for (const error of [Object.assign(new Error('Synthetic late Root fence'), { code: 'managed_reviews_authority_changed' }),
    new Proxy({}, { getOwnPropertyDescriptor() { throw new Error('Synthetic diagnostic trap'); } })]) {
    const f = await fixture(t, { collect: () => { throw error; } }); await f.open(); f.draft('Keep unknown scope'); await f.submit();
    await f.click('Проверить доступ'); await f.submit();
    assert.equal(all(f.host).find(node => node.tag === 'textarea').readOnly, true); assert.equal(collectCalls(f).length, 1);
  }
});

test('explicit local discard after unknown needs confirmation and never deletes or resends a Native operation', async t => {
  const f = await fixture(t, { collect: () => { throw new Error('Synthetic unknown ACK'); } }); await f.open();
  const body = f.draft('Retain until explicit discard'); await f.submit(); await f.click('Удалить черновик');
  assert.equal(body.value, 'Retain until explicit discard'); assert.equal(body.readOnly, true);
  f.discard(); await f.click('Удалить черновик'); assert.equal(body.value, ''); assert.equal(body.readOnly, false);
  assert.equal(f.handle.hasUnsavedChanges(), false); assert.equal(collectCalls(f).length, 1);
});

test('a changed entry cannot inherit, erase or resend a retained unknown draft', async t => {
  const f = await fixture(t, { collect: () => { throw new Error('Synthetic unknown ACK'); } }); await f.open(); f.draft('Old entry draft'); await f.submit();
  f.moveEntry(); f.handle.open(); await tick(); await tick(); await f.click('Отзывы через вход Сот');
  assert.equal(all(f.host).some(node => node.tag === 'textarea'), false);
  assert.ok(all(f.host).some(node => node.textContent === 'Сначала сохраните или закройте прежний черновик.'));
  assert.equal(f.handle.hasUnsavedChanges(), true); assert.equal(collectCalls(f).length, 1);
});

test('all three subjects isolate root/reply drafts and selectors even when returned parent text/id coincide', async t => {
  const subjects = ['app', 'project', 'person'].map((kind, index) => {
    const providerSubjectId = 'subject_' + String(index + 1).repeat(20);
    return { ...binding, subjectKind: kind, providerEntityType: { app: 'product', project: 'project', person: 'profile' }[kind],
      subjectRef: { ...binding.subjectRef, id: 'povedai:subject-' + kind }, providerSubjectId,
      api: { subject: 'https://reviews.example/api/public/v1/subjects/' + providerSubjectId,
        rating: 'https://reviews.example/api/public/v1/subjects/' + providerSubjectId + '/rating', reviews: 'https://reviews.example/api/public/v1/subjects/' + providerSubjectId + '/reviews' },
      publicPageUrl: 'https://reviews.example/subjects/' + providerSubjectId };
  });
  const f = await fixture(t, { subjects, list: () => ({ items: [approved('shared-parent')], nextCursor: null }) });
  await f.open(); f.draft('Private local app root'); await f.click('Обновить записи'); await f.click('Ответить'); f.draft('Private local app reply');
  for (const [label, kind] of [['Проект', 'project'], ['Человек', 'person']]) {
    await f.click(label); await f.click('Отзывы через вход Сот');
    assert.equal(all(f.host).find(node => node.tag === 'textarea').value, '');
    await f.click('Обновить записи'); await f.click('Ответить');
    assert.equal(all(f.host).find(node => node.tag === 'textarea').value, ''); f.draft('Synthetic ' + kind + ' reply'); await f.submit();
    const request = collectCalls(f).at(-1); assert.equal(request.args.subjectKind, kind); assert.equal(request.args.parentId, 'shared-parent');
    assert.equal(request.args.subjectRef.id, 'povedai:subject-' + kind);
  }
  await f.click('Приложение'); await f.click('Отзывы через вход Сот');
  assert.equal(all(f.host).find(node => node.tag === 'textarea').value, 'Private local app root');
  await f.click('Вернуться к черновику ответа'); assert.equal(all(f.host).find(node => node.tag === 'textarea').value, 'Private local app reply');
  assert.equal(collectCalls(f).length, 2, 'changing subject does not send retained app drafts');
});
