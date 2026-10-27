import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createSourceWorkspaceClient } from '../browser/index.mjs';

const canonical = value => Array.isArray(value) ? '[' + value.map(canonical).join(',') + ']'
  : value && typeof value === 'object' ? '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}' : JSON.stringify(value);
const hash = value => createHash('sha256').update(canonical(value)).digest('hex');
const json = (value, status = 200) => Response.json(value, { status });
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const context = (ref = 'A'.repeat(43), generation = 1) => ({ schema: 'soty.current-context.v2',
  selection: { kind: 'soty.workspace.v1', nativeId: 'board', incarnationId: 'one' },
  context: { ref, generation, expiresAt: 1900000000000 }, continuityDigest: 'c'.repeat(64), title: 'Personal board', role: 'owner', revision: 1 });
function fixture({ cold = false } = {}) {
  let current = context(), loseAck = false, beforeResponse = null, isCurrent = true, effectCount = 0, sessionReady = !cold;
  const calls = [], receipts = new Map();
  const fetcher = async (path, init) => {
    const args = JSON.parse(init.body); calls.push({ path, init, args });
    let response;
    if (path.endsWith('/session-continue')) { sessionReady = true; response = json({ schema: 'soty.source-session-continuation.v1', ready: true,
      renewable: true, sessionExpiresAt: 1900000000000, accessExpiresAt: 1900000000000, receiptDigest: '0'.repeat(64) }); }
    else if (!sessionReady) response = json({ ok: false, error: { code: 'authentication_required' } }, 401);
    else if (args.input.operation === 'soty.current_context' || args.input.operation === 'soty.context.select') response = json({ ok: true, data: structuredClone(current) });
    else if (args.contextRef !== current.context.ref) response = json({ ok: false, error: { code: 'source_workspace_context_denied' } }, 403);
    else if (path.endsWith('/query')) response = json({ ok: true, data: { requestId: args.requestId, records: [{ title: current.title }] } });
    else if (path.endsWith('/receipt')) response = json({ ok: true, data: receipts.get(args.requestId) ?? { requestId: args.requestId, inputDigest: args.input.inputDigest, outcome: 'not_applied' } });
    else if (path.endsWith('/invoke')) {
      const inputDigest = hash(args.input), previous = receipts.get(args.requestId);
      if (previous && previous.inputDigest !== inputDigest) response = json({ ok: false, error: { code: 'request_conflict' } }, 409);
      else {
        const result = previous ?? { requestId: args.requestId, inputDigest, outcome: 'committed', replayed: false,
          receipt: { id: 'row-' + ++effectCount, revision: effectCount },
          contextAfter: { schema: 'soty.context-after.v1', generation: current.context.generation, title: current.title, role: current.role, revision: effectCount,
            mutationRef: hash({ schema: 'soty.workspace-mutation.v1', requestId: args.requestId, inputDigest, contextRef: args.contextRef }) },
          workspaceReceipt: { schema: 'soty.workspace-receipt.v1', effects: [{ kind: 'created', resourceType: 'note', resourceId: 'row-' + effectCount }],
            receipt: { verificationMethod: 'domain_read', artifacts: [{ type: 'note', id: 'row-' + effectCount }] } } };
        receipts.set(args.requestId, result);
        if (loseAck) { loseAck = false; throw Error('response dropped after COMMIT'); }
        response = json({ ok: true, data: result });
      }
    } else assert.fail('unexpected transport route');
    if (beforeResponse) { const hook = beforeResponse; beforeResponse = null; return hook(response, args); }
    return response;
  };
  const api = createSourceWorkspaceClient({ fetch: fetcher, href: 'https://app.example/embed?sotyFrame=' + 'F'.repeat(43), isCurrent: () => isCurrent });
  return { api, calls, receipts, fetcher, context: () => current, effects: () => effectCount,
    renew(change = {}) { current = { ...structuredClone(current), ...change, context: { ...current.context, ref: 'R'.repeat(43) } }; },
    switch() { current = context('B'.repeat(43), 2); current.title = 'Other board'; },
    lostAck() { loseAck = true; }, hold(hook) { beforeResponse = hook; }, retire() { isCurrent = false; } };
}
const id = suffix => 'browser-intent-' + suffix;
const ready = f => f.api.currentContext({ requestId: id('context') });

test('a new intent refreshes the short session and context before its only invoke', async () => {
  const f = fixture(); await ready(f); f.renew();
  const [a, b] = await Promise.all([
    f.api.prepareInvoke({ requestId: id('renew-write'), input: { operation: 'notes.create', title: 'After renewal' } }),
    f.api.prepareInvoke({ requestId: id('renew-write'), input: { operation: 'notes.create', title: 'After renewal' } }),
  ]);
  assert.equal(a, b); assert.equal(a.contextRef, 'R'.repeat(43));
  assert.deepEqual(f.calls.slice(1).map(call => call.path), ['/api/embed/session-continue', '/api/embed/query']);
  await f.api.invoke(a); assert.equal(f.effects(), 1); assert.equal(f.calls.at(-1).path, '/api/embed/invoke');
});

test('a stale read refreshes once while foreign actors, children, generations and legacy rotation fail before a write', async () => {
  const f = fixture(); await ready(f); f.renew();
  assert.equal((await f.api.query({ requestId: id('renew-read'), input: { operation: 'notes.list' } })).records.length, 1);
  assert.deepEqual(f.calls.slice(1).map(call => call.path), ['/api/embed/query', '/api/embed/session-continue', '/api/embed/query', '/api/embed/query']);
  for (const change of [ { continuityDigest: 'd'.repeat(64) }, { continuityDigest: undefined },
    { selection: { kind: 'soty.workspace.v1', nativeId: 'other', incarnationId: 'one' } },
    { selection: { kind: 'soty.workspace.v1', nativeId: 'board', incarnationId: 'new' } }, { role: 'viewer' } ]) {
    const denied = fixture(); await ready(denied); denied.renew(change);
    await assert.rejects(denied.api.prepareInvoke({ requestId: id('foreign-renew'), input: { operation: 'notes.create', title: 'Never move' } }),
      { code: 'source_client_context_changed', effectUnknown: false });
    assert.equal(denied.api.context(), null); assert.equal(denied.effects(), 0);
    assert.equal(denied.calls.some(call => call.path.endsWith('/invoke')), false);
  }
  const generation = fixture(); await ready(generation); generation.switch();
  await assert.rejects(generation.api.prepareInvoke({ requestId: id('new-generation'), input: { operation: 'notes.create' } }), /source_client_context_changed/);
});

test('renewal cannot reprepare a sent unknown request, and a revoked continuation does not retry or invoke', async () => {
  const f = fixture(); await ready(f); const args = { requestId: id('unknown-renew'), input: { operation: 'notes.create', title: 'One' } };
  const old = await f.api.prepareInvoke(args); f.lostAck(); await assert.rejects(f.api.invoke(old)); f.renew();
  const next = await f.api.prepareInvoke({ requestId: id('separate-new'), input: { operation: 'notes.create', title: 'Two' } });
  assert.equal(next.contextRef, 'R'.repeat(43));
  await assert.rejects(f.api.prepareInvoke(args), /request_conflict/);
  await assert.rejects(f.api.invoke(old), error => error.code === 'source_client_context_changed' && error.effectUnknown);
  assert.equal(f.effects(), 1); assert.equal(f.calls.filter(call => call.path.endsWith('/invoke')).length, 1);
  const revoked = fixture(); await ready(revoked);
  revoked.hold(() => json({ ok: false, error: { code: 'source_app_native_access_denied' } }, 403));
  await assert.rejects(revoked.api.prepareInvoke({ requestId: id('revoked-write'), input: { operation: 'notes.create' } }),
    { code: 'source_app_native_access_denied', effectUnknown: false });
  assert.equal(revoked.calls.length, 2); assert.equal(revoked.effects(), 0);
});

test('cold session continuation uses the maintained same-frame ACK and no Native auth cookie fallback', async () => {
  const f = fixture();
  const value = await f.api.continueSession({ requestId: id('continue') });
  assert.equal(value.ready, true); assert.equal(f.calls[0].path, '/api/embed/session-continue');
  assert.deepEqual(f.calls[0].args, { requestId: id('continue') });
  assert.equal(f.calls[0].init.headers.get('x-soty-frame'), 'F'.repeat(43));
  f.hold(() => json({ schema: 'soty.source-session-continuation.v1', ready: false }));
  await assert.rejects(f.api.continueSession({ requestId: id('wrong-ack') }), /source_client_response_invalid/);
});

test('one connect call performs the cold handshake and valid resume, with no write or additional UI gesture', async () => {
  const f = fixture({ cold: true });
  assert.equal((await f.api.connect({ requestId: id('cold-connect') })).title, 'Personal board');
  assert.deepEqual(f.calls.map(call => call.path), ['/api/embed/session-continue', '/api/embed/query']);
  assert.equal(f.calls[0].args.requestId, f.calls[1].args.requestId);
  assert.equal((await f.api.connect({ requestId: id('resume-connect') })).context.ref, f.context().context.ref);
  assert.equal(f.effects(), 0);
});

test('a profile switch between connect stages, denied continuation or closed view cannot continue bootstrap', async () => {
  const f = fixture({ cold: true }); f.hold(response => { f.retire(); return response; });
  await assert.rejects(f.api.connect({ requestId: id('profile-switch') }), /source_client_context_changed/);
  assert.equal(f.calls.length, 1); assert.equal(f.api.context(), null);
  for (const code of ['authentication_required', 'source_app_root_changed', 'source_app_native_access_denied']) {
    const denied = fixture({ cold: true }); denied.hold(() => json({ ok: false, error: { code } }, 403));
    await assert.rejects(denied.api.connect({ requestId: id('denied-' + code) }), error => error.code === code);
    assert.equal(denied.calls.length, 1); assert.equal(denied.api.context(), null);
  }
});

test('workspace bootstrap and reads reuse the maintained exact frame transport with no Native fallback', async () => {
  assert.throws(() => createSourceWorkspaceClient({ isCurrent: false }), /source_client_input_invalid/);
  const f = fixture();
  await assert.rejects(f.api.query({ requestId: id('before-context'), input: { operation: 'notes.list' } }), /source_client_context_required/);
  assert.equal(f.calls.length, 0);
  const selected = await ready(f), result = await f.api.query({ requestId: id('read'), input: { operation: 'notes.list' } });
  assert.equal(result.records[0].title, selected.title); assert.equal(f.calls[0].args.input.operation, 'soty.current_context');
  assert.equal(f.calls[0].args.contextRef, undefined); assert.equal(f.calls[1].args.contextRef, selected.context.ref);
  for (const call of f.calls) {
    assert.equal(call.path, '/api/embed/query'); assert.equal(call.init.credentials, 'same-origin'); assert.equal(call.init.redirect, 'error');
    assert.equal(call.init.referrerPolicy, 'no-referrer'); assert.equal(call.init.headers.get('x-soty-frame'), 'F'.repeat(43));
    assert.equal(Object.hasOwn(call.args, 'userId'), false); assert.equal(Object.hasOwn(call.args, 'token'), false);
  }
  assert.equal(Object.isFrozen(result.records[0]), true);
});

test('explicit intents preserve a canonical immutable body and coalesce a double activation', async () => {
  const f = fixture(); await ready(f);
  const input = { operation: 'notes.create', title: 'Original', properties: { z: 1, a: true } }, args = { requestId: id('write'), input };
  const [a, b] = await Promise.all([f.api.prepareInvoke(args), f.api.prepareInvoke(args)]);
  assert.equal(a, b); input.title = 'Changed later';
  const [first, second] = await Promise.all([f.api.invoke(a), f.api.invoke(b)]);
  assert.equal(first.receipt.id, second.receipt.id); assert.equal(f.effects(), 1);
  const sent = f.calls.find(call => call.path.endsWith('/invoke'));
  assert.equal(sent.args.input.title, 'Original'); assert.equal(a.inputDigest, hash(a.input)); assert.equal(Object.isFrozen(a.input.properties), true);
  await f.api.invoke(a);
  assert.equal(f.calls.filter(call => call.path.endsWith('/invoke')).length, 1);
  assert.equal(f.calls.at(-1).path, '/api/embed/receipt', 'a repeated intent is a fresh receipt read, not another apply');
});

test('lost invoke ACK is recovered only with the original receipt; a different body cannot reuse the pending request id', async () => {
  const f = fixture(); await ready(f); f.lostAck();
  const intent = await f.api.prepareInvoke({ requestId: id('lost'), input: { operation: 'notes.create', title: 'Once' } });
  await assert.rejects(f.api.invoke(intent), error => error.effectUnknown === true);
  await assert.rejects(f.api.prepareInvoke({ requestId: id('lost'), input: { operation: 'notes.create', title: 'Other' } }), /request_conflict/);
  assert.equal((await f.api.invoke(intent)).outcome, 'committed');
  assert.equal(f.effects(), 1); assert.equal(f.calls.filter(call => call.path.endsWith('/invoke')).length, 1);
  assert.equal(f.calls.at(-1).args.contextRef, intent.contextRef); assert.equal(f.calls.at(-1).args.requestId, intent.requestId);
});

test('a new context invalidates prepared and unknown writes without replaying them under the new actor or child', async () => {
  const f = fixture(); await ready(f);
  const unsent = await f.api.prepareInvoke({ requestId: id('unsent'), input: { operation: 'notes.create', title: 'A' } });
  const unknown = await f.api.prepareInvoke({ requestId: id('unknown'), input: { operation: 'notes.create', title: 'A' } });
  f.lostAck(); await assert.rejects(f.api.invoke(unknown)); f.switch(); await ready(f);
  await assert.rejects(f.api.invoke(unsent), error => error.code === 'source_client_context_changed' && error.effectUnknown === false);
  await assert.rejects(f.api.invoke(unknown), error => error.code === 'source_client_context_changed' && error.effectUnknown === true);
  await assert.rejects(f.api.receipt(unknown), /source_client_context_changed/);
  assert.equal(f.calls.filter(call => call.path.endsWith('/invoke')).length, 1); assert.equal(f.effects(), 1);
});

test('a late read and a late bootstrap cannot repaint a new context or a retired view', async () => {
  const f = fixture(); await ready(f); const held = deferred();
  f.hold(async response => { await held.promise; return response; });
  const old = f.api.query({ requestId: id('held-read'), input: { operation: 'notes.list' } });
  f.switch(); await ready(f); held.resolve(); await assert.rejects(old, /source_client_context_changed/);
  const late = deferred(); f.hold(async response => { await late.promise; return response; });
  const bootstrap = ready(f); f.api.invalidate(); late.resolve(); await assert.rejects(bootstrap, /source_client_context_changed/);
  assert.equal(f.api.context(), null); f.retire(); await assert.rejects(ready(f), /source_client_context_changed/);
});

test('a copied intent cannot become a new invoke, but its exact read-only receipt reference can recover', async () => {
  const f = fixture(); await ready(f);
  const intent = await f.api.prepareInvoke({ requestId: id('saved-reference'), input: { operation: 'notes.create', title: 'A' } });
  await f.api.invoke(intent);
  await assert.rejects(f.api.invoke(structuredClone(intent)), /source_client_input_invalid/);
  assert.equal((await f.api.receipt({ requestId: intent.requestId, contextRef: intent.contextRef, inputDigest: intent.inputDigest })).outcome, 'committed');
  await assert.rejects(f.api.receipt({ requestId: id('absent-reference'), contextRef: intent.contextRef, inputDigest: '0'.repeat(64) }),
    error => error.code === 'source_receipt_unknown' && error.effectUnknown === true);
  assert.equal(f.effects(), 1);
});

test('query/invoke cannot smuggle bootstrap, ambiguous JSON, oversized payloads or an implicit request id', async () => {
  const f = fixture(); await ready(f); let getterCalls = 0;
  const badInput = { operation: 'notes.create', get title() { getterCalls++; return 'unsafe'; } };
  for (const args of [{ input: { operation: 'notes.create' } }, { requestId: 'short', input: { operation: 'notes.create' } },
    { requestId: id('actor'), input: { operation: 'notes.create' }, userId: 'other' },
    { requestId: id('bootstrap'), input: { operation: 'soty.current_context' } }, { requestId: id('getter'), input: badInput },
    { requestId: id('undefined'), input: { operation: 'notes.create', title: undefined } },
    { requestId: id('size'), input: { operation: 'notes.create', title: 'x'.repeat(65536) } }]) {
    await assert.rejects(f.api.prepareInvoke(args));
  }
  assert.equal(getterCalls, 0); assert.equal(f.calls.length, 1);
});

test('malformed JSON, wrong content type, oversized or mismatched responses fail closed', async () => {
  for (const response of [new Response('{"ok":true,"ok":true,"data":{}}', { headers: { 'content-type': 'application/json' } }),
    new Response('{}', { headers: { 'content-type': 'text/html' } }),
    new Response(' '.repeat(65537), { headers: { 'content-type': 'application/json' } }),
    json({ ok: true, data: { requestId: 'another-original-request' } })]) {
    const f = fixture(); await ready(f); f.hold(() => response);
    await assert.rejects(f.api.query({ requestId: id('malformed'), input: { operation: 'notes.list' } }), /source_client_response_invalid/);
  }
});

test('a receipt from a different mutation or generation never confirms a possibly committed write', async () => {
  for (const mutate of [value => { value.data.inputDigest = '0'.repeat(64); }, value => { value.data.contextAfter.mutationRef = '0'.repeat(64); },
    value => { value.data.contextAfter.generation++; }, value => { value.data.workspaceReceipt.receipt.verificationMethod = 'trust_me'; }]) {
    const f = fixture(); await ready(f); const intent = await f.api.prepareInvoke({ requestId: id('forged-proof'), input: { operation: 'notes.create', title: 'A' } });
    f.hold(async response => { const body = await response.json(); mutate(body); return json(body); });
    await assert.rejects(f.api.invoke(intent), error => error.code === 'source_client_response_invalid' && error.effectUnknown);
    assert.equal(f.effects(), 1, 'a rejected proof is not a rollback');
    assert.equal((await f.api.receipt(intent)).outcome, 'committed');
  }
});

test('generic workspace proof accepts different opaque domain receipts without inventing a CRUD id or revision', async () => {
  for (const receipt of [{ accepted: true, etag: 'opaque-revision' }, ['domain-result', { accepted: true }], 'native-opaque-token', undefined]) {
    const f = fixture(); await ready(f);
    const intent = await f.api.prepareInvoke({ requestId: id('domain-shape'), input: { operation: 'notes.create', title: 'A' } });
    f.hold(async response => { const body = await response.json(); delete body.data.replayed;
      if (receipt === undefined) delete body.data.receipt; else body.data.receipt = receipt;
      return json(body); });
    const result = await f.api.invoke(intent);
    assert.deepEqual(result.receipt, receipt); assert.equal(result.outcome, 'committed');
  }
});

test('explicit recovery reads under a fresh context, retains the original receipt and never invokes again', async () => {
  const f = fixture(); await ready(f);
  const intent = await f.api.prepareInvoke({ requestId: id('fresh-recovery'), input: { operation: 'notes.create', title: 'Original' } });
  const original = await f.api.invoke(intent), originalBytes = JSON.stringify(original);
  f.switch(); await ready(f);
  f.hold(async response => { const old = (await response.json()).data;
    return json({ ok: true, data: { ...old, recovery: { schema: 'soty.workspace-receipt-recovery.v1', contextRef: intent.contextRef,
      receiptDigest: hash(old), contextAfter: { ...old.contextAfter, generation: 2, title: 'Other board', revision: 9,
        mutationRef: hash({ schema: 'soty.workspace-mutation.v1', requestId: intent.requestId, inputDigest: intent.inputDigest, contextRef: f.context().context.ref }) } } } });
  });
  const result = await f.api.recoverReceipt(intent);
  assert.equal(result.contextAfter.generation, 1); assert.equal(result.recovery.contextAfter.generation, 2);
  assert.equal(f.calls.at(-1).args.contextRef, f.context().context.ref); assert.equal(f.calls.at(-1).args.requestId, intent.requestId);
  assert.equal(f.calls.at(-1).args.input.inputDigest, intent.inputDigest); assert.equal(JSON.stringify(original), originalBytes);
  assert.equal(intent.contextRef, 'A'.repeat(43)); assert.equal(f.calls.filter(call => call.path.endsWith('/invoke')).length, 1);
  await assert.rejects(f.api.invoke(intent), /source_client_context_changed/);
});

test('foreign/missing legacy recovery, changed request or forged original/fresh envelopes remain unknown', async () => {
  for (const mode of ['legacy', 'no-row', 'original-digest', 'old-ref', 'fresh-generation', 'fresh-mutation']) {
    const f = fixture(); await ready(f); const intent = await f.api.prepareInvoke({ requestId: id('recover-negative'), input: { operation: 'notes.create', title: 'A' } });
    await f.api.invoke(intent); f.switch(); await ready(f);
    f.hold(async response => {
      const original = (await response.json()).data;
      if (mode === 'legacy') return json({ ok: true, data: original });
      if (mode === 'no-row') return json({ ok: true, data: { requestId: intent.requestId, inputDigest: intent.inputDigest, outcome: 'not_applied' } });
      const recovery = { schema: 'soty.workspace-receipt-recovery.v1', contextRef: intent.contextRef, receiptDigest: hash(original),
        contextAfter: { ...original.contextAfter, generation: 2, mutationRef: hash({ schema: 'soty.workspace-mutation.v1', requestId: intent.requestId, inputDigest: intent.inputDigest, contextRef: f.context().context.ref }) } };
      if (mode === 'original-digest') recovery.receiptDigest = '0'.repeat(64);
      if (mode === 'old-ref') recovery.contextRef = 'Z'.repeat(43);
      if (mode === 'fresh-generation') recovery.contextAfter.generation++;
      if (mode === 'fresh-mutation') recovery.contextAfter.mutationRef = '0'.repeat(64);
      return json({ ok: true, data: { ...original, recovery } });
    });
    await assert.rejects(f.api.recoverReceipt(intent), error => error.effectUnknown === true, mode);
    assert.equal(f.calls.filter(call => call.path.endsWith('/invoke')).length, 1, mode);
  }
});

test('an explicitly selected child acquires a new lease and never moves a retained old intent', async () => {
  const f = fixture(); await ready(f); const intent = await f.api.prepareInvoke({ requestId: id('old-child'), input: { operation: 'notes.create', title: 'A' } });
  f.switch(); const selected = await f.api.selectContext({ requestId: id('selection'), nativeId: 'other-child', generation: 2 });
  assert.equal(selected.title, 'Other board'); assert.equal(f.calls.at(-1).args.input.operation, 'soty.context.select');
  assert.equal(f.calls.at(-1).args.contextRef, undefined); await assert.rejects(f.api.invoke(intent), /source_client_context_changed/);
  f.api.dispose(); await assert.rejects(ready(f), /source_client_context_changed/);
});
