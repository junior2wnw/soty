import test from 'node:test';
import assert from 'node:assert/strict';
import { MessageChannel } from 'node:worker_threads';
import { mountProjectCaptureBridge, projectCaptureRequest, requestProjectCapture } from './project-feedback-capture.mjs';

const turn = () => new Promise(resolve => setImmediate(resolve));
const request = (requestId = 'selected-one') => ({ schema: 'soty.feedback.capture.v1', type: 'capture_request', requestId,
  sourceId: 'source-fixture', projectId: ' Частный проект/А ', contextRevision: 1, kind: 'audio' });
const payload = () => [{ kind: 'audio', name: 'selected.webm', mimeType: 'audio/webm', dataBase64: 'AQID' }];
function fixture(t, options = {}) {
  let listener, captureCalls = 0, assertions = 0;
  const peer = { approved: true, window: {}, origin: 'https://source.example', sourceId: 'source-fixture', appId: 'app-one',
    accountId: 'account-one', generation: 1, slot: {}, title: 'Native application' };
  const view = { addEventListener(name, value) { assert.equal(name, 'message'); listener = value; },
    removeEventListener(_name, value) { if (listener === value) listener = undefined; } };
  const handle = mountProjectCaptureBridge({ view, readPeer: () => peer,
    ...(options.preparePeer?{preparePeer:signal=>options.preparePeer(peer,signal)}:{}),
    ...(options.onCaptureActive?{onCaptureActive:options.onCaptureActive}:{}),
    async assertPeer(value, signal) { assertions++; return options.assertPeer ? options.assertPeer(value, signal) : true; },
    async capture(value) { captureCalls++; return options.capture ? options.capture(value) : payload(); },
    timeoutMs: options.timeoutMs || 8000 });
  t.after(() => handle.dispose());
  const ports = [];
  t.after(() => ports.forEach(port => port.close()));
  function deliver(input = request(), change = {}) {
    const channel = new MessageChannel(); ports.push(channel.port1, channel.port2);
    const replies = []; channel.port2.on('message', value => replies.push(value));
    const next = new Promise(resolve => channel.port2.once('message', resolve));
    listener?.({ data: input, source: peer.window, origin: peer.origin, ports: [channel.port1], ...change });
    return { next, replies, cancel() { channel.port2.postMessage({ schema: 'soty.feedback.capture.v1', type: 'capture_cancel', requestId: input.requestId }); } };
  }
  return { peer, handle, deliver, parent: { postMessage(data, target, transferred) {
    assert.equal(target, 'https://root.example');
    ports.push(...transferred);
    listener?.({ data, origin: peer.origin, source: peer.window, ports: transferred });
  } }, counts: () => ({ captureCalls, assertions }) };
}

test('only exact current peer can open Root selection, response contains no slot/actor and duplicate request is not recaptured', async t => {
  const f = fixture(t), result = await f.deliver().next;
  assert.equal(result.type, 'capture_result'); assert.equal(result.projectId, request().projectId);
  assert.deepEqual(result.attachments, payload());
  assert.deepEqual(Object.keys(result).sort(), ['attachments', 'contextRevision', 'kind', 'projectId', 'requestId', 'schema', 'sourceId', 'type']);
  const replay = await f.deliver().next;
  assert.equal(replay.code, 'capture_replayed'); assert.equal(f.counts().captureCalls, 1);
});
test('pre-picker renewal captures the new slot; renewal during selection discards old media',async t=>{
  let old,prepared;const f=fixture(t,{async preparePeer(peer){old=peer.slot;peer.slot={};peer.generation++;prepared=peer.slot;return true;},
    async assertPeer(peer){assert.equal(peer.slot===prepared,true);assert.equal(peer.slot===old,false);return true;}});
  assert.equal((await f.deliver().next).type,'capture_result');
  let release,begun;const waiting=new Promise(done=>{release=done;}),started=new Promise(done=>{begun=done;});
  const second=fixture(t,{async preparePeer(){return true;},async capture(){begun();await waiting;return payload();}}),reply=second.deliver();
  await started;second.peer.slot={};second.peer.generation++;release();assert.equal((await reply.next).code,'capture_context_changed');
});
test('pre-picker preparation cannot retarget the source window/account even before the dialog',async t=>{
  const f=fixture(t,{async preparePeer(peer){peer.accountId='different-account';return true;}});
  assert.equal((await f.deliver().next).code,'capture_context_changed');assert.equal(f.counts().captureCalls,0);
});

test('foreign window/origin/source/profile and actor/slot fields are denied before Root capture', async t => {
  const f = fixture(t);
  f.deliver(request(), { source: {} }); f.deliver(request(), { origin: 'https://foreign.example' });
  f.deliver({ ...request(), sourceId: 'foreign-source' }); f.deliver({ ...request(), actor: 'forged' });
  f.peer.approved = false; f.deliver(); await turn();
  assert.deepEqual(f.counts(), { captureCalls: 0, assertions: 0 });
});

test('native Root authority denial cannot open recorder/picker', async t => {
  const f = fixture(t, { assertPeer: () => false });
  assert.equal((await f.deliver().next).code, 'capture_context_changed'); assert.equal(f.counts().captureCalls, 0);
});

test('mutable host account/project-generation changes after capture do not retarget selected data', async t => {
  let release, begun;
  const started = new Promise(resolve => { begun = resolve; });
  const pending = new Promise(resolve => { release = resolve; });
  const f = fixture(t, { async capture() { begun(); await pending; return payload(); } });
  const response = f.deliver(); await started;
  f.peer.accountId = 'account-two'; f.peer.generation++;
  release(); const result = await response.next;
  assert.equal(result.code, 'capture_context_changed'); assert.equal(Object.hasOwn(result, 'attachments'), false);
});

test('Source cancellation/Root dispose stop selection and never send a second reply', async t => {
  for (const action of ['cancel', 'dispose']) {
    let release, begun;
    const started = new Promise(resolve => { begun = resolve; });
    const pending = new Promise(resolve => { release = resolve; });
    let capturedSignal;
    const f = fixture(t, { async capture({ signal }) { capturedSignal = signal; begun(); await pending; return payload(); } });
    const response = f.deliver(); await started;
    if (action === 'cancel') response.cancel(); else f.handle.dispose();
    const result = await response.next; assert.equal(result.type, 'capture_error'); assert.equal(capturedSignal.aborted, true);
    release(); await turn(); assert.equal(response.replies.length, 1);
  }
});

test('ignored-abort capture holds its slot after deadline and drops late private bytes', async t => {
  let release, begun;
  const started = new Promise(resolve => { begun = resolve; });
  const pending = new Promise(resolve => { release = resolve; });
  const f = fixture(t, { timeoutMs: 25, async capture() { begun(); await pending; return payload(); } });
  const response = f.deliver(); await started;
  assert.equal((await response.next).code, 'capture_timeout');
  assert.equal((await f.deliver(request('another')).next).code, 'capture_busy');
  release(); await turn(); assert.equal(response.replies.length, 1);
});

test('kind/size/closed payload reject invalid media and request getters never run', async t => {
  const f = fixture(t, { capture: () => [{ ...payload()[0], grant: 'forged' }] });
  assert.equal((await f.deliver().next).code, 'capture_failed');
  let calls = 0;
  assert.equal(projectCaptureRequest({ ...request(), get projectId() { calls++; return 'one'; } }), null);
  assert.equal(calls, 0);
});

test('actual MessageChannel Source helper returns selected preview only to captured context', async t => {
  const f = fixture(t);
  const selected = await requestProjectCapture({ parent: f.parent, parentOrigin: 'https://root.example', sourceId: 'source-fixture',
    projectId: request().projectId, contextRevision: 1, kind: 'audio', isCurrent: () => true, channelFactory: () => new MessageChannel() });
  assert.deepEqual(selected, payload()); assert.equal(f.counts().captureCalls, 1);
});

test('Source profile/project change after Root selection cannot import old media into new draft', async t => {
  let release, begun, current = true;
  const pending = new Promise(resolve => { release = resolve; });
  const started = new Promise(resolve => { begun = resolve; });
  const f = fixture(t, { async capture() { begun(); await pending; return payload(); } });
  const selected = requestProjectCapture({ parent: f.parent, parentOrigin: 'https://root.example', sourceId: 'source-fixture',
    projectId: request().projectId, contextRevision: 1, kind: 'audio', isCurrent: () => current, channelFactory: () => new MessageChannel() });
  await started; current = false; release();
  await assert.rejects(selected, error => error.code === 'capture_context_changed');
});
