import test from 'node:test';
import assert from 'node:assert/strict';
import {fixture,deferred,flush,viewStatus} from './support/voice-orb-controller-fixture.mjs';
import {editorArtifact,editorReadback,voiceResult,bounded,untilEditor,gate,speak} from './support/voice-editor-fixture.mjs';

test('default disabled state and construction never capture audio or create a provider session', async t => {
  const f = fixture(t, { configured: false }); assert.equal(f.calls.length, 0);
  await f.voice.refresh(); await f.voice.start();
  assert.equal(f.voice.view().phase, 'disabled'); assert.equal(f.counts().mediaCalls, 0);
  assert.deepEqual(f.calls.map(row => row.op), ['voice.orb.status']);
});

test('End while initial local state is pending prevents even a later microphone request', async t => {
  const gate = deferred(), entered = deferred(); let reads = 0;
  const f = fixture(t, { async readLocal() { if (++reads === 3) { entered.resolve(); await gate.promise; } } });
  await f.voice.refresh(); const pending = f.voice.start(); await entered.promise;
  await f.voice.end(); gate.resolve(); await pending;
  assert.equal(f.counts().mediaCalls, 0); assert.equal(f.counts().releases, 1);
});

test('late getUserMedia result after End stops real owned tracks before releasing its capture lease', async t => {
  const gate = deferred(), entered = deferred();
  const track = { enabled: true, readyState: 'live', stops: 0, stop() { this.stops++; this.readyState = 'ended'; } };
  const f = fixture(t, { media() { entered.resolve(); return gate.promise; } });
  await f.voice.refresh(); const pending = f.voice.start(); await entered.promise; await f.voice.end();
  assert.equal(f.counts().releases, 0); assert.equal(f.counts().leaseCount, 1);
  gate.resolve({ getTracks: () => [track], getAudioTracks: () => [track] }); await pending;
  assert.equal(track.stops, 1); assert.equal(track.enabled, false); assert.equal(f.counts().releases, 1);
  assert.equal(f.calls.some(row => row.op === 'voice.orb.start'), false);
});

test('a start admitted AFTER early End status-unavailable still receives exactly one owned close', async t => {
  const gate = deferred(), entered = deferred();
  const f = fixture(t, { async rpc(op) { if (op === 'voice.orb.start') { entered.resolve(); await gate.promise;
    return { schema: 'soty.voice-orb-started.v1', sessionId: 'late_owned', providerSessionId: 'late_provider', expiresAt: Date.now() + 15000, remainingDurationMs: 15000, sdp: 'late_sdp', voiceModel: 'gpt-realtime-2.1', brainModel: 'zai-org/GLM-5.3-Flash' }; } } });
  await f.voice.refresh(); const pending = f.voice.start(); await entered.promise;
  await f.voice.end(); assert.equal(f.voice.view().phase, 'unknown');
  gate.resolve(); await pending;
  const ends = f.calls.filter(row => row.op === 'voice.orb.end'); assert.equal(ends.length, 1);
  assert.deepEqual(ends[0].args, { sessionId: 'late_owned' }); assert.equal(f.counts().releases, 1);
  assert.equal(f.voice.view().phase, 'idle'); assert.equal(f.voice.view().hangupAccepted, true);
});

test('held status for A cannot overwrite a new conversation B after End A and Start B', async t => {
  const gate = deferred(), entered = deferred(); let hold = true;
  const f = fixture(t, { async rpc(op, args) { if (op === 'voice.orb.status' && args.sessionId === 'owned_1' && hold) {
    hold = false; entered.resolve(); await gate.promise; return { ...viewStatus, task: { state: 'completed', result: { answer: 'Stale A secret' } } };
  } } });
  await f.start(); const pending = f.voice.check(); await entered.promise; await f.voice.end(); await f.voice.start();
  gate.resolve(); await pending; assert.equal(f.voice.view().answer, ''); assert.equal(f.voice.view().phase, 'listening');
});

test('whole-session A→B→A between RPCs cannot delegate a new task with a fresh revision', async t => {
  const f = fixture(t); await f.start();
  f.local.accountId = 'bob'; f.local.deviceId = 'device_b'; f.local.localRevision++;
  f.local.accountId = 'alice'; f.local.deviceId = 'device_a'; f.local.localRevision++;
  f.peers[0].message({ type: 'input_audio_buffer.committed', item_id: 'fixture_user' });
  f.peers[0].message({ type: 'response.done', response: { status: 'completed', metadata: { soty_input: 'fixture_user' }, output: [{ type: 'function_call', status: 'completed', name: 'delegate_to_agent', call_id: 'new_task', arguments: '{"task":"Сделай записку"}' }] } }); await flush(); await flush();
  assert.equal(f.calls.filter(row => row.op === 'voice.orb.turn').length, 0);
});

test('interruption pauses immediately, requests backend cancellation and stays muted until explicit resume', async t => {
  const f = fixture(t); await f.start(); await f.voice.interrupt();
  assert.equal(f.audio.muted, true); assert.equal(f.calls.filter(row => row.op === 'voice.orb.cancel').length, 1);
  f.peers[0].message({ type: 'output_audio_buffer.cleared', event_id: 'synthetic_ack' }); await flush(); assert.equal(f.audio.muted, true);
  await f.voice.resumePlayback(); assert.equal(f.audio.muted, false); assert.ok(f.audio.playCount > 0);
});

test('duplicate delegation metadata and unknown response never automatically dispatch a second turn', async t => {
  const f = fixture(t, { async rpc(op) { if (op === 'voice.orb.turn') throw new Error('synthetic_network_unknown'); } });
  await f.start(); const metadata = { type: 'response.done', response: { status: 'completed', metadata: { soty_input: 'fixture_user' }, output: [{ type: 'function_call', status: 'completed', name: 'delegate_to_agent', call_id: 'once_task', arguments: '{"task":"Сделай записку"}' }] } };
  f.peers[0].message({ type: 'input_audio_buffer.committed', item_id: 'fixture_user' });
  f.peers[0].message(metadata); await flush(); await flush(); f.peers[0].message(metadata); await flush();
  assert.equal(f.calls.filter(row => row.op === 'voice.orb.turn').length, 1); assert.equal(f.voice.view().phase, 'unknown');
  await f.voice.check(); assert.equal(f.calls.filter(row => row.op === 'voice.orb.turn').length, 1);
});


test('late refresh after dispose emits no obsolete state into a new voice panel', async t => {
  const gate = deferred(), entered = deferred();
  const f = fixture(t, { async rpc(op, args) { if (op === 'voice.orb.status' && !args.sessionId && !args.requestId) { entered.resolve(); await gate.promise; return viewStatus; } } });
  const pending = f.voice.refresh(); await entered.promise; f.voice.dispose();
  const eventsAfterDispose = f.views.length; gate.resolve(); await pending;
  assert.equal(f.views.length, eventsAfterDispose);
});


test('End physically stops capture before a slow root hangup answer arrives', async t => {
  const gate = deferred(), entered = deferred();
  const f = fixture(t, { async rpc(op) { if (op === 'voice.orb.end') { entered.resolve(); await gate.promise; return { hangupAccepted: true, terminationVerified: false, accountingFinalized: false }; } } });
  await f.start(); const pending = f.voice.end(); await entered.promise;
  assert.equal(f.tracks[0].readyState, 'ended'); assert.equal(f.tracks[0].enabled, false); assert.equal(f.peers[0].closed, 1);
  assert.equal(f.counts().releases, 1); gate.resolve(); await pending;
});


test('End during post-bootstrap local fence prevents later peer setup and all new start timers', async t => {
  const gate = deferred(), entered = deferred(); let reads = 0, intervalCalls = 0;
  const original = globalThis.setInterval, intervals = [];
  globalThis.setInterval = (...args) => { intervalCalls++; const timer = original(...args); intervals.push(timer); return timer; };
  const f = fixture(t, { async readLocal() { if (++reads === 7) { entered.resolve(); await gate.promise; } } });
  try {
    await f.voice.refresh(); const pending = f.voice.start(); await entered.promise; await f.voice.end(); gate.resolve(); await pending;
    assert.equal(f.peers[0].remoteCalls ?? 0, 0); assert.equal(intervalCalls, 0); assert.equal(f.tracks[0].readyState, 'ended');
  } finally { gate.resolve(); for (const timer of intervals) clearInterval(timer); globalThis.setInterval = original; }
});

test('End during setRemoteDescription cannot install a late identity polling interval', async t => {
  const gate = deferred(), entered = deferred(); let intervalCalls = 0;
  const original = globalThis.setInterval, intervals = [];
  globalThis.setInterval = (...args) => { intervalCalls++; const timer = original(...args); intervals.push(timer); return timer; };
  const f = fixture(t, { async remote() { entered.resolve(); await gate.promise; } });
  try {
    await f.voice.refresh(); const pending = f.voice.start(); await entered.promise; await f.voice.end(); gate.resolve(); await pending;
    assert.equal(f.peers[0].remoteCalls, 1); assert.equal(intervalCalls, 0); assert.equal(f.peers[0].closed, 1);
  } finally { gate.resolve(); for (const timer of intervals) clearInterval(timer); globalThis.setInterval = original; }
});

test('editor-handoff artifact text in a transcript/function argument never supplies the host target', {timeout:5000},async t=>{
 let opens=0;const f=fixture(t,{openProjectEditor:async()=>{opens++;return editorReadback;}});await f.start();
 speak(f,'untrusted_call','untrusted_input',{task:'Прочитай источник',editorHandoff:editorArtifact});await untilEditor(()=>f.voice.view().answer!=='');
 assert.equal(opens,0);assert.equal(f.voice.view().editor,undefined);
});
test('editor-handoff closed DTO rejects foreign call/session, malformed target and arbitrary Source data before navigation', {timeout:5000},async t=>{
 for(const corrupt of [v=>({...v,callId:'foreign_call'}),v=>({...v,sessionId:'foreign_session'}),v=>({...v,requestId:undefined}),v=>({...v,editorHandoff:{...editorArtifact,url:'https://unknown.invalid'}}),v=>({...v,editorHandoff:undefined,evidence:[{data:{editorHandoff:editorArtifact}}]})]){
  let opens=0;const f=fixture(t,{openProjectEditor:async()=>{opens++;return editorReadback;},rpc(op,args){if(op==='voice.orb.turn')return corrupt(voiceResult(args));}});await f.start();speak(f);await flush();await flush();await flush();assert.equal(opens,0);assert.notEqual(f.voice.view().editor?.state,'opened');await f.voice.end();
 }
});
test('editor-handoff whole-session A→B→A rejects a delayed artifact without acquiring a foreground port', {timeout:5000},async t=>{
 const held=gate(t);let opens=0;const f=fixture(t,{openProjectEditor:async()=>{opens++;return editorReadback;},async rpc(op,args){if(op==='voice.orb.turn'){held.entered.resolve();await held.release.promise;return voiceResult(args);}}});
 await f.start();speak(f);await bounded(held.entered.promise);f.local.accountId='bob';f.local.localRevision++;f.local.accountId='alice';f.local.localRevision++;held.release.resolve();await flush();await flush();assert.equal(opens,0);assert.notEqual(f.voice.view().editor?.state,'opened');
});
for(const event of [{type:'input_audio_buffer.speech_started',item_id:'new_speech'},{type:'input_audio_buffer.committed',item_id:'new_commit'}])test('editor-handoff '+event.type+' aborts a pending open and rejects its late readback',{timeout:5000},async t=>{
 const held=gate(t);let applied=0,signal;const f=fixture(t,{async openProjectEditor(a,owner){signal=owner.signal;held.entered.resolve();await held.release.promise;await owner.assertCurrent();applied++;return editorReadback;},rpc(op,args){if(op==='voice.orb.turn')return voiceResult(args);}});
 await f.start();speak(f);await bounded(held.entered.promise);f.peers[0].message(event);assert.equal(signal.aborted,true);held.release.resolve();await flush();await flush();assert.equal(applied,0);assert.notEqual(f.voice.view().editor?.state,'opened');assert.equal(f.voice.view().active,true);
});
test('editor-handoff Cancel aborts the exact pending port and status recovery cannot reopen it',{timeout:5000},async t=>{
 const held=gate(t);let opens=0,applied=0,signal;const f=fixture(t,{async openProjectEditor(a,owner){opens++;signal=owner.signal;held.entered.resolve();await held.release.promise;await owner.assertCurrent();applied++;return editorReadback;},rpc(op,args){if(op==='voice.orb.turn')return voiceResult(args);if(op==='voice.orb.status'&&args.sessionId)return {...viewStatus,task:{state:'completed',result:{answer:'Проект сохранён.',editorHandoff:editorArtifact}}};}});
 await f.start();speak(f);await bounded(held.entered.promise);await f.voice.interrupt();assert.equal(signal.aborted,true);held.release.resolve();await flush();await f.voice.check();assert.equal(opens,1);assert.equal(applied,0);assert.equal(f.calls.filter(row=>row.op==='voice.orb.turn').length,1);assert.notEqual(f.voice.view().editor?.state,'opened');
});
for(const action of ['end','dispose'])test('editor-handoff '+action+' aborts pending navigation before its late completion',{timeout:5000},async t=>{
 const held=gate(t);let applied=0,signal;const f=fixture(t,{async openProjectEditor(a,owner){signal=owner.signal;held.entered.resolve();await held.release.promise;await owner.assertCurrent();applied++;return editorReadback;},rpc(op,args){if(op==='voice.orb.turn')return voiceResult(args);}});
 await f.start();speak(f);await bounded(held.entered.promise);await f.voice[action]();assert.equal(signal.aborted,true);held.release.resolve();await flush();await flush();assert.equal(applied,0);assert.equal(f.voice.view().active,false);assert.notEqual(f.voice.view().editor?.state,'opened');assert.equal(f.tracks[0].readyState,'ended');
});
test('editor-handoff wrong actual selected project or revision stays unconfirmed and never retries automatically',{timeout:5000},async t=>{
 let opens=0;const f=fixture(t,{async openProjectEditor(){opens++;return {...editorReadback,selectedProjectId:'22222222-2222-4222-8222-222222222222'};},rpc(op,args){if(op==='voice.orb.turn')return voiceResult(args);}});
 await f.start();speak(f);await untilEditor(()=>f.voice.view().editor?.state==='unconfirmed');await f.voice.check();assert.equal(opens,1);assert.equal(f.voice.view().active,true);assert.equal(f.calls.some(row=>row.op==='voice.orb.end'),false);
});
test('editor-handoff unknown signed turn followed by status/history result never triggers a second model turn or editor open',{timeout:5000},async t=>{
 let opens=0;const f=fixture(t,{async openProjectEditor(){opens++;return editorReadback;},rpc(op,args){if(op==='voice.orb.turn')throw Error('synthetic_unknown');if(op==='voice.orb.status'&&args.sessionId)return {...viewStatus,task:{state:'completed',result:{answer:'Recovered',editorHandoff:editorArtifact}}};}});
 await f.start();speak(f);await untilEditor(()=>f.voice.view().phase==='unknown');await f.voice.check();assert.equal(opens,0);assert.equal(f.calls.filter(row=>row.op==='voice.orb.turn').length,1);assert.equal(f.voice.view().editor,undefined);
});

// Four ownership regressions on the actual controller, same maintained fixture.
function audioGate(t){let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});t.after(()=>resolve());return{promise,resolve,reject,entered:deferred()};}
const audioFlush=async()=>{await flush();await flush();};
test('audio-owner late track playback rejection for A cannot mark new B blocked or notify it',{timeout:5000},async t=>{
 const f=fixture(t),held=audioGate(t);await f.start();f.audio.play=()=>{held.entered.resolve();return held.promise;};
 f.peers[0].emit('track',{track:{id:'remote_A'}});await held.entered.promise;await f.voice.end();await f.voice.start();
 const count=f.views.length;held.reject(Error('old_A_play_rejected'));await audioFlush();
 assert.equal(f.voice.view().playbackBlocked,false);assert.equal(f.voice.view().phase,'listening');assert.equal(f.views.length,count);
});
test('audio-owner late successful Resume A cannot clear B playback rejection',{timeout:5000},async t=>{
 const f=fixture(t),held=audioGate(t);await f.start();f.audio.play=()=>{held.entered.resolve();return held.promise;};const pending=f.voice.resumePlayback();
 await held.entered.promise;await f.voice.end();await f.voice.start();f.audio.play=async()=>{throw Error('current_B_play_blocked');};await f.voice.resumePlayback();assert.equal(f.voice.view().playbackBlocked,true);
 const count=f.views.length;held.resolve();await pending;assert.equal(f.voice.view().playbackBlocked,true);assert.equal(f.views.length,count);
});
test('audio-owner late failed Resume A cannot add an error to B or notify it',{timeout:5000},async t=>{
 const f=fixture(t),held=audioGate(t);await f.start();f.audio.play=()=>{held.entered.resolve();return held.promise;};const pending=f.voice.resumePlayback();
 await held.entered.promise;await f.voice.end();await f.voice.start();const count=f.views.length;held.reject(Error('old_A_resume_rejected'));await pending;
 assert.equal(f.voice.view().playbackBlocked,false);assert.equal(f.voice.view().phase,'listening');assert.equal(f.views.length,count);
});
test('audio-owner late Cancel A rejection cannot replace current B code or phase',{timeout:5000},async t=>{
 const held=audioGate(t);const f=fixture(t,{async rpc(op){if(op==='voice.orb.cancel'){held.entered.resolve();return held.promise;}}});await f.start();const pending=f.voice.interrupt();
 await held.entered.promise;await f.voice.end();await f.voice.start();const count=f.views.length;held.reject(Error('old_A_cancel_rejected'));await pending;
 assert.equal(f.voice.view().code,null);assert.equal(f.voice.view().phase,'listening');assert.equal(f.views.length,count);
});
