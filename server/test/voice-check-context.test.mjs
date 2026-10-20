import test from 'node:test';
import assert from 'node:assert/strict';
import {fixture,viewStatus,flush} from './support/voice-orb-controller-fixture.mjs';
import {gate,speak,bounded,untilEditor,editorArtifact} from './support/voice-editor-fixture.mjs';
const result = (args, answer) => ({ schema: 'soty.voice-orb-turn.v1', sessionId: args.sessionId, callId: args.callId, answer });
const status = (callId, answer) => ({ ...viewStatus, sessionId: 'owned_1', task: { callId, state: 'completed', result: { answer, editorHandoff: editorArtifact } } });

for (const kind of ['success', 'error']) test(`held ${kind} status A cannot change new call B within the same conversation/peer`, { timeout: 5000 }, async t => {
  const heldStatus = gate(t), heldTurn = gate(t); let opens = 0;
  const f = fixture(t, { async rpc(op, args) {
    if (op === 'voice.orb.turn' && args.callId === 'call_A') throw Error('synthetic_unknown_A');
    if (op === 'voice.orb.turn' && args.callId === 'call_B') { heldTurn.entered.resolve(); await heldTurn.release.promise; return result(args, 'Current B answer'); }
    if (op === 'voice.orb.status' && args.sessionId) { heldStatus.entered.resolve(); await heldStatus.release.promise;
      if (kind === 'error') throw Error('synthetic_late_A_status_failure'); return status('call_A', 'Stale A answer'); }
  }, openProjectEditor: async () => { opens++; throw Error('status_must_not_open_editor'); } });
  await f.start(); speak(f, 'call_A', 'input_A'); await untilEditor(() => f.voice.view().phase === 'unknown');
  const checking = f.voice.check(); await bounded(heldStatus.entered.promise);
  speak(f, 'call_B', 'input_B'); await bounded(heldTurn.entered.promise); assert.equal(f.voice.view().phase, 'working');
  const before = f.voice.view(), viewCount = f.views.length; heldStatus.release.resolve(); await bounded(checking);
  assert.equal(f.voice.view().answer, before.answer); assert.equal(f.voice.view().phase, 'working'); assert.equal(f.voice.view().code, before.code);
  assert.equal(f.views.length, viewCount); assert.equal(f.counts().peerCount, 1); assert.equal(opens, 0);
  heldTurn.release.resolve(); await untilEditor(() => f.voice.view().answer === 'Current B answer');
  assert.equal(f.calls.filter(call => call.op === 'voice.orb.turn').length, 2);
});

test('wrong session or wrong call status DTO cannot recover the current call or open a project', { timeout: 5000 }, async t => {
  let wrongSession = true, opens = 0;
  const f = fixture(t, { async rpc(op, args) {
    if (op === 'voice.orb.turn') throw Error('synthetic_unknown_current');
    if (op === 'voice.orb.status' && args.sessionId) return wrongSession ? { ...status('call_A', 'Wrong session answer'), sessionId: 'foreign_session' }
      : status('foreign_call', 'Wrong call answer');
  }, openProjectEditor: async () => { opens++; throw Error('status_must_not_open_editor'); } });
  await f.start(); speak(f, 'call_A', 'input_A'); await untilEditor(() => f.voice.view().phase === 'unknown');
  await f.voice.check(); assert.equal(f.voice.view().answer, ''); assert.equal(f.voice.view().phase, 'unknown'); assert.equal(f.voice.view().code, 'voice_orb_unknown');
  wrongSession = false; await f.voice.check(); assert.equal(f.voice.view().answer, ''); assert.equal(f.voice.view().phase, 'unknown'); assert.equal(opens, 0);
  assert.equal(f.calls.filter(call => call.op === 'voice.orb.turn').length, 1);
});

test('matching current status recovers the answer without a second model turn or automatic editor open', { timeout: 5000 }, async t => {
  let opens = 0;
  const f = fixture(t, { async rpc(op, args) {
    if (op === 'voice.orb.turn') throw Error('synthetic_unknown_current');
    if (op === 'voice.orb.status' && args.sessionId) return status('call_A', 'Recovered current answer');
  }, openProjectEditor: async () => { opens++; throw Error('status_must_not_open_editor'); } });
  await f.start(); speak(f, 'call_A', 'input_A'); await untilEditor(() => f.voice.view().phase === 'unknown');
  await f.voice.check(); await flush(); assert.equal(f.voice.view().answer, 'Recovered current answer'); assert.equal(f.voice.view().phase, 'listening');
  assert.equal(f.voice.view().code, null); assert.equal(f.voice.view().editor, undefined); assert.equal(opens, 0);
  assert.equal(f.calls.filter(call => call.op === 'voice.orb.turn').length, 1); assert.equal(f.calls.filter(call => call.op === 'voice.orb.status' && call.args.sessionId).length, 1);
});
