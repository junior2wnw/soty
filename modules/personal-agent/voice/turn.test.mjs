import test from 'node:test';
import assert from 'node:assert/strict';
import { VoiceTranscript, NativeDelegationScheduler } from './turn.mjs';

const scope = () => ({ accountId: 'A', deviceId: 'D', roomId: 'R', mode: 'group', generation: 1 });
const binding = overrides => ({ scope: scope(), epoch: 'E1', ...overrides });
const speech = (seq, overrides) => ({ epoch: 'E1', seq, eventId: `event-${seq}`, role: 'user',
  speakerId: 'human-1', provenance: 'admitted-port-label', delta: 'word ', startMs: seq * 10, endMs: seq * 10 + 5, ...overrides });
const typed = (seq, overrides) => ({ epoch: 'E1', seq, role: 'user', speakerId: 'human-1',
  provenance: 'admitted-port-label', text: 'typed', ...overrides });
const task = (id = 'T1', overrides) => ({ epoch: 'E1', id, offsetMs: 0, ...overrides });
const settle = async () => { await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); };
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };

function clock() {
  let now = 0, id = 0;
  const jobs = new Map();
  return {
    now: () => now,
    setTimeout(callback, delay) { const handle = ++id; jobs.set(handle, { callback, at: now + delay }); return handle; },
    clearTimeout(handle) { jobs.delete(handle); },
    advance(ms) {
      now += ms;
      for (let count = 0; count < 200; count++) {
        const next = [...jobs].filter(([, job]) => job.at <= now).sort((a, b) => a[1].at - b[1].at)[0];
        if (!next) return;
        jobs.delete(next[0]); next[1].callback();
      }
      throw new Error('unbounded_fixture_timer');
    },
    callbacks: () => [...jobs.values()].map(job => job.callback),
    count: () => jobs.size,
  };
}

function fixture(overrides = {}) {
  const time = clock(), runs = [], errors = [];
  const scheduler = new NativeDelegationScheduler(binding({ clock: time, hasHumanContext: () => true,
    run: (value, signal) => { runs.push({ value, signal }); }, onError: (error, value) => { errors.push({ error, value }); }, ...overrides }));
  return { scheduler, time, runs, errors };
}

test('same speaker preserves repeated words and backchannels; different humans/provenance do not merge', () => {
  const transcript = new VoiceTranscript(binding());
  assert.equal(transcript.appendSpeech(speech(0, { delta: 'да ' })), 'accepted');
  transcript.appendSpeech(speech(1, { role: 'assistant', speakerId: 'agent', delta: 'угу' }));
  transcript.appendSpeech(speech(2, { speakerId: 'human-2', delta: 'я' }));
  transcript.appendSpeech(speech(3, { delta: 'да' }));
  transcript.appendSpeech(speech(4, { provenance: 'different-port-label', delta: 'нет' }));
  assert.deepEqual(transcript.snapshot().turns.map(({ role, speakerId, provenance, text }) => [role, speakerId, provenance, text]), [
    ['user', 'human-1', 'admitted-port-label', 'да да'], ['assistant', 'agent', 'admitted-port-label', 'угу'],
    ['user', 'human-2', 'admitted-port-label', 'я'], ['user', 'human-1', 'different-port-label', 'нет'],
  ]);
  assert.deepEqual(transcript.snapshot().turns.map(({ firstSeq, lastSeq, startMs, endMs }) => [firstSeq, lastSeq, startMs, endMs]),
    [[0, 3, 0, 35], [1, 1, 10, 15], [2, 2, 20, 25], [4, 4, 40, 45]]);
  // Text in the earlier human turn now spans the intervening speakers. This is not a global word timeline.
  assert.deepEqual(transcript.snapshot().turns.map(turn => turn.lastSeq), [3, 1, 2, 4]);
});

test('global accepted seq, exact duplicate ID, stale epoch and gap/typed turn boundaries', () => {
  const transcript = new VoiceTranscript(binding());
  assert.equal(transcript.appendSpeech(speech(0)), 'accepted');
  assert.equal(transcript.appendSpeech(speech(1, { eventId: 'event-0' })), 'duplicate');
  assert.equal(transcript.snapshot().seq, 0);
  assert.equal(transcript.appendSpeech(speech(0, { eventId: 'fresh' })), 'stale');
  assert.equal(transcript.appendSpeech(speech(1, { epoch: 'E0' })), 'stale');
  assert.equal(transcript.appendSpeech(speech(1, { startMs: 1605, endMs: 1606 })), 'accepted');
  assert.equal(transcript.snapshot().turns.length, 1);
  transcript.appendSpeech(speech(2, { startMs: 3207, endMs: 3208 }));
  transcript.appendText(typed(3));
  transcript.appendSpeech(speech(4, { startMs: 3210, endMs: 3211 }));
  assert.equal(transcript.snapshot().turns.length, 4);
  assert.deepEqual(transcript.snapshot().turns[2], { id: 'turn-3', role: 'user', speakerId: 'human-1',
    provenance: 'admitted-port-label', text: 'typed', startMs: null, endMs: null, firstSeq: 3, lastSeq: 3 });
  assert.equal(transcript.appendText(typed(5, { seq: Number.MAX_SAFE_INTEGER + 1 })), 'invalid');
});

test('1:1 still requires explicit speaker/provenance; closed schema never invokes getters', () => {
  const transcript = new VoiceTranscript(binding({ scope: { ...scope(), mode: 'one-to-one' } }));
  let reads = 0;
  const accessor = speech(0); Object.defineProperty(accessor, 'delta', { get() { reads++; return 'private'; } });
  for (const value of [accessor, { ...speech(0), extra: true }, { ...speech(0), [Symbol('extra')]: true },
    Object.assign(Object.create({ inherited: true }), speech(0)), { ...speech(0), speakerId: undefined },
    { ...speech(0), provenance: '' }, { ...speech(0), eventId: undefined }]) {
    assert.equal(transcript.appendSpeech(value), 'invalid');
  }
  assert.equal(reads, 0);
  assert.equal(transcript.snapshot().seq, -1);
  assert.equal(transcript.snapshot().eventIdCount, 0);
  assert.equal(transcript.appendSpeech(Object.assign(Object.create(null), speech(0))), 'accepted');
});

test('oversize/malformed UTF16 and nonfinite time reject before UTF8 encoding', () => {
  const transcript = new VoiceTranscript(binding());
  const encode = TextEncoder.prototype.encode;
  let encodes = 0;
  TextEncoder.prototype.encode = function (...args) { encodes++; return encode.apply(this, args); };
  try {
    for (const delta of ['x'.repeat(4001), '\ud800', '\udfff', 'a\ud800b']) assert.equal(transcript.appendSpeech(speech(0, { delta })), 'invalid');
    for (const text of ['x'.repeat(4001), '\ud800']) assert.equal(transcript.appendText(typed(0, { text })), 'invalid');
    assert.equal(transcript.appendText(typed(0, { role: 'assistant', text: 'x'.repeat(9001) })), 'invalid');
    for (const override of [{ startMs: NaN }, { endMs: Infinity }, { endMs: -1 }, { seq: NaN }]) assert.equal(transcript.appendSpeech(speech(0, override)), 'invalid');
    assert.equal(encodes, 0);
  } finally { TextEncoder.prototype.encode = encode; }
  assert.equal(transcript.appendText(typed(0, { role: 'assistant', speakerId: 'agent', text: 'x'.repeat(9000) })), 'accepted');
  assert.equal(transcript.appendSpeech(speech(1, { delta: '😀'.repeat(2000) })), 'accepted');
});

test('24 turns and 64000 UTF8 bytes retain complete Unicode tail, without resurrecting evicted active turn', () => {
  const transcript = new VoiceTranscript(binding());
  transcript.appendSpeech(speech(0, { delta: 'evicted' }));
  for (let seq = 1; seq < 30; seq++) transcript.appendSpeech(speech(seq, { speakerId: `human-${seq}`, delta: 'x' }));
  assert.equal(transcript.snapshot().turns.length, 24);
  transcript.appendSpeech(speech(30, { delta: 'new' }));
  assert.equal(transcript.snapshot().turns.at(-1).text, 'new');
  for (let seq = 31; seq < 41; seq++) transcript.appendSpeech(speech(seq, { delta: '😀'.repeat(2000) }));
  const snapshot = transcript.snapshot();
  assert.equal(snapshot.turns.length, 1);
  assert.equal(snapshot.bytes, 64000);
  assert.equal(snapshot.turns[0].text, '😀'.repeat(16000));
  assert.equal(Buffer.byteLength(snapshot.turns[0].text), 64000);
});

test('2048 event IDs remain deduplicated after transcript trim; cap fails closed until terminal end', () => {
  const transcript = new VoiceTranscript(binding());
  for (let seq = 0; seq < 2048; seq++) assert.equal(transcript.appendSpeech(speech(seq, { delta: 'x' })), 'accepted');
  assert.equal(transcript.appendSpeech(speech(2048)), 'capacity');
  assert.equal(transcript.appendSpeech(speech(2048, { eventId: 'event-0' })), 'duplicate');
  assert.equal(transcript.snapshot().eventIdCount, 2048);
  assert.equal(transcript.snapshot().seq, 2047);
  const noEvent = speech(2048); delete noEvent.eventId;
  assert.equal(transcript.appendSpeech(noEvent), 'accepted');
  transcript.end();
  assert.equal(transcript.snapshot().eventIdCount, 0);
  assert.equal(transcript.snapshot().turns.length, 0);
  assert.equal(transcript.snapshot().bytes, 0);
  assert.equal(transcript.appendSpeech(speech(2049)), 'closed');
  assert.equal(transcript.hasHumanContext(), false);
});

test('UTF8 tail cutoff skips continuation bytes rather than introducing replacement characters', () => {
  const transcript = new VoiceTranscript(binding());
  for (let seq = 0; seq < 6; seq++) transcript.appendSpeech(speech(seq, { delta: '汉'.repeat(4000) }));
  assert.equal(transcript.snapshot().bytes, 63999);
  assert.equal(transcript.snapshot().turns[0].text, '汉'.repeat(21333));
});

test('scope is closed immutable metadata copied from lifecycle fields, with no accessors', () => {
  const source = scope(), transcript = new VoiceTranscript({ scope: source, epoch: 'E1' });
  source.accountId = 'changed';
  assert.equal(transcript.snapshot().scope.accountId, 'A');
  assert.ok(Object.isFrozen(transcript.snapshot().scope));
  let reads = 0;
  const accessor = scope(); Object.defineProperty(accessor, 'accountId', { get() { reads++; return 'private'; } });
  for (const value of [accessor, { ...scope(), extra: true }, { ...scope(), generation: NaN },
    { ...scope(), generation: 0 }, { ...scope(), mode: 'other' }, { ...scope(), accountId: '\ud800' }]) {
    assert.throws(() => new VoiceTranscript({ scope: value, epoch: 'E1' }), /invalid_binding_data/);
  }
  assert.equal(reads, 0);
  assert.throws(() => new VoiceTranscript(binding({ epoch: '' })), /invalid_binding_data/);
  transcript.appendText(typed(0));
  assert.throws(() => { transcript.snapshot().turns[0].text = 'changed'; }, TypeError);
});

test('no human context does not run; new text only refreshes existing task after full quiet window', async () => {
  const transcript = new VoiceTranscript(binding());
  const f = fixture({ hasHumanContext: () => transcript.hasHumanContext() });
  assert.equal(f.scheduler.receive(task()), 'queued');
  f.time.advance(5000);
  assert.equal(f.runs.length, 0);
  transcript.appendSpeech(speech(0)); f.scheduler.inputChanged('E1');
  f.time.advance(1000);
  transcript.appendSpeech(speech(1)); f.scheduler.inputChanged('E1');
  f.time.advance(1599); assert.equal(f.runs.length, 0);
  f.time.advance(1); assert.equal(f.runs.length, 1);
  await settle(); assert.equal(f.scheduler.status('T1'), 'completed');
  transcript.appendText(typed(2)); f.scheduler.inputChanged('E1'); f.time.advance(5000);
  assert.equal(f.runs.length, 1);
});

test('pending coalescing and 100 IDs keep old dedup, no FIFO forgetting', () => {
  const f = fixture();
  for (let id = 0; id < 100; id++) assert.equal(f.scheduler.receive(task(`T${id}`)), 'queued');
  assert.equal(f.scheduler.snapshot().pendingId, 'T99');
  assert.equal(f.scheduler.status('T0'), 'superseded');
  assert.equal(f.scheduler.receive(task('T100')), 'capacity');
  assert.equal(f.scheduler.receive(task('T0')), 'duplicate');
  assert.equal(f.scheduler.snapshot().statusCount, 100);
  assert.equal(f.time.count(), 1);
  f.scheduler.end(); assert.equal(f.time.count(), 0);
  assert.equal(f.scheduler.snapshot().statusCount, 0);
});

test('cancel/newest pending retains one aborted native job until settlement, late failure cannot change status', async () => {
  const old = deferred(), f = fixture({ run(value, signal) { f.runs.push({ value, signal }); return value.id === 'old' ? old.promise : undefined; } });
  f.scheduler.receive(task('old')); f.time.advance(1600);
  const signal = f.runs[0].signal;
  f.scheduler.receive(task('next')); f.scheduler.receive(task('newest'));
  assert.equal(signal.aborted, true);
  assert.equal(f.scheduler.snapshot().runningId, 'old');
  assert.equal(f.scheduler.snapshot().retainedRunning, true);
  assert.equal(f.scheduler.snapshot().pendingId, 'newest');
  f.time.advance(20_000); assert.equal(f.runs.length, 1);
  old.reject(new Error('late private port error')); await settle();
  assert.equal(f.scheduler.status('old'), 'superseded'); assert.equal(f.errors.length, 0);
  f.time.advance(1599); assert.equal(f.runs.length, 1);
  f.time.advance(1); assert.equal(f.runs.length, 2);
  await settle(); assert.equal(f.scheduler.status('newest'), 'completed');
});

test('inputChanged requeues only same existing ID and still retains aborted work', async () => {
  const old = deferred(), f = fixture({ run(value, signal) { f.runs.push({ value, signal }); return f.runs.length === 1 ? old.promise : undefined; } });
  f.scheduler.receive(task()); f.time.advance(1600);
  assert.equal(f.scheduler.inputChanged('E1'), 'updated');
  assert.equal(f.scheduler.status('T1'), 'pending');
  assert.equal(f.scheduler.snapshot().runningId, 'T1');
  f.time.advance(10_000); assert.equal(f.runs.length, 1);
  old.resolve(); await settle();
  f.time.advance(1600); assert.equal(f.runs.length, 2);
  await settle(); assert.equal(f.scheduler.status('T1'), 'completed');
});

test('cancel is prompt but retained running remains until actual settlement', async () => {
  const work = deferred(), f = fixture({ run: () => work.promise });
  f.scheduler.receive(task()); f.time.advance(1600);
  assert.equal(f.scheduler.cancel('E0'), 'stale');
  assert.equal(f.scheduler.cancel('E1'), 'cancelled');
  assert.equal(f.scheduler.status('T1'), 'cancelled');
  assert.equal(f.scheduler.snapshot().retainedRunning, true);
  assert.equal(f.scheduler.receive(task()), 'duplicate');
  work.resolve(); await settle();
  assert.equal(f.scheduler.status('T1'), 'cancelled');
  assert.equal(f.scheduler.snapshot().retainedRunning, false);
});

test('old epoch input/task/result and equal IDs across two instances cannot cross scope', async () => {
  const work = deferred(), old = fixture({ run: () => work.promise });
  old.scheduler.receive(task()); old.time.advance(1600); old.scheduler.end();
  const fresh = fixture({ epoch: 'E2', scope: { ...scope(), accountId: 'B', generation: 2 } });
  assert.equal(fresh.scheduler.receive(task()), 'stale');
  assert.equal(fresh.scheduler.inputChanged('E1'), 'stale');
  assert.equal(fresh.scheduler.receive(task('T1', { epoch: 'E2' })), 'queued');
  fresh.time.advance(1600); await settle();
  work.resolve(); await settle();
  assert.equal(fresh.scheduler.status('T1'), 'completed');
  assert.equal(fresh.runs[0].value.scope.accountId, 'B');
  assert.equal(old.scheduler.status('T1'), undefined);
  assert.equal(old.scheduler.receive(task()), 'closed');
  const one = new VoiceTranscript(binding()), two = new VoiceTranscript(binding({ epoch: 'E2' }));
  one.appendSpeech(speech(0)); assert.equal(two.snapshot().turns.length, 0);
  assert.equal(two.appendSpeech(speech(1)), 'stale');
  one.end(); assert.equal(one.snapshot().turns.length, 0);
});

test('native message getters/unknown fields/NaN fail closed without consuming status IDs', () => {
  const f = fixture(); let reads = 0;
  const accessor = task(); Object.defineProperty(accessor, 'id', { get() { reads++; return 'private'; } });
  for (const value of [accessor, { ...task(), extra: 1 }, { ...task(), offsetMs: NaN },
    { ...task(), offsetMs: -1 }, { ...task(), id: '\ud800' }, { ...task(), epoch: undefined }]) {
    assert.equal(f.scheduler.receive(value), 'invalid');
  }
  assert.equal(reads, 0); assert.equal(f.scheduler.snapshot().statusCount, 0);
});

test('only literal true context admits; false-like/truthy/Promise rejection is observed without run', async () => {
  for (const context of [false, 1, 'true', {}, Promise.resolve(true), Promise.reject(new Error('context failed'))]) {
    const f = fixture({ hasHumanContext: () => context });
    assert.equal(f.scheduler.receive(task()), 'queued'); f.time.advance(20_000);
    assert.equal(f.runs.length, 0); assert.equal(f.scheduler.status('T1'), 'pending'); f.scheduler.end();
  }
  const f = fixture({ hasHumanContext() { throw new Error('context failed'); } });
  f.scheduler.receive(task()); f.time.advance(20_000); assert.equal(f.runs.length, 0);
  await settle();
});

test('context revoked before timer fire cannot dispatch; later refreshed context starts a new quiet window', async () => {
  let admitted = true; const f = fixture({ hasHumanContext: () => admitted });
  f.scheduler.receive(task()); admitted = false; f.time.advance(1600);
  assert.equal(f.runs.length, 0);
  admitted = true; f.scheduler.inputChanged('E1'); f.time.advance(1600);
  assert.equal(f.runs.length, 1); await settle();
});

test('reentrant hasHumanContext end/cancel is fenced after port call', () => {
  for (const action of ['end', 'cancel']) {
    let scheduler, calls = 0;
    const f = fixture({ hasHumanContext() { calls++; action === 'end' ? scheduler.end() : scheduler.cancel('E1'); return true; } });
    scheduler = f.scheduler;
    scheduler.receive(task()); f.time.advance(10_000);
    assert.equal(calls, 1); assert.equal(f.runs.length, 0); assert.equal(f.time.count(), 0);
    assert.equal(scheduler.snapshot().pendingId, null);
  }
});

test('reentrant receive/inputChanged from context and run is rejected as transition, no duplicate dispatch', async () => {
  let scheduler; const results = [], time = clock(), runs = [];
  scheduler = new NativeDelegationScheduler(binding({ clock: time,
    hasHumanContext() { results.push(scheduler.receive(task('recursive')), scheduler.inputChanged('E1')); return true; },
    run() { runs.push(1); results.push(scheduler.receive(task('recursive')), scheduler.inputChanged('E1')); } }));
  scheduler.receive(task()); const duplicateCallbacks = time.callbacks(); time.advance(1600);
  duplicateCallbacks[0](); duplicateCallbacks[0](); await settle();
  assert.equal(runs.length, 1); assert.ok(results.every(result => result === 'transition'));
  assert.equal(scheduler.snapshot().statusCount, 1);
});

test('run can end synchronously, but its unresolved work still owns slot and cannot revive', async () => {
  const work = deferred(); let scheduler, signal;
  const f = fixture({ run(value, currentSignal) { signal = currentSignal; scheduler.end(); return work.promise; } });
  scheduler = f.scheduler; scheduler.receive(task()); f.time.advance(1600);
  assert.equal(signal.aborted, true); assert.equal(scheduler.snapshot().closed, true);
  assert.equal(scheduler.snapshot().retainedRunning, true); assert.equal(scheduler.snapshot().statusCount, 0);
  work.reject(new Error('late')); await settle();
  assert.equal(scheduler.snapshot().retainedRunning, false); assert.equal(f.errors.length, 0);
});

test('sync throw/rejected run errors settle once; reentrant onError end prevents replacement dispatch', async () => {
  for (const asyncFailure of [false, true]) {
    let scheduler, errors = 0;
    const f = fixture({ run() { if (asyncFailure) return Promise.reject(new Error('private')); throw new Error('private'); },
      onError() { errors++; scheduler.end(); return Promise.reject(new Error('secondary')); } });
    scheduler = f.scheduler; scheduler.receive(task()); f.time.advance(1600); await settle();
    assert.equal(errors, 1); assert.equal(scheduler.snapshot().closed, true);
    assert.equal(scheduler.snapshot().retainedRunning, false);
  }
});

test('clock reentrancy at now/setTimeout/clearTimeout fences work and clears orphan registration', () => {
  for (const phase of ['now', 'setTimeout', 'clearTimeout']) {
    const time = clock(); let scheduler, armed = false;
    const custom = {
      now() { if (armed && phase === 'now') scheduler.end(); return time.now(); },
      setTimeout(callback, delay) { const handle = time.setTimeout(callback, delay); if (armed && phase === 'setTimeout') scheduler.end(); return handle; },
      clearTimeout(handle) { time.clearTimeout(handle); if (armed && phase === 'clearTimeout') scheduler.end(); },
    };
    const runs = [];
    scheduler = new NativeDelegationScheduler(binding({ clock: custom, hasHumanContext: () => true, run: () => runs.push(1) }));
    if (phase === 'clearTimeout') scheduler.receive(task('first'));
    armed = true; scheduler.receive(task('second')); time.advance(20_000);
    assert.equal(scheduler.snapshot().closed, true); assert.equal(runs.length, 0); assert.equal(time.count(), 0);
  }
});

test('synchronous/early/duplicate clock callbacks cannot run before quiet deadline or twice', async () => {
  const time = clock(); let runCount = 0;
  const custom = { now: time.now, clearTimeout: time.clearTimeout,
    setTimeout(callback, delay) { callback(); return time.setTimeout(callback, delay); } };
  const scheduler = new NativeDelegationScheduler(binding({ clock: custom, hasHumanContext: () => true, run: () => runCount++ }));
  scheduler.receive(task()); const callback = time.callbacks()[0]; callback();
  assert.equal(runCount, 0);
  assert.equal(time.count(), 1);
  scheduler.inputChanged('E1'); assert.equal(time.count(), 1);
  const stale = time.callbacks()[0]; time.advance(1600); stale(); await settle();
  assert.equal(runCount, 1);
});

test('default clock handles wall adjustment after quiet deadline without stranding native task', async () => {
  const { spawnSync } = await import('node:child_process');
  const source = new URL('./turn.mjs', import.meta.url).href;
  const script = `
    import assert from 'node:assert/strict';
    const { NativeDelegationScheduler } = await import(process.argv[1]);
    const originalDate = Date.now, originalSet = globalThis.setTimeout, originalClear = globalThis.clearTimeout;
    const originalPerformance = Object.getOwnPropertyDescriptor(globalThis, 'performance');
    let elapsed = 0, wall = 10000, next = 0, runs = 0, scheduler, observation;
    const timers = new Map();
    Date.now = () => wall;
    Object.defineProperty(globalThis, 'performance', { configurable: true, value: { now() { assert.equal(this, globalThis.performance); return elapsed; } } });
    globalThis.setTimeout = (callback, delay) => { const id = ++next; timers.set(id, { callback, at: elapsed + delay }); return id; };
    globalThis.clearTimeout = id => timers.delete(id);
    try {
      scheduler = new NativeDelegationScheduler({ scope: { accountId: 'A', deviceId: 'D', roomId: 'R', mode: 'group', generation: 1 }, epoch: 'E1', hasHumanContext: () => true, run: () => { runs++; } });
      const receive = scheduler.receive({ epoch: 'E1', id: 'native-1', offsetMs: 0 });
      elapsed = 1600; wall = 10600;
      for (const [id, timer] of [...timers]) if (timer.at <= elapsed) { timers.delete(id); timer.callback(); }
      const atQuietDeadline = { runs, remainingTimers: timers.size, snapshot: scheduler.snapshot() };
      elapsed = 10000; wall = 20000;
      for (const [id, timer] of [...timers]) if (timer.at <= elapsed) { timers.delete(id); timer.callback(); }
      await Promise.resolve(); await Promise.resolve();
      observation = { noCustomClock: true, receive, actualQuietElapsedMs: 1600, wallAdjustmentMs: -1000, atQuietDeadline,
        afterCatchup: { runs, remainingTimers: timers.size, snapshot: scheduler.snapshot() } };
    } finally {
      scheduler?.end(); Date.now = originalDate; globalThis.setTimeout = originalSet; globalThis.clearTimeout = originalClear;
      Object.defineProperty(globalThis, 'performance', originalPerformance);
    }
    console.log(JSON.stringify(observation));
    assert.equal(observation.afterCatchup.runs, 1, 'Default runtime clock must not strand a native task after an ordinary wall-clock adjustment');
    assert.equal(observation.atQuietDeadline.runs, 1);
    assert.equal(observation.afterCatchup.remainingTimers, 0);
    assert.equal(observation.afterCatchup.snapshot.pendingId, null);
  `;
  const result = spawnSync(process.execPath, ['--input-type=module', '--eval', script, source], { encoding: 'utf8', timeout: 5000, maxBuffer: 64_000 });
  assert.equal(result.error, undefined);
  assert.equal(result.status, 0, result.stderr);
  const observation = JSON.parse(result.stdout);
  assert.equal(observation.noCustomClock, true);
  assert.equal(observation.actualQuietElapsedMs, 1600);
  assert.equal(observation.wallAdjustmentMs, -1000);
  assert.equal(observation.atQuietDeadline.runs, 1);
  assert.equal(observation.afterCatchup.runs, 1);
});

test('default clock absence/nonfinite/throw fails closed without Date fallback', async () => {
  const { spawnSync } = await import('node:child_process');
  const source = new URL('./turn.mjs', import.meta.url).href;
  const script = `
    import assert from 'node:assert/strict';
    const { NativeDelegationScheduler } = await import(process.argv[1]);
    const originalPerformance = Object.getOwnPropertyDescriptor(globalThis, 'performance'), originalDate = Date.now;
    let wallReads = 0;
    Date.now = () => { wallReads++; return 10000; };
    try {
      for (const performance of [undefined, { now: () => NaN }, { now: () => Infinity }, { now() { throw new Error('clock unavailable'); } }]) {
        Object.defineProperty(globalThis, 'performance', { configurable: true, value: performance });
        const scheduler = new NativeDelegationScheduler({ scope: { accountId: 'A', deviceId: 'D', roomId: 'R', mode: 'group', generation: 1 }, epoch: 'E1', hasHumanContext: () => true, run: () => assert.fail('invalid clock ran work') });
        try {
          assert.equal(scheduler.receive({ epoch: 'E1', id: 'native-1', offsetMs: 0 }), 'invalid');
          assert.equal(scheduler.snapshot().statusCount, 0);
          assert.equal(scheduler.snapshot().pendingId, null);
        } finally { scheduler.end(); }
      }
      assert.equal(wallReads, 0);
    } finally { Date.now = originalDate; Object.defineProperty(globalThis, 'performance', originalPerformance); }
  `;
  const result = spawnSync(process.execPath, ['--input-type=module', '--eval', script, source], { encoding: 'utf8', timeout: 5000, maxBuffer: 64_000 });
  assert.equal(result.error, undefined);
  assert.equal(result.status, 0, result.stderr);
});

test('consumed early one-shot timer preserves a future dispatch without running before quiet deadline', async () => {
  let now = 0, id = 0, runs = 0;
  const timers = new Map();
  const custom = { now: () => now,
    setTimeout(callback, delay) { const handle = ++id; timers.set(handle, { callback, at: now + delay }); return handle; },
    clearTimeout(handle) { timers.delete(handle); } };
  const scheduler = new NativeDelegationScheduler(binding({ clock: custom, hasHumanContext: () => true, run: () => { runs++; } }));
  assert.equal(scheduler.receive(task('native-1')), 'queued');
  const [handle, timer] = [...timers][0];
  now = 1599.6; timers.delete(handle); timer.callback();
  assert.equal(runs, 0); assert.equal(timers.size, 1);
  const rearmed = [...timers.values()][0];
  assert.ok(rearmed.at >= 1600); assert.ok(rearmed.at <= 1600.6);
  timer.callback(); assert.equal(timers.size, 1);
  now = 1600;
  for (const [key, value] of [...timers]) if (value.at <= now) { timers.delete(key); value.callback(); }
  assert.equal(runs, 0);
  now = 2000;
  for (const [key, value] of [...timers]) if (value.at <= now) { timers.delete(key); value.callback(); }
  await settle();
  assert.equal(runs, 1, 'A consumed early timer must preserve a future dispatch opportunity, without running before quiet deadline');
  assert.equal(timers.size, 0); assert.equal(scheduler.snapshot().pendingId, null);
  assert.equal(scheduler.status('native-1'), 'completed'); scheduler.end();
});

test('rearmed early timer is fenced by newer input and end, with only one registration retained', () => {
  let now = 0, id = 0, runs = 0;
  const timers = new Map();
  const custom = { now: () => now,
    setTimeout(callback, delay) { const handle = ++id; timers.set(handle, { callback, at: now + delay }); return handle; },
    clearTimeout(handle) { timers.delete(handle); } };
  const scheduler = new NativeDelegationScheduler(binding({ clock: custom, hasHumanContext: () => true, run: () => { runs++; } }));
  scheduler.receive(task()); const [handle, first] = [...timers][0];
  now = 1599.6; timers.delete(handle); first.callback();
  const stale = [...timers.values()][0].callback;
  scheduler.inputChanged('E1'); assert.equal(timers.size, 1);
  now = 2000; stale(); assert.equal(runs, 0); assert.equal(timers.size, 1);
  const latest = [...timers.values()][0].callback;
  scheduler.end(); assert.equal(timers.size, 0);
  now = 10000; first.callback(); stale(); latest(); assert.equal(runs, 0);
});

test('consumed early rearm cannot read retired context after clearTimeout ends instance', () => {
  let now = 0, id = 0, armed = false, retiredContextReads = 0, runs = 0, scheduler;
  const timers = new Map();
  const custom = { now: () => now,
    setTimeout(callback, delay) { const handle = ++id; timers.set(handle, { callback, at: now + delay }); return handle; },
    clearTimeout(handle) { timers.delete(handle); if (armed) scheduler.end(); } };
  scheduler = new NativeDelegationScheduler(binding({ clock: custom,
    hasHumanContext() { if (scheduler.snapshot().closed) retiredContextReads++; return true; },
    run() { runs++; } }));
  scheduler.receive(task('native-1')); const [handle, timer] = [...timers][0];
  now = 1599.6; armed = true; timers.delete(handle); timer.callback();
  assert.equal(retiredContextReads, 0, 'A terminal instance must not invoke its retired context port after a cleanup callback ended it');
  assert.equal(runs, 0); assert.equal(scheduler.snapshot().closed, true);
  assert.equal(timers.size, 0); assert.equal(scheduler.snapshot().pendingId, null);
  now = 2000; timer.callback(); assert.equal(retiredContextReads, 0); assert.equal(runs, 0);
});

test('consumed early rearm cannot read cancelled task context after clearTimeout cancels it', () => {
  let now = 0, id = 0, armed = false, retired = false, retiredContextReads = 0, runs = 0, scheduler;
  const timers = new Map();
  const custom = { now: () => now,
    setTimeout(callback, delay) { const handle = ++id; timers.set(handle, { callback, at: now + delay }); return handle; },
    clearTimeout(handle) { timers.delete(handle); if (armed) { retired = true; scheduler.cancel('E1'); } } };
  scheduler = new NativeDelegationScheduler(binding({ clock: custom,
    hasHumanContext() { if (retired) retiredContextReads++; return true; }, run() { runs++; } }));
  scheduler.receive(task()); const [handle, timer] = [...timers][0];
  now = 1599.6; armed = true; timers.delete(handle); timer.callback();
  assert.equal(retiredContextReads, 0); assert.equal(runs, 0); assert.equal(timers.size, 0);
  assert.equal(scheduler.snapshot().closed, false); assert.equal(scheduler.status('T1'), 'cancelled');
  assert.equal(scheduler.snapshot().pendingId, null); scheduler.end();
});
