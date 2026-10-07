// Technical turn state only. Scope, epoch, and speaker labels are data, not authority.
const GAP_MS = 1600;
const MAX_TURNS = 24;
const MAX_BYTES = 64_000;
const MAX_EVENTS = 2048;
const MAX_TASKS = 100;
const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { ignoreBOM: true });
const ROLES = new Set(['user', 'assistant']);
const SCOPE_KEYS = ['accountId', 'deviceId', 'roomId', 'mode', 'generation'];
const SPEECH_KEYS = ['epoch', 'seq', 'eventId', 'role', 'speakerId', 'provenance', 'delta', 'startMs', 'endMs'];
const TEXT_KEYS = ['epoch', 'seq', 'role', 'speakerId', 'provenance', 'text'];

// Read descriptors, never message getters. Symbols, inherited fields and extras fail closed.
function dataRecord(value, allowed, required = allowed) {
  if (value === null || typeof value !== 'object') return null;
  try {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== null && prototype !== Object.prototype) return null;
    const keys = Reflect.ownKeys(value);
    if (keys.length > allowed.length || keys.some(key => typeof key !== 'string' || !allowed.includes(key))) return null;
    const record = Object.create(null);
    for (const key of keys) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor || !Object.hasOwn(descriptor, 'value')) return null;
      record[key] = descriptor.value;
    }
    return required.every(key => Object.hasOwn(record, key)) ? record : null;
  } catch { return null; }
}

function text(value, max) {
  if (typeof value !== 'string' || value.length === 0 || value.length > max) return false;
  // Check bounded UTF-16 before allocating UTF-8. Lone surrogates cannot be repaired silently.
  for (let index = 0; index < value.length; index++) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(++index);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) return false;
  }
  return true;
}

function binding(scope, epoch) {
  const copy = dataRecord(scope, SCOPE_KEYS);
  if (!copy || !text(copy.accountId, 256) || !text(copy.deviceId, 256) || !text(copy.roomId, 256)
    || !['one-to-one', 'group'].includes(copy.mode)
    || !Number.isSafeInteger(copy.generation) || copy.generation < 1 || !text(epoch, 128)) {
    throw new TypeError('invalid_binding_data');
  }
  return Object.freeze({ scope: Object.freeze({ ...copy }), epoch });
}

export class VoiceTranscript {
  #binding;
  #turns = [];
  #active = new Map();
  #events = new Set();
  #seq = -1;
  #bytes = 0;
  #closed = false;

  constructor({ scope, epoch }) { this.#binding = binding(scope, epoch); }

  #event(value, keys, required) {
    const event = dataRecord(value, keys, required);
    if (this.#closed) return { code: 'closed' };
    if (!event || !text(event.epoch, 128) || !Number.isSafeInteger(event.seq) || event.seq < 0
      || !ROLES.has(event.role) || !text(event.speakerId, 256) || !text(event.provenance, 256)) return { code: 'invalid' };
    if (event.epoch !== this.#binding.epoch || event.seq <= this.#seq) return { code: 'stale' };
    return { event };
  }

  appendSpeech(value) {
    if (this.#closed) return 'closed';
    const parsed = this.#event(value, SPEECH_KEYS, SPEECH_KEYS.filter(key => key !== 'eventId'));
    if (!parsed.event) return parsed.code;
    const event = parsed.event;
    if (!text(event.delta, 4000) || !Number.isFinite(event.startMs) || !Number.isFinite(event.endMs)
      || event.startMs < 0 || event.endMs < event.startMs
      || (Object.hasOwn(event, 'eventId') && !text(event.eventId, 256))) return 'invalid';
    if (event.eventId !== undefined && this.#events.has(event.eventId)) return 'duplicate';
    if (event.eventId !== undefined && this.#events.size >= MAX_EVENTS) return 'capacity';
    const key = JSON.stringify([event.role, event.speakerId, event.provenance]);
    const previous = this.#active.get(key);
    const continues = previous && event.startMs - previous.endMs <= GAP_MS;
    const turn = continues ? previous.turn : {
      id: `turn-${event.seq}`, role: event.role, speakerId: event.speakerId, provenance: event.provenance, text: '',
      startMs: event.startMs, endMs: event.endMs, firstSeq: event.seq, lastSeq: event.seq,
    };
    if (!continues) this.#turns.push(turn);
    turn.text += event.delta;
    turn.startMs = Math.min(turn.startMs, event.startMs);
    turn.endMs = Math.max(turn.endMs, event.endMs);
    turn.lastSeq = event.seq;
    this.#active.set(key, { turn, endMs: continues ? Math.max(previous.endMs, event.endMs) : event.endMs });
    if (event.eventId !== undefined) this.#events.add(event.eventId);
    this.#seq = event.seq;
    this.#trim();
    return 'accepted';
  }

  appendText(value) {
    if (this.#closed) return 'closed';
    const parsed = this.#event(value, TEXT_KEYS, TEXT_KEYS);
    if (!parsed.event) return parsed.code;
    const event = parsed.event;
    if (!text(event.text, event.role === 'assistant' ? 9000 : 4000)) return 'invalid';
    this.#active.clear();
    this.#turns.push({ id: `turn-${event.seq}`, role: event.role, speakerId: event.speakerId,
      provenance: event.provenance, text: event.text, startMs: null, endMs: null, firstSeq: event.seq, lastSeq: event.seq });
    this.#seq = event.seq;
    this.#trim();
    return 'accepted';
  }

  #trim() {
    while (this.#turns.length > MAX_TURNS) this.#turns.shift();
    let bytes = this.#turns.reduce((sum, turn) => sum + encoder.encode(turn.text).byteLength, 0);
    while (bytes > MAX_BYTES && this.#turns.length > 1) bytes -= encoder.encode(this.#turns.shift().text).byteLength;
    if (bytes > MAX_BYTES) {
      const encoded = encoder.encode(this.#turns[0].text);
      let start = encoded.byteLength - MAX_BYTES;
      while ((encoded[start] & 0xc0) === 0x80) start++;
      this.#turns[0].text = decoder.decode(encoded.subarray(start));
      bytes = encoded.byteLength - start;
    }
    this.#bytes = bytes;
    for (const [key, segment] of this.#active) if (!this.#turns.includes(segment.turn)) this.#active.delete(key);
  }

  hasHumanContext() { return !this.#closed && this.#turns.some(turn => turn.role === 'user' && turn.text.trim().length > 0); }

  snapshot() {
    return Object.freeze({ ...this.#binding, closed: this.#closed, seq: this.#seq, bytes: this.#bytes,
      eventIdCount: this.#events.size, turns: Object.freeze(this.#turns.map(turn => Object.freeze({ ...turn }))) });
  }

  end() { this.#closed = true; this.#turns = []; this.#active.clear(); this.#events.clear(); this.#seq = -1; this.#bytes = 0; }
}

const realClock = Object.freeze({ now: () => globalThis.performance.now(),
  setTimeout: (callback, delay) => globalThis.setTimeout(callback, delay),
  clearTimeout: handle => globalThis.clearTimeout(handle) });

// A nonboolean context result is never an admission. Observe rejected async port values.
function discardAsync(value) {
  if (value !== null && (typeof value === 'object' || typeof value === 'function')) {
    try { void Promise.resolve(value).catch(() => {}); } catch {}
  }
}

export class NativeDelegationScheduler {
  #binding;
  #ports;
  #clock;
  #statuses = new Map();
  #pending = null;
  #running = null;
  #timer = null;
  #lastInputAt;
  #lastTime = 0;
  #version = 0;
  #callbackDepth = 0;
  #closed = false;

  constructor({ scope, epoch, hasHumanContext, run, onError = () => {}, clock = realClock }) {
    this.#binding = binding(scope, epoch);
    if (typeof hasHumanContext !== 'function' || typeof run !== 'function' || typeof onError !== 'function'
      || typeof clock?.now !== 'function' || typeof clock?.setTimeout !== 'function' || typeof clock?.clearTimeout !== 'function') {
      throw new TypeError('invalid_trusted_ports');
    }
    this.#ports = { hasHumanContext, run, onError };
    this.#clock = { now: clock.now.bind(clock), setTimeout: clock.setTimeout.bind(clock), clearTimeout: clock.clearTimeout.bind(clock) };
  }

  #call(callback) {
    this.#callbackDepth++;
    try { return callback(); }
    finally { this.#callbackDepth--; }
  }

  #now(version) {
    let now;
    try { now = this.#call(() => this.#clock.now()); } catch { return null; }
    if (typeof now !== 'number') discardAsync(now);
    if (this.#closed || this.#version !== version || !Number.isFinite(now) || now < this.#lastTime) return null;
    this.#lastTime = now;
    return now;
  }

  #clearTimer() {
    const timer = this.#timer;
    this.#timer = null;
    if (timer?.registered) {
      try { discardAsync(this.#call(() => this.#clock.clearTimeout(timer.handle))); } catch {}
    }
  }

  #abortRunning() {
    const running = this.#running;
    if (running && !running.controller.signal.aborted) this.#call(() => running.controller.abort());
  }

  receive(value) {
    if (this.#closed) return 'closed';
    if (this.#callbackDepth) return 'transition';
    const version = this.#version;
    const task = dataRecord(value, ['epoch', 'id', 'offsetMs']);
    if (this.#closed || this.#version !== version) return 'stale';
    if (!task || !text(task.epoch, 128) || !text(task.id, 256) || !Number.isFinite(task.offsetMs) || task.offsetMs < 0) return 'invalid';
    if (task.epoch !== this.#binding.epoch) return 'stale';
    if (this.#statuses.has(task.id)) return 'duplicate';
    if (this.#statuses.size >= MAX_TASKS) return 'capacity';
    const now = this.#now(version);
    if (now === null) return this.#closed ? 'closed' : 'invalid';
    const nextVersion = ++this.#version;
    if (this.#pending) this.#statuses.set(this.#pending.task.id, 'superseded');
    if (this.#running?.valid) { this.#running.valid = false; this.#statuses.set(this.#running.task.id, 'superseded'); }
    const immutableTask = Object.freeze({ ...task, scope: this.#binding.scope });
    this.#pending = { task: immutableTask, receivedAt: now };
    this.#statuses.set(task.id, 'pending');
    this.#clearTimer();
    this.#abortRunning();
    if (!this.#closed && this.#version === nextVersion) this.#schedule();
    return this.#closed ? 'closed' : 'queued';
  }

  inputChanged(epoch) {
    if (this.#closed) return 'closed';
    if (this.#callbackDepth) return 'transition';
    if (epoch !== this.#binding.epoch) return 'stale';
    const now = this.#now(this.#version);
    if (now === null) return this.#closed ? 'closed' : 'invalid';
    const version = ++this.#version;
    this.#lastInputAt = now;
    if (this.#running?.valid) {
      this.#running.valid = false;
      if (!this.#pending) this.#pending = { task: this.#running.task, receivedAt: now };
      this.#statuses.set(this.#running.task.id, 'pending');
    }
    this.#clearTimer();
    this.#abortRunning();
    if (!this.#closed && this.#version === version) this.#schedule();
    return this.#closed ? 'closed' : 'updated';
  }

  cancel(epoch) {
    if (this.#closed) return 'closed';
    if (epoch !== this.#binding.epoch) return 'stale';
    this.#version++;
    if (this.#pending) this.#statuses.set(this.#pending.task.id, 'cancelled');
    this.#pending = null;
    if (this.#running?.valid) { this.#running.valid = false; this.#statuses.set(this.#running.task.id, 'cancelled'); }
    this.#clearTimer();
    this.#abortRunning();
    return 'cancelled';
  }

  #context(pending, version) {
    if (this.#closed || this.#version !== version || this.#pending !== pending || this.#running) return false;
    let result;
    try { result = this.#call(() => this.#ports.hasHumanContext(pending.task)); }
    catch { result = false; }
    if (result !== true) discardAsync(result);
    return !this.#closed && this.#version === version && this.#pending === pending && !this.#running && result === true;
  }

  #schedule() {
    this.#clearTimer();
    const pending = this.#pending;
    const version = this.#version;
    if (this.#closed || !pending || this.#running) return;
    if (!this.#context(pending, version)) {
      if (this.#pending === pending) pending.contextReadyAt = undefined;
      return;
    }
    const now = this.#now(version);
    if (now === null || this.#pending !== pending || this.#running) return;
    pending.contextReadyAt ??= now;
    const dueAt = Math.max(pending.receivedAt, pending.contextReadyAt, this.#lastInputAt ?? 0) + GAP_MS;
    this.#arm(pending, version, dueAt, now);
  }

  #arm(pending, version, dueAt, now) {
    const timer = { registered: false, handle: undefined, version, pending, dueAt };
    this.#timer = timer;
    try {
      timer.handle = this.#call(() => this.#clock.setTimeout(() => this.#fire(timer), Math.max(0, Math.ceil(dueAt - now))));
      timer.registered = true;
    } catch { if (this.#timer === timer) this.#timer = null; return; }
    if (this.#closed || this.#version !== version || this.#timer !== timer) {
      try { discardAsync(this.#call(() => this.#clock.clearTimeout(timer.handle))); } catch {}
    }
  }

  #fire(timer) {
    if (this.#callbackDepth || this.#closed || this.#timer !== timer || this.#version !== timer.version) return;
    const pending = timer.pending;
    if (!this.#context(pending, timer.version)) {
      if (this.#timer === timer) this.#clearTimer();
      if (this.#pending === pending) pending.contextReadyAt = undefined;
      return;
    }
    const now = this.#now(timer.version);
    if (now === null) { if (this.#timer === timer) this.#clearTimer(); return; }
    if (this.#pending !== pending || this.#running) return;
    if (now < timer.dueAt) {
      // A one-shot timer may already be consumed. Keep one future opportunity,
      // using the original deadline, not a fresh quiet window or early dispatch.
      this.#clearTimer();
      if (this.#context(pending, timer.version)) this.#arm(pending, timer.version, timer.dueAt, now);
      return;
    }
    this.#timer = null;
    const running = { task: pending.task, controller: new AbortController(), valid: true };
    this.#pending = null;
    this.#running = running;
    this.#version++;
    this.#statuses.set(running.task.id, 'running');
    let work;
    try { work = this.#call(() => this.#ports.run(running.task, running.controller.signal)); }
    catch (error) { this.#finish(running, true, error); return; }
    // Cancellation inside run does not free this slot before its returned work settles.
    void Promise.resolve(work).then(() => this.#finish(running, false), error => this.#finish(running, true, error));
  }

  #finish(running, failed, error) {
    if (this.#running !== running) return;
    this.#running = null;
    const version = ++this.#version;
    if (!this.#closed && running.valid) {
      this.#statuses.set(running.task.id, failed ? 'failed' : 'completed');
      if (failed) {
        try { discardAsync(this.#call(() => this.#ports.onError(error, running.task))); } catch {}
      }
    }
    if (!this.#closed && this.#version === version) this.#schedule();
  }

  status(id) { return text(id, 256) ? this.#statuses.get(id) : undefined; }

  snapshot() {
    return Object.freeze({ ...this.#binding, closed: this.#closed, pendingId: this.#pending?.task.id ?? null,
      runningId: this.#running?.task.id ?? null, retainedRunning: this.#running !== null,
      statusCount: this.#statuses.size });
  }

  end() {
    if (this.#closed) return;
    this.#closed = true;
    this.#version++;
    this.#pending = null;
    if (this.#running) this.#running.valid = false;
    this.#statuses.clear();
    this.#clearTimer();
    this.#abortRunning();
  }
}
