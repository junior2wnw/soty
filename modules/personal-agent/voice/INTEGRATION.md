# Technical voice turn — SOURCE only

`turn.mjs` ports the neutral transcript/quiet-window scheduler from
`D:/agent-ufa-advisor/lib/live-dialogue.ts` and the session/event fencing idea from
`live-client-state.ts`. The donor's startup-history prompt, workspace/reply UI,
business data and speech/model defaults are excluded. This component performs no
transport, microphone, model, memory, tool or budget operations and has no UI imports.

Create `VoiceTranscript({scope, epoch})` and
`NativeDelegationScheduler({scope, epoch, hasHumanContext, run, onError?, clock?})`.
Scope is a copied frozen closed record `{accountId, deviceId, roomId, mode,
generation}` matching call lifecycle metadata (`mode`: `one-to-one` or `group`,
positive safe-integer generation). IDs are nonempty well-formed strings <=256
UTF-16 units; epoch is an opaque string <=128. A new session needs new instances
and terminal `end()` on both old instances. Scope/epoch are not cryptographic
authority. Explicit `speakerId` and `provenance` are bounded opaque data labels;
they neither identify nor admit a participant. The host must establish participant
admission and consent for even transient agent transcript processing, plus caller
authorization, independently of these labels.

Transcript accepts only own plain data fields:

- `appendSpeech({epoch, seq, eventId?, role, speakerId, provenance, delta, startMs, endMs})`
- `appendText({epoch, seq, role, speakerId, provenance, text})`
- `snapshot()`, `hasHumanContext()`, `end()`

Role is `user` or `assistant`. A global nonnegative safe-integer `seq` must increase
for accepted events; old sequence/epoch is rejected, with no reorder buffer.
Audio keeps exact received fragments including repeated words and backchannels,
joining only the same role/speaker/provenance within a 1600 ms gap. Typed input
starts a new turn and clears audio grouping. As in the donor, interleaved speakers
and backchannels do not split that speaker's active audio turn: later fragments
can extend an earlier array entry. The snapshot is therefore not a globally
chronological word/event transcript. Each turn retains `firstSeq`/`lastSeq` and
the observed audio envelope `startMs`/`endMs` (null for typed text); these ranges
can overlap. Ranges cover the observed aggregate, even if its older text prefix
has been trimmed, and cannot reconstruct exact per-word timing. A future display
needing that timing requires a separately bounded event representation.
Audio/user text <=4000 UTF-16 units,
assistant typed text <=9000; malformed Unicode, unknown fields and accessors are
rejected before encoding, without invoking message getters. Retention is <=24
turns and 64000 UTF-8 text bytes, trimming whole older turns or a Unicode-safe
tail. Optional event IDs deduplicate across trimming; 2048 distinct IDs is a
fail-closed cap (new IDs rejected, never FIFO-forgotten). Methods return
`accepted`, `invalid`, `stale`, `duplicate`, `capacity` or `closed`. Frozen snapshots
are copies. `end()` erases internally retained text/event IDs and closes forever;
previous snapshots held by callers cannot be revoked, so the host owns their erasure.

Scheduler accepts `receive({epoch,id,offsetMs})` (nonnegative finite offset),
`inputChanged(epoch)`, `cancel(epoch)`, `status(id)`, `snapshot()` and `end()`.
Only an explicitly supplied native ID creates work. Transcript changes can only
refresh an existing task. `hasHumanContext(task)` must return literal `true`
synchronously; truthy values, Promises and throws fail closed, and rejected async
returns are observed. The scheduler waits 1600 ms of quiet after receipt, context
availability and last input. `run(frozenTask, signal)` and `onError(error, task)`
are trusted host ports; task includes the frozen scope. There is no authority or
side-effect admission within this module. Clock ports must provide monotonic
finite milliseconds and ordinary asynchronous timers. The built-in clock uses
`globalThis.performance.now()` with its receiver, without a Date.now fallback;
missing/nonfinite clock values fail closed through the existing clock fence.
This prevents the reproduced wall-clock adjustment from stranding a task after
its one timer has elapsed. [W3C High Resolution Time 3](https://www.w3.org/TR/hr-time-3/)
describes the monotonic/wall-clock distinction; the consulted version is a
Working Draft dated 1 September 2026, not a final Recommendation.
[Node v24.19.0 performance.now](https://nodejs.org/download/release/v24.19.0/docs/api/perf_hooks.html#performancenow)
and [its global performance API](https://nodejs.org/download/release/v24.19.0/docs/api/globals.html#performance)
document the runtime API and receiver. This API choice is an engineering
inference supported by the actual offline counter, not live suspension/latency
qualification. Custom clock contract and the 1600 ms interval are unchanged.
An active one-shot timer delivered before the quiet deadline is consumed and
rearmed once for the rounded-up remaining interval, retaining the same deadline
and checking current context/version. Stale callbacks cannot retain duplicate
timers. The consumed-early regression is a synthetic delivery seam, not evidence
of a live browser/Node occurrence; [Node v24.19.0 timers](https://nodejs.org/download/release/v24.19.0/docs/api/timers.html#settimeoutcallback-delay-args)
do not promise exact callback timing. Invalid/backward custom time fails closed.
The context port is fenced both before invocation and after its result: a
cleanup callback that ends/cancels the current task cannot cause a subsequent
read of that retired task's context during rearm. This is tested callback
reentry, not a claimed live participant-data leak; real data-read admission
remains a separate trusted-host responsibility.

Hardening beyond the donor: at most one actual running job and one newest pending
task; abort/cancel/supersede updates status promptly but retains running ownership
until the returned work actually settles. An uncooperative job blocks replacements.
Late completion cannot overwrite cancellation or a replacement. 100 native IDs
per instance is a fail-closed dedup cap. Reentrant receive/inputChanged from trusted
callbacks return `transition`; cancel/end remain prompt and are fenced after the
callback. Timer callbacks are identity/version guarded. `end()` clears pending
work/status IDs permanently, while an unresolved native job remains owned until
settlement. No claim is made that abort reverses an already performed side effect.
Running/pending ownership and the 100-ID cap are per instance, not aggregate
quotas across users, tabs, projects or replacement epochs. A closed old instance
may still own an unsettled run while a new instance starts another job. New
epoch/end does not release authority or budget and does not guarantee that an
old effect has stopped, nor total concurrency of one. The trusted stable host
and gateway must retain actual job ownership across replacements and enforce
their own aggregate capacity/budget limits; this module has no global registry.

Offline check: `node --test modules/personal-agent/voice/turn.test.mjs` (Node24.19).
These tests qualify the SOURCE state machine, not live voice or GLM. Later gates
remain participant admission/consent, media adapter and epoch binding, GLM-only
transport and budget reservation/reconciliation, trusted task/tool policy, and
real multi-participant cancellation/session replacement verification.
