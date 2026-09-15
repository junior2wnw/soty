import { once } from "node:events";
import { randomUUID } from "node:crypto";
import { cleanProviderChunk, createAnswerGate, createCompletionCollector, createSseDecoder, hasMeaningfulOutput, hasUsableCompletion, responseWasAborted } from "./inference-stream.js";

const retryableStatuses = new Set([401, 403, 408, 429, 500, 502, 503, 504]);

function failure(code, status = 502, retryable = true) {
  return Object.assign(new Error(code), { publicCode: code, httpStatus: status, retryable });
}

function abortError(signal) {
  return signal.reason instanceof Error ? signal.reason : failure("model-client-disconnected", 499, false);
}

async function abortable(promise, signal) {
  if (signal.aborted) throw abortError(signal);
  let rejectAbort;
  const aborted = new Promise((_, reject) => { rejectAbort = () => reject(abortError(signal)); });
  signal.addEventListener("abort", rejectAbort, { once: true });
  try { return await Promise.race([promise, aborted]); }
  finally { signal.removeEventListener("abort", rejectAbort); }
}

export function createProviderQueue({ concurrency = 8, maximumQueued = 64, waitMs = 5000 } = {}) {
  let active = 0;
  const pending = [];
  const makeRelease = () => {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      active--;
      while (pending.length && active < concurrency) {
        const item = pending.shift();
        item.cleanup();
        if (item.signal.aborted) { item.reject(abortError(item.signal)); continue; }
        active++;
        item.resolve(makeRelease());
      }
    };
  };
  return {
    snapshot: () => ({ active, queued: pending.length }),
    acquire(signal) {
      if (signal.aborted) return Promise.reject(abortError(signal));
      if (active < concurrency) { active++; return Promise.resolve(makeRelease()); }
      if (pending.length >= maximumQueued) return Promise.reject(failure("model-upstream-busy", 503));
      return new Promise((resolve, reject) => {
        let timer;
        const item = { resolve, reject, signal, cleanup: () => { clearTimeout(timer); signal.removeEventListener("abort", cancel); } };
        const remove = (error) => {
          const index = pending.indexOf(item);
          if (index < 0) return;
          pending.splice(index, 1); item.cleanup(); reject(error);
        };
        const cancel = () => remove(abortError(signal));
        timer = setTimeout(() => remove(failure("model-upstream-busy", 503)), waitMs);
        timer.unref?.();
        signal.addEventListener("abort", cancel, { once: true });
        pending.push(item);
      });
    }
  };
}

export function createInferenceRelay({
  providers, fetchImpl = fetch, requestTimeoutMs = 75000, firstTokenTimeoutMs = 20000,
  idleTimeoutMs = 30000, streamTimeoutMs = 1800000, concurrency = 8, maximumQueued = 64,
  queueWaitMs = 5000, failureThreshold = 3, cooldownMs = 30000, internalStream = true,
  normalizeTools = true, providerStrategy = "fallback", emptyToolFallback = false, onEvent = () => {}
}) {
  if (!["race", "fallback"].includes(providerStrategy)) throw new Error("invalid-provider-strategy");
  const states = providers.map((provider) => ({ ...provider, failures: 0, cooldownUntil: 0, probing: false,
    queue: createProviderQueue({ concurrency, maximumQueued, waitMs: queueWaitMs }) }));
  const emit = (value) => { try { onEvent(value); } catch { /* Metrics never interrupt inference. */ } };

  return {
    snapshot: () => states.map((state) => ({ name: state.name, ...state.queue.snapshot(),
      unavailable: state.cooldownUntil > Date.now(), recovering: state.probing })),
    async forward({ body, res, signal }) {
      const requestId = randomUUID();
      const started = performance.now();
      const deadline = new AbortController();
      const requestSignal = AbortSignal.any([signal, deadline.signal]);
      let timer = setTimeout(() => deadline.abort(failure("model-upstream-timeout", 504)), requestTimeoutMs);
      timer.unref?.();
      let startedOutput = false;
      let attempts = 0;
      let lastError = failure("model-upstream-unavailable", 503);
      res.setHeader("X-Soty-Request-Id", requestId);
      const onFirstOutput = () => {
        if (startedOutput) return;
        startedOutput = true;
        if (body.stream === true) {
          clearTimeout(timer);
          timer = setTimeout(() => deadline.abort(failure("model-upstream-timeout", 504)), streamTimeoutMs);
          timer.unref?.();
        }
      };
      try {
        if (providerStrategy === "race") {
          await raceProviders({ states, body, res, signal: requestSignal, fetchImpl, firstTokenTimeoutMs,
            idleTimeoutMs, internalStream, normalizeTools, onFirstOutput, emit, requestId, started,
            failureThreshold, cooldownMs, emptyToolFallback });
          return;
        }
        for (const state of states) {
          if (requestSignal.aborted) throw abortError(requestSignal);
          if (state.cooldownUntil > Date.now() || state.probing) continue;
          const recovering = state.failures >= failureThreshold;
          if (recovering) state.probing = true;
          let release;
          const attemptStart = performance.now();
          try {
            release = await state.queue.acquire(requestSignal);
            if (!recovering && state.cooldownUntil > Date.now()) continue;
            attempts++;
            const metrics = await compatibleAttempt({ state, body, res, signal: requestSignal, fetchImpl,
              firstTokenTimeoutMs, idleTimeoutMs, internalStream, normalizeTools, onFirstOutput, emptyToolFallback,
              onCompatibilityRetry: () => { attempts++; emit({ event: "inference_compatibility_retry", requestId, provider: state.name, reason: "empty-auto-tool-answer" }); } });
            state.failures = 0; state.cooldownUntil = 0;
            emit({ event: "inference_complete", requestId, provider: state.name, attempts,
              firstOutputMs: metrics.firstOutputMs, durationMs: Math.round(performance.now() - started) });
            return;
          } catch (error) {
            if (signal.aborted || res.destroyed) throw abortError(signal.aborted ? signal : requestSignal);
            lastError = error?.publicCode ? error : failure("model-upstream-unavailable");
            if (lastError.publicCode !== "model-upstream-busy" && lastError.retryable !== false) {
              state.failures++;
              if (state.failures >= failureThreshold || lastError.retryAfterMs) {
                state.failures = Math.max(state.failures, failureThreshold);
                state.cooldownUntil = Date.now() + Math.max(cooldownMs, lastError.retryAfterMs || 0);
              }
            }
            emit({ event: "inference_attempt_failed", requestId, provider: state.name,
              code: lastError.publicCode, status: lastError.httpStatus,
              durationMs: Math.round(performance.now() - attemptStart), outputCommitted: res.headersSent });
            // Once any output is visible, replaying a tool call or text would corrupt the conversation.
            if (res.headersSent || lastError.retryable === false || requestSignal.aborted) throw lastError;
          } finally {
            release?.();
            if (recovering) state.probing = false;
          }
        }
        const nextReady = Math.min(...states.map((state) => state.cooldownUntil).filter((time) => time > Date.now()));
        if (Number.isFinite(nextReady)) res.setHeader("Retry-After", String(Math.max(1, Math.ceil((nextReady - Date.now()) / 1000))));
        throw lastError;
      } finally {
        clearTimeout(timer);
      }
    }
  };
}

async function raceProviders({ states, body, res, signal, fetchImpl, firstTokenTimeoutMs, idleTimeoutMs,
  internalStream, normalizeTools, onFirstOutput, emit, requestId, started, failureThreshold, cooldownMs, emptyToolFallback }) {
  const candidates = states.filter(state => state.cooldownUntil <= Date.now() && !state.probing)
    .map(state => ({ state, controller: new AbortController() }));
  let winner = null, winnerError, attempts = 0;
  const select = candidate => {
    if (signal.aborted) throw abortError(signal);
    if (winner && winner !== candidate) throw failure("model-race-lost", 499, false);
    if (winner) return;
    winner = candidate;
    emit({ event: "inference_provider_selected", requestId, provider: candidate.state.name,
      strategy: "race", attempts, firstOutputMs: Math.round(performance.now() - started) });
    for (const other of candidates) if (other !== candidate) other.controller.abort(failure("model-race-lost", 499, false));
  };
  const jobs = candidates.map(async candidate => {
    const { state, controller } = candidate;
    const attemptSignal = AbortSignal.any([signal, controller.signal]);
    const recovering = state.failures >= failureThreshold;
    if (recovering) state.probing = true;
    let release;
    const attemptStart = performance.now();
    try {
      release = await state.queue.acquire(attemptSignal);
      if (!recovering && state.cooldownUntil > Date.now()) throw failure("model-upstream-cooling-down", 503);
      attempts++;
      const metrics = await compatibleAttempt({ state, body, res, signal: attemptSignal, fetchImpl,
        firstTokenTimeoutMs, idleTimeoutMs, internalStream, normalizeTools, onFirstOutput,
        beforeOutput: () => select(candidate), emptyToolFallback,
        onCompatibilityRetry: () => { attempts++; emit({ event: "inference_compatibility_retry", requestId, provider: state.name, reason: "empty-auto-tool-answer" }); } });
      state.failures = 0; state.cooldownUntil = 0;
      emit({ event: "inference_complete", requestId, provider: state.name, strategy: "race", attempts,
        firstOutputMs: metrics.firstOutputMs, durationMs: Math.round(performance.now() - started) });
    } catch (error) {
      if (controller.signal.aborted && !signal.aborted) {
        emit({ event: "inference_attempt_cancelled", requestId, provider: state.name, reason: "race-lost",
          durationMs: Math.round(performance.now() - attemptStart) });
        throw abortError(controller.signal);
      }
      if (signal.aborted || res.destroyed) throw abortError(signal);
      const problem = error?.publicCode ? error : failure("model-upstream-unavailable");
      if (winner === candidate) winnerError = problem;
      if (!["model-upstream-busy", "model-upstream-cooling-down"].includes(problem.publicCode) && problem.retryable !== false) {
        state.failures++;
        if (state.failures >= failureThreshold || problem.retryAfterMs) {
          state.failures = Math.max(state.failures, failureThreshold);
          state.cooldownUntil = Date.now() + Math.max(cooldownMs, problem.retryAfterMs || 0);
        }
      }
      emit({ event: "inference_attempt_failed", requestId, provider: state.name,
        code: problem.publicCode, status: problem.httpStatus,
        durationMs: Math.round(performance.now() - attemptStart), outputCommitted: winner === candidate && res.headersSent });
      throw problem;
    } finally {
      release?.();
      if (recovering) state.probing = false;
    }
  });
  try {
    await Promise.any(jobs);
  } catch (aggregate) {
    if (signal.aborted) throw abortError(signal);
    const nextReady = Math.min(...states.map(state => state.cooldownUntil).filter(time => time > Date.now()));
    if (Number.isFinite(nextReady) && !res.headersSent) res.setHeader("Retry-After", String(Math.max(1, Math.ceil((nextReady - Date.now()) / 1000))));
    throw winnerError || aggregate.errors?.find(error => error.publicCode && error.publicCode !== "model-race-lost")
      || failure("model-upstream-unavailable", 503);
  } finally {
    for (const candidate of candidates) candidate.controller.abort(failure("model-race-lost", 499, false));
    // Losers must release queue slots before this request is considered finished.
    await Promise.allSettled(jobs);
  }
}

function unusableCompletion(value, body) {
  const optionalTools = body.tools?.length && [undefined, "auto", "none"].includes(body.tool_choice);
  const stoppedWithoutCall = value.choices?.length && value.choices.every(choice => {
    const message = choice.message || choice.delta || {};
    return choice.finish_reason === "stop" && !message.tool_calls?.length && !message.function_call && !message.refusal;
  });
  return failure(optionalTools && stoppedWithoutCall ? "model-upstream-tool-selection-empty" : "model-upstream-empty-response");
}

async function compatibleAttempt(options) {
  try { return await relayAttempt(options); }
  catch (error) {
    if (!options.emptyToolFallback || error.publicCode !== "model-upstream-tool-selection-empty"
      || options.res.headersSent || options.signal.aborted) throw error;
    // A MiniMax/Gonka auto-tool response can end with stop but contain only
    // an unclosed reasoning block. No answer or tool call has been committed.
    // Retry that final-answer case once; required/explicit tools are never disabled.
    const body = { ...options.body };
    delete body.tools; delete body.tool_choice; delete body.parallel_tool_calls;
    options.onCompatibilityRetry?.();
    return await relayAttempt({ ...options, body });
  }
}

async function relayAttempt({ state, body, res, signal, fetchImpl, firstTokenTimeoutMs, idleTimeoutMs,
  internalStream, normalizeTools, onFirstOutput, beforeOutput = () => {} }) {
  const controller = new AbortController();
  const attemptSignal = AbortSignal.any([signal, controller.signal]);
  const started = performance.now();
  let firstOutputMs = null;
  let timeout = setTimeout(() => controller.abort(failure("model-upstream-timeout", 504)), firstTokenTimeoutMs);
  timeout.unref?.();
  let upstream;
  let reader;
  const progress = () => {
    clearTimeout(timeout);
    timeout = setTimeout(() => controller.abort(failure("model-upstream-timeout", 504)), idleTimeoutMs);
    timeout.unref?.();
  };
  const accept = () => {
    if (firstOutputMs !== null) return;
    beforeOutput();
    firstOutputMs = Math.round(performance.now() - started);
    onFirstOutput();
  };
  const header = (stream) => {
    res.status(200);
    res.setHeader("Cache-Control", "no-store");
    res.setHeader("X-Content-Type-Options", "nosniff");
    res.setHeader("X-Soty-Model-Proxy", "gonka");
    res.setHeader("Content-Type", stream ? "text/event-stream; charset=utf-8" : "application/json; charset=utf-8");
    if (stream) res.setHeader("X-Accel-Buffering", "no");
  };
  const write = async (text) => {
    if (res.destroyed || attemptSignal.aborted) throw abortError(attemptSignal);
    if (!res.write(text)) await once(res, "drain", { signal: attemptSignal });
  };
  try {
    const outgoing = internalStream ? { ...body, stream: true, stream_options: { ...body.stream_options, include_usage: true } } : body;
    upstream = await abortable(fetchImpl(new URL("chat/completions", `${state.baseUrl}/`), {
      method: "POST", redirect: "error", signal: attemptSignal,
      headers: { Authorization: `Bearer ${state.apiKey}`, "Content-Type": "application/json",
        Accept: outgoing.stream === true ? "text/event-stream" : "application/json" },
      body: JSON.stringify(outgoing)
    }), attemptSignal);
    if (!upstream.ok) {
      const error = failure("model-upstream-rejected", upstream.status, retryableStatuses.has(upstream.status));
      const retry = upstream.headers.get("retry-after");
      if (retry) {
        const seconds = Number(retry);
        error.retryAfterMs = Math.min(300000, Math.max(0, Number.isFinite(seconds) ? seconds * 1000 : Date.parse(retry) - Date.now())) || 0;
      }
      throw error;
    }
    const tools = new Set();
    if (upstream.headers.get("content-type")?.includes("application/json")) {
      const value = cleanProviderChunk(await abortable(upstream.json(), attemptSignal), tools, normalizeTools);
      if (responseWasAborted(value)) throw failure("model-upstream-incomplete");
      if (!hasUsableCompletion(value)) throw unusableCompletion(value, body);
      progress(); accept();
      if (body.stream === true) {
        header(true);
        const chunk = { ...value, object: "chat.completion.chunk", choices: value.choices.map(({ message, ...choice }) => ({ ...choice, delta: message })) };
        await write(`data: ${JSON.stringify(chunk)}\n\ndata: [DONE]\n\n`);
        res.end();
      } else { header(false); res.json(value); }
      return { firstOutputMs };
    }
    if (!upstream.headers.get("content-type")?.includes("text/event-stream") || !upstream.body) throw failure("model-upstream-invalid-response");
    reader = upstream.body.getReader();
    const decoder = createSseDecoder();
    const collector = createCompletionCollector({ normalizeTools });
    const answerGate = createAnswerGate();
    const prelude = [];
    let preludeBytes = 0;
    let done = false;
    const event = async (item) => {
      if (done) return;
      if (item.type === "done") {
        const completed = collector.finish();
        if (!hasUsableCompletion(completed)) throw unusableCompletion(completed, body);
        accept();
        done = true;
        if (body.stream === true) {
          if (!res.headersSent) { header(true); for (const part of prelude) await write(part); prelude.length = 0; }
          await write("data: [DONE]\n\n");
        }
        else { header(false); res.json(completed); }
        return;
      }
      let encoded;
      if (item.type === "comment") encoded = `${item.raw}\n\n`;
      else {
        const value = cleanProviderChunk(item.value, tools, normalizeTools);
        if (responseWasAborted(value)) throw failure("model-upstream-incomplete");
        collector.add(value);
        encoded = `data: ${JSON.stringify(value)}\n\n`;
        if (hasMeaningfulOutput(value)) progress();
        if (answerGate.add(value) && body.stream === true) accept();
      }
      if (body.stream !== true) return;
      if (firstOutputMs === null) {
        preludeBytes += Buffer.byteLength(encoded);
        if (preludeBytes > 2 * 1024 * 1024) throw failure("model-upstream-invalid-response");
        prelude.push(encoded);
      } else {
        if (!res.headersSent) {
          header(true);
          for (const part of prelude) await write(part);
          prelude.length = 0;
        }
        await write(encoded);
      }
    };
    while (!done) {
      const next = await abortable(reader.read(), attemptSignal);
      for (const item of next.done ? decoder.end() : decoder.push(next.value)) await event(item);
      if (next.done) break;
    }
    if (!done) throw failure("model-upstream-incomplete");
    if (body.stream === true) res.end();
    return { firstOutputMs };
  } finally {
    clearTimeout(timeout);
    controller.abort();
    try { if (reader) await reader.cancel(); else await upstream?.body?.cancel(); } catch { /* Abort already released the transport. */ }
  }
}
