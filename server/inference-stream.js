import { StringDecoder } from "node:string_decoder";

// Billing extensions describe the shared provider account, not a customer's key.
export function cleanProviderChunk(value, toolChoices = new Set(), normalizeTools = true) {
  const chunk = { ...value };
  delete chunk.x_joingonka;
  for (const choice of chunk.choices || []) {
    const message = choice.message || choice.delta || {};
    if (message.tool_calls?.length) toolChoices.add(choice.index ?? 0);
    if (normalizeTools && choice.finish_reason === "stop" && toolChoices.has(choice.index ?? 0)) choice.finish_reason = "tool_calls";
  }
  return chunk;
}

export function hasMeaningfulOutput(chunk) {
  return (chunk.choices || []).some((choice) => {
    const message = choice.delta || choice.message || {};
    return Boolean(message.content || message.reasoning_content || message.reasoning || message.refusal
      || message.tool_calls?.length || message.function_call?.name || message.function_call?.arguments
      || choice.finish_reason);
  });
}

// Provider activity is not yet a usable answer. In particular, MiniMax may
// return only <think>...</think>, or stop halfway through a tool's arguments.
// Keep the original chunks intact for tool continuations; this gate only
// decides when a candidate can be committed to the client.
export function createAnswerGate() {
  const choices = new Map();
  let visible = false;
  return {
    add(chunk) {
      for (const choice of chunk.choices || []) {
        const index = choice.index ?? 0;
        let state = choices.get(index);
        if (!state) { state = { thinking: false, pending: "" }; choices.set(index, state); }
        const message = choice.delta || choice.message || {};
        if (typeof message.refusal === "string" && message.refusal.trim()) visible = true;
        if (typeof message.content !== "string") continue;
        state.pending += message.content;
        while (state.pending) {
          const marker = state.thinking ? "</think>" : "<think>";
          const lower = state.pending.toLowerCase(), at = lower.indexOf(marker);
          if (at >= 0) {
            if (!state.thinking && state.pending.slice(0, at).trim()) visible = true;
            state.pending = state.pending.slice(at + marker.length);
            state.thinking = !state.thinking;
            continue;
          }
          let keep = 0;
          for (let n = 1; n < marker.length; n++) if (lower.endsWith(marker.slice(0, n))) keep = n;
          const take = state.pending.length - keep;
          if (!state.thinking && state.pending.slice(0, take).trim()) visible = true;
          state.pending = state.pending.slice(take);
          break;
        }
      }
      return visible;
    }
  };
}

export function hasUsableCompletion(value) {
  const validArguments = text => {
    if (typeof text !== "string") return false;
    try { const parsed = JSON.parse(text); return parsed !== null && typeof parsed === "object" && !Array.isArray(parsed); }
    catch { return false; }
  };
  return Boolean(value.choices?.length) && value.choices.every(choice => {
    if (!choice.finish_reason || responseWasAborted({ choices: [choice] })) return false;
    if (choice.finish_reason === "content_filter") return true;
    const message = choice.message || choice.delta || {};
    if (message.tool_calls?.length) return new Set(message.tool_calls.map(call => call.id)).size === message.tool_calls.length
      && message.tool_calls.every(call => typeof call.id === "string" && call.id.trim()
      && call.type === "function" && typeof call.function?.name === "string" && call.function.name.trim()
      && validArguments(call.function.arguments));
    if (message.function_call) return Boolean(message.function_call.name && validArguments(message.function_call.arguments));
    return createAnswerGate().add({ choices: [choice] });
  });
}

export function responseWasAborted(chunk) {
  return Boolean(chunk.error) || (chunk.choices || []).some((choice) => ["abort", "error", "cancelled", "canceled"].includes(choice.finish_reason));
}

export function createSseDecoder({ maximumBufferedBytes = 2 * 1024 * 1024 } = {}) {
  const decoder = new StringDecoder("utf8");
  let buffer = "";
  const extract = (final = false) => {
    const events = [];
    while (true) {
      const separator = /\r?\n\r?\n/u.exec(buffer);
      if (!separator && !final) break;
      const block = separator ? buffer.slice(0, separator.index) : buffer;
      buffer = separator ? buffer.slice(separator.index + separator[0].length) : "";
      if (block) {
        if (Buffer.byteLength(block) > maximumBufferedBytes) throw new Error("upstream-event-too-large");
        const data = block.split(/\r?\n/u).filter((line) => line.startsWith("data:")).map((line) => line.slice(5).trimStart()).join("\n");
        if (!data) events.push({ type: "comment", raw: block });
        else if (data.trim() === "[DONE]") events.push({ type: "done" });
        else {
          let value;
          try { value = JSON.parse(data); } catch { throw new Error("upstream-invalid-event"); }
          if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("upstream-invalid-event");
          events.push({ type: "data", value });
        }
      }
      if (!separator) break;
    }
    if (Buffer.byteLength(buffer) > maximumBufferedBytes) throw new Error("upstream-event-too-large");
    return events;
  };
  return {
    push(bytes) { buffer += decoder.write(Buffer.from(bytes)); return extract(); },
    end() { buffer += decoder.end(); return extract(true); }
  };
}

export function createCompletionCollector({ normalizeTools = true } = {}) {
  const metadata = { object: "chat.completion" };
  const choices = new Map();
  let bytes = 0;
  return {
    add(chunk) {
      bytes += Buffer.byteLength(JSON.stringify(chunk));
      if (bytes > 16 * 1024 * 1024) throw new Error("upstream-response-too-large");
      for (const key of ["id", "created", "model", "system_fingerprint", "service_tier", "usage"]) {
        if (chunk[key] !== undefined) metadata[key] = chunk[key];
      }
      for (const incoming of chunk.choices || []) {
        const index = incoming.index ?? 0;
        let choice = choices.get(index);
        if (!choice) {
          choice = { index, message: { role: "assistant", content: null }, finish_reason: null, tools: new Map() };
          choices.set(index, choice);
        }
        const delta = incoming.message || incoming.delta || {};
        if (delta.role) choice.message.role = delta.role;
        for (const key of ["content", "reasoning_content", "reasoning", "refusal"]) {
          if (typeof delta[key] === "string") choice.message[key] = (choice.message[key] || "") + delta[key];
        }
        if (Array.isArray(delta.reasoning_details)) {
          choice.message.reasoning_details = [...(choice.message.reasoning_details || []), ...delta.reasoning_details];
        }
        if (Array.isArray(delta.annotations)) {
          choice.message.annotations = [...(choice.message.annotations || []), ...delta.annotations];
        }
        if (delta.function_call) {
          choice.message.function_call ||= { name: "", arguments: "" };
          for (const key of ["name", "arguments"]) {
            if (typeof delta.function_call[key] === "string") choice.message.function_call[key] += delta.function_call[key];
          }
        }
        for (const [position, fragment] of (delta.tool_calls || []).entries()) {
          const toolIndex = fragment.index ?? position;
          let tool = choice.tools.get(toolIndex);
          if (!tool) {
            tool = { id: "", type: "function", function: { name: "", arguments: "" } };
            choice.tools.set(toolIndex, tool);
          }
          if (fragment.id) tool.id = fragment.id;
          if (fragment.type) tool.type = fragment.type;
          if (fragment.function?.name) tool.function.name += fragment.function.name;
          if (typeof fragment.function?.arguments === "string") tool.function.arguments += fragment.function.arguments;
        }
        if (incoming.finish_reason !== undefined && incoming.finish_reason !== null) choice.finish_reason = incoming.finish_reason;
        if (incoming.logprobs === null && choice.logprobs === undefined) choice.logprobs = null;
        if (incoming.logprobs) {
          choice.logprobs ||= {};
          for (const key of ["content", "refusal"]) if (Array.isArray(incoming.logprobs[key])) choice.logprobs[key] = [...(choice.logprobs[key] || []), ...incoming.logprobs[key]];
        }
      }
    },
    finish() {
      const result = [...choices.values()].sort((a, b) => a.index - b.index).map(({ tools, ...choice }) => {
        if (tools.size) {
          choice.message.tool_calls = [...tools.entries()].sort(([a], [b]) => a - b).map(([, tool]) => tool);
          if (!choice.message.content) choice.message.content = null;
          if (normalizeTools && choice.finish_reason === "stop") choice.finish_reason = "tool_calls";
        }
        return choice;
      });
      if (!result.length || result.some((choice) => !choice.finish_reason)) throw new Error("upstream-incomplete-response");
      return { ...metadata, choices: result };
    }
  };
}
