import { GLM_MODEL, createGlmProvider } from '../index.mjs';

export const chunk = (delta = {}, extra = {}) => ({ id: 'chatcmpl_synthetic', model: GLM_MODEL,
  choices: [{ index: 0, delta, finish_reason: null }], ...extra });
export const finish = (reason = 'stop', extra = {}) => chunk({}, { choices: [{ index: 0, delta: {}, finish_reason: reason }], ...extra });
export const request = (changes = {}) => ({ requestId: 'request_synthetic', messages: [{ role: 'user', content: 'Synthetic request' }], ...changes });
export const fail = code => error => error.code === code && error.message === code;
export const tick = () => new Promise(resolve => setImmediate(resolve));
export function deferred() { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; }
export function wire(events, { done = true } = {}) {
  return events.map(event => typeof event === 'string' ? event : `data: ${JSON.stringify(event)}\r\n\r\n`).join('') + (done ? 'data: [DONE]\r\n\r\n' : '');
}
export function streamed(events, options = {}) {
  const bytes = options.bytes ?? new TextEncoder().encode(wire(events, options));
  let index = 0;
  return new Response(new ReadableStream({ pull(controller) {
    if (index >= bytes.length) { controller.close(); return; }
    const end = Math.min(index + (options.width ?? 7), bytes.length); controller.enqueue(bytes.slice(index, end)); index = end;
  } }), { headers: { 'Content-Type': 'text/event-stream', 'X-Request-Id': 'broker_synthetic' } });
}
export function tool(name = 'read', validateArguments = args => typeof args.path === 'string' && Object.keys(args).length === 1) {
  return { name, description: 'Read a synthetic resource', parameters: {
    type: 'object', properties: { path: { type: 'string', maxLength: 100 } }, required: ['path'], additionalProperties: false }, validateArguments };
}
export function provider(t, options = {}) {
  const value = createGlmProvider(options); t.after(() => value.close()); return value;
}
