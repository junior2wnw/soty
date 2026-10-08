import { types } from 'node:util';
import { AccessError } from '../modules/capabilities/server/validation.mjs';

const invalid = () => { throw new AccessError('native_source_configuration_invalid'); };
const requireThat = value => { if (!value) invalid(); };

// Private installed CODE only. Capture the callbacks before any host storage is
// opened; no HTTP/JSON record can supply a function or create Native authority.
export function captureNativeSourceHandlers(options) {
  requireThat(options && typeof options === 'object' && !types.isProxy(options)
    && [Object.prototype, null].includes(Object.getPrototypeOf(options)));
  const option = Object.getOwnPropertyDescriptor(options, 'nativeSourceHandlers');
  requireThat(!option || Object.hasOwn(option, 'value'));
  const input = option?.value === undefined ? [] : option.value;
  requireThat(!types.isProxy(input) && Array.isArray(input)
    && Object.getPrototypeOf(input) === Array.prototype);
  const descriptors = Object.getOwnPropertyDescriptors(input), length = descriptors.length.value;
  requireThat(length <= 64 && Reflect.ownKeys(descriptors).length === length + 1);
  const seen = new Set();
  const handlers = Object.freeze(Array.from({ length }, (_, index) => {
    const item = descriptors[index];
    requireThat(item && Object.hasOwn(item, 'value'));
    const row = item.value;
    requireThat(row && typeof row === 'object' && !types.isProxy(row)
      && [Object.prototype, null].includes(Object.getPrototypeOf(row)));
    const fields = Object.getOwnPropertyDescriptors(row);
    requireThat(Reflect.ownKeys(fields).length === 2
      && ['handle', 'close'].every(key => fields[key]?.enumerable
        && Object.hasOwn(fields[key], 'value') && typeof fields[key].value === 'function'
        && !types.isProxy(fields[key].value)));
    requireThat(!seen.has(fields.handle.value));
    seen.add(fields.handle.value);
    return Object.freeze({ handle: fields.handle.value, close: fields.close.value });
  }));
  let closed = false, cleanup;
  function unavailable(res) {
    if (res.writableEnded || res.destroyed) return;
    if (res.headersSent) { res.destroy(); return; }
    res.status(503).set('Cache-Control', 'no-store').json({ error: { code: 'native_source_unavailable' } });
  }
  return Object.freeze({
    enabled: handlers.length > 0,
    async middleware(req, res, next) {
      if (closed) { unavailable(res); return; }
      try {
        for (const row of handlers) {
          const handled = await row.handle(req, res);
          if (closed) { unavailable(res); return; }
          if (handled === true) return;
          if (handled !== false || res.headersSent || res.writableEnded || res.destroyed) {
            unavailable(res); return;
          }
        }
        next();
      } catch { unavailable(res); }
    },
    close() {
      if (closed) return cleanup ?? Promise.resolve();
      closed = true;
      const callbacks = [...new Set(handlers.map(row => row.close))];
      cleanup = Promise.all(callbacks.map(callback => {
        try { return Promise.resolve(callback()).catch(() => {}); }
        catch { return Promise.resolve(); }
      })).then(() => undefined);
      return cleanup;
    },
  });
}
