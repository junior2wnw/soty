const SCHEMA = "glass-ui.widget-context.v1";
const modes = new Set(["glance", "interactive"]);
let nextEpoch = 0;
const check = (value, message) => { if (!value) throw new TypeError("GlassWidgetContext: " + message); };
function exactOrigin(value) {
  let parsed; try { parsed = new URL(value); } catch { /* Invalid configuration, never a wildcard. */ }
  check(typeof value === "string" && parsed && ["http:", "https:"].includes(parsed.protocol)
    && parsed.origin === value && !parsed.username && !parsed.password, "invalid origin");
  return value;
}
function envelope(value, type) {
  if (!value || typeof value !== "object" || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) return false;
  const fields = Object.getOwnPropertyDescriptors(value), keys = Reflect.ownKeys(fields);
  const expected = type === "ready" ? ["schema", "type", ...(Object.hasOwn(fields, "epoch") ? ["epoch"] : [])] : ["schema", "type", "mode", "epoch"];
  return keys.length === expected.length && keys.every(key => expected.includes(key)
    && fields[key].enumerable && Object.hasOwn(fields[key], "value"))
    && fields.schema?.value === SCHEMA && fields.type?.value === type
    && (type === "ready" || modes.has(fields.mode?.value))
    && (!Object.hasOwn(fields, "epoch") || Number.isSafeInteger(fields.epoch.value) && fields.epoch.value > 0);
}
function eventView(view) {
  check(view && typeof view.addEventListener === "function" && typeof view.removeEventListener === "function", "invalid view");
  return view;
}

/** UI presentation only. It never changes Source authority or dispatches commands. */
export function bindGlassWidgetContext(root, { parentOrigin, view = globalThis.window } = {}) {
  const origin = exactOrigin(parentOrigin), events = eventView(view);
  check(root?.nodeType === 1 && typeof root.setAttribute === "function" && typeof root.removeAttribute === "function", "invalid root");
  const previous = root.getAttribute("data-glass-widget-mode");
  let disposed = false, epoch = 0;
  const announce = value => {
    try { events.parent.postMessage({ schema: SCHEMA, type: "ready", ...(value === undefined ? {} : { epoch: value }) }, origin); } catch { /* Closing parent. */ }
  };
  root.setAttribute("data-glass-widget-mode", "interactive");
  const receive = event => {
    if (disposed || events.parent === events || event.source !== events.parent || event.origin !== origin
      || !envelope(event.data, "mode") || event.data.epoch < epoch) return;
    epoch = event.data.epoch;
    root.setAttribute("data-glass-widget-mode", event.data.mode);
    announce(epoch);
  };
  events.addEventListener("message", receive);
  if (events.parent && events.parent !== events) announce();
  return { dispose() {
    if (disposed) return; disposed = true; events.removeEventListener("message", receive);
    if (previous === null) root.removeAttribute("data-glass-widget-mode");
    else root.setAttribute("data-glass-widget-mode", previous);
  } };
}

/** Bind once per iframe/reset lifetime; readiness is never inherited by a new binding. */
export function createGlassWidgetHost(frame, { origin, initialMode = "interactive", view = globalThis.window,
  isCurrent = () => true, onReady, onReset } = {}) {
  const targetOrigin = exactOrigin(origin), events = eventView(view);
  check(frame && typeof frame.addEventListener === "function" && typeof frame.removeEventListener === "function", "invalid frame");
  check(modes.has(initialMode) && typeof isCurrent === "function"
    && [onReady, onReset].every(value => value === undefined || typeof value === "function"), "invalid host options");
  let mode = initialMode, disposed = false, ready = false, epoch = 0;
  const current = () => { try { return !disposed && isCurrent() === true; } catch { return false; } };
  const send = () => {
    if (!current()) return;
    // A detached/closing browsing context must not break the host's lifecycle.
    try { frame.contentWindow?.postMessage({ schema: SCHEMA, type: "mode", mode, epoch }, targetOrigin); } catch { /* No live receiver. */ }
  };
  const reset = () => {
    if (!current()) return;
    check(Number.isSafeInteger(++nextEpoch), "epoch exhausted"); epoch = nextEpoch; ready = false;
    onReset?.(); send();
  };
  const receive = event => {
    if (!current() || event.source !== frame.contentWindow || event.origin !== targetOrigin || !envelope(event.data, "ready")) return;
    if (!Object.hasOwn(event.data, "epoch")) { send(); return; }
    if (event.data.epoch !== epoch) return;
    if (!ready && current()) { ready = true; onReady?.(); }
  };
  events.addEventListener("message", receive); frame.addEventListener("load", reset); reset();
  return {
    setMode(next) { check(modes.has(next), "invalid mode"); if (!current() || next === mode) return; mode = next; send(); },
    dispose() {
      if (disposed) return; disposed = true;
      events.removeEventListener("message", receive); frame.removeEventListener("load", reset);
    },
  };
}
