import {
  GLASS_CORE_PRESETS,
  composeGlassThemes,
  createGlassSite,
  defineGlassTheme,
} from "./site.js";

const ROLE_PATTERN = /^[a-z][a-z0-9-]{0,47}$/;
const isPlainObject = (value) => (
  value !== null
  && typeof value === "object"
  && !Array.isArray(value)
  && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
);
const cloneJson = (value) => JSON.parse(JSON.stringify(value));

export const GLASS_UI_LAYERS = Object.freeze({
  shell: 0,
  structure: 10,
  content: 20,
  card: 30,
  field: 35,
  control: 40,
  accent: 45,
  overlay: 80,
});

export const GLASS_UI_THEME = defineGlassTheme({
  name: "Universal optical interface",
  presets: {
    "ui-shell": {
      preset: "quiet",
      layer: GLASS_UI_LAYERS.shell,
      shape: { radius: 0.075, radiusMax: 96, bevel: 0.09, irregularity: 0.14 },
      optics: { ior: 1.47, depth: 82, refraction: 0.96, roughness: 0.004 },
      style: { tint: [0.82, 0.93, 1], tintStrength: 0.03, matte: 0.002 },
      surface: { texture: 0.1, scale: 1.25, grain: 0.1, rear: 0.18 },
      interaction: 0.14,
      motion: { mode: "group", coupling: 0.94, tilt: 0.32, parallax: 0.5, lift: 0.8, contentDepth: 0.18 },
    },
    "ui-navigation": {
      preset: "quiet",
      layer: GLASS_UI_LAYERS.structure,
      shape: { radius: 0.5, radiusMax: 999, bevel: 0.31, irregularity: 0.14 },
      optics: { ior: 1.47, depth: 72, refraction: 1.12, roughness: 0.005 },
      style: { tint: [0.76, 0.9, 1], tintStrength: 0.06, matte: 0.003 },
      surface: { texture: 0.12, scale: 1.2, grain: 0.12, rear: 0.2 },
      interaction: 0.24,
      motion: { mode: "group", coupling: 0.9, tilt: 0.55, parallax: 0.9, lift: 1.4, contentDepth: 0.3 },
    },
    "ui-panel": {
      preset: "quiet",
      layer: GLASS_UI_LAYERS.content,
      shape: { radius: 0.18, radiusMax: 104, bevel: 0.2, irregularity: 0.16 },
      optics: { ior: 1.47, depth: 88, refraction: 1.1, roughness: 0.005 },
      style: { tint: [0.75, 0.9, 1], tintStrength: 0.065, matte: 0.004 },
      surface: { texture: 0.12, scale: 1.12, grain: 0.14, rear: 0.22 },
      interaction: 0.26,
      motion: { mode: "group", coupling: 0.88, tilt: 0.62, parallax: 1, lift: 1.8, contentDepth: 0.36 },
    },
    "ui-card": {
      preset: "quiet",
      layer: GLASS_UI_LAYERS.card,
      shape: { radius: 0.26, radiusMax: 94, bevel: 0.24, irregularity: 0.15 },
      optics: { ior: 1.47, depth: 78, refraction: 1.08, roughness: 0.006 },
      style: { tint: [0.72, 0.89, 1], tintStrength: 0.08, matte: 0.006 },
      surface: { texture: 0.12, scale: 1.2, grain: 0.14, rear: 0.21 },
      interaction: 0.34,
      motion: { mode: "surface", coupling: 0.84, tilt: 0.82, parallax: 1.25, lift: 2.2, contentDepth: 0.42 },
    },
    "ui-field": {
      preset: "quiet",
      layer: GLASS_UI_LAYERS.field,
      shape: { radius: 0.5, radiusMax: 999, bevel: 0.45, irregularity: 0.12 },
      optics: { ior: 1.48, depth: 56, refraction: 1.18, roughness: 0.005 },
      style: { tint: [0.7, 0.88, 1], tintStrength: 0.1, matte: 0.005 },
      surface: { texture: 0.11, scale: 1.24, grain: 0.12, rear: 0.2 },
      interaction: 0,
      motion: { enabled: false, mode: "surface", coupling: 0, tilt: 0, parallax: 0, lift: 0, contentDepth: 0 },
    },
    "ui-control": {
      preset: "quiet",
      layer: GLASS_UI_LAYERS.control,
      shape: { radius: 0.5, radiusMax: 999, bevel: 0.48, irregularity: 0.12 },
      optics: { ior: 1.49, depth: 64, refraction: 1.28, roughness: 0.004 },
      style: { tint: [0.68, 0.87, 1], tintStrength: 0.12, matte: 0.004 },
      surface: { texture: 0.1, scale: 1.28, grain: 0.11, rear: 0.21 },
      interaction: 0.48,
      motion: { mode: "surface", coupling: 0.78, tilt: 1.15, parallax: 1.8, lift: 2.8, contentDepth: 0.5 },
    },
    "ui-accent": {
      preset: "ui-control",
      layer: GLASS_UI_LAYERS.accent,
      optics: { depth: 72, refraction: 1.36 },
      style: { tint: [0.36, 0.77, 1], tintStrength: 0.28, matte: 0.005 },
      interaction: 0.54,
    },
    "ui-line": {
      preset: "line",
      layer: GLASS_UI_LAYERS.card,
      shape: { bevel: 0.48 },
      optics: { depth: 34, refraction: 1.2, roughness: 0.004 },
      style: { tint: [0.48, 0.82, 1], tintStrength: 0.18, matte: 0.002 },
      interaction: 0.08,
    },
    "ui-overlay": {
      preset: "quiet",
      layer: GLASS_UI_LAYERS.overlay,
      shape: { radius: 0.22, radiusMax: 118, bevel: 0.26, irregularity: 0.16 },
      optics: { ior: 1.48, depth: 104, refraction: 1.18, roughness: 0.008 },
      style: { tint: [0.68, 0.86, 1], tintStrength: 0.12, matte: 0.012 },
      surface: { texture: 0.16, scale: 1.1, grain: 0.17, rear: 0.27 },
      interaction: 0.38,
      motion: { mode: "surface", coupling: 0.72, tilt: 1, parallax: 1.6, lift: 4, contentDepth: 0.52 },
    },
  },
});

const workingMaterial = (layer, depth, refraction, shape = {}) => ({
  preset: "quiet",
  layer,
  shape: { radius: 0.36, radiusMax: 16, bevel: 0.03, irregularity: 0.02, ...shape },
  optics: { ior: 1.44, depth, refraction, roughness: 0.008 },
  style: { tint: [0.86, 0.91, 0.94], tintStrength: 0.035, matte: 0.004 },
  surface: { texture: 0.012, scale: 1, grain: 0.01, rear: 0.025 },
  interaction: 0,
  motion: { enabled: false, mode: "surface", coupling: 0, tilt: 0, parallax: 0, lift: 0, contentDepth: 0 },
});

/** Opt-in reading/working material; the original optical theme stays unchanged. */
export const GLASS_UI_WORKING_THEME = defineGlassTheme({
  name: "Quiet working glass",
  presets: {
    "ui-shell": workingMaterial(GLASS_UI_LAYERS.shell, 16, 0.12, { radius: 0.12, radiusMax: 24, bevel: 0.025 }),
    "ui-navigation": workingMaterial(GLASS_UI_LAYERS.structure, 18, 0.14, { radius: 0.22, radiusMax: 20 }),
    "ui-panel": workingMaterial(GLASS_UI_LAYERS.content, 22, 0.18, { radius: 0.16, radiusMax: 24, bevel: 0.045 }),
    "ui-card": workingMaterial(GLASS_UI_LAYERS.card, 18, 0.16, { radius: 0.2, radiusMax: 20, bevel: 0.04 }),
    "ui-field": workingMaterial(GLASS_UI_LAYERS.field, 10, 0.1),
    "ui-control": workingMaterial(GLASS_UI_LAYERS.control, 10, 0.12),
    "ui-accent": {
      ...workingMaterial(GLASS_UI_LAYERS.accent, 12, 0.14),
      style: { tint: [0.67, 0.62, 0.92], tintStrength: 0.16, matte: 0.004 },
    },
    "ui-line": {
      ...workingMaterial(GLASS_UI_LAYERS.card, 8, 0.1, { radius: 0.5, radiusMax: 999 }),
      preset: "line",
    },
    "ui-overlay": workingMaterial(GLASS_UI_LAYERS.overlay, 26, 0.22, { radius: 0.16, radiusMax: 24, bevel: 0.055 }),
  },
});

export const DEFAULT_GLASS_UI_ROLES = Object.freeze({
  shell: Object.freeze({ preset: "ui-shell", hitArea: "container" }),
  navigation: Object.freeze({ preset: "ui-navigation", hitArea: "container" }),
  panel: Object.freeze({ preset: "ui-panel", hitArea: "container" }),
  card: Object.freeze({ preset: "ui-card", hitArea: "container" }),
  field: Object.freeze({ preset: "ui-field", hitArea: "control" }),
  control: Object.freeze({ preset: "ui-control", hitArea: "control" }),
  accent: Object.freeze({ preset: "ui-accent", hitArea: "control" }),
  line: Object.freeze({ preset: "ui-line", hitArea: "container" }),
  overlay: Object.freeze({ preset: "ui-overlay", hitArea: "container" }),
});

const normalizeRoleDefinition = (name, definition, availablePresets) => {
  if (!ROLE_PATTERN.test(name)) throw new Error(`GlassUI: invalid role name "${name}"`);
  const normalized = typeof definition === "string" ? { preset: definition } : definition;
  if (!isPlainObject(normalized)) throw new Error(`GlassUI: role "${name}" must be a preset or object`);
  const preset = String(normalized.preset || "").trim();
  if (!preset || !availablePresets.has(preset)) {
    throw new Error(`GlassUI: role "${name}" uses unknown preset "${preset || "none"}"`);
  }
  const options = normalized.options ?? {};
  if (!isPlainObject(options) || "preset" in options || "target" in options || "content" in options) {
    throw new Error(`GlassUI: role "${name}" has invalid options`);
  }
  const hitArea = String(normalized.hitArea || "container");
  if (!new Set(["container", "control"]).has(hitArea)) {
    throw new Error(`GlassUI: role "${name}" has invalid hitArea "${hitArea}"`);
  }
  return { preset, options: cloneJson(options), hitArea };
};

export const defineGlassRoles = (input = DEFAULT_GLASS_UI_ROLES, theme = GLASS_UI_THEME) => {
  if (!isPlainObject(input)) throw new Error("GlassUI: roles must be an object");
  const normalizedTheme = defineGlassTheme(theme);
  const availablePresets = new Set([
    ...GLASS_CORE_PRESETS,
    ...Object.keys(normalizedTheme.presets),
  ]);
  return Object.fromEntries(Object.entries(input).map(([name, definition]) => [
    name,
    normalizeRoleDefinition(name, definition, availablePresets),
  ]));
};

const resolveScope = (root, documentImpl) => {
  if (!root) return documentImpl;
  if (typeof root === "string") {
    const resolved = documentImpl?.querySelector?.(root);
    if (!resolved) throw new Error(`GlassUI: root not found: ${root}`);
    return resolved;
  }
  return root;
};

const attributeSnapshot = (target, name) => ({
  present: target.hasAttribute(name),
  value: target.getAttribute(name),
});
const restoreAttribute = (target, name, snapshot) => {
  if (snapshot.present) target.setAttribute(name, snapshot.value ?? "");
  else target.removeAttribute(name);
};
const parseOptions = (value) => {
  if (!value) return {};
  try {
    const parsed = JSON.parse(value);
    return isPlainObject(parsed) ? parsed : {};
  } catch {
    return {};
  }
};
const collectTargets = (scope, reference) => {
  if (typeof reference === "string") return [...scope.querySelectorAll(reference)];
  if (reference?.nodeType === 1) return [reference];
  if (reference && typeof reference[Symbol.iterator] === "function") return [...reference];
  throw new Error("GlassUI: target must be an element, selector or iterable");
};
const RANGE_SELECTOR = 'input[type="range"]';
const IGNORE_SELECTOR = ":not(:where([data-glass-ignore], [data-glass-ignore] *))";
const ignoredTarget = (target) => {
  for (let node = target; node; node = node.parentNode || node.host) {
    if (node.hasAttribute?.("data-glass-ignore")) return true;
  }
  return false;
};
const rangeProgress = (target) => {
  const min = Number(target.min || 0);
  const max = Number(target.max || 100);
  const value = Number(target.value);
  if (!Number.isFinite(min) || !Number.isFinite(max) || max <= min || !Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(100, ((value - min) / (max - min)) * 100));
};

const checkedRenderPolicy = (value) => {
  if (!["auto", "static", "optical"].includes(value)) throw new Error("GlassUI: invalid render policy");
  return value;
};
const runningScene = (scene) => Boolean(scene?.gl && scene.running && scene.initialized && !scene.gl.isContextLost());
const validOpticalFrame = (scene) => runningScene(scene)
  && scene.gl.getError() === scene.gl.NO_ERROR && scene.getMetrics().opticalPasses >= 1;
// Author/configuration mistakes must not be disguised as a hardware fallback.
const configurationError = (error) => error instanceof TypeError || error instanceof SyntaxError
  || /^(?:GlassUI:|OpticalGlass site:|OpticalGlass: (?:canvas not found|invalid |unknown preset|live DOM|volume engraving|element target not found|mount root not found)|background source is empty)/u.test(error?.message || "");

/**
 * Semantic, layout-independent UI layer on top of the optical renderer.
 * Existing DOM stays accessible; roles only choose material and optical order.
 */
export const createGlassUI = async ({
  root,
  background,
  fallbackBackground,
  canvas,
  theme,
  roles,
  observe = true,
  enhanceControls = true,
  strict = true,
  autoDestroy = true,
  onError,
  renderPolicy = "auto",
  onRenderStatus,
  documentImpl = globalThis.document,
  ...sceneOptions
} = {}) => {
  if (!documentImpl) throw new Error("GlassUI: document is unavailable");
  checkedRenderPolicy(renderPolicy);
  if (onRenderStatus !== undefined && typeof onRenderStatus !== "function") throw new Error("GlassUI: invalid render status observer");
  if (renderPolicy !== "static" && !background) throw new Error("OpticalGlass site: background is required");
  const scope = resolveScope(root, documentImpl);
  if (!scope || ![1, 9, 11].includes(scope.nodeType) || typeof scope.querySelectorAll !== "function") throw new Error("GlassUI: invalid root");
  if (sceneOptions.selector !== undefined) {
    if (typeof sceneOptions.selector !== "string" || !sceneOptions.selector.trim()) throw new Error("GlassUI: invalid selector");
    scope.querySelectorAll(sceneOptions.selector);
  }
  let currentTheme = theme ? composeGlassThemes(GLASS_UI_THEME, theme) : GLASS_UI_THEME;
  let roleDefinitions = defineGlassRoles({ ...DEFAULT_GLASS_UI_ROLES, ...(roles || {}) }, currentTheme);
  const managedTargets = new Set();
  const snapshots = new WeakMap();
  const overrides = new WeakMap();
  const diagnostics = [];
  const enhancedRanges = new Map();
  let destroyed = false;
  let glass = null;
  const defaultView = documentImpl.defaultView;
  const materialRoot = scope.nodeType === 1 ? scope : scope.host || documentImpl.documentElement;
  const rootAttributes = Object.fromEntries(["data-glass-render-mode", "data-glass-render-policy", "data-glass-render-reason"]
    .map((name) => [name, attributeSnapshot(materialRoot, name)]));
  const hadRootClass = materialRoot.classList.contains("glass-ui");
  let renderStatus = Object.freeze({ policy: renderPolicy, mode: "static", reason: "initializing", gpuReady: false });
  let generation = 0, renderQueue = Promise.resolve(), cancelPaint = null, detachContext = null;
  const forcedColors = defaultView?.matchMedia?.("(forced-colors: active)");
  const reducedMotion = defaultView?.matchMedia?.("(prefers-reduced-motion: reduce)");

  const notifyStatus = (mode, reason, gpuReady = false) => {
    renderStatus = Object.freeze({ policy: renderPolicy, mode, reason, gpuReady });
    materialRoot.classList.add("glass-ui");
    materialRoot.setAttribute("data-glass-render-mode", mode);
    materialRoot.setAttribute("data-glass-render-policy", renderPolicy);
    materialRoot.setAttribute("data-glass-render-reason", reason);
    try { onRenderStatus?.(renderStatus); } catch { /* Host observers do not own material initialization. */ }
    return renderStatus;
  };
  const staticReason = () => renderPolicy === "static" ? "policy-static" : ignoredTarget(scope) ? "ignored"
    : forcedColors?.matches ? "forced-colors" : reducedMotion?.matches ? "reduced-motion" : null;
  const nextPaint = () => new Promise((resolve) => {
    if (!defaultView?.requestAnimationFrame) { resolve(false); return; }
    let finished = false, timer, frame;
    const finish = (painted) => {
      if (finished) return;
      finished = true;
      defaultView.cancelAnimationFrame?.(frame);
      clearTimeout(timer);
      cancelPaint = null;
      resolve(painted);
    };
    cancelPaint = () => finish(false);
    frame = defaultView.requestAnimationFrame(() => finish(true));
    // A background document need not receive animation frames. Do not keep
    // initialization or cleanup waiting indefinitely for GPU proof.
    timer = setTimeout(() => finish(false), 1000);
  });
  const releaseGlass = () => {
    cancelPaint?.();
    detachContext?.(); detachContext = null;
    const old = glass; glass = null;
    old?.destroy();
  };
  const scheduleRender = () => {
    const ticket = ++generation, reason = staticReason();
    if (!reason && glass && renderStatus.mode === "optical" && validOpticalFrame(glass.scene)) {
      return Promise.resolve(notifyStatus("optical", "ready", true));
    }
    releaseGlass();
    notifyStatus("static", reason || "initializing");
    renderQueue = renderQueue.catch(() => {}).then(async () => {
      if (destroyed || ticket !== generation || reason) return renderStatus;
      let candidate;
      const initialTheme = currentTheme, initialBackground = background;
      try {
        candidate = await createGlassSite({ ...sceneOptions, root: scope, canvas, background,
          selector: ":is(" + (sceneOptions.selector || "[data-optical-glass], [data-liquid-glass]") + ")" + IGNORE_SELECTOR,
          fallbackBackground, theme: currentTheme, observe, autoDestroy: false, documentImpl });
        if (destroyed || ticket !== generation) { candidate.destroy(); return renderStatus; }
        glass = candidate;
        const onContextLost = (event) => {
          event.preventDefault?.();
          if (destroyed || glass !== candidate) return;
          generation++;
          releaseGlass();
          notifyStatus("static", "context-lost");
        };
        candidate.canvas.addEventListener("webglcontextlost", onContextLost);
        detachContext = () => candidate.canvas.removeEventListener("webglcontextlost", onContextLost);
        if (currentTheme !== initialTheme) candidate.setTheme(currentTheme);
        if (background !== initialBackground) await candidate.setBackground(background);
        if (!runningScene(candidate.scene)) { releaseGlass(); return notifyStatus("static", "unavailable"); }
        notifyStatus("static", "awaiting-frame");
        const painted = await nextPaint();
        if (destroyed || ticket !== generation) return renderStatus;
        if (!painted || !validOpticalFrame(candidate.scene)) { releaseGlass(); return notifyStatus("static", "unavailable"); }
        return notifyStatus("optical", "ready", true);
      } catch (error) {
        if (glass === candidate) releaseGlass();
        if (destroyed || ticket !== generation) return renderStatus;
        if (configurationError(error)) throw error;
        diagnostics.push({ message: error?.message || "Optical rendering unavailable", role: null });
        if (diagnostics.length > 100) diagnostics.shift();
        try { onError?.(error, { target: null, rendering: true }); } catch { /* A readable fallback remains usable. */ }
        return notifyStatus("static", "unavailable");
      }
    });
    return renderQueue;
  };

  const syncRange = (target) => {
    if (!enhanceControls || ignoredTarget(target) || !target?.matches?.(RANGE_SELECTOR)) return;
    if (!enhancedRanges.has(target)) {
      enhancedRanges.set(target, {
        value: target.style.getPropertyValue("--glass-ui-range-progress"),
        priority: target.style.getPropertyPriority("--glass-ui-range-progress"),
      });
    }
    target.style.setProperty("--glass-ui-range-progress", `${rangeProgress(target).toFixed(3)}%`);
  };

  const scanNativeControls = (node = scope) => {
    if (!enhanceControls) return;
    if (node?.matches?.(RANGE_SELECTOR)) syncRange(node);
    node?.querySelectorAll?.(RANGE_SELECTOR).forEach(syncRange);
  };

  const releaseNativeControls = (node = scope) => {
    enhancedRanges.forEach((snapshot, target) => {
      if (node !== scope && node !== target && !node?.contains?.(target)) return;
      if (snapshot.value) {
        target.style.setProperty("--glass-ui-range-progress", snapshot.value, snapshot.priority);
      } else {
        target.style.removeProperty("--glass-ui-range-progress");
      }
      enhancedRanges.delete(target);
    });
  };

  const handleNativeInput = (event) => syncRange(event.target);

  const remember = (target) => {
    if (snapshots.has(target)) return snapshots.get(target);
    const snapshot = {
      role: attributeSnapshot(target, "data-glass-role"),
      preset: attributeSnapshot(target, "data-optical-glass"),
      options: attributeSnapshot(target, "data-glass-options"),
      state: attributeSnapshot(target, "data-glass-ui-state"),
      hitArea: attributeSnapshot(target, "data-glass-ui-hit"),
      safeArea: attributeSnapshot(target, "data-glass-safe-area"),
    };
    snapshots.set(target, snapshot);
    managedTargets.add(target);
    return snapshot;
  };

  const releaseIgnoredTargets = () => {
    managedTargets.forEach((target) => {
      if (!ignoredTarget(target)) return;
      const snapshot = snapshots.get(target);
      if (overrides.has(target)) restoreAttribute(target, "data-glass-role", snapshot.role);
      for (const [name, value] of [["data-optical-glass", snapshot.preset],
        ["data-glass-options", snapshot.options], ["data-glass-ui-state", snapshot.state],
        ["data-glass-ui-hit", snapshot.hitArea], ["data-glass-safe-area", snapshot.safeArea]]) restoreAttribute(target, name, value);
      releaseNativeControls(target);
      managedTargets.delete(target); snapshots.delete(target); overrides.delete(target);
    });
  };

  const report = (message, target) => {
    const error = message instanceof Error ? message : new Error(String(message));
    diagnostics.push({ message: error.message, role: target?.getAttribute?.("data-glass-role") || null });
    if (diagnostics.length > 100) diagnostics.shift();
    target?.setAttribute?.("data-glass-ui-state", "error");
    onError?.(error, { target });
    return error;
  };

  const applyTarget = (target, failFast = false) => {
    if (!target?.getAttribute || ignoredTarget(target)) return false;
    const role = String(target.getAttribute("data-glass-role") || "").trim();
    if (!role) return false;
    const definition = roleDefinitions[role];
    if (!definition) {
      const error = report(`GlassUI: unknown role "${role}"`, target);
      if (failFast) throw error;
      return false;
    }
    const snapshot = remember(target);
    const authoredOptions = parseOptions(snapshot.options.value);
    const localOptions = overrides.get(target) || {};
    const nativeField = role === "field" && (target.matches?.("input, textarea, select, [contenteditable]")
      || target.querySelector?.("input, textarea, select, [contenteditable]"));
    const multiline = nativeField && (target.matches?.("textarea, [contenteditable]") || target.querySelector?.("textarea, [contenteditable]"));
    const mergedOptions = { ...(multiline ? { shape: { radius: 0.1, radiusMax: 20 } } : {}),
      ...definition.options, ...authoredOptions, ...localOptions };
    if (nativeField && !snapshot.safeArea.present) target.setAttribute("data-glass-safe-area", "off");
    else restoreAttribute(target, "data-glass-safe-area", snapshot.safeArea);
    target.setAttribute("data-optical-glass", definition.preset);
    target.setAttribute("data-glass-ui-hit", definition.hitArea);
    if (Object.keys(mergedOptions).length) {
      target.setAttribute("data-glass-options", JSON.stringify(mergedOptions));
    } else if (!snapshot.options.present) {
      target.removeAttribute("data-glass-options");
    }
    target.setAttribute("data-glass-ui-state", "ready");
    return true;
  };

  const scan = (node = scope, failFast = false) => {
    releaseIgnoredTargets();
    if (node?.nodeType === 1 && node.hasAttribute?.("data-glass-role")) applyTarget(node, failFast);
    node?.querySelectorAll?.("[data-glass-role]").forEach((target) => applyTarget(target, failFast));
  };

  const MutationObserverImpl = documentImpl.defaultView?.MutationObserver ?? globalThis.MutationObserver;
  if (observe && !MutationObserverImpl) throw new Error("GlassUI: MutationObserver is unavailable");
  if (strict) {
    const initialTargets = [
      ...(scope?.nodeType === 1 && scope.hasAttribute?.("data-glass-role") ? [scope] : []),
      ...(scope?.querySelectorAll?.("[data-glass-role]") || []),
    ];
    initialTargets.forEach((target) => {
      const role = String(target.getAttribute("data-glass-role") || "").trim();
      if (!ignoredTarget(target) && !roleDefinitions[role]) throw new Error(`GlassUI: unknown role "${role}"`);
    });
  }
  scan(scope);
  scanNativeControls(scope);
  if (enhanceControls) scope.addEventListener?.("input", handleNativeInput);
  const roleObserver = observe ? new MutationObserverImpl((records) => {
    records.forEach((record) => {
      if (record.type === "attributes") {
        if (record.attributeName === "data-glass-ignore") {
          releaseNativeControls(record.target);
          scan(record.target);
          scanNativeControls(record.target);
          glass?.composition.refresh();
          if (record.target === scope || record.target === materialRoot) handlePreference();
          return;
        }
        if (record.target.hasAttribute("data-glass-role")) applyTarget(record.target);
        else if (snapshots.has(record.target)) {
          const snapshot = snapshots.get(record.target);
          restoreAttribute(record.target, "data-optical-glass", snapshot.preset);
          restoreAttribute(record.target, "data-glass-options", snapshot.options);
          restoreAttribute(record.target, "data-glass-ui-state", snapshot.state);
          restoreAttribute(record.target, "data-glass-ui-hit", snapshot.hitArea);
          restoreAttribute(record.target, "data-glass-safe-area", snapshot.safeArea);
        }
        return;
      }
      record.addedNodes.forEach((node) => {
        scan(node);
        scanNativeControls(node);
      });
      record.removedNodes.forEach((node) => {
        releaseNativeControls(node);
        managedTargets.forEach((target) => {
          if (node !== target && !node.contains?.(target)) return;
          if (scope.contains?.(target)) return;
          const snapshot = snapshots.get(target);
          if (overrides.has(target)) restoreAttribute(target, "data-glass-role", snapshot.role);
          restoreAttribute(target, "data-optical-glass", snapshot.preset);
          restoreAttribute(target, "data-glass-options", snapshot.options);
          restoreAttribute(target, "data-glass-ui-state", snapshot.state);
          restoreAttribute(target, "data-glass-ui-hit", snapshot.hitArea);
          restoreAttribute(target, "data-glass-safe-area", snapshot.safeArea);
          managedTargets.delete(target);
          snapshots.delete(target);
          overrides.delete(target);
        });
      });
    });
  }) : null;
  roleObserver?.observe(scope, {
    subtree: true,
    childList: true,
    attributes: true,
    attributeFilter: ["data-glass-role", "data-glass-ignore"],
  });

  const controller = {
    get glass() { return renderStatus.mode === "optical" ? glass : null; },
    get scene() { return renderStatus.mode === "optical" ? glass?.scene || null : null; },
    get composition() { return renderStatus.mode === "optical" ? glass?.composition || null : null; },
    get canvas() { return renderStatus.mode === "optical" ? glass?.canvas || null : null; },
    get renderPolicy() { return renderPolicy; },
    get renderMode() { return renderStatus.mode; },
    get renderStatus() { return renderStatus; },
    setRenderPolicy(policy) {
      if (destroyed) throw new Error("GlassUI: controller is destroyed");
      checkedRenderPolicy(policy);
      if (policy !== "static" && !background) throw new Error("OpticalGlass site: background is required");
      renderPolicy = policy;
      return scheduleRender();
    },
    get theme() {
      return cloneJson(currentTheme);
    },
    get roles() {
      return cloneJson(roleDefinitions);
    },
    get diagnostics() {
      return diagnostics.map((item) => ({ ...item }));
    },
    setRole(reference, role, options = {}) {
      if (destroyed) throw new Error("GlassUI: controller is destroyed");
      if (!roleDefinitions[role]) throw new Error(`GlassUI: unknown role "${role}"`);
      if (!isPlainObject(options) || "preset" in options || "target" in options || "content" in options) {
        throw new Error("GlassUI: invalid role options");
      }
      const targets = collectTargets(scope, reference).filter((target) => !ignoredTarget(target));
      targets.forEach((target) => {
        remember(target);
        overrides.set(target, cloneJson(options));
        target.setAttribute("data-glass-role", role);
        applyTarget(target, true);
      });
      glass?.composition.refresh();
      return targets;
    },
    removeRole(reference) {
      if (destroyed) throw new Error("GlassUI: controller is destroyed");
      const targets = collectTargets(scope, reference).filter((target) => !ignoredTarget(target));
      targets.forEach((target) => {
        remember(target);
        // The imperative API owns the removal until cleanup, including its rollback.
        overrides.set(target, {});
        target.removeAttribute("data-glass-role");
        target.removeAttribute("data-optical-glass");
        target.removeAttribute("data-glass-options");
        target.removeAttribute("data-glass-ui-state");
        target.removeAttribute("data-glass-ui-hit");
        restoreAttribute(target, "data-glass-safe-area", snapshots.get(target).safeArea);
      });
      glass?.composition.refresh();
      return targets;
    },
    registerRole(name, definition) {
      if (destroyed) throw new Error("GlassUI: controller is destroyed");
      roleDefinitions = defineGlassRoles({ ...roleDefinitions, [name]: definition }, currentTheme);
      scan(scope, strict);
      glass?.composition.refresh();
      return this.roles[name];
    },
    setTheme(nextTheme) {
      if (destroyed) throw new Error("GlassUI: controller is destroyed");
      const composed = composeGlassThemes(GLASS_UI_THEME, nextTheme);
      const nextRoles = defineGlassRoles(roleDefinitions, composed);
      glass?.setTheme(composed);
      currentTheme = composed;
      roleDefinitions = nextRoles;
      scan(scope, strict);
      return this.theme;
    },
    setBackground(source, options) {
      if (destroyed) throw new Error("GlassUI: controller is destroyed");
      if (!source) throw new Error("OpticalGlass site: background is required");
      background = source;
      return glass ? glass.setBackground(source, options) : Promise.resolve(null);
    },
    refresh() {
      if (destroyed) throw new Error("GlassUI: controller is destroyed");
      scan(scope, strict);
      glass?.composition.refresh();
      return this;
    },
    destroy() {
      if (destroyed) return;
      destroyed = true;
      generation++;
      defaultView?.removeEventListener?.("pagehide", handlePageHide);
      forcedColors?.removeEventListener?.("change", handlePreference);
      reducedMotion?.removeEventListener?.("change", handlePreference);
      scope.removeEventListener?.("input", handleNativeInput);
      roleObserver?.disconnect();
      releaseGlass();
      releaseNativeControls(scope);
      managedTargets.forEach((target) => {
        const snapshot = snapshots.get(target);
        if (overrides.has(target)) restoreAttribute(target, "data-glass-role", snapshot.role);
        restoreAttribute(target, "data-optical-glass", snapshot.preset);
        restoreAttribute(target, "data-glass-options", snapshot.options);
        restoreAttribute(target, "data-glass-ui-state", snapshot.state);
        restoreAttribute(target, "data-glass-ui-hit", snapshot.hitArea);
        restoreAttribute(target, "data-glass-safe-area", snapshot.safeArea);
      });
      managedTargets.clear();
      renderStatus = Object.freeze({ policy: renderPolicy, mode: "static", reason: "destroyed", gpuReady: false });
      for (const [name, snapshot] of Object.entries(rootAttributes)) restoreAttribute(materialRoot, name, snapshot);
      if (!hadRootClass) materialRoot.classList.remove("glass-ui");
    },
  };

  const handlePageHide = (event) => {
    if (!event.persisted) controller.destroy();
  };
  const handlePreference = () => {
    if (!destroyed) scheduleRender().catch((error) => { try { report(error); } catch { /* Async host diagnostics do not own native controls. */ } });
  };
  forcedColors?.addEventListener?.("change", handlePreference);
  reducedMotion?.addEventListener?.("change", handlePreference);
  if (autoDestroy) defaultView?.addEventListener?.("pagehide", handlePageHide);
  try { await scheduleRender(); }
  catch (error) { controller.destroy(); throw error; }
  return controller;
};

export default createGlassUI;
