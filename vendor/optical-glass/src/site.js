import OpticalGlass from "./liquid-glass.js";

export const GLASS_THEME_FORMAT = "optical-glass-theme";
export const GLASS_THEME_VERSION = 1;
export const GLASS_CORE_PRESETS = Object.freeze(Object.keys(OpticalGlass.presets));
const CORE_PRESET_NAMES = new Set(GLASS_CORE_PRESETS);

const isPlainObject = (value) => (
  value !== null
  && typeof value === "object"
  && !Array.isArray(value)
  && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
);

const cloneJson = (value) => JSON.parse(JSON.stringify(value));

const parseThemeInput = (input) => {
  let theme = input;
  if (typeof theme === "string") {
    try {
      theme = JSON.parse(theme);
    } catch (error) {
      throw new Error(`OpticalGlass site: invalid theme JSON: ${error.message}`);
    }
  }
  if (!isPlainObject(theme)) throw new Error("OpticalGlass site: theme must be an object");
  if (theme.format && theme.format !== GLASS_THEME_FORMAT) {
    throw new Error(`OpticalGlass site: unsupported theme format "${theme.format}"`);
  }
  const version = Number(theme.version ?? GLASS_THEME_VERSION);
  if (version !== GLASS_THEME_VERSION) {
    throw new Error(`OpticalGlass site: unsupported theme version ${version}`);
  }
  return theme;
};

const presetReferences = (specification) => (
  Array.isArray(specification.preset)
    ? specification.preset.map(String).filter(Boolean)
    : String(specification.preset || "").split(/[\s,]+/).filter(Boolean)
);

const orderThemePresets = (presets) => {
  const names = new Set(Object.keys(presets));
  const resolved = new Set();
  const resolving = [];
  const order = [];

  const visit = (name) => {
    if (resolved.has(name)) return;
    const cycleIndex = resolving.indexOf(name);
    if (cycleIndex >= 0) {
      const cycle = [...resolving.slice(cycleIndex), name].join(" -> ");
      throw new Error(`OpticalGlass site: circular preset inheritance: ${cycle}`);
    }
    resolving.push(name);
    presetReferences(presets[name]).forEach((dependency) => {
      if (names.has(dependency)) visit(dependency);
      else if (!CORE_PRESET_NAMES.has(dependency)) {
        throw new Error(`OpticalGlass site: preset "${name}" inherits unknown preset "${dependency}"`);
      }
    });
    resolving.pop();
    resolved.add(name);
    order.push(name);
  };

  names.forEach(visit);
  return order;
};

/** Validate and detach a portable, JSON-safe theme. */
export const defineGlassTheme = (input) => {
  const theme = parseThemeInput(input);
  if (!isPlainObject(theme.presets) || Object.keys(theme.presets).length === 0) {
    throw new Error("OpticalGlass site: theme must contain at least one preset");
  }

  const presets = {};
  Object.entries(theme.presets).forEach(([name, specification]) => {
    if (!/^[a-z][a-z0-9-]{0,63}$/i.test(name)) {
      throw new Error(`OpticalGlass site: invalid preset name "${name}"`);
    }
    if (!isPlainObject(specification)) {
      throw new Error(`OpticalGlass site: preset "${name}" must be an object`);
    }
    if (CORE_PRESET_NAMES.has(name)) {
      throw new Error(`OpticalGlass site: preset name "${name}" is reserved`);
    }
    if ("target" in specification || "content" in specification) {
      throw new Error(`OpticalGlass site: preset "${name}" cannot contain target or content`);
    }
    presets[name] = cloneJson(specification);
  });

  orderThemePresets(presets);

  return {
    format: GLASS_THEME_FORMAT,
    version: GLASS_THEME_VERSION,
    name: String(theme.name || "Site glass"),
    presets,
  };
};

/** Combine complete or partial portable themes before resolving inheritance. */
export const composeGlassThemes = (...inputs) => {
  const themes = inputs.flat().filter(Boolean).map(parseThemeInput);
  if (!themes.length) throw new Error("OpticalGlass site: at least one theme is required");
  const presets = {};
  themes.forEach((theme) => {
    if (!isPlainObject(theme.presets)) {
      throw new Error("OpticalGlass site: every composed theme must contain presets");
    }
    Object.entries(theme.presets).forEach(([name, specification]) => {
      presets[name] = cloneJson(specification);
    });
  });
  return defineGlassTheme({
    format: GLASS_THEME_FORMAT,
    version: GLASS_THEME_VERSION,
    name: String(themes.at(-1)?.name || "Composed glass theme"),
    presets,
  });
};

export const glassThemeToJSON = (theme, indentation = 2) => (
  JSON.stringify(defineGlassTheme(theme), null, Math.max(0, Math.min(8, Number(indentation) || 0)))
);

export const DEFAULT_GLASS_THEME = defineGlassTheme({
  name: "Calm production glass",
  presets: {
    "site-panel": {
      preset: "quiet",
      shape: { radius: 0.2, radiusMax: 120, bevel: 0.055, irregularity: 0.28 },
      optics: { ior: 1.47, depth: 68, refraction: 0.76, roughness: 0.012 },
      style: { tint: [0.78, 0.92, 1], tintStrength: 0.18, matte: 0.025 },
      surface: { texture: 0.22, scale: 1, grain: 0.24, rear: 0.24 },
      motion: { tilt: 1.1, parallax: 1.8, lift: 2.6, contentDepth: 0.65 },
      interaction: 0.44,
    },
    "site-control": {
      preset: "quiet",
      shape: { radius: 0.5, radiusMax: 999, bevel: 0.085, irregularity: 0.2 },
      optics: { ior: 1.47, depth: 42, refraction: 0.72, roughness: 0.01 },
      style: { tint: [0.76, 0.91, 1], tintStrength: 0.2, matte: 0.02 },
      surface: { texture: 0.18, scale: 1, grain: 0.2, rear: 0.2 },
      motion: { mode: "surface", tilt: 1.25, parallax: 2, lift: 3, contentDepth: 0.6 },
      interaction: 0.48,
    },
    "site-line": {
      preset: "line",
      optics: { depth: 18, refraction: 0.58, roughness: 0.008 },
      style: { tint: [0.58, 0.86, 1], tintStrength: 0.26, matte: 0.01 },
      interaction: 0.22,
    },
  },
});

const resolveScope = (root, documentImpl) => {
  if (!root) return documentImpl;
  if (typeof root === "string") {
    const resolved = documentImpl?.querySelector?.(root);
    if (!resolved) throw new Error(`OpticalGlass site: root not found: ${root}`);
    return resolved;
  }
  return root;
};

const queryWithin = (scope, selector, documentImpl) => (
  scope?.querySelector?.(selector) ?? documentImpl?.querySelector?.(selector) ?? null
);

const ensureSiteStyles = (documentImpl) => {
  if (documentImpl.getElementById?.("optical-glass-site-styles")) return;
  const style = documentImpl.createElement("style");
  style.id = "optical-glass-site-styles";
  style.textContent = `
    .optical-glass-site-root { isolation: isolate; }
    .optical-glass-site-root > .optical-glass-site-canvas {
      z-index: 0;
      pointer-events: none;
    }
    .optical-glass-site-root > :where(:not(.optical-glass-site-canvas)) {
      position: relative;
      z-index: 1;
    }
  `;
  documentImpl.head?.append(style);
};

const prepareCanvas = ({ canvas, canvasMode = "root", scope, documentImpl }) => {
  if (!["root", "viewport"].includes(canvasMode)) {
    throw new Error(`OpticalGlass site: unsupported canvas mode "${canvasMode}"`);
  }
  if (canvas && typeof canvas !== "string") return { canvas, owned: false, restoreHost: null };
  if (typeof canvas === "string") {
    const resolved = queryWithin(scope, canvas, documentImpl);
    if (!resolved) throw new Error(`OpticalGlass site: canvas not found: ${canvas}`);
    return { canvas: resolved, owned: false, restoreHost: null };
  }

  if (scope?.nodeType === 11) {
    throw new Error("OpticalGlass site: ShadowRoot integrations require an existing canvas");
  }

  const host = scope?.nodeType === 9 ? scope.body : scope;
  if (!host?.prepend || !documentImpl?.createElement) {
    throw new Error("OpticalGlass site: an existing canvas is required for this root");
  }

  const created = documentImpl.createElement("canvas");
  created.className = "optical-glass-site-canvas";
  created.setAttribute("aria-hidden", "true");
  created.dataset.opticalGlassCanvasMode = canvasMode;
  const viewportCanvas = scope?.nodeType === 9 || canvasMode === "viewport";
  Object.assign(created.style, {
    position: viewportCanvas ? "fixed" : "absolute",
    zIndex: "0",
    inset: "0",
    display: "block",
    width: viewportCanvas ? "100vw" : "100%",
    height: viewportCanvas ? "100vh" : "100%",
    pointerEvents: "none",
  });

  const previousPosition = host.style.position;
  const previousIsolation = host.style.isolation;
  const hadRootClass = host.classList.contains("optical-glass-site-root");
  const computedPosition = documentImpl.defaultView?.getComputedStyle?.(host).position;
  if (!computedPosition || computedPosition === "static") host.style.position = "relative";
  host.style.isolation = "isolate";
  host.classList.add("optical-glass-site-root");
  host.prepend(created);

  return {
    canvas: created,
    owned: true,
    restoreHost: () => {
      host.style.position = previousPosition;
      host.style.isolation = previousIsolation;
      if (!hadRootClass) host.classList.remove("optical-glass-site-root");
    },
  };
};

const installTheme = (scene, theme) => {
  orderThemePresets(theme.presets).forEach((name) => {
    scene.definePreset(name, theme.presets[name]);
  });
};

/**
 * One-call integration for a site or isolated section.
 * Creates a canvas when omitted, mounts declarative elements and starts rendering.
 */
export const createGlassSite = async ({
  root,
  canvas,
  canvasMode = "root",
  background,
  fallbackBackground,
  theme = DEFAULT_GLASS_THEME,
  selector = "[data-optical-glass], [data-liquid-glass]",
  observe = true,
  autoDestroy = true,
  documentImpl = globalThis.document,
  ...sceneOptions
} = {}) => {
  if (!documentImpl) throw new Error("OpticalGlass site: document is unavailable");
  if (!background) throw new Error("OpticalGlass site: background is required");

  ensureSiteStyles(documentImpl);
  const scope = resolveScope(root, documentImpl);
  const canvasSetup = prepareCanvas({ canvas, canvasMode, scope, documentImpl });
  let currentTheme = defineGlassTheme(theme);
  let destroyed = false;
  let composition;
  let scene;

  try {
    scene = OpticalGlass.create({
      ...sceneOptions,
      root: scope,
      canvas: canvasSetup.canvas,
      background,
      fallbackBackground,
    });
    installTheme(scene, currentTheme);
    composition = scene.mount(scope, { selector, observe });
    await scene.start();
  } catch (error) {
    composition?.destroy?.();
    scene?.destroy?.();
    if (canvasSetup.owned) canvasSetup.canvas.remove?.();
    canvasSetup.restoreHost?.();
    throw error;
  }

  const defaultView = documentImpl.defaultView;
  const controller = {
    scene,
    composition,
    canvas: canvasSetup.canvas,
    get theme() {
      return cloneJson(currentTheme);
    },
    setTheme(nextTheme) {
      if (destroyed) throw new Error("OpticalGlass site: controller is destroyed");
      const normalizedTheme = defineGlassTheme(nextTheme);
      installTheme(scene, normalizedTheme);
      currentTheme = normalizedTheme;
      composition.refresh();
      return this.theme;
    },
    exportTheme(indentation = 2) {
      return glassThemeToJSON(currentTheme, indentation);
    },
    setBackground(source, options) {
      if (destroyed) throw new Error("OpticalGlass site: controller is destroyed");
      return scene.setBackground(source, options);
    },
    destroy() {
      if (destroyed) return;
      destroyed = true;
      defaultView?.removeEventListener?.("pagehide", handlePageHide);
      composition.destroy();
      scene.destroy();
      if (canvasSetup.owned) canvasSetup.canvas.remove();
      canvasSetup.restoreHost?.();
    },
  };

  const handlePageHide = (event) => {
    if (!event.persisted) controller.destroy();
  };
  if (autoDestroy) defaultView?.addEventListener?.("pagehide", handlePageHide);
  return controller;
};

export default createGlassSite;
