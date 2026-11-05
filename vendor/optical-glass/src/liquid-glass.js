const OpticalGlass = (() => {
  "use strict";

  const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
  const nextPowerOfTwo = (value) => 2 ** Math.ceil(Math.log2(Math.max(1, value)));
  const resolveValue = (value, context) =>
    typeof value === "function" ? value(context) : value;

  const VOLUME_ENGRAVING_DEFAULTS = Object.freeze({
    thickness: 0.54,
    position: 0,
    scatter: 1,
    facets: 0.82,
    parallax: 1,
    grain: 0.52,
    glow: 0.68,
    dispersion: 0.32,
  });

  const mergeGlassSpecification = (base = {}, patch = {}) => {
    const merged = {
      ...base,
      ...patch,
      shape: patch.shape ? { ...(base.shape || {}), ...patch.shape } : base.shape,
      optics: patch.optics ? { ...(base.optics || {}), ...patch.optics } : base.optics,
      style: patch.style ? { ...(base.style || {}), ...patch.style } : base.style,
      surface: patch.surface ? { ...(base.surface || {}), ...patch.surface } : base.surface,
      motion: patch.motion ? { ...(base.motion || {}), ...patch.motion } : base.motion,
    };
    for (const property of ["shape", "optics", "style", "surface", "motion"]) {
      if (merged[property] === undefined) delete merged[property];
    }
    for (const property of ["layers", "content"]) {
      if (Object.prototype.hasOwnProperty.call(patch, property)) {
        merged[property] = patch[property];
      } else if (Object.prototype.hasOwnProperty.call(base, property)) {
        merged[property] = base[property];
      } else {
        delete merged[property];
      }
    }
    return merged;
  };

  const BUILT_IN_PRESETS = Object.freeze({
    quiet: {
      shape: { radius: 0.24, radiusMax: 144, bevel: 0.075, irregularity: 0.16 },
      optics: { ior: 1.455, refraction: 0.58, roughness: 0.009, depth: 46 },
      style: { tint: [0.88, 0.96, 1], tintStrength: 0.09, matte: 0.014 },
      surface: { texture: 0.16, scale: 1.1, grain: 0.18, rear: 0.14 },
      interaction: 0.3,
      motion: {
        mode: "group",
        coupling: 0.86,
        parallax: 1,
        tilt: 0.6,
        lift: 1.5,
        scale: 0.001,
        press: 0.004,
        inertia: 0.11,
        layerSeparation: 0.45,
        contentDepth: 0.4,
      },
    },
    clear: {
      shape: { radius: 0.34, radiusMax: 242, bevel: 0.205, irregularity: 1.1 },
      optics: { ior: 1.47, refraction: 1.08, roughness: 0.018, depth: 48 },
      style: { tint: [0.82, 0.94, 1], tintStrength: 0.32, matte: 0.015 },
      surface: { texture: 0.82, scale: 1, grain: 0.72, rear: 0.82 },
      interaction: 1,
      motion: { mode: "group", coupling: 0.78, parallax: 6.2, tilt: 4.8, lift: 9, scale: 0.006, press: 0.014, inertia: 0.13, layerSeparation: 3.2 },
    },
    frosted: {
      shape: { radius: 0.3, radiusMax: 220, bevel: 0.19, irregularity: 1.25 },
      optics: { ior: 1.46, refraction: 1.02, roughness: 0.075, depth: 62 },
      style: { tint: [0.72, 0.88, 1], tintStrength: 0.44, matte: 0.32 },
      surface: { texture: 0.72, scale: 0.92, grain: 0.86, rear: 0.78 },
      interaction: 0.86,
      motion: { mode: "group", coupling: 0.76, parallax: 5.2, tilt: 4.1, lift: 7, scale: 0.005, press: 0.012, inertia: 0.115, layerSeparation: 2.8 },
    },
    panel: {
      shape: { radius: 0.12, radiusMax: 58, bevel: 0.13, irregularity: 0.72 },
      optics: { ior: 1.455, refraction: 0.78, roughness: 0.046, depth: 54 },
      style: { tint: [0.7, 0.86, 0.96], tintStrength: 0.38, matte: 0.18 },
      surface: { texture: 0.58, scale: 1.2, grain: 0.62, rear: 0.7 },
      interaction: 0.42,
      motion: { mode: "group", coupling: 0.72, parallax: 4.4, tilt: 3.25, lift: 6, scale: 0.004, press: 0.01, inertia: 0.09, layerSeparation: 2.4 },
    },
    control: {
      shape: { radius: 0.5, radiusMax: 999, bevel: 0.22, irregularity: 0.48 },
      optics: { ior: 1.47, refraction: 1.12, roughness: 0.028, depth: 38 },
      style: { tint: [0.78, 0.92, 1], tintStrength: 0.46, matte: 0.055 },
      surface: { texture: 0.48, scale: 1.1, grain: 0.5, rear: 0.6 },
      interaction: 1.18,
      motion: { mode: "surface", coupling: 0.68, parallax: 7.4, tilt: 5.6, lift: 11, scale: 0.008, press: 0.018, inertia: 0.17, layerSeparation: 3.8 },
    },
    dense: {
      shape: { radius: 0.31, radiusMax: 220, bevel: 0.2, irregularity: 1.05 },
      optics: { ior: 1.49, refraction: 1.3, roughness: 0.055, depth: 108 },
      style: { tint: [0.66, 0.84, 0.98], tintStrength: 0.68, matte: 0.24 },
      surface: { texture: 0.82, scale: 0.84, grain: 0.74, rear: 0.92 },
      interaction: 0.96,
      motion: { mode: "surface", coupling: 0.7, parallax: 6.8, tilt: 5.1, lift: 12, scale: 0.007, press: 0.015, inertia: 0.12, layerSeparation: 4.2 },
    },
    line: {
      shape: { radius: 0.5, radiusMax: 999, bevel: 0.32, irregularity: 0.26 },
      optics: { ior: 1.47, refraction: 1.18, roughness: 0.022, depth: 24 },
      style: { tint: [0.72, 0.9, 1], tintStrength: 0.42, matte: 0.025 },
      surface: { texture: 0.32, scale: 1.4, grain: 0.34, rear: 0.38 },
      interaction: 0.72,
      motion: { mode: "surface", coupling: 0.62, parallax: 4.8, tilt: 3.8, lift: 5, scale: 0.004, press: 0.012, inertia: 0.16, layerSeparation: 2.6 },
    },
  });

  const createCanvas = (width = 1, height = 1) => {
    const canvas = document.createElement("canvas");
    canvas.width = Math.max(1, Math.ceil(width));
    canvas.height = Math.max(1, Math.ceil(height));
    return canvas;
  };

  const imageDimensions = (image) => [
    image?.naturalWidth || image?.videoWidth || image?.width || 1,
    image?.naturalHeight || image?.videoHeight || image?.height || 1,
  ];

  const createFallbackBackground = () => {
    const canvas = createCanvas(1600, 1000);
    const context = canvas.getContext("2d");
    const base = context.createLinearGradient(0, 0, canvas.width, canvas.height);
    base.addColorStop(0, "#173346");
    base.addColorStop(0.46, "#557c87");
    base.addColorStop(1, "#d5b08d");
    context.fillStyle = base;
    context.fillRect(0, 0, canvas.width, canvas.height);

    const glow = context.createRadialGradient(1180, 190, 20, 1180, 190, 720);
    glow.addColorStop(0, "rgba(255, 236, 198, .72)");
    glow.addColorStop(0.42, "rgba(199, 224, 222, .22)");
    glow.addColorStop(1, "rgba(17, 41, 57, 0)");
    context.fillStyle = glow;
    context.fillRect(0, 0, canvas.width, canvas.height);
    return canvas;
  };

  const loadImage = (source, { crossOrigin } = {}) =>
    new Promise((resolve, reject) => {
      if (source instanceof HTMLImageElement) {
        if (source.complete && source.naturalWidth) resolve(source);
        else {
          source.addEventListener("load", () => resolve(source), { once: true });
          source.addEventListener("error", reject, { once: true });
        }
        return;
      }

      const image = new Image();
      if (crossOrigin) image.crossOrigin = crossOrigin;
      image.decoding = "async";
      image.addEventListener("load", () => resolve(image), { once: true });
      image.addEventListener("error", reject, { once: true });
      image.src = source;
    });

  const VERTEX_SHADER = `#version 300 es
    in vec2 a_position;

    void main() {
      gl_Position = vec4(a_position, 0.0, 1.0);
    }
  `;

  const fragmentShader = () => `#version 300 es
    precision highp float;

    uniform sampler2D u_background;
    uniform sampler2D u_scene;
    uniform sampler2D u_fieldAtlas;
    uniform sampler2D u_colorAtlas;
    uniform sampler2D u_shapeAtlas;
    uniform vec2 u_resolution;
    uniform vec2 u_imageSize;
    uniform float u_dpr;
    uniform float u_time;
    uniform float u_textureStrength;
    uniform int u_activeElement;
    uniform vec4 u_rect;
    uniform vec4 u_state;
    uniform vec4 u_shape;
    uniform vec4 u_optics;
    uniform vec4 u_style;
    uniform vec4 u_styleExtra;
    uniform vec4 u_surface;
    uniform vec4 u_contentScreen;
    uniform vec4 u_contentAtlasRect;
    uniform vec4 u_shapeAtlasRect;
    uniform vec4 u_contentOpts;
    uniform vec4 u_volumeOpts;
    uniform vec4 u_volumeStyle;

    out vec4 outColor;

    vec3 srgbToLinear(vec3 color) {
      vec3 low = color / 12.92;
      vec3 high = pow((color + 0.055) / 1.055, vec3(2.4));
      return mix(low, high, step(vec3(0.04045), color));
    }

    vec3 linearToSrgb(vec3 color) {
      color = max(color, vec3(0.0));
      vec3 low = color * 12.92;
      vec3 high = 1.055 * pow(color, vec3(1.0 / 2.4)) - 0.055;
      return mix(low, high, step(vec3(0.0031308), color));
    }

    float sdRoundRect(vec2 p, vec4 rect, float radius) {
      vec2 halfSize = rect.zw * 0.5;
      vec2 center = rect.xy + halfSize;
      vec2 q = abs(p - center) - halfSize + vec2(radius);
      return min(max(q.x, q.y), 0.0) + length(max(q, 0.0)) - radius;
    }

    vec2 coverUv(vec2 screenUv) {
      float screenAspect = u_resolution.x / u_resolution.y;
      float imageAspect = u_imageSize.x / u_imageSize.y;
      vec2 uv = screenUv;

      if (screenAspect > imageAspect) {
        float visibleHeight = imageAspect / screenAspect;
        uv.y = (uv.y - 0.5) * visibleHeight + 0.5;
      } else {
        float visibleWidth = screenAspect / imageAspect;
        uv.x = (uv.x - 0.5) * visibleWidth + 0.5;
      }

      return clamp(uv, 0.001, 0.999);
    }

    vec3 sampleBackground(vec2 pixel) {
      vec2 screenUv = clamp(pixel / u_resolution, 0.0, 1.0);
      return texture(u_background, coverUv(screenUv)).rgb;
    }

    vec3 sampleBackgroundLod(vec2 pixel, float lod) {
      vec2 screenUv = clamp(pixel / u_resolution, 0.0, 1.0);
      return textureLod(u_background, coverUv(screenUv), lod).rgb;
    }

    vec3 sampleScene(vec2 pixel) {
      vec2 uv = clamp(pixel / u_resolution, 0.0, 1.0);
      uv.y = 1.0 - uv.y;
      return texture(u_scene, uv).rgb;
    }

    vec4 sampleAtlas(
      sampler2D atlas,
      vec2 pixel,
      vec4 screenRect,
      vec4 atlasRect
    ) {
      vec2 local = (pixel - screenRect.xy) / max(screenRect.zw, vec2(1.0));
      float inside = step(0.0, local.x) * step(0.0, local.y)
        * step(local.x, 1.0) * step(local.y, 1.0);
      vec2 uv = atlasRect.xy + clamp(local, 0.0, 1.0) * atlasRect.zw;
      return texture(atlas, uv) * inside;
    }

    vec2 tiltLensPoint(vec2 p, vec4 rect, vec4 state, float interaction) {
      float hover = state.x;
      vec2 pointer = state.zw;
      vec2 center = rect.xy + rect.zw * 0.5;
      vec2 halfSize = max(rect.zw * 0.5, vec2(1.0));
      vec2 local = p - center;
      vec2 normalized = local / halfSize;
      vec2 compression = vec2(1.0)
        + abs(pointer) * hover * interaction * vec2(0.046, 0.04);
      float perspective = 1.0
        + hover * interaction * 0.058 * dot(pointer, normalized);
      return center + local * compression / max(perspective, 0.86);
    }

    float materialNoise(vec2 p, float seed) {
      vec2 q = p / u_dpr;
      float low = sin(q.x * 0.012 + seed * 1.73)
        + sin(q.y * 0.009 - seed * 2.17)
        + sin((q.x + q.y) * 0.0055 + seed * 4.31);
      float cross = sin((q.x - q.y) * 0.0037 - seed * 3.13);
      return low * 0.27 + cross * 0.19;
    }

    float crystalHash(vec3 p, float seed) {
      return fract(sin(dot(
        p + vec3(seed * 1.73, seed * 2.17, seed * 3.11),
        vec3(12.9898, 78.233, 37.719)
      )) * 43758.5453);
    }

    vec2 microSurfaceSlope(vec2 p, float seed, float scale) {
      vec2 q = p / u_dpr * max(scale, 0.08);
      float diagonalA = (q.x + q.y) * 0.041 + seed * 2.7;
      float diagonalB = (q.x - q.y) * 0.033 - seed * 1.9;
      return vec2(
        cos(q.x * 0.057 + seed) * 0.012
          + cos(diagonalA) * 0.009
          + cos(diagonalB) * 0.006,
        -sin(q.y * 0.052 - seed * 1.3) * 0.012
          + cos(diagonalA) * 0.009
          - cos(diagonalB) * 0.006
      );
    }

    float sampleShapeAtlas(vec2 pixel, vec4 rect, vec4 atlasRect) {
      vec2 local = clamp((pixel - rect.xy) / max(rect.zw, vec2(1.0)), 0.0, 1.0);
      vec2 halfTexel = 0.5 / vec2(textureSize(u_shapeAtlas, 0));
      vec2 uv = clamp(atlasRect.xy + local * atlasRect.zw,
        atlasRect.xy + halfTexel, atlasRect.xy + atlasRect.zw - halfTexel);
      return texture(u_shapeAtlas, uv).r;
    }

    float extendShapeDistance(float edgeDistance, float outsideDistance) {
      return outsideDistance > 0.0
        ? max(edgeDistance, 0.0) + outsideDistance
        : edgeDistance;
    }

    float lensDistance(
      vec2 screenP,
      vec4 rect,
      vec4 state,
      vec4 shape,
      vec4 shapeAtlasRect,
      vec4 contentOpts
    ) {
      float interaction = contentOpts.w;
      vec2 p = tiltLensPoint(screenP, rect, state, interaction);
      float distanceToShape;
      if (shape.x < 0.0) {
        vec2 nearest = clamp(p, rect.xy, rect.xy + rect.zw);
        float encodedDistance = sampleShapeAtlas(nearest, rect, shapeAtlasRect);
        float edgeDistance = (0.5 - encodedDistance)
          * 2.0 * shape.y * u_dpr;
        distanceToShape = extendShapeDistance(edgeDistance, length(p - nearest));
      } else {
        float radius = min(min(rect.z, rect.w) * shape.x, shape.y * u_dpr);
        distanceToShape = sdRoundRect(p, rect, radius);
      }
      float variation = materialNoise(p, contentOpts.z)
        * shape.w * u_dpr;
      return distanceToShape + variation;
    }

    float glassProfile(float distanceToShape, float bevel) {
      float t = clamp(-distanceToShape / max(bevel, 1.0), 0.0, 1.0);
      return sqrt(max(t * (2.0 - t), 0.0));
    }

    float surfaceHeight(
      vec2 screenP,
      vec4 rect,
      vec4 state,
      vec4 shape,
      vec4 optics,
      vec4 shapeAtlasRect,
      vec4 contentScreen,
      vec4 contentAtlasRect,
      vec4 contentOpts
    ) {
      vec2 lensP = tiltLensPoint(screenP, rect, state, contentOpts.w);
      float distanceToShape = lensDistance(
        screenP, rect, state, shape, shapeAtlasRect, contentOpts
      );
      float bevel = min(min(rect.z, rect.w) * shape.z, 176.0 * u_dpr);
      float shapeProfile = glassProfile(distanceToShape, bevel);
      if (shape.x < 0.0) {
        shapeProfile = sampleAtlas(
          u_shapeAtlas,
          lensP,
          rect,
          shapeAtlasRect
        ).g;
      }
      float frontHeight = shapeProfile * optics.w * u_dpr;
      float roundedField = sampleAtlas(
        u_fieldAtlas,
        lensP,
        contentScreen,
        contentAtlasRect
      ).g;
      float smoothGroove = roundedField * roundedField
        * (3.0 - 2.0 * roundedField);
      float surfaceEngraving = 1.0 - step(0.0001, u_volumeOpts.x);
      return frontHeight
        - smoothGroove * contentOpts.x * u_dpr * surfaceEngraving;
    }

    vec3 renderLens(
      vec3 scene,
      vec2 p,
      vec4 rect,
      vec4 state,
      vec4 shape,
      vec4 optics,
      vec4 style,
      vec4 surface,
      vec4 shapeAtlasRect,
      vec4 contentScreen,
      vec4 contentAtlasRect,
      vec4 contentOpts
    ) {
      float hover = state.x;
      float pressed = state.y;
      vec2 pointer = state.zw;
      float interaction = contentOpts.w;
      vec2 lensP = tiltLensPoint(p, rect, state, interaction);
      float distanceFront = lensDistance(
        p, rect, state, shape, shapeAtlasRect, contentOpts
      );
      float front = 1.0 - smoothstep(
        -2.0 * u_dpr,
        2.8 * u_dpr,
        distanceFront
      );

      float bevel = min(min(rect.z, rect.w) * shape.z, 176.0 * u_dpr);
      float inner = smoothstep(0.0, bevel, -distanceFront);
      float edgeZone = front * (1.0 - inner);

      vec2 shadowOffset = vec2(
        pointer.x * 8.5 * hover * interaction * u_dpr,
        (13.0 + pointer.y * 3.4 * hover * interaction
          + hover * 4.0 - pressed * 5.0) * u_dpr
      );
      float shadowDistance = lensDistance(
        p - shadowOffset,
        rect,
        state,
        shape,
        shapeAtlasRect,
        contentOpts
      );
      float shadowSigma = (22.0 + min(optics.w, 240.0) * 0.18) * u_dpr;
      float shadowRange = shadowSigma * 4.2;
      float outsideShadowDistance = max(shadowDistance, 0.0);
      float normalizedShadowDistance = outsideShadowDistance
        / max(shadowSigma, 1.0);
      float softShadow = exp(
        -0.5 * normalizedShadowDistance * normalizedShadowDistance
      );
      softShadow *= 1.0 - smoothstep(
        shadowSigma * 3.4,
        shadowRange,
        outsideShadowDistance
      );
      softShadow *= (1.0 - front) * (0.36 + hover * 0.07);
      float shadowDensity = 1.0 - exp(-max(optics.w, 0.0) / 42.0);
      scene *= 1.0 - softShadow * (0.13 + shadowDensity * 0.29);
      if (front < 0.0001) return scene;

      float epsilon = max(1.5, 1.55 * u_dpr);
      float heightLeft = surfaceHeight(
        p - vec2(epsilon, 0.0), rect, state, shape, optics,
        shapeAtlasRect, contentScreen, contentAtlasRect, contentOpts
      );
      float heightRight = surfaceHeight(
        p + vec2(epsilon, 0.0), rect, state, shape, optics,
        shapeAtlasRect, contentScreen, contentAtlasRect, contentOpts
      );
      float heightTop = surfaceHeight(
        p - vec2(0.0, epsilon), rect, state, shape, optics,
        shapeAtlasRect, contentScreen, contentAtlasRect, contentOpts
      );
      float heightBottom = surfaceHeight(
        p + vec2(0.0, epsilon), rect, state, shape, optics,
        shapeAtlasRect, contentScreen, contentAtlasRect, contentOpts
      );
      vec2 surfaceGradient = vec2(
        heightRight - heightLeft,
        heightBottom - heightTop
      ) / (2.0 * epsilon);

      float seed = contentOpts.z;
      float textureAmount = max(surface.x * u_textureStrength, 0.0);
      float localThickness = clamp(
        (heightLeft + heightRight + heightTop + heightBottom)
          / max(4.0 * optics.w * u_dpr, 1.0),
        0.0,
        1.0
      );
      vec3 normal = normalize(vec3(-surfaceGradient, 1.0));
      normal.xy += microSurfaceSlope(lensP, seed, surface.y)
        * textureAmount * 1.5 * (0.22 + inner * 0.78);
      normal.xy += pointer * hover * interaction * edgeZone * 0.058;
      normal = normalize(normal);

      vec2 rearDomain = p / u_dpr;
      vec2 rearSlope = vec2(
        cos(rearDomain.x * 0.008 + seed * 2.3) * 0.024
          + cos((rearDomain.x + rearDomain.y) * 0.004 - seed) * 0.014,
        -sin(rearDomain.y * 0.01 - seed * 1.7) * 0.023
          + sin((rearDomain.x - rearDomain.y) * 0.0035 + seed) * 0.013
      ) * (0.72 + shape.w * 0.18) * textureAmount * surface.w;
      vec2 rearWarp = rearSlope * optics.w * optics.y
        * 1.7 * u_dpr * front * (0.24 + localThickness * 0.76);

      vec3 viewRay = vec3(0.0, 0.0, -1.0);
      vec3 internalRay = refract(viewRay, normal, 1.0 / optics.x);
      float thicknessPixels = max(optics.w * u_dpr * localThickness, 0.0);
      float transmissionCosine = max(abs(internalRay.z), 0.18);
      float opticalPathPixels = thicknessPixels / transmissionCosine;
      float opticalPath = opticalPathPixels / max(52.0 * u_dpr, 1.0);
      vec2 refraction = internalRay.xy * opticalPathPixels * optics.y;
      vec3 rearNormal = normalize(vec3(-rearSlope * 0.72, 1.0));
      vec3 exitRay = refract(internalRay, rearNormal, optics.x);
      float exitTransmission = step(0.0001, dot(exitRay, exitRay));
      vec3 trappedRay = reflect(internalRay, rearNormal);
      vec2 exitDirection = mix(trappedRay.xy * 0.28, exitRay.xy, exitTransmission);
      float backdropGap = min(18.0, 3.0 + optics.w * 0.11) * u_dpr;
      refraction += exitDirection * backdropGap * optics.y
        * (0.28 + localThickness * 0.72);
      refraction += pointer * vec2(-5.1, -4.1)
        * hover * interaction * edgeZone * optics.y * u_dpr;
      float depthScale = clamp(optics.w / 52.0, 0.15, 5.0);
      float maximumOffset = min(rect.z, rect.w) * clamp(
        mix(0.1, 0.18, localThickness) * sqrt(depthScale),
        0.05,
        0.36
      );
      float refractionLength = length(refraction);
      refraction *= min(1.0, maximumOffset / max(refractionLength, 0.001));

      float dispersion = 0.018 + (optics.x - 1.0) * 0.025;
      vec3 direct = sampleScene(p + rearWarp);
      float red = sampleScene(
        p + rearWarp + refraction * (1.0 + dispersion)
      ).r;
      float green = sampleScene(p + rearWarp + refraction).g;
      float blue = sampleScene(
        p + rearWarp + refraction * (1.0 - dispersion * 0.82)
      ).b;
      vec3 refracted = vec3(red, green, blue);
      float curvedArea = smoothstep(
        0.012,
        0.64,
        length(surfaceGradient)
      );
      float textureRefraction = clamp(textureAmount * 0.09, 0.0, 0.26)
        * inner;
      refracted = mix(
        direct,
        refracted,
        max(max(curvedArea, edgeZone * 0.86), textureRefraction)
      );

      float surfaceRoughness = clamp(
        optics.z + textureAmount * surface.z * 0.012,
        0.0,
        0.68
      );
      float surfaceScatter = smoothstep(0.018, 0.62, surfaceRoughness)
        * mix(0.11, 0.36, sqrt(localThickness));
      float volumeScatter = 1.0 - exp(
        -style.w * (2.15 + textureAmount * surface.z * 0.08)
          * opticalPath
      );
      float scatterMix = clamp(
        surfaceScatter + volumeScatter * (1.0 - surfaceScatter),
        0.0,
        0.86
      );
      if (scatterMix > 0.001) {
        float scatterRadius = (
          0.8
          + surfaceRoughness * 19.0
          + sqrt(max(opticalPath, 0.0)) * style.w * 33.0
        ) * u_dpr;
        vec2 scatterAxis = normalize(vec2(0.86, 0.52) + rearSlope * 2.4);
        vec2 blurA = scatterAxis * scatterRadius;
        vec2 blurB = vec2(-scatterAxis.y, scatterAxis.x) * scatterRadius;
        vec2 scatterCenter = p + rearWarp + refraction;
        vec3 roughSample = sampleScene(scatterCenter) * 0.36;
        roughSample += sampleScene(scatterCenter + blurA) * 0.16;
        roughSample += sampleScene(scatterCenter - blurA) * 0.16;
        roughSample += sampleScene(scatterCenter + blurB) * 0.16;
        roughSample += sampleScene(scatterCenter - blurB) * 0.16;
        refracted = mix(refracted, roughSample, scatterMix);
      }

      vec3 incident = vec3(0.0, 0.0, -1.0);
      vec3 reflectedRay = reflect(incident, normal);
      vec3 reflectedEnvironment = sampleScene(
        p - reflectedRay.xy * (42.0 + edgeZone * 24.0) * u_dpr
      );
      reflectedEnvironment = mix(
        reflectedEnvironment,
        vec3(0.96, 0.985, 1.0),
        0.12
      );

      float f0 = pow((optics.x - 1.0) / (optics.x + 1.0), 2.0);
      float fresnel = f0 + (1.0 - f0)
        * pow(clamp(1.0 - normal.z, 0.0, 1.0), 5.0);
      vec3 glass = mix(
        refracted,
        reflectedEnvironment,
        clamp(fresnel, 0.0, 0.62)
      );

      vec4 contentField = sampleAtlas(
        u_fieldAtlas,
        lensP,
        contentScreen,
        contentAtlasRect
      );
      vec4 contentColor = sampleAtlas(
        u_colorAtlas,
        lensP,
        contentScreen,
        contentAtlasRect
      );
      float pigmentMask = smoothstep(0.2, 0.82, contentField.r)
        * contentColor.a;
      float pigmentDensity = max(contentOpts.y, 0.0);
      float grooveVolume = smoothstep(0.08, 0.96, contentField.g);
      float volumeEnabled = step(0.0001, u_volumeOpts.x);
      glass *= 1.0 - grooveVolume * front * 0.018
        * (1.0 - volumeEnabled);

      float tintStrength = clamp(u_styleExtra.x, 0.0, 4.0);
      vec3 absorption = vec3(0.016, 0.013, 0.011)
        + (vec3(1.0) - clamp(style.rgb, 0.0, 1.0))
          * 0.095 * tintStrength;
      vec3 transmission = exp(-absorption * opticalPath);
      glass *= transmission;

      float densityVariation = 1.0 + materialNoise(
        lensP * (0.54 + surface.y * 0.08),
        seed + 4.7
      ) * textureAmount * 0.045;
      float mattePath = opticalPath * max(densityVariation, 0.72);
      float matteScatter = 1.0 - exp(-style.w * 2.15 * mattePath);
      vec3 scatteringColor = mix(
        vec3(0.78, 0.865, 0.92),
        style.rgb,
        clamp(0.08 + tintStrength * 0.12, 0.08, 0.56)
      );
      glass = mix(
        glass,
        scatteringColor,
        matteScatter * (0.11 + style.w * 0.13)
      );
      float frostedLuma = dot(glass, vec3(0.2126, 0.7152, 0.0722));
      glass = mix(
        glass,
        vec3(frostedLuma),
        matteScatter * (0.055 + style.w * 0.075)
      );
      float frostGrain = fract(sin(dot(
        floor(p / max(u_dpr * 1.35, 1.0)),
        vec2(12.9898, 78.233)
      ) + seed * 17.17) * 43758.5453) - 0.5;
      glass += frostGrain * sqrt(localThickness) * (
        sqrt(matteScatter) * style.w * 0.012
          + surfaceRoughness * textureAmount * surface.z * 0.0045
      );

      if (volumeEnabled > 0.5) {
        float volumeThickness = clamp(u_volumeOpts.x, 0.025, 0.96);
        float volumePosition = clamp(u_volumeOpts.y, -0.92, 0.92);
        float volumeScatter = clamp(u_volumeOpts.z, 0.0, 4.0);
        float volumeFacets = clamp(u_volumeOpts.w, 0.0, 2.5);
        float volumeParallax = clamp(u_volumeStyle.x, 0.0, 3.0);
        float volumeGrain = clamp(u_volumeStyle.y, 0.0, 1.0);
        float volumeGlow = clamp(u_volumeStyle.z, 0.0, 3.0);
        float volumeDispersion = clamp(u_volumeStyle.w, 0.0, 1.5);
        float centerZ = volumePosition * 0.42;
        vec2 volumeView = (
          vec2(0.075, -0.055)
          + pointer * hover * interaction * 0.14
          + internalRay.xy * 0.38
          + rearSlope * 0.3
        ) * volumeParallax;
        float volumeDepthPixels = optics.w * u_dpr * volumeThickness;
        float accumulatedVolume = 0.0;
        float depthMoment = 0.0;
        float crystalSparkle = 0.0;
        float frontMask = 0.0;
        float backMask = 0.0;
        const int volumeSteps = 7;
        for (int index = 0; index < volumeSteps; index++) {
          float slice = float(index) / float(volumeSteps - 1);
          float sliceZ = centerZ + (slice - 0.5) * volumeThickness;
          vec2 volumePoint = lensP
            + volumeView * sliceZ * optics.w * u_dpr;
          vec4 sliceField = sampleAtlas(
            u_fieldAtlas,
            volumePoint,
            contentScreen,
            contentAtlasRect
          );
          float localPosition = sliceField.a * 2.0 - 1.0;
          float sliceCoordinate = abs(
            (slice - 0.5) * 2.0 - localPosition * 0.78
          );
          float localDepth = clamp(sliceField.b, 0.0, 1.0);
          float localDepthGate = 1.0 - smoothstep(
            localDepth + 0.035,
            localDepth + 0.16,
            sliceCoordinate
          );
          float sliceMask = sliceField.r * localDepthGate;
          float crystal = crystalHash(
            vec3(
              floor(volumePoint / max(u_dpr * 1.45, 1.0)),
              float(index) * 9.17
            ),
            seed
          );
          float crystalDensity = mix(
            1.0,
            0.54 + crystal * 1.18,
            volumeGrain
          );
          float sliceDensity = sliceMask * crystalDensity
            * (0.105 + volumeScatter * 0.052);
          float visibleSlice = (1.0 - accumulatedVolume)
            * clamp(sliceDensity, 0.0, 0.72);
          accumulatedVolume += visibleSlice;
          depthMoment += visibleSlice * slice;
          crystalSparkle += visibleSlice
            * pow(crystal, mix(24.0, 9.0, volumeGrain));
          if (index == 0) frontMask = sliceMask;
          if (index == volumeSteps - 1) backMask = sliceMask;
        }

        vec2 centerShift = volumeView * centerZ * optics.w * u_dpr;
        vec2 volumeCenter = lensP + centerShift;
        vec4 volumeContentColor = sampleAtlas(
          u_colorAtlas,
          volumeCenter,
          contentScreen,
          contentAtlasRect
        );
        float volumeEpsilon = max(1.2, 1.25 * u_dpr);
        float fieldLeft = sampleAtlas(
          u_fieldAtlas,
          volumeCenter - vec2(volumeEpsilon, 0.0),
          contentScreen,
          contentAtlasRect
        ).r;
        float fieldRight = sampleAtlas(
          u_fieldAtlas,
          volumeCenter + vec2(volumeEpsilon, 0.0),
          contentScreen,
          contentAtlasRect
        ).r;
        float fieldTop = sampleAtlas(
          u_fieldAtlas,
          volumeCenter - vec2(0.0, volumeEpsilon),
          contentScreen,
          contentAtlasRect
        ).r;
        float fieldBottom = sampleAtlas(
          u_fieldAtlas,
          volumeCenter + vec2(0.0, volumeEpsilon),
          contentScreen,
          contentAtlasRect
        ).r;
        vec2 volumeGradient = vec2(
          fieldRight - fieldLeft,
          fieldBottom - fieldTop
        ) / (2.0 * volumeEpsilon);
        float volumeEdge = smoothstep(
          0.008,
          0.16,
          length(volumeGradient) * u_dpr
        );
        vec3 volumeNormal = normalize(vec3(
          -volumeGradient * (3.4 + volumeFacets * 6.8),
          1.0
        ));
        vec3 volumeLight = normalize(vec3(
          -0.46 + pointer.x * hover * interaction * 0.24,
          -0.7 + pointer.y * hover * interaction * 0.2,
          0.72
        ));
        vec3 volumeHalf = normalize(volumeLight + vec3(0.0, 0.0, 1.0));
        float facetSpecular = pow(
          max(dot(volumeNormal, volumeHalf), 0.0),
          mix(72.0, 22.0, clamp(volumeFacets * 0.5, 0.0, 1.0))
        ) * volumeEdge;
        float extrusionSide = abs(backMask - frontMask)
          * clamp(volumeFacets, 0.0, 1.8);
        float resolvedDepth = depthMoment / max(accumulatedVolume, 0.001);
        float depthPath = clamp(
          0.5 + centerZ + (resolvedDepth - 0.5) * volumeThickness,
          0.0,
          1.0
        );
        vec3 internalColor = mix(
          srgbToLinear(volumeContentColor.rgb),
          vec3(0.72, 0.91, 1.0),
          0.72
        );
        internalColor *= mix(vec3(1.0), transmission, depthPath * 0.84);
        internalColor *= vec3(
          1.0 + volumeDispersion * 0.055,
          1.0,
          1.0 + volumeDispersion * 0.09
        );
        float volumeEnergy = clamp(
          accumulatedVolume * pigmentDensity * front,
          0.0,
          1.35
        );
        float depthLighting = mix(0.72, 1.14, resolvedDepth);
        glass *= 1.0 - volumeEnergy
          * (0.022 + extrusionSide * 0.055);
        glass += internalColor * volumeEnergy
          * (0.09 + volumeScatter * 0.13)
          * depthLighting
          * (1.0 - matteScatter * 0.34);
        glass += vec3(0.9, 0.975, 1.0)
          * (facetSpecular * (0.18 + volumeFacets * 0.2)
            + crystalSparkle * volumeGlow * 0.58)
          * pigmentDensity
          * front;
      }

      // Pigment lies inside the engraved floor: volume frost changes the light
      // above it, but must not repeatedly desaturate the deposited color itself.
      float engravingDepth = clamp(contentOpts.x / 18.0, 0.0, 1.6);
      float pigmentCoverage = 1.0 - exp(
        -pigmentMask * pigmentDensity * (4.4 + engravingDepth * 0.9)
      );
      vec3 pigment = srgbToLinear(contentColor.rgb);
      pigment *= mix(vec3(1.0), transmission, 0.1 + engravingDepth * 0.07);
      glass = mix(
        glass,
        pigment,
        pigmentCoverage * front * (1.0 - volumeEnabled)
      );

      vec3 lightDirection = normalize(vec3(
        -0.5 + pointer.x * hover * interaction * 0.42,
        -0.72 + pointer.y * hover * interaction * 0.32,
        1.0
      ));
      vec3 halfDirection = normalize(lightDirection + vec3(0.0, 0.0, 1.0));
      float specPower = mix(132.0, 44.0, surfaceRoughness);
      float tightSpecular = pow(
        max(dot(normal, halfDirection), 0.0),
        specPower
      );
      float broadSpecular = pow(
        max(dot(normal, halfDirection), 0.0),
        mix(34.0, 16.0, surfaceRoughness)
      ) * (edgeZone + curvedArea * 0.24);

      vec2 pointerPosition = rect.xy + rect.zw
        * (pointer * 0.49 + vec2(0.5, 0.22));
      float cursorGlint = exp(
        -length((p - pointerPosition) / max(rect.z, rect.w)) * 18.0
      ) * hover;
      glass += vec3(1.0, 0.995, 0.98)
        * tightSpecular * (0.54 + hover * 0.5)
        * (1.0 - surfaceRoughness * 0.62);
      glass += vec3(0.84, 0.94, 1.0)
        * broadSpecular * 0.08 * (1.0 - surfaceRoughness * 0.46);
      glass += vec3(0.97, 0.99, 1.0) * cursorGlint * 0.075;

      float silhouette = 1.0 - smoothstep(
        0.0,
        4.2 * u_dpr,
        abs(distanceFront)
      );
      float causticBand = smoothstep(
        5.0 * u_dpr,
        16.0 * u_dpr,
        -distanceFront
      ) * (1.0 - smoothstep(
        22.0 * u_dpr,
        54.0 * u_dpr,
        -distanceFront
      ));
      float lightFacing = clamp(dot(normal, lightDirection), 0.0, 1.0);
      vec2 lightPlane = normalize(lightDirection.xy);
      float rimOrientation = dot(normal.xy, lightPlane);
      float illuminatedRim = silhouette
        * smoothstep(-0.18, 0.9, rimOrientation);
      float shadedRim = silhouette
        * smoothstep(-0.12, 0.92, -rimOrientation);
      float transmittedRim = causticBand
        * smoothstep(-0.28, 0.88, -rimOrientation);
      float transmittedEnergy = dot(
        transmission,
        vec3(0.2126, 0.7152, 0.0722)
      );
      float causticFocus = clamp(
        length(surfaceGradient) * opticalPath * 0.22,
        0.0,
        1.0
      );
      glass *= 1.0 - shadedRim
        * (0.025 + fresnel * 0.16 + (1.0 - exitTransmission) * 0.08);
      glass += vec3(0.98, 1.0, 1.0)
        * illuminatedRim
        * (0.025 + lightFacing * 0.095 + fresnel * 0.18);
      glass += vec3(0.82, 0.94, 1.0)
        * transmittedRim
        * (0.018 + causticFocus * 0.075)
        * transmittedEnergy
        * (1.0 - matteScatter * 0.55)
        * (0.88 + 0.12 * sin(u_time * 0.42 + seed));

      return mix(scene, glass, front);
    }

    void main() {
      vec2 p = vec2(gl_FragCoord.x, u_resolution.y - gl_FragCoord.y);
      vec2 centered = p / u_resolution - 0.5;
      vec3 scene;
      if (u_activeElement == -1) {
        scene = srgbToLinear(sampleBackground(p));
        float luminance = dot(scene, vec3(0.2126, 0.7152, 0.0722));
        scene = mix(vec3(luminance), scene, 0.95);
        scene *= 1.01 - dot(centered, centered) * 0.12;
      } else {
        scene = sampleScene(p);
        if (u_activeElement >= 0) {
          scene = renderLens(
            scene,
            p,
            u_rect,
            u_state,
            u_shape,
            u_optics,
            u_style,
            u_surface,
            u_shapeAtlasRect,
            u_contentScreen,
            u_contentAtlasRect,
            u_contentOpts
          );
        }
      }

      if (u_activeElement == -2) {
        scene = linearToSrgb(scene);
      }
      outColor = vec4(clamp(scene, 0.0, 1.0), 1.0);
    }
  `;

  class OpticalGlassScene {
    constructor({
      canvas,
      background,
      dprCap = 1.6,
      fpsCap = 0,
      root = document,
      crossOrigin,
      fallbackBackground,
      presets = {},
    } = {}) {
      this.root = root;
      this.canvas = typeof canvas === "string" ? root.querySelector(canvas) : canvas;
      if (!this.canvas) throw new Error("OpticalGlass: canvas not found");

      this.backgroundSource = background;
      this.fallbackBackground = fallbackBackground;
      this.crossOrigin = crossOrigin;
      this.backgroundLoadId = 0;
      this.dprCap = dprCap;
      this.fpsCap = clamp(Number(fpsCap) || 0, 0, 120);
      this.textureStrength = 1;
      this.elements = [];
      this.presets = { ...BUILT_IN_PRESETS, ...presets };
      this.mountControllers = new Set();
      this.menuControllers = new Set();
      this.deviceTiltControllers = new Set();
      this.deviceTilt = {
        x: 0,
        y: 0,
        amount: 0,
        targetX: 0,
        targetY: 0,
        targetAmount: 0,
      };
      this.running = false;
      this.initialized = false;
      this.destroyed = false;
      this.startPromise = null;
      this.programDirty = true;
      this.atlasDirty = true;
      this.lastDpr = 1;
      this.raf = 0;
      this.lastRenderTimestamp = 0;
      this.atlasRefreshTimer = 0;
      this.resizeRefreshRaf = 0;
      this.pendingResizeTargets = new Set();
      this.sceneTextures = [];
      this.sceneFramebuffers = [];
      this.linearBackgroundTexture = null;
      this.linearBackgroundFramebuffer = null;
      this.backgroundDirty = true;
      this.renderTargetSize = [0, 0];
      this.reducedMotionQuery = window.matchMedia("(prefers-reduced-motion: reduce)");
      this.onReducedMotionChange = (event) => {
        if (!event.matches) return;
        this.elements.forEach((element) => {
          element.target.style.setProperty("--label-x", "0px");
          element.target.style.setProperty("--label-y", "0px");
          element.runtime.motionFrame = "";
        });
      };
      if (typeof this.reducedMotionQuery.addEventListener === "function") {
        this.reducedMotionQuery.addEventListener("change", this.onReducedMotionChange);
      } else {
        this.reducedMotionQuery.addListener?.(this.onReducedMotionChange);
      }
      this.metrics = {
        fps: 0,
        frameTime: 0,
        opticalPasses: 0,
        shadedPixels: 0,
        precision: "rgba8",
        lastTimestamp: 0,
        sampleTime: 0,
        sampleFrames: 0,
      };

      this.gl = this.canvas.getContext("webgl2", {
        alpha: false,
        antialias: true,
        depth: false,
        premultipliedAlpha: false,
        powerPreference: "high-performance",
      });
    }

    definePreset(name, specification) {
      if (!name || !specification) throw new Error("OpticalGlass: invalid preset");
      const resolved = this._resolveSpecification(specification);
      delete resolved.preset;
      delete resolved.target;
      this.presets[name] = resolved;
      return this;
    }

    _resolveSpecification(specification = {}) {
      const presetNames = Array.isArray(specification.preset)
        ? specification.preset
        : String(specification.preset || "").split(/[\s,]+/).filter(Boolean);
      const fromPresets = presetNames.reduce((resolved, name) => {
        const preset = this.presets[name] ?? BUILT_IN_PRESETS[name];
        if (!preset) {
          const available = Object.keys(this.presets).sort().join(", ") || "none";
          throw new Error(`OpticalGlass: unknown preset "${name}" (available: ${available})`);
        }
        return mergeGlassSpecification(resolved, preset);
      }, {});
      return mergeGlassSpecification(fromPresets, specification);
    }

    _normalizeContent(content) {
      if (!content) return content;
      if (content.volume) {
        if (content.type === "live") {
          throw new Error("OpticalGlass: live DOM cannot be used as a volume engraving");
        }
        const volume = content.volume === true ? {} : content.volume;
        if (!volume || typeof volume !== "object" || Array.isArray(volume)) {
          throw new Error("OpticalGlass: volume engraving options must be an object");
        }
        return {
          ...content,
          volume: { ...VOLUME_ENGRAVING_DEFAULTS, ...volume },
        };
      }
      if (content.type !== "live"
        || Object.prototype.hasOwnProperty.call(content, "safeArea")) return content;
      return { ...content, safeArea: "auto" };
    }

    add(specification) {
      if (this.destroyed) throw new Error("OpticalGlass: scene is destroyed");
      specification = this._resolveSpecification(specification);
      const target = typeof specification.target === "string"
        ? this.root.querySelector(specification.target)
        : specification.target;
      if (!target) throw new Error("OpticalGlass: element target not found");
      const content = this._normalizeContent(specification.content) || null;

      const element = {
        target,
        shape: {
          radius: 0.34,
          radiusMax: 242,
          bevel: 0.205,
          irregularity: 1.25,
          rounding: 5.5,
          ...(specification.shape || {}),
        },
        optics: {
          ior: 1.47,
          refraction: 1,
          roughness: 0.015,
          depth: 48,
          ...(specification.optics || {}),
        },
        style: {
          tint: [0.82, 0.94, 1],
          tintStrength: 0.35,
          matte: 0,
          ...(specification.style || {}),
        },
        surface: {
          texture: 1,
          scale: 1,
          grain: 1,
          rear: 1,
          ...(specification.surface || {}),
        },
        motion: {
          enabled: content?.type === "live",
          mode: "group",
          coupling: 0.78,
          parallax: 1.8,
          tilt: 1.25,
          lift: 2.8,
          scale: 0.0015,
          press: 0.006,
          inertia: 0.11,
          layerSeparation: 0.8,
          contentDepth: 0.72,
          ...(specification.motion || {}),
        },
        layers: Array.isArray(specification.layers) && specification.layers.length
          ? specification.layers.map((layer) => this._cloneLayer(layer))
          : [{}],
        content,
        layer: specification.layer ?? 0,
        seed: specification.seed ?? this.elements.length * 2.41 + 1.37,
        interaction: specification.interaction ?? 0.48,
        state: {
          hover: 0,
          hoverTarget: 0,
          pressed: 0,
          pressedTarget: 0,
          pointerX: 0,
          pointerY: -0.65,
          pointerTargetX: 0,
          pointerTargetY: -0.65,
        },
        runtime: {
          contentScreen: [0, 0, 1, 1],
          contentLocal: [0, 0, 1, 1],
          contentAtlas: [0, 0, 1, 1],
          shapeAtlas: [0, 0, 1, 1],
          shapeRange: 1,
          grooveDepth: 0,
          fillOpacity: 0,
          image: null,
          motionStyle: null,
          motionFrame: "",
          motionParent: null,
          motionDepth: 0,
          composedState: null,
          safeAreaStyle: null,
        },
        listeners: [],
      };

      element.runtime.hadMountedClass = target.classList.contains("liquid-glass-mounted");
      target.classList.add("liquid-glass-mounted");
      this._syncLiveMotion(element);
      this._syncSafeArea(element);

      this.elements.push(element);
      this.elements.sort((first, second) => first.layer - second.layer);
      this._refreshMotionHierarchy();
      this._bindInteraction(element);
      this.resizeObserver?.observe(target);
      this.atlasDirty = true;
      if (this.initialized && element.content?.type === "image") {
        loadImage(element.content.source, {
          crossOrigin: element.content.crossOrigin ?? this.crossOrigin,
        }).then((image) => {
          if (this.destroyed) return;
          element.runtime.image = image;
          this.atlasDirty = true;
        });
      }
      return element;
    }

    addAll(selector, specification = {}, root = this.root) {
      const targets = typeof selector === "string"
        ? [...root.querySelectorAll(selector)]
        : [...selector];
      return targets.map((target, index) => this.add({
        ...(typeof specification === "function"
          ? specification({ target, index })
          : specification),
        target,
      }));
    }

    async start() {
      if (this.destroyed) throw new Error("OpticalGlass: scene is destroyed");
      if (this.running) return this;
      if (this.startPromise) return this.startPromise;
      if (!this.gl) {
        document.documentElement.classList.add("webgl-unavailable");
        return this;
      }

      if (this.initialized) {
        this.running = true;
        this.metrics.lastTimestamp = 0;
        this.raf = requestAnimationFrame((time) => this._render(time));
        return this;
      }

      this.startPromise = this._initializeScene();
      try {
        return await this.startPromise;
      } finally {
        this.startPromise = null;
      }
    }

    async _initializeScene() {
      let backgroundFallback = false;
      const resolveBackground = async () => {
        try {
          if (!this.backgroundSource) throw new Error("background source is empty");
          return await loadImage(this.backgroundSource, { crossOrigin: this.crossOrigin });
        } catch (error) {
          backgroundFallback = true;
          if (this.fallbackBackground) {
            try {
              return await loadImage(this.fallbackBackground, { crossOrigin: this.crossOrigin });
            } catch {
              // Fall through to the built-in optical gradient.
            }
          }
          return createFallbackBackground();
        }
      };
      const [background] = await Promise.all([
        resolveBackground(),
        document.fonts?.ready || Promise.resolve(),
        ...this.elements
          .filter((element) => element.content?.type === "image")
          .map(async (element) => {
            element.runtime.image = await loadImage(element.content.source, {
              crossOrigin: element.content.crossOrigin ?? this.crossOrigin,
            });
          }),
      ]);

      this.backgroundImage = background;
      this.canvas.dataset.opticalGlassBackground = backgroundFallback ? "fallback" : "ready";
      this._initializeGl();
      this._uploadBackground();
      this.initialized = true;
      this.running = true;
      this.resizeObserver = new ResizeObserver((entries) => {
        this._queueResizeRefresh(entries);
      });
      this.resizeObserver.observe(this.canvas);
      this.elements.forEach((element) => this.resizeObserver.observe(element.target));
      this.raf = requestAnimationFrame((time) => this._render(time));
      return this;
    }

    stop() {
      if (!this.running) return this;
      this.running = false;
      cancelAnimationFrame(this.raf);
      return this;
    }

    refresh() {
      window.clearTimeout(this.atlasRefreshTimer);
      this.atlasDirty = true;
    }

    async setBackground(source, { crossOrigin = this.crossOrigin } = {}) {
      if (this.destroyed) throw new Error("OpticalGlass: scene is destroyed");
      const loadId = ++this.backgroundLoadId;
      const image = await loadImage(source, { crossOrigin });
      if (loadId !== this.backgroundLoadId || this.destroyed) return this;

      this.backgroundSource = source;
      this.backgroundImage = image;
      this.canvas.dataset.opticalGlassBackground = "ready";
      if (this.initialized) this._uploadBackground();
      return this;
    }

    _scheduleAtlasRefresh(delay = 96) {
      window.clearTimeout(this.atlasRefreshTimer);
      this.atlasRefreshTimer = window.setTimeout(() => {
        this.atlasDirty = true;
      }, delay);
    }

    _queueResizeRefresh(entries = []) {
      entries.forEach((entry) => {
        const element = this._resolveElement(entry.target);
        if (element) this.pendingResizeTargets.add(element);
      });
      if (this.resizeRefreshRaf || this.destroyed) return;
      this.resizeRefreshRaf = requestAnimationFrame(() => {
        this.resizeRefreshRaf = 0;
        if (this.destroyed) return;
        const resizedElements = [...this.pendingResizeTargets];
        this.pendingResizeTargets.clear();
        resizedElements.forEach((element) => {
          if (this.elements.includes(element)) this._syncSafeArea(element);
        });
        this.atlasDirty = true;
      });
    }

    getMetrics() {
      return { ...this.metrics };
    }

    setTextureStrength(value) {
      this.textureStrength = clamp(Number(value) || 0, 0, 3);
      return this;
    }

    setDeviceTilt(x = 0, y = 0, amount = 1) {
      this.deviceTilt.targetX = clamp(Number(x) || 0, -1, 1);
      this.deviceTilt.targetY = clamp(Number(y) || 0, -1, 1);
      this.deviceTilt.targetAmount = clamp(Number(amount) || 0, 0, 1);
      return this;
    }

    clearDeviceTilt() {
      this.deviceTilt.targetX = 0;
      this.deviceTilt.targetY = 0;
      this.deviceTilt.targetAmount = 0;
      return this;
    }

    enableDeviceTilt({
      requestOnFirstInteraction = true,
      autoStart = true,
      maxAngle = 24,
      deadZone = 0.035,
      smoothing = 0.2,
      strength = 1,
      calibrationSamples = 8,
      motionFallback = true,
    } = {}) {
      const OrientationEvent = window.DeviceOrientationEvent;
      const MotionEvent = window.DeviceMotionEvent;
      let attached = false;
      let permission = "prompt";
      let source = "idle";
      let orientationSeenAt = 0;
      let filteredX = 0;
      let filteredY = 0;
      let orientationCalibration = null;
      let motionCalibration = null;
      const requiredSamples = clamp(Math.round(Number(calibrationSamples) || 8), 1, 30);
      const angleRange = clamp(Number(maxAngle) || 24, 4, 60);
      const sensorSmoothing = clamp(Number(smoothing) || 0.2, 0.03, 1);
      const sensorStrength = clamp(Number(strength) || 1, 0, 1.5);
      const normalizedDeadZone = clamp(Number(deadZone) || 0, 0, 0.3);
      const root = this.canvas.parentElement || document.body;
      let controller;

      const rotateToScreen = (x, y) => {
        const angle = Number(screen.orientation?.angle ?? window.orientation) || 0;
        if (Math.abs(angle) === 90) {
          return angle > 0 ? [y, -x] : [-y, x];
        }
        if (Math.abs(angle) === 180) return [-x, -y];
        return [x, y];
      };
      const angularDelta = (value, baseline) => {
        let delta = value - baseline;
        while (delta > 180) delta -= 360;
        while (delta < -180) delta += 360;
        return delta;
      };
      const collectBaseline = (calibration, x, y) => {
        const next = calibration || { x: 0, y: 0, samples: 0, ready: false };
        if (!next.ready) {
          next.samples += 1;
          next.x += (x - next.x) / next.samples;
          next.y += (y - next.y) / next.samples;
          next.ready = next.samples >= requiredSamples;
        }
        return next;
      };
      const applySensor = (x, y, nextSource) => {
        filteredX += (clamp(x, -1, 1) - filteredX) * sensorSmoothing;
        filteredY += (clamp(y, -1, 1) - filteredY) * sensorSmoothing;
        const magnitude = Math.hypot(filteredX, filteredY);
        let outputX = 0;
        let outputY = 0;
        if (magnitude > normalizedDeadZone) {
          const restoredMagnitude = clamp(
            (magnitude - normalizedDeadZone) / Math.max(1 - normalizedDeadZone, 0.001),
            0,
            1,
          );
          const scale = restoredMagnitude / Math.max(magnitude, 0.0001);
          outputX = filteredX * scale * sensorStrength;
          outputY = filteredY * scale * sensorStrength;
        }
        source = nextSource;
        this.setDeviceTilt(outputX, outputY, 1);
      };
      const onOrientation = (event) => {
        if (!Number.isFinite(event.beta) || !Number.isFinite(event.gamma)) return;
        orientationSeenAt = performance.now();
        const [screenX, screenY] = rotateToScreen(event.gamma, event.beta);
        orientationCalibration = collectBaseline(
          orientationCalibration,
          screenX,
          screenY,
        );
        if (!orientationCalibration.ready) {
          source = "calibrating";
          this.clearDeviceTilt();
          return;
        }
        applySensor(
          angularDelta(screenX, orientationCalibration.x) / angleRange,
          angularDelta(screenY, orientationCalibration.y) / angleRange,
          "orientation",
        );
      };
      const onMotion = (event) => {
        if (!motionFallback || performance.now() - orientationSeenAt < 900) return;
        const gravity = event.accelerationIncludingGravity;
        if (!gravity || !Number.isFinite(gravity.x) || !Number.isFinite(gravity.y)) return;
        const [screenX, screenY] = rotateToScreen(gravity.x, -gravity.y);
        motionCalibration = collectBaseline(motionCalibration, screenX, screenY);
        if (!motionCalibration.ready) {
          source = "calibrating";
          this.clearDeviceTilt();
          return;
        }
        const gravityRange = Math.max(Math.sin(angleRange * Math.PI / 180) * 9.80665, 0.8);
        applySensor(
          (screenX - motionCalibration.x) / gravityRange,
          (screenY - motionCalibration.y) / gravityRange,
          "motion",
        );
      };
      const calibrate = () => {
        orientationCalibration = null;
        motionCalibration = null;
        filteredX = 0;
        filteredY = 0;
        source = attached ? "calibrating" : "idle";
        this.clearDeviceTilt();
        return controller;
      };
      const onScreenOrientationChange = () => calibrate();
      const onVisibilityChange = () => {
        if (document.hidden) this.clearDeviceTilt();
        else calibrate();
      };
      const attach = () => {
        if (attached || (!OrientationEvent && !MotionEvent)) return false;
        if (OrientationEvent) {
          window.addEventListener("deviceorientation", onOrientation, { passive: true });
        }
        if (MotionEvent && motionFallback) {
          window.addEventListener("devicemotion", onMotion, { passive: true });
        }
        screen.orientation?.addEventListener?.("change", onScreenOrientationChange);
        window.addEventListener("orientationchange", onScreenOrientationChange, { passive: true });
        document.addEventListener("visibilitychange", onVisibilityChange);
        attached = true;
        source = "calibrating";
        return true;
      };
      const request = async () => {
        if (!OrientationEvent && !MotionEvent) return "unsupported";
        try {
          for (const EventConstructor of [OrientationEvent, MotionEvent]) {
            if (typeof EventConstructor?.requestPermission !== "function") continue;
            const result = await EventConstructor.requestPermission();
            if (result !== "granted") {
              permission = result;
              return result;
            }
          }
          permission = "granted";
          attach();
          return "granted";
        } catch (error) {
          permission = error?.name || "denied";
          return permission;
        }
      };
      const onFirstInteraction = () => void request();
      const permissionRequired = [OrientationEvent, MotionEvent].some(
        (EventConstructor) => typeof EventConstructor?.requestPermission === "function",
      );
      if (OrientationEvent || MotionEvent) {
        if (permissionRequired && requestOnFirstInteraction) {
          root.addEventListener("pointerdown", onFirstInteraction, {
            once: true,
            capture: true,
          });
        } else if (!permissionRequired && autoStart) {
          permission = "granted";
          attach();
        }
      }

      controller = {
        get active() { return attached; },
        get supported() { return Boolean(OrientationEvent || MotionEvent); },
        get permission() { return permission; },
        get source() { return source; },
        request,
        calibrate,
        disable: () => {
          root.removeEventListener("pointerdown", onFirstInteraction, true);
          if (attached) {
            window.removeEventListener("deviceorientation", onOrientation);
            window.removeEventListener("devicemotion", onMotion);
            screen.orientation?.removeEventListener?.("change", onScreenOrientationChange);
            window.removeEventListener("orientationchange", onScreenOrientationChange);
            document.removeEventListener("visibilitychange", onVisibilityChange);
          }
          attached = false;
          source = "idle";
          this.clearDeviceTilt();
          this.deviceTiltControllers.delete(controller);
        },
      };
      this.deviceTiltControllers.add(controller);
      return controller;
    }

    updateContent(reference, patch = {}) {
      const element = this._resolveElement(reference);
      if (!element?.content) throw new Error("OpticalGlass: element content not found");
      const previousSource = element.content.source;
      const nextContent = {
        ...element.content,
        ...patch,
        ...(patch.volume ? {
          volume: { ...(element.content.volume || {}), ...patch.volume },
        } : {}),
      };
      element.content = this._normalizeContent(nextContent);
      this._syncLiveMotion(element);
      this._syncSafeArea(element);
      this._scheduleAtlasRefresh(0);

      if (
        this.initialized
        && element.content.type === "image"
        && element.content.source !== previousSource
      ) {
        element.runtime.image = null;
        loadImage(element.content.source, {
          crossOrigin: element.content.crossOrigin ?? this.crossOrigin,
        }).then((image) => {
          if (this.destroyed) return;
          element.runtime.image = image;
          this.atlasDirty = true;
        });
      }
      return element.content;
    }

    update(reference, patch = {}) {
      const element = this._resolveElement(reference);
      if (!element) throw new Error("OpticalGlass: element not found");
      const resolved = this._resolveSpecification(patch);
      const previousSource = element.content?.source;
      const previousLayer = element.layer;

      if (resolved.shape) element.shape = { ...element.shape, ...resolved.shape };
      if (resolved.optics) element.optics = { ...element.optics, ...resolved.optics };
      if (resolved.style) element.style = { ...element.style, ...resolved.style };
      if (resolved.surface) element.surface = { ...element.surface, ...resolved.surface };
      if (resolved.motion) element.motion = { ...element.motion, ...resolved.motion };
      if (Object.prototype.hasOwnProperty.call(resolved, "content")) {
        element.content = this._normalizeContent(resolved.content);
        this._scheduleAtlasRefresh(0);
      }
      if (Array.isArray(resolved.layers) && resolved.layers.length) {
        element.layers = resolved.layers.map((layer) => this._cloneLayer(layer));
      }
      this._syncLiveMotion(element);
      this._syncSafeArea(element);
      for (const key of ["layer", "seed", "interaction"]) {
        if (resolved[key] !== undefined) element[key] = resolved[key];
      }

      if (element.layer !== previousLayer) {
        this.elements.sort((first, second) => first.layer - second.layer);
      }
      if (resolved.shape && element.shape.mask) this.atlasDirty = true;
      if (
        this.initialized
        && element.content?.type === "image"
        && element.content.source !== previousSource
      ) {
        element.runtime.image = null;
        loadImage(element.content.source, {
          crossOrigin: element.content.crossOrigin ?? this.crossOrigin,
        }).then((image) => {
          if (this.destroyed) return;
          element.runtime.image = image;
          this.atlasDirty = true;
        });
      }
      return element;
    }

    remove(reference) {
      const element = this._resolveElement(reference);
      if (!element) return false;
      element.listeners.forEach(([type, listener]) => {
        element.target.removeEventListener(type, listener);
      });
      element.listeners = [];
      this._releaseLiveMotion(element);
      this._releaseSafeArea(element);
      this.resizeObserver?.unobserve(element.target);
      this.pendingResizeTargets.delete(element);
      if (!element.runtime.hadMountedClass) {
        element.target.classList.remove("liquid-glass-mounted");
      }
      this.elements.splice(this.elements.indexOf(element), 1);
      this._refreshMotionHierarchy();
      this.atlasDirty = true;
      return true;
    }

    _declarativeSpecification(target) {
      const data = target.dataset;
      let options = {};
      if (data.glassOptions) {
        try {
          options = JSON.parse(data.glassOptions);
        } catch (error) {
          throw new Error(`OpticalGlass: invalid data-glass-options: ${error.message}`);
        }
      }

      const preset = String(data.opticalGlass || data.liquidGlass || "quiet").trim() || "quiet";
      let specification = mergeGlassSpecification(options, {
        target,
        preset,
        content: { type: "live", source: target },
      });
      const shapePresets = {
        pill: { radius: 0.5, radiusMax: 999, bevel: 0.22 },
        pebble: { radius: 0.36, radiusMax: 260, bevel: 0.21 },
        soft: { radius: 0.2, radiusMax: 110, bevel: 0.055 },
        square: { radius: 0.075, radiusMax: 34, bevel: 0.11 },
        line: { radius: 0.5, radiusMax: 999, bevel: 0.34 },
      };
      if (shapePresets[data.glassShape]) {
        specification = mergeGlassSpecification(specification, {
          shape: shapePresets[data.glassShape],
        });
      }

      const numericGroups = {
        glassRadius: ["shape", "radius"],
        glassRadiusMax: ["shape", "radiusMax"],
        glassBevel: ["shape", "bevel"],
        glassIrregularity: ["shape", "irregularity"],
        glassRounding: ["shape", "rounding"],
        glassIor: ["optics", "ior"],
        glassRefraction: ["optics", "refraction"],
        glassRoughness: ["optics", "roughness"],
        glassDepth: ["optics", "depth"],
        glassMatte: ["style", "matte"],
        glassTintStrength: ["style", "tintStrength"],
        glassTexture: ["surface", "texture"],
        glassTextureScale: ["surface", "scale"],
        glassGrain: ["surface", "grain"],
        glassRear: ["surface", "rear"],
        glassParallax: ["motion", "parallax"],
        glassTilt: ["motion", "tilt"],
        glassLift: ["motion", "lift"],
        glassMotionCoupling: ["motion", "coupling"],
        glassMotionScale: ["motion", "scale"],
        glassMotionInertia: ["motion", "inertia"],
        glassLayerSeparation: ["motion", "layerSeparation"],
        glassContentDepth: ["motion", "contentDepth"],
      };
      for (const [dataKey, [group, property]] of Object.entries(numericGroups)) {
        if (data[dataKey] === undefined) continue;
        const value = Number(data[dataKey]);
        if (!Number.isFinite(value)) continue;
        specification = mergeGlassSpecification(specification, {
          [group]: { [property]: value },
        });
      }

      if (data.glassMotion !== undefined) {
        specification = mergeGlassSpecification(specification, {
          motion: { enabled: !/^(?:false|0|off|no)$/i.test(data.glassMotion.trim()) },
        });
      }
      if (["group", "surface"].includes(data.glassMotionMode)) {
        specification = mergeGlassSpecification(specification, {
          motion: { mode: data.glassMotionMode },
        });
      }

      for (const [dataKey, property] of [
        ["glassLayer", "layer"],
        ["glassSeed", "seed"],
        ["glassInteraction", "interaction"],
      ]) {
        if (data[dataKey] === undefined) continue;
        const value = Number(data[dataKey]);
        if (Number.isFinite(value)) specification[property] = value;
      }

      if (data.glassTint) {
        const hex = data.glassTint.trim();
        const match = hex.match(/^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i);
        const tint = match
          ? match.slice(1).map((component) => parseInt(component, 16) / 255)
          : hex.split(/[\s,]+/).map(Number).slice(0, 3);
        if (tint.length === 3 && tint.every(Number.isFinite)) {
          specification = mergeGlassSpecification(specification, { style: { tint } });
        }
      }

      if (data.glassSafeArea !== undefined) {
        const rawSafeArea = data.glassSafeArea.trim();
        let safeArea = rawSafeArea;
        if (/^(?:true|auto|on|yes)$/i.test(rawSafeArea)) safeArea = "auto";
        else if (/^(?:false|off|no|none)$/i.test(rawSafeArea)) safeArea = false;
        else if (Number.isFinite(Number(rawSafeArea))) safeArea = Number(rawSafeArea);
        specification.content = { ...specification.content, safeArea };
      }

      const layerCount = clamp(Math.round(Number(data.glassLayers) || 1), 1, 16);
      if (layerCount > 1) {
        const center = (layerCount - 1) * 0.5;
        specification.layers = Array.from({ length: layerCount }, (_, index) => ({
          offset: [(index - center) * 1.4, (center - index) * 1.1],
          scale: 1 - Math.abs(index - center) * 0.003,
          content: index === layerCount - 1,
        }));
      }
      return specification;
    }

    mount(root = this.root, {
      selector = "[data-optical-glass], [data-liquid-glass]",
      observe = true,
      specification,
    } = {}) {
      const scope = typeof root === "string" ? this.root.querySelector(root) : root;
      if (!scope) throw new Error("OpticalGlass: mount root not found");
      const mounted = new Map();
      const owned = new Set();

      const resolveExtra = (target, index) => (typeof specification === "function"
        ? specification({ target, index })
        : specification || {});
      const applyTarget = (target, index = mounted.size) => {
        if (!(target instanceof Element) || !target.matches(selector)) return null;
        const declared = mergeGlassSpecification(
          this._declarativeSpecification(target),
          resolveExtra(target, index),
        );
        let element = mounted.get(target) || this._resolveElement(target);
        if (element) this.update(element, declared);
        else {
          element = this.add(declared);
          owned.add(element);
        }
        mounted.set(target, element);
        return element;
      };
      const scan = (node = scope) => {
        if (node instanceof Element) applyTarget(node);
        node.querySelectorAll?.(selector).forEach((target) => applyTarget(target));
        return controller;
      };
      const releaseTarget = (target) => {
        const element = mounted.get(target);
        if (!element) return;
        if (owned.has(element)) this.remove(element);
        owned.delete(element);
        mounted.delete(target);
      };

      const observer = observe ? new MutationObserver((records) => {
        records.forEach((record) => {
          if (record.type === "attributes") {
            if (!record.attributeName.startsWith("data-glass")
              && record.attributeName !== "data-optical-glass"
              && record.attributeName !== "data-liquid-glass") return;
            if (record.target.matches(selector)) applyTarget(record.target);
            else releaseTarget(record.target);
            return;
          }
          record.addedNodes.forEach((node) => scan(node));
          record.removedNodes.forEach((node) => {
            if (!(node instanceof Element)) return;
            [...mounted.keys()].forEach((target) => {
              if ((node === target || node.contains(target)) && !scope.contains(target)) {
                releaseTarget(target);
              }
            });
          });
        });
      }) : null;

      const controller = {
        get elements() {
          return [...mounted.values()];
        },
        refresh: () => scan(scope),
        disconnect: () => observer?.disconnect(),
        destroy: () => {
          observer?.disconnect();
          [...mounted.keys()].forEach(releaseTarget);
          this.mountControllers.delete(controller);
        },
      };
      scan(scope);
      observer?.observe(scope, { subtree: true, childList: true, attributes: true });
      this.mountControllers.add(controller);
      return controller;
    }

    _ensureMenuStyles(documentRef = document) {
      if (documentRef.getElementById("optical-glass-menu-styles")) return;
      const style = documentRef.createElement("style");
      style.id = "optical-glass-menu-styles";
      style.textContent = `
        .optical-glass-menu {
          --optical-menu-duration: 360ms;
          visibility: hidden;
          opacity: 0;
          pointer-events: none;
          transition:
            opacity calc(var(--optical-menu-duration) * .72) ease,
            transform var(--optical-menu-duration) cubic-bezier(.18,.9,.22,1),
            visibility 0s linear var(--optical-menu-duration);
        }
        .optical-glass-menu.is-open {
          visibility: visible;
          opacity: 1;
          pointer-events: auto;
          transform: none;
          transition-delay: 0s;
        }
        .optical-glass-menu[data-optical-menu-mode="popover"] {
          transform: translateY(16px) scale(.94) rotateX(-7deg);
          transform-origin: 72% 0%;
        }
        .optical-glass-menu[data-optical-menu-mode="drawer"] {
          transform: translateX(42px) scale(.985) rotateY(-4deg);
          transform-origin: 100% 50%;
        }
        .optical-glass-menu[data-optical-menu-mode="orbit"] {
          transform: scale(.72) rotate(-9deg);
          transform-origin: 50% 50%;
        }
        .optical-glass-menu[data-optical-menu-mode="orbit"] > * {
          opacity: 0;
          transform: translateY(12px) scale(.72);
          transition:
            opacity 180ms ease,
            transform 340ms cubic-bezier(.18,.9,.22,1);
          transition-delay: 0ms;
        }
        .optical-glass-menu[data-optical-menu-mode="orbit"].is-open > * {
          opacity: 1;
          transform: none;
          transition-delay: calc(var(--optical-menu-index, 0) * 34ms);
        }
        @media (prefers-reduced-motion: reduce) {
          .optical-glass-menu,
          .optical-glass-menu[data-optical-menu-mode="orbit"] > * {
            transition-duration: 1ms;
          }
        }
      `;
      documentRef.head.append(style);
    }

    createMenu({
      trigger,
      panel,
      mode = "popover",
      open = false,
      closeOnOutside = true,
      closeOnEscape = true,
      popupType,
      focusOnOpen = false,
      glass = { preset: "panel", layer: 8 },
    } = {}) {
      const triggerElement = typeof trigger === "string" ? this.root.querySelector(trigger) : trigger;
      const panelElement = typeof panel === "string" ? this.root.querySelector(panel) : panel;
      if (!triggerElement || !panelElement) throw new Error("OpticalGlass: menu elements not found");
      const documentRef = triggerElement.ownerDocument;
      this._ensureMenuStyles(documentRef);
      let currentMode = ["popover", "drawer", "orbit"].includes(mode) ? mode : "popover";
      let opened = false;
      let hideTimer = 0;
      const generatedId = panelElement.id || `optical-glass-menu-${this.menuControllers.size + 1}`;
      panelElement.id = generatedId;
      panelElement.classList.add("optical-glass-menu");
      panelElement.dataset.opticalMenuMode = currentMode;
      const resolvedPopupType = popupType
        || (panelElement.getAttribute("role") === "menu" ? "menu" : "dialog");
      const addedPanelRole = !panelElement.hasAttribute("role")
        && ["menu", "listbox", "tree", "grid", "dialog"].includes(resolvedPopupType);
      if (addedPanelRole
        && ["menu", "listbox", "tree", "grid", "dialog"].includes(resolvedPopupType)) {
        panelElement.setAttribute("role", resolvedPopupType);
      }
      const addedMenuItemRoles = [];
      const menuChildren = [...panelElement.children];
      const previousMenuIndexes = menuChildren.map((child) => ({
        child,
        value: child.style.getPropertyValue("--optical-menu-index"),
        priority: child.style.getPropertyPriority("--optical-menu-index"),
      }));
      menuChildren.forEach((child, index) => {
        child.style.setProperty("--optical-menu-index", String(index));
        if (resolvedPopupType === "menu"
          && child.matches("button, a")
          && !child.hasAttribute("role")) {
          child.setAttribute("role", "menuitem");
          addedMenuItemRoles.push(child);
        }
      });
      triggerElement.setAttribute("aria-controls", generatedId);
      triggerElement.setAttribute("aria-haspopup", resolvedPopupType);
      const addedGlass = glass !== false && !this._resolveElement(panelElement)
        ? this.add({ target: panelElement, content: { type: "live", source: panelElement }, ...glass })
        : null;

      const setOpen = (next, { restoreFocus = false } = {}) => {
        opened = Boolean(next);
        window.clearTimeout(hideTimer);
        if (opened) {
          panelElement.hidden = false;
          void panelElement.offsetWidth;
        }
        panelElement.classList.toggle("is-open", opened);
        panelElement.setAttribute("aria-hidden", String(!opened));
        panelElement.inert = !opened;
        triggerElement.setAttribute("aria-expanded", String(opened));
        if (opened && focusOnOpen) {
          window.requestAnimationFrame(() => {
            const focusTarget = panelElement.querySelector(
              "[autofocus], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex='-1'])",
            );
            focusTarget?.focus({ preventScroll: true });
          });
        }
        if (!opened && restoreFocus && panelElement.contains(documentRef.activeElement)) {
          triggerElement.focus({ preventScroll: true });
        }
        if (!opened) {
          hideTimer = window.setTimeout(() => {
            if (!opened) panelElement.hidden = true;
          }, 380);
        }
        return opened;
      };
      const onTrigger = () => setOpen(!opened);
      const onDocumentPointer = (event) => {
        if (!opened || !closeOnOutside) return;
        const path = event.composedPath();
        if (!path.includes(panelElement) && !path.includes(triggerElement)) setOpen(false);
      };
      const onDocumentKey = (event) => {
        if (opened && closeOnEscape && event.key === "Escape") setOpen(false, { restoreFocus: true });
      };
      triggerElement.addEventListener("click", onTrigger);
      documentRef.addEventListener("pointerdown", onDocumentPointer, true);
      documentRef.addEventListener("keydown", onDocumentKey);

      const controller = {
        get open() { return opened; },
        get mode() { return currentMode; },
        show: () => setOpen(true),
        hide: (options) => setOpen(false, options),
        toggle: () => setOpen(!opened),
        setMode: (nextMode) => {
          if (!["popover", "drawer", "orbit"].includes(nextMode)) return currentMode;
          currentMode = nextMode;
          panelElement.dataset.opticalMenuMode = currentMode;
          return currentMode;
        },
        destroy: () => {
          triggerElement.removeEventListener("click", onTrigger);
          documentRef.removeEventListener("pointerdown", onDocumentPointer, true);
          documentRef.removeEventListener("keydown", onDocumentKey);
          window.clearTimeout(hideTimer);
          panelElement.hidden = false;
          panelElement.classList.remove("optical-glass-menu", "is-open");
          panelElement.removeAttribute("aria-hidden");
          panelElement.removeAttribute("inert");
          if (addedPanelRole) panelElement.removeAttribute("role");
          addedMenuItemRoles.forEach((child) => child.removeAttribute("role"));
          previousMenuIndexes.forEach(({ child, value, priority }) => {
            if (value) child.style.setProperty("--optical-menu-index", value, priority);
            else child.style.removeProperty("--optical-menu-index");
          });
          triggerElement.removeAttribute("aria-controls");
          triggerElement.removeAttribute("aria-haspopup");
          triggerElement.removeAttribute("aria-expanded");
          if (addedGlass) this.remove(addedGlass);
          this.menuControllers.delete(controller);
        },
      };
      setOpen(open);
      if (!open) {
        window.clearTimeout(hideTimer);
        panelElement.hidden = true;
      }
      this.menuControllers.add(controller);
      return controller;
    }

    createPopover(options = {}) {
      return this.createMenu({ ...options, mode: "popover" });
    }

    createDrawer(options = {}) {
      return this.createMenu({ ...options, mode: "drawer", popupType: options.popupType || "dialog" });
    }

    createOrbitMenu(options = {}) {
      return this.createMenu({ ...options, mode: "orbit" });
    }

    _resolveElement(reference) {
      if (this.elements.includes(reference)) return reference;
      if (reference instanceof Element) {
        return this.elements.find((element) => element.target === reference) || null;
      }
      if (typeof reference === "string") {
        return this.elements.find((element) => element.target.matches(reference)) || null;
      }
      return null;
    }

    _cloneLayer(layer = {}) {
      return {
        ...layer,
        offset: Array.isArray(layer.offset) ? [...layer.offset] : layer.offset,
        scale: Array.isArray(layer.scale) ? [...layer.scale] : layer.scale,
        shape: layer.shape ? { ...layer.shape } : layer.shape,
        optics: layer.optics ? { ...layer.optics } : layer.optics,
        style: layer.style ? {
          ...layer.style,
          tint: Array.isArray(layer.style.tint) ? [...layer.style.tint] : layer.style.tint,
        } : layer.style,
        surface: layer.surface ? { ...layer.surface } : layer.surface,
        motion: layer.motion ? { ...layer.motion } : layer.motion,
      };
    }

    addLayer(reference, layer = {}, index) {
      const element = this._resolveElement(reference);
      if (!element) throw new Error("OpticalGlass: layered element not found");
      const nextLayer = this._cloneLayer(layer);
      const position = Number.isInteger(index)
        ? clamp(index, 0, element.layers.length)
        : element.layers.length;
      element.layers.splice(position, 0, nextLayer);
      return nextLayer;
    }

    updateLayer(reference, index, patch = {}) {
      const element = this._resolveElement(reference);
      const current = element?.layers[index];
      if (!current) throw new Error("OpticalGlass: layer not found");
      element.layers[index] = {
        ...current,
        ...patch,
        shape: patch.shape ? { ...(current.shape || {}), ...patch.shape } : current.shape,
        optics: patch.optics ? { ...(current.optics || {}), ...patch.optics } : current.optics,
        style: patch.style ? { ...(current.style || {}), ...patch.style } : current.style,
        surface: patch.surface ? { ...(current.surface || {}), ...patch.surface } : current.surface,
        motion: patch.motion ? { ...(current.motion || {}), ...patch.motion } : current.motion,
      };
      return element.layers[index];
    }

    removeLayer(reference, index) {
      const element = this._resolveElement(reference);
      if (!element || element.layers.length <= 1 || !element.layers[index]) return false;
      element.layers.splice(index, 1);
      return true;
    }

    setLayers(reference, layers = [{}]) {
      const element = this._resolveElement(reference);
      if (!element) throw new Error("OpticalGlass: layered element not found");
      if (!Array.isArray(layers) || layers.length === 0) {
        throw new Error("OpticalGlass: at least one layer is required");
      }
      element.layers = layers.map((layer) => this._cloneLayer(layer));
      return element.layers;
    }

    destroy() {
      if (this.destroyed) return this;
      this.stop();
      window.clearTimeout(this.atlasRefreshTimer);
      cancelAnimationFrame(this.resizeRefreshRaf);
      this.resizeRefreshRaf = 0;
      this.pendingResizeTargets.clear();
      if (typeof this.reducedMotionQuery.removeEventListener === "function") {
        this.reducedMotionQuery.removeEventListener("change", this.onReducedMotionChange);
      } else {
        this.reducedMotionQuery.removeListener?.(this.onReducedMotionChange);
      }
      this.mountControllers.forEach((controller) => controller.disconnect());
      this.mountControllers.clear();
      [...this.menuControllers].forEach((controller) => controller.destroy());
      [...this.deviceTiltControllers].forEach((controller) => controller.disable());
      this.resizeObserver?.disconnect();
      this.elements.forEach((element) => {
        element.listeners.forEach(([type, listener]) => {
          element.target.removeEventListener(type, listener);
        });
        element.listeners = [];
        this._releaseLiveMotion(element);
      });
      const gl = this.gl;
      if (gl) {
        [
          this.backgroundTexture,
          this.fieldTexture,
          this.colorTexture,
          this.shapeTexture,
          this.linearBackgroundTexture,
          ...this.sceneTextures,
        ].filter(Boolean).forEach((texture) => gl.deleteTexture(texture));
        [
          this.linearBackgroundFramebuffer,
          ...this.sceneFramebuffers,
        ].filter(Boolean).forEach((framebuffer) => gl.deleteFramebuffer(framebuffer));
        if (this.program) gl.deleteProgram(this.program);
        if (this.positionBuffer) gl.deleteBuffer(this.positionBuffer);
      }
      this.sceneTextures = [];
      this.sceneFramebuffers = [];
      this.destroyed = true;
      this.initialized = false;
      return this;
    }

    _initializeGl() {
      const gl = this.gl;
      const floatTargets = gl.getExtension("EXT_color_buffer_float")
        && gl.getExtension("OES_texture_float_linear");
      this.renderTargetFormat = floatTargets ? gl.RGBA16F : gl.RGBA8;
      this.renderTargetType = floatTargets ? gl.HALF_FLOAT : gl.UNSIGNED_BYTE;
      this.metrics.precision = floatTargets ? "rgba16f" : "rgba8";
      this.positionBuffer = gl.createBuffer();
      gl.bindBuffer(gl.ARRAY_BUFFER, this.positionBuffer);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        new Float32Array([-1, -1, 3, -1, -1, 3]),
        gl.STATIC_DRAW,
      );

      this.backgroundTexture = this._createTexture();
      this.fieldTexture = this._createTexture();
      this.colorTexture = this._createTexture();
      this.shapeTexture = this._createTexture();
      this._uploadEmptyAtlases();
    }

    _createTexture() {
      const gl = this.gl;
      const texture = gl.createTexture();
      gl.bindTexture(gl.TEXTURE_2D, texture);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
      return texture;
    }

    _ensureRenderTargets(width, height) {
      const gl = this.gl;
      if (
        this.renderTargetSize[0] === width
        && this.renderTargetSize[1] === height
        && this.sceneTextures.length === 2
        && this.linearBackgroundTexture
        && this.linearBackgroundFramebuffer
      ) return;

      this.sceneTextures.forEach((texture) => gl.deleteTexture(texture));
      this.sceneFramebuffers.forEach((framebuffer) => gl.deleteFramebuffer(framebuffer));
      if (this.linearBackgroundTexture) gl.deleteTexture(this.linearBackgroundTexture);
      if (this.linearBackgroundFramebuffer) {
        gl.deleteFramebuffer(this.linearBackgroundFramebuffer);
      }
      this.sceneTextures = [];
      this.sceneFramebuffers = [];
      this.linearBackgroundTexture = null;
      this.linearBackgroundFramebuffer = null;

      const createRenderTarget = () => {
        const texture = this._createTexture();
        gl.bindTexture(gl.TEXTURE_2D, texture);
        gl.texImage2D(
          gl.TEXTURE_2D,
          0,
          this.renderTargetFormat,
          width,
          height,
          0,
          gl.RGBA,
          this.renderTargetType,
          null,
        );
        const framebuffer = gl.createFramebuffer();
        gl.bindFramebuffer(gl.FRAMEBUFFER, framebuffer);
        gl.framebufferTexture2D(
          gl.FRAMEBUFFER,
          gl.COLOR_ATTACHMENT0,
          gl.TEXTURE_2D,
          texture,
          0,
        );
        if (gl.checkFramebufferStatus(gl.FRAMEBUFFER) !== gl.FRAMEBUFFER_COMPLETE) {
          throw new Error("OpticalGlass: layered framebuffer is incomplete");
        }
        return { texture, framebuffer };
      };

      for (let index = 0; index < 2; index += 1) {
        const target = createRenderTarget();
        this.sceneTextures.push(target.texture);
        this.sceneFramebuffers.push(target.framebuffer);
      }

      const backgroundTarget = createRenderTarget();
      this.linearBackgroundTexture = backgroundTarget.texture;
      this.linearBackgroundFramebuffer = backgroundTarget.framebuffer;

      gl.bindFramebuffer(gl.FRAMEBUFFER, null);
      this.renderTargetSize = [width, height];
      this.backgroundDirty = true;
    }

    _uploadEmptyAtlases() {
      const gl = this.gl;
      const empty = new Uint8Array([0, 0, 0, 0]);
      for (const texture of [this.fieldTexture, this.colorTexture, this.shapeTexture]) {
        gl.bindTexture(gl.TEXTURE_2D, texture);
        gl.texImage2D(
          gl.TEXTURE_2D, 0, gl.RGBA, 1, 1, 0,
          gl.RGBA, gl.UNSIGNED_BYTE, empty,
        );
      }
    }

    _uploadBackground() {
      const gl = this.gl;
      gl.bindTexture(gl.TEXTURE_2D, this.backgroundTexture);
      // The shader works in top-left screen coordinates, so DOM image rows are
      // uploaded without an additional vertical inversion.
      gl.pixelStorei(gl.UNPACK_FLIP_Y_WEBGL, false);
      gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, false);
      gl.texImage2D(
        gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA,
        gl.UNSIGNED_BYTE, this.backgroundImage,
      );
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR_MIPMAP_LINEAR);
      gl.generateMipmap(gl.TEXTURE_2D);
      this.backgroundDirty = true;
    }

    _compileShader(type, source) {
      const gl = this.gl;
      const shader = gl.createShader(type);
      gl.shaderSource(shader, source);
      gl.compileShader(shader);
      if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
        throw new Error(gl.getShaderInfoLog(shader) || "OpticalGlass shader compilation failed");
      }
      return shader;
    }

    _buildProgram() {
      const gl = this.gl;
      if (this.program) gl.deleteProgram(this.program);
      const program = gl.createProgram();
      const vertexShader = this._compileShader(gl.VERTEX_SHADER, VERTEX_SHADER);
      const pixelShader = this._compileShader(gl.FRAGMENT_SHADER, fragmentShader());
      gl.attachShader(program, vertexShader);
      gl.attachShader(program, pixelShader);
      gl.linkProgram(program);
      gl.deleteShader(vertexShader);
      gl.deleteShader(pixelShader);
      if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
        throw new Error(gl.getProgramInfoLog(program) || "OpticalGlass program link failed");
      }

      this.program = program;
      this.uniforms = {
        background: gl.getUniformLocation(program, "u_background"),
        scene: gl.getUniformLocation(program, "u_scene"),
        fieldAtlas: gl.getUniformLocation(program, "u_fieldAtlas"),
        colorAtlas: gl.getUniformLocation(program, "u_colorAtlas"),
        shapeAtlas: gl.getUniformLocation(program, "u_shapeAtlas"),
        resolution: gl.getUniformLocation(program, "u_resolution"),
        imageSize: gl.getUniformLocation(program, "u_imageSize"),
        dpr: gl.getUniformLocation(program, "u_dpr"),
        time: gl.getUniformLocation(program, "u_time"),
        textureStrength: gl.getUniformLocation(program, "u_textureStrength"),
        activeElement: gl.getUniformLocation(program, "u_activeElement"),
        element: {
          rect: gl.getUniformLocation(program, "u_rect"),
          state: gl.getUniformLocation(program, "u_state"),
          shape: gl.getUniformLocation(program, "u_shape"),
          optics: gl.getUniformLocation(program, "u_optics"),
          style: gl.getUniformLocation(program, "u_style"),
          styleExtra: gl.getUniformLocation(program, "u_styleExtra"),
          surface: gl.getUniformLocation(program, "u_surface"),
          contentScreen: gl.getUniformLocation(program, "u_contentScreen"),
          contentAtlas: gl.getUniformLocation(program, "u_contentAtlasRect"),
          shapeAtlas: gl.getUniformLocation(program, "u_shapeAtlasRect"),
          contentOpts: gl.getUniformLocation(program, "u_contentOpts"),
          volumeOpts: gl.getUniformLocation(program, "u_volumeOpts"),
          volumeStyle: gl.getUniformLocation(program, "u_volumeStyle"),
        },
      };

      gl.useProgram(program);
      const position = gl.getAttribLocation(program, "a_position");
      gl.bindBuffer(gl.ARRAY_BUFFER, this.positionBuffer);
      gl.enableVertexAttribArray(position);
      gl.vertexAttribPointer(position, 2, gl.FLOAT, false, 0, 0);
      gl.uniform1i(this.uniforms.background, 0);
      gl.uniform1i(this.uniforms.fieldAtlas, 1);
      gl.uniform1i(this.uniforms.colorAtlas, 2);
      gl.uniform1i(this.uniforms.shapeAtlas, 3);
      gl.uniform1i(this.uniforms.scene, 4);
      this.programDirty = false;
    }

    _resize() {
      const dpr = Math.min(window.devicePixelRatio || 1, this.dprCap);
      const width = Math.max(1, Math.round(this.canvas.clientWidth * dpr));
      const height = Math.max(1, Math.round(this.canvas.clientHeight * dpr));
      if (this.canvas.width !== width || this.canvas.height !== height) {
        this.canvas.width = width;
        this.canvas.height = height;
        this.gl.viewport(0, 0, width, height);
        this.atlasDirty = true;
      }
      this._ensureRenderTargets(width, height);
      this.lastDpr = dpr;
      return dpr;
    }

    _toCanvasRect(rect, canvasRect = this.canvas.getBoundingClientRect()) {
      return {
        left: rect.left - canvasRect.left,
        top: rect.top - canvasRect.top,
        width: rect.width,
        height: rect.height,
      };
    }

    _refreshMotionHierarchy() {
      const byTarget = new Map(this.elements.map((element) => [element.target, element]));
      this.elements.forEach((element) => {
        let node = element.target.parentElement;
        let parent = null;
        while (node && !parent) {
          parent = byTarget.get(node) || null;
          node = node.parentElement;
        }
        element.runtime.motionParent = parent;
      });
      const resolveDepth = (element, visited = new Set()) => {
        if (!element.runtime.motionParent || visited.has(element)) return 0;
        visited.add(element);
        return 1 + resolveDepth(element.runtime.motionParent, visited);
      };
      this.elements.forEach((element) => {
        element.runtime.motionDepth = resolveDepth(element);
      });
    }

    _ensureLiveMotionStyles(documentRef = document) {
      if (documentRef.getElementById("optical-glass-live-motion-styles")) return;
      const style = documentRef.createElement("style");
      style.id = "optical-glass-live-motion-styles";
      style.textContent = `
        .liquid-glass-live-motion {
          transform-style: preserve-3d;
          backface-visibility: hidden;
        }
        .liquid-glass-live-motion > :where(:not(.liquid-glass-mounted)) {
          translate: 0 0 var(--optical-content-depth, -1.35px);
          transform-style: preserve-3d;
          backface-visibility: hidden;
        }
      `;
      documentRef.head.append(style);
    }

    _resolveSafeInsets(element, rect = element.target.getBoundingClientRect()) {
      const safeArea = element.content?.safeArea;
      if (safeArea === undefined || safeArea === null || safeArea === false) return null;

      const minDimension = Math.max(0, Math.min(rect.width, rect.height));
      const radius = Math.min(
        minDimension * clamp(Number(element.shape.radius) || 0, 0, 0.5),
        Math.max(0, Number(element.shape.radiusMax) || minDimension),
      );
      const bevel = minDimension * clamp(Number(element.shape.bevel) || 0, 0, 0.5);
      // The corner inset protects against the circular silhouette.  A wide
      // optical bevel also bends and compresses content before the silhouette,
      // so reserve part of that transition without letting nested safe areas
      // consume the layout on narrow viewports.
      // This keeps live copy on the visually flat/readable part of thick
      // pebble-shaped glass instead of merely keeping it inside the DOM box.
      const automatic = radius * (1 - Math.SQRT1_2) + bevel * 0.18;

      if (safeArea === true || safeArea === "auto") {
        return { top: automatic, right: automatic, bottom: automatic, left: automatic };
      }
      if (Number.isFinite(Number(safeArea))) {
        const value = Math.max(0, Number(safeArea));
        return { top: value, right: value, bottom: value, left: value };
      }
      if (Array.isArray(safeArea)) {
        const inline = Math.max(0, Number(safeArea[0]) || 0);
        const block = Math.max(0, Number(safeArea[1] ?? safeArea[0]) || 0);
        return { top: block, right: inline, bottom: block, left: inline };
      }
      if (typeof safeArea === "object") {
        const inline = Math.max(0, Number(safeArea.inline) || automatic);
        const block = Math.max(0, Number(safeArea.block) || automatic);
        return {
          top: Math.max(0, Number(safeArea.top) || block),
          right: Math.max(0, Number(safeArea.right) || inline),
          bottom: Math.max(0, Number(safeArea.bottom) || block),
          left: Math.max(0, Number(safeArea.left) || inline),
        };
      }
      return { top: automatic, right: automatic, bottom: automatic, left: automatic };
    }

    _syncSafeArea(element) {
      const insets = element.content?.type === "live"
        ? this._resolveSafeInsets(element)
        : null;
      if (!insets) {
        this._releaseSafeArea(element);
        return;
      }

      const target = element.target;
      const properties = ["padding-top", "padding-right", "padding-bottom", "padding-left"];
      if (!element.runtime.safeAreaStyle) {
        const computed = getComputedStyle(target);
        element.runtime.safeAreaStyle = {
          hadClass: target.classList.contains("optical-glass-safe-area"),
          values: Object.fromEntries(properties.map((property) => [property, {
            value: target.style.getPropertyValue(property),
            priority: target.style.getPropertyPriority(property),
          }])),
          base: {
            top: Number.parseFloat(computed.paddingTop) || 0,
            right: Number.parseFloat(computed.paddingRight) || 0,
            bottom: Number.parseFloat(computed.paddingBottom) || 0,
            left: Number.parseFloat(computed.paddingLeft) || 0,
          },
          frame: "",
        };
        target.classList.add("optical-glass-safe-area");
      }

      const snapshot = element.runtime.safeAreaStyle;
      const resolved = {
        top: Math.max(snapshot.base.top, insets.top),
        right: Math.max(snapshot.base.right, insets.right),
        bottom: Math.max(snapshot.base.bottom, insets.bottom),
        left: Math.max(snapshot.base.left, insets.left),
      };
      const frame = Object.values(resolved).map((value) => value.toFixed(3)).join("|");
      if (frame === snapshot.frame) return;
      target.style.setProperty("padding-top", `${resolved.top.toFixed(3)}px`);
      target.style.setProperty("padding-right", `${resolved.right.toFixed(3)}px`);
      target.style.setProperty("padding-bottom", `${resolved.bottom.toFixed(3)}px`);
      target.style.setProperty("padding-left", `${resolved.left.toFixed(3)}px`);
      snapshot.frame = frame;
    }

    _releaseSafeArea(element) {
      const snapshot = element.runtime.safeAreaStyle;
      if (!snapshot) return;
      for (const [property, previous] of Object.entries(snapshot.values)) {
        if (previous.value) element.target.style.setProperty(property, previous.value, previous.priority);
        else element.target.style.removeProperty(property);
      }
      if (!snapshot.hadClass) element.target.classList.remove("optical-glass-safe-area");
      element.runtime.safeAreaStyle = null;
    }

    _syncLiveMotion(element) {
      const enabled = element.content?.type === "live" && element.motion?.enabled !== false;
      if (!enabled) {
        this._releaseLiveMotion(element);
        return;
      }
      if (element.runtime.motionStyle) return;

      this._ensureLiveMotionStyles(element.target.ownerDocument);

      const properties = [
        "translate",
        "rotate",
        "scale",
        "transform-style",
        "transform-origin",
        "will-change",
        "--optical-content-depth",
      ];
      const values = Object.fromEntries(properties.map((property) => [property, {
        value: element.target.style.getPropertyValue(property),
        priority: element.target.style.getPropertyPriority(property),
      }]));
      element.runtime.motionStyle = {
        hadClass: element.target.classList.contains("liquid-glass-live-motion"),
        values,
      };
      element.target.classList.add("liquid-glass-live-motion");
      element.target.style.setProperty("transform-style", "preserve-3d");
      element.target.style.setProperty("transform-origin", "50% 50%");
      element.target.style.setProperty("will-change", "translate, rotate, scale");
      element.runtime.motionFrame = "";
    }

    _releaseLiveMotion(element) {
      const snapshot = element.runtime.motionStyle;
      if (!snapshot) return;
      for (const [property, previous] of Object.entries(snapshot.values)) {
        if (previous.value) {
          element.target.style.setProperty(property, previous.value, previous.priority);
        } else {
          element.target.style.removeProperty(property);
        }
      }
      if (!snapshot.hadClass) element.target.classList.remove("liquid-glass-live-motion");
      element.runtime.motionStyle = null;
      element.runtime.motionFrame = "";
    }

    _applyLiveMotion(element, reducedMotion = false) {
      if (!element.runtime.motionStyle) return;
      const state = element.runtime.composedState || element.state;
      const motion = element.motion;
      const interaction = Math.max(0, Number(element.interaction) || 0);
      const hierarchyCoupling = element.runtime.motionParent
        ? Math.pow(clamp(Number(motion.coupling) || 0.7, 0, 1), Math.min(element.runtime.motionDepth, 3))
        : 1;
      const depthScale = Math.sqrt(clamp((Number(element.optics.depth) || 48) / 52, 0.25, 6));
      const hover = reducedMotion ? 0 : state.hover;
      const pressed = reducedMotion ? 0 : state.pressed;
      const pointerX = state.pointerX;
      const pointerY = state.pointerY;
      const pointerLength = Math.max(0.0001, Math.hypot(pointerX, pointerY));
      const response = hover * interaction * hierarchyCoupling;
      const parallax = Math.max(0, Number(motion.parallax) || 0) * depthScale;
      const x = pointerX * response * parallax;
      const y = pointerY * response * parallax * 0.72;
      const z = response * Math.max(0, Number(motion.lift) || 0) * depthScale
        - pressed * Math.max(0, Number(motion.lift) || 0) * 0.38;
      const axisX = -pointerY / pointerLength;
      const axisY = pointerX / pointerLength;
      const angle = pointerLength * response * Math.max(0, Number(motion.tilt) || 0);
      const scale = Math.max(
        0.92,
        1 + response * (Number(motion.scale) || 0) * depthScale
          - pressed * Math.max(0, Number(motion.press) || 0),
      );
      const contentDepth = -Math.max(0, Number(motion.contentDepth) || 0)
        * clamp(0.72 + depthScale * 0.34, 0.8, 1.8);
      const frame = [x, y, z, axisX, axisY, angle, scale, contentDepth]
        .map((value) => value.toFixed(3)).join("|");
      if (frame === element.runtime.motionFrame) return;

      element.target.style.setProperty("translate", `${x.toFixed(3)}px ${y.toFixed(3)}px ${z.toFixed(3)}px`);
      element.target.style.setProperty("rotate", `${axisX.toFixed(4)} ${axisY.toFixed(4)} 0 ${angle.toFixed(3)}deg`);
      element.target.style.setProperty("scale", scale.toFixed(5));
      element.target.style.setProperty("--optical-content-depth", `${contentDepth.toFixed(3)}px`);
      element.runtime.motionFrame = frame;
    }

    _bindInteraction(element) {
      const on = (type, listener) => {
        element.target.addEventListener(type, listener);
        element.listeners.push([type, listener]);
      };
      const updatePointer = (event) => {
        element.runtime.lastPointerType = event.pointerType || "mouse";
        const rect = element.target.getBoundingClientRect();
        element.state.pointerTargetX = clamp(
          ((event.clientX - rect.left) / rect.width) * 2 - 1,
          -1,
          1,
        );
        element.state.pointerTargetY = clamp(
          ((event.clientY - rect.top) / rect.height) * 2 - 1,
          -1,
          1,
        );
        const contentMotion = this.reducedMotionQuery.matches ? 0 : 1;
        element.target.style.setProperty(
          "--label-x",
          `${(-element.state.pointerTargetX * 10 * contentMotion).toFixed(1)}px`,
        );
        element.target.style.setProperty(
          "--label-y",
          `${(-element.state.pointerTargetY * 7.2 * contentMotion).toFixed(1)}px`,
        );
      };

      on("pointerenter", (event) => {
        element.state.hoverTarget = event.pointerType === "touch" ? 0.22 : 1;
        updatePointer(event);
      });
      on("pointermove", updatePointer);
      on("pointerdown", (event) => {
        element.runtime.lastPointerType = event.pointerType || "mouse";
        if (event.pointerType === "touch") {
          element.state.hoverTarget = Math.max(element.state.hoverTarget, 0.32);
        }
        element.state.pressedTarget = 1;
        element.target.classList.add("is-pressed");
      });
      on("pointerup", (event) => {
        element.state.pressedTarget = 0;
        element.target.classList.remove("is-pressed");
        if (event.pointerType === "touch") {
          requestAnimationFrame(() => this._resetInteraction(element, false));
        }
      });
      on("pointercancel", () => this._resetInteraction(element));
      on("pointerleave", () => this._resetInteraction(element));
      on("focusin", () => {
        if (element.state.hoverTarget < 0.1) {
          element.state.pointerTargetX = 0;
          element.state.pointerTargetY = 0;
        }
        element.state.hoverTarget = Math.max(element.state.hoverTarget, 0.58);
      });
      on("focusout", (event) => {
        if (!event.relatedTarget || !element.target.contains(event.relatedTarget)) {
          this._resetInteraction(element, false);
        }
      });
      on("keydown", (event) => {
        if (event.target !== element.target || !["Enter", " "].includes(event.key)) return;
        element.state.pressedTarget = 1;
        element.target.classList.add("is-pressed");
      });
      on("keyup", (event) => {
        if (event.target !== element.target || !["Enter", " "].includes(event.key)) return;
        element.state.pressedTarget = 0;
        element.target.classList.remove("is-pressed");
      });
    }

    _resetInteraction(element, preserveFocus = true) {
      const keepsFocus = preserveFocus && element.target.matches(":focus-within");
      element.state.hoverTarget = keepsFocus ? 0.58 : 0;
      element.state.pressedTarget = 0;
      element.state.pointerTargetX = 0;
      element.state.pointerTargetY = keepsFocus ? 0 : -0.65;
      element.target.style.setProperty("--label-x", "0px");
      element.target.style.setProperty("--label-y", "0px");
      element.target.classList.remove("is-pressed");
    }

    _renderContent(element, rect, dpr) {
      const content = element.content;
      if (!content) return null;
      const context = { element, target: element.target, rect, dpr };
      const type = content.type || "text";

      if (type === "live") return null;

      if (type === "text") {
        const label = content.source
          ? element.target.querySelector(content.source)
          : element.target.querySelector(".glass-button__label");
        const style = label ? getComputedStyle(label) : getComputedStyle(element.target);
        const text = String(resolveValue(content.text, context)
          ?? label?.textContent?.trim()
          ?? element.target.textContent.trim());
        let fontSizeCss = Number(resolveValue(content.fontSize, context))
          || Number.parseFloat(style.fontSize)
          || 64;
        const family = content.fontFamily || style.fontFamily || "sans-serif";
        const weight = content.fontWeight || style.fontWeight || 700;
        const letterSpacingCss = content.letterSpacing
          ?? Number.parseFloat(style.letterSpacing)
          ?? -fontSizeCss * 0.035;
        const wordSpacingCss = content.wordSpacing
          ?? Number.parseFloat(style.wordSpacing)
          ?? fontSizeCss * 0.06;
        const safeInsets = this._resolveSafeInsets(element, rect);
        const safeWidth = Math.max(
          1,
          rect.width - (safeInsets?.left || 0) - (safeInsets?.right || 0),
        );
        const measureCanvas = createCanvas();
        const measure = measureCanvas.getContext("2d");
        measure.font = `${weight} ${fontSizeCss * dpr}px ${family}`;
        if ("letterSpacing" in measure) measure.letterSpacing = `${letterSpacingCss * dpr}px`;
        if ("wordSpacing" in measure) measure.wordSpacing = `${wordSpacingCss * dpr}px`;
        const initialMetrics = measure.measureText(text);
        const requestedPadding = fontSizeCss * (content.paddingEm ?? 0.28) * 2;
        const fit = clamp(safeWidth / Math.max(1, initialMetrics.width / dpr + requestedPadding), 0.25, 1);
        fontSizeCss *= fit;
        const fontSize = fontSizeCss * dpr;
        measure.font = `${weight} ${fontSize}px ${family}`;
        const metrics = measure.measureText(text);
        const padding = Math.ceil(fontSize * (content.paddingEm ?? 0.28));
        const width = Math.ceil(metrics.width + padding * 2);
        const height = Math.ceil(fontSize * 1.34 + padding * 2);
        const mask = createCanvas(width, height);
        const maskContext = mask.getContext("2d");
        maskContext.font = `${weight} ${fontSize}px ${family}`;
        if ("letterSpacing" in maskContext) maskContext.letterSpacing = `${letterSpacingCss * dpr}px`;
        if ("wordSpacing" in maskContext) maskContext.wordSpacing = `${wordSpacingCss * dpr}px`;
        maskContext.textAlign = "center";
        maskContext.textBaseline = "middle";
        maskContext.fillStyle = "white";
        maskContext.fillText(text, width * 0.5, height * 0.5);

        const color = createCanvas(width, height);
        const colorContext = color.getContext("2d");
        colorContext.font = `${weight} ${fontSize}px ${family}`;
        if ("letterSpacing" in colorContext) colorContext.letterSpacing = `${letterSpacingCss * dpr}px`;
        if ("wordSpacing" in colorContext) colorContext.wordSpacing = `${wordSpacingCss * dpr}px`;
        colorContext.textAlign = "center";
        colorContext.textBaseline = "middle";
        colorContext.fillStyle = content.color || style.color || "#080b0e";
        colorContext.fillText(text, width * 0.5, height * 0.5);

        const offset = resolveValue(content.offset, context) || [0, 0];
        const centerX = (rect.left + rect.width * 0.5 + offset[0]) * dpr;
        const centerY = (rect.top + rect.height * 0.5 + offset[1]) * dpr;
        element.runtime.grooveDepth = fontSizeCss * (content.depthEm ?? 0.095);
        element.runtime.fillOpacity = content.fillOpacity ?? 1;
        return {
          mask,
          color,
          blur: fontSize * (content.roundingEm ?? 0.12),
          screenRect: [centerX - width * 0.5, centerY - height * 0.5, width, height],
        };
      }

      if (type === "image") {
        const image = element.runtime.image;
        if (!image) return null;
        const widthCss = Number(resolveValue(content.width, context)) || rect.width * 0.48;
        const heightCss = Number(resolveValue(content.height, context))
          || widthCss * (image.naturalHeight / image.naturalWidth);
        const width = Math.ceil(widthCss * dpr);
        const height = Math.ceil(heightCss * dpr);
        const mask = createCanvas(width, height);
        mask.getContext("2d").drawImage(image, 0, 0, width, height);
        const color = createCanvas(width, height);
        color.getContext("2d").drawImage(image, 0, 0, width, height);
        const depth = content.depthFromLuma ? createCanvas(width, height) : null;
        if (depth) depth.getContext("2d").drawImage(image, 0, 0, width, height);
        const offset = resolveValue(content.offset, context) || [0, 0];
        const centerX = (rect.left + rect.width * 0.5 + offset[0]) * dpr;
        const centerY = (rect.top + rect.height * 0.5 + offset[1]) * dpr;
        element.runtime.grooveDepth = Number(content.depth) || 7;
        element.runtime.fillOpacity = content.fillOpacity ?? 1;
        return {
          mask,
          color,
          depth,
          blur: (content.rounding ?? 8) * dpr,
          screenRect: [centerX - width * 0.5, centerY - height * 0.5, width, height],
        };
      }

      if (type === "custom" && typeof content.draw === "function") {
        const widthCss = Number(resolveValue(content.width, context)) || rect.width;
        const heightCss = Number(resolveValue(content.height, context)) || rect.height;
        const width = Math.ceil(widthCss * dpr);
        const height = Math.ceil(heightCss * dpr);
        const mask = createCanvas(width, height);
        const color = createCanvas(width, height);
        const depth = content.depthMap ? createCanvas(width, height) : null;
        const position = content.positionMap ? createCanvas(width, height) : null;
        content.draw({
          maskContext: mask.getContext("2d"),
          colorContext: color.getContext("2d"),
          depthContext: depth?.getContext("2d") || null,
          positionContext: position?.getContext("2d") || null,
          width,
          height,
          dpr,
          target: element.target,
        });
        const offset = resolveValue(content.offset, context) || [0, 0];
        const centerX = (rect.left + rect.width * 0.5 + offset[0]) * dpr;
        const centerY = (rect.top + rect.height * 0.5 + offset[1]) * dpr;
        element.runtime.grooveDepth = Number(content.depth) || 7;
        element.runtime.fillOpacity = content.fillOpacity ?? 1;
        return {
          mask,
          color,
          depth,
          position,
          blur: (content.rounding ?? 8) * dpr,
          screenRect: [centerX - width * 0.5, centerY - height * 0.5, width, height],
        };
      }

      return null;
    }

    _createFieldCanvas(mask, blurRadius, depthMap = null, positionMap = null) {
      const width = mask.width;
      const height = mask.height;
      const blur = createCanvas(width, height);
      const blurContext = blur.getContext("2d");
      blurContext.filter = `blur(${Math.max(1, blurRadius)}px)`;
      blurContext.drawImage(mask, 0, 0);
      blurContext.filter = "none";

      const maskData = mask.getContext("2d").getImageData(0, 0, width, height);
      const blurData = blurContext.getImageData(0, 0, width, height);
      const depthData = depthMap
        ? depthMap.getContext("2d").getImageData(0, 0, width, height).data
        : null;
      const positionData = positionMap
        ? positionMap.getContext("2d").getImageData(0, 0, width, height).data
        : null;
      const field = createCanvas(width, height);
      const fieldContext = field.getContext("2d");
      const output = fieldContext.createImageData(width, height);
      for (let index = 0; index < output.data.length; index += 4) {
        output.data[index] = maskData.data[index + 3];
        output.data[index + 1] = clamp(blurData.data[index + 3] * 1.22, 0, 255);
        const automaticDepth = clamp(blurData.data[index + 3] * 1.22, 0, 255);
        const paintedDepth = depthData
          ? (depthData[index] * 0.2126
            + depthData[index + 1] * 0.7152
            + depthData[index + 2] * 0.0722)
            * (depthData[index + 3] / 255)
          : automaticDepth;
        output.data[index + 2] = Math.round(clamp(paintedDepth, 0, 255));
        const paintedPosition = positionData
          ? (positionData[index] * 0.2126
            + positionData[index + 1] * 0.7152
            + positionData[index + 2] * 0.0722)
            * (positionData[index + 3] / 255)
          : 128;
        output.data[index + 3] = Math.round(clamp(paintedPosition, 0, 255));
      }
      fieldContext.putImageData(output, 0, 0);
      return field;
    }

    _roundShapeMask(mask, radius) {
      const amount = clamp(Number(radius) || 0, 0, Math.min(mask.width, mask.height) * 0.12);
      if (amount < 0.5) return mask;
      const blurred = createCanvas(mask.width, mask.height);
      const blurredContext = blurred.getContext("2d");
      blurredContext.filter = `blur(${amount}px)`;
      blurredContext.drawImage(mask, 0, 0);
      blurredContext.filter = "none";
      const source = blurredContext.getImageData(0, 0, mask.width, mask.height);
      const rounded = createCanvas(mask.width, mask.height);
      const roundedContext = rounded.getContext("2d");
      const output = roundedContext.createImageData(mask.width, mask.height);
      for (let index = 0; index < output.data.length; index += 4) {
        const alpha = source.data[index + 3] / 255;
        const t = clamp((alpha - 0.36) / 0.28, 0, 1);
        const smooth = t * t * (3 - 2 * t);
        output.data[index] = 255;
        output.data[index + 1] = 255;
        output.data[index + 2] = 255;
        output.data[index + 3] = Math.round(smooth * 255);
      }
      roundedContext.putImageData(output, 0, 0);
      return rounded;
    }

    _renderShape(element, rect, dpr) {
      const maskSpecification = element.shape.mask;
      const draw = typeof maskSpecification === "function"
        ? maskSpecification
        : maskSpecification?.draw;
      if (typeof draw !== "function") return null;

      const width = Math.max(2, Math.ceil(rect.width * dpr));
      const height = Math.max(2, Math.ceil(rect.height * dpr));
      const mask = createCanvas(width, height);
      const context = mask.getContext("2d");
      draw({
        context,
        width,
        height,
        dpr,
        target: element.target,
        element,
      });
      const roundedMask = this._roundShapeMask(
        mask,
        (element.shape.rounding ?? 5.5) * dpr,
      );

      const bevel = Math.min(
        Math.min(width, height) * element.shape.bevel,
        176 * dpr,
      );
      const range = Math.max(84 * dpr, bevel * 1.18);
      element.runtime.shapeRange = range / dpr;
      return {
        field: this._createSignedDistanceCanvas(roundedMask, range, bevel),
        width,
        height,
      };
    }

    _chamferDistance(alpha, width, height, sourceInside) {
      const size = width * height;
      const distance = new Float32Array(size);
      const infinity = width + height + 1;
      const diagonal = Math.SQRT2;

      for (let index = 0; index < size; index += 1) {
        const inside = alpha[index] >= 128;
        // The mask canvas ends in transparent pixels, even when its edge is filled.
        const x = index % width;
        const y = Math.floor(index / width);
        distance[index] = inside === sourceInside ? 0
          : !sourceInside ? Math.min(x + 1, width - x, y + 1, height - y)
          : infinity;
      }

      for (let y = 0; y < height; y += 1) {
        for (let x = 0; x < width; x += 1) {
          const index = y * width + x;
          let value = distance[index];
          if (x > 0) value = Math.min(value, distance[index - 1] + 1);
          if (y > 0) value = Math.min(value, distance[index - width] + 1);
          if (x > 0 && y > 0) {
            value = Math.min(value, distance[index - width - 1] + diagonal);
          }
          if (x + 1 < width && y > 0) {
            value = Math.min(value, distance[index - width + 1] + diagonal);
          }
          distance[index] = value;
        }
      }

      for (let y = height - 1; y >= 0; y -= 1) {
        for (let x = width - 1; x >= 0; x -= 1) {
          const index = y * width + x;
          let value = distance[index];
          if (x + 1 < width) value = Math.min(value, distance[index + 1] + 1);
          if (y + 1 < height) value = Math.min(value, distance[index + width] + 1);
          if (x + 1 < width && y + 1 < height) {
            value = Math.min(value, distance[index + width + 1] + diagonal);
          }
          if (x > 0 && y + 1 < height) {
            value = Math.min(value, distance[index + width - 1] + diagonal);
          }
          distance[index] = value;
        }
      }

      return distance;
    }

    _createSignedDistanceCanvas(mask, range, bevel) {
      const width = mask.width;
      const height = mask.height;
      const source = mask.getContext("2d").getImageData(0, 0, width, height);
      const alpha = new Uint8ClampedArray(width * height);
      for (let index = 0; index < alpha.length; index += 1) {
        alpha[index] = source.data[index * 4 + 3];
      }

      const distanceToInside = this._chamferDistance(alpha, width, height, true);
      const distanceToOutside = this._chamferDistance(alpha, width, height, false);
      const signedDistances = new Float32Array(alpha.length);
      const rawHeight = createCanvas(width, height);
      const rawHeightContext = rawHeight.getContext("2d");
      const rawHeightImage = rawHeightContext.createImageData(width, height);

      for (let index = 0; index < alpha.length; index += 1) {
        const inside = alpha[index] >= 128;
        let signedDistance = inside
          ? -distanceToOutside[index]
          : distanceToInside[index];
        signedDistance += 0.5 - alpha[index] / 255;
        signedDistances[index] = signedDistance;
        const depth = clamp(-signedDistance / Math.max(bevel, 1), 0, 1);
        const profile = Math.sqrt(Math.max(depth * (2 - depth), 0));
        const value = Math.round(profile * 255);
        const offset = index * 4;
        rawHeightImage.data[offset] = value;
        rawHeightImage.data[offset + 1] = value;
        rawHeightImage.data[offset + 2] = value;
        rawHeightImage.data[offset + 3] = 255;
      }
      rawHeightContext.putImageData(rawHeightImage, 0, 0);

      const smoothHeight = createCanvas(width, height);
      const smoothHeightContext = smoothHeight.getContext("2d");
      const smoothingRadius = clamp(bevel * 0.46, 8, 48);
      smoothHeightContext.filter = `blur(${smoothingRadius}px)`;
      smoothHeightContext.drawImage(rawHeight, 0, 0);
      smoothHeightContext.filter = "none";
      const smoothHeightData = smoothHeightContext.getImageData(
        0, 0, width, height,
      ).data;

      const field = createCanvas(width, height);
      const fieldContext = field.getContext("2d");
      const output = fieldContext.createImageData(width, height);

      for (let index = 0; index < alpha.length; index += 1) {
        const signedDistance = signedDistances[index];
        const encoded = clamp(0.5 - signedDistance / (range * 2), 0, 1);
        const offset = index * 4;
        const edgeGate = clamp(0.5 - signedDistance * 0.5, 0, 1);
        const raw = rawHeightImage.data[offset] / 255;
        const smooth = smoothHeightData[offset] / 255;
        const relaxedHeight = (raw * 0.02 + smooth * 0.98) * edgeGate;
        output.data[offset] = Math.round(encoded * 255);
        output.data[offset + 1] = Math.round(relaxedHeight * 255);
        output.data[offset + 2] = rawHeightImage.data[offset];
        output.data[offset + 3] = 255;
      }

      fieldContext.putImageData(output, 0, 0);
      return field;
    }

    _packAtlas(items) {
      if (!items.length) return null;
      const maxTextureSize = this.gl.getParameter(this.gl.MAX_TEXTURE_SIZE);
      const totalArea = items.reduce((sum, item) => sum + item.width * item.height, 0);
      const widest = Math.max(...items.map((item) => item.width + 4));
      let width = nextPowerOfTwo(Math.max(widest, Math.sqrt(totalArea) * 1.25));
      width = Math.min(width, maxTextureSize);

      const place = (atlasWidth) => {
        let x = 2;
        let y = 2;
        let rowHeight = 0;
        const placements = [];
        for (const item of items) {
          if (x + item.width + 2 > atlasWidth) {
            x = 2;
            y += rowHeight + 4;
            rowHeight = 0;
          }
          placements.push({ item, x, y });
          x += item.width + 4;
          rowHeight = Math.max(rowHeight, item.height);
        }
        return { placements, height: y + rowHeight + 2 };
      };

      let packed = place(width);
      while (packed.height > maxTextureSize && width < maxTextureSize) {
        width = Math.min(maxTextureSize, width * 2);
        packed = place(width);
      }
      const height = Math.min(maxTextureSize, nextPowerOfTwo(packed.height));
      return { width, height, ...packed };
    }

    _rebuildAtlas(dpr) {
      const canvasRect = this.canvas.getBoundingClientRect();
      const renderedContent = this.elements.map((element) => {
        const rect = this._toCanvasRect(
          element.target.getBoundingClientRect(),
          canvasRect,
        );
        const content = this._renderContent(element, rect, dpr);
        if (!content) {
          element.runtime.contentScreen = [0, 0, 1, 1];
          element.runtime.contentLocal = [0, 0, 1, 1];
          element.runtime.contentAtlas = [0, 0, 1, 1];
          element.runtime.grooveDepth = 0;
          element.runtime.fillOpacity = 0;
          return null;
        }
        const field = this._createFieldCanvas(
          content.mask,
          content.blur,
          content.depth,
          content.position,
        );
        return {
          element,
          field,
          color: content.color,
          width: field.width,
          height: field.height,
          screenRect: content.screenRect,
          baseRect: rect,
        };
      }).filter(Boolean);

      const gl = this.gl;
      gl.pixelStorei(gl.UNPACK_FLIP_Y_WEBGL, false);
      gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, false);

      const contentPacked = this._packAtlas(renderedContent);
      if (contentPacked) {
        const fieldAtlas = createCanvas(contentPacked.width, contentPacked.height);
        const colorAtlas = createCanvas(contentPacked.width, contentPacked.height);
        const fieldContext = fieldAtlas.getContext("2d");
        const colorContext = colorAtlas.getContext("2d");
        for (const placement of contentPacked.placements) {
          const { item, x, y } = placement;
          fieldContext.drawImage(item.field, x, y);
          colorContext.drawImage(item.color, x, y);
          item.element.runtime.contentScreen = item.screenRect;
          item.element.runtime.contentLocal = [
            item.screenRect[0] - item.baseRect.left * dpr,
            item.screenRect[1] - item.baseRect.top * dpr,
            item.screenRect[2],
            item.screenRect[3],
          ];
          item.element.runtime.contentAtlas = [
            x / contentPacked.width,
            y / contentPacked.height,
            item.width / contentPacked.width,
            item.height / contentPacked.height,
          ];
        }
        gl.bindTexture(gl.TEXTURE_2D, this.fieldTexture);
        gl.texImage2D(
          gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA,
          gl.UNSIGNED_BYTE, fieldAtlas,
        );
        gl.bindTexture(gl.TEXTURE_2D, this.colorTexture);
        gl.texImage2D(
          gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA,
          gl.UNSIGNED_BYTE, colorAtlas,
        );
      } else {
        const empty = new Uint8Array([0, 0, 0, 0]);
        for (const texture of [this.fieldTexture, this.colorTexture]) {
          gl.bindTexture(gl.TEXTURE_2D, texture);
          gl.texImage2D(
            gl.TEXTURE_2D, 0, gl.RGBA, 1, 1, 0,
            gl.RGBA, gl.UNSIGNED_BYTE, empty,
          );
        }
      }

      const renderedShapes = this.elements.map((element) => {
        const rect = this._toCanvasRect(
          element.target.getBoundingClientRect(),
          canvasRect,
        );
        const shape = this._renderShape(element, rect, dpr);
        if (!shape) {
          element.runtime.shapeAtlas = [0, 0, 1, 1];
          element.runtime.shapeRange = 1;
          return null;
        }
        return { element, ...shape };
      }).filter(Boolean);
      const shapePacked = this._packAtlas(renderedShapes);
      if (shapePacked) {
        const shapeAtlas = createCanvas(shapePacked.width, shapePacked.height);
        const shapeContext = shapeAtlas.getContext("2d");
        for (const placement of shapePacked.placements) {
          const { item, x, y } = placement;
          shapeContext.drawImage(item.field, x, y);
          item.element.runtime.shapeAtlas = [
            x / shapePacked.width,
            y / shapePacked.height,
            item.width / shapePacked.width,
            item.height / shapePacked.height,
          ];
        }
        gl.bindTexture(gl.TEXTURE_2D, this.shapeTexture);
        gl.texImage2D(
          gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA,
          gl.UNSIGNED_BYTE, shapeAtlas,
        );
      } else {
        gl.bindTexture(gl.TEXTURE_2D, this.shapeTexture);
        gl.texImage2D(
          gl.TEXTURE_2D, 0, gl.RGBA, 1, 1, 0,
          gl.RGBA, gl.UNSIGNED_BYTE, new Uint8Array([0, 0, 0, 0]),
        );
      }

      this.atlasDirty = false;
    }

    _updateElementState(element, easing) {
      const state = element.state;
      state.hover += (state.hoverTarget - state.hover) * easing;
      state.pressed += (state.pressedTarget - state.pressed) * easing;
      state.pointerX += (state.pointerTargetX - state.pointerX) * easing;
      state.pointerY += (state.pointerTargetY - state.pointerY) * easing;
    }

    _updateDeviceTilt(easing) {
      const tilt = this.deviceTilt;
      tilt.x += (tilt.targetX - tilt.x) * easing;
      tilt.y += (tilt.targetY - tilt.y) * easing;
      tilt.amount += (tilt.targetAmount - tilt.amount) * easing;
    }

    _composeElementState(element, reducedMotion = false) {
      const state = element.state;
      if (reducedMotion) {
        const composed = {
          hover: 0,
          pressed: 0,
          pointerX: 0,
          pointerY: 0,
        };
        element.runtime.composedState = composed;
        return composed;
      }
      const motion = element.motion || {};
      const acceptsSensor = motion.mode === "group" && !element.runtime.motionParent;
      const sensorAmount = reducedMotion || !acceptsSensor ? 0 : this.deviceTilt.amount;
      const pointerWeight = state.hover;
      const sensorWeight = (1 - pointerWeight) * sensorAmount;
      const totalWeight = pointerWeight + sensorWeight;
      const seedResponse = 0.98 + (Math.sin(element.seed * 2.17) * 0.5 + 0.5) * 0.04;
      const sensorX = this.deviceTilt.x * seedResponse;
      const sensorY = this.deviceTilt.y * seedResponse;
      const pointerX = totalWeight > 0.0001
        ? (state.pointerX * pointerWeight
          + sensorX * sensorWeight) / totalWeight
        : state.pointerX;
      const pointerY = totalWeight > 0.0001
        ? (state.pointerY * pointerWeight
          + sensorY * sensorWeight) / totalWeight
        : state.pointerY;
      const composed = {
        hover: clamp(totalWeight, 0, 1),
        pressed: state.pressed,
        pointerX: clamp(pointerX, -1, 1),
        pointerY: clamp(pointerY, -1, 1),
      };
      element.runtime.composedState = composed;
      return composed;
    }

    _setElementUniforms(
      element,
      uniforms,
      dpr,
      layer = {},
      layerIndex = 0,
      baseRect = this._toCanvasRect(element.target.getBoundingClientRect()),
    ) {
      const gl = this.gl;
      const offset = Array.isArray(layer.offset) ? layer.offset : [0, 0];
      const scaleValue = layer.scale ?? 1;
      const scale = Array.isArray(scaleValue) ? scaleValue : [scaleValue, scaleValue];
      const state = element.runtime.composedState || element.state;
      const layerMotion = layer.motion || {};
      const layerCenter = (element.layers.length - 1) * 0.5;
      const layerPosition = element.layers.length > 1
        ? (layerIndex - layerCenter) / Math.max(layerCenter, 1)
        : 0;
      const interaction = Math.max(0, Number(element.interaction) || 0);
      const separation = Number(
        layerMotion.separation ?? element.motion?.layerSeparation,
      ) || 0;
      const response = Math.max(0, Number(
        layerMotion.response ?? (1 + layerPosition * 0.12),
      ) || 0);
      const interactiveShiftX = state.pointerX * state.hover * interaction
        * separation * layerPosition;
      const interactiveShiftY = state.pointerY * state.hover * interaction
        * separation * layerPosition * 0.72;
      const interactiveScale = 1 + state.hover * interaction * layerPosition * 0.0024;
      const rect = {
        left: baseRect.left + (offset[0] || 0) + interactiveShiftX
          + baseRect.width * (1 - scale[0] * interactiveScale) * 0.5,
        top: baseRect.top + (offset[1] || 0) + interactiveShiftY
          + baseRect.height * (1 - scale[1] * interactiveScale) * 0.5,
        width: baseRect.width * scale[0] * interactiveScale,
        height: baseRect.height * scale[1] * interactiveScale,
      };
      const shape = { ...element.shape, ...(layer.shape || {}) };
      const optics = { ...element.optics, ...(layer.optics || {}) };
      const style = { ...element.style, ...(layer.style || {}) };
      const surface = { ...element.surface, ...(layer.surface || {}) };

      gl.uniform4f(uniforms.rect, rect.left * dpr, rect.top * dpr, rect.width * dpr, rect.height * dpr);
      gl.uniform4f(
        uniforms.state,
        state.hover,
        state.pressed,
        state.pointerX * response,
        state.pointerY * response,
      );
      gl.uniform4f(
        uniforms.shape,
        shape.mask ? -1 : shape.radius,
        shape.mask ? element.runtime.shapeRange : shape.radiusMax,
        shape.bevel,
        shape.irregularity,
      );
      gl.uniform4f(
        uniforms.optics,
        optics.ior,
        optics.refraction,
        optics.roughness,
        optics.depth,
      );
      const tint = Array.isArray(style.tint) ? style.tint : [0.82, 0.94, 1];
      gl.uniform4f(uniforms.style, tint[0], tint[1], tint[2], style.matte);
      gl.uniform4f(uniforms.styleExtra, style.tintStrength ?? 0.35, 0, 0, 0);
      gl.uniform4f(
        uniforms.surface,
        surface.texture,
        surface.scale,
        surface.grain,
        surface.rear,
      );
      const scaleX = rect.width / Math.max(baseRect.width, 1);
      const scaleY = rect.height / Math.max(baseRect.height, 1);
      const contentLocal = element.runtime.contentLocal || [
        element.runtime.contentScreen[0] - baseRect.left * dpr,
        element.runtime.contentScreen[1] - baseRect.top * dpr,
        element.runtime.contentScreen[2],
        element.runtime.contentScreen[3],
      ];
      const contentScreen = [
        rect.left * dpr + contentLocal[0] * scaleX,
        rect.top * dpr + contentLocal[1] * scaleY,
        contentLocal[2] * scaleX,
        contentLocal[3] * scaleY,
      ];
      gl.uniform4fv(uniforms.contentScreen, contentScreen);
      gl.uniform4fv(uniforms.contentAtlas, element.runtime.contentAtlas);
      gl.uniform4fv(uniforms.shapeAtlas, element.runtime.shapeAtlas);
      const contentVisible = layer.content ?? (
        element.layers.length === 1 || layerIndex === element.layers.length - 1
      );
      const volume = contentVisible ? element.content?.volume : null;
      const volumeDensity = volume && Number.isFinite(Number(volume.density))
        ? Math.max(0, Number(volume.density))
        : element.runtime.fillOpacity;
      gl.uniform4f(
        uniforms.contentOpts,
        contentVisible ? element.runtime.grooveDepth : 0,
        contentVisible ? volumeDensity : 0,
        layer.seed ?? element.seed + layerIndex * 0.83,
        layer.interaction ?? element.interaction,
      );
      gl.uniform4f(
        uniforms.volumeOpts,
        volume ? Number(volume.thickness) || 0 : 0,
        volume ? Number(volume.position) || 0 : 0,
        volume ? Number(volume.scatter) || 0 : 0,
        volume ? Number(volume.facets) || 0 : 0,
      );
      gl.uniform4f(
        uniforms.volumeStyle,
        volume ? Number(volume.parallax) || 0 : 0,
        volume ? Number(volume.grain) || 0 : 0,
        volume ? Number(volume.glow) || 0 : 0,
        volume ? Number(volume.dispersion) || 0 : 0,
      );
      return { rect, optics };
    }

    _isElementPaintable(target) {
      if (typeof target.checkVisibility === "function") {
        return target.checkVisibility({ visibilityProperty: true, contentVisibilityAuto: true });
      }
      const ownStyle = getComputedStyle(target);
      if (/^(hidden|collapse)$/.test(ownStyle.visibility)) return false;
      for (let ancestor = target; ancestor; ancestor = ancestor.parentElement) {
        const style = ancestor === target ? ownStyle : getComputedStyle(ancestor);
        if (style.display === "none") return false;
        // A skipped subtree hides its contents, not the ancestor's own box.
        if (ancestor !== target) {
          if (style.contentVisibility === "hidden") return false;
          if (ancestor.tagName === "DETAILS" && !ancestor.open) {
            const summary = Array.from(ancestor.children).find((child) => child.tagName === "SUMMARY");
            if (!summary?.contains(target)) return false;
          }
        }
      }
      return true;
    }

    _resolveAncestorClip(target, canvasRect, frameCache) {
      const clip = {
        left: 0,
        top: 0,
        right: canvasRect.width,
        bottom: canvasRect.height,
      };
      let ancestor = target.parentElement;
      while (ancestor && ancestor !== document.documentElement) {
        let cached = frameCache.get(ancestor);
        if (!cached) {
          const style = getComputedStyle(ancestor);
          const rect = ancestor.getBoundingClientRect();
          cached = {
            clipsX: /^(auto|hidden|clip|scroll)$/.test(style.overflowX),
            clipsY: /^(auto|hidden|clip|scroll)$/.test(style.overflowY),
            left: rect.left + ancestor.clientLeft - canvasRect.left,
            top: rect.top + ancestor.clientTop - canvasRect.top,
            width: ancestor.clientWidth,
            height: ancestor.clientHeight,
          };
          frameCache.set(ancestor, cached);
        }
        if (cached.clipsX) {
          clip.left = Math.max(clip.left, cached.left);
          clip.right = Math.min(clip.right, cached.left + cached.width);
        }
        if (cached.clipsY) {
          clip.top = Math.max(clip.top, cached.top);
          clip.bottom = Math.min(clip.bottom, cached.top + cached.height);
        }
        if (clip.right <= clip.left || clip.bottom <= clip.top) return null;
        ancestor = ancestor.parentElement;
      }
      return clip;
    }

    _scissorLayer(rect, optics, dpr, clipRect = null) {
      const gl = this.gl;
      const shadowSigma = 22 + Math.min(optics.depth, 240) * 0.18;
      const maximumInteractiveOffset = 34;
      const shadowReach = shadowSigma * 4.2 + maximumInteractiveOffset;
      const opticalReach = Math.max(96, shadowReach) * dpr;
      let left = Math.floor(rect.left * dpr - opticalReach);
      let right = Math.ceil((rect.left + rect.width) * dpr + opticalReach);
      let top = Math.floor(rect.top * dpr - opticalReach);
      let bottom = Math.ceil((rect.top + rect.height) * dpr + opticalReach);
      if (clipRect) {
        left = Math.max(left, Math.floor(clipRect.left * dpr));
        right = Math.min(right, Math.ceil(clipRect.right * dpr));
        top = Math.max(top, Math.floor(clipRect.top * dpr));
        bottom = Math.min(bottom, Math.ceil(clipRect.bottom * dpr));
      }
      const x = clamp(left, 0, this.canvas.width);
      const y = clamp(this.canvas.height - bottom, 0, this.canvas.height);
      const width = clamp(right, 0, this.canvas.width) - x;
      const height = clamp(this.canvas.height - top, 0, this.canvas.height) - y;
      if (width <= 0 || height <= 0) return null;
      gl.scissor(x, y, width, height);
      return { x, y, width, height, pixels: width * height };
    }

    _render(timestamp) {
      if (!this.running) return;
      if (
        this.fpsCap > 0
        && this.lastRenderTimestamp > 0
        && timestamp - this.lastRenderTimestamp < 1000 / this.fpsCap - 0.5
      ) {
        this.raf = requestAnimationFrame((time) => this._render(time));
        return;
      }
      this.lastRenderTimestamp = timestamp;
      const gl = this.gl;
      if (this.metrics.lastTimestamp > 0) {
        const delta = Math.min(timestamp - this.metrics.lastTimestamp, 100);
        this.metrics.sampleTime += delta;
        this.metrics.sampleFrames += 1;
        if (this.metrics.sampleTime >= 500) {
          this.metrics.frameTime = this.metrics.sampleTime / this.metrics.sampleFrames;
          this.metrics.fps = 1000 / this.metrics.frameTime;
          this.metrics.sampleTime = 0;
          this.metrics.sampleFrames = 0;
        }
      }
      this.metrics.lastTimestamp = timestamp;
      const dpr = this._resize();
      if (this.programDirty) this._buildProgram();
      if (this.atlasDirty) this._rebuildAtlas(dpr);

      gl.useProgram(this.program);
      gl.bindBuffer(gl.ARRAY_BUFFER, this.positionBuffer);
      gl.activeTexture(gl.TEXTURE0);
      gl.bindTexture(gl.TEXTURE_2D, this.backgroundTexture);
      gl.activeTexture(gl.TEXTURE1);
      gl.bindTexture(gl.TEXTURE_2D, this.fieldTexture);
      gl.activeTexture(gl.TEXTURE2);
      gl.bindTexture(gl.TEXTURE_2D, this.colorTexture);
      gl.activeTexture(gl.TEXTURE3);
      gl.bindTexture(gl.TEXTURE_2D, this.shapeTexture);
      gl.uniform2f(this.uniforms.resolution, this.canvas.width, this.canvas.height);
      gl.uniform2f(
        this.uniforms.imageSize,
        ...imageDimensions(this.backgroundImage),
      );
      gl.uniform1f(this.uniforms.dpr, dpr);
      gl.uniform1f(this.uniforms.time, timestamp * 0.001);
      gl.uniform1f(this.uniforms.textureStrength, this.textureStrength);

      const reducedMotion = this.reducedMotionQuery.matches;
      this._updateDeviceTilt(reducedMotion ? 1 : 0.075);
      this.elements.forEach((element) => {
        const easing = reducedMotion
          ? 1
          : clamp(Number(element.motion?.inertia) || 0.13, 0.025, 1);
        this._updateElementState(element, easing);
        this._composeElementState(element, reducedMotion);
        this._applyLiveMotion(element, reducedMotion);
      });

      if (this.backgroundDirty) {
        gl.disable(gl.SCISSOR_TEST);
        gl.bindFramebuffer(gl.FRAMEBUFFER, this.linearBackgroundFramebuffer);
        gl.uniform1i(this.uniforms.activeElement, -1);
        gl.drawArrays(gl.TRIANGLES, 0, 3);
        this.backgroundDirty = false;
      }

      // Both ping-pong targets start from the same linear background. Each
      // following pass only copies the region changed by the previous pass,
      // so a long list of small controls never performs N full-screen blits.
      gl.disable(gl.SCISSOR_TEST);
      gl.bindFramebuffer(gl.READ_FRAMEBUFFER, this.linearBackgroundFramebuffer);
      this.sceneFramebuffers.forEach((framebuffer) => {
        gl.bindFramebuffer(gl.DRAW_FRAMEBUFFER, framebuffer);
        gl.blitFramebuffer(
          0,
          0,
          this.canvas.width,
          this.canvas.height,
          0,
          0,
          this.canvas.width,
          this.canvas.height,
          gl.COLOR_BUFFER_BIT,
          gl.NEAREST,
        );
      });

      let sourceTexture = this.linearBackgroundTexture;
      let sourceFramebuffer = this.linearBackgroundFramebuffer;
      let opticalPasses = 0;
      let renderedPasses = 0;
      let previousRegion = null;
      let shadedPixels = 0;
      const canvasRect = this.canvas.getBoundingClientRect();
      const ancestorClipCache = new WeakMap();
      this.elements.forEach((element, elementIndex) => {
        if (!this._isElementPaintable(element.target)) return;
        const baseRect = this._toCanvasRect(
          element.target.getBoundingClientRect(),
          canvasRect,
        );
        if (baseRect.width < 0.5 || baseRect.height < 0.5) return;
        const ancestorClip = this._resolveAncestorClip(
          element.target,
          canvasRect,
          ancestorClipCache,
        );
        if (!ancestorClip) return;
        element.layers.forEach((layer, layerIndex) => {
          const targetIndex = renderedPasses % 2;
          gl.uniform1i(this.uniforms.activeElement, 0);
          const material = this._setElementUniforms(
            element,
            this.uniforms.element,
            dpr,
            layer,
            layerIndex,
            baseRect,
          );
          gl.enable(gl.SCISSOR_TEST);
          const region = this._scissorLayer(
            material.rect,
            material.optics,
            dpr,
            ancestorClip,
          );
          opticalPasses += 1;
          if (!region) return;

          gl.disable(gl.SCISSOR_TEST);
          if (previousRegion) {
            gl.bindFramebuffer(gl.READ_FRAMEBUFFER, sourceFramebuffer);
            gl.bindFramebuffer(gl.DRAW_FRAMEBUFFER, this.sceneFramebuffers[targetIndex]);
            gl.blitFramebuffer(
              previousRegion.x,
              previousRegion.y,
              previousRegion.x + previousRegion.width,
              previousRegion.y + previousRegion.height,
              previousRegion.x,
              previousRegion.y,
              previousRegion.x + previousRegion.width,
              previousRegion.y + previousRegion.height,
              gl.COLOR_BUFFER_BIT,
              gl.NEAREST,
            );
          }

          gl.bindFramebuffer(gl.FRAMEBUFFER, this.sceneFramebuffers[targetIndex]);
          gl.activeTexture(gl.TEXTURE4);
          gl.bindTexture(gl.TEXTURE_2D, sourceTexture);
          gl.enable(gl.SCISSOR_TEST);
          gl.scissor(region.x, region.y, region.width, region.height);
          gl.drawArrays(gl.TRIANGLES, 0, 3);
          shadedPixels += region.pixels;
          renderedPasses += 1;
          previousRegion = region;
          sourceTexture = this.sceneTextures[targetIndex];
          sourceFramebuffer = this.sceneFramebuffers[targetIndex];
        });
      });

      gl.disable(gl.SCISSOR_TEST);
      gl.bindFramebuffer(gl.FRAMEBUFFER, null);
      gl.activeTexture(gl.TEXTURE4);
      gl.bindTexture(gl.TEXTURE_2D, sourceTexture);
      gl.uniform1i(this.uniforms.activeElement, -2);
      gl.drawArrays(gl.TRIANGLES, 0, 3);
      this.metrics.opticalPasses = opticalPasses;
      this.metrics.shadedPixels = shadedPixels;
      this.raf = requestAnimationFrame((time) => this._render(time));
    }
  }

  return Object.freeze({
    create: (options) => new OpticalGlassScene(options),
    presets: BUILT_IN_PRESETS,
    volumeDefaults: VOLUME_ENGRAVING_DEFAULTS,
    live: (source, options = {}) => ({
      type: "live",
      source,
      safeArea: "auto",
      ...options,
    }),
    text: (text, options = {}) => ({ type: "text", text, ...options }),
    image: (source, options = {}) => ({ type: "image", source, ...options }),
    custom: (draw, options = {}) => ({ type: "custom", draw, ...options }),
    volume: (content, options = {}) => {
      if (!content || !["text", "image", "custom"].includes(content.type)) {
        throw new Error("OpticalGlass: volume() expects text(), image() or custom() content");
      }
      if (!options || typeof options !== "object" || Array.isArray(options)) {
        throw new Error("OpticalGlass: volume engraving options must be an object");
      }
      return {
        ...content,
        volume: { ...VOLUME_ENGRAVING_DEFAULTS, ...options },
      };
    },
    volumeText: (text, { volume = {}, ...options } = {}) => ({
      type: "text",
      text,
      ...options,
      volume: { ...VOLUME_ENGRAVING_DEFAULTS, ...volume },
    }),
    shape: (draw, options = {}) => ({ ...options, mask: { draw } }),
    path: (
      pathData,
      {
        viewBox = [0, 0, 100, 100],
        padding = 0.06,
        fillRule = "nonzero",
        ...options
      } = {},
    ) => ({
      ...options,
      mask: {
        draw: ({ context, width, height }) => {
          const [viewX, viewY, viewWidth, viewHeight] = viewBox;
          const scale = Math.min(
            width * (1 - padding * 2) / viewWidth,
            height * (1 - padding * 2) / viewHeight,
          );
          context.save();
          context.translate(
            (width - viewWidth * scale) * 0.5 - viewX * scale,
            (height - viewHeight * scale) * 0.5 - viewY * scale,
          );
          context.scale(scale, scale);
          context.fillStyle = "white";
          context.fill(new Path2D(pathData), fillRule);
          context.restore();
        },
      },
    }),
    bend: (target, { shape, optics, ...options } = {}) => ({
      target,
      ...options,
      shape: {
        radius: 0.5,
        radiusMax: 999,
        bevel: 0.48,
        irregularity: 0.75,
        ...(shape || {}),
      },
      optics: {
        depth: 24,
        refraction: 1.25,
        ...(optics || {}),
      },
    }),
  });
})();

const LiquidGlass = OpticalGlass;
if (typeof window !== "undefined") {
  window.OpticalGlass = OpticalGlass;
  window.LiquidGlass ||= OpticalGlass;
}

export { OpticalGlass, LiquidGlass };
export default OpticalGlass;
