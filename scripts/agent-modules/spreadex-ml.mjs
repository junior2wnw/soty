export const spreadExMlSchema = "soty.spreadex-ml.v1";
export const spreadExMlReleaseSchema = "soty.spreadex-ml.release.v1";

const profiles = new Set(["careful", "balanced", "opportunity"]);
const modes = new Set(["off", "assistant", "gate"]);
const requiredDeviceScopes = ["heartbeat", "predictions", "settings:read"];

export function createSpreadExMlIntegration(deps, options = {}) {
  const required = [
    "exists", "mkdir", "remove", "rename", "writeFile", "readJson", "chmod", "join", "download", "extract",
    "sha256", "verifyRelease", "runFile", "createWorker", "request", "saveState", "saveSecrets", "now", "randomId"
  ];
  for (const name of required) {
    if (typeof deps[name] !== "function") throw new Error(`spreadex-ml dependency required: ${name}`);
  }

  const platformKey = cleanPlatformKey(options.platformKey);
  const rootDir = String(options.rootDir || "");
  const baseUrl = normalizeSpreadExBaseUrl(options.baseUrl);
  const manifestUrl = normalizeManifestUrl(options.manifestUrl);
  if (!rootDir || !platformKey || !baseUrl || !manifestUrl) throw new Error("invalid-spreadex-ml-options");

  let state = normalizeSpreadExState(options.state);
  let secrets = normalizeSpreadExSecrets(options.secrets);
  let active = null;
  let phase = "unavailable";
  let lastError = "component-not-installed";
  let pairedAt = secrets.deviceToken ? state.pairedAt : "";
  let lastHeartbeatAt = "";
  let connection = "disconnected";
  let stopped = false;
  let heartbeatTimer = null;
  let socketTimer = null;
  let socket = null;
  let socketFailures = 0;
  let worker = null;
  let workerPhase = "stopped";
  let allowedModels = [];
  let taskProcessing = null;
  const queuedTasks = [];
  const completedTasks = new Map();
  const pendingPredictions = new Map();
  let updatePromise = null;
  let pairPromise = null;

  return {
    async initialize() {
      await deps.mkdir(rootDir);
      const recovered = await recoverActive();
      if (!recovered) {
        phase = "unavailable";
        lastError = "component-not-installed";
      }
      return this.status();
    },

    status() {
      const componentAvailable = Boolean(active && phase === "ready");
      return {
        ok: true,
        schema: spreadExMlSchema,
        paired: Boolean(secrets.deviceToken),
        pairedAt: secrets.deviceToken ? pairedAt : "",
        deviceId: secrets.deviceToken ? state.deviceId : "",
        scopes: secrets.deviceToken ? state.scopes : [],
        baseOrigin: new URL(baseUrl).origin,
        connection,
        lastHeartbeatAt,
        worker: workerPhase,
        modelAllowed: activeAllowed(),
        allowedModels,
        settings: publicSettings(state.settings),
        component: {
          available: componentAvailable,
          phase,
          version: active?.version || "",
          modelVersion: active?.modelVersion || "",
          featureSchema: active?.featureSchema || "",
          platform: active?.platform || platformKey,
          installedAt: active?.installedAt || "",
          lastError
        },
        ready: Boolean(secrets.deviceToken && state.settings.mode !== "off" && componentAvailable && activeAllowed())
      };
    },

    settings() {
      return { ok: true, schema: spreadExMlSchema, settings: publicSettings(state.settings) };
    },

    async updateSettings(value, source = "local") {
      const next = normalizeSpreadExSettings(value, state.settings);
      next.device_id = state.deviceId;
      const requestedRevision = safeRevision(value?.revision);
      if (source === "remote" && requestedRevision && requestedRevision < state.settings.revision) return this.settings();
      next.revision = requestedRevision || state.settings.revision + 1;
      state = { ...state, settings: next };
      await deps.saveState(state);
      return this.settings();
    },

    async pair(pairCode) {
      if (pairPromise) return await pairPromise;
      pairPromise = performPair(pairCode).finally(() => { pairPromise = null; });
      return await pairPromise;
    },

    async unpair() {
      closeSocket();
      secrets = {};
      state = { ...state, deviceId: "", pairedAt: "", websocketUrl: "", scopes: [] };
      allowedModels = [];
      pairedAt = "";
      connection = "disconnected";
      lastHeartbeatAt = "";
      await deps.saveSecrets(secrets);
      await deps.saveState(state);
      return this.status();
    },

    async syncRelease(release) {
      if (updatePromise) return await updatePromise;
      updatePromise = installRelease(release).finally(() => { updatePromise = null; });
      return await updatePromise;
    },

    async predict(value) {
      if (!active || phase !== "ready") throw new Error("spreadex-ml-unavailable");
      if (state.settings.mode === "off") throw new Error("spreadex-ml-disabled");
      if (!activeAllowed()) throw new Error("spreadex-ml-model-not-allowed");
      const input = normalizePredictionInput(value);
      if (!input) throw new Error("invalid-prediction-input");
      await requestWorker("observe", {
        request: { request_id: input.requestId, observed_at_ms: Date.now() },
        route: input.features
      }, 3_000);
      const response = await requestWorker("predict_batch", { requests: [{ request_id: input.requestId }] }, 5_000);
      const parsed = workerPredictions(response).find((item) => item.requestId === input.requestId || item.request_id === input.requestId);
      if (!parsed || parsed.ok === false || !Number.isFinite(parsed.probability) || parsed.probability < 0 || parsed.probability > 1) {
        throw new Error("spreadex-ml-invalid-prediction");
      }
      return {
        ok: true,
        schema: spreadExMlSchema,
        requestId: input.requestId,
        probability: parsed.probability,
        decision: cleanDecision(parsed.decision),
        reasons: cleanReasons(parsed.reasons),
        modelVersion: active.modelVersion,
        featureSchema: active.featureSchema,
        expiresAtMs: safeExpiresAt(parsed.expiresAtMs),
        outOfDistribution: parsed.outOfDistribution === true
      };
    },

    async processTasks(value) {
      return await processPredictionTasks(Array.isArray(value) ? value : []);
    },

    start() {
      stopped = false;
      scheduleHeartbeat(0);
      scheduleSocket(0);
    },

    stop() {
      stopped = true;
      if (heartbeatTimer) deps.clearTimer?.(heartbeatTimer);
      if (socketTimer) deps.clearTimer?.(socketTimer);
      heartbeatTimer = null;
      socketTimer = null;
      closeSocket();
      stopWorker();
    },

    redact(value) {
      let text = String(value || "");
      if (secrets.deviceToken && secrets.deviceToken.length >= 8) text = text.replaceAll(secrets.deviceToken, "<redacted>");
      return text;
    }
  };

  async function performPair(rawCode) {
    const pairCode = cleanPairCode(rawCode);
    if (!pairCode) throw new Error("invalid-pair-code");
    const result = await deps.request(new URL("/api/ml/agent/enroll", baseUrl).toString(), {
      method: "POST",
      body: {
        code: pairCode,
        name: options.deviceNick,
        device_name: options.deviceNick,
        agent_version: options.runtimeVersion,
        device_id: state.deviceId || options.deviceId,
        device_nick: options.deviceNick,
        platform: platformKey,
        runtime_version: options.runtimeVersion,
        capabilities: ["lightgbm", "settings", "health", "predictions"]
      },
      timeoutMs: 20_000
    });
    const deviceToken = cleanDeviceToken(result?.device_token || result?.token);
    const nextDeviceId = cleanId(result?.device?.id || result?.device_id || state.deviceId || options.deviceId, 180);
    const scopes = cleanScopes(result?.scopes);
    if (!result?.ok || !deviceToken || !nextDeviceId || !exactScopes(scopes, requiredDeviceScopes)) throw new Error("spreadex-pair-rejected");
    const nextPairedAt = deps.now();
    const nextSecrets = { deviceToken };
    const nextState = {
      ...state,
      deviceId: nextDeviceId,
      scopes,
      pairedAt: nextPairedAt,
      websocketUrl: normalizeWebSocketUrl(result.websocket_url, baseUrl),
      settings: normalizeSpreadExSettings({ ...(result.settings || state.settings), device_id: nextDeviceId }, state.settings)
    };
    await deps.saveSecrets(nextSecrets);
    try {
      await deps.saveState(nextState);
    } catch (error) {
      await deps.saveSecrets(secrets).catch(() => undefined);
      throw error;
    }
    secrets = nextSecrets;
    state = nextState;
    allowedModels = normalizeAllowedModels(result.allowed_models);
    pairedAt = nextPairedAt;
    scheduleHeartbeat(0);
    scheduleSocket(0);
    return { ok: true, schema: spreadExMlSchema, status: publicPairStatus() };
  }

  async function installRelease(rawRelease) {
    const release = normalizeSpreadExRelease(rawRelease, platformKey, manifestUrl);
    if (!release) {
      if (!active) {
        phase = "unavailable";
        lastError = rawRelease?.available === false ? "component-not-published" : "component-release-invalid";
      }
      return { ok: false, schema: spreadExMlSchema, error: lastError };
    }
    if (!deps.verifyRelease(release.signedPayload, release.signature)) {
      if (!active) phase = "unavailable";
      lastError = "component-signature-invalid";
      return { ok: false, schema: spreadExMlSchema, error: lastError };
    }
    if (active?.archiveSha256 === release.sha256 && active?.version === release.version) return { ok: true, schema: spreadExMlSchema, component: publicComponent() };

    phase = "installing";
    lastError = "";
    const releasesDir = deps.join(rootDir, "releases");
    const releaseId = `${release.version}-${release.sha256.slice(0, 12)}-${platformKey}`;
    const installDir = deps.join(releasesDir, releaseId);
    const stageDir = `${installDir}.stage-${deps.randomId()}`;
    const displacedDir = `${installDir}.rollback-${deps.randomId()}`;
    const archivePath = deps.join(rootDir, `${releaseId}.${deps.randomId()}${release.archive === "zip" ? ".zip" : ".tar.gz"}`);
    let displaced = false;
    const previous = active;
    try {
      await deps.mkdir(releasesDir);
      await deps.remove(stageDir);
      await deps.mkdir(stageDir);
      const bytes = await deps.download(release.url, release.maxBytes);
      if (deps.sha256(bytes) !== release.sha256) throw new Error("component-sha256-mismatch");
      await deps.writeFile(archivePath, bytes);
      await deps.extract(archivePath, stageDir, release.archive);
      const executable = deps.join(stageDir, release.executable);
      if (!deps.exists(executable)) throw new Error("component-executable-missing");
      await deps.chmod(executable, 0o755).catch(() => undefined);
      const selfTest = parseWorkerJson(await deps.runFile(executable, release.selfTestArgs, 20_000, undefined, stageDir));
      if (!validSelfTest(selfTest, release)) throw new Error("component-self-test-failed");
      const receipt = {
        schema: spreadExMlSchema,
        version: release.version,
        modelVersion: release.modelVersion,
        featureSchema: release.featureSchema,
        platform: platformKey,
        archiveSha256: release.sha256,
        executable: release.executable,
        selfTestArgs: release.selfTestArgs,
        workerArgs: release.workerArgs,
        predictArgs: release.predictArgs,
        installedAt: deps.now()
      };
      await deps.writeFile(deps.join(stageDir, "receipt.json"), `${JSON.stringify(receipt, null, 2)}\n`);
      if (deps.exists(installDir)) {
        await deps.rename(installDir, displacedDir);
        displaced = true;
      }
      await deps.rename(stageDir, installDir);
      const activated = { ...receipt, executable: deps.join(installDir, release.executable), installDir };
      await writeActiveReceipt(activated, previous);
      active = activated;
      stopWorker();
      phase = "ready";
      lastError = "";
      if (displaced) await deps.remove(displacedDir).catch(() => undefined);
      return { ok: true, schema: spreadExMlSchema, component: publicComponent() };
    } catch (error) {
      lastError = cleanError(error);
      phase = active ? "ready" : "unavailable";
      if (displaced && !deps.exists(installDir) && deps.exists(displacedDir)) await deps.rename(displacedDir, installDir).catch(() => undefined);
      return { ok: false, schema: spreadExMlSchema, error: lastError, rolledBack: Boolean(previous) };
    } finally {
      await deps.remove(stageDir).catch(() => undefined);
      await deps.remove(displacedDir).catch(() => undefined);
      await deps.remove(archivePath).catch(() => undefined);
    }
  }

  async function recoverActive() {
    const current = await deps.readJson(deps.join(rootDir, "active.json")).catch(() => null);
    const recovered = await validateReceipt(current);
    if (recovered) {
      active = recovered;
      phase = "ready";
      lastError = "";
      return true;
    }
    const previous = await deps.readJson(deps.join(rootDir, "previous.json")).catch(() => null);
    const rollback = await validateReceipt(previous);
    if (!rollback) return false;
    await writeActiveReceipt(rollback, null);
    active = rollback;
    phase = "ready";
    lastError = "";
    return true;
  }

  async function validateReceipt(value) {
    if (!validReceipt(value, platformKey)) return null;
    const installDir = deps.join(rootDir, "releases", value.releaseId || `${value.version}-${value.archiveSha256.slice(0, 12)}-${platformKey}`);
    const executable = deps.join(installDir, value.executable);
    if (!deps.exists(executable)) return null;
    const result = parseWorkerJson(await deps.runFile(executable, cleanArgs(value.selfTestArgs, ["--self-test"]), 20_000, undefined, installDir).catch(() => null));
    if (!result?.ok || result.featureSchema !== value.featureSchema || result.modelVersion !== value.modelVersion) return null;
    return { ...value, executable, installDir };
  }

  async function writeActiveReceipt(next, previous) {
    const releaseId = next.installDir ? String(next.installDir).split(/[\\/]/u).pop() : next.releaseId;
    const receipt = { ...next, releaseId, executable: String(next.executable).split(/[\\/]/u).pop(), installDir: undefined };
    if (previous) {
      const previousReleaseId = String(previous.installDir || "").split(/[\\/]/u).pop() || previous.releaseId;
      const previousReceipt = { ...previous, releaseId: previousReleaseId, executable: String(previous.executable).split(/[\\/]/u).pop(), installDir: undefined };
      await atomicJson(deps.join(rootDir, "previous.json"), previousReceipt);
    }
    await atomicJson(deps.join(rootDir, "active.json"), receipt);
  }

  async function atomicJson(path, value) {
    const next = `${path}.${deps.randomId()}.next`;
    await deps.writeFile(next, `${JSON.stringify(value, null, 2)}\n`);
    await deps.rename(next, path, true);
  }

  function scheduleHeartbeat(delay = 30_000) {
    if (stopped || !secrets.deviceToken || typeof deps.setTimer !== "function") return;
    if (heartbeatTimer) deps.clearTimer?.(heartbeatTimer);
    heartbeatTimer = deps.setTimer(() => void heartbeat(), delay);
  }

  async function heartbeat() {
    heartbeatTimer = null;
    if (stopped || !secrets.deviceToken) return;
    try {
      const status = publicStatusForRemote();
      const result = await authenticatedRequest("/api/ml/agent/heartbeat", { method: "POST", body: status, timeoutMs: 15_000 });
      if (!result?.ok) throw new Error("spreadex-heartbeat-rejected");
      lastHeartbeatAt = deps.now();
      connection = socket ? "connected" : "heartbeat";
      if (result.settings) await applyRemoteSettings(result.settings);
      allowedModels = normalizeAllowedModels(result.allowed_models);
      if (Array.isArray(result.prediction_tasks) && result.prediction_tasks.length) void processPredictionTasks(result.prediction_tasks).catch(() => undefined);
    } catch {
      connection = socket ? "connected" : "disconnected";
    } finally {
      scheduleHeartbeat(2_000);
    }
  }

  function scheduleSocket(delay) {
    if (stopped || !secrets.deviceToken || !state.websocketUrl || typeof deps.createWebSocket !== "function" || typeof deps.setTimer !== "function") return;
    if (socket || socketTimer) return;
    socketTimer = deps.setTimer(() => {
      socketTimer = null;
      openSocket();
    }, delay);
  }

  function openSocket() {
    if (stopped || socket || !secrets.deviceToken || !state.websocketUrl) return;
    let candidate;
    try { candidate = deps.createWebSocket(state.websocketUrl); }
    catch { scheduleSocket(backoff(++socketFailures)); return; }
    socket = candidate;
    connection = "connecting";
    candidate.addEventListener("open", () => {
      if (socket !== candidate) return;
      socketFailures = 0;
      connection = "connected";
      candidate.send(JSON.stringify({ type: "authenticate", token: secrets.deviceToken, device_id: state.deviceId }));
    });
    candidate.addEventListener("message", (event) => void handleSocketMessage(String(event.data || "")));
    candidate.addEventListener("close", () => socketClosed(candidate));
    candidate.addEventListener("error", () => socketClosed(candidate));
  }

  function socketClosed(candidate) {
    if (socket !== candidate) return;
    try { candidate.close(); } catch { /* Already closed. */ }
    socket = null;
    connection = "disconnected";
    scheduleSocket(backoff(++socketFailures));
  }

  function closeSocket() {
    const current = socket;
    socket = null;
    if (current) try { current.close(); } catch { /* Already closed. */ }
  }

  async function handleSocketMessage(text) {
    const message = parseJson(text);
    if (!message || typeof message !== "object") return;
    if (message.type === "settings" && message.settings) {
      await applyRemoteSettings(message.settings).catch(() => undefined);
      return;
    }
    if (message.type === "ping") {
      if (socket) socket.send(JSON.stringify({ type: "pong", at: deps.now() }));
      return;
    }
    if (message.type !== "predict") return;
    void processPredictionTasks([message]).catch(() => undefined);
  }

  async function processPredictionTasks(rawTasks) {
    queuedTasks.push(...rawTasks.slice(0, 32));
    if (taskProcessing) return await taskProcessing;
    taskProcessing = (async () => {
      while (queuedTasks.length) await processPredictionTaskBatch(queuedTasks.splice(0, 32));
    })().finally(() => { taskProcessing = null; });
    return await taskProcessing;
  }

  async function processPredictionTaskBatch(rawTasks) {
    pruneTaskCaches();
    const tasks = rawTasks.map(normalizeRemoteTask).filter((task) => task && (!task.targetDeviceId || task.targetDeviceId === state.deviceId)).slice(0, 32);
    const nowMs = Date.now();
    const fresh = tasks.filter((task) => !completedTasks.has(task.key));
    if (!fresh.length) return;
    const isCurrent = (task) => task.expiresAtMs > nowMs && task.observedAtMs + (task.maxAgeMs || state.settings.max_prediction_age_ms) > nowMs;
    const needScoring = fresh.filter((task) => !pendingPredictions.has(task.key) && isCurrent(task));
    for (const task of fresh.filter((item) => !pendingPredictions.has(item.key) && !isCurrent(item))) {
      pendingPredictions.set(task.key, failedPrediction(task, task.expiresAtMs <= nowMs ? "prediction-task-expired" : "prediction-snapshot-stale"));
    }

    if (needScoring.length) {
      if (!active || phase !== "ready" || state.settings.mode === "off" || !activeAllowed()) {
        const error = !active || phase !== "ready"
          ? "spreadex-ml-unavailable"
          : state.settings.mode === "off" ? "spreadex-ml-disabled" : "model-not-allowed";
        for (const task of needScoring) pendingPredictions.set(task.key, failedPrediction(task, error));
      } else {
        try {
          await Promise.all(needScoring.map((task) => requestWorker("observe", {
            request: {
              request_id: task.requestId,
              route_id: task.routeId,
              snapshot_id: task.snapshotId,
              observed_at_ms: task.observedAtMs,
              expires_at_ms: task.expiresAtMs,
              max_age_ms: task.maxAgeMs || state.settings.max_prediction_age_ms,
              notional_usd: task.notionalUsd,
              profile: task.profile,
              min_probability: task.minProbability
            },
            route: task.snapshot
          }, 5_000)));
          const response = await requestWorker("predict_batch", {
            requests: needScoring.map((task) => ({
              request_id: task.requestId,
              route_id: task.routeId,
              snapshot_id: task.snapshotId,
              profile: task.profile,
              min_probability: task.minProbability,
              max_age_ms: task.maxAgeMs || state.settings.max_prediction_age_ms,
              notional_usd: task.notionalUsd,
              expires_at_ms: task.expiresAtMs
            }))
          }, 8_000);
          const byRequest = new Map(workerPredictions(response).map((item) => [cleanId(item.request_id || item.requestId, 160), item]));
          for (const task of needScoring) {
            const prediction = byRequest.get(task.requestId);
            pendingPredictions.set(task.key, validRemotePrediction(prediction, task)
              ? successfulPrediction(task, prediction, active)
              : failedPrediction(task, "spreadex-ml-invalid-prediction"));
          }
        } catch (error) {
          for (const task of needScoring) pendingPredictions.set(task.key, failedPrediction(task, cleanError(error)));
        }
      }
    }

    const predictions = fresh.map((task) => pendingPredictions.get(task.key)).filter(Boolean);
    if (!predictions.length) return;
    const result = await authenticatedRequest("/api/ml/agent/predictions", {
      method: "POST",
      body: { schema: "spreadex.ml.v1", predictions },
      timeoutMs: 15_000
    });
    if (!result?.ok) throw new Error("spreadex-predictions-rejected");
    for (const task of fresh) {
      if (!pendingPredictions.has(task.key)) continue;
      completedTasks.set(task.key, Math.max(Date.now() + 60_000, task.expiresAtMs + 60_000));
      pendingPredictions.delete(task.key);
    }
  }

  async function requestWorker(method, params, timeoutMs) {
    if (!active || phase !== "ready") throw new Error("spreadex-ml-unavailable");
    if (!worker) {
      workerPhase = "starting";
      try {
        worker = deps.createWorker(active.executable, active.workerArgs || ["--jsonl"], active.installDir);
        workerPhase = "running";
      } catch (error) {
        workerPhase = "error";
        throw error;
      }
    }
    try {
      return await worker.request(method, params, timeoutMs);
    } catch (error) {
      stopWorker();
      workerPhase = "error";
      throw new Error(cleanError(error) || "spreadex-ml-worker-failed");
    }
  }

  function stopWorker() {
    const current = worker;
    worker = null;
    if (current) try { current.stop(); } catch { /* Already stopped. */ }
    if (workerPhase !== "error") workerPhase = "stopped";
  }

  function pruneTaskCaches() {
    const now = Date.now();
    for (const [key, expiresAt] of completedTasks) if (expiresAt <= now) completedTasks.delete(key);
    for (const [key, value] of pendingPredictions) if (Number(value?.expires_at_ms || 0) + 60_000 <= now) pendingPredictions.delete(key);
  }

  async function authenticatedRequest(pathname, request) {
    if (!secrets.deviceToken) throw new Error("spreadex-not-paired");
    return await deps.request(new URL(pathname, baseUrl).toString(), {
      ...request,
      headers: { Authorization: `Bearer ${secrets.deviceToken}` }
    });
  }

  async function applyRemoteSettings(value) {
    const revision = safeRevision(value?.revision);
    if (revision && revision <= state.settings.revision) return;
    const next = normalizeSpreadExSettings(value, state.settings);
    next.device_id = state.deviceId;
    next.revision = revision || state.settings.revision + 1;
    state = { ...state, settings: next };
    await deps.saveState(state);
  }

  function publicStatusForRemote() {
    const status = publicComponent();
    return {
      schema: spreadExMlSchema,
      device_id: state.deviceId,
      runtime_version: options.runtimeVersion,
      platform: platformKey,
      settings_revision: state.settings.revision,
      component: status
    };
  }

  function activeAllowed() {
    return Boolean(active && allowedModels.some((item) => item.model_version === active.modelVersion && item.feature_schema === active.featureSchema));
  }

  function publicPairStatus() {
    return { paired: Boolean(secrets.deviceToken), pairedAt, deviceId: state.deviceId, scopes: state.scopes, baseOrigin: new URL(baseUrl).origin };
  }

  function publicComponent() {
    return { available: Boolean(active && phase === "ready"), phase, version: active?.version || "", modelVersion: active?.modelVersion || "", featureSchema: active?.featureSchema || "", lastError };
  }
}

export function normalizeSpreadExBaseUrl(value) {
  try {
    const url = new URL(String(value || ""));
    const local = ["localhost", "127.0.0.1", "::1", "[::1]"].includes(url.hostname);
    if (url.protocol !== "https:" && !(local && url.protocol === "http:")) return "";
    url.username = "";
    url.password = "";
    url.search = "";
    url.hash = "";
    return url.toString().replace(/\/+$/u, "");
  } catch { return ""; }
}

export function spreadExOriginAllowed(origin, spreadExBaseUrl, sotyOrigins = []) {
  if (!origin) return true;
  let actual;
  try { actual = new URL(origin).origin; } catch { return false; }
  if (["http://localhost", "http://127.0.0.1", "http://[::1]"].includes(actual)) return true;
  const allowed = [spreadExBaseUrl, ...sotyOrigins].map((item) => {
    try { return new URL(item).origin; } catch { return ""; }
  }).filter(Boolean);
  return allowed.includes(actual);
}

export function normalizeSpreadExSettings(value, fallback = {}) {
  const base = publicSettings(fallback);
  const requestedMode = value?.mode === "advisory" ? "assistant" : value?.mode;
  const requestedProfile = value?.profile === "cautious" ? "careful" : value?.profile;
  const mode = modes.has(requestedMode) ? requestedMode : base.mode;
  const profile = profiles.has(requestedProfile) ? requestedProfile : base.profile;
  const minProbability = boundedNumber(value?.min_probability ?? value?.minimumConfidence ?? value?.minimum_confidence, 0.5, 0.99, base.min_probability);
  const effectiveMinProbability = boundedNumber(value?.effective_min_probability, 0.5, 0.99, minProbability);
  const maxPredictionAgeMs = boundedInteger(value?.max_prediction_age_ms, 500, 60_000, base.max_prediction_age_ms);
  const deviceId = cleanId(value?.device_id || value?.deviceId, 180) || base.device_id;
  return {
    mode,
    profile,
    min_probability: minProbability,
    effective_min_probability: effectiveMinProbability,
    max_prediction_age_ms: maxPredictionAgeMs,
    device_id: deviceId,
    revision: safeRevision(value?.revision) || base.revision
  };
}

export function canonicalSpreadExRelease(value) {
  return JSON.stringify(sortObject(stripSignature(value)));
}

function normalizeSpreadExRelease(value, platformKey, manifestUrl) {
  if (!value || value.schema !== spreadExMlReleaseSchema || value.available === false) return null;
  const version = cleanVersion(value.version);
  const modelVersion = cleanId(value.modelVersion, 160);
  const featureSchema = cleanId(value.featureSchema, 160);
  const signature = cleanSignature(value.signature);
  const platform = value.platforms?.[platformKey];
  const sha256 = String(platform?.sha256 || "").toLowerCase();
  const executable = cleanRelativePath(platform?.executable);
  const archive = cleanArchive(platform?.archive || platform?.url);
  const selfTestArgs = cleanArgs(platform?.selfTestArgs, ["--self-test"]);
  const predictArgs = cleanArgs(platform?.predictArgs, ["--predict-json"]);
  const workerArgs = cleanArgs(platform?.workerArgs, ["--jsonl"]);
  let url = "";
  try {
    const parsed = new URL(String(platform?.url || ""), manifestUrl);
    if (parsed.protocol === "https:" || (["localhost", "127.0.0.1", "::1", "[::1]"].includes(parsed.hostname) && parsed.protocol === "http:")) url = parsed.toString();
  } catch { /* Invalid artifact URL. */ }
  if (!version || !modelVersion || !featureSchema || !signature || !platform || !url || !/^[a-f0-9]{64}$/u.test(sha256) || !executable || !archive) return null;
  return {
    version, modelVersion, featureSchema, signature, url, sha256, executable, archive, selfTestArgs, predictArgs, workerArgs,
    maxBytes: boundedInteger(platform.maxBytes, 1_024, 500 * 1024 * 1024, 250 * 1024 * 1024),
    signedPayload: canonicalSpreadExRelease(value)
  };
}

function normalizeSpreadExState(value) {
  const deviceId = cleanId(value?.deviceId, 180);
  const settings = normalizeSpreadExSettings(value?.settings);
  settings.device_id = deviceId;
  return {
    deviceId,
    scopes: cleanScopes(value?.scopes),
    pairedAt: cleanDate(value?.pairedAt),
    websocketUrl: String(value?.websocketUrl || ""),
    settings
  };
}

function normalizeSpreadExSecrets(value) {
  const deviceToken = cleanDeviceToken(value?.deviceToken);
  return deviceToken ? { deviceToken } : {};
}

function publicSettings(value) {
  const requestedMode = value?.mode === "advisory" ? "assistant" : value?.mode;
  const requestedProfile = value?.profile === "cautious" ? "careful" : value?.profile;
  const minProbability = boundedNumber(value?.min_probability ?? value?.minimumConfidence, 0.5, 0.99, 0.78);
  return {
    mode: modes.has(requestedMode) ? requestedMode : "off",
    profile: profiles.has(requestedProfile) ? requestedProfile : "balanced",
    min_probability: minProbability,
    effective_min_probability: boundedNumber(value?.effective_min_probability, 0.5, 0.99, minProbability),
    max_prediction_age_ms: boundedInteger(value?.max_prediction_age_ms, 500, 60_000, 2_500),
    device_id: cleanId(value?.device_id || value?.deviceId, 180),
    revision: safeRevision(value?.revision)
  };
}

function normalizePredictionInput(value) {
  const requestId = cleanId(value?.request_id || value?.requestId, 160);
  const features = value?.features;
  if (!requestId || !features || typeof features !== "object" || Array.isArray(features)) return null;
  const json = JSON.stringify(features);
  if (json.length > 256_000) return null;
  return { requestId, features };
}

function normalizeRemoteTask(value) {
  const requestId = cleanId(value?.request_id || value?.requestId, 160);
  const routeId = cleanId(value?.route_id || value?.routeId, 200);
  const snapshotId = cleanId(value?.snapshot_id || value?.snapshotId, 200);
  const observedAtMs = boundedInteger(value?.observed_at_ms ?? value?.observed_at ?? value?.observedAt, 1, Number.MAX_SAFE_INTEGER, 0);
  const expiresAtMs = boundedInteger(value?.expires_at_ms ?? value?.expires_at ?? value?.expiresAt, 1, Number.MAX_SAFE_INTEGER, 0);
  const requestedProfile = value?.profile === "cautious" ? "careful" : value?.profile;
  const profile = profiles.has(requestedProfile) ? requestedProfile : "balanced";
  const minProbability = boundedNumber(value?.min_probability ?? value?.minProbability, 0.5, 0.99, 0.78);
  const targetDeviceId = cleanId(value?.target_device_id || value?.targetDeviceId, 180);
  const maxAgeMs = boundedInteger(value?.max_age_ms, 500, 60_000, 0);
  const notionalUsd = boundedNumber(value?.notional_usd, 0.01, 1_000_000_000, 0);
  const snapshot = value?.snapshot;
  if (!requestId || !routeId || !snapshotId || !observedAtMs || !expiresAtMs || expiresAtMs <= observedAtMs || !snapshot || typeof snapshot !== "object" || Array.isArray(snapshot)) return null;
  if (JSON.stringify(snapshot).length > 256_000) return null;
  return { key: `${requestId}:${snapshotId}`, requestId, routeId, snapshotId, observedAtMs, expiresAtMs, profile, minProbability, targetDeviceId, maxAgeMs, notionalUsd, snapshot };
}

function workerPredictions(value) {
  const payload = value?.result && typeof value.result === "object" ? value.result : value;
  if (Array.isArray(payload?.predictions)) return payload.predictions.filter((item) => item && typeof item === "object");
  if (payload && typeof payload === "object" && Number.isFinite(payload.probability)) return [payload];
  return [];
}

function validRemotePrediction(value, task) {
  if (!value || value.ok === false) return false;
  const requestId = cleanId(value.request_id || value.requestId, 160);
  const probability = Number(value.probability);
  return requestId === task.requestId && Number.isFinite(probability) && probability >= 0 && probability <= 1;
}

function successfulPrediction(task, value, component) {
  const probability = Number(value.probability);
  return {
    request_id: task.requestId,
    route_id: task.routeId,
    snapshot_id: task.snapshotId,
    ok: true,
    probability,
    decision: cleanDecision(value.decision || (probability >= task.minProbability ? "allow" : "deny")),
    reasons: cleanReasons(value.reasons),
    model_version: component.modelVersion,
    feature_schema: component.featureSchema,
    scored_at_ms: Date.now(),
    expires_at_ms: Math.min(task.expiresAtMs, safeExpiresAt(value.expires_at_ms || value.expiresAtMs)),
    out_of_distribution: value.out_of_distribution === true || value.outOfDistribution === true
  };
}

function failedPrediction(task, error) {
  return {
    request_id: task.requestId,
    route_id: task.routeId,
    snapshot_id: task.snapshotId,
    ok: false,
    error: cleanError(error),
    scored_at_ms: Date.now(),
    expires_at_ms: task.expiresAtMs
  };
}

function validSelfTest(result, release) {
  return result?.ok === true && result.featureSchema === release.featureSchema && result.modelVersion === release.modelVersion;
}

function validReceipt(value, platformKey) {
  return value?.schema === spreadExMlSchema && cleanVersion(value.version) && cleanId(value.modelVersion, 160) && cleanId(value.featureSchema, 160)
    && value.platform === platformKey && /^[a-f0-9]{64}$/u.test(String(value.archiveSha256 || "")) && cleanRelativePath(value.executable);
}

function parseWorkerJson(value) {
  if (value && typeof value === "object" && Number.isInteger(value.exitCode)) {
    if (value.exitCode !== 0) return null;
    value = value.stdout;
  }
  if (value && typeof value === "object") return value;
  return parseJson(String(value || "").trim().split(/\r?\n/u).filter(Boolean).at(-1));
}

function parseJson(value) {
  try { return JSON.parse(value); } catch { return null; }
}

function stripSignature(value) {
  if (Array.isArray(value)) return value.map(stripSignature);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value).filter(([key]) => key !== "signature").map(([key, item]) => [key, stripSignature(item)]));
}

function sortObject(value) {
  if (Array.isArray(value)) return value.map(sortObject);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortObject(value[key])]));
}

function normalizeManifestUrl(value) {
  try {
    const url = new URL(String(value || ""));
    return url.protocol === "https:" || (["localhost", "127.0.0.1", "::1", "[::1]"].includes(url.hostname) && url.protocol === "http:") ? url.toString() : "";
  } catch { return ""; }
}

function normalizeWebSocketUrl(value, baseUrl) {
  if (!value) return "";
  try {
    const url = new URL(String(value), baseUrl);
    const base = new URL(baseUrl);
    const local = ["localhost", "127.0.0.1", "::1", "[::1]"].includes(base.hostname);
    if (url.host !== base.host || (url.protocol !== "wss:" && !(local && url.protocol === "ws:"))) return "";
    url.username = "";
    url.password = "";
    return url.toString();
  } catch { return ""; }
}

function cleanPairCode(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{8,192}$/u.test(text) ? text : "";
}

function cleanDeviceToken(value) {
  const text = String(value || "").trim();
  return /^[\x21-\x7E]{32,2048}$/u.test(text) && !/[\s"'\\]/u.test(text) ? text : "";
}

function cleanSignature(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_+/=-]{64,512}$/u.test(text) ? text : "";
}

function cleanPlatformKey(value) {
  const text = String(value || "").trim();
  return /^(?:win32|linux|darwin)-(?:x64|arm64)$/u.test(text) ? text : "";
}

function cleanVersion(value) {
  const text = String(value || "").trim();
  return /^\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?$/u.test(text) ? text : "";
}

function cleanId(value, max) {
  const text = String(value || "").trim().slice(0, max);
  return /^[A-Za-z0-9][A-Za-z0-9_.:-]*$/u.test(text) ? text : "";
}

function cleanRelativePath(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9][A-Za-z0-9_.\/-]{0,200}$/u.test(text) && !text.includes("..") && !text.startsWith("/") ? text : "";
}

function cleanArchive(value) {
  const text = String(value || "").split(/[?#]/u)[0].toLowerCase();
  if (text.endsWith(".zip")) return "zip";
  if (text.endsWith(".tar.gz") || text.endsWith(".tgz")) return "tar.gz";
  return "";
}

function cleanArgs(value, fallback) {
  if (!Array.isArray(value)) return fallback;
  const args = value.map((item) => String(item || "").trim()).filter((item) => item && item.length <= 120 && !/[\r\n\0]/u.test(item)).slice(0, 12);
  return args.length ? args : fallback;
}

function cleanDecision(value) {
  if (value === "deny") return "block";
  return ["allow", "block"].includes(value) ? value : "block";
}

function cleanReasons(value) {
  return (Array.isArray(value) ? value : []).map((item) => String(item || "").replace(/[\r\n\t]+/gu, " ").trim().slice(0, 180)).filter(Boolean).slice(0, 5);
}

function cleanScopes(value) {
  return [...new Set((Array.isArray(value) ? value : []).map((item) => String(item || "").trim()).filter((item) => /^[a-z][a-z0-9:-]{0,63}$/u.test(item)))].sort();
}

function normalizeAllowedModels(value) {
  return (Array.isArray(value) ? value : []).map((item) => ({
    model_version: cleanId(item?.model_version, 160),
    feature_schema: cleanId(item?.feature_schema, 160)
  })).filter((item) => item.model_version && item.feature_schema).slice(0, 64);
}

function exactScopes(actual, expected) {
  const required = [...expected].sort();
  return actual.length === required.length && actual.every((item, index) => item === required[index]);
}

function cleanDate(value) {
  const text = String(value || "");
  return /^\d{4}-\d{2}-\d{2}T/u.test(text) && Number.isFinite(Date.parse(text)) ? new Date(text).toISOString() : "";
}

function safeRevision(value) {
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : 0;
}

function safeExpiresAt(value) {
  const number = Number(value);
  return Number.isSafeInteger(number) && number > Date.now() && number <= Date.now() + 60_000 ? number : Date.now() + 2_000;
}

function boundedNumber(value, min, max, fallback) {
  const number = Number(value);
  return Number.isFinite(number) && number >= min && number <= max ? number : fallback;
}

function boundedInteger(value, min, max, fallback) {
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= min && number <= max ? number : fallback;
}

function cleanError(error) {
  return String(error instanceof Error ? error.message : error || "spreadex-ml-error").replace(/[\r\n]+/gu, " ").slice(0, 240);
}

function backoff(failures) {
  return Math.min(60_000, 1_000 * 2 ** Math.min(failures, 6));
}
