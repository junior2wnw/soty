import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  canonicalSpreadExRelease,
  createSpreadExMlIntegration,
  normalizeSpreadExBaseUrl,
  spreadExMlReleaseSchema,
  spreadExOriginAllowed
} from "./agent-modules/spreadex-ml.mjs";

assert.equal(normalizeSpreadExBaseUrl("https://miniapp.spreadex.me/path?q=1"), "https://miniapp.spreadex.me/path");
assert.equal(normalizeSpreadExBaseUrl("http://miniapp.spreadex.me"), "");
assert.equal(spreadExOriginAllowed("https://miniapp.spreadex.me", "https://miniapp.spreadex.me", ["https://xn--n1afe0b.online"]), true);
assert.equal(spreadExOriginAllowed("https://miniapp.spreadex.me.evil.test", "https://miniapp.spreadex.me", ["https://xn--n1afe0b.online"]), false);
assert.equal(spreadExOriginAllowed("https://xn--n1afe0b.online", "https://miniapp.spreadex.me", ["https://xn--n1afe0b.online"]), true);

const files = new Map();
const dirs = new Set();
const states = [];
const secretWrites = [];
const requests = [];
const archive = Buffer.from("signed-spreadex-lightgbm-component");
const digest = createHash("sha256").update(archive).digest("hex");
const pathJoin = (...parts) => parts.join("/").replace(/\/+/gu, "/");
let selfTestFailureFor = "";
let workerStarts = 0;
let failNextWorkerRequest = false;

const deps = {
  exists: (path) => files.has(path) || dirs.has(path),
  mkdir: async (path) => { dirs.add(path); },
  remove: async (path) => {
    for (const key of [...files.keys()]) if (key === path || key.startsWith(`${path}/`)) files.delete(key);
    for (const key of [...dirs]) if (key === path || key.startsWith(`${path}/`)) dirs.delete(key);
  },
  rename: async (from, to, replace = false) => {
    if (replace) await deps.remove(to);
    if (files.has(from)) {
      files.set(to, files.get(from));
      files.delete(from);
    }
    for (const [key, value] of [...files.entries()]) {
      if (key.startsWith(`${from}/`)) {
        files.delete(key);
        files.set(`${to}${key.slice(from.length)}`, value);
      }
    }
    if (dirs.has(from)) dirs.delete(from);
    dirs.add(to);
  },
  writeFile: async (path, value) => { files.set(path, value); },
  readJson: async (path) => {
    if (!files.has(path)) throw new Error("missing");
    return JSON.parse(String(files.get(path)));
  },
  chmod: async () => undefined,
  join: pathJoin,
  download: async () => archive,
  extract: async (_archivePath, destination) => { files.set(pathJoin(destination, "spreadex-ml.exe"), Buffer.from("worker")); },
  sha256: (bytes) => createHash("sha256").update(bytes).digest("hex"),
  verifyRelease: (payload, signature) => payload.includes("spreadex-ml.exe") && signature === "A".repeat(88),
  runFile: async (file, args, _timeoutMs, input) => {
    if (args[0] === "--self-test") {
      const failed = selfTestFailureFor && file.includes(selfTestFailureFor);
      return { exitCode: failed ? 1 : 0, stdout: JSON.stringify({ ok: !failed, featureSchema: "spreadex.ml.features.v1", modelVersion: file.includes("2.0.0") ? "model-2" : "model-1" }) };
    }
    throw new Error(`unexpected one-shot worker call: ${args[0]}:${input || ""}`);
  },
  createWorker: () => {
    workerStarts += 1;
    return ({
    request: async (method, params) => {
      if (failNextWorkerRequest) {
        failNextWorkerRequest = false;
        throw new Error("spreadex-ml-worker-timeout");
      }
      if (method === "observe") return { ok: true };
      assert.equal(method, "predict_batch");
      return {
        ok: true,
        result: {
          predictions: params.requests.map((item) => ({
            ok: true,
            requestId: item.requestId || item.request_id,
            probability: 0.82,
            decision: "allow",
            reasons: ["stable spread"]
          }))
        }
      };
    },
    stop: () => undefined
  });
  },
  request: async (url, options) => {
    requests.push({ url, options });
    if (url.endsWith("/api/ml/agent/predictions")) return { ok: true, success: true, accepted: options.body.predictions.length, rejected: 0 };
    return {
      ok: true,
      token: "scoped-device-token-abcdefghijklmnopqrstuvwxyz0123456789",
      device: { id: "spreadex-device-1", name: "Trading PC" },
      scopes: ["heartbeat", "predictions", "settings:read"],
      allowed_models: [{ model_version: "model-1", feature_schema: "spreadex.ml.features.v1" }],
      websocket_url: "wss://miniapp.spreadex.me/api/ml/agent/ws",
      settings: { mode: "assistant", profile: "balanced", min_probability: 0.78, effective_min_probability: 0.78, max_prediction_age_ms: 2_500, revision: 4 }
    };
  },
  saveState: async (value) => { states.push(structuredClone(value)); },
  saveSecrets: async (value) => { secretWrites.push(structuredClone(value)); },
  now: () => "2026-08-11T12:00:00.000Z",
  randomId: (() => { let id = 0; return () => String(++id); })()
};

const integration = createSpreadExMlIntegration(deps, {
  rootDir: "root",
  baseUrl: "https://miniapp.spreadex.me",
  manifestUrl: "https://xn--n1afe0b.online/agent/manifest.json",
  platformKey: "win32-x64",
  runtimeVersion: "1.2.0",
  deviceId: "soty-device-1",
  deviceNick: "Trading PC"
});

await integration.initialize();
assert.equal(integration.status().component.available, false);
assert.equal(integration.status().component.lastError, "component-not-installed");

const paired = await integration.pair("pair_code_123456789");
assert.equal(paired.ok, true);
assert.equal(requests[0].url, "https://miniapp.spreadex.me/api/ml/agent/enroll");
assert.equal(requests[0].options.body.runtime_version, "1.2.0");
assert.equal(requests[0].options.body.agent_version, "1.2.0");
assert.equal(requests[0].options.body.device_name, "Trading PC");
assert.equal(secretWrites.length, 1);
assert.equal(integration.status().paired, true);
assert.equal(JSON.stringify(integration.status()).includes("scoped-device-token"), false);
assert.equal(states.at(-1).settings.revision, 4);

const release1 = release("1.0.0", "model-1", digest);
assert.doesNotThrow(() => JSON.parse(canonicalSpreadExRelease(release1)));
const installed = await integration.syncRelease(release1);
assert.equal(installed.ok, true);
assert.equal(integration.status().component.available, true);
assert.equal(integration.status().component.modelVersion, "model-1");
assert.equal(integration.status().ready, true);

const prediction = await integration.predict({ requestId: "req-1", features: { spread: 0.012, depth: 15_000 } });
assert.equal(prediction.probability, 0.82);
assert.equal(prediction.modelVersion, "model-1");

const taskNow = Date.now();
const remoteTask = {
  request_id: "remote-request-1",
  route_id: "binance-okx-btc",
  snapshot_id: "snapshot-1",
  target_device_id: "spreadex-device-1",
  observed_at: taskNow - 100,
  expires_at: taskNow + 10_000,
  profile: "balanced",
  min_probability: 0.78,
  snapshot: { snapshot_ts_ms: taskNow - 100, direct_spread: 0.012, reverse_spread: -0.013, long_limit: 60_000, short_limit: 55_000 }
};
await integration.processTasks([remoteTask]);
const predictionRequest = requests.at(-1);
assert.equal(predictionRequest.url, "https://miniapp.spreadex.me/api/ml/agent/predictions");
assert.equal(predictionRequest.options.body.schema, "spreadex.ml.v1");
assert.equal(predictionRequest.options.body.predictions[0].request_id, "remote-request-1");
assert.equal(predictionRequest.options.body.predictions[0].model_version, "model-1");
assert.equal(JSON.stringify(predictionRequest.options.body).includes("direct_spread"), false);
const requestCountAfterAck = requests.length;
await integration.processTasks([remoteTask]);
assert.equal(requests.length, requestCountAfterAck);

failNextWorkerRequest = true;
await integration.processTasks([{ ...remoteTask, request_id: "remote-request-timeout", snapshot_id: "snapshot-timeout" }]);
assert.equal(requests.at(-1).options.body.predictions[0].ok, false);
assert.equal(requests.at(-1).options.body.predictions[0].error, "spreadex-ml-worker-timeout");
const startsAfterTimeout = workerStarts;
await integration.processTasks([{ ...remoteTask, request_id: "remote-request-restart", snapshot_id: "snapshot-restart" }]);
assert.equal(requests.at(-1).options.body.predictions[0].ok, true);
assert.equal(workerStarts, startsAfterTimeout + 1);

selfTestFailureFor = "2.0.0";
const failed = await integration.syncRelease(release("2.0.0", "model-2", digest));
assert.equal(failed.ok, false);
assert.equal(failed.rolledBack, true);
assert.equal(integration.status().component.available, true);
assert.equal(integration.status().component.modelVersion, "model-1");

const unsignedIntegration = createSpreadExMlIntegration({ ...deps, verifyRelease: () => false }, {
  rootDir: "other",
  baseUrl: "https://miniapp.spreadex.me",
  manifestUrl: "https://xn--n1afe0b.online/agent/manifest.json",
  platformKey: "win32-x64",
  runtimeVersion: "1.2.0",
  deviceId: "soty-device-2",
  deviceNick: "Other PC"
});
await unsignedIntegration.initialize();
const rejected = await unsignedIntegration.syncRelease(release1);
assert.equal(rejected.ok, false);
assert.equal(rejected.error, "component-signature-invalid");
assert.equal(unsignedIntegration.status().component.available, false);

const broadTokenIntegration = createSpreadExMlIntegration({
  ...deps,
  request: async () => ({
    ok: true,
    token: "too-broad-device-token-abcdefghijklmnopqrstuvwxyz0123456789",
    device: { id: "broad-device" },
    scopes: ["heartbeat", "predictions", "settings:read", "trade"]
  })
}, {
  rootDir: "broad",
  baseUrl: "https://miniapp.spreadex.me",
  manifestUrl: "https://xn--n1afe0b.online/agent/manifest.json",
  platformKey: "win32-x64",
  runtimeVersion: "1.2.0",
  deviceId: "soty-device-3",
  deviceNick: "Broad PC"
});
await broadTokenIntegration.initialize();
await assert.rejects(() => broadTokenIntegration.pair("pair_code_too_broad"), /spreadex-pair-rejected/u);
assert.equal(broadTokenIntegration.status().paired, false);

console.log("SpreadEx ML self-test passed");

function release(version, modelVersion, sha256) {
  return {
    schema: spreadExMlReleaseSchema,
    available: true,
    version,
    modelVersion,
    featureSchema: "spreadex.ml.features.v1",
    signature: "A".repeat(88),
    platforms: {
      "win32-x64": {
        url: `/agent/components/spreadex-ml-${version}-windows-x64.zip`,
        sha256,
        executable: "spreadex-ml.exe",
        selfTestArgs: ["--self-test"],
        predictArgs: ["--predict-json"]
      }
    }
  };
}
