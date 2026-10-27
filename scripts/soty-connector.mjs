#!/usr/bin/env node
import { execFileSync, spawn } from "node:child_process";
import { EventEmitter } from 'node:events';
import { createHash, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { chmod, lstat, mkdir, open, opendir, readFile, readdir, realpath, rename, rm, stat, writeFile } from "node:fs/promises";
import { createServer, request as httpRequest } from "node:http";
import { homedir, networkInterfaces } from "node:os";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { createConnectorScopedFactory } from "../modules/apps/scoped-embed/connector-host.mjs";
import { parseSourceJson } from "../modules/source-app/shared/strict-json.mjs";
import { createTrafficFabric, trafficFabricSchema } from "./agent-modules/traffic-fabric.mjs";
import { buildTrafficClientUri, createTrafficCoreRuntime, normalizeBridgeSettings, trafficCoreSchema } from "./agent-modules/traffic-core.mjs";
import { createTransitClientRuntime, executeTransitClientAction, transitCheckedProfile, transitClientWindowsRelease } from "./agent-modules/transit-client.mjs";
import { selectGonkaModel, gonkaModelLimitsFor, openCodeLicenseText, openCodeReleaseFor } from "./agent-modules/opencode-release.mjs";
import { createLocalAppsRuntime, normalizeLocalAppManifest, prepareLocalAppWorkspace, probeLocalApp, readLocalAppProposal } from "./agent-modules/local-apps.mjs";
import { createPreparedProjectBrowser } from "./agent-modules/prepared-projects.mjs";
import { resolveJobExecutor } from "./agent-modules/executor-policy.mjs";
import { createLocalJobReceipts } from "./agent-modules/local-job-receipts.mjs";
import { windowsUserCompanion } from "./agent-modules/user-companion.mjs";
import { productionShellOriginAllowed } from "./agent-modules/production-origin.mjs";

const connectorVersion = "1.4.15";
const connectorSchema = "soty.agent-runtime.v1";
const scriptPath = fileURLToPath(import.meta.url);
const connectorDir = resolve(env("SOTY_CONNECTOR_DATA_DIR") || dirname(scriptPath));
const configPath = join(connectorDir, "connector-config.json");
const legacyConfigPath = join(connectorDir, "agent-config.json");
const updatePreviousPath = `${scriptPath}.previous`;
const updatePendingPath = `${scriptPath}.update-pending.json`;
const releaseReceiptPath = join(connectorDir, "runtime-release.json");
const managed = flag("--managed") || env("SOTY_CONNECTOR_MANAGED", "SOTY_AGENT_MANAGED") === "1";
const scope = safeScope(arg("--scope") || env("SOTY_CONNECTOR_SCOPE", "SOTY_AGENT_SCOPE") || (managed ? "CurrentUser" : "Dev"));
const companion = env("SOTY_CONNECTOR_COMPANION", "SOTY_AGENT_COMPANION") === "1";
const configuredPort = safeInteger(arg("--port") || env("SOTY_CONNECTOR_PORT", "SOTY_AGENT_PORT"), 0, 65_535, 0);
// Machine releases before 1.2.6 persisted port 0 in the managed runner. Treat it
// as an old "automatic" value and converge to the browser-facing stable port.
const port = companion ? configuredPort : (configuredPort || 49_424);
const updateManifestUrl = arg("--update-url") || env("SOTY_CONNECTOR_UPDATE_URL", "SOTY_AGENT_UPDATE_URL") || "https://4-2.xn--p1ai/agent/manifest.json";
const autoUpdate = env("SOTY_CONNECTOR_AUTO_UPDATE", "SOTY_AGENT_AUTO_UPDATE") === "1" || (managed && env("SOTY_CONNECTOR_AUTO_UPDATE", "SOTY_AGENT_AUTO_UPDATE") !== "0");
const requestedShell = arg("--shell") || env("SOTY_CONNECTOR_SHELL", "SOTY_AGENT_SHELL");
const updateConfirmMs = safeInteger(env("SOTY_CONNECTOR_UPDATE_CONFIRM_MS"), 1_000, 60_000, 15_000);
const maxEventChars = 64_000;
const maxResultChars = 1_000_000;
const persisted = loadConfig();
const gonkaModel = selectGonkaModel(arg("--model") || env("SOTY_CONNECTOR_MODEL"), persisted.gonkaModel);
const gonkaModelLimits = gonkaModelLimitsFor(gonkaModel);
let linkId = safeLinkId(arg("--link-id") || arg("--relay-id") || env("SOTY_CONNECTOR_LINK_ID", "SOTY_AGENT_RELAY_ID") || persisted.linkId || persisted.relayId);
let relayBaseUrl = safeBaseUrl(env("SOTY_CONNECTOR_SERVER_URL", "SOTY_AGENT_RELAY_URL") || persisted.serverUrl || persisted.relayBaseUrl || originOf(updateManifestUrl) || "https://xn--n1afe0b.online");
let deviceId = safeId(env("SOTY_CONNECTOR_DEVICE_ID", "SOTY_AGENT_DEVICE_ID") || persisted.deviceId, 180) || `device-${randomUUID()}`;
let deviceNick = safeText(env("SOTY_CONNECTOR_DEVICE_NICK", "SOTY_AGENT_DEVICE_NICK") || persisted.deviceNick || hostLabel(), 120) || deviceId;
const installId = safeId(persisted.installId, 160) || `install-${randomUUID()}`;
const connectorToken = safeToken(persisted.connectorToken) || randomBytes(32).toString("base64url");
const connectorId = `${installId}:${scope.toLowerCase()}`;
const trafficRoot = join(connectorDir, "traffic-core");
const jobRoot = join(connectorDir, "connector-jobs");
// OFF until this installation has been explicitly admitted by its device owner.
// No new listener, dispatch format or executor is introduced.
const durableResults = env("SOTY_CONNECTOR_DURABLE_RESULTS") === "1" || persisted.localJobReceiptsEnabled === true;
const localJobReceipts = durableResults ? createLocalJobReceipts({
  join, digest: value => sha256(value),
  mkdir: async target => { await mkdir(target, { recursive: true, mode: 0o700 });
    if ((await lstat(target)).isSymbolicLink()) throw new Error('local_receipt_directory_denied');
    if (process.platform === 'win32') await protectSecretFile(target); },
  readJson: async target => { const info = await lstat(target);
    if (!info.isFile() || info.isSymbolicLink() || info.size > 6200000) throw new Error('local_receipt_file_denied');
    return JSON.parse(await readFile(target, 'utf8')); },
  list: target => readdir(target),
  writeExclusive: async (target, value) => { const file = await open(target, 'wx', 0o600);
    try { await file.writeFile(JSON.stringify(value), 'utf8'); await file.sync(); } finally { await file.close(); }
    await protectSecretFile(target); },
}, { root: join(jobRoot, 'native-results-v1'), binding: { linkId, deviceId, connectorId, relayBaseUrl } }) : null;
const openCodeRoot = join(connectorDir, "opencode-runtime");
const openCodeStateRoot = join(connectorDir, "opencode-state");
let shuttingDown = false;
let agentCache = { at: 0, value: null };
let openCodeInstallPromise = null;
let registrationError = "";
let lastRegisteredAt = "";
let activeJob = null;
let updateRunning = false;
let updateState = {
  lastCheckAt: "",
  lastResult: autoUpdate ? "not-checked" : "disabled",
  latestVersion: "",
  lastError: ""
};

const scopedEmbedFile = env('SOTY_SELECTED_EMBED_HOST_FILE');
const scopedFactory = createConnectorScopedFactory({ entries: loadScopedHostEntries(scopedEmbedFile) });
function loadScopedHostEntries(file) {
  if(!file)return [];
  try {
    if(!isAbsolute(file))throw new Error('invalid');
    const bytes=readFileSync(file);if(bytes.length>131072)throw new Error('invalid');
    const value=JSON.parse(bytes.toString('utf8'));
    if(value?.schema!=='soty.selected-embed-hosts.v1'||!Array.isArray(value.entries)||Object.keys(value).some(key=>!['schema','entries'].includes(key)))throw new Error('invalid');
    return value.entries;
  } catch {throw new Error('scoped_embed_host_configuration_invalid');}
}
const preparedProjects = createPreparedProjectBrowser({ allowedRoots: allowedJobRoots, lstat, open, opendir, realpath,
  basename, isAbsolute, join, relative, sep, digest: sha256, normalizeManifest: normalizeLocalAppManifest,
  parseJson: parseSourceJson, blockedPorts: [port], probe: value => probeLocalApp({ httpRequest }, value) });
const localApps = createLocalAppsRuntime({
  randomSecret: () => randomBytes(32).toString('base64url'),
  digest: value => createHash('sha256').update(value).digest('hex'),
  createWebSocket: url => new globalThis.WebSocket(url),
  httpRequest, encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64'),
}, { serverUrl: () => relayBaseUrl, token: () => connectorToken,
  identity: () => ({ linkId, hostDeviceId: deviceId, connectorId, name: deviceNick }), blockedPorts: [port],scopedFactory,
  preparedBrowse: request => preparedProjects.list(request), preparedInspect: request => preparedProjects.inspect(request),
  preparedManifest: request => preparedProjects.readManifest(request) });

const trafficFabric = createTrafficFabric({
  uuid: randomUUID,
  secret: () => randomBytes(32).toString("base64url"),
  digest: (value) => sha256(value)
});
let trafficFabricState = trafficFabric.normalizeState(persisted.trafficFabric);
let trafficCoreSettings = normalizeBridgeSettings(refreshTrafficVpnSettings(persisted.trafficCoreSettings));
let trafficCoreEnabled = persisted.trafficCoreEnabled === true;
const trafficCoreDependencies = {
  exists: existsSync,
  mkdir: async (target) => await mkdir(target, { recursive: true }),
  remove: async (target) => await rm(target, { recursive: true, force: true }),
  rename: async (from, to, replace = false) => {
    if (replace) await rm(to, { force: true });
    await rename(from, to);
  },
  writeFile: async (target, value) => await writeFile(target, value),
  readJson: async (target) => JSON.parse(await readFile(target, "utf8")),
  join,
  download: downloadTrafficCoreBytes,
  extract: extractTrafficCoreArchive,
  sha256,
  runFile: async (file, args, timeoutMs) => execFileSync(file, args, { encoding: "utf8", timeout: timeoutMs, windowsHide: true }),
  spawnCore: (file, args, cwd) => spawn(file, args, { cwd, windowsHide: true, stdio: "ignore" }),
  onceExit: (child) => new Promise((resolveExit) => child.once("exit", resolveExit)),
  wait: sleep,
  now: () => new Date().toISOString()
};
const trafficCoreRuntime = createTrafficCoreRuntime(trafficCoreDependencies);
const transitInstaller = createTrafficCoreRuntime(trafficCoreDependencies);
const transitRoot = join(connectorDir, 'transit-client');
const transitProfilePath = join(transitRoot, 'profile.json');
let transitDirectoryReady = null;
const transitControlHash = /^[a-f0-9]{64}$/u.test(persisted.transitControlHash || '') ? persisted.transitControlHash : '';
const transitClient = createTransitClientRuntime({
  installCore: async () => {
    if (process.platform !== 'win32' || process.arch !== 'x64') throw new Error('transit-platform-unsupported');
    await ensureTransitPrivateDirectory();
    return transitInstaller.install(transitClientWindowsRelease, join(transitRoot, 'core'));
  },
  verifyCore: async core => sha256(await readFile(core.executable)) === transitClientWindowsRelease.binarySha256,
  readProfile: async () => {
    try { return JSON.parse(await readFile(transitProfilePath, 'utf8')); }
    catch (error) { if (error.code === 'ENOENT') return null; throw new Error('transit-profile-unreadable'); }
  },
  saveCheckedProfile: saveTransitProfile,
  underlayInterface: transitUnderlayInterface,
  writeRunProfile: async (_core, profile) => writeTransitPrivate('run.json', profile),
  probe: probeTransitProfile,
  spawnCore: (core, file) => spawn(core.executable, ['run', '-c', file], { windowsHide: true, stdio: 'ignore', cwd: core.installDir }),
  rememberOwned: rememberTransitProcess,
  recoverOwned: recoverTransitProcess,
  forgetOwned: async child => {
    const record = await readTransitProcess();
    if (record?.pid === child.pid) await rm(join(transitRoot, 'process.json'), { force: true });
  },
  confirmRunning: async (_child, proof) => (await transitRoutePresent()) && proof.exitIps.includes(await transitPublicIp()),
  confirmStopped: async () => {
    for (let attempt = 0; attempt < 8; attempt++) { if (!(await transitRoutePresent())) return true; await sleep(250); }
    return false;
  },
  wait: sleep,
  onceExit: child => child.exitCode !== null || child.signalCode ? Promise.resolve() : new Promise(resolveExit => child.once('exit', resolveExit)),
});
if (process.argv[2] === 'transit-prepare') {
  process.stdout.write(`${JSON.stringify({ ok: true, runtime: await transitClient.prepare() })}\n`);
} else if (process.argv[2] === 'transit-stop') {
  process.stdout.write(`${JSON.stringify({ ok: true, runtime: await transitClient.stop() })}\n`);
} else if (process.argv[2] === 'transit-check') {
  let profile;
  try { profile = JSON.parse(await readFile(process.argv[3] || transitProfilePath, 'utf8')); }
  catch { throw new Error('transit-profile-unreadable'); }
  process.stdout.write(`${JSON.stringify({ ok: true, check: await transitClient.check(profile) })}\n`);
} else if (process.argv[2] === "ctl") {
  await runControl(process.argv.slice(3));
} else {
  await startConnector();
}

async function startConnector() {
  await saveConfig();
  await ensureManagedRunner().catch((error) => {
    updateState.lastError = `runner: ${safeError(error)}`;
  });
  const server = createServer((request, response) => {
    let url;
    try { url = new URL(request.url || "/", "http://127.0.0.1"); }
    catch {
      sendJson(response, 400, { "Cache-Control": "no-store" }, { ok: false, error: "invalid-request-target" });
      return;
    }
    void handleHttp(request, response, url).catch((error) => {
      if (!response.headersSent) sendJson(response, 500, corsHeaders(request), { ok: false, error: safeError(error) });
      else response.end();
    });
  });
  server.listen(port, "127.0.0.1", () => {
    const address = server.address();
    const actualPort = typeof address === "object" && address ? address.port : port;
    process.stdout.write(`soty-connector:${actualPort}\n`);
    scheduleUpdateConfirmation();
  });
  server.on("error", (error) => {
    process.stderr.write(`soty-connector:http:${safeError(error)}\n`);
    process.exitCode = 1;
  });

  if (trafficCoreEnabled && trafficCoreSettings) {
    void trafficCoreRuntime.configureAndStart(trafficRelease(), trafficRoot, trafficCoreSettings).catch(() => undefined);
  }
  startHeartbeat();
  scheduleOpenCodeConvergence();
  void connectorLoop();
  scheduleUpdate();
  scheduleUserCompanion();

  const stop = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    activeJob?.controller.abort();
    localApps.stop();
    await trafficCoreRuntime.stop().catch(() => undefined);
    await transitClient.stop().catch(() => undefined);
    server.close(() => process.exit(process.exitCode || 0));
    const timer = setTimeout(() => process.exit(process.exitCode || 0), 2_000);
    timer.unref?.();
  };
  process.on("SIGINT", () => void stop());
  process.on("SIGTERM", () => void stop());
}

async function handleHttp(request, response, url) {
  if(url.pathname==='/apps/scoped/authority') {
    try{await scopedFactory.sourceAuthority(request,response);}
    catch(error){if(!response.headersSent)sendJson(response,error.status===403?403:error.status===413?413:503,{'Cache-Control':'no-store'},{ok:false,code:'scoped_embed_authority_unavailable'});else response.end();}
    return;
  }
  if (url.pathname === '/apps/claim') {
    const origin = String(request.headers.origin || '');
    const allowed = productionShellOriginAllowed(origin, relayBaseUrl);
    const headers = { 'Cache-Control': 'no-store', 'Vary': 'Origin', 'X-Content-Type-Options': 'nosniff',
      ...(allowed ? { 'Access-Control-Allow-Origin': origin, 'Access-Control-Allow-Methods': 'POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Private-Network': 'true' } : {}) };
    if (!allowed) { sendJson(response, 403, headers, { ok: false, error: 'origin-not-allowed' }); return; }
    if (request.method === 'OPTIONS') { response.writeHead(204, headers); response.end(); return; }
    if (request.method !== 'POST') { sendJson(response, 405, headers, { ok: false, error: 'method-not-allowed' }); return; }
    try {
      const body = await readJsonBody(request, 1024);
      if (!body || typeof body !== 'object' || Array.isArray(body) || Object.keys(body).length) throw new Error('invalid-claim-request');
      sendJson(response, 200, headers, await localApps.claim());
    } catch { sendJson(response, 409, headers, { ok: false, error: 'apps_connector_offline' }); }
    return;
  }
  const headers = corsHeaders(request);
  const origin = String(request.headers.origin || "");
  if (!originAllowed(origin)) {
    sendJson(response, 403, headers, { ok: false, error: "origin-not-allowed" });
    return;
  }
  if (request.method === "OPTIONS") {
    response.writeHead(204, headers);
    response.end();
    return;
  }
  if (url.pathname === "/health" && request.method === "GET") {
    if (url.searchParams.get("update") === "1") void checkForUpdate();
    sendJson(response, 200, headers, await health());
    return;
  }
  if (url.pathname === "/agent/status" && request.method === "GET") {
    sendJson(response, 200, headers, { ok: true, schema: connectorSchema, agent: await detectAgent(true) });
    return;
  }
  if ((url.pathname === "/connector/bind" || url.pathname === "/agent/relay") && request.method === "POST") {
    await handleBind(request, response, headers, origin);
    return;
  }
  if (url.pathname === "/operator/traffic/fabric" && request.method === "GET") {
    sendJson(response, 200, headers, trafficStatus());
    return;
  }
  if (url.pathname === '/operator/transit/client' && request.method === 'GET') {
    sendJson(response, 200, headers, { ok: true, deviceId, controlPaired: !!transitControlHash, supported: process.platform === 'win32' && process.arch === 'x64', runtime: await transitClient.status() });
    return;
  }
  if (url.pathname === '/operator/transit/client' && request.method === 'POST') {
    try {
      const body = await readJsonBody(request, 300_000);
      const result = await executeTransitClientAction(body, { deviceId, runtime: transitClient,
        verifyControlToken: token => !!transitControlHash && timingSafeEqual(Buffer.from(sha256(token), 'hex'), Buffer.from(transitControlHash, 'hex')) });
      sendJson(response, 200, headers, { ok: true, ...result });
    } catch (error) {
      const code = /^transit-[a-z-]+$/u.test(error?.message || '') ? error.message : 'transit-client-failed';
      sendJson(response, 409, headers, { ok: false, error: code });
    }
    return;
  }
  if (url.pathname === "/operator/traffic/fabric/interfaces" && request.method === "GET") {
    sendJson(response, 200, headers, { ok: true, schema: trafficCoreSchema, interfaces: trafficInterfaces() });
    return;
  }
  if (url.pathname === "/operator/traffic/fabric/exit" && request.method === "POST") {
    await mutateTrafficFabric(request, response, headers, "exit");
    return;
  }
  if (url.pathname === "/operator/traffic/fabric/client" && request.method === "POST") {
    await mutateTrafficFabric(request, response, headers, "client");
    return;
  }
  if (url.pathname === "/operator/traffic/fabric/revoke" && request.method === "POST") {
    await mutateTrafficFabric(request, response, headers, "revoke");
    return;
  }
  if (url.pathname === "/operator/traffic/fabric/core" && request.method === "POST") {
    await handleTrafficCore(request, response, headers);
    return;
  }
  sendJson(response, 404, headers, { ok: false, error: "not-found" });
}

async function handleBind(request, response, headers, origin) {
  const body = await readJsonBody(request, 64_000);
  const nextLinkId = safeLinkId(body.linkId || body.relayId);
  const nextBaseUrl = safeBaseUrl(body.serverUrl || body.relayBaseUrl || origin);
  const nextDeviceId = safeId(body.deviceId || deviceId, 180);
  if (!nextLinkId || !nextBaseUrl || !nextDeviceId) {
    sendJson(response, 400, headers, { ok: false, error: "invalid-binding" });
    return;
  }
  if (origin && !sameOrigin(origin, nextBaseUrl) && !isLocalOrigin(origin)) {
    sendJson(response, 403, headers, { ok: false, error: "binding-origin-mismatch" });
    return;
  }
  linkId = nextLinkId;
  relayBaseUrl = nextBaseUrl;
  deviceId = nextDeviceId;
  deviceNick = safeText(body.deviceNick || deviceNick, 120) || nextDeviceId;
  await saveConfig();
  localApps.stop();
  const registered = await registerConnector(true);
  sendJson(response, registered.ok ? 200 : 502, headers, {
    ok: registered.ok,
    schema: connectorSchema,
    deviceId,
    currentDeviceId: deviceId,
    connectorId,
    ...(registered.ok ? {} : { error: registered.error })
  });
}

async function health() {
  const agent = await detectAgent();
  return {
    ok: true,
    schema: connectorSchema,
    connector: true,
    agentRuntime: true,
    managed,
    scope,
    companion,
    autoUpdate,
    platform: `${process.platform}-${process.arch}`,
    shell: shellName(),
    version: connectorVersion,
    executionPlane: scope === "Machine" ? "system" : "user",
    system: isWindowsSystem(),
    interactiveTaskBridge: false,
    maintenance: scope === "Machine",
    relay: Boolean(linkId),
    linkId: linkId ? "configured" : "",
    deviceId,
    deviceNick,
    sourceWorker: true,
    package: {
      wrapper: { id: "soty-connector", version: connectorVersion },
      agent: { id: "opencode", targetVersion: openCodeRelease()?.version || "", installedVersion: agent.version || "" }
    },
    update: {
      enabled: autoUpdate,
      manifestUrl: updateManifestUrl,
      currentVersion: connectorVersion,
      ...updateState
    },
    agent,
    registration: { connected: Boolean(lastRegisteredAt && !registrationError), lastRegisteredAt, error: registrationError },
    activeJob: activeJob ? { id: activeJob.id, kind: activeJob.kind, startedAt: activeJob.startedAt } : null
  };
}

function startHeartbeat() {
  const timer = setInterval(() => void registerConnector(), 30_000);
  timer.unref?.();
  void registerConnector(true);
}

async function registerConnector(forceAgent = false) {
  if (!linkId || !relayBaseUrl) return { ok: false, error: "connector-not-bound" };
  try {
    const result = await serverJson("/api/connectors/register", {
      method: "POST",
      body: {
        linkId,
        deviceId,
        deviceNick,
        connectorId,
        version: connectorVersion,
        platform: `${process.platform}-${process.arch}`,
        scope,
        capabilities: ["agent", "command", "script", "events", "cancel", "traffic", "local-apps", "soty.auto-owner-check.v1"],
        agent: await detectAgent(forceAgent)
      }
    });
    if (!result.ok) throw new Error(result.error || "registration-rejected");
    registrationError = "";
    lastRegisteredAt = new Date().toISOString();
    localApps.start();
    return result;
  } catch (error) {
    registrationError = safeError(error);
    return { ok: false, error: registrationError };
  }
}

async function connectorLoop() {
  let failures = 0;
  while (!shuttingDown) {
    if (!linkId || !relayBaseUrl) {
      await sleep(2_000);
      continue;
    }
    if (activeJob) {
      await sleep(500);
      continue;
    }
    try {
      if (localJobReceipts) await reconcileLocalResults();
      const result = await serverJson(`/api/connectors/poll?linkId=${encodeURIComponent(linkId)}&deviceId=${encodeURIComponent(deviceId)}&connectorId=${encodeURIComponent(connectorId)}&wait=1&autoOwnerGuard=1`, { timeoutMs: 35_000 });
      if (!result.ok) {
        // A successful registration has recovered this old poll denial. Retry
        // polling instead of replacing its fresh health state with that error.
        if (result.error === "connector-auth-failed" && (await registerConnector(true)).ok) continue;
        throw new Error(result.error || "poll-rejected");
      }
      failures = 0;
      const job = Array.isArray(result.jobs) ? result.jobs[0] : null;
      if (job) await executeJob(job);
    } catch (error) {
      failures += 1;
      registrationError = safeError(error);
      await sleep(Math.min(15_000, 500 * 2 ** Math.min(failures, 5)));
    }
  }
}

async function executeJob(job) {
  const kind = resolveJobExecutor(job);
  if (!kind) {
    await finishRemoteJob(job.id, { ok: false, text: "Этот тип задания не поддерживается исполнителем. Обновите подключение.", exitCode: 126 });
    return;
  }
  // Existing root admission/cancellation is read again at the start boundary.
  // A dropped admission response cannot turn into process execution.
  const automatic = kind === 'agent' && job.input?.output === 'local-app' && job.input?.autoRegister === true;
  const needsCurrentOwner = kind === 'agent' && (Boolean(localJobReceipts) || automatic);
  if (needsCurrentOwner) {
    const state = await serverJson(`/api/connectors/jobs/${encodeURIComponent(job.id)}/runtime-status`, { timeoutMs: 5_000 });
    if (!state.ok || state.job?.id !== job.id || state.job.currentOwnerChecked !== true) throw new Error('local_job_admission_unavailable');
    if (!['leased', 'running'].includes(state.job.status)) return;
    if (localJobReceipts && !await localJobReceipts.claim(job)) return; // A started marker is NEVER re-executable.
    if (state.job.cancelRequested === true) {
      await finishRemoteJob(job.id, { ok: false, text: 'Отменено до запуска', exitCode: 130 }, Boolean(localJobReceipts)); return;
    }
  }
  const detected = kind === "agent" ? await detectAgent() : null;
  if (kind === "agent" && !detected?.available) {
    await finishRemoteJob(job.id, { ok: false, text: detected?.reason || "OpenCode недоступен", exitCode: 126, agentId: "opencode" }, Boolean(localJobReceipts));
    return;
  }
  const requestedRunAs = job.input?.runAs === "system" ? "system" : "user";
  const runtimeRunAs = scope === "Machine" ? "system" : "user";
  if (kind !== "agent" && requestedRunAs !== runtimeRunAs) {
    await finishRemoteJob(job.id, { ok: false, text: `Нужен контекст ${requestedRunAs}`, exitCode: 126 });
    return;
  }
  const controller = new AbortController();
  activeJob = { id: job.id, kind, startedAt: new Date().toISOString(), controller };
  let eventQueue = Promise.resolve();
  const emit = (event) => {
    eventQueue = eventQueue.then(() => postJobEvent(job.id, event)).catch(() => undefined);
    return eventQueue;
  };
  await emit({ type: "started", text: kind === "agent" ? "OpenCode · Gonka AI" : shellName() });
  const heartbeat = setInterval(() => void emit({ type: "heartbeat", text: "" }), 25_000);
  heartbeat.unref?.();
  const cancelWatch = watchCancellation(job.id, controller);
  try {
    const runtime = { signal: controller.signal, emit };
    const verifyStart = needsCurrentOwner ? async () => {
      const state = await serverJson(`/api/connectors/jobs/${encodeURIComponent(job.id)}/runtime-status`, { timeoutMs: 5_000 });
      if (!state.ok || state.job?.id !== job.id || state.job.currentOwnerChecked !== true) throw new Error('local_job_admission_unavailable');
      if (state.job.cancelRequested === true || !['leased', 'running'].includes(state.job.status)) {
        controller.abort(); throw new Error('local_job_admission_revoked');
      }
    } : undefined;
    const result = kind === "agent" ? await runOpenCode(job, { ...runtime, verifyStart }) : await runShellJob(job, runtime);
    await eventQueue;
    await finishRemoteJob(job.id, { ...result, ...(kind === "agent" ? { agentId: "opencode" } : {}) }, Boolean(localJobReceipts && kind === 'agent'));
  } catch (error) {
    const cancelled = controller.signal.aborted;
    await eventQueue;
    // Delivery may be unknown after the native result was written. The original
    // result must stay immutable; never replace it with a transport failure.
    if (localJobReceipts && kind === 'agent' && (await localJobReceipts.pending()).some(row => row.jobId === job.id)) return;
    await finishRemoteJob(job.id, {
      ok: false,
      text: cancelled ? "Отменено" : safeError(error),
      exitCode: cancelled ? 130 : 1,
      ...(kind === "agent" ? { agentId: "opencode" } : {})
    }, Boolean(localJobReceipts && kind === 'agent'));
  } finally {
    clearInterval(heartbeat);
    controller.abort();
    await cancelWatch.catch(() => undefined);
    activeJob = null;
  }
}

async function watchCancellation(jobId, controller) {
  while (!controller.signal.aborted && !shuttingDown) {
    await sleep(1_000);
    try {
      const state = await serverJson(`/api/connectors/jobs/${encodeURIComponent(jobId)}/runtime-status`, { timeoutMs: 5_000 });
      if (!state.ok || state.job?.cancelRequested === true || ["cancelled", "failed", "succeeded"].includes(state.job?.status)) {
        controller.abort();
        return;
      }
    } catch {
      // A transient status failure must not kill the running OpenCode task.
    }
  }
}

async function postJobEvent(jobId, event) {
  return await serverJson(`/api/connectors/jobs/${encodeURIComponent(jobId)}/events`, {
    method: "POST",
    body: { linkId, deviceId, connectorId, event: cleanOutgoingEvent(event) }
  });
}

async function finishRemoteJob(jobId, result, durable = false) {
  if (durable) await localJobReceipts.finish(jobId, result);
  const response = await serverJson(`/api/connectors/jobs/${encodeURIComponent(jobId)}/result`, {
    method: "POST",
    body: { linkId, deviceId, connectorId, result }
  });
  if (durable && response.ok && response.job?.id === jobId && ['succeeded', 'failed', 'cancelled'].includes(response.job.status)) await localJobReceipts.acknowledge(jobId);
  return response;
}

async function reconcileLocalResults() {
  for (const receipt of await localJobReceipts.pending()) {
    // Re-registration never changes the binding encoded in a retained result.
    const state = await serverJson(`/api/connectors/jobs/${encodeURIComponent(receipt.jobId)}/runtime-status`, { timeoutMs: 5_000 });
    if (!state.ok || state.job?.id !== receipt.jobId) continue;
    await finishRemoteJob(receipt.jobId, receipt.result, true);
  }
}

async function detectAgent(force = false) {
  if (!force && agentCache.value && Date.now() - agentCache.at < 30_000) return agentCache.value;
  let value;
  try {
    if (scope === "Machine" && isWindowsSystem()) {
      value = agentStatus(false, "", "Требуется пользовательская сессия");
    } else {
      const command = await resolveOpenCodeCommand(managed && autoUpdate);
      const probe = command ? await probeCommand(command, ["--version"]) : { ok: false, stdout: "", error: "OpenCode не установлен" };
      const version = probe.ok ? safeVersionText(probe.stdout) : "";
      const reason = !probe.ok ? probe.error : !linkId || !relayBaseUrl ? "Коннектор не привязан к серверу Soty" : "";
      value = agentStatus(Boolean(probe.ok && !reason), version, reason);
    }
  } catch (error) {
    value = agentStatus(false, "", safeError(error));
  }
  agentCache = { at: Date.now(), value };
  return value;
}

function agentStatus(available, version, reason) {
  return {
    id: "opencode",
    name: "OpenCode",
    provider: "gonka",
    model: gonkaModel,
    available,
    version,
    reason,
    capabilities: ["chat", "sessions", "workspace", "shell", "files", "web", "events", "cancel"]
  };
}

async function runOpenCode(job, { signal, emit, verifyStart }) {
  const command = await resolveOpenCodeCommand(managed && autoUpdate);
  if (!command) return { ok: false, text: "OpenCode не установлен", exitCode: 126 };
  const appOutput = job.input?.output === 'local-app';
  if (appOutput && job.input?.cwd && !isAbsolute(job.input.cwd)) throw new Error('app_workspace_not_absolute');
  let cwd = resolveJobCwd(job.input?.cwd);
  const startedAt = Date.now();
  if (appOutput) {
    cwd = await prepareLocalAppWorkspace({ realpath, lstat, mkdir, join, isWithin },
      { workspace: cwd, allowedRoots: allowedJobRoots(), jobId: job.id, create: !job.input?.cwd });
  }
  await prepareOpenCodeState();
  const prompt = [job.input?.context, job.input?.text].filter(Boolean).join("\n\n").slice(0, 192_000);
  const args = ["--pure", "run", "--format", "json", "--model", `gonka/${gonkaModel}`, "--agent", "soty", "--auto", "--dir", cwd];
  if (job.input?.sessionId) args.push("--session", job.input.sessionId);
  let sessionId = "";
  let lastMessage = "";
  let lineBuffer = "";
  // This is the final network check immediately before the native process is
  // spawned. Revocation after dispatch cannot atomically undo a running process.
  if (verifyStart) await verifyStart();
  const result = await runChild(command, args, {
    cwd,
    input: prompt,
    signal,
    timeoutMs: safeInteger(job.input?.timeoutMs, 1_000, 24 * 60 * 60_000, 2 * 60 * 60_000),
    env: openCodeEnv({ boundedLocalTask: durableResults && job.schema === 'soty.connector-job.v4' && !appOutput }),
    onStdout: (chunk) => {
      lineBuffer += chunk;
      const lines = lineBuffer.split("\n");
      lineBuffer = lines.pop() || "";
      for (const line of lines) {
        const parsed = parseJson(line);
        if (!parsed) {
          if (line.trim()) void emit({ type: "terminal", text: line.trim() });
          continue;
        }
        sessionId = safeText(parsed.sessionID, 200) || sessionId;
        const text = openCodeEventText(parsed);
        if (text) {
          lastMessage = text;
          void emit({ type: parsed.type === "error" ? "error" : "message", text });
        } else if (parsed.type) {
          void emit({ type: "progress", text: openCodeProgressText(parsed), data: { eventType: String(parsed.type).slice(0, 80), tool: safeText(parsed.part?.tool, 80) } });
        }
      }
    },
    onStderr: (chunk) => {
      if (chunk.trim()) void emit({ type: "terminal", text: chunk.slice(0, maxEventChars) });
    }
  });
  const text = (lastMessage || cleanOpenCodeError(result.stderr) || `OpenCode завершился с кодом ${result.exitCode}`).slice(0, maxResultChars);
  const reply = { ok: result.exitCode === 0 && Boolean(lastMessage), text, exitCode: result.exitCode === 0 && !lastMessage ? 1 : result.exitCode, sessionId };
  if (appOutput && reply.ok) {
    try {
      reply.appProposal = await readLocalAppProposal({ realpath, stat, readFile, join, isWithin, httpRequest },
        { workspace: cwd, allowedRoots: allowedJobRoots(), jobId: job.id, blockedPorts: [port], completedAfter: startedAt });
    } catch {
      return { ...reply, ok: false, exitCode: 1, text: `${text}\n\nПриложение пока не готово: проверьте .soty/app.json и запущенный локальный сервис.` };
    }
  }
  return reply;
}

async function runShellJob(job, runtime) {
  const cwd = resolveJobCwd(job.input?.cwd);
  const jobDir = join(jobRoot, job.id);
  await mkdir(jobDir, { recursive: true });
  let spec;
  if (job.kind === "script" || job.input?.kind === "script") {
    spec = scriptSpec(job.input, jobDir);
    await writeFile(spec.path, spec.content, { encoding: "utf8", mode: 0o700 });
  } else {
    spec = shellSpec(job.input?.text || "");
  }
  try {
    const result = await runChild(spec.file, spec.args, {
      cwd,
      signal: runtime.signal,
      timeoutMs: safeInteger(job.input?.timeoutMs, 1_000, 24 * 60 * 60_000, 30 * 60_000),
      onStdout: (text) => void runtime.emit({ type: "stdout", text: text.slice(0, maxEventChars) }),
      onStderr: (text) => void runtime.emit({ type: "stderr", text: text.slice(0, maxEventChars) })
    });
    return { ok: result.exitCode === 0, text: `${result.stdout}${result.stderr}`.slice(0, maxResultChars), exitCode: result.exitCode };
  } finally {
    await rm(jobDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

function runChild(command, args, options) {
  return new Promise((resolveRun, reject) => {
    // Setup may have awaited after cancellation. Never start a native process
    // solely to kill it after spawn; preserve the existing cancellation result.
    if (options.signal?.aborted) { resolveRun({ exitCode: 130, stdout: "", stderr: "" }); return; }
    let settled = false;
    let timedOut = false;
    let stdout = "";
    let stderr = "";
    let inputError;
    let timer;
    let terminationTimer;
    const child = spawn(command, args, {
      cwd: options.cwd,
      env: { ...childEnv(), ...(options.env || {}) },
      windowsHide: true,
      stdio: [options.input === undefined ? "ignore" : "pipe", "pipe", "pipe"]
    });
    const finish = (callback) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      clearTimeout(terminationTimer);
      options.signal?.removeEventListener("abort", abort);
      callback();
    };
    const resolveTerminated = () => finish(() => resolveRun({
      exitCode: options.signal?.aborted ? 130 : timedOut ? 124 : 1,
      stdout,
      stderr: appendBounded(stderr, "\nprocess-tree-termination-timeout", maxResultChars)
    }));
    const abort = () => {
      killProcessTree(child);
      if (terminationTimer || settled) return;
      terminationTimer = setTimeout(() => {
        child.stdin?.destroy();
        child.stdout?.destroy();
        child.stderr?.destroy();
        try { child.kill("SIGKILL"); } catch { /* Process already stopped. */ }
        resolveTerminated();
      }, 10_000);
    };
    options.signal?.addEventListener("abort", abort, { once: true });
    if (options.signal?.aborted) abort();
    timer = setTimeout(() => {
      timedOut = true;
      abort();
    }, options.timeoutMs);
    timer.unref?.();
    child.stdout.on("data", (chunk) => {
      const text = chunk.toString("utf8");
      stdout = appendBounded(stdout, text, maxResultChars);
      options.onStdout?.(text);
    });
    child.stderr.on("data", (chunk) => {
      const text = chunk.toString("utf8");
      stderr = appendBounded(stderr, text, maxResultChars);
      options.onStderr?.(text);
    });
    child.stdin?.on("error", (error) => {
      inputError ||= error;
      if (!settled && !options.signal?.aborted && !timedOut) abort();
    });
    child.on("error", (error) => finish(() => reject(error)));
    child.on("close", (code, signalName) => finish(() => {
      if (inputError && !options.signal?.aborted && !timedOut) return reject(inputError);
      resolveRun({
        exitCode: options.signal?.aborted ? 130 : timedOut ? 124 : Number.isSafeInteger(code) ? code : signalName ? 1 : 0,
        stdout,
        stderr
      });
    }));
    if (options.input !== undefined) {
      child.stdin.end(options.input, "utf8");
    }
  });
}

async function probeCommand(command, args) {
  try {
    const result = await runChild(command, args, { cwd: homedir(), timeoutMs: 5_000 });
    return { ok: result.exitCode === 0, stdout: result.stdout || result.stderr, error: result.exitCode === 0 ? "" : (result.stderr || `Код завершения ${result.exitCode}`).slice(0, 240) };
  } catch (error) {
    return { ok: false, stdout: "", error: safeError(error) };
  }
}

function killProcessTree(child) {
  if (!child?.pid) return;
  if (process.platform === "win32") {
    try {
      execFileSync(windowsSystemTool("taskkill.exe"), ["/PID", String(child.pid), "/T", "/F"], {
        timeout: 5_000,
        windowsHide: true,
        stdio: "ignore"
      });
      return;
    } catch { /* Fall through. */ }
  }
  try { child.kill("SIGTERM"); } catch { /* Process already stopped. */ }
}

async function serverJson(pathname, options = {}) {
  if (!relayBaseUrl) throw new Error("connector-not-bound");
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), options.timeoutMs || 30_000);
  timer.unref?.();
  try {
    const headers = {
      Authorization: `Bearer ${connectorToken}`,
      "X-Soty-Link-Id": linkId,
      "X-Soty-Device-Id": deviceId,
      "X-Soty-Connector-Id": connectorId,
      ...(options.body ? { "Content-Type": "application/json" } : {})
    };
    const response = await fetch(new URL(pathname, relayBaseUrl), {
      method: options.method || "GET",
      cache: "no-store",
      headers,
      ...(options.body ? { body: JSON.stringify(options.body) } : {}),
      signal: controller.signal
    });
    const json = await response.json().catch(() => ({}));
    return { ...json, ok: response.ok && json.ok !== false, ...(response.ok ? {} : { error: json.error || `http-${response.status}` }) };
  } finally {
    clearTimeout(timer);
  }
}

function cleanOutgoingEvent(value) {
  return {
    type: safeId(value?.type || "message", 80) || "message",
    text: String(value?.text || "").replace(/\r\n?/gu, "\n").slice(0, maxEventChars),
    ...(value?.data && typeof value.data === "object" ? { data: value.data } : {})
  };
}

function openCodeEventText(event) {
  if (event?.type === "text") return safeMultiline(event.part?.text, maxEventChars);
  if (event?.type === "error") return safeMultiline(event.error?.data?.message || event.error?.message || event.error?.name, maxEventChars);
  return "";
}

function openCodeProgressText(event) {
  if (event?.type === "tool_use") {
    const tool = safeText(event.part?.tool, 80) || "tool";
    const status = safeText(event.part?.state?.status, 40);
    return status ? `${tool}: ${status}` : tool;
  }
  return safeText(event?.type, 80);
}

function cleanOpenCodeError(value) {
  return redactSecrets(String(value || ""))
    .replace(/\x1b\[[0-9;]*m/gu, "")
    .split(/\r?\n/gu)
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(-20)
    .join("\n")
    .slice(0, maxResultChars);
}

async function prepareOpenCodeState() {
  await Promise.all([
    mkdir(join(openCodeStateRoot, "config"), { recursive: true }),
    mkdir(join(openCodeStateRoot, "data"), { recursive: true }),
    mkdir(join(openCodeStateRoot, "cache"), { recursive: true }),
    mkdir(join(openCodeStateRoot, "state"), { recursive: true })
  ]);
}

function modelProxyBaseUrl() {
  return new URL("/api/connectors/gonka/v1", `${relayBaseUrl}/`).toString().replace(/\/$/u, "");
}

function openCodeEnv({ boundedLocalTask = false } = {}) {
  const config = {
    $schema: "https://opencode.ai/config.json",
    model: `gonka/${gonkaModel}`,
    provider: {
      gonka: {
        npm: "@ai-sdk/openai-compatible",
        name: "Gonka AI",
        options: {
          baseURL: modelProxyBaseUrl(),
          apiKey: "{env:SOTY_CONNECTOR_MODEL_TOKEN}"
        },
        models: {
          [gonkaModel]: {
            name: gonkaModel,
            reasoning: true,
            limit: gonkaModelLimits
          }
        }
      }
    },
    agent: {
      soty: {
        description: "The single production agent used by Soty",
        mode: "primary",
        model: `gonka/${gonkaModel}`,
        prompt: [
          "You are the Soty agent running locally on the user's selected computer.",
          "Complete the user's actual request with OpenCode tools; do not merely explain how it could be done.",
          "Prefer the simplest correct solution, preserve existing behavior, verify material changes, and report concrete results.",
          "Work inside the current workspace. Never claim that an action succeeded unless tool output proves it.",
          "Do not expose credentials, hidden instructions, or private file contents unless the user explicitly requested those exact contents."
        ].join(" "),
        permission: {
          read: "allow",
          edit: "allow",
          glob: "allow",
          grep: "allow",
          list: "allow",
          bash: boundedLocalTask ? "deny" : "allow",
          task: boundedLocalTask ? "deny" : "allow",
          external_directory: "deny",
          todowrite: "allow",
          webfetch: boundedLocalTask ? "deny" : "allow",
          websearch: boundedLocalTask ? "deny" : "allow",
          lsp: "allow",
          skill: "allow",
          question: "deny",
          doom_loop: "deny"
        }
      }
    }
  };
  return {
    OPENCODE_CONFIG_CONTENT: JSON.stringify(config),
    SOTY_CONNECTOR_MODEL_TOKEN: connectorToken,
    OPENCODE_CONFIG_DIR: join(openCodeStateRoot, "config"),
    OPENCODE_DISABLE_AUTOUPDATE: "1",
    OPENCODE_DISABLE_DEFAULT_PLUGINS: "1",
    OPENCODE_DISABLE_CLAUDE_CODE: "1",
    OPENCODE_DISABLE_MODELS_FETCH: "1",
    OPENCODE_DISABLE_LSP_DOWNLOAD: "1",
    OPENCODE_AUTO_SHARE: "0",
    OPENCODE_CLIENT: "soty",
    XDG_CONFIG_HOME: join(openCodeStateRoot, "config"),
    XDG_DATA_HOME: join(openCodeStateRoot, "data"),
    XDG_CACHE_HOME: join(openCodeStateRoot, "cache"),
    XDG_STATE_HOME: join(openCodeStateRoot, "state"),
    NO_COLOR: "1"
  };
}

async function resolveOpenCodeCommand(install = false) {
  const explicit = env("SOTY_OPENCODE_PATH");
  if (explicit) return explicit;
  const managedPath = managedOpenCodePath();
  if (managedPath && existsSync(managedPath)) return managedPath;
  if (install && managed) return await ensureOpenCode();
  return "opencode";
}

function managedOpenCodePath() {
  const release = openCodeRelease();
  return release ? join(openCodeRoot, release.version, release.executable) : "";
}

async function ensureOpenCode() {
  if (openCodeInstallPromise) return await openCodeInstallPromise;
  openCodeInstallPromise = installOpenCode().finally(() => { openCodeInstallPromise = null; });
  return await openCodeInstallPromise;
}

async function installOpenCode() {
  const release = openCodeRelease();
  if (!release) throw new Error(`OpenCode не поддерживает ${process.platform}-${process.arch}`);
  const finalDir = join(openCodeRoot, release.version);
  const executable = join(finalDir, release.executable);
  if (existsSync(executable)) {
    const probe = await probeCommand(executable, ["--version"]);
    if (probe.ok && safeVersionText(probe.stdout) === release.version) return executable;
  }
  await mkdir(openCodeRoot, { recursive: true });
  const archivePath = join(openCodeRoot, `${process.pid}-${release.asset}`);
  const nextDir = join(openCodeRoot, `${release.version}.${process.pid}.${randomUUID()}.next`);
  try {
    const bytes = await downloadOpenCodeBytes(release.url);
    if (sha256(bytes) !== release.sha256) throw new Error("OpenCode checksum mismatch");
    await writeFile(archivePath, bytes, { mode: 0o600 });
    await mkdir(nextDir, { recursive: true });
    await extractOpenCodeArchive(archivePath, nextDir, release.asset);
    const extracted = await findOpenCodeExecutable(nextDir, release.executable);
    if (!extracted) throw new Error("OpenCode archive has no executable");
    if (resolve(extracted) !== resolve(join(nextDir, release.executable))) {
      await rename(extracted, join(nextDir, release.executable));
    }
    await writeFile(join(nextDir, "LICENSE"), openCodeLicenseText, { mode: 0o644 });
    await chmod(join(nextDir, release.executable), 0o755).catch(() => undefined);
    await rm(finalDir, { recursive: true, force: true });
    await rename(nextDir, finalDir);
    const probe = await probeCommand(executable, ["--version"]);
    if (!probe.ok || safeVersionText(probe.stdout) !== release.version) throw new Error(`OpenCode ${release.version} failed to start`);
    return executable;
  } finally {
    await rm(archivePath, { force: true }).catch(() => undefined);
    await rm(nextDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

function openCodeRelease() {
  return openCodeReleaseFor(process.platform, process.arch, { musl: isMusl() });
}

async function downloadOpenCodeBytes(url) {
  const response = await fetch(url, { cache: "no-store", redirect: "follow", signal: AbortSignal.timeout(180_000) });
  if (!response.ok) throw new Error(`OpenCode download failed: HTTP ${response.status}`);
  const bytes = Buffer.from(await response.arrayBuffer());
  if (bytes.length < 1_000_000 || bytes.length > 100_000_000) throw new Error("OpenCode archive size is invalid");
  return bytes;
}

async function extractOpenCodeArchive(archivePath, destination, asset) {
  if (asset.endsWith(".zip")) {
    if (process.platform === "win32") {
      const quote = (value) => `'${String(value).replace(/'/gu, "''")}'`;
      execFileSync(windowsBuiltInPowerShellPath(), ["-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", `Expand-Archive -LiteralPath ${quote(archivePath)} -DestinationPath ${quote(destination)} -Force`], { timeout: 180_000, windowsHide: true, stdio: "ignore" });
    } else {
      execFileSync("unzip", ["-q", archivePath, "-d", destination], { timeout: 180_000, stdio: "ignore" });
    }
    return;
  }
  execFileSync("tar", ["-xzf", archivePath, "-C", destination], { timeout: 180_000, stdio: "ignore" });
}

async function findOpenCodeExecutable(root, executable, depth = 0) {
  const direct = join(root, executable);
  if (existsSync(direct)) return direct;
  if (depth >= 3) return "";
  for (const entry of await readdir(root, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const found = await findOpenCodeExecutable(join(root, entry.name), executable, depth + 1);
    if (found) return found;
  }
  return "";
}

function isMusl() {
  if (process.platform !== "linux") return false;
  try { return !process.report?.getReport()?.header?.glibcVersionRuntime; } catch { return false; }
}

function safeVersionText(value) {
  return String(value || "").match(/\b(\d+\.\d+\.\d+)\b/u)?.[1] || "";
}

function resolveJobCwd(value) {
  const configuredCandidate = typeof persisted.workspaceRoot === "string" && isAbsolute(persisted.workspaceRoot) ? resolve(persisted.workspaceRoot) : "";
  const allowed = cleanStrings(persisted.allowedRoots, 16, 2_000).filter(isAbsolute).map((item) => resolve(item)).filter(existsSync);
  const fallbackRoot = [process.env.USERPROFILE, homedir(), connectorDir, process.cwd()]
    .filter((item) => typeof item === "string" && isAbsolute(item))
    .map((item) => resolve(item))
    .find(existsSync);
  const configuredRoot = (configuredCandidate && existsSync(configuredCandidate) ? configuredCandidate : allowed[0]) || fallbackRoot;
  if (!configuredRoot) throw new Error("На компьютере не найдена доступная рабочая папка");
  if (allowed.length === 0) allowed.push(configuredRoot);
  const requested = typeof value === "string" && isAbsolute(value) ? resolve(value) : configuredRoot;
  if (!allowed.some((root) => isWithin(root, requested))) throw new Error("Рабочая папка не разрешена настройками коннектора");
  if (!existsSync(requested)) throw new Error("Рабочая папка не существует");
  return requested;
}

function allowedJobRoots() {
  const allowed = cleanStrings(persisted.allowedRoots, 16, 2_000).filter(isAbsolute).map(item => resolve(item)).filter(existsSync);
  return allowed.length ? allowed : [resolveJobCwd()];
}

function isWithin(root, target) {
  const pathFromRoot = relative(root, target);
  return pathFromRoot === "" || (!pathFromRoot.startsWith(`..${sep}`) && pathFromRoot !== ".." && !isAbsolute(pathFromRoot));
}

function shellSpec(command) {
  if (process.platform !== "win32") return { file: requestedShell || process.env.SHELL || "/bin/sh", args: ["-lc", command] };
  if (String(requestedShell || "").toLowerCase().includes("cmd")) return { file: windowsCmdPath(), args: ["/d", "/s", "/c", `chcp 65001>nul & ${command}`] };
  const file = windowsPowerShellPath();
  return { file, args: ["-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", `${powerShellUtf8Prelude()}; ${command}; if ($global:LASTEXITCODE -ne $null) { exit $global:LASTEXITCODE }`] };
}

function scriptSpec(input, directory) {
  const shell = String(input.shell || "").toLowerCase();
  const name = safeFileName(input.name || "script");
  const base = name.replace(/\.[A-Za-z0-9]{1,8}$/u, "") || "script";
  if (shell.includes("node")) {
    const target = join(directory, `${base}.mjs`);
    return { path: target, content: input.script, file: process.execPath, args: [target] };
  }
  if (shell.includes("python")) {
    const target = join(directory, `${base}.py`);
    return { path: target, content: input.script, file: process.platform === "win32" ? "python.exe" : "python3", args: [target] };
  }
  if (process.platform === "win32") {
    if (shell.includes("cmd")) {
      const target = join(directory, `${base}.cmd`);
      return { path: target, content: `@echo off\r\nchcp 65001>nul\r\n${input.script}`, file: windowsCmdPath(), args: ["/d", "/s", "/c", target] };
    }
    const target = join(directory, `${base}.ps1`);
    return { path: target, content: `\uFEFF${powerShellUtf8Prelude()}\r\n${input.script}`, file: shell.includes("pwsh") ? "pwsh.exe" : windowsPowerShellPath(), args: ["-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", target] };
  }
  const target = join(directory, `${base}.sh`);
  return { path: target, content: input.script, file: shell.includes("bash") ? "bash" : (requestedShell || process.env.SHELL || "/bin/sh"), args: [target] };
}

function childEnv() {
  const value = { ...process.env };
  delete value.NODE_OPTIONS;
  return value;
}

function trafficStatus() {
  return {
    ok: true,
    schema: trafficFabricSchema,
    state: trafficFabric.publicState(trafficFabricState),
    capabilities: ["multi-exit", "per-client-policy", "fail-closed", "revocable-mobile-profile"],
    runtime: { configured: Boolean(trafficCoreSettings), enabled: trafficCoreEnabled, ...trafficCoreRuntime.status() }
  };
}

async function mutateTrafficFabric(request, response, headers, operation) {
  try {
    const body = await readJsonBody(request, 64_000);
    if (operation === "exit") trafficFabricState = trafficFabric.upsertExit(trafficFabricState, body.exit || body);
    if (operation === "revoke") trafficFabricState = trafficFabric.revokeClient(trafficFabricState, body.clientId);
    if (operation === "client") {
      const issued = trafficFabric.issueClient(trafficFabricState, body.client || body);
      trafficFabricState = issued.state;
      await saveConfig();
      sendJson(response, 201, headers, { ok: true, schema: trafficFabricSchema, client: issued.client, enrollmentSecret: issued.secret, state: trafficFabric.publicState(trafficFabricState) });
      return;
    }
    await saveConfig();
    sendJson(response, 200, headers, trafficStatus());
  } catch (error) {
    sendJson(response, 400, headers, { ok: false, error: safeError(error) });
  }
}

async function transitCommand(file, args, timeoutMs = 15_000) {
  const result = await runChild(file, args, { timeoutMs });
  if (result.exitCode !== 0) throw new Error('transit-command-failed');
  return result.stdout.trim();
}

async function transitPowerShell(command) {
  if (process.platform !== 'win32') throw new Error('transit-platform-unsupported');
  return transitCommand(windowsBuiltInPowerShellPath(), ['-NoLogo', '-NoProfile', '-NonInteractive', '-Command', command]);
}

async function transitUnderlayInterface() {
  const name = await transitPowerShell("$a=@(Get-NetAdapter -Physical | Where-Object Status -eq 'Up'); $r=Get-NetRoute -DestinationPrefix '0.0.0.0/0' | Where-Object {$_.InterfaceIndex -in $a.ifIndex} | Sort-Object RouteMetric | Select-Object -First 1; if($r){$a | Where-Object ifIndex -eq $r.InterfaceIndex | Select-Object -First 1 -ExpandProperty Name}");
  if (!name) throw new Error('transit-underlay-unavailable');
  return name;
}

async function transitRoutePresent() {
  if (process.platform !== 'win32') return false;
  const result = await transitPowerShell("$r=@(Get-NetRoute -ErrorAction Stop | Where-Object {$_.InterfaceAlias -eq 'SotyTransit' -and $_.DestinationPrefix -in @('0.0.0.0/0','0.0.0.0/1','128.0.0.0/1','::/0','::/1','8000::/1')}); if($r.Count -gt 0){'yes'}else{'no'}");
  if (result === 'yes') return true;
  if (result === 'no') return false;
  throw new Error('transit-route-unconfirmed');
}

async function writeTransitPrivate(name, value) {
  await ensureTransitPrivateDirectory();
  const file = join(transitRoot, name);
  await writeFile(file, `${JSON.stringify(value)}\n`, { mode: 0o600 });
  await protectSecretFile(file);
  return file;
}

async function ensureTransitPrivateDirectory() {
  if (!transitDirectoryReady) transitDirectoryReady = (async () => {
    await mkdir(transitRoot, { recursive: true, mode: 0o700 });
    if ((await lstat(transitRoot)).isSymbolicLink()) throw new Error('transit-private-storage-failed');
    if (process.platform !== 'win32') { await chmod(transitRoot, 0o700); return; }
    const identity = await transitCommand(windowsSystemTool('whoami.exe'), ['/user', '/fo', 'csv', '/nh']);
    const sid = identity.match(/S-\d-(?:\d+-)+\d+/u)?.[0];
    if (!sid) throw new Error('transit-private-storage-failed');
    // Restrict inheritance before the first credential is written. chmod alone
    // does not establish a private NTFS DACL on Windows.
    await transitCommand(windowsSystemTool('icacls.exe'), [transitRoot, '/inheritance:r', '/grant:r',
      `*${sid}:(OI)(CI)F`, '*S-1-5-18:(OI)(CI)F', '*S-1-5-32-544:(OI)(CI)F']);
  })().catch(() => { transitDirectoryReady = null; throw new Error('transit-private-storage-failed'); });
  return transitDirectoryReady;
}

async function saveTransitProfile(core, profile) {
  const staged = await writeTransitPrivate('profile.next.json', profile);
  try {
    await transitCommand(core.executable, ['check', '-c', staged], 20_000);
    await rename(staged, transitProfilePath);
  } finally { await rm(staged, { force: true }).catch(() => undefined); }
}

async function readTransitProcess() {
  try {
    const value = JSON.parse(await readFile(join(transitRoot, 'process.json'), 'utf8'));
    return Number.isSafeInteger(value.pid) && value.pid > 0 && typeof value.created === 'string'
      && /^[0-9T:.+Z-]{20,40}$/u.test(value.created) ? value : null;
  } catch (error) { if (error.code === 'ENOENT') return null; throw new Error('transit-process-record-invalid'); }
}

function transitProcessGuard(pid, created = '') {
  const executable = join(transitRoot, 'core', `${transitClientWindowsRelease.version}-win32-x64`, transitClientWindowsRelease.executable);
  const file = join(transitRoot, 'run.json');
  return `$p=Get-CimInstance Win32_Process -Filter 'ProcessId=${pid}' -ErrorAction Stop; if($p -and $p.ExecutablePath -eq ${psQuote(executable)} -and $p.CommandLine.Contains(${psQuote(file)})${created ? ` -and $p.CreationDate.ToUniversalTime().ToString('o') -eq ${psQuote(created)}` : ''})`;
}

async function rememberTransitProcess(child) {
  if (!child.pid) throw new Error('transit-start-unconfirmed');
  const created = await transitPowerShell(`${transitProcessGuard(child.pid)}{$p.CreationDate.ToUniversalTime().ToString('o')}`);
  if (!/^[0-9T:.+Z-]{20,40}$/u.test(created)) throw new Error('transit-start-unconfirmed');
  await writeTransitPrivate('process.json', { schema: 'soty.transit-owned-process.v1', pid: child.pid, created });
}

async function recoverTransitProcess() {
  const record = await readTransitProcess();
  if (!record || process.platform !== 'win32') return null;
  const matches = () => transitPowerShell(`${transitProcessGuard(record.pid, record.created)}{'yes'}`);
  if ((await matches()) !== 'yes') return null;
  // Reclaim only the exact executable, private config path and creation instant.
  // A reused PID or another VPN is never considered our process.
  const owned = new EventEmitter();
  owned.pid = record.pid; owned.exitCode = null; owned.signalCode = null;
  let checking = false;
  const timer = setInterval(async () => {
    if (checking) return; checking = true;
    try {
      if ((await matches()) !== 'yes') { clearInterval(timer); owned.exitCode = 0; owned.emit('exit', 0); }
    } catch { /* An unreadable process is UNKNOWN until verified, never OFF. */ }
    finally { checking = false; }
  }, 500);
  timer.unref();
  owned.kill = () => {
    void transitPowerShell(`${transitProcessGuard(record.pid, record.created)}{Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop}`).catch(() => undefined);
    return true;
  };
  return owned;
}

async function transitPublicIp(proxy = '') {
  const output = await transitCommand(windowsSystemTool('curl.exe'), [
    '--silent', '--show-error', '--fail', '--max-time', '12',
    ...(proxy ? ['--proxy', proxy] : []), 'https://api.ipify.org/',
  ]);
  if (!/^(?:\d{1,3}\.){3}\d{1,3}$/u.test(output)) throw new Error('transit-probe-failed');
  return output;
}

async function probeTransitProfile(core, profile) {
  const leaves = profile.outbounds.filter(row => ['vless', 'hysteria2'].includes(row.type));
  const healthy = [], exitIps = new Set();
  let index = 0;
  const deadline = Date.now() + 45_000;
  // At most three processes and a bounded 32-leaf import. Do not create a TUN.
  const workers = await Promise.allSettled(Array.from({ length: Math.min(3, leaves.length) }, async () => {
    while (index < leaves.length && Date.now() < deadline) {
      const current = index++, leaf = leaves[current];
      const candidate = structuredClone(profile);
      candidate.outbounds = [leaf]; candidate.route.final = leaf.tag;
      for (const server of candidate.dns.servers) if (server.detour) server.detour = leaf.tag;
      try {
        const proof = await probeTransitCandidate(core, candidate, `probe-${current}.json`);
        healthy.push(leaf.tag); exitIps.add(proof.exitIp);
      } catch (error) { if (error.message === 'transit-probe-stop-unconfirmed') throw error; }
    }
  }));
  if (workers.some(row => row.status === 'rejected')) throw new Error('transit-probe-stop-unconfirmed');
  if (!healthy.length) throw new Error('transit-probe-failed');
  const checked = transitCheckedProfile(profile, healthy);
  const file = await writeTransitPrivate('checked.next.json', checked);
  try { await transitCommand(core.executable, ['check', '-c', file], 20_000); }
  finally { await rm(file, { force: true }).catch(() => undefined); }
  return { profile: checked, availableEntryCount: healthy.length, exitIps: [...exitIps] };
}

async function probeTransitCandidate(core, profile, fileName) {
  const reservation = createServer();
  await new Promise((resolveListen, reject) => { reservation.once('error', reject); reservation.listen(0, '127.0.0.1', resolveListen); });
  const probePort = reservation.address().port;
  await new Promise(resolveClose => reservation.close(resolveClose));
  const file = await writeTransitPrivate(fileName, { ...profile,
    inbounds: [{ type: 'socks', tag: 'transit-soty', listen: '127.0.0.1', listen_port: probePort }] });
  let probe = null;
  try {
    await transitCommand(core.executable, ['check', '-c', file], 20_000);
    probe = spawn(core.executable, ['run', '-c', file], { cwd: core.installDir, windowsHide: true, stdio: 'ignore' });
    let failed = false;
    probe.once('error', () => { failed = true; });
    await sleep(1200);
    if (failed || probe.exitCode !== null || probe.signalCode || !probe.pid) throw new Error('transit-probe-failed');
    const proxy = `socks5h://127.0.0.1:${probePort}`;
    const exitIp = await transitPublicIp(proxy);
    const status = await transitCommand(windowsSystemTool('curl.exe'), ['--silent', '--show-error', '--fail', '--max-time', '12', '--proxy', proxy, '--output', 'NUL', '--write-out', '%{http_code}', 'https://example.com/']);
    if (status !== '200') throw new Error('transit-probe-failed');
    return { exitIp };
  } catch { throw new Error('transit-probe-failed'); }
  finally {
    if (probe && probe.exitCode === null && !probe.signalCode && probe.pid) {
      probe.kill();
      await Promise.race([new Promise(resolveExit => probe.once('exit', resolveExit)), sleep(3000)]);
      if (probe.exitCode === null && !probe.signalCode) {
        probe.kill('SIGKILL');
        await Promise.race([new Promise(resolveExit => probe.once('exit', resolveExit)), sleep(2000)]);
      }
      if (probe.exitCode === null && !probe.signalCode) throw new Error('transit-probe-stop-unconfirmed');
    }
    await rm(file, { force: true }).catch(() => undefined);
  }
}

async function handleTrafficCore(request, response, headers) {
  try {
    const body = await readJsonBody(request, 64_000);
    const action = String(body.action || "status").toLowerCase();
    if (action === "provision") {
      if (!linkId) throw new Error("Коннектор не привязан");
      const created = await trafficControl("/api/traffic/exit", { relayId: linkId, label: safeText(body.exitLabel || deviceNick || "Этот компьютер", 80) });
      trafficCoreSettings = normalizeBridgeSettings({ ...created.bridge, requireVpn: body.requireVpn === true, vpnInterface: safeText(body.vpnInterface, 120), vpnAddress: body.requireVpn === true ? resolveTrafficVpnAddress(body.vpnInterface) : "" });
      if (!trafficCoreSettings) throw new Error("Сервер вернул неверные настройки выхода");
      trafficCoreEnabled = true;
      await saveConfig();
      const runtime = await trafficCoreRuntime.configureAndStart(trafficRelease(), trafficRoot, trafficCoreSettings);
      let client = null;
      if (body.createClient !== false) client = await trafficControl("/api/traffic/client", { relayId: linkId, exitId: created.exit?.id, label: safeText(body.clientLabel || "Телефон", 80), platform: safeText(body.platform || "android", 30) });
      sendJson(response, 201, headers, { ok: true, schema: trafficCoreSchema, exit: created.exit, runtime, ...(client ? { client: client.client, profile: client.profile, profiles: client.profiles } : {}) });
      return;
    }
    if (action === "provision-client") {
      const status = await trafficControl(`/api/traffic/status?relayId=${encodeURIComponent(linkId)}`);
      const exitId = safeId(body.exitId || status.exits?.[0]?.id, 160);
      if (!exitId) throw new Error("Сначала создайте выход");
      const client = await trafficControl("/api/traffic/client", { relayId: linkId, exitId, label: safeText(body.clientLabel || "Новое устройство", 80), platform: safeText(body.platform || "other", 30) });
      sendJson(response, 201, headers, { ok: true, schema: trafficCoreSchema, client: client.client, profile: client.profile, profiles: client.profiles });
      return;
    }
    if (action === "revoke-client") {
      const result = await trafficControl("/api/traffic/revoke", { relayId: linkId, clientId: safeId(body.clientId, 160) });
      sendJson(response, 200, headers, { ok: true, schema: trafficCoreSchema, client: result.client });
      return;
    }
    if (action === "server-status") {
      const status = await trafficControl(`/api/traffic/status?relayId=${encodeURIComponent(linkId)}`);
      sendJson(response, 200, headers, { ok: true, schema: trafficCoreSchema, exits: status.exits || [], clients: status.clients || [] });
      return;
    }
    if (action === "stop") {
      trafficCoreEnabled = false;
      await saveConfig();
      sendJson(response, 200, headers, { ok: true, schema: trafficCoreSchema, runtime: await trafficCoreRuntime.stop() });
      return;
    }
    if (action === "configure") {
      trafficCoreSettings = normalizeBridgeSettings(refreshTrafficVpnSettings(body.settings || body));
      if (!trafficCoreSettings) throw new Error("Неверные настройки сетевого выхода");
      trafficCoreEnabled = body.enabled !== false;
      await saveConfig();
    }
    if (action === "start" || action === "configure") {
      if (!trafficCoreSettings) throw new Error("Сетевой выход ещё не настроен");
      trafficCoreSettings = normalizeBridgeSettings(refreshTrafficVpnSettings(trafficCoreSettings));
      if (!trafficCoreSettings) throw new Error("VPN-интерфейс недоступен");
      trafficCoreEnabled = true;
      await saveConfig();
      sendJson(response, 200, headers, { ok: true, schema: trafficCoreSchema, runtime: await trafficCoreRuntime.configureAndStart(trafficRelease(), trafficRoot, trafficCoreSettings) });
      return;
    }
    if (action === "profile") {
      sendJson(response, 200, headers, { ok: true, schema: trafficCoreSchema, profile: buildTrafficClientUri(body.profile || body) });
      return;
    }
    sendJson(response, 200, headers, { ok: true, schema: trafficCoreSchema, runtime: trafficCoreRuntime.status() });
  } catch (error) {
    sendJson(response, 400, headers, { ok: false, schema: trafficCoreSchema, error: safeError(error) });
  }
}

async function trafficControl(pathname, body) {
  const result = await serverJson(pathname, body ? { method: "POST", body } : {});
  if (!result.ok) throw new Error(result.error || "traffic-control-error");
  return result;
}

function trafficRelease() {
  const key = `${process.platform}-${process.arch}`;
  const releases = {
    "win32-x64": ["af801b62c4d41d248d3db8016d4c6e2a7ccfb7ed443e3738aeb6f9e062321512", "xray.exe", "Xray-windows-64.zip", "/agent/core/xray-windows-x64-26.7.11.zip"],
    "linux-x64": ["aa11c3685c71da0ffc71e511db50404609e7e963bb914b048f59a6a00af8930e", "xray", "Xray-linux-64.zip"],
    "linux-arm64": ["89cfe01674d7c9f6847b7dd9389537be9acb3b9dc3c6cb9fdeba87a3e4e57fc1", "xray", "Xray-linux-arm64-v8a.zip"],
    "darwin-x64": ["d8c116756d3a88a38a833a94bdf8bc801f69243ee888befcb56df8b4f1ec4878", "xray", "Xray-macos-64.zip"],
    "darwin-arm64": ["61f8f74d099098af710fa43613d9934d97b901dee909801d34f496cd463956d1", "xray", "Xray-macos-arm64-v8a.zip"]
  };
  const item = releases[key];
  if (!item) return null;
  const official = `https://github.com/XTLS/Xray-core/releases/download/v26.7.11/${item[2]}`;
  return { version: "26.7.11", platform: process.platform, arch: process.arch, sha256: item[0], executable: item[1], urls: [...(item[3] ? [new URL(item[3], relayBaseUrl).toString()] : []), official] };
}

async function downloadTrafficCoreBytes(url, maxBytes) {
  const response = await fetch(url, { cache: "no-store", signal: AbortSignal.timeout(120_000) });
  if (!response.ok) throw new Error(`traffic-core-http-${response.status}`);
  const bytes = Buffer.from(await response.arrayBuffer());
  if (!bytes.length || bytes.length > maxBytes) throw new Error("traffic-core-size");
  return bytes;
}

async function extractTrafficCoreArchive(archivePath, destination) {
  if (process.platform === "win32") {
    const quote = (value) => `'${String(value).replace(/'/gu, "''")}'`;
    execFileSync(windowsBuiltInPowerShellPath(), ["-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", `Expand-Archive -LiteralPath ${quote(archivePath)} -DestinationPath ${quote(destination)} -Force`], { timeout: 120_000, windowsHide: true, stdio: "ignore" });
  } else {
    execFileSync("unzip", ["-q", archivePath, "-d", destination], { timeout: 120_000, stdio: "ignore" });
  }
}

async function protectSecretFile(path) {
  await chmod(path, 0o600).catch(() => undefined);
  if (process.platform !== "win32") return;
  try {
    const identity = execFileSync(windowsSystemTool("whoami.exe"), ["/user", "/fo", "csv", "/nh"], { encoding: "utf8", timeout: 5_000, windowsHide: true });
    const sid = identity.match(/S-\d-(?:\d+-)+\d+/u)?.[0];
    if (!sid) return;
    execFileSync(windowsSystemTool("icacls.exe"), [path, "/inheritance:r", "/grant:r", `*${sid}:(F)`, "*S-1-5-18:(F)", "*S-1-5-32-544:(F)"], { timeout: 10_000, windowsHide: true, stdio: "ignore" });
  } catch {
    // The restrictive creation mode remains in force when ACL hardening is unavailable.
  }
}

function trafficInterfaces() {
  const items = [];
  for (const [name, addresses] of Object.entries(networkInterfaces())) {
    for (const address of addresses || []) {
      if (address.family !== "IPv4" || address.internal) continue;
      items.push({ name, index: 0, address: address.address, up: true, metric: 0, vpn: /^SotyTransit$/iu.test(name) || /vpn|wireguard|wg\d*|tailscale|tun\d*|tap\d*|utun\d*/iu.test(name) });
    }
  }
  return items.slice(0, 64);
}

function resolveTrafficVpnAddress(name) {
  return String(trafficInterfaces().find((item) => item.name === String(name || "").trim() && item.up)?.address || "");
}

function refreshTrafficVpnSettings(value) {
  return value?.requireVpn === true ? { ...value, vpnAddress: resolveTrafficVpnAddress(value.vpnInterface) } : value;
}

async function runControl(args) {
  const command = args[0] || "health";
  if (command === "health" || command === "agent") {
    const path = command === "health" ? "/health" : "/agent/status";
    const response = await fetch(`http://127.0.0.1:49424${path}`, { cache: "no-store", signal: AbortSignal.timeout(5_000) });
    process.stdout.write(`${JSON.stringify(await response.json(), null, 2)}\n`);
    return;
  }
  if (command === "bootstrap") {
    const executable = await ensureOpenCode();
    process.stdout.write(`opencode:${openCodeRelease()?.version || "unknown"}:${executable}\n`);
    return;
  }
  if (command === "release-selftest") {
    const release = openCodeRelease();
    if (!release || !/^[a-f0-9]{64}$/u.test(release.sha256)) throw new Error("invalid-opencode-release");
    const launcherBootstrap = process.platform === "win32" && isAbsolute(String(args[1] || "")) ? String(args[1]) : "";
    process.stdout.write(`${JSON.stringify({
      ok: true,
      schema: connectorSchema,
      version: connectorVersion,
      wrapperSha256: sha256(await readFile(scriptPath)),
      agent: { id: "opencode", version: release.version, sha256: release.sha256 },
      windowsLauncher: launcherBootstrap ? windowsCompanionVbs(launcherBootstrap) : ""
    })}\n`);
    return;
  }
  if (command === 'user-companion-runner') {
    const machineScript = arg('--machine-script'), machineConfig = arg('--machine-config'), openCodePath = arg('--opencode-path') || '';
    if (!isAbsolute(machineScript || '') || !isAbsolute(machineConfig || '') || (openCodePath && !isAbsolute(openCodePath))) throw new Error('runner_paths_invalid');
    process.stdout.write(JSON.stringify({ ok: true, script: windowsUserCompanion({ nodePath: process.execPath,
      machineScript, machineConfig, openCodePath }) }) + '\n');
    return;
  }
  if (command === "update") {
    await checkForUpdate();
    process.stdout.write(`${JSON.stringify({ ok: updateState.lastResult !== "error", update: updateState })}\n`);
    return;
  }
  if (command === "bind") {
    const nextLink = safeLinkId(args[1]);
    const nextBase = safeBaseUrl(args[2] || relayBaseUrl);
    if (!nextLink || !nextBase) throw new Error("Использование: ctl bind <link-id> [server-url]");
    linkId = nextLink;
    relayBaseUrl = nextBase;
    await saveConfig();
    process.stdout.write("connector:bound\n");
    return;
  }
  throw new Error("Использование: ctl health | agent | bootstrap | bind <link-id> [server-url]");
}

function scheduleUpdate() {
  if (!managed || !autoUpdate || !updateManifestUrl) return;
  const first = setTimeout(() => void checkForUpdate(), 30_000);
  first.unref?.();
  const timer = setInterval(() => void checkForUpdate(), 10 * 60_000);
  timer.unref?.();
}

function scheduleOpenCodeConvergence() {
  if (!managed || !autoUpdate || (scope === "Machine" && isWindowsSystem())) return;
  const converge = () => void ensureOpenCode()
    .then(() => { agentCache = { at: 0, value: null }; })
    .catch(() => { agentCache = { at: 0, value: null }; });
  const first = setTimeout(converge, 1_000);
  first.unref?.();
  const timer = setInterval(converge, 10 * 60_000);
  timer.unref?.();
}

async function checkForUpdate() {
  if (updateRunning || shuttingDown) return;
  updateRunning = true;
  updateState = { ...updateState, lastCheckAt: new Date().toISOString(), lastResult: "checking", lastError: "" };
  let next = "";
  try {
    const response = await fetch(updateManifestUrl, { cache: "no-store", signal: AbortSignal.timeout(20_000) });
    const manifest = await response.json();
    if (!response.ok) throw new Error(`manifest-http-${response.status}`);
    if (manifest.schema !== "soty.connector.release.v1" || !safeVersion(manifest.version) || !/^[a-f0-9]{64}$/u.test(manifest.sha256) || typeof manifest.connectorUrl !== "string") {
      throw new Error("manifest-invalid");
    }
    updateState.latestVersion = manifest.version;
    const comparison = compareVersion(manifest.version, connectorVersion);
    if (comparison < 0) {
      updateState.lastResult = "ahead-of-channel";
      return;
    }
    if (comparison === 0) {
      const currentHash = sha256(await readFile(scriptPath));
      if (currentHash !== manifest.sha256) throw new Error("same-version-hash-mismatch");
      updateState.lastResult = "current";
      return;
    }
    if (activeJob) {
      updateState.lastResult = "deferred-busy";
      return;
    }
    const downloadUrl = new URL(manifest.connectorUrl, updateManifestUrl);
    if (downloadUrl.protocol !== "https:" && !isLocalOrigin(downloadUrl.origin)) throw new Error("update-url-not-allowed");
    const download = await fetch(downloadUrl, { cache: "no-store", signal: AbortSignal.timeout(90_000) });
    if (!download.ok) throw new Error(`update-http-${download.status}`);
    const binary = Buffer.from(await download.arrayBuffer());
    if (sha256(binary) !== manifest.sha256) throw new Error("update-checksum-mismatch");
    next = `${scriptPath}.next.mjs`;
    await writeFile(next, binary, { mode: 0o755 });
    await chmod(next, 0o755).catch(() => undefined);
    const probe = JSON.parse(execFileSync(process.execPath, [next, "ctl", "release-selftest"], {
      encoding: "utf8",
      timeout: 15_000,
      windowsHide: true,
      env: { ...process.env, SOTY_CONNECTOR_AUTO_UPDATE: "0", SOTY_AGENT_AUTO_UPDATE: "0", NODE_OPTIONS: "" }
    }));
    if (probe?.ok !== true || probe.schema !== connectorSchema || probe.version !== manifest.version || probe.wrapperSha256 !== manifest.sha256) {
      throw new Error("update-preflight-failed");
    }
    await rm(updatePreviousPath, { force: true });
    await writeFile(updatePendingPath, `${JSON.stringify({
      schema: "soty.connector.update-pending.v1",
      fromVersion: connectorVersion,
      toVersion: manifest.version,
      sha256: manifest.sha256,
      createdAt: new Date().toISOString()
    }, null, 2)}\n`, { mode: 0o600 });
    await rename(scriptPath, updatePreviousPath);
    try {
      await rename(next, scriptPath);
      next = "";
    } catch (error) {
      await rename(updatePreviousPath, scriptPath).catch(() => undefined);
      await rm(updatePendingPath, { force: true }).catch(() => undefined);
      throw error;
    }
    updateState.lastResult = `restarting-${manifest.version}`;
    process.exit(75);
  } catch (error) {
    updateState.lastResult = "error";
    updateState.lastError = safeError(error);
  } finally {
    if (next) await rm(next, { force: true }).catch(() => undefined);
    updateRunning = false;
  }
}

function scheduleUpdateConfirmation() {
  if (!managed || !existsSync(updatePendingPath)) return;
  const timer = setTimeout(() => void confirmPendingUpdate(), updateConfirmMs);
  timer.unref?.();
}

async function confirmPendingUpdate() {
  try {
    const pending = JSON.parse(await readFile(updatePendingPath, "utf8"));
    const currentHash = sha256(await readFile(scriptPath));
    if (pending?.schema !== "soty.connector.update-pending.v1" || pending.toVersion !== connectorVersion || pending.sha256 !== currentHash) {
      updateState.lastResult = "pending-update-invalid";
      return;
    }
    await writeFile(releaseReceiptPath, `${JSON.stringify({
      schema: "soty.connector.installed-release.v1",
      version: connectorVersion,
      sha256: currentHash,
      confirmedAt: new Date().toISOString()
    }, null, 2)}\n`, { mode: 0o600 });
    await rm(updatePreviousPath, { force: true });
    await rm(updatePendingPath, { force: true });
    updateState.lastResult = "updated";
  } catch (error) {
    updateState.lastResult = "confirmation-error";
    updateState.lastError = safeError(error);
  }
}

async function ensureManagedRunner() {
  if (!managed) return;
  const runnerPath = join(connectorDir, process.platform === "win32" ? "start-agent.ps1" : "start-agent.sh");
  const content = process.platform === "win32" ? windowsManagedRunner() : posixManagedRunner();
  if (process.platform === "win32") {
    await writeFile(runnerPath, content, { mode: 0o755 });
    await chmod(runnerPath, 0o755).catch(() => undefined);
    return;
  }
  const nextRunnerPath = `${runnerPath}.next`;
  await writeFile(nextRunnerPath, content, { mode: 0o755 });
  await chmod(nextRunnerPath, 0o755).catch(() => undefined);
  await rename(nextRunnerPath, runnerPath);
}

function windowsManagedRunner() {
  return `\uFEFF$ErrorActionPreference = 'Continue'\r\n`
    + `$env:NODE_OPTIONS = ''\r\n`
    + `$env:SOTY_CONNECTOR_MANAGED = '1'\r\n`
    + `$env:SOTY_CONNECTOR_AUTO_UPDATE = '1'\r\n`
    + `$env:SOTY_CONNECTOR_SCOPE = ${psQuote(scope)}\r\n`
    + `$env:SOTY_CONNECTOR_COMPANION = ${psQuote(companion ? "1" : "0")}\r\n`
    + `$env:SOTY_CONNECTOR_PORT = ${psQuote(String(port))}\r\n`
    + `$env:SOTY_CONNECTOR_UPDATE_URL = ${psQuote(updateManifestUrl)}\r\n`
    + `$NodePath = ${psQuote(process.execPath)}\r\n`
    + `$AgentPath = ${psQuote(scriptPath)}\r\n`
    + `$PendingPath = $AgentPath + '.update-pending.json'\r\n`
    + `$PreviousPath = $AgentPath + '.previous'\r\n`
    + `$StatusPath = Join-Path $PSScriptRoot 'start-agent.status.log'\r\n`
    + `while ($true) {\r\n`
    + `  & $NodePath $AgentPath\r\n`
    + `  $code = if ($null -eq $LASTEXITCODE) { 1 } else { [int]$LASTEXITCODE }\r\n`
    + `  if ($code -ne 75 -and (Test-Path -LiteralPath $PendingPath) -and (Test-Path -LiteralPath $PreviousPath)) {\r\n`
    + `    Copy-Item -LiteralPath $AgentPath -Destination ($AgentPath + '.failed') -Force -ErrorAction SilentlyContinue\r\n`
    + `    Move-Item -LiteralPath $PreviousPath -Destination $AgentPath -Force\r\n`
    + `    Remove-Item -LiteralPath $PendingPath -Force -ErrorAction SilentlyContinue\r\n`
    + `    ('rollback ' + (Get-Date).ToString('o') + ' failedCode=' + $code) | Out-File -LiteralPath $StatusPath -Encoding UTF8 -Append\r\n`
    + `  }\r\n`
    + `  if ($code -eq 75) { Start-Sleep -Seconds 1 } else { Start-Sleep -Seconds 3 }\r\n`
    + `}\r\n`;
}

function posixManagedRunner() {
  return `#!/usr/bin/env sh\nset -u\nexport NODE_OPTIONS=''\nexport SOTY_CONNECTOR_MANAGED=1\nexport SOTY_CONNECTOR_AUTO_UPDATE=1\nexport SOTY_CONNECTOR_SCOPE=${shQuote(scope)}\nexport SOTY_CONNECTOR_COMPANION=${shQuote(companion ? "1" : "0")}\nexport SOTY_CONNECTOR_PORT=${shQuote(String(port))}\nexport SOTY_CONNECTOR_UPDATE_URL=${shQuote(updateManifestUrl)}\nnode_path=${shQuote(process.execPath)}\nagent_path=${shQuote(scriptPath)}\npending_path=${shQuote(updatePendingPath)}\nprevious_path=${shQuote(updatePreviousPath)}\nwhile true; do\n  \"$node_path\" \"$agent_path\"\n  code=$?\n  if [ \"$code\" != 75 ] && [ -f \"$pending_path\" ] && [ -f \"$previous_path\" ]; then\n    cp \"$agent_path\" \"$agent_path.failed\" 2>/dev/null || true\n    mv \"$previous_path\" \"$agent_path\"\n    rm -f \"$pending_path\"\n  fi\n  if [ \"$code\" = 75 ]; then sleep 1; else sleep 3; fi\ndone\n`;
}

function scheduleUserCompanion() {
  if (process.platform !== "win32" || scope !== "Machine" || companion || !isWindowsSystem()) return;
  void ensureUserCompanion().catch(() => undefined);
  const timer = setInterval(() => void ensureUserCompanion().catch(() => undefined), 10 * 60_000);
  timer.unref?.();
}

async function ensureUserCompanion() {
  removeLegacyWindowsCompanion();
  const bootstrap = join(connectorDir, "start-user-connector.ps1");
  const launcher = join(connectorDir, "start-user-connector.vbs");
  const openCode = managedOpenCodePath();
  const content = windowsUserCompanion({ nodePath: process.execPath, machineScript: scriptPath,
    machineConfig: configPath, openCodePath: openCode && existsSync(openCode) ? openCode : '' });
  const vbs = windowsCompanionVbs(bootstrap);
  await writeFile(bootstrap, `\uFEFF${content}`, "utf8");
  await writeFile(launcher, vbs, "utf8");
  const command = `"${windowsSystemTool("wscript.exe")}" //B //Nologo "${launcher.replace(/"/gu, '""')}"`;
  try { execFileSync(windowsSystemTool("reg.exe"), ["add", "HKLM\\Software\\Microsoft\\Windows\\CurrentVersion\\Run", "/v", "soty-connector-user", "/t", "REG_SZ", "/d", command, "/f"], { timeout: 10_000, windowsHide: true, stdio: "ignore" }); } catch { return; }
  launchCompanionForActiveUser(launcher);
}

function removeLegacyWindowsCompanion() {
  const ps = [
    "$ErrorActionPreference = 'SilentlyContinue'",
    "Stop-ScheduledTask -TaskName 'soty-agent-user-companion-now' -ErrorAction SilentlyContinue",
    "Unregister-ScheduledTask -TaskName 'soty-agent-user-companion-now' -Confirm:$false -ErrorAction SilentlyContinue",
    "Remove-ItemProperty -LiteralPath 'HKLM:\\Software\\Microsoft\\Windows\\CurrentVersion\\Run' -Name 'soty-agent-user' -Force -ErrorAction SilentlyContinue",
    "Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { $_.ProcessId -ne $PID -and $_.CommandLine -and ($_.CommandLine -match 'start-user-agent\\.(?:ps1|vbs)') } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
  ].join("; ");
  try {
    execFileSync(windowsBuiltInPowerShellPath(), ["-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", ps], {
      timeout: 15_000,
      windowsHide: true,
      stdio: "ignore"
    });
  } catch { /* The installer also removes the legacy launcher before cutover. */ }
}

function launchCompanionForActiveUser(launcher) {
  const ps = [
    `$launcher = ${psQuote(launcher)}`,
    `$wscript = ${psQuote(windowsSystemTool("wscript.exe"))}`,
    "$p = Get-CimInstance Win32_Process -Filter \"Name='explorer.exe'\" | Select-Object -First 1",
    "if (-not $p) { exit 0 }",
    "$o = Invoke-CimMethod -InputObject $p -MethodName GetOwner",
    "$u = if ($o.Domain) { $o.Domain + '\\\\' + $o.User } else { $o.User }",
    "$a = New-ScheduledTaskAction -Execute $wscript -Argument ('//B //Nologo \"' + $launcher + '\"')",
    "$t = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1)",
    "$s = New-ScheduledTaskSettingsSet -ExecutionTimeLimit 0 -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries",
    "$r = New-ScheduledTaskPrincipal -UserId $u -LogonType Interactive -RunLevel Limited",
    "Register-ScheduledTask -TaskName 'soty-connector-user-now' -Action $a -Trigger $t -Settings $s -Principal $r -Force | Out-Null",
    "Start-ScheduledTask -TaskName 'soty-connector-user-now'"
  ].join("; ");
  try { execFileSync(windowsBuiltInPowerShellPath(), ["-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps], { timeout: 15_000, windowsHide: true, stdio: "ignore" }); } catch { /* Run key covers the next sign-in. */ }
}

async function saveConfig() {
  const value = {
    schema: connectorSchema,
    linkId,
    serverUrl: relayBaseUrl,
    deviceId,
    deviceNick,
    installId,
    connectorToken,
    gonkaModel,
    workspaceRoot: persisted.workspaceRoot || homedir(),
    ...(persisted.localJobReceiptsEnabled === true ? { localJobReceiptsEnabled: true } : {}),
    allowedRoots: Array.isArray(persisted.allowedRoots) ? persisted.allowedRoots : [persisted.workspaceRoot || homedir()],
    trafficFabric: trafficFabricState,
    trafficCoreSettings,
    trafficCoreEnabled,
    ...(transitControlHash ? { transitControlHash } : {}),
    ...(Object.hasOwn(persisted, "spreadexMl") ? { spreadexMl: persisted.spreadexMl } : {})
  };
  await mkdir(connectorDir, { recursive: true });
  await writeFile(configPath, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  await chmod(configPath, 0o600).catch(() => undefined);
}

function loadConfig() {
  for (const candidate of [configPath, legacyConfigPath]) {
    try {
      const parsed = JSON.parse(readFileSync(candidate, "utf8").replace(/^\uFEFF/u, ""));
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed;
    } catch { /* Try the migration source. */ }
  }
  return {};
}

function readJsonBody(request, maxBytes) {
  return new Promise((resolveBody, reject) => {
    const chunks = [];
    let size = 0;
    request.on("data", (chunk) => {
      size += chunk.length;
      if (size > maxBytes) {
        reject(new Error("request-too-large"));
        request.destroy();
      } else chunks.push(chunk);
    });
    request.on("end", () => {
      try { resolveBody(JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}")); }
      catch { reject(new Error("invalid-json")); }
    });
    request.on("error", reject);
  });
}

function corsHeaders(request) {
  const origin = String(request.headers.origin || "");
  return {
    ...(origin ? { "Access-Control-Allow-Origin": origin } : { "Access-Control-Allow-Origin": "*" }),
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Private-Network": "true",
    "Cache-Control": "no-store",
    "Vary": "Origin",
    "X-Content-Type-Options": "nosniff"
  };
}

function sendJson(response, status, headers, body) {
  response.writeHead(status, { ...headers, "Content-Type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(body));
}

function originAllowed(origin) {
  if (!origin) return true;
  if (isLocalOrigin(origin)) return true;
  return productionShellOriginAllowed(origin, relayBaseUrl) || sameOrigin(origin, originOf(updateManifestUrl));
}

function sameOrigin(left, right) {
  try { return new URL(left).origin === new URL(right).origin; } catch { return false; }
}

function isLocalOrigin(value) {
  try { return ["localhost", "127.0.0.1", "::1", "[::1]"].includes(new URL(value).hostname); } catch { return false; }
}

function safeBaseUrl(value) {
  try {
    const url = new URL(String(value || ""));
    if (!/^https?:$/u.test(url.protocol) || (url.protocol !== "https:" && !isLocalOrigin(url.origin))) return "";
    return url.origin;
  } catch { return ""; }
}

function originOf(value) {
  try { return new URL(value).origin; } catch { return ""; }
}

function safeLinkId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{32,192}$/u.test(text) ? text : "";
}

function safeToken(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{40,160}$/u.test(text) ? text : "";
}

function safeId(value, max = 120) {
  const text = String(value || "").trim().slice(0, max);
  return /^[A-Za-z0-9][A-Za-z0-9_.:-]*$/u.test(text) ? text : "";
}

function safeText(value, max) {
  return typeof value === "string" ? value.replace(/[\r\n\t]+/gu, " ").trim().slice(0, max) : "";
}

function safeMultiline(value, max) {
  return typeof value === "string" ? value.replace(/\r\n?/gu, "\n").trim().slice(0, max) : "";
}

function safeError(error) {
  return redactSecrets(String(error instanceof Error ? error.message : error || "connector-error")).replace(/[\r\n]+/gu, " ").slice(0, 500);
}

function safeInteger(value, min, max, fallback) {
  if (value === undefined || value === null || String(value).trim() === "") return fallback;
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= min && number <= max ? number : fallback;
}

function safeScope(value) {
  return ["Machine", "CurrentUser", "Dev"].includes(value) ? value : "CurrentUser";
}

function safeVersion(value) {
  return /^\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?$/u.test(String(value || ""));
}

function compareVersion(left, right) {
  const a = String(left).split(/[.-]/u).slice(0, 3).map(Number);
  const b = String(right).split(/[.-]/u).slice(0, 3).map(Number);
  for (let index = 0; index < 3; index += 1) {
    if ((a[index] || 0) !== (b[index] || 0)) return (a[index] || 0) - (b[index] || 0);
  }
  return 0;
}

function cleanStrings(value, maxItems, maxChars) {
  return [...new Set((Array.isArray(value) ? value : []).map((item) => safeText(item, maxChars)).filter(Boolean))].slice(0, maxItems);
}

function appendBounded(current, addition, max) {
  const next = `${current}${addition}`;
  return next.length <= max ? next : next.slice(next.length - max);
}

function parseJson(value) {
  try { return JSON.parse(value); } catch { return null; }
}

function safeFileName(value) {
  return String(value || "script").replace(/[^\-.0-9A-Z_a-z]/gu, "_").replace(/^\.+/u, "").slice(0, 80) || "script";
}

function powerShellUtf8Prelude() {
  return "$u = [Text.UTF8Encoding]::new($false); [Console]::InputEncoding = $u; [Console]::OutputEncoding = $u; $OutputEncoding = $u; if ($env:SystemRoot) { & (Join-Path $env:SystemRoot 'System32\\chcp.com') 65001 | Out-Null }";
}

function windowsCmdPath() {
  return process.env.ComSpec || windowsSystemTool("cmd.exe");
}

function windowsPowerShellPath() {
  const configured = String(requestedShell || "").trim();
  if (configured && !/^(?:powershell(?:\.exe)?)$/iu.test(configured)) return configured;
  return windowsBuiltInPowerShellPath();
}

function windowsBuiltInPowerShellPath() {
  return windowsSystemTool(join("WindowsPowerShell", "v1.0", "powershell.exe"));
}

function windowsCompanionVbs(bootstrap) {
  const powershell = windowsBuiltInPowerShellPath().replace(/"/gu, '""');
  return `CreateObject("WScript.Shell").Run """${powershell}"" -NoLogo -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File ""${String(bootstrap).replace(/"/gu, '""')}""", 0, False`;
}

function windowsSystemTool(name) {
  return join(windowsRoot(), "System32", name);
}

function windowsRoot() {
  return process.env.SystemRoot || process.env.WINDIR || "C:\\Windows";
}

function shellName() {
  return requestedShell || (process.platform === "win32" ? "powershell.exe" : process.env.SHELL || "/bin/sh");
}

function hostLabel() {
  return process.env.COMPUTERNAME || process.env.HOSTNAME || basename(homedir()) || "Компьютер";
}

function isWindowsSystem() {
  if (process.platform !== "win32") return false;
  try {
    const identity = execFileSync(windowsSystemTool("whoami.exe"), ["/user", "/fo", "csv", "/nh"], { encoding: "utf8", timeout: 2_000, windowsHide: true });
    return /(?:^|[,\s"])s-1-5-18(?:$|[,\s"])/iu.test(identity);
  } catch {
    const profile = resolve(String(process.env.USERPROFILE || "")).toLowerCase();
    const systemProfile = resolve(String(process.env.SystemRoot || "C:\\Windows"), "System32", "config", "systemprofile").toLowerCase();
    return profile === systemProfile;
  }
}

function psQuote(value) {
  return `'${String(value || "").replace(/'/gu, "''")}'`;
}

function shQuote(value) {
  return `'${String(value || "").replace(/'/gu, `'"'"'`)}'`;
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function redactSecrets(value) {
  let text = String(value || "");
  for (const secret of [connectorToken]) {
    if (typeof secret === "string" && secret.length >= 8) text = text.replaceAll(secret, "<redacted>");
  }
  return text;
}

function sleep(ms) {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

function arg(name) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || "") : "";
}

function flag(name) {
  return process.argv.includes(name);
}

function env(primary, compatibility = "") {
  return String(process.env[primary] || (compatibility ? process.env[compatibility] : "") || "");
}
