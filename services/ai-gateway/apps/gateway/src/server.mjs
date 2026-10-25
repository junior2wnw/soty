import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { migrate } from './migrate.mjs';
import { Store } from './store.mjs';
import { createApp } from './app.mjs';
import { importLegacyKeys } from './legacy-import.mjs';
import { createConnector, providersFromEnv } from '../../../packages/connector/index.mjs';
import { customerTimeoutsFromEnv } from './customer-timeouts.mjs';
import { loadSotyBridgeConfig, createSotyBridge } from './soty-bridge.mjs';

export async function startServer(env = process.env) {
  const timeouts = customerTimeoutsFromEnv(env);
  const bridgeConfig = await loadSotyBridgeConfig(env.SOTY_BRIDGE_CONFIG_FILE);
  const pool = new pg.Pool({ connectionString: env.DATABASE_URL, max: 12, idleTimeoutMillis: 30000, connectionTimeoutMillis: 5000 });
  pool.on('error', () => process.stderr.write('{"event":"database_connection_error"}\n'));
  await migrate(pool);
  const store = new Store(pool, { pepper: env.API_KEY_PEPPER, dailyServiceBudgetMicrorubles: env.SERVICE_DAILY_BUDGET_URUB ?? '50000000', dailyProbeBudgetMicrorubles: env.HEALTH_PROBE_DAILY_BUDGET_URUB ?? '10000000', responseCacheEnabled: env.RESPONSE_CACHE_ENABLED !== 'false', includedAllowance: bridgeConfig?.allowance });
  const imported = await importLegacyKeys(store, env.LEGACY_KEYS_FILE);
  const connector = createConnector({ providers: providersFromEnv(env), env, timeouts, store: store.connectorHooks(), onAttempt: attempt => store.recordAttempt(attempt) });
  await connector.refreshCatalog();
  const baseUrl = env.PUBLIC_BASE_URL ?? 'http://localhost:8080';
  const trustedProxies = env.TRUSTED_PROXY_CIDRS ? env.TRUSTED_PROXY_CIDRS.split(',').map(value => value.trim()).filter(Boolean) : ['127.0.0.1', '::1'];
  if (trustedProxies.some(value => !/^[0-9a-f.:/]+$/i.test(value))) throw new Error('Invalid trusted proxy network');
  const sotyBridge = bridgeConfig ? createSotyBridge({ config: bridgeConfig, store }) : null;
  const common = { store, connector, baseUrl, sotyBridge, customerDeadlineMs: timeouts.deadlineMs, releaseId: env.RELEASE_ID ?? 'development', cookieSecure: env.COOKIE_SECURE !== 'false', staticDirectory: env.WEB_DIST_DIR ?? fileURLToPath(new URL('../../web/dist/', import.meta.url)), trustProxy: env.TRUST_PROXY === 'true' ? trustedProxies : false };
  const publicApp = await createApp(common);
  const adminApp = await createApp({ ...common, admin: true });
  await publicApp.listen({ host: env.HOST ?? '0.0.0.0', port: Number(env.PORT ?? 8080) });
  await adminApp.listen({ host: env.ADMIN_HOST ?? '127.0.0.1', port: Number(env.ADMIN_PORT ?? 8081) });
  await connector.startHealthWorker({ enabled: env.HEALTH_PROBES_ENABLED === 'true', reserveUrub: env.HEALTH_PROBE_RESERVE_URUB ?? '100000', dailyBudgetUrub: store.dailyProbeBudget.toString() });
  let sweeping = false;
  const maintenance = setInterval(async () => {
    if (sweeping) return;
    sweeping = true;
    try { await store.sweep(); } catch { process.stderr.write('{"event":"maintenance_failed"}\n'); }
    finally { sweeping = false; }
  }, 60_000);
  maintenance.unref();
  const close = async () => { clearInterval(maintenance); await connector.stop(); await Promise.all([publicApp.close(), adminApp.close()]); await pool.end(); };
  process.stdout.write(JSON.stringify({ event: 'gateway_started', port: Number(env.PORT ?? 8080), models: connector.models().length, legacyImported: imported.imported, legacyUnchanged: imported.unchanged, secureCookie: common.cookieSecure }) + '\n');
  return { publicApp, adminApp, store, connector, pool, close };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  startServer().then(runtime => {
    for (const signal of ['SIGTERM', 'SIGINT']) process.once(signal, () => runtime.close().then(() => process.exit(0), () => process.exit(1)));
  }).catch(error => {
    const code = /^[A-Za-z0-9_]{1,80}$/.test(error?.code ?? '') ? error.code : 'configuration_or_initialization_failed';
    process.stderr.write(JSON.stringify({ event: 'gateway_start_failed', code, name: String(error?.name ?? 'Error').replace(/[^A-Za-z]/g, '').slice(0, 40) }) + '\n');
    process.exitCode = 1;
  });
}
