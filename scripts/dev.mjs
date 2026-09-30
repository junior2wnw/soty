import { fork } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { createServer as createViteServer } from 'vite';

const rootDir = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const platformEnvironment = ['PATH', 'Path', 'SystemRoot', 'SYSTEMROOT', 'WINDIR', 'HOME', 'USERPROFILE', 'TMPDIR', 'TEMP', 'TMP', 'COMSPEC', 'ComSpec', 'PATHEXT'];

function portNumber(value, name) {
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1024 || port > 65535) throw new Error(`${name} must be a port between 1024 and 65535`);
  return port;
}

function developmentEnvironment(options) {
  const env = Object.fromEntries(platformEnvironment.filter(key => process.env[key] !== undefined).map(key => [key, process.env[key]]));
  Object.assign(env, {
    NODE_ENV: 'development', HOST: '127.0.0.1', PORT: String(options.apiPort), DATA_DIR: options.dataDir,
    SOTY_DIST_DIR: join(rootDir, 'public'), SOTY_CONNECT_ORIGINS: options.origin,
    SOTY_DISCOVERY_ORIGIN: options.origin,
    SOTY_APP_ORIGIN_TEMPLATE: `http://{appId}.${options.namedApps ? 'legacy.' : ''}localhost:${options.apiPort}`,
    ...(options.namedApps ? { SOTY_NAMED_APP_ZONE: `http://named.localhost:${options.apiPort}` } : {}),
    SOTY_LOCAL_CONNECTOR_PORT: String(options.connectorPort),
  });
  // Development never inherits production server credentials or database paths.
  // An inference provider is opt-in, using development-specific variables only.
  for (const field of ['GONKA_API_KEY', 'GONKA_BASE_URL', 'GONKA_APPLICATION_TOKENS']) {
    const value = process.env[`SOTY_DEV_${field}`];
    if (value) env[`SOTY_${field}`] = value;
  }
  return env;
}

export async function startDevelopment(input = {}) {
  const options = {
    host: input.host || '127.0.0.1',
    port: portNumber(input.port ?? 5173, 'UI port'),
    apiPort: portNumber(input.apiPort ?? 5174, 'API port'),
    connectorPort: portNumber(input.connectorPort ?? 49425, 'Connector port'),
    namedApps: input.namedApps === true,
    dataDir: resolve(input.dataDir || join(rootDir, 'var', 'dev', 'data')),
  };
  if (!['127.0.0.1', 'localhost'].includes(options.host)) throw new Error('Development host must be 127.0.0.1 or localhost');
  if (new Set([options.port, options.apiPort, options.connectorPort]).size !== 3) throw new Error('UI, API and connector ports must be different');
  options.origin = `http://${options.host}:${options.port}`;
  await mkdir(options.dataDir, { recursive: true });

  let vite, closing, childExited = false, settleDone;
  const done = new Promise(resolveDone => { settleDone = resolveDone; });
  const api = fork(join(rootDir, 'server', 'index.js'), [], {
    cwd: rootDir, env: developmentEnvironment(options), execArgv: [], windowsHide: true,
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  const exited = new Promise(resolveExit => api.once('exit', (code, signal) => {
    childExited = true; resolveExit();
    if (!closing) void close(new Error(`Development API stopped (${signal || code || 'unknown'})`));
  }));
  if (input.quiet) { api.stdout.resume(); api.stderr.resume(); }
  else { api.stdout.pipe(process.stdout, { end: false }); api.stderr.pipe(process.stderr, { end: false }); }
  const onAbort = () => { void close(); };
  input.signal?.addEventListener('abort', onAbort, { once: true });

  function close(error) {
    if (closing) return closing;
    closing = (async () => {
      input.signal?.removeEventListener('abort', onAbort);
      await vite?.close();
      if (!childExited) {
        if (api.connected) api.disconnect();
        else api.kill('SIGTERM');
        let timer;
        await Promise.race([exited, new Promise(resolveWait => { timer = setTimeout(resolveWait, 2000); timer.unref(); })]);
        clearTimeout(timer);
        if (!childExited) { api.kill('SIGKILL'); await exited; }
      }
      settleDone(error || null);
    })();
    return closing;
  }

  try {
    if (input.signal?.aborted) throw new Error('Development startup cancelled');
    await new Promise((resolveReady, reject) => {
      const timer = setTimeout(() => finish(new Error('Development API startup timed out')), 15_000);
      const onMessage = message => { if (message?.type === 'soty:ready' && message.port === options.apiPort) finish(); };
      const onError = () => finish(new Error('Cannot start development API'));
      const onExit = () => finish(new Error('Cannot start development API; check the API port and server output'));
      function finish(error) {
        clearTimeout(timer); api.off('message', onMessage); api.off('error', onError); api.off('exit', onExit);
        if (error) reject(error); else resolveReady();
      }
      api.on('message', onMessage); api.once('error', onError); api.once('exit', onExit);
    });
    const health = await fetch(`http://127.0.0.1:${options.apiPort}/health`, { signal: AbortSignal.timeout(3000) });
    if (!health.ok || (await health.json()).ok !== true) throw new Error('Development API health check failed');
    vite = await createViteServer({
      root: rootDir, configFile: join(rootDir, 'vite.config.ts'),
      envDir: false, envPrefix: 'SOTY_DEV_PUBLIC_',
      logLevel: input.quiet ? 'silent' : 'info',
      server: {
        host: options.host, port: options.port, strictPort: true,
        cors: { origin: options.origin }, allowedHosts: [options.host],
        watch: { ignored: ['**/var/**', '**/data/**', '**/output/**', '**/backups/**'] },
        fs: { deny: [
          '**/.env', '**/.env.*', '**/.git', '**/.git/**', `${rootDir.replaceAll('\\', '/')}/.codex*/**`,
          `${rootDir.replaceAll('\\', '/')}/{data,var,output,backups,deploy}/**`, '**/*{config,secrets}.json',
          '**/*.{crt,pem,key,pfx,p12,log,db,sqlite,sqlite3,sqlite-wal,sqlite-shm}',
        ] },
        proxy: {
          '^/(?:api|ws|agents)(?:/|\\?|$)|^/(?:health|ready)(?:\\?|$)': {
            target: `http://127.0.0.1:${options.apiPort}`, ws: true,
            // Preserve Host and Origin: Connect and room WebSockets validate
            // the browser's origin, including the frontend port.
            changeOrigin: false,
          },
        },
      },
    });
    if (closing) { await vite.close(); throw new Error('Development API stopped during startup'); }
    await vite.listen();
    if (!input.quiet) {
      console.log(`\nСоты: ${options.origin}`);
      console.log(`API и приложения: 127.0.0.1:${options.apiPort}; данные: ${options.dataDir}`);
      console.log(`Локальный dev-коннектор (отдельный запуск): 127.0.0.1:${options.connectorPort}`);
      console.log('Ctrl+C завершает интерфейс и API. Инструкции: docs/development-world.md\n');
    }
    return { origin: options.origin, apiPort: options.apiPort, connectorPort: options.connectorPort, close, done };
  } catch (error) {
    await close(error);
    throw error;
  }
}

async function main() {
  const { values } = parseArgs({ options: {
    host: { type: 'string', default: process.env.SOTY_DEV_HOST || '127.0.0.1' },
    port: { type: 'string', default: process.env.SOTY_DEV_PORT || '5173' },
    'api-port': { type: 'string', default: process.env.SOTY_DEV_API_PORT || '5174' },
    'connector-port': { type: 'string', default: process.env.SOTY_DEV_CONNECTOR_PORT || '49425' },
    'data-dir': { type: 'string', default: process.env.SOTY_DEV_DATA_DIR || join(rootDir, 'var', 'dev', 'data') },
    'named-apps': { type: 'boolean', default: false },
    help: { type: 'boolean', short: 'h' },
  } });
  if (values.help) {
    console.log('npm run dev -- [--host 127.0.0.1|localhost] [--port 5173] [--api-port 5174] [--connector-port 49425] [--data-dir path] [--named-apps]');
    return;
  }
  let running;
  const controller = new AbortController();
  const stop = () => controller.abort();
  process.once('SIGINT', stop); process.once('SIGTERM', stop);
  try {
    running = await startDevelopment({ host: values.host, port: values.port, apiPort: values['api-port'], connectorPort: values['connector-port'], dataDir: values['data-dir'], namedApps: values['named-apps'], signal: controller.signal });
    const error = await running.done;
    if (error) throw error;
  } catch (error) { if (!controller.signal.aborted) throw error; }
  finally { process.off('SIGINT', stop); process.off('SIGTERM', stop); }
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  main().catch(error => { console.error(`Development startup failed: ${error.message}`); process.exitCode = 1; });
}
