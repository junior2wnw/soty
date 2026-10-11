import { createServer } from 'node:net';
import { spawn } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const wait = ms => new Promise(done => setTimeout(done, ms));

// Keep the OS listener until the other fixture servers have bound. A port
// obtained by listen(0) and immediately closed is not a reservation.
export async function leaseLoopbackPort() {
  const server = createServer(socket => socket.destroy());
  await new Promise((done, fail) => {
    server.once('error', fail);
    server.listen(0, '127.0.0.1', done);
  });
  const port = server.address().port;
  let release;
  return { port, release() {
    return release ??= new Promise((done, fail) => server.close(error => error ? fail(error) : done()));
  } };
}

/** Owns only this disposable fixture's CLI. Never include child output, Env or
 * claim contents in diagnostics: these may contain identity/claim material. */
export function startScopedConnector({ port, directory, runtimeDir, origin, selectedHostFile }) {
  const child = spawn(process.execPath, [
    fileURLToPath(new URL('../../../../public/agent/soty-connector.mjs', import.meta.url)),
    '--port', String(port), '--scope', 'Dev',
  ], {
    windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'], cwd: runtimeDir,
    env: {
      PATH: [dirname(process.execPath), process.env.SystemRoot ? join(process.env.SystemRoot, 'System32') : '/usr/bin', '/bin'].join(process.platform === 'win32' ? ';' : ':'),
      ...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}),
      TEMP: directory, TMP: directory, USERPROFILE: directory,
      APPDATA: join(directory, 'appdata'), LOCALAPPDATA: join(directory, 'localappdata'),
      SOTY_CONNECTOR_DATA_DIR: runtimeDir, SOTY_CONNECTOR_SERVER_URL: origin,
      SOTY_CONNECTOR_LINK_ID: 'fixture_scoped_link_12345678901234567', SOTY_CONNECTOR_DEVICE_ID: 'fixture_scoped_host',
      SOTY_CONNECTOR_DEVICE_NICK: 'Synthetic scoped device', SOTY_CONNECTOR_AUTO_UPDATE: '0', SOTY_CONNECTOR_MANAGED: '0',
      SOTY_CONNECTOR_UPDATE_URL: origin + '/agent/manifest.json',
      ...(selectedHostFile ? { SOTY_SELECTED_EMBED_HOST_FILE: selectedHostFile } : {}),
    },
  });
  let listening = false, closed = false, stopping = false, failure = null;
  let outputBytes = 0, attempts = 0, lastHttpStatus = null, exitCode = null, signal = null;
  let complete;
  const completion = new Promise(done => { complete = done; });
  const diagnostics = () => ({ listening, closed, stopping, failure, outputBytes, attempts, lastHttpStatus, exitCode, signal });
  function error(code) {
    const value = new Error(code + ': ' + JSON.stringify(diagnostics()));
    value.code = code; value.diagnostics = diagnostics(); return value;
  }
  function lines(stream, inspect) {
    let tail = '';
    stream.on('data', bytes => {
      outputBytes += bytes.length;
      tail += bytes.toString('utf8');
      let boundary;
      while ((boundary = tail.indexOf('\n')) !== -1) {
        inspect(tail.slice(0, boundary).replace(/\r$/, ''));
        tail = tail.slice(boundary + 1);
      }
      // Bounded internal buffer only; never put the raw line in an error.
      if (tail.length > 8192) tail = tail.slice(-8192);
    });
  }
  lines(child.stdout, line => {
    const marker = /^soty-connector:(\d{1,5})$/.exec(line);
    if (marker) {
      if (Number(marker[1]) === port) listening = true;
      else failure ??= 'scoped_connector_listening_port_mismatch';
    }
  });
  lines(child.stderr, line => {
    if (!line.startsWith('soty-connector:http:')) return;
    const code = /\b(EADDRINUSE|EACCES|EADDRNOTAVAIL|EINVAL|EPERM)\b/.exec(line)?.[1];
    failure ??= 'scoped_connector_http_bind_' + (code ? code.toLowerCase() : 'error');
  });
  child.once('error', () => { failure ??= 'scoped_connector_spawn_error'; });
  child.once('exit', (code, exitSignal) => {
    exitCode = Number.isInteger(code) ? code : null;
    signal = ['SIGTERM', 'SIGKILL', 'SIGINT', 'SIGABRT', 'SIGSEGV'].includes(exitSignal) ? exitSignal : exitSignal ? 'other' : null;
    if (!stopping) failure ??= signal ? 'scoped_connector_child_signal' : 'scoped_connector_child_exit';
  });
  child.once('close', () => { closed = true; complete(); });

  async function waitForClaim(probe, timeout = 60000) {
    const deadline = Date.now() + timeout;
    const assertRunning = () => {
      if (failure) throw error(failure);
      if (stopping || closed) throw error('scoped_connector_stopped');
    };
    while (Date.now() < deadline) {
      assertRunning();
      // Do not accept an unrelated process's HTTP 200 on the same port.
      if (listening) {
        let result;
        attempts++;
        try { result = await probe(Math.min(10000, Math.max(1, deadline - Date.now()))); }
        catch { /* Only safe status/counters are retained; the next turn checks child failure. */ }
        assertRunning();
        if (Number.isInteger(result?.status) && result.status >= 100 && result.status <= 599) lastHttpStatus = result.status;
        if (result?.status === 200) {
          if (!result.body || !['hostDeviceId', 'connectorId', 'claimCode'].every(key => typeof result.body[key] === 'string' && result.body[key])) {
            throw error('scoped_connector_claim_payload_invalid');
          }
          return result.body;
        }
      }
      await wait(Math.min(30, Math.max(1, deadline - Date.now())));
    }
    assertRunning();
    throw error('scoped_connector_claim_timeout');
  }
  async function endedWithin(ms) {
    if (closed) return true;
    let timer;
    try { return await Promise.race([completion.then(() => true), new Promise(done => { timer = setTimeout(() => done(false), ms); })]); }
    finally { clearTimeout(timer); }
  }
  let stopped;
  function stop() {
    return stopped ??= (async () => {
      stopping = true;
      if (closed) return;
      child.kill('SIGTERM');
      if (await endedWithin(3000)) return;
      child.kill('SIGKILL');
      if (!await endedWithin(3000)) throw error('scoped_connector_cleanup_timeout');
    })();
  }
  return { waitForClaim, stop, diagnostics };
}
