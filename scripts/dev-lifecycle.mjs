const STOP_MS = 2000;
const failure = (code, message) => Object.assign(new Error(message), { code });

function bounded(work, code, message) {
  let timer;
  return Promise.race([Promise.resolve().then(work), new Promise((_resolve, reject) => {
    timer = setTimeout(() => reject(failure(code, message)), STOP_MS);
  })]).finally(() => clearTimeout(timer));
}

/** Internal development resource ownership; no public server factory injection. */
export function createDevelopmentLifecycle({ api, frontend, signal }) {
  let closing, primaryFailure, childExited = api.exitCode !== null || api.signalCode !== null, childClosed = false, resolveClosed, settleDone;
  const closed = new Promise(resolve => { resolveClosed = resolve; });
  // Some hosts report exit, drained pipes and IPC disconnect without emitting
  // ChildProcess.close. Require all owned resources, never exit alone.
  const openPipes = new Set([api.stdout, api.stderr].filter(stream => stream && !stream.closed));
  const hadIPC = api.channel !== undefined;
  let ipcDisconnected = false;
  const checkClosed = () => {
    // connected=false may precede flushing queued IPC. Only the observed
    // disconnect or cleared known channel proves that owned resource closed.
    const ipcClosed = !hadIPC || ipcDisconnected || api.channel === null;
    if (childClosed || !childExited || !ipcClosed || openPipes.size) return;
    childClosed = true; api.off('disconnect', onDisconnect); resolveClosed();
  };
  const onDisconnect = () => { ipcDisconnected = true; checkClosed(); };
  for (const stream of openPipes) stream.once('close', () => { openPipes.delete(stream); checkClosed(); });
  api.once('disconnect', onDisconnect);
  const done = new Promise(resolve => { settleDone = resolve; });
  const stopping = new AbortController(), frontendCloses = new WeakMap();
  const observe = promise => { void promise.catch(() => {}); };
  api.once('exit', (code, signal) => {
    childExited = true; checkClosed();
    if (!closing) observe(close(new Error('Development API stopped (' + (signal || code || 'unknown') + ')')));
  });
  api.once('error', error => observe(close(error)));
  checkClosed();
  const onAbort = () => observe(close());
  signal?.addEventListener('abort', onAbort, { once: true });
  if (signal?.aborted) queueMicrotask(onAbort);

  function closeFrontend(server) {
    if (!server) return Promise.resolve();
    let pending = frontendCloses.get(server);
    if (!pending) {
      pending = bounded(() => server.close(), 'DEV_UI_CLOSE_TIMEOUT', 'Development UI shutdown did not finish.');
      frontendCloses.set(server, pending);
    }
    return pending;
  }
  async function closeBackend() {
    if (childClosed) return;
    if (!childExited) {
      if (api.connected) api.disconnect(); else api.kill('SIGTERM');
    }
    try { await bounded(() => closed, 'DEV_API_CLOSE_TIMEOUT', 'Development API shutdown did not finish.'); }
    catch (error) {
      if (!childExited) api.kill('SIGKILL');
      await bounded(() => closed, 'DEV_API_KILL_TIMEOUT', 'Development API did not exit after SIGKILL.');
      throw error;
    }
  }
  function close(error) {
    if (error && !primaryFailure) primaryFailure = error;
    if (closing) return closing;
    signal?.removeEventListener('abort', onAbort);
    stopping.abort(error || signal?.reason || failure('DEV_STOPPED', 'Development stopped.'));
    closing = Promise.allSettled([closeFrontend(frontend()), closeBackend()]).then(results => {
      const cleanupFailure = results.find(result => result.status === 'rejected')?.reason;
      const outcome = primaryFailure && cleanupFailure && primaryFailure !== cleanupFailure
        ? new AggregateError([primaryFailure, cleanupFailure], primaryFailure.message, { cause: primaryFailure })
        : primaryFailure || cleanupFailure || null;
      settleDone(outcome);
      if (cleanupFailure) throw outcome;
    });
    return closing;
  }
  return Object.freeze({ close, done, signal: stopping.signal,
    // A cancelled createViteServer can still finish later. Never publish it.
    adoptFrontend(server) { return stopping.signal.aborted ? closeFrontend(server) : Promise.resolve(); } });
}

/** Stop waiting immediately, while still observing a later factory settlement. */
export function awaitDevelopment(work, signal) {
  return new Promise((resolve, reject) => {
    const abort = () => reject(signal.reason || failure('DEV_STOPPED', 'Development stopped.'));
    if (signal.aborted) abort(); else signal.addEventListener('abort', abort, { once: true });
    Promise.resolve(work).then(value => { signal.removeEventListener('abort', abort); resolve(value); },
      error => { signal.removeEventListener('abort', abort); reject(error); });
  });
}
