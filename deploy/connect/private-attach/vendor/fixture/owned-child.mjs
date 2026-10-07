// Fixture-only native ownership. This is not a host/engine lease or authority.
import { spawn, ChildProcess } from 'node:child_process';
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
const fail = code => { throw Object.assign(new Error(code), { code }); };
export async function withOwnedChild(command, args, options, work) {
  let child;
  let closed = false, failed = false, forced = false, code = null, signal = null, closing;
  let resolveClosed;
  const closedPromise = new Promise(resolve => { resolveClosed = resolve; });
  const closeNative = () => closing ??= (async () => {
    for (const stream of child.stdio || []) {
      try { stream?.destroy(); } catch { failed = true; }
    }
    if (!closed) await Promise.race([closedPromise, wait(2000)]);
    // ChildProcess native handle, not a supplied PID/selector. A known exited
    // child is reaped before close; never send a signal after native exit.
    if (!closed && child.exitCode === null && child.signalCode === null) {
      forced = true;
      try { ChildProcess.prototype.kill.call(child, 'SIGTERM'); } catch { failed = true; }
      await Promise.race([closedPromise, wait(2000)]);
    }
    if (!closed && child.exitCode === null && child.signalCode === null) {
      try { ChildProcess.prototype.kill.call(child, 'SIGKILL'); } catch { failed = true; }
      await Promise.race([closedPromise, wait(2000)]);
    }
    if (!closed) fail('fixture_child_native_cleanup_unknown');
  })();
  const owner = Object.freeze({ closedPromise, closeNative,
    snapshot: () => Object.freeze({ closed, failed, forced, code, signal }) });
  let result, thrown, hasThrown = false;
  child = spawn(command, args, options);
  try {
    // Every returned-handle setup operation is inside its owned finally.
    child.once('error', () => { failed = true; });
    child.once('close', (exitCode, exitSignal) => { closed = true; code = exitCode; signal = exitSignal; resolveClosed(); });
    for (const stream of child.stdio || []) stream?.on('error', () => {});
    result = await work(child, owner);
  }
  catch (error) { hasThrown = true; thrown = error; }
  finally { await closeNative(); }
  if (hasThrown) throw thrown; // No message/code/reason getter is inspected.
  if (failed || forced) fail('fixture_child_forced_or_failed_cleanup');
  return result;
}
