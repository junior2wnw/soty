import { createRootStartup } from './startup.js';

const root = await createRootStartup({ env: process.env });
let closing = false;
root.server.once('listening', () => {
  if (closing) return;
  console.log(`soty.online listening on ${root.server.address().port}`);
  console.log(`traffic tunnel ${root.trafficTunnel.enabled ? `enabled on ${root.trafficTunnel.publicPath}` : 'disabled'}`);
  if (process.send) process.send({ type: 'soty:ready', port: root.server.address().port });
});
function shutdown() {
  if (closing) return;
  closing = true;
  // The retained parent owns its existing 45s stop deadline. Keep this IPC
  // alive until the original close promise settles; a bare EXIT is no ACK.
  let acknowledge = process.connected === true && typeof process.send === 'function';
  try { if (acknowledge) process.channel.ref(); }
  catch { acknowledge = false; process.exitCode = 1; }
  const disconnect = () => {
    try { if (process.connected) process.disconnect(); }
    catch { process.exitCode = 1; }
  };
  const settled = outcome => {
    if (outcome === 'rejected') process.exitCode = 1;
    if (!acknowledge || !process.connected) return;
    try {
      process.send({ type: 'soty:closed', outcome }, error => {
        if (error) process.exitCode = 1;
        disconnect();
      });
    } catch { process.exitCode = 1; disconnect(); }
  };
  let cleanup;
  try { cleanup = root.close(); }
  catch { settled('rejected'); return; }
  void Promise.resolve(cleanup).then(() => settled('fulfilled'), () => settled('rejected'));
}
process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);
if (process.send) {
  process.once('disconnect', shutdown);
  process.on('message', (message, handle) => {
    if (handle !== undefined || !message || typeof message !== 'object' || Array.isArray(message)
      || Object.getPrototypeOf(message) !== Object.prototype) return;
    const fields = Reflect.ownKeys(message);
    if (fields.length !== 1 || fields[0] !== 'type') return;
    const type = Object.getOwnPropertyDescriptor(message, 'type');
    if (type && Object.hasOwn(type, 'value') && type.value === 'soty:shutdown') shutdown();
  });
}
try { await root.start(); }
catch (error) { await root.close().catch(() => {}); throw error; }
