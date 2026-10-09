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
  if (process.connected) process.disconnect();
  void root.close().catch(() => { process.exitCode = 1; });
}
process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);
if (process.send) process.once('disconnect', shutdown);
try { await root.start(); }
catch (error) { await root.close().catch(() => {}); throw error; }
