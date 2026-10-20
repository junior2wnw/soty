import { createServer } from 'node:http';
let stopping = false;
const server = createServer((req, res) => {
  if (req.url === '/health') { res.setHeader('Content-Type', 'application/json'); res.end('{"ok":true}'); }
  else process.send?.({ type: 'held-request' });
  // All other requests are deliberately held until the owned process stops.
});
function stop() {
  if (stopping) return; stopping = true;
  server.closeAllConnections(); server.close();
  if (process.connected) process.disconnect();
}
process.once('disconnect', stop); process.once('SIGTERM', stop); process.once('SIGINT', stop);
server.listen(Number(process.env.PORT || 0), '127.0.0.1', () => {
  process.send?.({ type: 'soty:ready', port: server.address().port });
});
