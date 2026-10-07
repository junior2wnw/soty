// Private process-wide fence shared by HTTP middleware and native upgrade entry.
// It never creates an actor or performs a route/auth/domain operation.
const closing = new WeakMap();
const ignore = () => {};
function terminalErrors(stream) {
  if (stream.closed) return;
  const cleanup = () => stream.off('error', ignore);
  stream.once('error', ignore); stream.once('close', cleanup);
}
export function markHttpSocketClosing(socket) {
  if (closing.has(socket)) return;
  closing.set(socket, { following: 0 });
  socket.once('close', () => closing.delete(socket));
}
export const isHttpSocketClosing = socket => closing.has(socket);
function quarantine(request, socket, response) {
  const state = closing.get(socket);
  if (!state) return false;
  // One following parser-owned message is tolerated without destroying the
  // first rejection's response. Further pipeline is an unsupported abort.
  state.following++;
  request.pause(); socket.pause(); terminalErrors(request);
  if (response) { response.shouldKeepAlive = false; terminalErrors(response); }
  if (state.following > 1) socket.destroy();
  return true;
}
export function fenceClosingHttpSocket(req, res, next) {
  if (!quarantine(req, req.socket, res)) next();
}
export const fenceClosingUpgrade = (request, socket) => quarantine(request, socket);
