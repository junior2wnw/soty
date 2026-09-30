// Expiry must also run during cache admission; disconnected rooms may receive
// no further message that would otherwise prune their waiting join requests.
export function pruneWaiting(room, now = Date.now()) {
  for (const [requestId, waiting] of room.waiting) {
    const decisionAt = waiting.acceptedAt || waiting.deniedAt || 0;
    const startedAt = waiting.joinCreatedAt || waiting.disconnectedAt || waiting.rateStartedAt || now;
    if (now - (decisionAt || startedAt) <= (decisionAt ? 60_000 : 10 * 60_000)) continue;
    if (!decisionAt && waiting.ws?.readyState === 1) waiting.ws.close(1000, 'join expired');
    room.waiting.delete(requestId);
  }
}
