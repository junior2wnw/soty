export function addWaiter(map, key, req, res, buildPayload, timeoutMs = 30_000) {
  const waiter = {
    res,
    buildPayload,
    timer: setTimeout(() => {
      removeWaiter(map, key, waiter);
      res.json(buildPayload());
    }, Math.max(1000, timeoutMs))
  };
  res.on("close", () => {
    if (res.writableEnded) {
      return;
    }
    clearTimeout(waiter.timer);
    removeWaiter(map, key, waiter);
  });
  const waiters = map.get(key) || new Set();
  waiters.add(waiter);
  map.set(key, waiters);
}

export function flushWaiters(map, key, buildPayload = null) {
  const waiters = map.get(key);
  if (!waiters) {
    return;
  }
  map.delete(key);
  for (const waiter of waiters) {
    clearTimeout(waiter.timer);
    waiter.res.json((buildPayload || waiter.buildPayload)());
  }
}

function removeWaiter(map, key, waiter) {
  const waiters = map.get(key);
  if (!waiters) {
    return;
  }
  waiters.delete(waiter);
  if (waiters.size === 0) {
    map.delete(key);
  }
}
