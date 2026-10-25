export const DEFAULT_PROBE_PROFILES = Object.freeze({
  'MiniMaxAI/MiniMax-M2.7': { maxTokens: 512, timeoutMs: 20_000 },
  'deepseek-ai/DeepSeek-V4-Flash-0731': { maxTokens: 1024, timeoutMs: 45_000 },
  'zai-org/GLM-5.3-Flash': { maxTokens: 512, timeoutMs: 20_000 },
});

export function createHealthWorker({ models, refreshCatalog, probe, store = {}, now = Date.now, health, live, enabled = false, intervalMs = 10_000, catalogIntervalMs = 60_000, maxConcurrent = 2, dailyBudgetUrub = '50000000', dailyStartCap = 60_000, reserveUrub = null, probeMaxTokens = 64, probeTimeoutMs = 20_000, profiles = DEFAULT_PROBE_PROFILES, budgetTimezone = 'Asia/Yekaterinburg' }) {
  const inflight = new Map();
  const due = new Map();
  let timer, refreshing = false, ticking = false, stopped = false;
  let nextCatalogAt = now() + catalogIntervalMs;
  let nextEvaluationAt = 0;
  let date = '', starts = 0, localReserved = 0n;
  let lastTickAt = null, lastError = null;
  const budgetDay = new Intl.DateTimeFormat('en-CA', { timeZone: budgetTimezone, year: 'numeric', month: '2-digit', day: '2-digit' });
  const skip = (key, reason) => health.set(key, { ...health.get(key), skippedReason: reason });
  async function tick() {
    if (ticking || stopped || !live) return;
    ticking = true;
    try {
      lastTickAt = new Date(now()).toISOString();
      const today = budgetDay.format(new Date(now()));
      if (today !== date) { date = today; starts = 0; localReserved = BigInt(await store.loadDailyProbeSpend?.() ?? '0'); }
      if (now() >= nextCatalogAt && !refreshing) {
        refreshing = true; nextCatalogAt = now() + catalogIntervalMs;
        refreshCatalog().catch(() => { lastError = 'catalog_refresh_failed'; }).finally(() => { refreshing = false; });
      }
      if (now() < nextEvaluationAt) return;
      nextEvaluationAt = now() + intervalMs;
      const candidates = models().flatMap((model) => model.routes.map((route) => ({ model: model.id, modelEntry: model, route, key: `${route.providerId}:${model.id}` }))).sort((a, b) => (due.get(a.key) ?? 0) - (due.get(b.key) ?? 0));
      for (const { model, modelEntry, route, key } of candidates) {
        if (!enabled) { skip(key, 'probes_disabled'); continue; }
        const cost = typeof reserveUrub === 'string' && /^[1-9][0-9]*$/.test(reserveUrub) ? BigInt(reserveUrub) : null;
        if (!cost) { skip(key, 'probe_cost_unknown'); continue; }
        if (route.catalogState !== 'present') { skip(key, 'catalog_route_missing'); continue; }
        const prior = health.get(key);
        if (prior?.lastSuccessAt && now() - prior.lastSuccessAt < intervalMs) { skip(key, 'fresh_real_or_probe_success'); continue; }
        if (route.cooldownUntil && Date.parse(route.cooldownUntil) > now()) { skip(key, 'cooldown'); continue; }
        if (prior?.cooldownUntil > now()) { skip(key, 'cooldown'); continue; }
        if (inflight.has(key)) { skip(key, 'probe_inflight'); continue; }
        if (inflight.size >= maxConcurrent) { skip(key, 'probe_concurrency'); continue; }
        if ((due.get(key) ?? 0) > now()) continue;
        if (starts >= dailyStartCap) { skip(key, 'probe_start_cap'); continue; }
        if (localReserved + cost > BigInt(dailyBudgetUrub)) { skip(key, 'monitoring_budget_exhausted'); continue; }
        const reservation = store.reserveProbeSpend ? await store.reserveProbeSpend(cost.toString()) : { id: null };
        if (!reservation) { skip(key, 'monitoring_budget_exhausted'); continue; }
        localReserved += cost;
        if (!store.reserveProbeSpend) await store.addDailyProbeSpend?.(cost.toString());
        starts += 1;
        due.set(key, now() + intervalMs);
        const controller = new AbortController();
        const profile = profiles[model] ?? { maxTokens: probeMaxTokens, timeoutMs: probeTimeoutMs };
        const reservationDay = date;
        let conservativeCharge = cost;
        const running = probe(route.providerId, model, { signal: controller.signal, maxTokens: profile.maxTokens, timeoutMs: profile.timeoutMs }).then((result) => {
          const usage = result?.usage ?? result?.body?.usage;
          const inputRate = modelEntry.inputPriceMicrorublesPerMillion;
          const outputRate = modelEntry.outputPriceMicrorublesPerMillion;
          if (usage && Number.isSafeInteger(usage.prompt_tokens) && usage.prompt_tokens >= 0 && Number.isSafeInteger(usage.completion_tokens) && usage.completion_tokens >= 0 && usage.prompt_tokens + usage.completion_tokens > 0 && /^[1-9][0-9]*$/.test(inputRate ?? '') && /^[1-9][0-9]*$/.test(outputRate ?? '')) {
            // Retail cost is a conservative monitoring-budget estimate, not an
            // upstream invoice. Missing usage retains the whole reservation.
            conservativeCharge = (BigInt(usage.prompt_tokens) * BigInt(inputRate) + BigInt(usage.completion_tokens) * BigInt(outputRate) + 999_999n) / 1_000_000n;
          }
        }).catch(() => {}).finally(async () => {
          try {
            if (reservation.id && store.settleProbeSpend) {
              await store.settleProbeSpend(reservation.id, conservativeCharge.toString());
              if (date === reservationDay) localReserved += conservativeCharge - cost;
            }
          }
          catch { lastError = 'probe_settlement_failed'; }
          inflight.delete(key);
        });
        inflight.set(key, { controller, running });
      }
    } catch { lastError = 'monitoring_state_failed'; }
    finally { ticking = false; }
  }
  return {
    tick,
    start() { if (timer) return; stopped = false; timer = setInterval(() => void tick(), 1000); timer.unref?.(); void tick(); },
    async stop() { stopped = true; clearInterval(timer); timer = undefined; for (const { controller } of inflight.values()) controller.abort(); await Promise.allSettled([...inflight.values()].map((x) => x.running)); },
    snapshot: () => ({ running: Boolean(timer), enabled, intervalMs, lastTickAt, lastError, inflight: inflight.size, dailyStarts: starts, dailyReservedUrub: localReserved.toString(), dailyBudgetUrub, budgetTimezone, probeReserveUrub: reserveUrub, budgetCostBasis: 'conservative_configured_retail_estimate', profiles }),
  };
}
