const navigationFields = ['fetchStart', 'domainLookupStart', 'domainLookupEnd', 'connectStart', 'secureConnectionStart', 'connectEnd',
  'requestStart', 'responseStart', 'responseEnd', 'domInteractive', 'domContentLoadedEventEnd', 'loadEventEnd', 'workerStart', 'transferSize', 'encodedBodySize'] as const;
const resourceFields = ['startTime', 'workerStart', 'fetchStart', 'requestStart', 'responseStart', 'responseEnd', 'duration', 'transferSize', 'encodedBodySize'] as const;
type Clock = Pick<Performance, 'timeOrigin' | 'now' | 'getEntriesByType' | 'getEntriesByName'>;

/** Numeric local diagnostics only. URLs, RPC bodies, identities and credentials
 * never enter the DOM trace or leave the device. Navigation's own clock excludes
 * the time an automation command spent waiting before navigation actually began. */
export function recordStartupTiming(root: HTMLElement, clock: Clock = performance): void {
  try {
    const navigation = clock.getEntriesByType('navigation')[0] as PerformanceNavigationTiming | undefined;
    if (!navigation) { root.dataset.startupTimingStatus = 'unsupported'; return; }
    const numbers = (value: object, fields: readonly string[]) => Object.fromEntries(fields.flatMap(field => {
      const number = (value as Record<string, unknown>)[field];
      return typeof number === 'number' && Number.isFinite(number) && number >= 0 && number <= Number.MAX_SAFE_INTEGER
        ? [[field, Math.round(number)]] : [];
    }));
    if (root.dataset.startupEntryOffsetMs === undefined) {
      const offset = clock.now();
      if (Number.isFinite(offset) && offset >= 0) root.dataset.startupEntryOffsetMs = String(Math.round(offset));
    }
    if (Number.isFinite(clock.timeOrigin) && clock.timeOrigin > 0) root.dataset.startupTimeOrigin = String(Math.round(clock.timeOrigin));
    root.dataset.startupNavigation = JSON.stringify({ ...numbers(navigation, navigationFields),
      ...(['navigate', 'reload', 'back_forward', 'prerender'].includes(navigation.type) ? { type: navigation.type } : {}) });
    const script = Array.from(root.ownerDocument.querySelectorAll<HTMLScriptElement>('script[type="module"][src]'))
      .find(value => { const url = new URL(value.src, root.ownerDocument.baseURI); return url.origin === new URL(root.ownerDocument.baseURI).origin && /^\/assets\/index-[A-Za-z0-9_-]+\.js$/u.test(url.pathname); });
    const entry = script && clock.getEntriesByName(script.src).at(-1);
    if (entry) root.dataset.startupEntryResource = JSON.stringify(numbers(entry, resourceFields));
    root.dataset.startupTimingStatus = 'available';
  } catch { root.dataset.startupTimingStatus = 'unsupported'; }
}
