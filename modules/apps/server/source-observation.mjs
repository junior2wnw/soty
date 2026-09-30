// A report from the connector is limited evidence, not proof of a working UI,
// safe project, public DNS/TLS or immutable source code.
export const SOURCE_OBSERVATION_TTL_MS = 45_000;
const empty = state => ({ state, observedAt: null, freshUntil: null,
  evidence: state === 'offline' ? 'connector-offline' : 'not-observed' });

export function describeSourceObservation({ connected, observed, now = Date.now() }) {
  if (!connected) return empty('offline');
  if (!observed || !['ready', 'stopped'].includes(observed.state)
    || !Number.isSafeInteger(observed.at) || observed.at < 0
    || observed.at > now || observed.at > Number.MAX_SAFE_INTEGER - SOURCE_OBSERVATION_TTL_MS) return empty('unknown');
  const freshUntil = observed.at + SOURCE_OBSERVATION_TTL_MS;
  return { state: now < freshUntil ? (observed.state === 'ready' ? 'responding' : 'unreachable') : 'unknown',
    observedAt: observed.at, freshUntil, evidence: observed.evidence === 'connector-v2-observation' ? 'connector-v2-observation' : 'connector-v1-observation' };
}
