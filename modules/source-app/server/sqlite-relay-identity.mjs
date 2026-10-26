import { createRelayIdentityVerifier } from './relay-identity.mjs';
import { createSqliteRelayNonceStore } from './sqlite-relay-nonces.mjs';

/** One verifier for one app on a local host. Close it when the app stops. */
export function createSqliteRelayIdentity({ issuer, appId, databasePath, fetch, clock, maxItems, busyTimeoutMs } = {}) {
  let store;
  // Validate the issuer and app ID before creating any local file.
  const verify = createRelayIdentityVerifier({ issuer, appId, fetch, clock,
    consume: (id, expiresAt) => store.consume(id, expiresAt) });
  store = createSqliteRelayNonceStore({ databasePath, clock, maxItems, busyTimeoutMs });
  return Object.freeze({ verify, close: () => store.close() });
}
