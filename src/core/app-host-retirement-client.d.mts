import type { ConnectClient, LocalState } from '../../modules/connect/browser/index.mjs';
export function createRootHostClient(client: ConnectClient, options?: { view?: Window }): {
  client: ConnectClient;
  observe(state: LocalState): void;
};
