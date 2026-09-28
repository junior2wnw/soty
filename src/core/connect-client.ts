import { createConnectClient } from '../../modules/connect/browser/index.mjs';
import type { LocalState } from '../../modules/connect/browser/index.mjs';

const listeners = new Set<(state: LocalState) => void>();
export function observeAccount(listener: (state: LocalState) => void): () => void {
  listeners.add(listener); return () => listeners.delete(listener);
}

// One queue and one durable browser identity across the new world and existing rooms.
export const accountClient = createConnectClient({
  projectId: 'soty', endpoint: '/api/connect/rpc', dbName: 'soty-connect-v1',
  onState: state => { for (const listener of listeners) listener(state); },
});
