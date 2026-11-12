import { createConnectClient } from '../../modules/connect/browser/index.mjs';
import type { LocalState } from '../../modules/connect/browser/index.mjs';
import { createRootHostClient } from './app-host-retirement-client.mjs';

const listeners = new Set<(state: LocalState) => void>();
export function observeAccount(listener: (state: LocalState) => void): () => void {
  listeners.add(listener); return () => listeners.delete(listener);
}

// One queue and one durable browser identity across the new world and existing rooms.
let observeHostState: ((state: LocalState) => void) | undefined;
const client = createConnectClient({
  projectId: 'soty', endpoint: '/api/connect/rpc', dbName: 'soty-connect-v1',
  scopedAppCleanup: true,
  scopedVoiceCleanup: true,
  capturePlatformInvitation: true,
  onState: state => {
    observeHostState?.(state);
    const outcomes: Promise<void>[] = [];
    // Every committed observer runs synchronously; one screen fault cannot
    // prevent another owner from retiring its resources. SDK sees failures.
    for (const listener of [...listeners]) {
      try { outcomes.push(Promise.resolve(listener(state))); }
      catch (error) { outcomes.push(Promise.reject(error)); }
    }
    return Promise.all(outcomes).then(() => undefined);
  },
});
const rootHost = createRootHostClient(client);
observeHostState = rootHost.observe;
export const accountClient = rootHost.client;
