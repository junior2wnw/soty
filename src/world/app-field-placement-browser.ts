import { createAppFieldPlacementController as composePlacement, type AppFieldPlacementController } from './app-field-placement.mjs';
import { createFieldPersistence, type FieldLocalStore } from './field-persistence';
import type { DirectoryApi } from './field-directory';
export type { AppFieldPlacementController, AppFieldPlacementSnapshot, AppFieldPlacementResult } from './app-field-placement.mjs';

/** Browser composition has a distinct basename so Vite cannot resolve the
 * extensionless import to the host-neutral .mjs controller instead. */
export function createAppFieldPlacementController({ api, accountId, appId, isCurrent = () => true, store, signal, randomId, initialAppIds }:
  {api: DirectoryApi; accountId: string; appId: string; isCurrent?: () => boolean; store?: FieldLocalStore; signal?: AbortSignal; randomId?: () => string;
    initialAppIds?: readonly string[]}): AppFieldPlacementController {
  const persistence = createFieldPersistence({ api, accountId, isCurrent, ...(store ? { store } : {}) });
  try { return composePlacement({ persistence, accountId, appId, isCurrent, ...(signal ? { signal } : {}), ...(randomId ? { randomId } : {}),
    ...(initialAppIds ? { initialAppIds } : {}) }); }
  catch (error) { persistence.dispose(); throw error; }
}
