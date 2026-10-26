import type { FieldDocument } from '../../modules/field/contract.mjs';
export interface OwnAppOverview { id: string; name: string; state: 'ready' | 'starting' | 'offline' | 'stopped'; createdAt: number }
export function unplacedOwnApps(apps: unknown[], accountId: string, shortcuts: FieldDocument['shortcuts']): OwnAppOverview[];
