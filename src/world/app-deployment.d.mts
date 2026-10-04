import type { AppInspection } from './app-settings.types';
import type { WorldAppRecord } from './types';
export interface AppDeployment {
  schema: 'soty.app-deployment.v1'; checkedAt: number; shellOrigin: string;
  app: { id: string; name: string; state: 'enabled' | 'revoked' };
  addresses: { revision: number; canonical: { id: string; origin: string } | null;
    aliases: { id: string; origin: string; state: 'bound' | 'tombstone'; active: boolean }[] };
  publication: { policyEpoch: number; launchPolicy: 'restricted' | 'anyone'; listed: boolean; activeTargetRevision: number };
  source: { port: number; entryPath: string; revision: number; digest: string; profile: string };
}
export class AppDeploymentError extends Error { code: string; }
export function deploymentOrigin(value: unknown): string;
export function preferredInspectionEntry(snapshot: AppInspection): WorldAppRecord['entry'];
export function exportAppDeployment(snapshot: AppInspection, shellOrigin: string): AppDeployment;
export function validateAppDeployment(value: unknown): AppDeployment;
