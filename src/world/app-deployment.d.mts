import type { AppInspection } from './app-settings.types';
import type { WorldAppRecord } from './types';
interface AppDeploymentBase {
  checkedAt: number; shellOrigin: string;
  app: { id: string; name: string; state: 'enabled' | 'revoked' };
  addresses: { revision: number; canonical: { id: string; origin: string } | null;
    aliases: { id: string; origin: string; state: 'bound' | 'tombstone'; active: boolean }[] };
  publication: { policyEpoch: number; launchPolicy: 'restricted' | 'anyone'; listed: boolean; activeTargetRevision: number };
}
interface AppDeploymentSource { port: number; entryPath: string; revision: number; digest: string; profile: string }
export interface AppDeploymentNativeEntry { origin: string; domainId: string; committedPolicyEpoch: number; admissionKeyHash: string;
  target: { revision: number; digest: string; profile: 'soty.relay-restricted.v1'; port: number; entryPath: string } }
export type AppDeployment = AppDeploymentBase & (
  { schema: 'soty.app-deployment.v1'; source: AppDeploymentSource } |
  { schema: 'soty.app-deployment.v2'; source: AppDeploymentSource & { launchEntry: {domainId:string;origin:string;path:string}|null }; nativeEntry: AppDeploymentNativeEntry|null }
);
export class AppDeploymentError extends Error { code: string; }
export function deploymentOrigin(value: unknown): string;
export function preferredInspectionEntry(snapshot: AppInspection): WorldAppRecord['entry'];
export function exportAppDeployment(snapshot: AppInspection, shellOrigin: string): AppDeployment;
export function validateAppDeployment(value: unknown): AppDeployment;
