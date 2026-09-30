import type { WorldAppPublication, WorldAppRecord } from './types';

export function describeAppAudience(app: Pick<WorldAppRecord, 'status' | 'ownerAccountId' | 'grants' | 'publication'>,
  accountId: string | null | undefined): { label: string; icon: string; publicNamed: boolean; details: string[] };
export function publicationFromInspection(snapshot: {
  app: { state: string };
  publication: { launchPolicy: 'restricted' | 'anyone'; activeDomainIds: string[] };
  addresses: { aliases: { id: string; state: string; active: boolean }[] };
}): WorldAppPublication;
