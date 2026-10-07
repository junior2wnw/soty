export type MemoryScope = Readonly<{
  issuer: string;
  accountId: string;
  audienceKind: 'personal' | 'project' | 'community';
  audienceId: string;
  projectId: string | null;
}>;
export type MemoryOperation = 'open' | 'remember' | 'recall' | 'supersede' | 'delete' | 'export';
export type CurrentLease = Readonly<{ leaseId: string; epoch: number; expiresAt: number }>;
export type MemoryRecordInput = Readonly<{
  id: string;
  type: 'fact' | 'decision' | 'preference' | 'error';
  text: string;
  source: string;
  freshUntil: number;
  retainUntil: number;
  importance?: number;
  confidence?: number;
}>;
export type MemoryRecordReference = Readonly<{ id: string; expectedRevision: number }>;
export type MemoryRecord = Required<MemoryRecordInput> & { revision: number; createdAt: number };
export type MemoryLimits = Readonly<{
  records: number; contentBytes: number; recordBytes: number; receipts: number; tombstones: number;
  recallLimit: number; candidates: number; embeddingConcurrency: number; embeddingTimeoutMs: number; retentionMs: number;
}>;
export type WriteReceipt = { id: string; revision: number; replaced: Array<{ id: string; revision: number }>;
  restoreFloor: number; replayed: boolean };
export type DeleteReceipt = { id: string; revision: number; erased: true; restoreFloor: number; replayed: boolean };
export type MemoryExport = {
  schema: 'soty.personal-memory-export.v1'; scope: MemoryScope; restoreFloor: number; exportedAt: number;
  records: MemoryRecord[];
  tombstones: Array<{ id: string; revision: number; erasedAt: number; restoreFloor: number; mutationId: string }>;
};
export interface MemoryPartition<Context = unknown> {
  remember(input: { context: Context; signal?: AbortSignal; mutationId: string; record: MemoryRecordInput }): Promise<WriteReceipt>;
  supersede(input: { context: Context; signal?: AbortSignal; mutationId: string;
    records: MemoryRecordReference[]; record: MemoryRecordInput }): Promise<WriteReceipt>;
  delete(input: { context: Context; signal?: AbortSignal; mutationId: string; record: MemoryRecordReference }): DeleteReceipt;
  recall(input: { context: Context; signal?: AbortSignal; query: string; limit?: number }): Promise<{ records: Array<MemoryRecord & { stale: boolean }> }>;
  export(input: { context: Context; signal?: AbortSignal }): MemoryExport;
  close(): void;
}
export function openMemoryPartition<Context = unknown>(options: {
  databasePath: string; scope: MemoryScope; context: Context;
  verifyContext(request: Readonly<{ context: Context; scope: MemoryScope; operation: MemoryOperation; partitionId: string }>): CurrentLease | false;
  readRestoreFloor(request: Readonly<{ scope: MemoryScope; partitionId: string }>): number;
  advanceRestoreFloor(request: Readonly<{ scope: MemoryScope; partitionId: string; expectedFloor: number;
    mutationId: string; erasedIds: readonly string[] }>): number;
  embedding?: Readonly<{ id: string; dimensions: number;
    embed(text: string, options: { purpose: 'record' | 'query'; signal: AbortSignal }): number[] | Float32Array | Promise<number[] | Float32Array> }>;
  clock?: () => number; limits?: Partial<MemoryLimits>;
}): MemoryPartition<Context>;
export class MemoryError extends Error { readonly code: string; constructor(code: string); }
export const DEFAULT_LIMITS: MemoryLimits;
export const SCHEMA: 'soty.personal-memory.sqlite.v1';
