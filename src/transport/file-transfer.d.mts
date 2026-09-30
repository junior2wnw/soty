export const maxFileBytes: number;
export const fileChunkBytes: number;
export const maxWireChunkBytes: number;
export const maxSocketBufferedBytes: number;
export const maxPendingFileBytes: number;
export const maxPendingFileChunks: number;
export const maxControlQueueBytes: number;
export const maxControlQueueCount: number;
export class FileTransferError extends Error { readonly code: string; readonly fileId?: string; constructor(code: string, fileId?: string); }
export function wireBytes(value: string): number;
export function estimatedChunkWireBytes(bytes: number): number;
export function validChunk(file: unknown): boolean;
export function ciphertextMatchesBytes(value: unknown, bytes: number): boolean;
export function createFileOutbox(options: {
  socket: () => { readonly readyState: number; readonly bufferedAmount: number; send(value: string): void } | null;
  clock?: () => number; timeoutMs?: number; tickMs?: number; bytesPerSecond?: number;
  maxBytes?: number; maxEntries?: number; socketBytes?: number;
}): {
  reserve(id: string, bytes: number): { send(wire: string): Promise<void>; cancel(error?: Error): void };
  ack(id: string): void; pump(): void; close(): void;
  reject(id: string, code: string): void;
  cancelFile(fileId: string): void;
  stats(): { pending: number; reservedBytes: number; committed: number };
};
