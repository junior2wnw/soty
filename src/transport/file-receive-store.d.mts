export const maxMemoryFileBytes: number;
export const maxMemoryReceiveBytes: number;
export interface ReceiveSpool {
  put(index: number, bytes: Uint8Array): Promise<void>;
  finish(type?: string): Promise<Blob>;
}
export function createFileReceiveStore(options?: { storage?: StorageManager; locks?: LockManager }): {
  create(fileId: string, totalBytes: number, totalChunks: number): ReceiveSpool;
  remove(fileId: string): Promise<void>;
  stats(): { files: number; operations: number; reservedMemory: number };
  close(): Promise<void>;
};
