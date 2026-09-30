export interface CommandFileMetadata { name: string; type: string; size: number; autoDownload: boolean; delivery: string }
export interface CommandFileTransfer { write(text: string, flush?: boolean): Promise<string>; close(): void; hasPending(): boolean }
export function createCommandFileTransfer(options: {
  sendChunk: (id: string, meta: CommandFileMetadata, chunk: Uint8Array, index: number, total: number) => Promise<void>;
  report: (event: { state: 'started' | 'stored'; name: string; size: number }) => void;
  maxBytes: number;
  maxChunkBytes?: number;
}): CommandFileTransfer;
