/** Bounded ceilings are implementation choices, not historical user requirements.
 * Writer enablement is separate from the compatible reader and offline migration. */
export const ATTACHMENT_LIMITS = Object.freeze({ batch: 10, chunkBytes: 512 * 1024,
  fileBytes: 25 * 1024 * 1024, batchBytes: 25 * 1024 * 1024, accountBytes: 256 * 1024 * 1024,
  storeBytes: 2 * 1024 * 1024 * 1024, databaseBytes: 3 * 1024 * 1024 * 1024, records: 20000, pendingMs: 24 * 60 * 60 * 1000 });
export function attachmentManifest(size, chunks) {
  if (!Number.isSafeInteger(size) || size < 0 || size > ATTACHMENT_LIMITS.fileBytes || !Array.isArray(chunks)
    || chunks.length !== Math.ceil(size / ATTACHMENT_LIMITS.chunkBytes)) throw Error('invalid_attachment_manifest');
  for (let index = 0; index < chunks.length; index++) {
    const part = chunks[index];
    if (!part || Object.keys(part).sort().join(',') !== 'bytes,sha256' || !/^[a-f0-9]{64}$/u.test(part.sha256)
      || part.bytes !== Math.min(ATTACHMENT_LIMITS.chunkBytes, size - index * ATTACHMENT_LIMITS.chunkBytes)) throw Error('invalid_attachment_manifest');
  }
  return JSON.stringify({ schema: 'soty.chat-attachment.chunks.v1', size, chunkBytes: ATTACHMENT_LIMITS.chunkBytes, chunks });
}
