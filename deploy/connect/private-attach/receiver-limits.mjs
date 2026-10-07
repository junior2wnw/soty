// Fixed receiver bounds, independent of wire metadata. No reader specialization.
export const RECEIVER_LIMITS = Object.freeze({ plaintextBytes:256*1024*1024,
  fileBytes:128*1024*1024, extractedBytes:240*1024*1024, entries:10000,
  headers:10000, pathBytes:1024*1024, pathDepth:32, externalFiles:4,
  externalBytes:2*1024*1024, freeSpaceReserveBytes:16*1024*1024,
  wallMs:120000, idleMs:15000 });
