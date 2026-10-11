export function createBatch({ now = () => new Date().toISOString() } = {}) {
  const startedAt = now(), serials = new Set();
  const counts = { verified: 0, unverified: 0, repeats: 0, withoutSerial: 0 };
  return {
    add(result) {
      if (!result?.written) return false;
      if (!result.verified) { counts.unverified++; return true; }
      const serial = typeof result.result?.serialNumber === 'string' ? result.result.serialNumber.trim().toLowerCase() : '';
      if (!serial) counts.withoutSerial++;
      else if (serials.has(serial)) counts.repeats++;
      else { serials.add(serial); counts.verified++; }
      return true;
    },
    get counts() { return { ...counts }; },
    get full() { return Object.values(counts).reduce((sum, value) => sum + value, 0) >= 1000; },
    report({ includeIdentifiers = false } = {}) { return { schema: 'soty.nfc-batch.v1', startedAt, exportedAt: now(), ...counts, ...(includeIdentifiers ? { serialNumbers: [...serials] } : {}), counting: 'distinct-observed-serials-not-guaranteed-physical-tag-identity' }; },
  };
}
