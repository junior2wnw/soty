export interface JournalStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}
export interface RequestReservation {
  fingerprint: string;
  requestId: string;
  reused: boolean;
}
export interface CreateResponse {
  ok: boolean;
  error?: string;
  httpStatus?: number;
  job?: { id?: unknown };
}
const journalKey = "soty:connector:create-journal:v1";
const journalSchema = "soty.connector-create-journal.v1";
const capacity = 32;
const fingerprintPattern = /^[a-f0-9]{64}$/u;
const requestPattern = /^[A-Za-z0-9_.:-]{1,128}$/u;
type Entry = Pick<RequestReservation, "fingerprint" | "requestId">;
type Lock = <T>(operation: () => T | Promise<T>) => Promise<T>;

export function createRequestJournal({ storage, digest, randomId, lock }: {
  storage: JournalStorage;
  digest: (text: string) => Promise<string>;
  randomId: () => string;
  lock: Lock;
}) {
  function read(): Entry[] {
    const raw = storage.getItem(journalKey);
    if (raw === null) return [];
    if (raw.length > 16384) throw new Error("request-journal-invalid");
    const value = JSON.parse(raw);
    if (!value || value.schema !== journalSchema || Object.keys(value).length !== 2 || !Array.isArray(value.entries)
        || value.entries.length > capacity) throw new Error("request-journal-invalid");
    const fingerprints = new Set<string>();
    const ids = new Set<string>();
    for (const entry of value.entries) {
      if (!entry || Object.keys(entry).length !== 2 || typeof entry.fingerprint !== "string" || !fingerprintPattern.test(entry.fingerprint)
          || typeof entry.requestId !== "string" || !requestPattern.test(entry.requestId)
          || fingerprints.has(entry.fingerprint) || ids.has(entry.requestId)) throw new Error("request-journal-invalid");
      fingerprints.add(entry.fingerprint);
      ids.add(entry.requestId);
    }
    return value.entries;
  }
  function write(entries: Entry[]) {
    const serialized = JSON.stringify({ schema: journalSchema, entries });
    storage.setItem(journalKey, serialized);
    if (storage.getItem(journalKey) !== serialized) throw new Error("request-journal-write-unconfirmed");
  }
  return {
    async reserve(identityAndBody: unknown): Promise<RequestReservation> {
      const fingerprint = await digest(stableJson(identityAndBody));
      if (!fingerprintPattern.test(fingerprint)) throw new Error("request-fingerprint-invalid");
      return lock(() => {
        const entries = read();
        const prior = entries.find((entry) => entry.fingerprint === fingerprint);
        if (prior) return { ...prior, reused: true };
        if (entries.length >= capacity) throw new Error("request-journal-full");
        const requestId = randomId();
        if (!requestPattern.test(requestId) || entries.some((entry) => entry.requestId === requestId)) throw new Error("request-id-invalid");
        const entry = { fingerprint, requestId };
        write([...entries, entry]);
        return { ...entry, reused: false };
      });
    },
    async acknowledge(reservation: RequestReservation) {
      return lock(() => {
        const entries = read();
        write(entries.filter((entry) => entry.fingerprint !== reservation.fingerprint || entry.requestId !== reservation.requestId));
      });
    }
  };
}

export function browserRequestJournal() {
  if (!navigator.locks || !globalThis.crypto?.subtle || !globalThis.crypto.randomUUID) throw new Error("request-journal-unavailable");
  const digest = async (text: string) => [...new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text)))].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  const journal = createRequestJournal({
    storage: localStorage,
    digest,
    randomId: () => crypto.randomUUID(),
    lock: async (operation) => await navigator.locks.request(journalKey, operation)
  });
  return {
    ...journal,
    async submission<T>(identityAndBody: unknown, operation: () => Promise<T>): Promise<T> {
      const fingerprint = await digest(stableJson(identityAndBody));
      // Keep reservation, create/reconciliation and tracking ACK in one logical boundary.
      // Distinct requests use distinct locks; execution/event waiting happens after release.
      return await navigator.locks.request(`${journalKey}:submission:${fingerprint}`, operation);
    }
  };
}

export async function reconcileCreate(send: () => Promise<CreateResponse>, signal?: AbortSignal) {
  let ambiguous = false;
  let response: CreateResponse = { ok: false, error: "cancelled" };
  for (let attempt = 0; attempt < 2; attempt++) {
    if (signal?.aborted) return { response, ambiguous: true };
    try { response = await send(); }
    catch { response = { ok: false, error: "network" }; }
    const uncertain = !response.httpStatus || response.httpStatus >= 500
      || (response.httpStatus >= 200 && response.httpStatus < 300 && (!response.ok || typeof response.job?.id !== "string"));
    if (!uncertain) return { response, ambiguous };
    ambiguous = true;
  }
  return { response, ambiguous };
}

function stableJson(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) => {
    if (item && typeof item === "object" && !Array.isArray(item)) {
      return Object.fromEntries(Object.entries(item).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0));
    }
    return item;
  });
}
