export interface PreparedLocation { rootId: string; segments: string[] }
export interface PreparedPayload { expectedAccountId: string; hostDeviceId: string; connectorId: string; location: PreparedLocation; manifestDigest: string; requestId: string }
export function createPreparedAppState(options: { accountId: string; deviceId: string; storage: Pick<Storage, 'getItem' | 'setItem'>;
  locks?: Pick<LockManager, 'request'>; randomId?: () => string }): {
  read(): PreparedPayload[]; prepare(input: Omit<PreparedPayload, 'requestId'>): Promise<{ payload: PreparedPayload; created: boolean }>; acknowledge(expected: PreparedPayload): Promise<boolean>; forget(expected: PreparedPayload): Promise<boolean>;
};
