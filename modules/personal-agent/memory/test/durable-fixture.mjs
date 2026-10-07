import { DatabaseSync } from 'node:sqlite';

// Synthetic external authority fixture. Production must use the existing controller's trusted durable state.
export function openFloorFixture(file, clock = () => 1000) {
  const db = new DatabaseSync(file);
  db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
    CREATE TABLE IF NOT EXISTS floors(partition_id TEXT PRIMARY KEY, floor INTEGER NOT NULL) STRICT;
    CREATE TABLE IF NOT EXISTS erase_intents(partition_id TEXT NOT NULL,mutation_id TEXT NOT NULL,
      expected_floor INTEGER NOT NULL,next_floor INTEGER NOT NULL,ids TEXT NOT NULL,
      PRIMARY KEY(partition_id,mutation_id)) STRICT;`);
  const context = Object.freeze({});
  const verifyContext = ({ context: candidate }) => candidate === context
    ? { leaseId: 'synthetic_lease', epoch: 1, expiresAt: clock() + 1000000 } : false;
  const readRestoreFloor = ({ partitionId }) => db.prepare('SELECT floor FROM floors WHERE partition_id=?').get(partitionId)?.floor ?? 0;
  const advanceRestoreFloor = request => {
    db.exec('BEGIN IMMEDIATE');
    try {
      const old = db.prepare('SELECT * FROM erase_intents WHERE partition_id=? AND mutation_id=?').get(request.partitionId, request.mutationId);
      if (old) {
        if (old.expected_floor !== request.expectedFloor || old.ids !== JSON.stringify(request.erasedIds)) throw new Error('SYNTHETIC_FLOOR_INTENT_CHANGED');
        db.exec('COMMIT'); return old.next_floor;
      }
      const floor = readRestoreFloor(request);
      if (floor !== request.expectedFloor) throw new Error('SYNTHETIC_FLOOR_CAS');
      const next = floor + 1;
      db.prepare('INSERT INTO erase_intents VALUES(?,?,?,?,?)').run(request.partitionId, request.mutationId, floor, next, JSON.stringify(request.erasedIds));
      db.prepare('INSERT INTO floors VALUES(?,?) ON CONFLICT(partition_id) DO UPDATE SET floor=excluded.floor').run(request.partitionId, next);
      db.exec('COMMIT'); return next;
    } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  };
  return { context, verifyContext, readRestoreFloor, advanceRestoreFloor,
    intents: () => db.prepare('SELECT * FROM erase_intents ORDER BY partition_id,mutation_id').all(), close: () => db.close() };
}
