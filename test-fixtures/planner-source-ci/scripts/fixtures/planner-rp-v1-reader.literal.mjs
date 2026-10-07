// Literal external pre-START oracle for the unguarded ab378 basic release.
// That release does NOT refuse format2 itself; operators must run this before START.
import { DatabaseSync } from 'node:sqlite';
import { existsSync } from 'node:fs';
export function readLegacyPlannerRp(path) {
  if (!existsSync(path)) return { supported: true, version: 1 };
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    const future = db
      .prepare(
        "SELECT 1 FROM sqlite_master WHERE name LIKE 'planner_soty_rp_%' OR name='planner_soty_rebind_receipts' LIMIT 1",
      )
      .get();
    if (future)
      throw Object.assign(new Error('planner_rp_reader_incompatible'), {
        code: 'planner_rp_reader_incompatible',
      });
    return { supported: true, version: 1 };
  } finally {
    db.close();
  }
}
