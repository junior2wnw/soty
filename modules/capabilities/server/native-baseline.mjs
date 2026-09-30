/** The reader baseline can recognize a native record, but cannot execute or
 * settle its effect. A future native reconciler must supply the Notes proof.
 * Recheck table presence: another compatible writer may explicitly migrate an
 * already opened v1 store. A cached false must never become a release bypass. */
export function createNativeBaselineGuard({ db, error }) {
  if (!db || typeof db.prepare !== 'function' || typeof error !== 'function') throw new TypeError('native_baseline_configuration');
  const present = db.prepare("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='cap_native_note_intents'");
  let lookup;
  function available() { return Boolean(present.get()); }
  function find(invocationId) {
    if (!available()) return null;
    lookup ??= db.prepare('SELECT invocation_id,started_at,input_purged_at FROM cap_native_note_intents WHERE invocation_id=?');
    return lookup.get(invocationId) || null;
  }
  function assertGeneric(invocationId) {
    if (find(invocationId)) throw error('native_reconciliation_required');
  }
  return Object.freeze({ available, find, assertGeneric });
}
