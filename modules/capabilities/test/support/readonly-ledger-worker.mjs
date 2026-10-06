import { DatabaseSync } from 'node:sqlite';
import { createCatalog } from '../../server/catalog.mjs';
import { createAccessStore } from '../../server/access.mjs';
import { createInvocationStore } from '../../server/invocations.mjs';
import { initializeCapabilitiesSchema } from '../../server/schema.mjs';
import { canonicalHash, newId } from '../../server/validation.mjs';

// Explicit synthetic host composition, not a Source adapter or public API.
// The two processes use the actual ACL, budgets, SQL transaction and private
// readonly ledger port. Actual Root/App/Planner transport is a separate gate.
let db, core, actor;
process.on('message', ({ id, command, args }) => {
  try {
    let result;
    if (command === 'initialize') {
      db = new DatabaseSync(args.file);
      db.exec('PRAGMA busy_timeout=3000');
      initializeCapabilitiesSchema(db, { projectId: args.projectId });
      const registry = createCatalog([args.catalog]);
      const pin = db
        .prepare('SELECT digest FROM cap_contracts WHERE capability_id=? AND version=?')
        .get(args.reference.capabilityId, args.reference.version);
      if (pin?.digest !== args.reference.digest) throw new Error('fixture_pin_mismatch');
      function transaction(callback) {
        db.exec('BEGIN IMMEDIATE');
        try {
          const value = callback();
          if (value?.then) throw new Error('fixture_async_transaction');
          db.exec('COMMIT');
          return value;
        } catch (error) {
          if (db.isTransaction) db.exec('ROLLBACK');
          throw error;
        }
      }
      const access = createAccessStore({
        db,
        transaction,
        catalog: registry,
        actorActive: (value) =>
          value.accountId === args.owner.accountId && value.deviceId === args.owner.deviceId,
      });
      createInvocationStore({
        db,
        transaction,
        authorize: access.authorizeInvocation,
        reserveBudget: access.reserveBudget,
        settleBudget: access.settleBudget,
        canonicalHash,
        newId,
        readonlyContracts: [args.reference],
        captureReadonlyCore: (value) => {
          core = value;
        },
        validateReadonlyInput: ({ reference, input }) =>
          registry.validateInput(registry.get(reference.capabilityId, reference.version), input),
      });
      actor = access.authenticateCredential({ token: args.token, audience: args.audience });
      result = { ready: true, pid: process.pid };
    } else if (command === 'admit') {
      const admitted = core.admit({ actor, ...args });
      result = {
        invocationId: admitted.invocation.invocationId,
        status: admitted.invocation.status,
        reused: admitted.reused,
      };
    } else if (command === 'claim' || command === 'claim-crash') {
      const claimed = core.claim({ actor, ...args });
      if (command === 'claim-crash' && claimed.claimed) process.exit(17); // after COMMIT, before any Source dispatch
      result = { claimed: claimed.claimed, status: claimed.invocation.status };
    } else if (command === 'cancel') {
      result = { status: core.cancel({ actor, ...args }).invocation.status };
    } else if (command === 'close') {
      db?.close();
      db = null;
      process.send({ id, result: { closed: true } }, () => process.exit(0));
      return;
    } else throw new Error('fixture_command_invalid');
    process.send({ id, result });
  } catch (error) {
    process.send({
      id,
      error: /^[a-z_]{1,80}$/u.test(error?.code ?? '') ? error.code : 'fixture_operation_failed',
    });
  }
});
