import { openMemoryPartition } from '../index.mjs';
import { openFloorFixture } from './durable-fixture.mjs';
import { personal, record } from './fixture.mjs';

let authority, store;
try {
  authority = openFloorFixture(process.argv[3]);
  store = openMemoryPartition({ databasePath: process.argv[2], scope: personal(), context: authority.context,
    verifyContext: authority.verifyContext, readRestoreFloor: authority.readRestoreFloor,
    advanceRestoreFloor: authority.advanceRestoreFloor, clock: () => 1000 });
  process.send({ ready: true });
  process.once('message', async message => {
    if (message !== 'go') process.exitCode = 2;
    else {
      try {
        const result = await store.supersede({ context: authority.context, mutationId: `replace_${process.argv[4]}`,
          records: [{ id: 'record_a', expectedRevision: 1 }], record: record(`record_${process.argv[4]}`, `synthetic child ${process.argv[4]}`) });
        process.send({ committed: true, id: result.id });
      } catch (error) { process.send({ committed: false, code: error.code ?? 'unexpected' }); }
    }
    store.close(); authority.close(); process.disconnect();
  });
} catch (error) {
  store?.close(); authority?.close();
  process.send({ setupError: error.code ?? 'unexpected' }); process.disconnect(); process.exitCode = 1;
}
