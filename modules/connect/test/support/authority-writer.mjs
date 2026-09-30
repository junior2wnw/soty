// A separate real SQLite writer for the host-fence acceptance cases.
import { parentPort, workerData } from 'node:worker_threads';
import { DatabaseSync } from 'node:sqlite';
import { createConnectService } from '../../server/index.mjs';

const flags = new Int32Array(workerData.flags);
const signal = index => { Atomics.store(flags, index, 1); Atomics.notify(flags, index); };
if (workerData.mode === 'revoke') {
  const service = createConnectService({ databasePath: workerData.databasePath,
    projectId: workerData.projectId, allowedOrigins: [workerData.origin] });
  parentPort.postMessage({ phase: 'ready' });
  parentPort.once('message', async request => {
    signal(0);
    try {
      const result = await service.handle(request);
      signal(1); parentPort.postMessage({ phase: 'result', result });
    } finally { service.close(); parentPort.close(); }
  });
} else if (workerData.mode === 'lock') {
  const db = new DatabaseSync(workerData.databasePath);
  db.exec('PRAGMA busy_timeout=1000; BEGIN IMMEDIATE');
  parentPort.postMessage({ phase: 'ready' });
  parentPort.once('message', () => {
    // Starts only after the main thread is ready to attempt its short fence.
    setTimeout(() => {
      try { db.exec('COMMIT'); signal(1); parentPort.postMessage({ phase: 'released' }); }
      finally { db.close(); parentPort.close(); }
    }, 350);
  });
} else throw new Error('test_worker_mode_invalid');
