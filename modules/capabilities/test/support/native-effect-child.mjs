import { DatabaseSync } from 'node:sqlite';
import { openNativeStores, ORIGIN, INPUT } from './native-connected.mjs';

let stores, config;
const halted = new Int32Array(new SharedArrayBuffer(4));
function checkpoint(seam) {
  // One small IPC message, with no preceding queued messages after the ready
  // handshake. The parent kills this process only after receiving the marker.
  process.send({ phase: 'checkpoint', seam });
  while (true) Atomics.wait(halted, 0, 0, 1000);
}

process.on('message', async message => {
  if (message.config) {
    config = message.config;
    stores = openNativeStores({ directory: config.directory, now: () => config.now, notesLimits: config.notesLimits,
      verify: (token, mode, check) => config.rejectedPort === 'verifier'
        ? Promise.reject(new Error('synthetic rejected verifier')) : check(token, mode),
      fence: fence => callback => {
        const output = fence(callback);
        return config.rejectedPort === 'fence' ? Promise.reject(new Error('synthetic rejected fence')) : output;
      },
      port: port => ({ ...port, storageIdentity() {
        return config.rejectedPort === 'identity' ? Promise.reject(new Error('synthetic rejected identity')) : port.storageIdentity();
      }, validateDraftInput(args) {
        return config.rejectedPort === 'validation' ? Promise.reject(new Error('synthetic rejected validation')) : port.validateDraftInput(args);
      }, readCreateProof(args) {
        if (config.rejectedPort === 'read') return Promise.reject(new Error('synthetic rejected proof port'));
        return port.readCreateProof(args);
      }, createDraftForInvocation(args) {
        if (config.rejectedPort === 'create') return Promise.reject(new Error('synthetic rejected create port'));
        const proof = port.createDraftForInvocation(args);
        if (config.seam === 'notes-commit') checkpoint(config.seam);
        return proof;
      } }) });
    if (config.seam === 'receipt-before-purge') {
      const prepare = DatabaseSync.prototype.prepare;
      DatabaseSync.prototype.prepare = function(sql) {
        const statement = prepare.call(this, sql);
        if (/^INSERT INTO cap_receipts\(/u.test(sql)) {
          const run = statement.run.bind(statement);
          statement.run = (...args) => { const value = run(...args); checkpoint(config.seam); return value; };
        }
        return statement;
      };
    }
    if (config.seam === 'caps-commit') {
      const exec = DatabaseSync.prototype.exec;
      DatabaseSync.prototype.exec = function(sql) {
        const caps = sql === 'COMMIT' && this.prepare("SELECT 1 FROM sqlite_schema WHERE name='cap_receipts'").get();
        const terminal = caps && this.prepare('SELECT count(*) AS n FROM cap_receipts').get().n > 0;
        const value = exec.call(this, sql); if (terminal) checkpoint(config.seam); return value;
      };
    }
    process.send({ phase: 'ready' }); return;
  }
  if (!message.start) return;
  try {
    if (config.mode === 'signed') {
      const response = await stores.connect.handle(config.request);
      process.send({ phase: 'result', ok: response.ok, code: response.error?.code });
    } else {
      const actor = stores.caps.authenticateCredential({ token: config.token, audience: ORIGIN });
      if (config.rejectedPort === 'identity') stores.native.readiness();
      const admitted = stores.native.admit({ actor, idempotencyKey: config.key || 'native_crash_key', input: config.input || INPUT });
      const invocationId = admitted.invocation.invocationId;
      if (config.seam === 'admission') checkpoint(config.seam);
      stores.native.beginAttempt({ invocationId });
      if (config.seam === 'marker') checkpoint(config.seam);
      const result = stores.native.execute({ invocationId });
      process.send({ phase: 'result', ok: true, invocationId, reused: admitted.reused, outcome: result.outcome });
    }
  } catch (error) { process.send({ phase: 'result', ok: false, code: typeof error?.code === 'string' ? error.code : 'test_unexpected_failure' }); }
  stores.close(); process.disconnect();
});
