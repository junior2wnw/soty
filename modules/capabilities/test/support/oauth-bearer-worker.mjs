import { openBearerStores } from './oauth-bearer.mjs';
let stores, args;
process.on('message', message => {
  try {
    if (message.initialize) {
      args = message.initialize;
      stores = openBearerStores({ directory: args.directory, now: () => args.now }); process.send({ ready: true });
    } else if (message.run) {
      let result;
      try {
        const actor = stores.caps.oauth.authenticateBearer({ token: args.token, audience: args.audience });
        const admitted = stores.caps.nativeNotes.admit({ actor, input: args.input, idempotencyKey: args.key });
        result = { admitted: true, reused: admitted.reused, invocationId: admitted.invocation.invocationId };
      } catch (error) { result = { errorCode: error.code ?? 'unknown' }; }
      stores.close(); stores = null; process.send({ result }, () => process.disconnect());
    }
  } catch (error) {
    try { stores?.close(); } catch {}
    process.exitCode = 1; process.send({ failure: error.code ?? 'failure' }, () => process.disconnect());
  }
});
