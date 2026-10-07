import { createOrdinaryAppStore } from '../../examples/ordinary-app/store.mjs';
import { createOrdinaryAppNativePort } from '../../examples/ordinary-app/native.mjs';
import { createNativeAuthorityRuntime } from '../../server/native-authority.mjs';

const parts = []; for await (const part of process.stdin) parts.push(part);
const input = JSON.parse(Buffer.concat(parts).toString('utf8'));
const store = createOrdinaryAppStore({ ...input.options, key: Buffer.from(input.keyBase64, 'base64'), initialize: false });
const runtime = createNativeAuthorityRuntime(createOrdinaryAppNativePort({ store, resourceId: 'selected', incarnationId: 'one' }));
try {
  const proof = await runtime.capture(input.binding, { headers: { cookie: 'ordinary_native_board=' + input.nativeToken } });
  const result = await runtime.call(proof, 'execute', input.args); process.stdout.write(JSON.stringify({ outcome: result.outcome, replayed: result.replayed }));
} catch (error) { process.stdout.write(JSON.stringify({ error: typeof error.code === 'string' ? error.code : 'worker_unknown' })); process.exitCode = 1; }
finally { runtime.close(); store.close(); }
