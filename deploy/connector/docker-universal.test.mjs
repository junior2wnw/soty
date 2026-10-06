import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DockerApi, SafeError } from './docker-api.mjs';
import { dockerHttpFixture, frame } from './test-support/docker-http.mjs';
import { args } from './test-support/rollout-engine.mjs';
import { captureUniversalPreparedness } from './universal-policy.mjs';

const dto = () => captureUniversalPreparedness({ compiledLegacyMode: true, universalConfigured: false, reviewsConfigured: false, humanProfile: null, humanHttpEnabled: false });
async function fixture(t) {
  const root = await mkdtemp(path.join(await realpath(tmpdir()), 'soty-universal-policy-')), f = await dockerHttpFixture(root);
  t.after(async () => { await f.close(); assert.equal(path.dirname(path.resolve(root)), await realpath(tmpdir()));
    assert.match(path.basename(root), /^soty-universal-policy-/u); await rm(root, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); });
  return { ...f, api: new DockerApi({ socketPath: f.socket }) };
}

test('actual Docker HTTP fixture runs only the fixed private reader and sanitizes its closed stdout', async t => {
  const f = await fixture(t); assert.deepEqual(await f.api.universalPreparedness(args.originalId), dto());
  const create = f.calls.find(call => call.route.endsWith('/exec') && call.method === 'POST').body;
  assert.equal(create.Cmd[0], 'node'); assert.equal(create.WorkingDir, '/app'); assert.equal(create.AttachStderr, false); assert.equal(create.Privileged, false);
  assert.equal(create.Cmd[3].includes('readUniversalOperator()'), true); assert.equal(create.Cmd[3].includes('process.argv'), false);
  assert.equal(Object.hasOwn(create, 'Env'), false); assert.equal(Object.hasOwn(create, 'User'), false);
  assert.equal(f.calls.filter(call => call.route.endsWith('/start') && call.route.startsWith('/exec/')).length, 1);
});

test('bounded actual HTTP ingress rejects extra credentials/oversized output without returning source text', async t => {
  for (const value of [{ ...dto(), token: 'PRIVATE_SYNTHETIC_TOKEN' }, { ...dto(), extra: 'x'.repeat(100000) }]) {
    const f = await fixture(t); f.setOriginalMeasurement(value);
    await assert.rejects(f.api.universalPreparedness(args.originalId), error => error.code === 'universal_measurement_failed'
      && error.message === error.code && !error.cause && !error.message.includes('PRIVATE_SYNTHETIC_TOKEN'));
  }
});

test('ambiguous exec CREATE/START are never retried, never accepted as ready', async () => {
  for (const uncertain of ['create', 'start']) {
    const calls = [], id = '1'.repeat(64), execId = '2'.repeat(64), fake = { async inspect() { return { Id: id, Image: args.candidateImage, State: { Running: true, StartedAt: 'fixed' } }; },
      async request(method, route) { calls.push(route);
        if (route.endsWith('/exec')) { if (uncertain === 'create') throw new SafeError('engine_response_ambiguous'); return { Id: execId }; }
        if (route.endsWith('/start')) throw new SafeError('engine_response_ambiguous');
        throw new Error('unexpected_retry');
      } };
    await assert.rejects(DockerApi.prototype.universalPreparedness.call(fake, id), error => error.code === 'universal_measurement_unresolved');
    assert.equal(calls.filter(route => route.endsWith('/exec')).length, 1); assert.equal(calls.filter(route => route.endsWith('/start')).length, uncertain === 'start' ? 1 : 0);
  }
});

test('actual attached exec response loss settles as unresolved and issues no duplicate execution', async t => {
  const f = await fixture(t); f.setExecAbort(true);
  await assert.rejects(f.api.universalPreparedness(args.originalId), error => error.code === 'universal_measurement_unresolved');
  assert.equal(f.calls.filter(call => call.route.endsWith('/exec')).length, 1);
  assert.equal(f.calls.filter(call => call.route.startsWith('/exec/') && call.route.endsWith('/start')).length, 1);
});

test('cross-container, changed command, running/nonzero exec, stderr frame and restarted container cannot supply readiness', async () => {
  for (const fault of ['container', 'command', 'running', 'exit', 'stderr', 'restart']) {
    const id = '1'.repeat(64), execId = '2'.repeat(64); let command, reads = 0;
    const fake = { async inspect() { return { Id: id, Image: args.candidateImage, State: { Running: true, StartedAt: fault === 'restart' && reads++ ? 'new' : 'fixed' } }; },
      async request(method, route, body) {
        if (route.endsWith('/exec')) { command = body.Cmd; return { Id: execId }; }
        if (route.endsWith('/start')) return frame(dto(), fault === 'stderr' ? 2 : 1);
        return { ID: execId, ContainerID: fault === 'container' ? '3'.repeat(64) : id, Running: fault === 'running', ExitCode: fault === 'exit' ? 1 : 0,
          ProcessConfig: { privileged: false, tty: false, entrypoint: 'node', arguments: fault === 'command' ? ['-e', 'other'] : command.slice(1) } };
      } };
    await assert.rejects(DockerApi.prototype.universalPreparedness.call(fake, id), error => /^universal_measurement_/u.test(error.code));
  }
});
