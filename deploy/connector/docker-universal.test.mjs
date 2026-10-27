import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DockerApi, SafeError, universalMeasurementDiagnostic } from './docker-api.mjs';
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
    const fake = { timeoutMs:60, async inspect() { return { Id: id, Image: args.candidateImage, State: { Running: true, StartedAt: fault === 'restart' && reads++ ? 'new' : 'fixed' } }; },
      async request(method, route, body) {
        if (route.endsWith('/exec')) { command = body.Cmd; return { Id: execId }; }
        if (route.endsWith('/start')) return frame(dto(), fault === 'stderr' ? 2 : 1);
        return { ID: execId, ContainerID: fault === 'container' ? '3'.repeat(64) : id, Running: fault === 'running', ExitCode: fault === 'exit' ? 1 : 0,
          ProcessConfig: { privileged: false, tty: false, entrypoint: 'node', arguments: fault === 'command' ? ['-e', 'other'] : command.slice(1) } };
      } };
    await assert.rejects(DockerApi.prototype.universalPreparedness.call(fake, id), error => /^universal_measurement_/u.test(error.code));
  }
});

test('a complete valid DTO waits for terminal metadata of the SAME exec within the original deadline', async t => {
  const f=await fixture(t),request=f.api.request.bind(f.api);let inspections=0;
  f.api.request=async(method,route,...rest)=>{const value=await request(method,route,...rest);
    if(route.startsWith('/exec/')&&route.endsWith('/json'))return {...value,Running:++inspections<3};return value;};
  assert.deepEqual(await f.api.universalPreparedness(args.originalId),dto());
  const create=f.calls.filter(c=>c.route.endsWith('/exec')),start=f.calls.filter(c=>c.route.startsWith('/exec/')&&c.route.endsWith('/start'));
  assert.equal(create.length,1);assert.equal(start.length,1);assert.equal(inspections,3);
  const execId=start[0].route.split('/')[2];
  assert.equal(f.calls.filter(c=>c.route.startsWith('/exec/')&&c.route.endsWith('/json')).every(c=>c.route==='/exec/'+execId+'/json'),true);
});

test('nonzero exit and command/container/privilege mismatch keep safe HTTP and exec evidence without raw process data', async t => {
  for(const fault of ['exit','command','container','privilege'])await t.test(fault,async t=>{
    const f=await fixture(t),request=f.api.request.bind(f.api);
    f.api.request=async(method,route,...rest)=>{const value=await request(method,route,...rest);
      if(route.startsWith('/exec/')&&route.endsWith('/json')){
        if(fault==='exit')value.ExitCode=7;
        if(fault==='command')value.ProcessConfig.arguments=['PRIVATE_SYNTHETIC_SECRET'];
        if(fault==='container')value.ContainerID='9'.repeat(64);
        if(fault==='privilege')value.ProcessConfig.privileged=true;
      }return value;};
    await assert.rejects(f.api.universalPreparedness(args.originalId),error=>{
      const d=universalMeasurementDiagnostic(error);assert.equal(error.code,'universal_measurement_unresolved');assert.equal(d.stage,'exec-inspect');assert.equal(d.httpStatus,200);
      assert.equal(d.errorCategory,fault==='exit'?'exec_nonzero_exit':'exec_metadata_mismatch');assert.match(d.execId,/^[a-f0-9]{64}$/);
      assert.equal(d.exec.exitCode,fault==='exit'?7:0);assert.equal(JSON.stringify(d).includes('PRIVATE_SYNTHETIC_SECRET'),false);
      assert.equal(Object.hasOwn(d,'stdout'),false);assert.equal(Object.hasOwn(d,'stderr'),false);return true;});
    assert.equal(f.calls.filter(c=>c.route.endsWith('/exec')).length,1);assert.equal(f.calls.filter(c=>c.route.endsWith('/start')&&c.route.startsWith('/exec/')).length,1);
  });
});

test('HTTP timeout and unknown lost stdout retain the failed stage and never replay or infer success from exit metadata', async t => {
  for(const fault of ['timeout','lost-stdout'])await t.test(fault,async t=>{
    const f=await fixture(t);if(fault==='timeout'){f.api.timeoutMs=70;f.setExecStartDelay(200);}else f.setExecAbort(true);
    await assert.rejects(f.api.universalPreparedness(args.originalId),error=>{
      const d=universalMeasurementDiagnostic(error);assert.equal(error.code,'universal_measurement_unresolved');assert.equal(d.stage,'exec-start');assert.equal(fault==='timeout'?d.httpStatus===200:[null,200].includes(d.httpStatus),true);assert.equal(d.errorCategory,'http_ambiguous');
      assert.match(d.execId,/^[a-f0-9]{64}$/);assert.equal(d.inspectPolls,0);assert.equal(d.exec,null);if(fault==='timeout')assert.equal(d.deadlineMs,70);return true;});
    assert.equal(f.calls.filter(c=>c.route.endsWith('/exec')).length,1);assert.equal(f.calls.filter(c=>c.route.startsWith('/exec/')&&c.route.endsWith('/start')).length,1);
    assert.equal(f.calls.some(c=>c.route.startsWith('/exec/')&&c.route.endsWith('/json')),false);
  });
});

test('a running exec cannot extend its original operation deadline or authorize a second execution', { timeout: 2000 }, async t => {
  let now=10000,command,inspections=0;const calls=[],execId='b'.repeat(64);
  t.mock.method(Date,'now',()=>now);
  const engine={timeoutMs:90,
    async inspect(id,options){calls.push(['container',options.timeoutMs]);options.onStatus(200);now+=10;
      return {Id:id,Image:args.originalImage,State:{Running:true,StartedAt:'fixed'}};},
    async request(method,route,body,_raw,_limit,options){calls.push([route,options.timeoutMs]);options.onStatus(200);
      if(method==='POST'&&route===`/containers/${args.originalId}/exec`){command=body.Cmd;now+=15;return {Id:execId};}
      if(method==='POST'&&route===`/exec/${execId}/start`){now+=20;return frame(dto());}
      if(method==='GET'&&route===`/exec/${execId}/json`){now+=++inspections===1?25:20;
        return {ID:execId,ContainerID:args.originalId,Running:true,ExitCode:0,
          ProcessConfig:{privileged:false,tty:false,entrypoint:command[0],arguments:command.slice(1)}};}
      throw Error('unexpected_docker_request');
    }};
  await assert.rejects(DockerApi.prototype.universalPreparedness.call(engine,args.originalId),error=>{
    const d=universalMeasurementDiagnostic(error);assert.equal(error.code,'universal_measurement_unresolved');
    assert.equal(d.errorCategory,'exec_running_deadline');assert.equal(d.exec.running,true);assert.equal(d.deadlineMs,90);
    assert.equal(d.elapsedMs,90);assert.equal(d.inspectPolls,2);return true;});
  assert.deepEqual(calls,[['container',90],[`/containers/${args.originalId}/exec`,80],
    [`/exec/${execId}/start`,65],[`/exec/${execId}/json`,45],[`/exec/${execId}/json`,20]]);
});

test('untrusted error properties cannot manufacture a journal-safe measurement diagnostic',()=>{
  const error=new SafeError('universal_measurement_unresolved');error.universalMeasurementDiagnostic={stdout:'PRIVATE_SYNTHETIC_SECRET'};
  assert.equal(universalMeasurementDiagnostic(error),null);
});

test('actual HTTP status is retained without an engine response body or a duplicate CREATE',async t=>{
  const f=await fixture(t);f.setExecCreateStatus(503);
  await assert.rejects(f.api.universalPreparedness(args.originalId),error=>{
    const d=universalMeasurementDiagnostic(error);assert.equal(d.stage,'exec-create');assert.equal(d.httpStatus,503);assert.equal(d.execId,null);assert.equal(d.errorCategory,'http_status');
    assert.equal(JSON.stringify(d).includes('PRIVATE_SYNTHETIC_ENGINE_BODY'),false);return true;});
  assert.equal(f.calls.filter(c=>c.route.endsWith('/exec')).length,1);assert.equal(f.calls.some(c=>c.route.startsWith('/exec/')&&c.route.endsWith('/start')),false);
});

test('settling never accepts a later nonzero exit or changed command, and invalid output cannot authorize polling',async t=>{
  for(const fault of ['exit-after-running','command-after-running','invalid-output-running'])await t.test(fault,async t=>{
    const f=await fixture(t),request=f.api.request.bind(f.api);let polls=0;
    if(fault==='invalid-output-running')f.setOriginalMeasurement({...dto(),token:'PRIVATE_SYNTHETIC_TOKEN'});
    f.api.request=async(method,route,...rest)=>{const v=await request(method,route,...rest);
      if(route.startsWith('/exec/')&&route.endsWith('/json')){
        polls++;if(polls===1)v.Running=true;
        else if(fault==='exit-after-running')v.ExitCode=9;
        else if(fault==='command-after-running')v.ProcessConfig.arguments=['PRIVATE_SYNTHETIC_ARG'];
      }return v;};
    await assert.rejects(f.api.universalPreparedness(args.originalId),error=>{
      const d=universalMeasurementDiagnostic(error);assert.equal(d.errorCategory,fault==='exit-after-running'?'exec_nonzero_exit':fault==='command-after-running'?'exec_metadata_mismatch':'runtime_output_invalid');
      assert.equal(d.inspectPolls,fault==='invalid-output-running'?1:2);assert.equal(JSON.stringify(d).includes('PRIVATE_SYNTHETIC'),false);return true;});
    assert.equal(f.calls.filter(c=>c.route.endsWith('/exec')).length,1);assert.equal(f.calls.filter(c=>c.route.startsWith('/exec/')&&c.route.endsWith('/start')).length,1);
  });
});
