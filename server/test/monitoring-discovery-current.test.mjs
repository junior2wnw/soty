import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve, sep } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { spawn } from 'node:child_process';
import ts from 'typescript';
import { createHttpApp } from '../http-app.js';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';
import { monitoringCapabilityCatalog } from '../../modules/monitoring/catalog.mjs';
import { monitoringCapabilityCatalog as adapterCatalog } from '../../modules/monitoring/capabilities.mjs';

async function temporaryConsumer(t) {
  const parent = await realpath(tmpdir()), directory = await realpath(await mkdtemp(join(parent, 'soty-monitoring-discovery-')));
  const marker = randomUUID(); await writeFile(join(directory, 'test-owner'), marker, { flag: 'wx' });
  t.after(async () => {
    assert.equal(await realpath(directory), directory); assert.equal(dirname(directory), parent);
    assert.match(basename(directory), /^soty-monitoring-discovery-/u);
    assert.equal(await readFile(join(directory, 'test-owner'), 'utf8'), marker);
    await rm(directory, { recursive: true, force: true });
  });
  return directory;
}

async function downloadClosure(t, f, directory) {
  const response = await fetch(f.origin + '/.well-known/soty-app-platform', { signal: t.signal });
  assert.equal(response.status, 200); const discovery = await response.json();
  assert.equal(discovery.schema, 'soty.app-platform-discovery.v1');
  assert.ok(discovery.monitoring?.files.includes(discovery.monitoring.entry));
  const documentation = await fetch(new URL(discovery.monitoring.documentation, f.origin), { signal: t.signal });
  assert.equal(documentation.status, 200); assert.match(documentation.headers.get('content-type'), /text\/markdown/u);
  assert.match(await documentation.text(), /monitoring\./u);
  const manifestResponse = await fetch(new URL(discovery.monitoring.manifest, f.origin), { signal: t.signal });
  assert.equal(manifestResponse.status, 200); const manifest = await manifestResponse.json();
  assert.equal(manifest.id, 'soty.monitoring'); assert.equal(manifest.collectionDefault, true);
  assert.equal(manifest.executionFromManifest, false, 'downloadable metadata is not authority');
  const descriptor = new URL(manifest.descriptorFactory.split('#')[0], new URL(discovery.monitoring.entry, f.origin));
  const pending = [...discovery.monitoring.files.map(value => new URL(value, f.origin)), descriptor], seen = new Map();
  while (pending.length) {
    const url = pending.shift(); if (seen.has(url.href)) continue;
    assert.equal(url.origin, f.origin); assert.ok(url.pathname.startsWith('/agents/sdk/'));
    assert.equal(url.search + url.hash, ''); assert.equal(/[%\\]/u.test(url.pathname), false);
    const reply = await fetch(url, { signal: t.signal });
    assert.equal(reply.status, 200, 'public runtime dependency must be served: ' + url.pathname);
    const bytes = Buffer.from(await reply.arrayBuffer());
    const file = resolve(directory, '.' + url.pathname); assert.ok(file.startsWith(directory + sep));
    await mkdir(dirname(file), { recursive: true }); await writeFile(file, bytes);
    seen.set(url.href, { file, bytes: bytes.length, type: reply.headers.get('content-type') });
    if (!/\.m?js$/u.test(url.pathname)) continue;
    assert.match(reply.headers.get('content-type'), /javascript/u, 'JavaScript cannot be a fallback HTML page');
    const source = ts.createSourceFile(url.pathname, bytes.toString('utf8'), ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
    for (const statement of source.statements) {
      if (!ts.isImportDeclaration(statement) && !ts.isExportDeclaration(statement)) continue;
      const specifier = statement.moduleSpecifier?.text; if (!specifier) continue;
      if (specifier.startsWith('node:')) continue;
      assert.ok(specifier.startsWith('.'), 'portable runtime has no undeclared package dependency');
      pending.push(new URL(specifier, url));
    }
  }
  return { discovery, manifest, seen, entry: seen.get(new URL(discovery.monitoring.entry, f.origin).href).file,
    server: seen.get(new URL(discovery.monitoring.server, f.origin).href).file, descriptor: seen.get(descriptor.href).file };
}

function childEnvironment() {
  return Object.fromEntries(['SystemRoot', 'WINDIR', 'TEMP', 'TMP'].filter(key => typeof process.env[key] === 'string').map(key => [key, process.env[key]]));
}

async function importDownloaded(t, directory, graph, usage = '') {
  const script = `import assert from 'node:assert/strict';
    const sdk=await import(${JSON.stringify(pathToFileURL(graph.entry).href)});
    const server=await import(${JSON.stringify(pathToFileURL(graph.server).href)});
    const descriptor=await import(${JSON.stringify(pathToFileURL(graph.descriptor).href)});
    assert.equal(typeof sdk.createMonitoringClient,'function');
    assert.equal(typeof server.createMonitoringService,'function');
    assert.equal(typeof descriptor.createMonitoringDescriptor,'function');
    ${usage}`;
  const child = spawn(process.execPath, ['--max-old-space-size=512', '--input-type=module', '--eval', script], {
    cwd: directory, env: childEnvironment(), signal: t.signal, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = ''; for (const stream of [child.stdout, child.stderr]) stream.on('data', bytes => { output += bytes; assert.ok(output.length < 100000); });
  const result = await new Promise((resolveResult, reject) => { child.once('error', reject); child.once('exit', (code, signal) => resolveResult({ code, signal })); });
  assert.equal(result.signal, null); assert.equal(result.code, 0, output);
}

test('actual anonymous Root discovery downloads a complete monitoring runtime closure for an independent Node consumer', { timeout: 30000 }, async t => {
  const f = await nativeHttpFixture(t, { enabled: false, appFactory: (dist, options) => createHttpApp(dist, {
    ...options, env: {}, monitoring: { storageMode: 'reader' },
  }) });
  const directory = await temporaryConsumer(t), graph = await downloadClosure(t, f, directory);
  await importDownloaded(t, directory, graph);
  assert.ok(graph.seen.size >= graph.discovery.monitoring.files.length);
});

test('portable catalog extraction preserves the exact prior reader/writer metadata and existing export', () => {
  assert.equal(adapterCatalog, monitoringCapabilityCatalog);
  const before = 'bc07bc4957e2bf8428dda13d9a5f25ae8da17b872260b5fe0dd588965804002a';
  const current = { reader: monitoringCapabilityCatalog(), writer: monitoringCapabilityCatalog({ writer: true }) };
  assert.equal(createHash('sha256').update(JSON.stringify(current)).digest('hex'), before);
});

async function monitoringRoot(t) {
  const operators = [];
  const factory = (dist, options) => createHttpApp(dist, { ...options, env: {}, monitoring: {
    storageMode: 'writer', platformPolicy: { readAccountIds: [...operators], configureAccountIds: [...operators], adminAccountIds: [...operators] },
  } });
  const f = await nativeHttpFixture(t, { enabled: false, appFactory: factory });
  const owner = nativeIdentity('Downloaded SDK synthetic owner'), outsider = nativeIdentity('Downloaded SDK synthetic outsider');
  const own = await f.bootstrap(owner), other = await f.bootstrap(outsider);
  operators.push(own.accountId);
  // This is an explicit trusted fixture configuration after ordinary signed
  // enrollment. Invitation administration and the platform operator are separate.
  await f.restart({ enabled: false });
  const call = (actor, op, args = {}) => f.call(actor, op, { expectedAccountId: actor === owner ? own.accountId : other.accountId, ...args });
  return { f, owner, outsider, own, other, call, target: { kind: 'platform' }, file: join(f.directory, 'monitoring', 'monitoring.sqlite') };
}

async function downloadedClient(t, f, actor, accountId, directory, graph, phase, at) {
  const script = `import assert from 'node:assert/strict';
    const settings=JSON.parse(process.argv[2]);
    const {createMonitoringClient}=await import(settings.entry);
    const pending=new Map();let sequence=0;
    process.on('message',message=>{const resolve=pending.get(message.id);if(resolve){pending.delete(message.id);resolve(message.value);}});
    const request=(op,args)=>new Promise(resolve=>{const id=++sequence;pending.set(id,resolve);process.send({id,op,args});});
    const values=new Map(),storage={getItem:key=>values.get(key)??null,setItem:(key,value)=>values.set(key,value),removeItem:key=>values.delete(key)};
    const client=createMonitoringClient({request,currentAccountId:()=>settings.accountId,isCurrent:()=>true,scope:{kind:'platform'},storage});
    try{
      const state=await client.describe();
      if(settings.phase==='write'){
        assert.equal(state.config.enabled,true);assert.equal(state.config.revision,0);
        await client.defineMetric({requestId:'downloaded-definition',id:'custom.downloaded',kind:'counter',unit:'count'});
        for(const[index,value]of[4,5].entries())assert.equal((await client.record({eventId:'downloaded-event-'+index,metricId:'custom.downloaded',value,observedAt:settings.at})).recorded,true);
        await client.setEnabled({requestId:'downloaded-disable',expectedRevision:state.config.revision,enabled:false});
        assert.equal(client.pending(),null);
      }else{assert.equal(state.config.enabled,false);assert.equal(state.config.revision,1);}
      const query=await client.query({metricIds:['custom.downloaded'],from:settings.at-1000-60000,to:settings.at-1000+60000,maxPoints:2});
      assert.equal(query.config.enabled,false);assert.equal(query.series[0].unit,'count');
      assert.deepEqual(query.series[0].points.map(point=>point.value),[null,9]);
      assert.deepEqual(query.series[0].points.map(point=>point.samples),[0,2]);
      const module=await import(settings.server);assert.equal(typeof module.createMonitoringService,'function');
      process.send({done:true,result:{revision:query.config.revision,values:query.series[0].points.map(point=>point.value)}});
    }catch(error){process.send({failed:true,code:error.code??error.name,message:error.message});process.exitCode=1;}
    finally{process.disconnect();}`;
  const file = join(directory, 'consumer-' + phase + '.mjs'); await writeFile(file, script, { flag: 'wx' });
  const settings = { entry: pathToFileURL(graph.entry).href, server: pathToFileURL(graph.server).href, accountId, phase, at };
  const child = spawn(process.execPath, ['--max-old-space-size=512', file, JSON.stringify(settings)], {
    cwd: directory, env: childEnvironment(), signal: t.signal, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  let result, failure, output = ''; const operations = [];
  for (const stream of [child.stdout, child.stderr]) stream.on('data', bytes => { output += bytes; assert.ok(output.length < 100000); });
  child.on('message', async message => {
    if (message.done) { result = message.result; return; }
    if (message.failed) { failure = message; return; }
    assert.ok(Number.isSafeInteger(message.id) && /^monitoring\./u.test(message.op));
    operations.push(message.op);
    // No private key/credential enters the independent process. Every request
    // goes through the real signed Root HTTP boundary with the original actor.
    const value = await f.call(actor, message.op, message.args);
    if (child.connected) child.send({ id: message.id, value });
  });
  const stopped = await new Promise((resolveResult, reject) => { child.once('error', reject); child.once('exit', (code, signal) => resolveResult({ code, signal })); });
  assert.equal(stopped.signal, null); assert.equal(stopped.code, 0, failure?.message || output);
  assert.ok(result); return { result, operations };
}

test('downloaded SDK in independent Node uses signed Root and retains real SQL history/config through reopen', { timeout: 45000 }, async t => {
  const { f, owner, outsider, own, other, target, file, call } = await monitoringRoot(t);
  const directory = await temporaryConsumer(t), graph = await downloadClosure(t, f, directory);
  const at = (Math.floor(Date.now() / 60000) - 2) * 60000 + 1000;
  const written = await downloadedClient(t, f, owner, own.accountId, directory, graph, 'write', at);
  assert.deepEqual(written.operations, ['monitoring.describe', 'monitoring.metric.put', 'monitoring.metric.record', 'monitoring.metric.record', 'monitoring.config.put', 'monitoring.query']);
  const rows = () => f.sql(file, db => ({
    data: { ...db.prepare("SELECT sum(sum) AS value,sum(samples) AS samples FROM monitoring_buckets WHERE scope_key='platform' AND metric_id='custom.downloaded'").get() },
    config: { ...db.prepare("SELECT enabled,revision FROM monitoring_configs WHERE scope_key='platform'").get() },
    registryId: db.prepare('SELECT registry_id FROM monitoring_meta').get().registry_id,
  }));
  const before = rows(); assert.deepEqual(before.data, { value: 9, samples: 2 }); assert.deepEqual(before.config, { enabled: 0, revision: 1 });
  const denied = await call(outsider, 'monitoring.describe', { scope: target });
  assert.equal(denied.ok, false); assert.equal(denied.error.code, 'monitoring_forbidden');
  assert.equal(JSON.stringify(denied).includes('custom.downloaded'), false);
  await f.restart({ enabled: false });
  const reopened = await downloadedClient(t, f, owner, own.accountId, directory, graph, 'read', at);
  assert.deepEqual(reopened.operations, ['monitoring.describe', 'monitoring.query']);
  assert.deepEqual(rows(), before, 'same real registry/config/samples, not a fresh empty database');
  assert.notEqual(own.accountId, other.accountId); assert.equal(good(await call(owner, 'monitoring.describe', { scope: target })).config.enabled, false);
});

test('downloaded server and SDK are reusable by an independent closed-policy host with real SQLite reopen', { timeout: 30000 }, async t => {
  const f = await nativeHttpFixture(t, { enabled: false, appFactory: (dist, options) => createHttpApp(dist, { ...options, env: {}, monitoring: { storageMode: 'reader' } }) });
  const directory = await temporaryConsumer(t), graph = await downloadClosure(t, f, directory);
  const databasePath = join(directory, 'independent.sqlite');
  await importDownloaded(t, directory, graph, `
    const actor=Object.freeze({accountId:'standalone-owner',deviceId:'standalone-device'}),scope={kind:'app',appId:'app-'+ 'c'.repeat(32)};
    let active=true;
    const options={databasePath:${JSON.stringify(databasePath)},projectId:'independent-app',mode:'writer',
      currentActor:value=>active&&value===actor,
      withAuthority({actor:value,scope:target,right},callback){
        if(!active||value!==actor||target.kind!=='app'||target.appId!==scope.appId||!['read','configure','record','admin'].includes(right))throw new server.MonitoringError('monitoring_forbidden',403);
        return callback();
      }};
    let service=server.createMonitoringService(options);
    const values=new Map(),storage={getItem:key=>values.get(key)??null,setItem:(key,value)=>values.set(key,value),removeItem:key=>values.delete(key)};
    const client=sdk.createMonitoringClient({scope,storage,currentAccountId:()=>actor.accountId,request:async(op,args)=>service.execute({op,actor,args})});
    const at=(Math.floor(Date.now()/60000)-2)*60000+1000;
    await client.defineMetric({requestId:'independent-define',id:'custom.independent',kind:'counter',unit:'count'});
    await client.record({eventId:'independent-observe',metricId:'custom.independent',value:7,observedAt:at});
    await client.setEnabled({requestId:'independent-disable',expectedRevision:0,enabled:false});
    service.close();service=server.createMonitoringService(options);
    assert.equal((await client.describe()).config.enabled,false);
    const query=await client.query({metricIds:['custom.independent'],from:at-1000,to:at-1000+60000,maxPoints:1});
    assert.deepEqual(query.series[0].points.map(point=>point.value),[7]);
    const args={expectedAccountId:actor.accountId,scope};
    assert.throws(()=>service.execute({op:'monitoring.describe',actor:{...actor},args}),{code:'monitoring_forbidden'});
    assert.throws(()=>service.execute({op:'monitoring.describe',actor,args:{...args,scope:{kind:'platform'}}}),{code:'monitoring_forbidden'});
    active=false;assert.throws(()=>service.execute({op:'monitoring.describe',actor,args}),{code:'monitoring_forbidden'});
    service.close();`);
});
