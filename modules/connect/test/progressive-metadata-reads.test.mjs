import test from 'node:test';
import assert from 'node:assert/strict';
import { resolve, dirname, join } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
const source = process.argv[2] ? resolve(process.argv[2]) : fileURLToPath(new URL('../browser/client.mjs', import.meta.url));
const repo = process.argv[3] ? resolve(process.argv[3]) : resolve(dirname(source), '../../..');
const { createClientWithStorage } = await import(pathToFileURL(source));
const { ConnectError, canonicalJson } = await import(new URL('./crypto.mjs', pathToFileURL(source)));
const { validateState } = await import(new URL('./storage.mjs', pathToFileURL(source)));
const { createConnectService } = await import(pathToFileURL(join(repo, 'modules/connect/server/index.mjs')));
const { deriveCanonicalAppCoverage } = await import(pathToFileURL(join(repo,'src/world/canonical-app-coverage.mjs')));
const { createCanvasObjectsService } = await import(pathToFileURL(join(repo,'modules/canvas-objects/server.mjs')));
const projectId = 'progressive-connect-test', origin = 'https://progressive.fixture.invalid', endpoint = origin + '/rpc', scope = { projectId, endpoint };
const reads = ['world.profile.get', 'world.community.list', 'world.directory.search', 'world.directory.resolve', 'world.field.get', 'apps.directory.search', 'apps.directory.resolve', 'apps.devices', 'contacts.list'];
const backgroundReads = ['call.inbox', 'notifications.status', 'apps.cover.read', 'apps.discussion.inbox','canvas.pages.read','canvas.pages.content','canvas.pages.wait'];
const coverageReads=['canvas.objects.get','canvas.widgets.get'];
const relayIdentityRead='apps.relay.identity';
const effects = ['world.profile.update', 'world.field.put', 'apps.launch', 'contacts.requestAccount', 'apps.directory.search.extra', 'apps.discussion.read', 'apps.relay.identity.close', 'apps.relay.identity.extra'];
const gate = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const turn = () => new Promise(done => setTimeout(done, 5));
const outcome = promise => promise.then(value => ({ value }), error => ({ error }));
async function until(check) { const start = performance.now(); while (!check() && performance.now() - start < 800) await turn(); return check(); }
function memoryStorage() {
  let state = null, hook = null;
  const copy = () => state === null ? null : structuredClone(state);
  return {
    async read() { const value = copy(), next = hook; hook = null; if (next) await next(); return value; },
    async claim(value) { if (state === null) { validateState(value, scope); state = structuredClone(value); } return copy(); },
    async compareAndSwap(expected, value) { if (state?.localRevision !== expected) throw new ConnectError('LOCAL_CONFLICT', 'Synthetic conflict'); validateState(value, scope); state = structuredClone(value); return copy(); },
    inspect: copy, nextRead(next) { hook = next; },
  };
}
async function fixture(t) {
  const state = { traffic: [], executed: [], active: 0, peak: 0, before: null, after: null };
  const service = createConnectService({ databasePath: ':memory:', projectId, allowedOrigins: [origin], extensions: [{
    // Built-in operations must keep their genuine dispatcher and exact argument contracts.
    operations: new Set([...reads, ...backgroundReads, ...coverageReads, ...effects,relayIdentityRead,'canvas.objects.create','canvas.objects.project'].filter(op => !['contacts.list', 'contacts.requestAccount'].includes(op))),
    execute({ op, args, actor }) { state.executed.push({ op, accountId: actor.accountId, args: structuredClone(args) }); return state.result?state.result({op,args,actor}):{ operation: op, args, accountId: actor.accountId, deviceId: actor.deviceId }; },
  }], canRequestContact: () => true });
  const clients = [];
  const fetcher = async (_url, options) => {
    const body = JSON.parse(options.body); assert.equal(body.protocol, 1);
    state.traffic.push({ op: body.op, target: body.args?.operation, signed: Boolean(body.proof), challengeId: body.proof?.challengeId, publicJwk: body.proof?.publicJwk }); state.active++; state.peak = Math.max(state.peak, state.active);
    try {
      if (state.before) await state.before(body,options);
      const result = await service.handle({ ...body, origin });
      if (state.after) await state.after(body, result);
      return new Response(JSON.stringify(result), { status: result.ok ? 200 : 400 });
    } finally { state.active--; }
  };
  const client = store => { const value = createClientWithStorage({ projectId, endpoint, fetch: fetcher }, store); clients.push(value); return value; };
  t.after(() => { clients.forEach(value => value.dispose()); service.close(); });
  const store = memoryStorage(), secondStore = memoryStorage(), primary = client(store);
  const a = await primary.bootstrap('Synthetic A'), b = await client(secondStore).bootstrap('Synthetic B');
  const merged = store.inspect(); merged.installations.push(...secondStore.inspect().installations); merged.localRevision++;
  await store.compareAndSwap(merged.localRevision - 1, merged);
  const other = client(store); state.traffic.length = 0; state.executed.length = 0; state.peak = 0;
  return { primary, other, store, state, service, a: a.accountId, b: b.accountId, context: { expectedAccountId: a.accountId },
    signingPublicJwk: store.inspect().installations.find(value => value.accountId === a.accountId).signingPublicJwk };
}

test('signed profile, apps and local state finish while a community response is held', async t => {
  const f = await fixture(t), entered = gate(), release = gate();
  f.state.before = async body => { if (body.op === 'world.community.list' && body.proof) { entered.resolve(); await release.promise; } };
  const held = f.primary.extension('world.community.list', {}, f.context); await entered.promise;
  let profileDone = false, appsDone = false, localDone = false;
  const profile = f.primary.extension('world.profile.get', {}, f.context).then(value => { profileDone = true; return value; });
  const apps = f.primary.extension('apps.directory.search', {}, f.context).then(value => { appsDone = true; return value; });
  const local = f.primary.getLocalState().then(value => { localDone = true; return value; });
  let early; try { early = await until(() => profileDone && appsDone && localDone); } finally { release.resolve(); }
  const values = await Promise.all([held, profile, apps, local]);
  assert.equal(early, true, 'A read must release the browser state queue before awaiting its response'); assert.ok(values.every(value => value.accountId === f.a));
});

for (const operation of backgroundReads) {
  test(`field reads and local navigation do not wait for a held ${operation}`, async t => {
    const f = await fixture(t), entered = gate(), release = gate();
    f.state.before = async body => { if (body.op === operation && body.proof) { entered.resolve(); await release.promise; } };
    const held = f.primary.extension(operation, {}, f.context); await entered.promise;
    let profileDone = false, appsDone = false, localDone = false;
    const profile = f.primary.extension('world.profile.get', {}, f.context).then(value => { profileDone = true; return value; });
    const apps = f.primary.extension('apps.directory.resolve', {}, f.context).then(value => { appsDone = true; return value; });
    const local = f.primary.getLocalState().then(value => { localDone = true; return value; });
    let early; try { early = await until(() => profileDone && appsDone && localDone); } finally { release.resolve(); }
    await Promise.all([held, profile, apps, local]);
    assert.equal(early, true, 'An optional background response must not hold the field or identity queue');
  });
  for (const phase of ['challenge', 'response']) {
    test(`${operation} rejects an account round trip during its ${phase}`, async t => {
      const f = await fixture(t), entered = gate(), release = gate();
      f.state.after = async body => {
        if (phase === 'challenge' ? body.op === 'challenge' && body.args.operation === operation : body.op === operation && body.proof) {
          entered.resolve(); await release.promise;
        }
      };
      const pending = outcome(f.primary.extension(operation, {}, f.context)); await entered.promise;
      try { await f.other.switchProfile(f.b); await f.other.switchProfile(f.a); } finally { release.resolve(); }
      assert.equal((await pending).error?.code, 'ACTIVE_PROFILE_CHANGED');
      assert.equal(f.state.executed.length, phase === 'challenge' ? 0 : 1);
    });
  }
}

test('all nine exact bound reads overlap with unique real challenges and verified signatures', async t => {
  const f = await fixture(t), release = gate();
  f.state.before = async body => { if (reads.includes(body.op) && body.proof) await release.promise; };
  const pending = reads.map(op => f.primary.extension(op, op === 'contacts.list' ? {} : { marker: op }, f.context)); let early;
  try { early = await until(() => f.state.traffic.filter(row => row.signed).length === 9); } finally { release.resolve(); }
  const values = await Promise.all(pending), signed = f.state.traffic.filter(row => row.signed);
  assert.equal(early, true); assert.equal(signed.length, 9); assert.equal(new Set(signed.map(row => row.challengeId)).size, 9); assert.equal(f.state.executed.length, 8);
  assert.ok(f.state.executed.every(row => row.accountId === f.a)); assert.ok(values.slice(0, -1).every(value => value.accountId === f.a));
  assert.ok(Array.isArray(values.at(-1).contacts), 'The ninth read is the real contacts.list handler');
  assert.ok(signed.every(row => canonicalJson(row.publicJwk) === canonicalJson(f.signingPublicJwk)), 'Every real verified proof uses the captured A installation');
  assert.ok(f.state.peak > 1);
});

test('A to B to A during a challenge rejects the captured read before signing', async t => {
  const f = await fixture(t), entered = gate(), release = gate();
  f.state.after = async body => { if (body.op === 'challenge' && body.args.operation === 'apps.directory.search') { entered.resolve(); await release.promise; } };
  const pending = outcome(f.primary.extension('apps.directory.search', {}, f.context)); await entered.promise;
  await f.other.switchProfile(f.b); await f.other.switchProfile(f.a); release.resolve(); const result = await pending;
  assert.equal(result.error?.code, 'ACTIVE_PROFILE_CHANGED'); assert.equal(f.state.traffic.filter(row => row.signed).length, 0); assert.equal(f.state.executed.length, 0);
});

test('A to B to A after a signed read rejects its stale reply', async t => {
  const f = await fixture(t), entered = gate(), release = gate();
  f.state.after = async body => { if (body.op === 'apps.directory.search' && body.proof) { entered.resolve(); await release.promise; } };
  const pending = outcome(f.primary.extension('apps.directory.search', {}, f.context)); await entered.promise;
  await f.other.switchProfile(f.b); await f.other.switchProfile(f.a); release.resolve(); const result = await pending;
  assert.equal(result.error?.code, 'ACTIVE_PROFILE_CHANGED'); assert.equal(f.state.executed.length, 1); assert.equal(f.state.executed[0].accountId, f.a);
});

test('mismatched expected account and malformed contexts dispatch no challenge', async t => {
  const f = await fixture(t); await f.other.switchProfile(f.b);
  await assert.rejects(f.primary.extension('apps.directory.search', {}, f.context), { code: 'ACTIVE_PROFILE_CHANGED' });
  for (const context of [null, [], {}, { expectedAccountId: '' }, { expectedAccountId: f.a, extra: true }]) await assert.rejects(f.primary.extension('apps.directory.search', {}, context), { code: 'INVALID_ARGUMENT' });
  assert.equal(f.state.traffic.length, 0);
});

test('a page wait can be cancelled without blocking identity or dispatching a held read',async t=>{
 const f=await fixture(t),entered=gate(),abort=new AbortController();
 f.state.before=async(body,options)=>{if(body.op==='canvas.pages.wait'&&body.proof){entered.resolve();await new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(new DOMException('Aborted','AbortError')),{once:true}));}};
 const pending=outcome(f.primary.extension('canvas.pages.wait',{pageId:'page_'+'a'.repeat(32),after:'a'.repeat(64)},{...f.context,signal:abort.signal}));
 const started=await until(()=>f.state.traffic.some(row=>row.op==='canvas.pages.wait'&&row.signed));assert.equal(started,true,'the actual signed page wait must reach the transport');
 let local=false;await f.primary.getLocalState().then(()=>{local=true;});assert.equal(local,true);abort.abort();
 assert.equal((await pending).error?.code,'REQUEST_CANCELLED');assert.equal(f.state.executed.length,0);
 assert.equal((await f.primary.extension('world.profile.get',{},f.context)).accountId,f.a);
});

test('cancellation context remains closed to exact read operations and rejects pre-aborted work before its challenge',async t=>{
 const f=await fixture(t),controller=new AbortController();controller.abort();
 await assert.rejects(f.primary.extension('canvas.pages.wait',{}, {...f.context,signal:controller.signal}),{code:'REQUEST_CANCELLED'});
 for(const op of ['apps.launch','world.profile.update','canvas.pages.put','canvas.pages.wait.extra','canvas.objects.update','canvas.widgets.remove','canvas.objects.get.extra','canvas.widgets.get.extra','canvas.objects.list','canvas.widgets.resolve'])await assert.rejects(f.primary.extension(op,{}, {...f.context,signal:controller.signal}),{code:'INVALID_ARGUMENT'});
 await assert.rejects(f.primary.extension('canvas.pages.wait',{}, {...f.context,signal:{aborted:false}}),{code:'INVALID_ARGUMENT'});
 assert.equal(f.state.traffic.length,0);
});

for(const operation of coverageReads){
 test(`coverage transport ${operation} accepts signal through the signed fetch and verified actor`,async t=>{
  const f=await fixture(t),abort=new AbortController(),args={expectedAccountId:f.a,...(operation==='canvas.objects.get'?{objectId:'object_fixture'}:{widgetId:'widget-fixture'})};
  const result=await f.primary.extension(operation,args,{...f.context,signal:abort.signal});
  assert.equal(result.operation,operation);assert.equal(result.accountId,f.a);assert.deepEqual(result.args,args);
  const signed=f.state.traffic.filter(row=>row.signed);assert.equal(signed.length,1);assert.equal(signed[0].op,operation);
  assert.equal(canonicalJson(signed[0].publicJwk),canonicalJson(f.signingPublicJwk));assert.equal(f.state.executed[0].accountId,f.a);
 });
 test(`coverage ${operation} pre-abort and wrong account dispatch no challenge`,async t=>{
  const f=await fixture(t),abort=new AbortController();abort.abort();
  await assert.rejects(f.primary.extension(operation,{}, {...f.context,signal:abort.signal}),{code:'REQUEST_CANCELLED'});
  await f.other.switchProfile(f.b);f.state.traffic.length=0;
  await assert.rejects(f.primary.extension(operation,{}, {...f.context,signal:new AbortController().signal}),{code:'ACTIVE_PROFILE_CHANGED'});
  assert.equal(f.state.traffic.length,0);assert.equal(f.state.executed.length,0);
 });
 test(`coverage ${operation} aborts held fetch while identity and other reads remain usable`,async t=>{
  const f=await fixture(t),abort=new AbortController();
  f.state.before=async(body,options)=>{if(body.op===operation&&body.proof)await new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(new DOMException('Aborted','AbortError')),{once:true}));};
  const pending=outcome(f.primary.extension(operation,{}, {...f.context,signal:abort.signal}));
  assert.equal(await until(()=>f.state.traffic.some(row=>row.op===operation&&row.signed)),true,'the exact owner metadata read reaches actual fetch');
  let local=false,profile=false;
  const navigation=f.primary.getLocalState().then(()=>{local=true;}),read=f.primary.extension('world.profile.get',{},f.context).then(()=>{profile=true;});
  let early;try{early=await until(()=>local&&profile);}finally{abort.abort();}
  await Promise.all([navigation,read]);assert.equal(early,true,'metadata does not own the identity queue while fetch is held');
  assert.equal((await pending).error?.code,'REQUEST_CANCELLED');assert.equal(f.state.executed.some(row=>row.op===operation),false);
 });
 for(const phase of ['challenge','response'])test(`coverage ${operation} retains the original account round-trip fence during ${phase}`,async t=>{
  const f=await fixture(t),entered=gate(),release=gate();
  f.state.after=async body=>{if(phase==='challenge'?body.op==='challenge'&&body.args.operation===operation:body.op===operation&&body.proof){entered.resolve();await release.promise;}};
  const pending=outcome(f.primary.extension(operation,{}, {...f.context,signal:new AbortController().signal}));await entered.promise;
  try{await f.other.switchProfile(f.b);await f.other.switchProfile(f.a);}finally{release.resolve();}
  assert.equal((await pending).error?.code,'ACTIVE_PROFILE_CHANGED');assert.equal(f.state.executed.length,phase==='challenge'?0:1);
 });
 test(`coverage ${operation} disposal after challenge prevents a stale signature`,async t=>{
  const f=await fixture(t),entered=gate(),release=gate();
  f.state.after=async body=>{if(body.op==='challenge'&&body.args.operation===operation){entered.resolve();await release.promise;}};
  const pending=outcome(f.primary.extension(operation,{}, {...f.context,signal:new AbortController().signal}));await entered.promise;
  f.primary.dispose();release.resolve();assert.equal((await pending).error?.code,'CLIENT_DISPOSED');
  assert.equal(f.state.traffic.some(row=>row.signed),false);assert.equal(f.state.executed.length,0);
 });
}

test('the real signed batch projection alone supplies Native and widget representation without owner GET',async t=>{
 const f=await fixture(t),appId='app-'+'a'.repeat(32),calls=[];
 const objects=createCanvasObjectsService({databasePath:':memory:',projectId,
  actorActive:actor=>f.service.isActorActive(actor),withActorAuthorityFence:(actor,callback)=>f.service.withActorAuthorityFence(actor,callback),
  withProtectionAuthority:({object},callback)=>callback({kind:object.protection.kind,unlocked:false}),
  providers:[{providerId:'fixture.native',ownerRepresents:[],withBindingAuthority(_request,callback){return callback({allowed:true,protection:{kind:'password',sealed:true}});},project(){calls.push('content');throw Error('locked content forbidden');}},
   {providerId:'fixture.widget',ownerRepresents:()=>[{kind:'app',id:appId}],withBindingAuthority(_request,callback){return callback({allowed:true,protection:{kind:'none'}});},project(){calls.push('content');throw Error('hidden content forbidden');}}]});
 t.after(()=>objects.close());f.state.result=request=>objects.execute(request);
 const create=(providerId,locked,hidden)=>f.primary.extension('canvas.objects.create',{expectedAccountId:f.a,requestId:'create_'+providerId,
  binding:{providerId,ref:'fixture_ref'},presentation:{mask:{shape:'hex'}},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:hidden,others:false},protection:{kind:locked?'password':'none'}},f.context);
 const native=(await create('fixture.native',true,false)).current,widget=(await create('fixture.widget',false,true)).current;
 f.state.traffic.length=0;f.state.executed.length=0;
 const response=await f.primary.extension('canvas.objects.project',{expectedAccountId:f.a,objectIds:[native.objectId,widget.objectId]},f.context);
 const document={schema:'soty.field.v2',contexts:[{contextId:'personal',title:'Mine',x:0,y:0}],shortcuts:[native,widget].map((item,index)=>({shortcutId:'fixture_'+index,entity:{kind:'object',id:item.objectId},contextId:'personal',placement:{kind:'hex',slot:[index,0]}}))};
 const result=deriveCanonicalAppCoverage({document,representations:new Map(response.items.map(item=>[item.objectId,item.ownerRepresents]))});
 assert.equal(result.status,'ready');assert.deepEqual([...result.appIds],[appId]);assert.deepEqual(result.unknownObjectIds,[]);
 assert.deepEqual(f.state.executed.map(row=>row.op),['canvas.objects.project']);assert.equal(f.state.traffic.filter(row=>row.signed).length,1);assert.deepEqual(calls,[]);
 assert.equal(response.items[0].view.state,'locked');assert.equal(response.items[1].available,false);
});

test('payload and expected-account strings are captured before queueing', async t => {
  const f = await fixture(t), entered = gate(), release = gate(); f.store.nextRead(async () => { entered.resolve(); await release.promise; });
  const held = f.primary.getLocalState(); await entered.promise;
  const args = { query: 'captured' }, context = { expectedAccountId: f.a }, pending = f.primary.extension('apps.directory.search', args, context);
  args.query = 'mutated'; context.expectedAccountId = f.b; release.resolve(); await held;
  const result = await pending; assert.equal(result.accountId, f.a); assert.deepEqual(result.args, { query: 'captured' }); assert.equal(Object.hasOwn(f.state.executed[0].args, 'expectedAccountId'), false);
});

test('mutations retain signed queue order and later metadata admission waits for an active write', async t => {
  const f = await fixture(t), entered = gate(), release = gate();
  f.state.before = async body => { if (body.op === 'world.profile.update' && body.proof) { entered.resolve(); await release.promise; } };
  const first = f.primary.extension('world.profile.update', {}, f.context); await entered.promise;
  const second = f.primary.extension('world.field.put', {}, f.context), read = f.primary.extension('apps.directory.search', {}, f.context); let early;
  try { await turn(); await turn(); early = f.state.traffic.filter(row => row.signed).map(row => row.op); } finally { release.resolve(); }
  await Promise.all([first, second, read]); assert.deepEqual(early, ['world.profile.update']); assert.deepEqual(f.state.executed.map(row => row.op), ['world.profile.update', 'world.field.put', 'apps.directory.search']);
});

test('a bound relay identity request completes behind a held mutation without bypassing queued writes or reusing a nonce', async t => {
  const f=await fixture(t),entered=gate(),release=gate(),proofs=[];
  f.state.before=async body=>{
    if(body.op===relayIdentityRead&&body.proof)proofs.push(structuredClone(body));
    if(body.op==='world.profile.update'&&body.proof){entered.resolve();await release.promise;}
  };
  const first=f.primary.extension('world.profile.update',{marker:'first'},f.context);await entered.promise;
  const second=f.primary.extension('world.field.put',{marker:'second'},f.context);
  const args={appId:'app-'+'a'.repeat(32)},context={expectedAccountId:f.a};let completed=0;
  const grants=Array.from({length:3},()=>f.primary.extension(relayIdentityRead,args,context).then(value=>{completed++;return value;}));
  args.appId='app-'+'b'.repeat(32);context.expectedAccountId=f.b;
  let early,beforeRelease;try{early=await until(()=>completed===3);beforeRelease=f.state.executed.map(value=>value.op);}finally{release.resolve();}
  const values=await Promise.all(grants);await Promise.all([first,second]);
  assert.equal(early,true,'current-window identity must not wait behind an unrelated network-held mutation');
  assert.deepEqual(beforeRelease,[relayIdentityRead,relayIdentityRead,relayIdentityRead],'only the fixed identity read leaves the mutation queue');
  assert.deepEqual(f.state.executed.map(value=>value.op),[relayIdentityRead,relayIdentityRead,relayIdentityRead,'world.profile.update','world.field.put']);
  assert.ok(values.every(value=>value.accountId===f.a&&value.args.appId==='app-'+'a'.repeat(32)),'payload and expected account are captured before asynchronous admission');
  assert.equal(proofs.length,3);assert.equal(new Set(proofs.map(value=>value.proof.challengeId)).size,3);
  assert.ok(proofs.every(value=>canonicalJson(value.proof.publicJwk)===canonicalJson(f.signingPublicJwk)));
  const replay=await f.service.handle({...proofs[0],origin});assert.equal(replay.ok,false);assert.equal(replay.error.code,'challenge_consumed');
});

for(const phase of ['challenge','response'])test(`relay identity rejects profile ABA during its ${phase} while an unrelated mutation is held`,async t=>{
  const f=await fixture(t),mutationEntered=gate(),mutationRelease=gate(),entered=gate(),release=gate();
  f.state.before=async body=>{if(body.op==='world.profile.update'&&body.proof){mutationEntered.resolve();await mutationRelease.promise;}};
  f.state.after=async body=>{if(phase==='challenge'?body.op==='challenge'&&body.args.operation===relayIdentityRead:body.op===relayIdentityRead&&body.proof){entered.resolve();await release.promise;}};
  const mutation=outcome(f.primary.extension('world.profile.update',{},f.context));await mutationEntered.promise;
  const grant=outcome(f.primary.extension(relayIdentityRead,{},f.context));
  try{await entered.promise;await f.other.switchProfile(f.b);await f.other.switchProfile(f.a);}finally{release.resolve();mutationRelease.resolve();}
  assert.equal((await grant).error?.code,'ACTIVE_PROFILE_CHANGED');await mutation;
  assert.equal(f.state.traffic.filter(value=>value.op===relayIdentityRead&&value.signed).length,phase==='challenge'?0:1,'no stale challenge may be signed or stale result applied');
});

test('relay identity retires an unchanged-account generation before dispatch when another tab changes local state',async t=>{
  const f=await fixture(t);await f.other.startEnrollment('Other-tab enrollment');
  const entered=gate(),release=gate();f.store.nextRead(async()=>{entered.resolve();await release.promise;});
  const grant=outcome(f.primary.extension(relayIdentityRead,{},f.context));await entered.promise;
  await f.other.discardPendingEnrollment();release.resolve();
  assert.equal((await grant).error?.code,'ACTIVE_PROFILE_CHANGED');
  assert.equal(f.state.traffic.some(value=>value.target===relayIdentityRead||value.op===relayIdentityRead),false);
});

test('relay identity rejects disposal and an account mismatch before a stale signature',async t=>{
  const f=await fixture(t);await f.other.switchProfile(f.b);
  await assert.rejects(f.primary.extension(relayIdentityRead,{},f.context),{code:'ACTIVE_PROFILE_CHANGED'});assert.equal(f.state.traffic.length,0);
  await f.other.switchProfile(f.a);const entered=gate(),release=gate();
  f.state.after=async body=>{if(body.op==='challenge'&&body.args.operation===relayIdentityRead){entered.resolve();await release.promise;}};
  const grant=outcome(f.primary.extension(relayIdentityRead,{},f.context));await entered.promise;f.primary.dispose();release.resolve();
  assert.equal((await grant).error?.code,'CLIENT_DISPOSED');assert.equal(f.state.traffic.some(value=>value.op===relayIdentityRead&&value.signed),false);
});

test('relay identity requires a valid independent challenge bound to its exact payload',async t=>{
  const f=await fixture(t);
  f.state.before=async body=>{if(body.op===relayIdentityRead&&body.proof)body.args.appId='app-'+'b'.repeat(32);};
  await assert.rejects(f.primary.extension(relayIdentityRead,{appId:'app-'+'a'.repeat(32)},f.context),{code:'challenge_mismatch'});
  assert.equal(f.state.executed.length,0);
});

test('unbound relay identity, close and lookalike operations keep the normal serialized queue',async t=>{
  const f=await fixture(t),entered=gate(),release=gate();
  f.state.before=async body=>{if(body.op==='world.profile.update'&&body.proof){entered.resolve();await release.promise;}};
  const mutation=f.primary.extension('world.profile.update',{},f.context);await entered.promise;
  const calls=[f.primary.extension(relayIdentityRead),f.primary.extension('apps.relay.identity.close',{},f.context),f.primary.extension('apps.relay.identity.extra',{},f.context)];
  let early;try{await turn();await turn();early=f.state.traffic.filter(value=>value.signed).map(value=>value.op);}finally{release.resolve();}
  await Promise.all([mutation,...calls]);assert.deepEqual(early,['world.profile.update']);
  assert.deepEqual(f.state.executed.map(value=>value.op),['world.profile.update',relayIdentityRead,'apps.relay.identity.close','apps.relay.identity.extra']);
});

test('effect operations and lookalike names do not enter the exact read allowlist', async t => {
  const f = await fixture(t);
  for (const op of ['world.field.put', 'apps.launch', 'contacts.requestAccount', 'apps.directory.search.extra']) {
    const entered = gate(), release = gate();
    f.state.before = async body => { if (body.op === op && body.proof) { entered.resolve(); await release.promise; } };
    const before = f.state.traffic.length, held = f.primary.extension(op, op === 'contacts.requestAccount' ? { accountId: f.b } : {}, f.context); await entered.promise;
    const next = f.primary.extension('world.profile.update', {}, f.context); let early;
    try { await turn(); await turn(); early = f.state.traffic.slice(before).filter(row => row.signed).map(row => row.op); } finally { release.resolve(); }
    await Promise.all([held, next]); assert.deepEqual(early, [op]);
  }
});

test('legacy two-argument reads keep their serialized behavior', async t => {
  const f = await fixture(t), entered = gate(), release = gate();
  f.state.before = async body => { if (body.op === 'world.community.list' && body.proof) { entered.resolve(); await release.promise; } };
  const held = f.primary.extension('world.community.list'); await entered.promise;
  let complete = false; const next = f.primary.extension('apps.directory.search').then(value => { complete = true; return value; }); let early;
  try { await turn(); await turn(); early = complete; } finally { release.resolve(); }
  await Promise.all([held, next]); assert.equal(early, false);
});

test('disposal during a held challenge prevents a later read signature', async t => {
  const f = await fixture(t), entered = gate(), release = gate();
  f.state.after = async body => { if (body.op === 'challenge' && body.args.operation === 'apps.directory.search') { entered.resolve(); await release.promise; } };
  const pending = outcome(f.primary.extension('apps.directory.search', {}, f.context)); await entered.promise; f.primary.dispose(); release.resolve(); const result = await pending;
  assert.equal(result.error?.code, 'CLIENT_DISPOSED'); assert.equal(f.state.executed.length, 0); assert.equal(f.state.traffic.filter(row => row.signed).length, 0);
});

for (const phase of ['before challenge', 'before signature', 'after response']) {
  test('disposal while the current-state transaction is pending ' + phase + ' rejects the read', async t => {
    const f = await fixture(t), entered = gate(), release = gate();
    const hold = async () => { entered.resolve(); await release.promise; };
    if (phase === 'before challenge') {
      // In the new path, capture revision, installation, then the first RPC fence
      // are three genuine storage snapshots. The legacy path also reaches its
      // third read, at its pre-signature fence, so the negative control settles.
      let count = 0;
      const next = async () => { if (++count === 3) await hold(); else f.store.nextRead(next); };
      f.store.nextRead(next);
    } else {
      f.state.after = body => {
        if (phase === 'before signature' && body.op === 'challenge' && body.args.operation === 'apps.directory.search'
          || phase === 'after response' && body.op === 'apps.directory.search' && body.proof) f.store.nextRead(hold);
      };
    }
    const pending = outcome(f.primary.extension('apps.directory.search', {}, f.context));
    await entered.promise; f.primary.dispose(); release.resolve(); const result = await pending;
    assert.equal(result.error?.code, 'CLIENT_DISPOSED', 'A snapshot taken before dispose cannot authorize continuation after it');
    assert.equal(f.state.traffic.filter(row => row.op === 'challenge').length, phase === 'before challenge' ? 0 : 1);
    assert.equal(f.state.traffic.filter(row => row.signed).length, phase === 'after response' ? 1 : 0);
    assert.equal(f.state.executed.length, phase === 'after response' ? 1 : 0);
  });
}
