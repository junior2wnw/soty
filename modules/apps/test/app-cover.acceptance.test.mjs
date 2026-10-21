import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { generateKeyPairSync, randomUUID, sign } from 'node:crypto';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import sharp from 'sharp';
import { createConnectService, digestArgs } from '../../connect/server/index.mjs';
import { createWorldService } from '../../world/server/index.mjs';
import { createAppsService, operations } from '../server/index.mjs';
import { appCoverOperations } from '../server/cover.mjs';
import { migrateAppsSchema, inspectAppsSchema, ensureCanonicalDomain, ensureInitialPublication, runtimeTargetDigest,
  RUNTIME_PROFILE, SCOPED_RUNTIME_PROFILE, RESOURCE_RUNTIME_PROFILE } from '../server/schema.mjs';
import { baseAppsSchema, hasAudienceSchema } from '../server/audience.mjs';
import { createScopedAdmissionRegistry } from '../scoped-embed/admissions.mjs';
import { HIVE_SELECTED_SOURCE } from '../scoped-embed/resource-route-adapters.mjs';
import { createCoverDecoder, canonicalizeCover } from '../../cover-art/server/codec.mjs';
import { coverSchemaStatements } from '../../cover-art/server/schema.mjs';

const ORIGIN='https://covers.example', LEGACY='https://{appId}.legacy.example', NAMED='https://apps.example';
const success=value=>{assert.equal(value.ok,true,value.error?.code);return value;};
function keys() { const a=generateKeyPairSync('ec',{namedCurve:'prime256v1'}),b=generateKeyPairSync('ec',{namedCurve:'prime256v1'});
  return {privateKey:a.privateKey,publicJwk:a.publicKey.export({format:'jwk'}),encryptionPublicJwk:b.publicKey.export({format:'jwk'})}; }
async function signed(connect,identity,op,args={}) {
  const challenge=success(await connect.handle({op:'challenge',origin:ORIGIN,args:{operation:op,digest:digestArgs(args)}}));
  return connect.handle({op,args,origin:ORIGIN,proof:{challengeId:challenge.challengeId,publicJwk:identity.publicJwk,
    signature:sign('sha256',Buffer.from(challenge.message),{key:identity.privateKey,dsaEncoding:'ieee-p1363'}).toString('base64url')}});
}
const raster=async color=>(await sharp({create:{width:512,height:512,channels:3,background:color??'#567890'}}).png().toBuffer()).toString('base64');
const oldTables=['app_devices','local_apps','local_app_grants','app_domain_zones','app_domain_heads','app_domains','app_domain_receipts',
  'app_runtime_targets','app_publications','app_publication_domains','app_publication_receipts','app_source_heads','app_source_receipts',
  'app_saved_heads','app_saved_entries','app_saved_receipts','app_discussion_heads','app_discussion_conversations','app_discussion_messages',
  'app_discussion_changes','app_discussion_usage','app_discussion_rates','app_audiences','app_discussion_head_audiences','app_discussion_conversation_audiences','app_scoped_embed_admissions'];
async function fixture(t,{base=6,decoder=createCoverDecoder()}={}) {
  const parent=realpathSync(tmpdir()),folder=mkdtempSync(join(parent,'soty-app-cover-')),owned=randomUUID();
  writeFileSync(join(folder,'owned'),owned,{flag:'wx'});mkdirSync(join(folder,'apps'));
  const databasePath=join(folder,'apps/registry.sqlite'),connectPath=join(folder,'connect.sqlite');let apps,connect,depth=0,serial=0;
  const startConnect=()=>createConnectService({databasePath:connectPath,projectId:'cover-acceptance',allowedOrigins:[ORIGIN],extensions:[
    {operations,execute:request=>apps.execute(request)},
    ...(apps?.coverExtension?[{operations:new Set(appCoverOperations),executeAsync:request=>apps.coverExtension.executeAsync(request)}]:[]),
  ]});
  connect=startConnect();
  const world=createWorldService({databasePath:join(folder,'world.sqlite'),projectId:'cover-acceptance',withCurrentContacts:(actor,fn)=>connect.withCurrentContacts(actor,fn)});
  const identities={owner:keys(),friend:keys(),stranger:keys()},accounts={};
  t.after(()=>{apps?.close();world.close();connect.close();const actual=realpathSync(folder);assert.equal(dirname(actual),parent);
    assert.match(basename(actual),/^soty-app-cover-/u);assert.equal(readFileSync(join(actual,'owned'),'utf8'),owned);rmSync(actual,{recursive:true});});
  for(const name of Object.keys(identities))accounts[name]=success(await signed(connect,identities[name],'bootstrap',{
    label:'Synthetic '+name,encryptionPublicJwk:identities[name].encryptionPublicJwk}));
  const id='app-'+'c'.repeat(32),key='synthetic-link|synthetic-host|synthetic-connector',flags={
    allowScopedEmbedMigration:base>=7,allowSelectedResourceMigration:base>=8,allowAudienceMigration:true};
  const profileType=base===6?RUNTIME_PROFILE:base===7?SCOPED_RUNTIME_PROFILE:RESOURCE_RUNTIME_PROFILE;
  const target={appId:id,revision:2,ownerAccountId:accounts.owner.accountId,connectorKey:key,port:8081,entryPath:base===6?'/':'/embed',profile:profileType};
  const targetDigest=runtimeTargetDigest(target),profiles=base===6?[]:[{
    schema:profileType,appId:id,connector:{linkId:'synthetic-link',hostDeviceId:'synthetic-host',connectorId:'synthetic-connector'},
    target:{revision:2,digest:targetDigest},sourceProfile:base===7?{id:'synthetic.cover-source',version:1,digest:'b'.repeat(64)}:HIVE_SELECTED_SOURCE,
    resource:base===7?{registryId:'soty',tenantId:accounts.owner.accountId,environmentId:'fixture',appId:id,resourceId:'selected',workspaceId:'selected'}:
      {registryId:'soty',tenantId:accounts.owner.accountId,environmentId:'fixture',appId:id,resourceId:'selected',selection:{kind:'hive.project.v1',nativeId:'synthetic-project',incarnationId:'fixture-incarnation'}},
    issuer:ORIGIN+'/human-identity',clientId:'synthetic-cover-source',embedOrigin:LEGACY.replace('{appId}',id),nativeOrigin:'http://127.0.0.1:8081',parentOrigin:ORIGIN,
  }];
  const seed=new DatabaseSync(databasePath);seed.exec('PRAGMA foreign_keys=ON');
  try {
    migrateAppsSchema(seed,{legacyTemplate:LEGACY,...flags});seed.exec('BEGIN IMMEDIATE');
    seed.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(key,accounts.owner.accountId,'{"linkId":"synthetic-link","hostDeviceId":"synthetic-host","connectorId":"synthetic-connector"}','Synthetic offline source',1);
    seed.prepare('INSERT INTO local_apps VALUES(?,?,?,?,?,?,?,?,?,?,?)').run(id,accounts.owner.accountId,key,'Synthetic cover app',8080,'/',JSON.stringify({accountIds:[accounts.stranger.accountId],communityIds:[]}),'enabled',1,1,1);
    seed.prepare('INSERT INTO local_app_grants VALUES(?,?,?)').run(id,'account',accounts.stranger.accountId);
    const row=seed.prepare('SELECT * FROM local_apps WHERE id=?').get(id);ensureCanonicalDomain(seed,row,LEGACY);ensureInitialPublication(seed,row);
    seed.prepare('INSERT INTO app_runtime_targets VALUES(?,?,?,?,?,?,?,?,?)').run(id,2,accounts.owner.accountId,key,8081,target.entryPath,profileType,targetDigest,2);
    if(profiles.length)createScopedAdmissionRegistry({db:seed,profiles}).commit({...target,digest:targetDigest});
    seed.prepare('UPDATE app_source_heads SET required_binding_version=2 WHERE app_id=?').run(id);
    seed.prepare('UPDATE app_publications SET active_target_revision=2,policy_epoch=2 WHERE app_id=?').run(id);seed.exec('COMMIT');
  }finally{seed.close();}
  function open(extra={}) {apps=createAppsService({databasePath,appOriginTemplate:LEGACY,namedAppZone:NAMED,shellOrigins:[ORIGIN],...flags,
    scopedEmbedProfiles:profiles,actorActive:actor=>connect.isActorActive(actor),
    withCurrentContacts:(actor,fn)=>connect.withCurrentContacts(actor,view=>{depth++;try{return fn(view);}finally{depth--;}}),
    withAuthorityFence:fn=>{assert.ok(depth>0,'actual Connect contact fence precedes World writer');return world.withCommunityAuthorityFence(fn);},
    canAccessCommunity:(a,c)=>world.canAccessCommunity(a,c),activeCommunityIds:a=>world.activeCommunityIds(a),isGroupAdmin:(a,c)=>world.isGroupAdmin(a,c),
    readCommunityAuthority:(actor,owner,ids)=>world.appCommunityAuthority(actor.accountId,owner,ids),...extra});}
  open();
  const raw=(name,op,args={})=>signed(connect,identities[name],op,{expectedAccountId:accounts[name].accountId,...args});
  const call=async(name,op,args={})=>success(await raw(name,op,args));
  function read(fn) {const db=new DatabaseSync(databasePath,{readOnly:true});try{return fn(db);}finally{db.close();}}
  function write(fn) {const db=new DatabaseSync(databasePath);db.exec('PRAGMA foreign_keys=ON;BEGIN IMMEDIATE');try{const v=fn(db);db.exec('COMMIT');return v;}catch(e){if(db.isTransaction)db.exec('ROLLBACK');throw e;}finally{db.close();}}
  async function args() {const v=await call('owner','apps.cover.status',{appId:id});return {appId:id,requestId:`cover1.${v.serverTime}.${(++serial).toString(16).padStart(32,'0')}`,
    expectedTargetRevision:v.scope.targetRevision,expectedPolicyEpoch:v.scope.policyEpoch,expectedCoverRevision:v.scope.coverRevision};}
  function request(now=Date.now()) {return `cover1.${now}.${(++serial).toString(16).padStart(32,'0')}`;}
  async function publish(audience,activeDomainIds=[]) {const p=await call('owner','apps.publication.get',{appId:id}),named=['link','public'].includes(audience);
    return call('owner','apps.publication.update',{appId:id,requestId:'policy-'+ ++serial,expectedPolicyEpoch:p.policyEpoch,expectedTargetRevision:p.activeTargetRevision,
      audience,launchPolicy:named?'anyone':'restricted',listed:audience==='public',activeDomainIds,
      ...(named?{exposureAck:{scope:'whole-port',targetRevision:p.target.revision,targetDigest:p.target.digest,profile:p.target.profile}}:{})});}
  return {id,profiles,accounts,identities,get connect(){return connect;},world,databasePath,connectPath,read,write,args,request,raw,call,publish,
    snapshot:()=>read(db=>({meta:JSON.stringify(db.prepare("SELECT * FROM apps_meta WHERE key<>'schema' ORDER BY key").all()),
      ...Object.fromEntries(oldTables.filter(name=>base>=7||name!=='app_scoped_embed_admissions').map(name=>[name,JSON.stringify(db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all())]))})),
    enable(extra={}){apps.close();open({allowCoverMigration:true,coverDecoder:decoder,...extra});connect.close();connect=startConnect();},
    reopen(extra={}){apps.close();open({coverDecoder:decoder,...extra});connect.close();connect=startConnect();},get apps(){return apps;}};
}

for(const base of [6,7,8])test(`Apps cover format${base+6} preserves base${base}, old rows and signed upload/commit/read after restart`,async t=>{
  const f=await fixture(t,{base});assert.equal(f.read(inspectAppsSchema),'v'+(base+3));assert.equal(f.apps.coverExtension,null);
  assert.equal(f.read(db=>db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name GLOB 'app_cover_*'").get().n),0);
  await f.call('owner','apps.saved.set',{appId:f.id,saved:true,expectedRevision:0,requestId:'saved-before-cover'});
  const discussion=await f.call('owner','apps.discussion.context',{appId:f.id});
  await f.call('owner','apps.discussion.send',{appId:f.id,domainId:discussion.context.entry.domainId,path:discussion.context.entry.path,conversationId:discussion.context.conversationId,requestId:'message-before-cover',body:'Preserve this original discussion'});
  const before=f.snapshot();f.enable();assert.equal(f.read(inspectAppsSchema),'v'+(base+6));assert.deepEqual(f.snapshot(),before);
  assert.equal(baseAppsSchema('v'+(base+6)),'v'+base);assert.equal(f.read(hasAudienceSchema),true);
  const stable=f.read(db=>db.prepare('SELECT store_id FROM app_cover_usage').get().store_id),input={...await f.args(),mime:'image/png',bytesBase64:await raster()};
  const prepared=await f.call('owner','apps.cover.prepare',input);assert.equal(prepared.draft.state,'ready');assert.equal((await f.call('owner','apps.cover.read',{appId:f.id})).image,null);
  const commit={appId:f.id,requestId:f.request(),draftId:prepared.draft.id},accepted=await f.call('owner','apps.cover.commit',commit);
  const image=(await f.call('owner','apps.cover.read',{appId:f.id})).image;assert.ok(image.base64);assert.equal(Buffer.from(image.base64,'base64').length,image.byteCount);
  assert.deepEqual(f.snapshot(),before,'cover commit does not alter Source, publication, ACL, saved or discussion rows');
  f.reopen();assert.equal(f.read(inspectAppsSchema),'v'+(base+6));assert.equal(f.read(db=>db.prepare('SELECT store_id FROM app_cover_usage').get().store_id),stable);
  assert.deepEqual((await f.call('owner','apps.cover.read',{appId:f.id})).image,image);
  await f.call('owner','apps.cover.remove',{...await f.args(),mode:'none'});
  const replay=await f.call('owner','apps.cover.commit',commit);assert.equal(replay.replayed,true);assert.equal(replay.receipt.versionId,accepted.receipt.versionId);assert.equal(replay.current.mode,'none');
  await f.call('owner','apps.cover.restore',{...await f.args(),versionId:accepted.receipt.versionId});assert.deepEqual((await f.call('owner','apps.cover.read',{appId:f.id})).image,image);
  const recrop=await f.call('owner','apps.cover.prepareFromVersion',{...await f.args(),versionId:accepted.receipt.versionId,crop:{x:.4,y:.5,zoom:2}});
  await f.call('owner','apps.cover.cancel',{appId:f.id,requestId:f.request(),draftId:recrop.draft.id});
  assert.ok((await f.call('owner','apps.cover.versions',{appId:f.id})).versions.length>=3);
  if(base>=7){f.reopen({scopedEmbedProfiles:[]});const refused=await f.raw('owner','apps.cover.read',{appId:f.id});assert.equal(refused.error?.code,'app_scoped_admission_required');}
});

test('cover signed reads enforce current publication, reciprocal friends and owner-only history without public byte URLs',async t=>{
  const f=await fixture(t);f.enable();const p=await f.call('owner','apps.cover.prepare',{...await f.args(),mime:'image/png',bytesBase64:await raster()});
  await f.call('owner','apps.cover.commit',{appId:f.id,requestId:f.request(),draftId:p.draft.id});
  assert.ok((await f.call('stranger','apps.cover.read',{appId:f.id})).image,'legacy direct grant permits current cover');
  assert.equal((await f.raw('stranger','apps.cover.versions',{appId:f.id})).ok,false);
  assert.equal((await f.raw('stranger','apps.cover.status',{appId:f.id})).ok,false);
  await f.publish('private');assert.equal((await f.raw('stranger','apps.cover.read',{appId:f.id})).ok,false);
  await f.publish('friends');assert.equal((await f.raw('friend','apps.cover.read',{appId:f.id})).ok,false);
  const card=success(await signed(f.connect,f.identities.owner,'card.get'));
  const request=success(await signed(f.connect,f.identities.friend,'contacts.request',{cardId:card.cardId}));
  const relation=success(await signed(f.connect,f.identities.owner,'contacts.accept',{requestId:request.requestId}));
  assert.ok((await f.call('friend','apps.cover.read',{appId:f.id})).image);
  success(await signed(f.connect,f.identities.owner,'contacts.remove',{relationshipId:relation.relationshipId}));
  assert.equal((await f.raw('friend','apps.cover.read',{appId:f.id})).ok,false);f.reopen();assert.equal((await f.raw('friend','apps.cover.read',{appId:f.id})).ok,false);
  const inspection=await f.call('owner','apps.inspect',{appId:f.id});
  const alias=await f.call('owner','apps.domains.claim',{appId:f.id,requestId:'cover-alias',expectedDomainsRevision:inspection.addresses.revision,slug:'cover-alias'});
  await f.publish('link',[alias.receipt.domainId]);assert.ok((await f.call('stranger','apps.cover.read',{appId:f.id})).image);
  await f.publish('public',[alias.receipt.domainId]);assert.ok((await f.call('friend','apps.cover.read',{appId:f.id})).image);
  await f.publish('private');assert.equal((await f.raw('friend','apps.cover.read',{appId:f.id})).ok,false);
  f.write(db=>db.prepare("UPDATE local_apps SET state='revoked' WHERE id=?").run(f.id));assert.equal((await f.raw('owner','apps.cover.read',{appId:f.id})).ok,false);
});

test('cover Connect boundary rejects wrong accounts, injected routing, oversized base64, stale policy and cover CAS',async t=>{
  const f=await fixture(t);f.enable();
  assert.equal((await f.raw('owner','apps.cover.status',{appId:f.id,expectedAccountId:f.accounts.stranger.accountId})).error?.code,'authentication_required');
  assert.equal((await signed(f.connect,f.identities.owner,'apps.cover.status',{appId:f.id})).error?.code,'apps_cover_account_required');
  const first={...await f.args(),mime:'image/png',bytesBase64:await raster()},prepared=await f.call('owner','apps.cover.prepare',first);
  assert.equal((await f.raw('stranger','apps.cover.prepare',first)).error?.code,'cover_target_unavailable');
  assert.equal((await f.raw('owner','apps.cover.prepare',{...await f.args(),mime:'image/png',bytesBase64:'A'.repeat(4*Math.ceil(320*1024/3)+4)})).error?.code,'cover_input_limit');
  assert.equal((await f.raw('owner','apps.cover.prepare',{...first,storeInstanceId:'foreign_store'})).error?.code,'cover_arguments_invalid');
  await f.publish('private');assert.equal((await f.raw('owner','apps.cover.commit',{appId:f.id,requestId:f.request(),draftId:prepared.draft.id})).error?.code,'cover_target_changed');
  const a=await f.call('owner','apps.cover.prepare',{...await f.args(),mime:'image/png',bytesBase64:await raster('#112233')});
  await f.call('owner','apps.cover.cancel',{appId:f.id,requestId:f.request(),draftId:prepared.draft.id});
  const b=await f.call('owner','apps.cover.prepare',{...await f.args(),mime:'image/png',bytesBase64:await raster('#223344')});
  await f.call('owner','apps.cover.commit',{appId:f.id,requestId:f.request(),draftId:a.draft.id});
  assert.equal((await f.raw('owner','apps.cover.commit',{appId:f.id,requestId:f.request(),draftId:b.draft.id})).error?.code,'cover_revision_conflict');
  const c=await f.call('owner','apps.cover.prepare',{...await f.args(),mime:'image/png',bytesBase64:await raster('#334455')});
  f.write(db=>{const app=db.prepare('SELECT * FROM local_apps WHERE id=?').get(f.id),target={appId:f.id,revision:3,
    ownerAccountId:app.owner_account_id,connectorKey:app.connector_key,port:8082,entryPath:'/',profile:RUNTIME_PROFILE};
    db.prepare('INSERT INTO app_runtime_targets VALUES(?,?,?,?,?,?,?,?,?)').run(f.id,3,target.ownerAccountId,target.connectorKey,target.port,target.entryPath,target.profile,runtimeTargetDigest(target),Date.now());
    db.prepare('UPDATE app_publications SET active_target_revision=3,policy_epoch=policy_epoch+1 WHERE app_id=?').run(f.id);});
  assert.equal((await f.raw('owner','apps.cover.commit',{appId:f.id,requestId:f.request(),draftId:c.draft.id})).error?.code,'cover_target_changed');
  const db=new DatabaseSync(f.connectPath);try{db.prepare("UPDATE installations SET state='revoked',revoked_at=? WHERE id=?").run(Date.now(),f.accounts.owner.deviceId);}finally{db.close();}
  assert.equal((await f.raw('owner','apps.cover.read',{appId:f.id})).error?.code,'device_revoked');
});

test('actual async Connect preparation releases fences during decode and rechecks changed publication before readiness',async t=>{
  let enter,release;const entered=new Promise(r=>enter=r),held=new Promise(r=>release=r);
  const f=await fixture(t,{decoder:createCoverDecoder({decode:async input=>{enter();await held;return canonicalizeCover(input);}})});f.enable();
  const args={...await f.args(),mime:'image/png',bytesBase64:await raster()},pending=f.raw('owner','apps.cover.prepare',args);await entered;
  await f.publish('private');release();const result=await pending;assert.equal(result.error?.code,'cover_target_changed');
  assert.equal((await f.call('owner','apps.cover.read',{appId:f.id})).image,null);
  assert.equal(f.read(db=>db.prepare('SELECT reserved_bytes,draft_count FROM app_cover_usage').get().reserved_bytes),0);
});

test('cover migration is explicit, admits exact fragments only and rejects a changed base without touching prior data',async t=>{
  const f=await fixture(t),before=f.snapshot();
  assert.equal((await f.connect.handle({op:'challenge',origin:ORIGIN,args:{operation:'apps.cover.read',digest:digestArgs({})}})).ok,false,'feature-OFF does not register executing cover capabilities');
  f.reopen();
  assert.equal(f.apps.coverExtension,null,'a shared decoder alone cannot enable an unmigrated cover store');
  assert.equal(f.read(inspectAppsSchema),'v9');assert.deepEqual(f.snapshot(),before);
  assert.equal((await f.connect.handle({op:'challenge',origin:ORIGIN,args:{operation:'apps.cover.read',digest:digestArgs({})}})).ok,false);
  f.write(db=>{assert.equal(migrateAppsSchema(db,{legacyTemplate:LEGACY}).migrated,false);});
  assert.deepEqual(f.snapshot(),before);assert.equal(f.read(inspectAppsSchema),'v9');
  assert.throws(()=>f.write(db=>{for(const sql of coverSchemaStatements('app'))db.exec(sql);assert.throws(()=>inspectAppsSchema(db),{code:'apps_schema_unsupported'});throw Object.assign(new Error('rollback fixture'),{code:'fixture_rollback'});}),{code:'fixture_rollback'});
  assert.equal(f.read(inspectAppsSchema),'v9');assert.deepEqual(f.snapshot(),before);
  f.enable();assert.throws(()=>f.write(db=>migrateAppsSchema(db,{legacyTemplate:LEGACY,allowSelectedResourceMigration:true})),{code:'apps_audience_base_upgrade_required'});
  assert.equal(f.read(inspectAppsSchema),'v12');assert.deepEqual(f.snapshot(),before);
  f.write(db=>db.exec('DROP TRIGGER app_cover_version_undo_delete'));
  assert.throws(()=>f.read(inspectAppsSchema),{code:'apps_schema_unsupported'});
  assert.throws(()=>f.reopen(),{code:'apps_schema_unsupported'});
});

test('new empty Apps registry explicitly creates complete format14 and invalid migration flags never coerce to permission',t=>{
  for(const invalid of [0,1,'yes',null])assert.throws(()=>createAppsService({allowCoverMigration:invalid,coverDecoder:createCoverDecoder()}),
    {code:'apps_cover_migration_configuration_invalid'});
  const db=new DatabaseSync(':memory:');t.after(()=>db.close());db.exec('PRAGMA foreign_keys=ON');
  assert.throws(()=>migrateAppsSchema(db,{allowAudienceMigration:'yes',allowCoverMigration:true}),{code:'apps_cover_migration_configuration_invalid'});
  assert.equal(inspectAppsSchema(db),'empty');
  assert.equal(migrateAppsSchema(db,{allowCoverMigration:true,allowSelectedResourceMigration:true}).schema,'soty.apps-registry.v14');
  assert.equal(inspectAppsSchema(db),'v14');assert.equal(baseAppsSchema('v14'),'v8');assert.equal(hasAudienceSchema(db),true);
  assert.match(db.prepare('SELECT store_id FROM app_cover_usage').get().store_id,/^apps_cover_[a-f0-9]{32}$/u);
});
