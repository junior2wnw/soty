// Synthetic actual Connect/World/Apps fixture, preserved from public acceptance test lines1..96.
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { generateKeyPairSync, randomUUID, sign } from 'node:crypto';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import sharp from 'sharp';
import { createConnectService, digestArgs } from '../../../modules/connect/server/index.mjs';
import { createWorldService } from '../../../modules/world/server/index.mjs';
import { createAppsService, operations } from '../../../modules/apps/server/index.mjs';
import { appCoverOperations } from '../../../modules/apps/server/cover.mjs';
import { migrateAppsSchema, inspectAppsSchema, ensureCanonicalDomain, ensureInitialPublication, runtimeTargetDigest,
  RUNTIME_PROFILE, SCOPED_RUNTIME_PROFILE, RESOURCE_RUNTIME_PROFILE } from '../../../modules/apps/server/schema.mjs';
import { baseAppsSchema, hasAudienceSchema } from '../../../modules/apps/server/audience.mjs';
import { createScopedAdmissionRegistry } from '../../../modules/apps/scoped-embed/admissions.mjs';
import { HIVE_SELECTED_SOURCE } from '../../../modules/apps/scoped-embed/resource-route-adapters.mjs';
import { createCoverDecoder, canonicalizeCover } from '../../../modules/cover-art/server/codec.mjs';
import { coverSchemaStatements } from '../../../modules/cover-art/server/schema.mjs';

const ORIGIN='https://covers.example', LEGACY='https://{appId}.legacy.example', NAMED='https://apps.example';
const success=value=>{assert.equal(value.ok,true,value.error?.code);return value;};
function keys() { const a=generateKeyPairSync('ec',{namedCurve:'prime256v1'}),b=generateKeyPairSync('ec',{namedCurve:'prime256v1'});
  return {privateKey:a.privateKey,publicJwk:a.publicKey.export({format:'jwk'}),encryptionPublicJwk:b.publicKey.export({format:'jwk'})}; }
async function signed(connect,identity,op,args={}) {
  const challenge=success(await connect.handle({op:'challenge',origin:ORIGIN,args:{operation:op,digest:digestArgs(args)}}));
  return connect.handle({op,args,origin:ORIGIN,proof:{challengeId:challenge.challengeId,publicJwk:identity.publicJwk,
    signature:sign('sha256',Buffer.from(challenge.message),{key:identity.privateKey,dsaEncoding:'ieee-p1363'}).toString('base64url')}});
}
export const raster=async color=>(await sharp({create:{width:512,height:512,channels:3,background:color??'#567890'}}).png().toBuffer()).toString('base64');
const oldTables=['app_devices','local_apps','local_app_grants','app_domain_zones','app_domain_heads','app_domains','app_domain_receipts',
  'app_runtime_targets','app_publications','app_publication_domains','app_publication_receipts','app_source_heads','app_source_receipts',
  'app_saved_heads','app_saved_entries','app_saved_receipts','app_discussion_heads','app_discussion_conversations','app_discussion_messages',
  'app_discussion_changes','app_discussion_usage','app_discussion_rates','app_audiences','app_discussion_head_audiences','app_discussion_conversation_audiences','app_scoped_embed_admissions'];
export async function signedAppsCoverFixture(t,{base=6,decoder=createCoverDecoder()}={}) {
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
