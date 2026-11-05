import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash,generateKeyPairSync,sign} from 'node:crypto';
import {mkdtempSync,rmSync,realpathSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createConnectService,digestArgs} from '../server/index.mjs';

const origin='https://monitoring-host.test',code=value=>error=>error?.code===value;
function identity(){const keys=generateKeyPairSync('ec',{namedCurve:'prime256v1'}),encryption=generateKeyPairSync('ec',{namedCurve:'prime256v1'});return{privateKey:keys.privateKey,publicJwk:keys.publicKey.export({format:'jwk'}),encryptionPublicJwk:encryption.publicKey.export({format:'jwk'})};}
async function invoke(service,keys,op,args={}){
 const challenge=await service.handle({origin,op:'challenge',args:{operation:op,digest:digestArgs(args)}});assert.equal(challenge.ok,true,challenge.error?.code);
 const result=await service.handle({origin,op,args,proof:{challengeId:challenge.challengeId,publicJwk:keys.publicJwk,signature:sign('sha256',Buffer.from(challenge.message),{key:keys.privateKey,dsaEncoding:'ieee-p1363'}).toString('base64url')}});
 assert.equal(result.ok,true,result.error?.code);return result;
}
async function fixture(t){
 const parent=realpathSync(tmpdir()),directory=mkdtempSync(path.join(parent,'soty-monitoring-admin-')),databasePath=path.join(directory,'accounts.sqlite'),owner=identity(),stranger=identity();let service,actor;
 const extension={operations:new Set(['monitoring.capture']),execute(request){actor=request.actor;return{captured:true};}};
 const open=admins=>createConnectService({databasePath,projectId:'monitoring-host',allowedOrigins:[origin],platformAdminAccountIds:admins,extensions:[extension]});
 service=open([]);t.after(()=>{service.close();assert.equal(path.dirname(realpathSync(directory)),parent);assert.match(path.basename(directory),/^soty-monitoring-admin-/);rmSync(directory,{recursive:true});});
 const own=await invoke(service,owner,'bootstrap',{label:'Synthetic operator',encryptionPublicJwk:owner.encryptionPublicJwk});await invoke(service,stranger,'bootstrap',{label:'Synthetic member',encryptionPublicJwk:stranger.encryptionPublicJwk});service.close();service=open([own.accountId]);
 const capture=async keys=>{await invoke(service,keys,'monitoring.capture');return actor;};
 const dataHash=()=>{const db=new DatabaseSync(databasePath,{readOnly:true});try{return createHash('sha256').update(JSON.stringify(['platform_admission','platform_invitation_receipts','accounts','installations'].map(name=>[name,db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all()]))).digest('hex');}finally{db.close();}};
 return{get service(){return service;},owner,stranger,capture,dataHash,reopen(admins=[own.accountId]){service.close();service=open(admins);}};
}

test('host admission uses the canonical CAS and persistent receipt; receipt lookup cannot mutate data',async t=>{
 const f=await fixture(t),actor=await f.capture(f.owner),request={requestId:'monitoring_policy_1',expectedRevision:0,inviteOnly:false,defaultQuota:5};
 const call=(op,args)=>f.service.platformAdmission({actor,op,args});
 assert.equal(call('platform.admission.get',{}).manageAllowed,true);
 const before=f.dataHash();assert.deepEqual(call('platform.admission.receipt',request),{found:false});assert.equal(f.dataHash(),before);
 const result=call('platform.admission.update',request);assert.equal(result.revision,1);assert.equal(result.defaultQuota,5);
 const after=f.dataHash();assert.deepEqual(call('platform.admission.receipt',request),{found:true,result});assert.equal(f.dataHash(),after);
 assert.deepEqual(call('platform.admission.update',request),result);assert.equal(f.dataHash(),after);
 assert.throws(()=>call('platform.admission.receipt',{...request,defaultQuota:6}),code('platform_request_conflict'));
 assert.throws(()=>call('platform.admission.update',{...request,requestId:'stale_policy'}),code('platform_policy_conflict'));
 assert.throws(()=>call('platform.invites.issue',{}),code('unsupported_operation'));
 f.reopen();const currentActor=await f.capture(f.owner),reopenedHash=f.dataHash();
 assert.deepEqual(f.service.platformAdmission({actor:currentActor,op:'platform.admission.receipt',args:request}),{found:true,result});assert.equal(f.dataHash(),reopenedHash);
});

test('host admission rejects copied, foreign and truly revoked actors rather than trusting account JSON',async t=>{
 const f=await fixture(t),actor=await f.capture(f.owner),stranger=await f.capture(f.stranger),request={requestId:'denied_policy',expectedRevision:0,inviteOnly:false,defaultQuota:5};
 assert.throws(()=>f.service.platformAdmission({actor:{...actor},op:'platform.admission.update',args:request}),code('connect_authority_actor_invalid'));
 assert.throws(()=>f.service.platformAdmission({actor:stranger,op:'platform.admission.update',args:request}),code('platform_admin_required'));
 assert.throws(()=>f.service.platformAdmission({actor:stranger,op:'platform.admission.receipt',args:request}),code('platform_admin_required'));
 const phone=identity(),start=await invoke(f.service,phone,'enrollment.start',{label:'Synthetic second device',encryptionPublicJwk:phone.encryptionPublicJwk});
 await invoke(f.service,f.owner,'enrollment.approve',{requestId:start.requestId,wrappedKey:{schema:'test.encrypted',ciphertext:'synthetic'}});
 const joined=await invoke(f.service,phone,'enrollment.finish',{requestId:start.requestId,expectedAccountId:actor.accountId}),phoneActor=await f.capture(phone);
 await invoke(f.service,f.owner,'device.revoke',{deviceId:joined.deviceId});
 assert.throws(()=>f.service.platformAdmission({actor:phoneActor,op:'platform.admission.receipt',args:request}),code('device_revoked'));
});

test('the host-only receipt port is not an unsigned or additional signed RPC',async t=>{
 const f=await fixture(t);
 const request=await f.service.handle({origin,op:'platform.admission.receipt',args:{}});assert.equal(request.ok,false);
 const challenge=await f.service.handle({origin,op:'challenge',args:{operation:'platform.admission.receipt',digest:digestArgs({})}});assert.equal(challenge.ok,false);assert.equal(challenge.error.code,'unsupported_operation');
});

test('private platform admin fingerprint observes actual loaded Set, is readonly and adds no RPC',async t=>{
 const f=await fixture(t),owner=await f.capture(f.owner),other=await f.capture(f.stranger),admins=[owner.accountId],hash=ids=>createHash('sha256').update(JSON.stringify([...new Set(ids)].sort())).digest('hex');
 f.reopen(admins);const before=f.dataHash();assert.equal(f.service.platformAdminAccessSha256(),hash([owner.accountId]));admins[0]=other.accountId;
 assert.equal(f.service.platformAdminAccessSha256(),hash([owner.accountId]));assert.equal(f.dataHash(),before);
 const current=await f.capture(f.owner);assert.equal(f.service.platformAdmission({actor:current,op:'platform.admission.get'}).manageAllowed,true);
 const unsigned=await f.service.handle({origin,op:'platformAdminAccessSha256',args:{}});assert.equal(unsigned.ok,false);
 const challenge=await f.service.handle({origin,op:'challenge',args:{operation:'platformAdminAccessSha256',digest:digestArgs({})}});assert.equal(challenge.ok,false);assert.equal(challenge.error.code,'unsupported_operation');
 f.reopen([]);const revoked=await f.capture(f.owner);assert.equal(f.service.platformAdminAccessSha256(),hash([]));assert.equal(f.service.platformAdmission({actor:revoked,op:'platform.admission.get'}).manageAllowed,false);
});
