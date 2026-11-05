import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,readFileSync,existsSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename,resolve} from 'node:path';
import {generateKeyPairSync,sign} from 'node:crypto';
import {canonicalJson,digestArgs,createConnectService} from '../server/index.mjs';
import {migrateDatabase} from '../server/schema.mjs';
import {createEphemeralChallengeStore} from '../server/ephemeral-challenges.mjs';

const OP='canvas.vault.unlock',OTHER='profile.rename',TIME=1800000000000;
const args=password=>({expectedAccountId:'synthetic_account',objectId:'synthetic_object',password});
const dictionary=['wrong-synthetic-password','review-synthetic-password','another-synthetic-password'];
function row(id='challenge_synthetic_01',operation=OP,input=args(dictionary[1])){
  const digest=digestArgs(input),expires_at=TIME+90000,origin='https://synthetic.example';
  return{id,origin,operation,digest,message:canonicalJson({schema:'connect.proof.v1',projectId:'soty',origin,operation,digest,challengeId:id,expiresAt:expires_at}),expires_at,consumed_at:null};
}
const insert=(db,value)=>db.prepare('INSERT INTO challenges(id,origin,operation,digest,message,expires_at,consumed_at) VALUES(?,?,?,?,?,?,?)')
  .run(value.id,value.origin,value.operation,value.digest,value.message,value.expires_at,value.consumed_at);
const schema=db=>JSON.stringify([db.prepare('PRAGMA user_version').get(),db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()]);
function fixture(t){
  const directory=mkdtempSync(join(tmpdir(),'soty-ephemeral-challenges-')),filename=join(directory,'connect.sqlite'),connections=[];
  const track=db=>{connections.push(db);return db;},db=track(new DatabaseSync(filename));migrateDatabase(db,'soty');
  t.after(()=>{for(const connection of connections.reverse())try{connection.close();}catch{}assert.equal(dirname(resolve(directory)),resolve(tmpdir()));assert.ok(basename(directory).startsWith('soty-ephemeral-challenges-'));rmSync(directory,{recursive:true,force:true});});
  return{directory,filename,db,track};
}
function bytes(filename){return Buffer.concat(['','-wal','-journal'].filter(suffix=>existsSync(filename+suffix)).map(suffix=>readFileSync(filename+suffix)));}
function guessPasswords(filename){const data=bytes(filename);return dictionary.filter(password=>data.includes(Buffer.from(digestArgs(args(password)))));}
const noPasswordOracle=filename=>assert.deepEqual(guessPasswords(filename),[],'SQLite current files must not expose a cheap known-arguments password oracle');
const memory=(db,options={})=>createEphemeralChallengeStore({db,operations:new Set([OP]),clock:()=>TIME,...options});

test('the identical offline dictionary oracle detects old SQL persistence and passes RAM issue/consume/restart plus WAL scan',t=>{
  const f=fixture(t),legacy=row();insert(f.db,legacy);
  assert.throws(()=>noPasswordOracle(f.filename),error=>error.code==='ERR_ASSERTION'&&error.message.includes('password oracle'));
  const before=schema(f.db),store=memory(f.db);assert.equal(store.readiness().ready,true);noPasswordOracle(f.filename);assert.equal(schema(f.db),before,'no schema or reader format migration');
  store.issue(legacy);assert.deepEqual(store.get(legacy.id),legacy);assert.equal(f.db.prepare('SELECT count(*) AS count FROM challenges WHERE operation=?').get(OP).count,0);
  noPasswordOracle(f.filename);assert.equal(store.consume(legacy.id,TIME+1),true);assert.equal(store.consume(legacy.id,TIME+2),false);assert.equal(store.get(legacy.id).consumed_at,TIME+1);noPasswordOracle(f.filename);
  store.close();assert.equal(store.readiness().ready,false);assert.equal(store.hasOperation(OP),true,'closed policy cannot downgrade to SQL');f.db.close();
  const reopened=f.track(new DatabaseSync(f.filename));migrateDatabase(reopened,'soty');const next=memory(reopened);assert.equal(next.get(legacy.id),undefined);noPasswordOracle(f.filename);next.close();
});

test('constructor erases already-deleted legacy freelist fingerprints even when its DELETE finds zero rows',t=>{
  const f=fixture(t);f.db.exec('PRAGMA secure_delete=OFF');
  f.db.exec('BEGIN IMMEDIATE');for(let index=0;index<256;index++)insert(f.db,row('challenge_freelist_'+String(index).padStart(5,'0')));f.db.exec('COMMIT');
  insert(f.db,row('challenge_ordinary_01',OTHER,{label:'ordinary fixture'}));f.db.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get();
  f.db.prepare('DELETE FROM challenges WHERE operation=?').run(OP);f.db.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get();
  assert.equal(f.db.prepare('SELECT count(*) AS count FROM challenges WHERE operation=?').get(OP).count,0);assert.ok(f.db.prepare('PRAGMA freelist_count').get().freelist_count>0);
  assert.ok(guessPasswords(f.filename).includes(dictionary[1]),'the control must actually leave the digest in freed main-file pages');
  const store=memory(f.db);noPasswordOracle(f.filename);assert.equal(f.db.prepare('PRAGMA freelist_count').get().freelist_count,0);store.close();
});

test('ordinary persisted challenges and all schema/identity metadata remain unchanged',t=>{
  const f=fixture(t),ordinary=row('challenge_ordinary_01',OTHER,{label:'ordinary fixture'}),sensitive=row();insert(f.db,ordinary);insert(f.db,sensitive);
  const before=schema(f.db),meta=f.db.prepare('SELECT * FROM connect_meta ORDER BY key').all(),store=memory(f.db);
  assert.deepEqual({...f.db.prepare('SELECT * FROM challenges WHERE id=?').get(ordinary.id)},ordinary);assert.equal(store.hasOperation(OTHER),false);
  assert.throws(()=>store.issue(ordinary),{code:'connect_ephemeral_challenge_invalid'});assert.equal(schema(f.db),before);assert.deepEqual(f.db.prepare('SELECT * FROM connect_meta ORDER BY key').all(),meta);
  store.cleanup(TIME+90001);assert.deepEqual({...f.db.prepare('SELECT * FROM challenges WHERE id=?').get(ordinary.id)},ordinary,'RAM expiry does not change ordinary SQL persistence');store.close();
});

test('immutable operation policy and returned row copies prevent later metadata or caller mutation from downgrading storage',t=>{
  const f=fixture(t),operations=new Set([OP]),store=memory(f.db,{operations});operations.clear();operations.add('notes.capture');assert.equal(store.hasOperation(OP),true);assert.equal(store.hasOperation('notes.capture'),false);
  const value=row();store.issue(value);value.digest='A'.repeat(43);value.message='mutated';const retrieved=store.get(value.id);retrieved.message='changed again';retrieved.consumed_at=TIME;
  assert.equal(store.get(value.id).message,row().message);assert.equal(store.get(value.id).consumed_at,null);assert.throws(()=>store.issue(row(value.id)),{code:'connect_ephemeral_challenge_conflict'});store.close();
});

test('capacity preserves every unexpired challenge; expiry and replay are bounded and consumed once',t=>{
  const f=fixture(t);let now=TIME;const store=memory(f.db,{maxEntries:2,clock:()=>now}),first=row(),second=row('challenge_synthetic_02');store.issue(first);store.issue(second);
  assert.throws(()=>store.issue(row('challenge_synthetic_03')),{code:'connect_ephemeral_challenge_capacity'});assert.ok(store.get(first.id));assert.ok(store.get(second.id));
  assert.equal(store.consume(first.id,now),true);assert.equal(store.consume(first.id,now+1),false);assert.throws(()=>store.issue(row('challenge_synthetic_03')),{code:'connect_ephemeral_challenge_capacity'},'consumed tombstone remains replay evidence until expiry');
  now=TIME+90000;store.cleanup(now);assert.equal(store.get(first.id),undefined);assert.equal(store.consume(second.id,now),false);
  store.issue({...row('challenge_synthetic_03'),expires_at:now+90000});assert.equal(store.get('challenge_synthetic_03').consumed_at,null);store.close();assert.throws(()=>store.get(first.id),{code:'connect_ephemeral_challenge_closed'});
});

test('issue rejects unbounded/malformed rows and cannot extend the existing 90-second TTL',t=>{
  const f=fixture(t),store=memory(f.db),valid=row();
  for(const invalid of[{...valid,expires_at:TIME},{...valid,expires_at:TIME+90001},{...valid,message:'x'.repeat(8193)},{...valid,origin:'x'.repeat(2049)},{...valid,consumed_at:TIME},{...valid,extra:'secret'},{...valid,digest:'='.repeat(43)}])assert.throws(()=>store.issue(invalid),{code:'connect_ephemeral_challenge_invalid'});
  store.issue(valid);assert.ok(store.get(valid.id));store.close();
});

test('a held external WAL reader cannot be reported as erased; after release the exact gate succeeds',t=>{
  const f=fixture(t),legacy=row();insert(f.db,legacy);f.db.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get();
  const reader=f.track(new DatabaseSync(f.filename));reader.exec('BEGIN');assert.equal(reader.prepare('SELECT digest FROM challenges WHERE id=?').get(legacy.id).digest,legacy.digest);
  assert.throws(()=>memory(f.db),{code:'connect_ephemeral_erasure_failed'});assert.ok(guessPasswords(f.filename).includes(dictionary[1]),'old reader really pins still-sensitive file bytes');
  assert.equal(f.db.prepare('PRAGMA busy_timeout').get().timeout,5000,'the failed startup gate restores the caller connection policy');
  reader.exec('ROLLBACK');reader.close();const store=memory(f.db);assert.equal(store.readiness().ready,true);noPasswordOracle(f.filename);store.close();
});

test('read-only storage and a VACUUM fault fail closed without a readiness object or schema changes',t=>{
  for(const mode of['readonly','vacuum']){
    const f=fixture(t),before=schema(f.db);insert(f.db,row());let wrapped=f.db;
    if(mode==='readonly')f.db.exec('PRAGMA query_only=ON');
    else wrapped=new Proxy(f.db,{get(target,key){if(key==='exec')return sql=>{if(sql==='VACUUM')throw Error('synthetic write fault');return target.exec(sql);};const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;}});
    assert.throws(()=>memory(wrapped),{code:'connect_ephemeral_erasure_failed'});assert.equal(schema(f.db),before);
    if(mode==='readonly')f.db.exec('PRAGMA query_only=OFF');
    const store=memory(f.db);noPasswordOracle(f.filename);store.close();
  }
});

test('a pre-existing transaction is rejected without consuming the callers transaction',t=>{
  const f=fixture(t);f.db.exec('BEGIN IMMEDIATE');assert.throws(()=>memory(f.db),{code:'connect_ephemeral_erasure_failed'});assert.equal(f.db.isTransaction,true);f.db.exec('ROLLBACK');
  const store=memory(f.db);store.close();
});

test('an empty snapshot leaves the ordinary SQL-only connection unchanged',t=>{
  const f=fixture(t),ordinary=row('challenge_ordinary_01',OTHER,{label:'ordinary fixture'});insert(f.db,ordinary);const store=memory(f.db,{operations:new Set()});
  assert.equal(store.hasOperation(OP),false);assert.deepEqual({...f.db.prepare('SELECT * FROM challenges WHERE id=?').get(ordinary.id)},ordinary);store.close();
});

test('actual Core signed dispatch retains memory policy, single-use proofs and ordinary persisted challenges across restart',async t=>{
  const f=fixture(t);f.db.close();const origin='https://synthetic.example',key=generateKeyPairSync('ec',{namedCurve:'prime256v1'}),encryption=generateKeyPairSync('ec',{namedCurve:'prime256v1'});
  const publicJwk=key.publicKey.export({format:'jwk'}),extension={operations:new Set([OP]),challengeStorage:'memory',execute:()=>({accepted:true})};
  const open=()=>f.track(createConnectService({databasePath:f.filename,projectId:'soty',allowedOrigins:[origin],clock:()=>TIME,extensions:[extension]}));
  let service=open();
  async function prepare(op,input){const challenge=await service.handle({op:'challenge',origin,args:{operation:op,digest:digestArgs(input)}});assert.equal(challenge.ok,true,challenge.error?.code);
    return{op,args:input,origin,proof:{challengeId:challenge.challengeId,publicJwk,signature:sign('sha256',Buffer.from(challenge.message),{key:key.privateKey,dsaEncoding:'ieee-p1363'}).toString('base64url')}};}
  const boot=await service.handle(await prepare('bootstrap',{label:'Synthetic person',encryptionPublicJwk:encryption.publicKey.export({format:'jwk'})}));assert.equal(boot.ok,true);
  extension.challengeStorage='persistent';
  const secret=await prepare(OP,args(dictionary[1]));noPasswordOracle(f.filename);assert.equal((await service.handle(secret)).accepted,true);assert.equal((await service.handle(secret)).error.code,'challenge_consumed');noPasswordOracle(f.filename);
  const pendingSecret=await prepare(OP,args(dictionary[1])),ordinary=await prepare(OTHER,{label:'Synthetic renamed'});
  service.close();extension.challengeStorage='memory';service=open();assert.equal((await service.handle(pendingSecret)).error.code,'challenge_expired');noPasswordOracle(f.filename);
  assert.equal((await service.handle(ordinary)).ok,true,'ordinary challenge remains redeemable after a process lifetime');
  const db=f.track(new DatabaseSync(f.filename));assert.equal(db.prepare('SELECT count(*) AS count FROM challenges WHERE operation=?').get(OP).count,0);assert.ok(db.prepare('SELECT count(*) AS count FROM challenges WHERE operation=?').get(OTHER).count>0);db.close();service.close();
});

test('actual Core never falls back to a matching SQL challenge for a missing ephemeral proof',async t=>{
  const f=fixture(t);f.db.close();const origin='https://synthetic.example',key=generateKeyPairSync('ec',{namedCurve:'prime256v1'}),publicJwk=key.publicKey.export({format:'jwk'});
  const extension={operations:new Set([OP]),challengeStorage:'memory',execute:()=>({accepted:true})};
  const open=()=>f.track(createConnectService({databasePath:f.filename,projectId:'soty',allowedOrigins:[origin],clock:()=>TIME,extensions:[extension]}));
  let service=open();const input=args(dictionary[1]),challenge=await service.handle({op:'challenge',origin,args:{operation:OP,digest:digestArgs(input)}});assert.equal(challenge.ok,true);service.close();service=open();
  const shadow=f.track(new DatabaseSync(f.filename));insert(shadow,{id:challenge.challengeId,origin,operation:OP,digest:digestArgs(input),message:challenge.message,expires_at:challenge.expiresAt,consumed_at:null});shadow.close();
  const reply=await service.handle({op:OP,args:input,origin,proof:{challengeId:challenge.challengeId,publicJwk,signature:sign('sha256',Buffer.from(challenge.message),{key:key.privateKey,dsaEncoding:'ieee-p1363'}).toString('base64url')}});
  assert.equal(reply.ok,false);assert.equal(reply.error.code,'challenge_expired','the valid matching SQL proof must not bypass RAM lifetime');service.close();service=open();noPasswordOracle(f.filename);service.close();
});
