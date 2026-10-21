import { DatabaseSync } from 'node:sqlite';
import { mkdtemp,rm,realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join,dirname,basename } from 'node:path';
import sharp from 'sharp';
import { createCoverStore } from '../../server/index.mjs';
import { coverPrefix,coverSchemaStatements,coverSchemaSeed } from '../../server/schema.mjs';
import { createCoverDecoder } from '../../server/codec.mjs';
import { migrateLegacyAvatarTable } from '../../server/legacy-avatar.mjs';

export const alice=Object.freeze({accountId:'owner_alice',deviceId:'device_alice',label:'Synthetic Alice'});
export const bob=Object.freeze({accountId:'owner_bob',deviceId:'device_bob',label:'Synthetic Bob'});
export const app1='app-'+'1'.repeat(32),app2='app-'+'2'.repeat(32),app3='app-'+'3'.repeat(32);
export async function image(color='#567890',width=960,height=960) {return sharp({create:{width,height,channels:3,background:color}}).png().toBuffer();}
export const dataUrl=(bytes,mime='image/png')=>`data:${mime};base64,${Buffer.from(bytes).toString('base64')}`;
export async function fixture(t,{kind='app',format=14,limits={},decoder=createCoverDecoder(),legacyPair,onCommitFailure}={}) {
  const folder=await mkdtemp(join(tmpdir(),'soty-cover-runtime-'));let db=new DatabaseSync(join(folder,'store.sqlite')),store,now=1_900_000_000_000,serial=0,authorityDepth=0;
  const instanceId=`cover_fixture_${kind}`,p=coverPrefix(kind),revoked=new Set(),readers=new Set();
  db.exec('PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;');
  if(kind==='app') {
    db.exec(`CREATE TABLE apps_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);CREATE TABLE local_apps(id TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,state TEXT NOT NULL,target_revision INTEGER NOT NULL,policy_epoch INTEGER NOT NULL,is_public INTEGER NOT NULL)`);
    db.prepare("INSERT INTO apps_meta VALUES('schema',?)").run(`soty.apps-registry.v${format}`);
    for(const [target,owner] of [[app1,alice.accountId],[app2,alice.accountId],[app3,bob.accountId]])db.prepare("INSERT INTO local_apps VALUES(?,?,'active',1,1,0)").run(target,owner);
  } else {
    db.exec("CREATE TABLE world_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);INSERT INTO world_meta VALUES('lineage','soty.world.sqlite.v1'),('schema_version','5');CREATE TABLE profiles(account_id TEXT PRIMARY KEY,revision INTEGER NOT NULL,avatar_revision INTEGER,audience TEXT NOT NULL,display_name TEXT NOT NULL);CREATE TABLE profile_avatars(account_id TEXT PRIMARY KEY REFERENCES profiles(account_id),mime TEXT NOT NULL,image BLOB NOT NULL,thumbnail_mime TEXT NOT NULL,thumbnail BLOB NOT NULL); PRAGMA user_version=5;");
    for(const actor of [alice,bob])db.prepare("INSERT INTO profiles VALUES(?,7,NULL,'private',?)").run(actor.accountId,actor.label);
    if(legacyPair){db.prepare('INSERT INTO profile_avatars VALUES(?,?,?,?,?)').run(alice.accountId,legacyPair.mime??'image/png',legacyPair.image,legacyPair.thumbnailMime??'image/png',legacyPair.thumbnail);db.prepare('UPDATE profiles SET avatar_revision=7 WHERE account_id=?').run(alice.accountId);}
  }
  db.exec('BEGIN IMMEDIATE');
  for(const sql of coverSchemaStatements(kind))db.exec(sql);
  db.exec(coverSchemaSeed(kind,instanceId));
  if(kind==='profile'){migrateLegacyAvatarTable(db,{now});db.prepare("UPDATE world_meta SET value='6' WHERE key='schema_version'").run();}
  db.exec(`PRAGMA user_version=${kind==='app'?format:6}; COMMIT;`);
  const ports=()=>({db,kind,instanceId,decoder,clock:()=>now,limits:{mutationsPerMinute:10000,...limits},
    withAuthority(actor,operation){if(revoked.has(actor.deviceId))throw Object.assign(new Error('device_revoked'),{code:'device_revoked'});if(authorityDepth)throw new Error('fixture_authority_nested');authorityDepth++;try{return operation();}finally{authorityDepth--;}} ,
    readTarget(actor,targetId){if(authorityDepth!==1||!db.isTransaction)throw new Error('fixture_outside_fence');
      const row=kind==='app'?db.prepare('SELECT * FROM local_apps WHERE id=?').get(targetId):db.prepare('SELECT * FROM profiles WHERE account_id=?').get(targetId);if(!row)return null;
      const owner=kind==='app'?row.owner_account_id:row.account_id;
      return {ownerAccountId:owner,targetRevision:kind==='app'?row.target_revision:row.revision,policyEpoch:kind==='app'?row.policy_epoch:row.revision,state:kind==='app'?row.state:'active',canManage:owner===actor.accountId,canRead:owner===actor.accountId||(kind==='app'?Boolean(row.is_public):row.audience==='public')||readers.has(`${actor.accountId}:${targetId}`)};
    },
    onCommit({targetId,mode}){if(authorityDepth!==1||!db.isTransaction)throw new Error('fixture_outside_commit');if(onCommitFailure?.())throw new Error('fixture_commit_failure');if(kind==='profile')db.prepare('UPDATE profiles SET avatar_revision=CASE WHEN ?=\'custom\' THEN revision+1 ELSE NULL END,revision=revision+1 WHERE account_id=?').run(mode,targetId);},
    ...(kind==='profile'?{projectOwner(actor,targetId){const row=db.prepare('SELECT * FROM profiles WHERE account_id=?').get(targetId);return {profileId:row.account_id,revision:row.revision,avatarRevision:row.avatar_revision,displayName:row.display_name};}}:{}),
  });
  store=createCoverStore(ports());
  t.after(async()=>{store.dispose();db.close();const actual=await realpath(folder),temporary=await realpath(tmpdir());if(dirname(actual).toLowerCase()!==temporary.toLowerCase()||!basename(actual).startsWith('soty-cover-runtime-'))throw new Error('unsafe_fixture_cleanup');await rm(actual,{recursive:true,force:true});});
  return {kind,p,instanceId,revoked,readers,decoder,get db(){return db;},get store(){return store;},get now(){return now;},advance(ms){now+=ms;},setClock(value){now=value;},
    request(){return `cover1.${now}.${(++serial).toString(16).padStart(32,'0')}`;},
    args(targetId=kind==='app'?app1:alice.accountId){const row=kind==='app'?db.prepare('SELECT target_revision,policy_epoch FROM local_apps WHERE id=?').get(targetId):db.prepare('SELECT revision FROM profiles WHERE account_id=?').get(targetId);return {requestId:this.request(),targetId,expectedTargetRevision:kind==='app'?row.target_revision:row.revision,expectedPolicyEpoch:kind==='app'?row.policy_epoch:row.revision,expectedCoverRevision:db.prepare(`SELECT revision FROM ${p}_heads WHERE target_id=?`).get(targetId)?.revision??0};},
    async publish({actor=alice,targetId=kind==='app'?app1:alice.accountId,color='#567890'}={}){const args={...this.args(targetId),bytes:await image(color),mime:'image/png'};const prepared=await store.prepareUpload(actor,args);const commitArgs={requestId:this.request(),targetId,draftId:prepared.draft.id};return {prepared,prepareArgs:args,commitArgs,committed:store.commit(actor,commitArgs)};},
    reopen(){store.dispose();db.close();db=new DatabaseSync(join(folder,'store.sqlite'));db.exec('PRAGMA foreign_keys=ON');store=createCoverStore(ports());},ports,
  };
}
