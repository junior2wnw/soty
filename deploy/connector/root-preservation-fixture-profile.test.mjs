import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,mkdir,rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { initializeSyntheticRootFeedback,initializeSyntheticRootRegistration,initializeSyntheticRootHumanIdentity } from './root-preservation-fixture-profile.mjs';
import { initializeFeedbackSchema } from '../../modules/feedback/server/schema.mjs';
import { initializeRegistrationSchema } from '../../modules/app-contract/server/schema.mjs';
import { initializeHumanIdentitySchema } from '../../modules/human-identity/schema.mjs';
import { createFeedbackService } from '../../modules/feedback/server/index.mjs';
import { createUniversalApps } from '../../server/universal-apps.js';
import { readStorageFormat } from './storage-probe.mjs';
import { snapshotStorage } from './storage-snapshot.mjs';

const denied=()=>{assert.fail('startup profile regression must not call an authority or provider');};
const apps=()=>({operations:new Set(),execute:denied,withAppAuthority:denied,withReviewedAppAuthority:denied});
async function root(t){const directory=await mkdtemp(path.join(tmpdir(),'soty-root-fixture-profile-'));t.after(()=>rm(directory,{recursive:true,force:true}));return directory;}
async function initialize(directory,store,file,fn){
  await mkdir(path.join(directory,store),{recursive:true});const filename=path.join(directory,store,file),db=new DatabaseSync(filename);
  try{db.exec('PRAGMA foreign_keys=ON');fn(db);}finally{db.close();}return filename;
}
function metadata(file,table){const db=new DatabaseSync(file,{readOnly:true});
  try{return Object.fromEntries(db.prepare(`SELECT key,value FROM ${table}`).all().map(row=>[row.key,row.value]));}finally{db.close();}
}

test('actual default feedback constructor reopens the fixture identity, without registry overrides',async t=>{
  const directory=await root(t),file=await initialize(directory,'feedback','feedback.sqlite',initializeSyntheticRootFeedback);
  const before=metadata(file,'feedback_meta');assert.equal(before.registry_id,'soty');assert.equal(before.environment_id,'production');
  for(let i=0;i<2;i++){
    const service=createFeedbackService({databasePath:file,actorActive:()=>false,withAppAuthority:denied});
    service.close();assert.deepEqual(metadata(file,'feedback_meta'),before);
  }
});

for(const [registry,environment] of [['REG.soty','production'],['soty','wrong-environment']])test(`unchanged default feedback rejects ${registry}/${environment}`,async t=>{
  const directory=await root(t),file=await initialize(directory,'feedback','feedback.sqlite',db=>initializeFeedbackSchema(db,registry,environment));
  assert.throws(()=>createFeedbackService({databasePath:file,actorActive:()=>false,withAppAuthority:denied}),error=>error.code==='feedback_storage_unknown');
});

test('actual Root universal-apps default constructors reopen both fixture stores with no actor/provider calls',async t=>{
  const directory=await root(t),feedback=await initialize(directory,'feedback','feedback.sqlite',initializeSyntheticRootFeedback);
  const registration=await initialize(directory,'app-registration','registry.sqlite',initializeSyntheticRootRegistration);
  const before={feedback:metadata(feedback,'feedback_meta'),registration:metadata(registration,'registration_metadata')};
  assert.equal(before.registration.registry_id,'soty');assert.equal(before.registration.environment_id,'production');
  for(let i=0;i<2;i++){
    const service=createUniversalApps({dataDir:directory,apps:apps(),actorActive:()=>false});service.close();
    assert.deepEqual({feedback:metadata(feedback,'feedback_meta'),registration:metadata(registration,'registration_metadata')},before);
  }
  for(const [file,tables] of [[feedback,['feedback_installations','feedback_tickets']],[registration,['registration_heads','registration_receipts']]]){
    const db=new DatabaseSync(file,{readOnly:true});try{for(const table of tables)assert.equal(db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n,0);}finally{db.close();}
  }
});

for(const [registryId,environmentId,code] of [['REG.soty','production','registration_registry_mismatch'],['soty','wrong-environment','registration_environment_mismatch']])test(`unchanged Root constructor rejects registration ${registryId}/${environmentId}`,async t=>{
  const directory=await root(t);await initialize(directory,'feedback','feedback.sqlite',initializeSyntheticRootFeedback);
  await initialize(directory,'app-registration','registry.sqlite',db=>initializeRegistrationSchema(db,{registryId,environmentId}));
  assert.throws(()=>createUniversalApps({dataDir:directory,apps:apps(),actorActive:()=>false}),error=>error.code===code);
});

test('genuine Human Identity fixture uses the Root registry/environment and keeps its synthetic issuer',async t=>{
  const directory=await root(t),file=await initialize(directory,'human-identity','identity.sqlite',initializeSyntheticRootHumanIdentity);
  const before=metadata(file,'human_identity_meta');assert.equal(before.registry_id,'soty');assert.equal(before.environment_id,'production');
  assert.equal(before.issuer,'https://human.fixture.invalid/human-identity');
  const db=new DatabaseSync(file),profile={enabled:true,registryId:'soty',environmentId:'production',issuer:before.issuer};
  try{
    db.function('human_identity_gc_epoch',()=>0);
    assert.equal(initializeHumanIdentitySchema(db,profile).schemaVersion,3);
    for(const change of [{registryId:'REG.soty'},{environmentId:'wrong-environment'},{issuer:'https://other.fixture.invalid/human-identity'}])
      assert.throws(()=>initializeHumanIdentitySchema(db,{...profile,...change}),error=>error.code==='human_identity_storage_identity_mismatch');
  }finally{db.close();}
  assert.deepEqual(metadata(file,'human_identity_meta'),before);
});

test('the three genuine fixture profiles also pass the unchanged independent v7 format reader',async t=>{
  const directory=await root(t),scratch=await root(t);
  await initialize(directory,'feedback','feedback.sqlite',initializeSyntheticRootFeedback);
  await initialize(directory,'app-registration','registry.sqlite',initializeSyntheticRootRegistration);
  await initialize(directory,'human-identity','identity.sqlite',initializeSyntheticRootHumanIdentity);
  const format=await readStorageFormat(await snapshotStorage(directory,path.join(scratch,'snapshot')));
  assert.equal(format.ok,true);assert.equal(format.schema,'soty.storage-format.v7');
  assert.equal(format.feedback,1);assert.equal(format.appRegistration,1);assert.equal(format.humanIdentity,3);
});
