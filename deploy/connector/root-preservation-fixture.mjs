import { DatabaseSync } from 'node:sqlite';
import { mkdir,readdir,writeFile,chown,lstat,mkdtemp,rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { createRoomStore } from '../../server/room-storage-v2.mjs';
import { migrateAppsSchema } from '../../modules/apps/server/schema.mjs';
import { migrateNotes } from '../../modules/notes/server/schema.mjs';
import { initializeCapabilitiesSchema } from '../../modules/capabilities/server/schema.mjs';
import { migrateDatabase } from '../../modules/connect/server/schema.mjs';
import { initializeSyntheticRootFeedback,initializeSyntheticRootRegistration,initializeSyntheticRootHumanIdentity } from './root-preservation-fixture-profile.mjs';
import { migrateWorld } from '../../modules/world/server/schema.mjs';
import { migrateField } from '../../modules/world/server/field.mjs';
import { readStorageFormat } from './storage-probe.mjs';
import { snapshotStorage } from './storage-snapshot.mjs';

/** Only invented records. Not a production backup/observer or an application boot. */
export async function createSyntheticRootFixture(root){
  await mkdir(root,{recursive:true,mode:0o700});if((await readdir(root)).length)throw Error('fixture_target_not_empty');
  await writeFile(path.join(root,'.preservation-synthetic'),'Only synthetic fixture data.\n',{flag:'wx',mode:0o600});
  createRoomStore(root).close();
  const db=async(dir,file,initialize)=>{await mkdir(path.join(root,dir),{recursive:true,mode:0o700});const handle=new DatabaseSync(path.join(root,dir,file));
    try{handle.exec('PRAGMA foreign_keys=ON;');await initialize(handle);}finally{handle.close();}};
  await db('apps','registry.sqlite',s=>migrateAppsSchema(s,{allowScopedEmbedMigration:true,allowSelectedResourceMigration:true,allowCoverMigration:true}));
  await db('notes','notes.sqlite',s=>migrateNotes(s,'soty',{allowNativeMigration:true}));
  await db('capabilities','capabilities.sqlite',s=>{
    initializeCapabilitiesSchema(s,{projectId:'soty',allowNativeMigration:true});
    initializeCapabilitiesSchema(s,{projectId:'soty',allowNativeMigration:true,allowOAuthMigration:true});
  });
  await db('human-identity','identity.sqlite',initializeSyntheticRootHumanIdentity);
  await db('connect','accounts.sqlite',s=>migrateDatabase(s,'soty'));
  await db('feedback','feedback.sqlite',initializeSyntheticRootFeedback);
  await db('app-registration','registry.sqlite',initializeSyntheticRootRegistration);
  await db('world','world.sqlite',s=>{migrateWorld(s,'soty');migrateField(s);});
  const canary=new DatabaseSync(path.join(root,'unclassified.sqlite'));
  try{canary.exec('CREATE TABLE unknown_values(id INTEGER PRIMARY KEY,body BLOB,n REAL);CREATE VIRTUAL TABLE unknown_fts USING fts5(body);');
    canary.prepare('INSERT INTO unknown_values VALUES(?,?,?)').run(9007199254740993n,Buffer.from([0,255,13]),-0.125);
    canary.prepare('INSERT INTO unknown_fts(body) VALUES(?)').run('Synthetic canary');}finally{canary.close();}
  // A readOnly SQLite connection may create SHM on a writable fixture. Validate
  // the frozen formats in the stock private snapshot, never on the source.
  const scratch=await mkdtemp(path.join(tmpdir(),'soty-fixture-probe-'));let format;
  try{format=await readStorageFormat(await snapshotStorage(root,path.join(scratch,'state')));}
  finally{await rm(scratch,{recursive:true,force:true});}
  const expected={rooms:2,apps:14,notes:2,capabilities:3,appRegistration:1,feedback:1,humanIdentity:3,connect:4,world:6};
  if(!Object.entries(expected).every(([key,value])=>format[key]===value))throw Error('fixture_formats_incomplete');
  return {ok:true,schema:'soty.root-preservation-fixture.v2',stores:expected,unknownCanary:true};
}
if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href){
  try{
    if(process.platform!=='linux'||process.argv.length!==5||process.argv[2]!=='--initialize-owned-fixture'
      ||!/^\d{1,9}$/u.test(process.argv[3])||!/^\d{1,9}$/u.test(process.argv[4]))throw Error();
    const result=await createSyntheticRootFixture('/data'),uid=Number(process.argv[3]),gid=Number(process.argv[4]);
    const own=async file=>{const stat=await lstat(file);if(stat.isSymbolicLink())throw Error();if(stat.isDirectory())for(const entry of await readdir(file))await own(path.join(file,entry));await chown(file,uid,gid);};
    await own('/data');process.stdout.write(JSON.stringify(result)+'\n');
  }catch{process.stdout.write('{"ok":false,"code":"fixture_initialization_failed"}\n');process.exitCode=1;}
}
