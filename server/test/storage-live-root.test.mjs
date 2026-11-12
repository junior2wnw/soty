import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {join,dirname,basename,resolve} from 'node:path';
import {mkdtemp,rm,writeFile,mkdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {readStorageFormat} from '../../deploy/connector/storage-probe.mjs';
import {assertStorageCompatible,currentStorageReaders,storageReaderLabel,storageReaders} from '../../deploy/connector/storage-guard.mjs';
import {snapshotStorage} from '../../deploy/connector/storage-snapshot.mjs';
import {migrateAppsSchema} from '../../modules/apps/server/schema.mjs';
import {rootSelfIpFixture} from './support/self-ip-root-fixture.mjs';
import {good} from './support/native-capability-http.mjs';

const image=readers=>({Id:'sha256:'+'a'.repeat(64),Config:{Labels:{[storageReaderLabel]:readers}}});
const featurePaths={canvasObjects:'canvas/objects.sqlite',canvasWidgets:'canvas/widgets.sqlite',canvasVault:'canvas/vault.sqlite',canvasPages:'canvas/pages.sqlite',materials:'materials/materials.sqlite',invitations:'invitations/invitations.sqlite',referrals:'referrals/referrals.sqlite',appAgentDirectory:'app-agent-directory/metadata.sqlite',universalPublication:'app-registration/publication-journal.sqlite'};

test('actual Root new stores/endpoint journal have exact content-free reader formats; old image refuses and offline snapshot retains every feature',{timeout:20000},async t=>{
 const f=await rootSelfIpFixture(t);good(await f.ownerCall('apps.registerEndpoint',await f.args()));
 const observed=await readStorageFormat(f.directory);assert.equal(observed.schema,'soty.storage-format.v10');
 for(const key of Object.keys(featurePaths))assert.equal(observed[key],key==='invitations'?2:1,key);assert.equal(observed.appsEndpoint,1);
 assertStorageCompatible(image(currentStorageReaders),observed);
 const old=JSON.parse(currentStorageReaders);old.version=7;old.readers.notes=[1,2];old.readers.feedback=[1];old.readers.world=[6,7];for(const key of [...Object.keys(featurePaths),'appsEndpoint','appsFrameContinuity','monitoring'])delete old.readers[key];
 assert.doesNotThrow(()=>storageReaders(image(JSON.stringify(old))),'the historical V7 fixture must remain a recognized image before its incompatible data is tested');
 assert.throws(()=>assertStorageCompatible(image(JSON.stringify(old)),observed),{code:'storage_reader_incompatible'});
 assert.equal(JSON.stringify(observed).includes(f.account.accountId),false);assert.equal(JSON.stringify(observed).includes(f.source.address),false);
 f.runtime.stop();await f.app.locals.closeServices();const snapshotParent=await mkdtemp(join(tmpdir(),'soty-platform-snapshot-'));t.after(()=>{assert.equal(dirname(resolve(snapshotParent)),resolve(tmpdir()));assert.ok(basename(snapshotParent).startsWith('soty-platform-snapshot-'));return rm(snapshotParent,{recursive:true,force:true});});const destination=join(snapshotParent,'snapshot');
 await snapshotStorage(f.directory,destination);assert.deepEqual(await readStorageFormat(destination),observed,'snapshot must not silently omit newer module directories');
 for(const [key,relative]of Object.entries(featurePaths)){
  const db=new DatabaseSync(join(f.directory,relative));try{const version=db.prepare('PRAGMA user_version').get().user_version;db.exec('PRAGMA user_version=99');
    await assert.rejects(readStorageFormat(f.directory),/storage_format_unknown|storage_format_unreadable/,key);db.exec(`PRAGMA user_version=${version}`);
    db.exec('CREATE TABLE probe_unapproved_extra(secret TEXT)');await assert.rejects(readStorageFormat(f.directory),/storage_format_unreadable/,key);db.exec('DROP TABLE probe_unapproved_extra');
  }finally{db.close();}
 }
 assert.deepEqual(await readStorageFormat(f.directory),observed);
 await writeFile(join(f.directory,'canvas','unapproved.sqlite'),'not a recognized store');await assert.rejects(readStorageFormat(f.directory),/storage_format_unreadable/);
});

test('all nine ordinary/selected audience/cover endpoint schema families are recognized independently',async t=>{
 const root=await mkdtemp(join(tmpdir(),'soty-probe-endpoint-'));t.after(()=>{assert.equal(dirname(resolve(root)),resolve(tmpdir()));assert.ok(basename(root).startsWith('soty-probe-endpoint-'));return rm(root,{recursive:true,force:true});});
 for(let version=6;version<=14;version++){
  const folder=join(root,String(version));await mkdir(join(folder,'apps'),{recursive:true});const db=new DatabaseSync(join(folder,'apps','registry.sqlite'));
  try{const base=version>=12?version-6:version>=9?version-3:version;migrateAppsSchema(db,{allowSelfIpTargets:true,allowScopedEmbedMigration:base>=7,allowSelectedResourceMigration:base>=8,allowAudienceMigration:version>=9,allowCoverMigration:version>=12});
    const observed=await readStorageFormat(folder);assert.equal(observed.apps,version);assert.equal(observed.appsEndpoint,1);
  }finally{db.close();}
 }
});
