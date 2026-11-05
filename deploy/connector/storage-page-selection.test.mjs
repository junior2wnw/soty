import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {createHash} from 'node:crypto';
import {mkdtemp,mkdir,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,basename,resolve} from 'node:path';
import {initializeCanvasStorage} from '../../modules/canvas-objects/storage.mjs';
import {initializeCanvasPagesStorage} from '../../modules/canvas-pages/storage.mjs';
import {initializeCanvasStorage as oldObjects} from '../../modules/canvas-objects/test-fixtures/storage-v1.mjs';
import {initializeCanvasPagesStorage as oldPages} from '../../modules/canvas-pages/test-fixtures/storage-v1.mjs';
import {readStorageFormat} from './storage-probe.mjs';
import {assertStorageCompatible,currentStorageReaders,storageReaderLabel,storageReaders} from './storage-guard.mjs';
import {snapshotStorage} from './storage-snapshot.mjs';

// Literal reviewed pins: never derived by importing the probe's pin registry.
const families=[
 {kind:'canvasObjects',file:'objects.sqlite',open:initializeCanvasStorage,old:oldObjects,count:8,digest:'1d4aa16fdc36844c37acd0499bc50eecd9f1593734652559bad1d6da0219aecb',guard:'canvas_object_revision'},
 {kind:'canvasPages',file:'pages.sqlite',open:initializeCanvasPagesStorage,old:oldPages,count:9,digest:'7ff460bd4a0521de7ce742314438a20816938641d2cc5d1337566e1476ef5085',guard:'canvas_page_immutable'},
];
const image=label=>({Id:'sha256:'+'b'.repeat(64),Config:{Labels:{[storageReaderLabel]:typeof label==='string'?label:JSON.stringify(label)}}});
async function directory(t){const root=await mkdtemp(join(tmpdir(),'soty-page-policy-storage-'));t.after(()=>{assert.equal(dirname(resolve(root)),resolve(tmpdir()));assert.ok(basename(root).startsWith('soty-page-policy-storage-'));return rm(root,{recursive:true,force:true});});return root;}
function layout(db){return db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all().map(r=>[r.type,r.name,r.tbl_name,r.sql.replace(/\s+/gu,' ').trim()]);}

test('Object2/Page2 have frozen reviewed layouts; read-only host probe, offline snapshot and historical reader labels enforce the floor',async t=>{
 const root=await directory(t);await mkdir(join(root,'canvas'));
 for(const f of families){const db=new DatabaseSync(join(root,'canvas',f.file));try{assert.equal(f.open(db,'soty'),1);assert.equal(f.open(db,'soty',{allowFormat2Migration:true}),2);const rows=layout(db);assert.equal(rows.length,f.count);assert.equal(createHash('sha256').update(JSON.stringify(rows)).digest('hex'),f.digest);assert.throws(()=>f.old(db,'soty'));assert.equal(f.open(db,'soty'),2);}finally{db.close();}}
 const files=families.map(f=>join(root,'canvas',f.file)),before=await Promise.all(files.map(f=>readFile(f))),observed=await readStorageFormat(root);assert.equal(observed.canvasObjects,2);assert.equal(observed.canvasPages,2);assert.deepEqual(await Promise.all(files.map(f=>readFile(f))),before);assertStorageCompatible(image(currentStorageReaders),observed);
 for(const f of families){const old=JSON.parse(currentStorageReaders);old.readers[f.kind]=[1];assert.throws(()=>assertStorageCompatible(image(old),observed),{code:'storage_reader_incompatible'},'an old label cannot acquire format2 by copying a container label');}
 const snapshotParent=await directory(t),copy=join(snapshotParent,'snapshot');await snapshotStorage(root,copy);assert.deepEqual(await readStorageFormat(copy),observed);
 const docker=await readFile(new URL('../../Dockerfile',import.meta.url),'utf8'),label=docker.match(/^LABEL io\.soty\.storage\.readers="(.+)"$/mu)?.[1];assert.ok(label);assert.deepEqual(storageReaders(image(label.replaceAll('\\"','"'))).canvasObjects,[1,2]);assert.deepEqual(storageReaders(image(label.replaceAll('\\"','"'))).canvasPages,[1,2]);
});

for(const family of families)for(const [name,mutate,code]of[
 ['future',db=>db.exec('PRAGMA user_version=3'),'storage_format_unknown'],
 ['extra table',db=>db.exec('CREATE TABLE unreviewed_canvas_copy(content TEXT)'),'storage_format_unreadable'],
 ['missing guard',db=>db.exec('DROP TRIGGER '+family.guard),'storage_format_unreadable'],
])test(`${family.kind}2 probe fails closed on ${name}`,async t=>{const root=await directory(t);await mkdir(join(root,'canvas'));const db=new DatabaseSync(join(root,'canvas',family.file));try{family.open(db,'soty',{allowFormat2Migration:true});assert.equal((await readStorageFormat(root))[family.kind],2);mutate(db);await assert.rejects(readStorageFormat(root),{code});}finally{db.close();}});
