import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {randomBytes} from 'node:crypto';
import {mkdtemp,rm,readFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createOrdinaryAppStore} from '../examples/ordinary-app/store.mjs';

test('real SQLite mid-DDL fault rolls back all Native schema; independent reader rejection also occurs before init COMMIT; cold explicit initialize works',async t=>{
  const directory=await mkdtemp(join(tmpdir(),'soty-native-init-fault-'));t.after(()=>rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:30}));
  for(const fault of ['sql','reader']){
    const path=join(directory,fault+'.sqlite'),options={databasePath:path,realmId:'board',key:randomBytes(32),keyId:'synthetic',initialize:true,format:2};
    const original=DatabaseSync.prototype.exec;let injected=false;
    DatabaseSync.prototype.exec=function(sql){
      if(!injected&&sql.includes('CREATE TABLE native_meta')){injected=true;return original.call(this,sql+(fault==='sql'?'; CREATE TABLE native_principals(broken INTEGER);':'; DROP TRIGGER native_link_immutable;'));}
      return original.call(this,sql);
    };
    try{assert.throws(()=>createOrdinaryAppStore(options));}finally{DatabaseSync.prototype.exec=original;}
    assert.equal(injected,true);
    const db=new DatabaseSync(path,{readOnly:true});try{assert.equal(db.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").get().n,0,'failed initialization must leave no accepted/partial authority objects');}finally{db.close();}
    assert.throws(()=>createOrdinaryAppStore({...options,initialize:false}));
    const current=createOrdinaryAppStore(options);assert.equal(current.format,2);assert.equal(current.db.prepare('PRAGMA foreign_key_check').all().length,0);current.close();
  }
});
test('unknown existing authority schema refuses before any mutation/WAL/start even initialize:true',async t=>{
  const directory=await mkdtemp(join(tmpdir(),'soty-native-unknown-'));t.after(()=>rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:30}));
  const path=join(directory,'unknown.sqlite'),db=new DatabaseSync(path);db.exec('CREATE TABLE private_legacy(important TEXT)');db.prepare('INSERT INTO private_legacy VALUES(?)').run('retained');db.close();
  const before=await readFile(path);assert.throws(()=>createOrdinaryAppStore({databasePath:path,realmId:'board',key:randomBytes(32),keyId:'synthetic',initialize:true,format:2}));
  assert.deepEqual(await readFile(path),before);
});
