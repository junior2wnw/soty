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
test('ANY refused startup closes actual DB handle and zeroes copied key: BEGIN/ROLLBACK/reader/WAL faults preserve primary rejection and next writer opens',async t=>{
  const directory=await mkdtemp(join(tmpdir(),'soty-native-startup-lifecycle-'));t.after(()=>rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:30}));
  for(const fault of ['begin','rollback','reader','wal']){
    const path=join(directory,fault+'.sqlite'),originalKey=randomBytes(32),primary=new Error('synthetic_'+fault+'_fault');
    const options={databasePath:path,realmId:'board',key:originalKey,keyId:'synthetic',initialize:true,format:2};
    const exec=DatabaseSync.prototype.exec,prepare=DatabaseSync.prototype.prepare,from=Buffer.from;let capturedDb,keyCopy,rollbackFailed=false;
    Buffer.from=function(value,...args){const result=from.call(Buffer,value,...args);if(value===originalKey)keyCopy=result;return result;};
    DatabaseSync.prototype.exec=function(sql){capturedDb=this;
      if(fault==='begin'&&sql==='BEGIN IMMEDIATE')throw primary;
      if(fault==='rollback'&&sql.includes('CREATE TABLE native_meta')){exec.call(this,sql);throw primary;}
      if(fault==='rollback'&&sql==='ROLLBACK'){rollbackFailed=true;throw new Error('secondary_rollback_fault');}
      if(fault==='wal'&&sql==='PRAGMA journal_mode=WAL;')throw primary;
      return exec.call(this,sql);};
    DatabaseSync.prototype.prepare=function(sql){if(fault==='reader'&&sql==='SELECT * FROM native_meta')throw primary;return prepare.call(this,sql);};
    try{assert.throws(()=>createOrdinaryAppStore(options),error=>error===primary);}
    finally{DatabaseSync.prototype.exec=exec;DatabaseSync.prototype.prepare=prepare;Buffer.from=from;}
    try{assert.ok(keyCopy);assert.equal(keyCopy.every(byte=>byte===0),true);assert.equal(originalKey.some(byte=>byte!==0),true);
      assert.throws(()=>capturedDb.prepare('SELECT 1'),error=>error.code==='ERR_INVALID_STATE');
      if(fault==='rollback')assert.equal(rollbackFailed,true);
    }finally{if(capturedDb?.isOpen)capturedDb.close();keyCopy?.fill(0);}
    const db=new DatabaseSync(path);try{assert.equal(db.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").get().n,fault==='wal'?20:0);db.exec('BEGIN IMMEDIATE;ROLLBACK');}finally{db.close();}
    const current=createOrdinaryAppStore(options);current.close();
  }
});
