import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createNotesService } from '../server/index.mjs';

const actor={accountId:'change_owner',deviceId:'change_device'};
const args={expectedAccountId:actor.accountId,noteId:'change_note',mutationId:'change_once',expectedRevision:0,title:'Note',body:'Real content',items:[],color:'plain',pinned:false,state:'active'};
function fixture(t,onChange){
  const parent=tmpdir(),directory=mkdtempSync(join(parent,'soty-note-change-')),databasePath=join(directory,'notes.sqlite');
  const notes=createNotesService({databasePath,projectId:'change-test',onChange});
  t.after(()=>{notes.close();assert.equal(dirname(directory),parent);assert.ok(directory.startsWith(join(parent,'soty-note-change-')));rmSync(directory,{recursive:true,force:true});});
  return {notes,databasePath,put:extra=>notes.execute({actor,op:'notes.put',args:{...args,...extra}})};
}
test('note change is observable only after durable COMMIT; replay/read/failed CAS do not emit',t=>{
  const changes=[];let path;
  const f=fixture(t,event=>{const db=new DatabaseSync(path,{readOnly:true});try{assert.equal(db.prepare('SELECT revision FROM notes WHERE id=?').get(event.noteId).revision,event.revision);}finally{db.close();}changes.push(event);});path=f.databasePath;
  f.put();assert.deepEqual(changes,[{accountId:actor.accountId,noteId:args.noteId,revision:1}]);
  assert.equal(f.put().replayed,true);
  f.notes.execute({actor,op:'notes.get',args:{expectedAccountId:actor.accountId,noteId:args.noteId}});
  assert.throws(()=>f.put({mutationId:'stale_mutation'}),/notes_revision_conflict/u);
  assert.equal(changes.length,1);
  f.put({mutationId:'trash_once',expectedRevision:1,state:'trashed'});
  f.notes.execute({actor,op:'notes.purge',args:{expectedAccountId:actor.accountId,noteId:args.noteId,mutationId:'purge_once',expectedRevision:2}});
  assert.deepEqual(changes.map(value=>value.revision),[1,2,3]);
});
test('a real deferred foreign key COMMIT failure produces neither a note nor a live change',t=>{
  const changes=[],f=fixture(t,event=>changes.push(event)),db=new DatabaseSync(f.databasePath);
  try{
    db.exec('CREATE TABLE change_parent(id INTEGER PRIMARY KEY); CREATE TABLE change_child(parent INTEGER REFERENCES change_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER change_fail AFTER INSERT ON notes BEGIN INSERT INTO change_child VALUES(1); END;');
    assert.throws(()=>f.put(),/FOREIGN KEY/u);
    assert.equal(db.prepare('SELECT count(*) n FROM notes').get().n,0);assert.equal(changes.length,0);
    db.exec('DROP TRIGGER change_fail');f.put();assert.equal(changes.length,1);
  }finally{db.close();}
});
test('a failed live listener cannot turn a committed note into a failed mutation',t=>{
  const f=fixture(t,()=>{throw new Error('listener_failed');});assert.equal(f.put().revision,1);assert.equal(f.put().replayed,true);
});
