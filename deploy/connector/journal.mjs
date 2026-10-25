import {open,rename,unlink} from 'node:fs/promises';
import path from 'node:path';
export async function journal(file,value) {
 const tmp=file+'.tmp-'+process.pid;let fd;
 try{fd=await open(tmp,'wx',0o600);await fd.writeFile(JSON.stringify(value)+'\n');await fd.sync();await fd.close();fd=null;await rename(tmp,file);
  // Windows fixture hosts do not permit directory fsync. Production Unix keeps
  // its durable directory commit; atomic rename/file fsync remain on both.
  if(process.platform!=='win32'){const dir=await open(path.dirname(file),'r');try{await dir.sync();}finally{await dir.close();}}}
 catch(error){if(fd)await fd.close();await unlink(tmp).catch(()=>{});throw error;}
}
// A failed checkpoint is recorded again by recovery in the same phase. It
// must not run its external operation twice or prevent recovery from starting.
export function createReleaseJournal(file,{afterJournal}={}) {
 const visited=new Set();
 return async state=>{
  await journal(file,state);
  if(afterJournal&&!visited.has(state.phase)){
   visited.add(state.phase);await afterJournal(state);
  }
 };
}
