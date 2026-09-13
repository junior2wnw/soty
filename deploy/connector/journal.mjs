import {open,rename,unlink} from 'node:fs/promises';
import path from 'node:path';
export async function journal(file,value) {
 const tmp=file+'.tmp-'+process.pid;let fd;
 try{fd=await open(tmp,'wx',0o600);await fd.writeFile(JSON.stringify(value)+'\n');await fd.sync();await fd.close();fd=null;await rename(tmp,file);const dir=await open(path.dirname(file),'r');try{await dir.sync();}finally{await dir.close();}}
 catch(error){if(fd)await fd.close();await unlink(tmp).catch(()=>{});throw error;}
}
