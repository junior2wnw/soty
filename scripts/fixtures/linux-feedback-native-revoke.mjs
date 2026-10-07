import {DatabaseSync} from 'node:sqlite';
import {realpath} from 'node:fs/promises';
// Second owned OS Native writer. No tokens/Root proof/key/command are passed.
// Only this synthetic fixture's exact library session can be revoked.
let bytes=0,parts=[];for await(const part of process.stdin){bytes+=part.length;if(bytes>4000)throw new Error('native_revoke_refused');parts.push(part);}
const value=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(Buffer.concat(parts)));
if(process.platform!=='linux'||process.getuid()!==1000||Object.keys(value).sort().join(',')!=='databasePath,nativeSessionHash'
  ||typeof value.databasePath!=='string'||!/^\/tmp\/soty-ordinary-installed-[A-Za-z0-9]+\/library\.sqlite$/u.test(value.databasePath)
  ||await realpath(value.databasePath)!==value.databasePath||!/^[a-f0-9]{64}$/u.test(value.nativeSessionHash))throw new Error('native_revoke_refused');
const db=new DatabaseSync(value.databasePath);
try{
  db.exec('PRAGMA busy_timeout=5000;BEGIN IMMEDIATE');
  const changed=db.prepare('UPDATE native_sessions SET active=0,generation=generation+1 WHERE id_hash=? AND active=1').run(value.nativeSessionHash);
  if(changed.changes!==1)throw new Error('native_revoke_refused');db.exec('COMMIT');
}finally{if(db.isTransaction)db.exec('ROLLBACK');db.close();}
process.stdout.write('{"nativeRevokeCommitted":true}\n');
