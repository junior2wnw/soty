import {DatabaseSync} from 'node:sqlite';
// Owned synthetic Native writer in a second OS process, never production.
let text='';for await(const part of process.stdin){text+=part;if(text.length>4000)process.exit(78);}
const input=JSON.parse(text),db=new DatabaseSync(input.databasePath);db.exec('PRAGMA busy_timeout=5000;BEGIN IMMEDIATE');
db.prepare('UPDATE native_sessions SET active=0,generation=generation+1 WHERE id_hash=?').run(input.nativeSessionHash);db.exec('COMMIT');db.close();
process.stdout.write('{"nativeRevokeCommitted":true}\n');
