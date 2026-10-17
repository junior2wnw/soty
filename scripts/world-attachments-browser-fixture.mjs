// OWN localhost product acceptance only. Real signed Connect, World SQLite and
// an actual SDK encrypted restore run before the local attachment writer starts.
import { createServer as httpServer } from 'node:http';
import { createServer as viteServer } from 'vite';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { DatabaseSync } from 'node:sqlite';
import { createConnectService } from '../modules/connect/server/index.mjs';
import { createConnectHandler } from '../modules/connect/server/http.mjs';
import { createWorldService } from '../modules/world/server/index.mjs';
import { migrateChatAttachments } from '../modules/world/server/attachments.mjs';
import { qualifyOwnAttachmentRestore } from '../modules/world/test/support/attachment-restore.mjs';
const projectId='m02-browser-own',root=await mkdtemp(join(tmpdir(),'soty-m02-browser-own-')),seed=join(root,'seed');
await mkdir(join(seed,'world'),{recursive:true});await mkdir(join(seed,'rooms'));
const worldPath=join(seed,'world/world.sqlite'),connectPath=join(seed,'connector-store.sqlite');
let world=createWorldService({databasePath:worldPath,projectId}),connect=createConnectService({databasePath:connectPath,projectId,allowedOrigins:['http://127.0.0.1'],extensions:[world]});
connect.close();world.close();const db=new DatabaseSync(worldPath);migrateChatAttachments(db,projectId);db.close();
const restore=await qualifyOwnAttachmentRestore(seed,join(root,'migration-restore'));
const read=createWorldService({databasePath:join(restore.directory,'world/world.sqlite'),projectId});if(read.schemaVersion!==4)throw Error('own_migration_restore_failed');read.close();
let handler;const server=httpServer(async(req,res)=>{
  if(req.url==='/health'&&req.method==='GET'){
    const checked=new DatabaseSync(worldPath,{readOnly:true});let ok=false;
    try{ok=checked.prepare('PRAGMA quick_check').get().quick_check==='ok';}finally{checked.close();}
    res.setHeader('Content-Type','application/json');res.setHeader('Cache-Control','no-store');res.end(JSON.stringify({ok}));return;
  }
  if(!handler||!await handler(req,res)){res.statusCode=404;res.end();}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const vite=await viteServer({cacheDir:join(root,'vite-cache'),logLevel:'error',server:{host:'127.0.0.1',port:0,
  watch:{ignored:['**/.playwright-cli/**','**/output/**']},
  proxy:{'/health':`http://127.0.0.1:${server.address().port}`,'/api/connect/rpc':`http://127.0.0.1:${server.address().port}`}}});await vite.listen();
const origin=`http://127.0.0.1:${vite.httpServer.address().port}`;
world=createWorldService({databasePath:worldPath,projectId,attachmentWrites:true});
connect=createConnectService({databasePath:connectPath,projectId,allowedOrigins:[origin],extensions:[world]});handler=createConnectHandler(connect);
console.log(JSON.stringify({schema:'soty.m02.browser-own-ready.v1',mode:'own-local-browser',url:origin+'/src/world/attachment-fixture.html',origin,schemaVersion:4,
  ownMigrationEncryptedRestore:restore.receipt,production:false}));
let stopping=false;
async function stop(){if(stopping)return;stopping=true;await vite.close();await new Promise(r=>server.close(r));connect.close();world.close();
  if(dirname(root)!==resolve(tmpdir())||!/^soty-m02-browser-own-/u.test(root.split(/[\\/]/u).at(-1)))throw Error('own_cleanup_refused');await rm(root,{recursive:true,force:true});process.exit(0);}
process.once('SIGINT',()=>{void stop();});process.once('SIGTERM',()=>{void stop();});
const input=createInterface({input:process.stdin});input.on('line',line=>{if(line.trim()==='quit')void stop();});
