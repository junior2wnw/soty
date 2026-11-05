import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {join} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createServer} from 'node:http';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {randomBytes,createHash} from 'node:crypto';
import {pathToFileURL} from 'node:url';
import {fixture} from './fixture.mjs';

const requestId=()=>crypto.randomUUID(),connect=client=>client.connect({requestId:requestId()});
const read=client=>client.query({requestId:requestId(),input:{operation:'reading.list'}});

test('portable frozen Native consumer: signed HTTP action → personal SQLite → replay → cold restart, no stale actor access',async t=>{
 const f=await fixture();t.after(()=>f.close());const alice=f.browser();await connect(alice.client);
 const initial=await read(alice.client);assert.deepEqual(initial.items,[]);
 const unsigned=await fetch(f.upstreamOrigin+'/api/embed/query',{method:'POST',headers:{'content-type':'application/json'},
  body:JSON.stringify({requestId:requestId(),input:{operation:'reading.list'}})});
 assert.equal(unsigned.status,403,'a direct listener and author metadata cannot create Root authority');
 const input={operation:'reading.add',title:'Read a real book'},intent=await alice.client.prepareInvoke({requestId:requestId(),input});
 const receipt=await alice.client.invoke(intent);assert.equal(receipt.outcome,'committed');
 assert.equal(receipt.workspaceReceipt.receipt.verificationMethod,'domain_read');
 assert.deepEqual((await read(alice.client)).items,[{id:intent.requestId,title:input.title}]);
 const connection=new DatabaseSync(f.databasePath,{readOnly:true});
 assert.equal(connection.prepare('SELECT count(*) AS n FROM soty_workspace_documents').get().n,1);
 assert.equal(connection.prepare('SELECT count(*) AS n FROM soty_workspace_receipts').get().n,1);connection.close();
 assert.equal((await alice.client.invoke(intent)).outcome,'committed');
 f.frame('bob');const bob=f.browser();bob.cookie=alice.cookie;
 await assert.rejects(()=>read(bob.client));await assert.rejects(()=>connect(bob.client));
 bob.cookie='';await connect(bob.client);assert.deepEqual((await read(bob.client)).items,[]);
 f.frame('alice');await f.restart();const reopened=f.browser();reopened.cookie=alice.cookie;await connect(reopened.client);
 const recovered=await reopened.client.recoverReceipt({requestId:intent.requestId,contextRef:intent.contextRef,inputDigest:intent.inputDigest});
 assert.equal(recovered.outcome,'committed');assert.equal(recovered.recovery.contextRef,intent.contextRef);
 assert.deepEqual((await read(reopened.client)).items,[{id:intent.requestId,title:input.title}]);
 f.host().setIdentityAccess({issuer:f.profile.issuer,subject:'alice'},false);
 await assert.rejects(()=>read(reopened.client));f.closeRoot();await assert.rejects(()=>connect(reopened.client));
 const provenance=JSON.parse(await readFile(join(f.project,'consumer-provenance.json')));
 assert.equal(provenance.sdk.version,'0.1.0-preview.21');assert.equal(provenance.grantsCreated,0);
 assert.equal(provenance.sdk.archiveSha256,'84cb887a4eddb01934465a2be6b8d58a378f7f9e63d42d1d406ac8c3f3438e65');
 const helper=await readFile(new URL('../../../../../vendor/optical-glass/src/widget-context.js',import.meta.url));
 assert.equal(provenance.glassWidgetContextSha256,createHash('sha256').update(helper).digest('hex'));
 assert.deepEqual((await readFile(join(f.project,'source/assets/ui.js'))).subarray(0,helper.length),helper);
 assert.deepEqual(await readFile(join(f.project,'source/domain.mjs')),await readFile(join(f.sdkDirectory,'dist/templates/reading-list/domain.mjs')));
});

test('factory preserves every supported ui override and SDK fixed route boundary',async t=>{
 const values={document:'<title>Author supplied</title>',script:'/* author script */',style:'/* author stylesheet */',sdk:'/* frozen SDK override */'};
 const f=await fixture({ui:values});t.after(()=>f.close());
 for(const[path,key]of [['/embed','document'],['/assets/ui.js','script'],['/assets/ui.css','style'],['/assets/source-sdk.js','sdk']]){
  const response=await fetch(f.embedOrigin+path);assert.equal(response.status,200);assert.equal(await response.text(),values[key]);
 }
 for(const path of ['/embed?other=1','/operator.private.json','/source/domain.mjs','/other-widget-entry','/assets/widget-context.js'])assert.equal((await fetch(f.embedOrigin+path)).status,404);
});

test('a lost reply after Native COMMIT recovers the same receipt after restart without another mutation',async t=>{
 const f=await fixture();t.after(()=>f.close());const first=f.browser();await connect(first.client);
 const intent=await first.client.prepareInvoke({requestId:requestId(),input:{operation:'reading.add',title:'Committed before reply was lost'}});
 first.loseNextInvokeResponse();await assert.rejects(()=>first.client.invoke(intent),error=>error.effectUnknown===true);
 await f.restart();const reopened=f.browser();reopened.cookie=first.cookie;await connect(reopened.client);
 const receipt=await reopened.client.recoverReceipt({requestId:intent.requestId,contextRef:intent.contextRef,inputDigest:intent.inputDigest});
 assert.equal(receipt.outcome,'committed');assert.deepEqual((await read(reopened.client)).items,[{id:intent.requestId,title:'Committed before reply was lost'}]);
 const connection=new DatabaseSync(f.databasePath,{readOnly:true});
 assert.equal(connection.prepare('SELECT count(*) AS n FROM soty_workspace_documents').get().n,1);
 assert.equal(connection.prepare('SELECT count(*) AS n FROM soty_workspace_receipts').get().n,1);connection.close();
});

test('prepared project runs the frozen installer check/init and installed server lifecycle with its own assets',async t=>{
 const f=await fixture();t.after(()=>f.close());
 const reservation=createServer();await new Promise(done=>reservation.listen(0,'127.0.0.1',done));
 const port=reservation.address().port;await new Promise(done=>reservation.close(done));
 const {digest:_derived,...profile}=f.profile;
 // External HTTPS gateway origin: no claim that this internal HTTP asset smoke
 // proves TLS or Root admission. HTTP proxy-origin != listener is refused by CLI.
 profile.embedOrigin=profile.nativeOrigin='https://reading-widget.localhost';
 const secrets=join(f.project,'secrets');await mkdir(secrets,{mode:0o700});
 for(const name of ['cipher.key','transport.key'])await writeFile(join(secrets,name),randomBytes(32).toString('base64url'),{mode:0o600,flag:'wx'});
 const config={schema:'soty.root-workspace.operator.v1',profile,connectorPort:new URL(f.parentOrigin).port*1,
  listener:{host:'127.0.0.1',port},storage:{directory:join(f.directory,'installed-state'),keyId:'test-source-key',cipherKeyFile:'cipher.key'},
  transportKeyFile:'transport.key',native:{resourceTitle:'Список чтения',domainModule:join(f.project,'source/domain.mjs'),uiDirectory:join(f.project,'source/assets')},policy:{personalWorkspaces:true}};
 await mkdir(config.storage.directory,{mode:0o700});
 const configPath=join(f.project,'operator.private.json');await writeFile(configPath,JSON.stringify(config),{mode:0o600,flag:'wx'});
 const cli=join(f.sdkDirectory,'dist/install.mjs'),run=promisify(execFile);
 const checked=await run(process.execPath,[cli,'check','--config',configPath],{cwd:f.project,windowsHide:true});
 assert.equal(JSON.parse(checked.stdout).connected,false);
 const initialized=await run(process.execPath,[cli,'init','--config',configPath],{cwd:f.project,windowsHide:true});
 assert.equal(JSON.parse(initialized.stdout).initialized,true);
 await assert.rejects(()=>run(process.execPath,[cli,'init','--config',configPath],{cwd:f.project,windowsHide:true}),'reinit cannot erase existing Source data');
 const install=await import(pathToFileURL(cli).href),handle=await install.loadSourceOperatorConfiguration(configPath);
 let app;try{app=await install.startInstalledSource(handle);
  for(const[path,file]of [['/embed','embed.html'],['/assets/ui.js','ui.js'],['/assets/ui.css','ui.css']]){
   const result=await fetch('http://127.0.0.1:'+port+path);assert.equal(result.status,200);
   assert.deepEqual(Buffer.from(await result.arrayBuffer()),await readFile(join(f.project,'source/assets',file)));
  }
 }finally{await app?.close();install.disposeSourceOperatorConfiguration(handle);}
});
