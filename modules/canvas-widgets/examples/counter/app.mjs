import {createServer} from 'node:http';
import {DatabaseSync} from 'node:sqlite';
import {readFile,mkdir} from 'node:fs/promises';
import {dirname,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {parseArgs} from 'node:util';

const escape=value=>value.replace(/[&<>"']/gu,char=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
const requestId=value=>typeof value==='string'&&/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/u.test(value);

/** Ordinary application: the shared UI is an explicit build dependency. Root
 * admission controls who reaches this shared counter, not this UI module. */
export async function createGlassCounter({databasePath,glassCss,sourceSdk,widgetContext,parentOrigin,port=0}={}){
 if(typeof databasePath!=='string'||typeof glassCss!=='string'||typeof sourceSdk!=='string'||typeof widgetContext!=='string'||!Number.isSafeInteger(port)||port<0||port>65535)throw Error('counter_configuration_invalid');
 const origin=new URL(parentOrigin);if(origin.origin!==parentOrigin||!['http:','https:'].includes(origin.protocol))throw Error('counter_parent_origin_invalid');
 const [document,script,style]=await Promise.all(['index.html','ui.js','ui.css'].map(name=>readFile(new URL(name,import.meta.url),'utf8')));
 await mkdir(dirname(databasePath),{recursive:true});const db=new DatabaseSync(databasePath);
 db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS state(id INTEGER PRIMARY KEY CHECK(id=1),value INTEGER NOT NULL,revision INTEGER NOT NULL); INSERT OR IGNORE INTO state VALUES(1,0,1); CREATE TABLE IF NOT EXISTS counter_receipts(request_id TEXT PRIMARY KEY,value INTEGER NOT NULL,revision INTEGER NOT NULL)');
 const state=()=>({...db.prepare('SELECT value,revision FROM state WHERE id=1').get()});
 const increment=id=>{
  db.exec('BEGIN IMMEDIATE');try{
   const previous=db.prepare('SELECT value,revision FROM counter_receipts WHERE request_id=?').get(id);
   if(previous){db.exec('COMMIT');return{...state(),receipt:{requestId:id,...previous},replayed:true};}
   const current=state();if(!Number.isSafeInteger(current.value)||!Number.isSafeInteger(current.revision)||current.value>=Number.MAX_SAFE_INTEGER||current.revision>=Number.MAX_SAFE_INTEGER)throw Error('counter_limit');
   db.prepare('UPDATE state SET value=value+1,revision=revision+1 WHERE id=1').run();const next=state();
   db.prepare('INSERT INTO counter_receipts VALUES(?,?,?)').run(id,next.value,next.revision);db.exec('COMMIT');return{...next,receipt:{requestId:id,...next},replayed:false};
  }catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}
 };
 const assets=new Map([
  ['/',{type:'text/html; charset=utf-8',body:document.replace('PARENT_ORIGIN',escape(parentOrigin))}],
  ['/assets/ui.js',{type:'text/javascript; charset=utf-8',body:script}],
  ['/assets/ui.css',{type:'text/css; charset=utf-8',body:glassCss+'\n'+style}],
  ['/assets/source-sdk.js',{type:'text/javascript; charset=utf-8',body:sourceSdk}],
  ['/assets/widget-context.js',{type:'text/javascript; charset=utf-8',body:widgetContext}],
 ]);
 const server=createServer(async(req,res)=>{
  const send=(status,body,type='application/json; charset=utf-8')=>{res.writeHead(status,{'Content-Type':type,'Cache-Control':'no-store','X-Content-Type-Options':'nosniff'});res.end(req.method==='HEAD'?undefined:typeof body==='string'?body:JSON.stringify(body));};
  try{
   const url=new URL(req.url,'http://counter.local');if(url.search)return send(404,{error:'not_found'});
   if(url.pathname==='/api/state'&&req.method==='GET')return send(200,state());
   if(url.pathname==='/api/increment'&&req.method==='POST'){
    if(req.headers['content-type']?.split(';',1)[0].trim().toLowerCase()!=='application/json')return send(415,{error:'counter_json_required'});
    let length=0;const parts=[];for await(const part of req){length+=part.length;if(length>1024)return send(413,{error:'counter_input_limit'});parts.push(part);}
    let input;try{input=JSON.parse(Buffer.concat(parts).toString('utf8'));}catch{return send(400,{error:'counter_input_invalid'});}
    if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).length!==1||!requestId(input.requestId))return send(400,{error:'counter_input_invalid'});
    return send(200,increment(input.requestId));
   }
   const asset=assets.get(url.pathname);if(asset&&['GET','HEAD'].includes(req.method))return send(200,asset.body,asset.type);
   send(404,{error:'not_found'});
  }catch{send(503,{error:'counter_unavailable'});}
 });
 try{await new Promise((yes,no)=>{server.once('error',no);server.listen(port,'127.0.0.1',()=>{server.off('error',no);yes();});});}catch(error){db.close();throw error;}
 let closed=false;
 return{server,port:server.address().port,state,async close(){if(closed)return;closed=true;server.closeAllConnections();await new Promise(done=>server.close(done));db.close();}};
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const{values}=parseArgs({options:{port:{type:'string'},'database-path':{type:'string'},'glass-css':{type:'string'},'source-sdk':{type:'string'},'widget-context':{type:'string'},'parent-origin':{type:'string'}},strict:true});
 const [glassCss,sourceSdk,widgetContext]=await Promise.all([readFile(values['glass-css'],'utf8'),readFile(values['source-sdk'],'utf8'),readFile(values['widget-context'],'utf8')]);
 const app=await createGlassCounter({databasePath:resolve(values['database-path']),glassCss,sourceSdk,widgetContext,parentOrigin:values['parent-origin'],port:Number(values.port??0)});
 process.stdout.write(JSON.stringify({schema:'soty.glass-counter.started.v1',pid:process.pid,port:app.port,state:app.state()})+'\n');
 for(const signal of['SIGINT','SIGTERM'])process.on(signal,()=>{void app.close().then(()=>process.exit(0));});
}
