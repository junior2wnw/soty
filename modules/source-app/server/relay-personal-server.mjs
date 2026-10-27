import {createServer} from 'node:http';
import {DatabaseSync} from 'node:sqlite';
import {existsSync,lstatSync,openSync,closeSync} from 'node:fs';
import {isAbsolute} from 'node:path';
import {randomBytes} from 'node:crypto';
import {createSqliteRelayIdentity} from './sqlite-relay-identity.mjs';
import {PERSONAL_DOCUMENT_DDL,personalPrincipalId,withPersonalDocuments} from './personal-documents.mjs';
import {parseSourceJson} from '../shared/strict-json.mjs';
import {check,fields,digest,jsonCopy,requestId,syncResult} from './wire.mjs';
import {workspaceReceipt} from './workspace-context.mjs';

const SCHEMA='soty.relay-personal.v1';
const DDL=`CREATE TABLE soty_relay_meta(id INTEGER PRIMARY KEY CHECK(id=1),schema TEXT NOT NULL,app_id TEXT NOT NULL,issuer TEXT NOT NULL);
CREATE TABLE soty_workspace_people(id TEXT PRIMARY KEY,issuer TEXT NOT NULL,subject TEXT NOT NULL,incarnation TEXT NOT NULL,active INTEGER NOT NULL CHECK(active IN(0,1)),generation INTEGER NOT NULL,revision INTEGER NOT NULL,UNIQUE(issuer,subject));
${PERSONAL_DOCUMENT_DDL};
CREATE TABLE soty_relay_receipts(principal_id TEXT NOT NULL REFERENCES soty_workspace_people(id),request_id TEXT NOT NULL,input_digest TEXT NOT NULL,result_json TEXT NOT NULL,PRIMARY KEY(principal_id,request_id));`;

/** Experimental trusted-code host for a NEW relay app. The Root signature is
 * checked per request. It is not a sandbox, runner or Source commit fence. */
export function createRelayPersonalServer(options){
  const value=fields(options,['appId','issuer','databasePath','nonceDatabasePath','domain','ui','listen'],['initialize','fetch','clock']);
  const listener=fields(value.listen,['host','port']),domain=fields(value.domain,['personalDocuments','read'],['execute']);
  check(listener.host==='127.0.0.1'&&Number.isSafeInteger(listener.port)&&listener.port>=1024&&listener.port<=65535
    &&typeof value.databasePath==='string'&&isAbsolute(value.databasePath)&&typeof value.nonceDatabasePath==='string'&&isAbsolute(value.nonceDatabasePath)
    &&value.databasePath!==value.nonceDatabasePath&&domain.personalDocuments===true&&typeof domain.read==='function'
    &&(domain.execute===undefined||typeof domain.execute==='function')&&typeof value.ui==='string'&&value.ui.trim()&&Buffer.byteLength(value.ui)<=4194304
    &&(value.initialize===undefined||typeof value.initialize==='boolean')&&(value.fetch===undefined||typeof value.fetch==='function')
    &&(value.clock===undefined||typeof value.clock==='function'),
    'relay_personal_configuration_invalid',503);
  for(const fn of [domain.read,domain.execute].filter(Boolean))check(!['AsyncFunction','AsyncGeneratorFunction'].includes(fn.constructor?.name),'relay_personal_configuration_invalid',503);
  const identity=createSqliteRelayIdentity({issuer:value.issuer,appId:value.appId,databasePath:value.nonceDatabasePath,
    ...(value.fetch?{fetch:value.fetch}:{}),...(value.clock?{clock:value.clock}:{})});
  let database,closed=false,inTransaction=false;
  try{
    if(!existsSync(value.databasePath)){check(value.initialize===true,'relay_personal_not_initialized',503);const file=openSync(value.databasePath,'wx',0o600);closeSync(file);}
    const stat=lstatSync(value.databasePath);check(stat.isFile()&&!stat.isSymbolicLink(),'relay_personal_file_invalid',503);
    database=new DatabaseSync(value.databasePath);database.exec('PRAGMA foreign_keys=ON;PRAGMA busy_timeout=5000');
    const tables=database.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all();
    if(!tables.length){check(value.initialize===true,'relay_personal_not_initialized',503);database.exec('BEGIN IMMEDIATE');
      try{database.exec(DDL);database.prepare('INSERT INTO soty_relay_meta VALUES(1,?,?,?)').run(SCHEMA,value.appId,value.issuer);database.exec('COMMIT');}
      catch(error){if(database.isTransaction)database.exec('ROLLBACK');throw error;}}
    const meta=database.prepare('SELECT * FROM soty_relay_meta').all();
    check(meta.length===1&&meta[0].schema===SCHEMA&&meta[0].app_id===value.appId&&meta[0].issuer===value.issuer
      &&database.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='soty_workspace_documents'").get()?.sql===PERSONAL_DOCUMENT_DDL
      &&database.prepare('PRAGMA foreign_key_check').all().length===0,'relay_personal_schema_mismatch',503);
    database.exec('PRAGMA journal_mode=WAL');
  }catch(error){try{database?.close();}finally{identity.close();}throw error;}
  const namespace=digest([SCHEMA,value.appId,value.issuer]);
  function transaction(action){check(!closed&&!inTransaction,'relay_personal_unavailable',503);inTransaction=true;
    try{database.exec('BEGIN IMMEDIATE');const result=syncResult(action());check(database.isTransaction,'relay_personal_transaction_lost',503);database.exec('COMMIT');return result;}
    catch(error){if(database.isTransaction)database.exec('ROLLBACK');throw error;}finally{inTransaction=false;}}
  function person(identityValue){const principalId=personalPrincipalId(identityValue),row=database.prepare('SELECT id,active FROM soty_workspace_people WHERE issuer=? AND subject=?').get(identityValue.issuer,identityValue.subject);
    if(row)check(row.id===principalId&&row.active===1,'relay_personal_access_denied',403);
    else database.prepare('INSERT INTO soty_workspace_people VALUES(?,?,?,?,1,1,1)').run(principalId,identityValue.issuer,identityValue.subject,randomBytes(24).toString('base64url'));
    return principalId;}
  function respond(res,status,body){const text=JSON.stringify(body);check(Buffer.byteLength(text)<=65536,'relay_personal_response_limit',503);
    res.writeHead(status,{'content-type':'application/json; charset=utf-8','cache-control':'no-store'});res.end(text);}
  async function body(req){const parts=[];let bytes=0;for await(const part of req){bytes+=part.length;check(bytes<=65536,'source_app_payload_limit',413);parts.push(part);}
    return parseSourceJson(new TextDecoder('utf-8',{fatal:true}).decode(Buffer.concat(parts)),{bytes:65536});}
  const server=createServer(async(req,res)=>{try{
    if((req.url==='/'||req.url==='/embed')&&['HEAD','GET'].includes(req.method)){
      res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store'});res.end(req.method==='HEAD'?undefined:value.ui);return;
    }
    check(req.method==='POST'&&['/api/query','/api/invoke','/api/receipt'].includes(req.url),'relay_personal_not_found',404);
    const input=await body(req),args=fields(input,['requestId',req.url==='/api/receipt'?'inputDigest':'input']);
    check(requestId(args.requestId),'source_app_input_invalid');
    if(req.url==='/api/receipt')check(typeof args.inputDigest==='string'&&/^[a-f0-9]{64}$/u.test(args.inputDigest),'source_app_input_invalid');
    else check(args.input&&typeof args.input==='object'&&!Array.isArray(args.input),'source_app_input_invalid');
    const verified=await identity.verify(req);
    const result=transaction(()=>{const principalId=person(verified),resourceId='personal:'+principalId;
      if(req.url==='/api/receipt'){
        const prior=database.prepare('SELECT input_digest,result_json FROM soty_relay_receipts WHERE principal_id=? AND request_id=?').get(principalId,args.requestId);
        check(!prior||prior.input_digest===args.inputDigest,'source_workspace_request_conflict',409);
        return prior?JSON.parse(prior.result_json):{requestId:args.requestId,inputDigest:args.inputDigest,outcome:'not_applied'};
      }
      const execute=req.url==='/api/invoke',inputDigest=digest(args.input);
      if(execute){check(domain.execute,'relay_personal_read_only',403);
        const prior=database.prepare('SELECT input_digest,result_json FROM soty_relay_receipts WHERE principal_id=? AND request_id=?').get(principalId,args.requestId);
        check(!prior||prior.input_digest===inputDigest,'source_workspace_request_conflict',409);if(prior)return JSON.parse(prior.result_json);}
      const data=withPersonalDocuments({database,namespace,principalId,resourceId,method:execute?'execute':'read',inTransaction:()=>inTransaction},
        current=>domain[execute?'execute':'read'](current,args));
      if(!execute){const copied=jsonCopy(data);fields(copied,[],Object.keys(copied??{}));check(!Object.hasOwn(copied,'requestId'),'source_workspace_domain_invalid',503);
        return{requestId:args.requestId,...copied};}
      const value=fields(syncResult(data),['workspaceReceipt'],['result','data']);
      workspaceReceipt(value.workspaceReceipt);
      const original=jsonCopy({requestId:args.requestId,inputDigest,outcome:'committed',...value});
      // A durable receipt must remain readable through the same bounded wire
      // format after an interrupted response; do not commit an oversized ACK.
      parseSourceJson(JSON.stringify({ok:true,data:original}),{bytes:65536});
      database.prepare('INSERT INTO soty_relay_receipts VALUES(?,?,?,?)').run(principalId,args.requestId,inputDigest,JSON.stringify(original));return original;
    });
    respond(res,200,{ok:true,data:result});
  }catch(error){if(!res.headersSent&&!res.destroyed){const status=Number.isSafeInteger(error?.status)?error.status:503;
    respond(res,status,{ok:false,error:{code:typeof error?.code==='string'&&/^[a-z0-9_]{1,128}$/u.test(error.code)?error.code:'relay_personal_unavailable'}});}else res.destroy();}});
  return Object.freeze({server,async listen(){check(!closed,'relay_personal_unavailable',503);await new Promise((resolve,reject)=>{
    server.once('error',reject);server.listen(listener.port,listener.host,()=>{server.off('error',reject);resolve();});});return listener.port;},
    async close(){if(closed)return;closed=true;server.closeAllConnections();if(server.listening)await new Promise(done=>server.close(done));try{database.close();}finally{identity.close();}}});
}
