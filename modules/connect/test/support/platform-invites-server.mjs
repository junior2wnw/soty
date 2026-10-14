import { createServer } from 'node:http';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, extname, sep } from 'node:path';
import QRCode from 'qrcode';
import { createConnectService } from '../../server/index.mjs';
import { createConnectHandler } from '../../server/http.mjs';
const port=Number(process.env.INVITE_FIXTURE_PORT||5197), origin=`http://127.0.0.1:${port}`;
const databasePath=join(mkdtempSync(join(tmpdir(),'soty-invite-ui-')),'accounts.sqlite');
let service=createConnectService({databasePath,projectId:'connect-invitation-fixture',allowedOrigins:[origin]});
let handler=createConnectHandler(service);
const root=resolve('modules/connect');
const server=createServer(async(req,res)=>{
 try {
 if(req.url.startsWith('/fixture/')&&req.method==='POST'){
  let body='';for await(const chunk of req){body+=chunk;if(body.length>512){res.writeHead(413).end();return;}}
  const data=JSON.parse(body);
  if(req.url==='/fixture/admin') {service.close();service=createConnectService({databasePath,projectId:'connect-invitation-fixture',allowedOrigins:[origin],platformAdminAccountIds:[data.accountId]});handler=createConnectHandler(service);res.end('{}');return;}
  if(req.url==='/fixture/qr') {res.setHeader('Content-Type','application/json');res.end(JSON.stringify({src:await QRCode.toDataURL(data.url,{width:224,margin:2})}));return;}
 }
 if(await handler(req,res))return;
 let pathname=new URL(req.url,origin).pathname;
 if(pathname==='/')pathname='/modules/connect/test/support/platform-invites.html';
 const path=resolve('.'+pathname);if(!path.startsWith(root+sep)||!['.mjs','.html','.css'].includes(extname(path))) {res.writeHead(404).end();return;}
 res.setHeader('Content-Type',extname(path)==='.mjs'?'text/javascript':extname(path)==='.css'?'text/css':'text/html');res.end(readFileSync(path));
 } catch {res.writeHead(500).end('fixture failed');}
});
server.listen(port,'127.0.0.1',()=>console.log(JSON.stringify({origin,synthetic:true})));
process.on('SIGINT',()=>{server.close();service.close();process.exit();});
