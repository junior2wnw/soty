import {readFile,mkdir} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {join} from 'node:path';
import {createRelayPersonalServer} from '../.soty-sdk/dist/server.mjs';
import {createDomain} from '../.soty-sdk/dist/templates/reading-list/domain.mjs';

const root=new URL('../',import.meta.url);
const config=JSON.parse(await readFile(new URL('.soty/app-auth.json',root),'utf8'));
const descriptor=JSON.parse(await readFile(new URL('.soty/app.json',root),'utf8'));
if(config.schema!=='soty.app-auth-configuration.v1'||!/^app-[a-f0-9]{32}$/u.test(config.appId)
  ||typeof config.issuer!=='string'||new URL(config.issuer).pathname!=='/human-identity'){
  throw Object.assign(Error('Configure the registered appId and trusted issuer; do not add a password fallback.'),{code:'soty_app_auth_configuration_required'});
}
const data=fileURLToPath(new URL('data/',root));await mkdir(data,{mode:0o700,recursive:true});
const app=createRelayPersonalServer({appId:config.appId,issuer:config.issuer,databasePath:join(data,'app.sqlite'),
  nonceDatabasePath:join(data,'auth-nonces.sqlite'),domain:createDomain(),initialize:process.argv.includes('--initialize'),
  ui:await readFile(new URL('./ui.html',import.meta.url),'utf8'),
  browserSdk:await readFile(new URL('../.soty-sdk/dist/browser.mjs',import.meta.url),'utf8'),
  listen:{host:'127.0.0.1',port:descriptor.port}});
await app.listen();
console.log(JSON.stringify({listening:true,port:descriptor.port,authentication:'soty-window',embedded:true}));
let closing=false;
for(const signal of ['SIGINT','SIGTERM'])process.once(signal,()=>{if(!closing){closing=true;void app.close();}});
