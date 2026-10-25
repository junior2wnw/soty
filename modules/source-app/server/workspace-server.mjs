import {createServer} from 'node:http';
import {createSourceAppBff} from './bff.mjs';
import {selectedResourceProfile} from '../../apps/scoped-embed/resource-profile.mjs';
import {fields,check} from './wire.mjs';

/** Thin Node HTTP shell over the existing BFF. Only these three public assets
 * are served; paths, imports and private operator files never come from HTTP. */
export function createRootWorkspaceServer(options) {
  const value=fields(options,['profile','transportKey','connectorPort','host','ui'],['listen','clock','onVerifiedOperation']);
  const profile=selectedResourceProfile(value.profile),ui=fields(value.ui,['document','script','style','sdk']);
  check(value.host?.profile?.digest===profile.digest,'source_workspace_host_profile_mismatch',503);
  const listener=fields(value.listen??{host:'127.0.0.1',port:63347},['host','port']);
  check(listener.host==='127.0.0.1'&&Number.isSafeInteger(listener.port)&&listener.port>=1024&&listener.port<=65535,'source_workspace_listener_invalid',503);
  const bytes=input=>{check(typeof input==='string'||input instanceof Uint8Array,'source_workspace_ui_invalid',503);const result=Buffer.from(input);
    check(result.length>0&&result.length<=4194304,'source_workspace_ui_invalid',503);return result;};
  const document=bytes(ui.document),assets=new Map([
    ['/assets/ui.js',{bytes:bytes(ui.script),type:'text/javascript; charset=utf-8'}],
    ['/assets/ui.css',{bytes:bytes(ui.style),type:'text/css; charset=utf-8'}],
    ['/assets/source-sdk.js',{bytes:bytes(ui.sdk),type:'text/javascript; charset=utf-8'}],
  ]);
  const bff=createSourceAppBff({profile:value.profile,transportKey:value.transportKey,connectorPort:value.connectorPort,
    storage:value.host.storage,native:value.host.native,...(value.clock?{clock:value.clock}:{}),
    ...(value.onVerifiedOperation?{onVerifiedOperation:value.onVerifiedOperation}:{})});
  const server=createServer(async(req,res)=>{try{
    if(await bff.handleRequest(req,res))return;
    const url=new URL(req.url??'/',profile.embedOrigin);
    if(!['GET','HEAD'].includes(req.method)||url.search||url.hash){res.writeHead(404).end();return;}
    if(url.pathname==='/embed'){
      res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store','referrer-policy':'no-referrer',
        'x-content-type-options':'nosniff','content-security-policy':"default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors "+profile.parentOrigin});
      res.end(req.method==='HEAD'?undefined:document);return;
    }
    const asset=assets.get(url.pathname);if(!asset){res.writeHead(404).end();return;}
    res.writeHead(200,{'content-type':asset.type,'cache-control':'no-store','x-content-type-options':'nosniff'});
    res.end(req.method==='HEAD'?undefined:asset.bytes);
  }catch{if(!res.headersSent)res.writeHead(503,{'content-type':'application/json'}).end('{"ok":false,"error":{"code":"source_workspace_unavailable"}}');else res.destroy();}});
  let closed=false;
  return Object.freeze({server,bff,host:value.host,
    async listen(){check(!closed,'source_workspace_server_closed',503);
      await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(listener.port,listener.host,()=>{server.off('error',reject);resolve();});});return listener.port;},
    async close(){if(closed)return;closed=true;server.closeAllConnections();if(server.listening)await new Promise(resolve=>server.close(resolve));bff.close();value.host.close();},
  });
}
