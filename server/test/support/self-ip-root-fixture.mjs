import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { networkInterfaces } from 'node:os';
import { join } from 'node:path';
import WebSocket, { WebSocketServer } from 'ws';
import { nativeHttpFixture, nativeIdentity, good } from './native-capability-http.mjs';
import { createHttpApp } from '../../http-app.js';
import { createLocalAppsRuntime } from '../../../scripts/agent-modules/local-apps.mjs';
import { selfIPv4 } from '../../../modules/apps/server/endpoint-targets.mjs';

const sha = value => createHash('sha256').update(value).digest('hex');
const ownIp = () => {
  const values = Object.values(networkInterfaces()).flat().filter(value => value && value.family === 'IPv4' && !value.internal);
  const address = values.find(value => { try { return selfIPv4(value.address); } catch { return false; } })?.address;
  assert.ok(address, 'R09 actual self-IP gate requires an OS-assigned IPv4; absence is not a passing/skip result'); return address;
};
export const until = async (read, label = 'self-IP fixture') => {
  const deadline = Date.now() + 10000;
  for (;;) { const value = await read(); if (value) return value; assert.ok(Date.now() < deadline, label); await new Promise(done => setTimeout(done, 10)); }
};
export async function markerPair(t) {
  const address = ownIp(), seen = [], accepted = [], peers = new Set(), held = [];
  let holdHead = false, holdGet = false;
  const handler = marker => (req, res) => {
    seen.push({ marker, method: req.method, path: req.url, host: req.headers.host });
    const reply = () => {
      if (req.url === '/redirect') res.writeHead(302, { location: `http://127.0.0.1:${self.address().port}/identity` }).end();
      else res.writeHead(200, { 'content-type': 'text/plain' }).end(req.url==='/hold'?'SECRET_HELD_AFTER_LOCK':marker);
    };
    if ((holdHead && req.method==='HEAD' || holdGet && req.url==='/hold') && marker==='SELF_IP_ONLY') held.push(reply); else reply();
  };
  const self = createServer(handler('SELF_IP_ONLY')), loop = createServer(handler('LOOPBACK_ONLY'));
  const wss = new WebSocketServer({ noServer: true });
  for (const [server, marker] of [[self, 'SELF_IP_WS'], [loop, 'LOOPBACK_WS']]) {
    server.on('connection', socket => { accepted.push({marker,address:socket.localAddress});peers.add(socket); socket.once('close', () => peers.delete(socket)); });
    server.on('upgrade', (req, socket, head) => wss.handleUpgrade(req, socket, head, peer => {
      peer.on('error', () => {}); peer.send(marker); peer.on('message', bytes => peer.send(`${marker}:${bytes}`));
    }));
  }
  await new Promise(done => self.listen(0, address, done));
  await new Promise((done, reject) => { loop.once('error', reject); loop.listen(self.address().port, '127.0.0.1', done); });
  t.after(async () => { for (const finish of held) finish(); for (const socket of peers) socket.destroy(); for (const peer of wss.clients) peer.terminate(); wss.close(); await Promise.all([self,loop].map(server => new Promise(done => server.close(done)))); });
  return { address, port: self.address().port, seen, accepted, hold() { holdHead = true; }, holdGet(){holdGet=true;}, release() { holdHead = false; holdGet=false; for (const finish of held.splice(0)) finish(); } };
}
export function rawHttp(port, path, { host, cookie, origin, method='GET', body }={}) {
  return new Promise((resolve, reject) => {
    const req = request({ hostname:'127.0.0.1', port, path, method, headers:{...(host?{host}:{}),...(cookie?{cookie}:{}),...(origin?{origin}:{}),...(body?{'content-type':'application/json'}:{})} }, res => {
      const chunks=[];res.on('data',chunk=>chunks.push(chunk));res.on('error',reject);res.on('end',()=>resolve({status:res.statusCode,headers:res.headers,text:Buffer.concat(chunks).toString()}));
    });req.on('error',reject);req.end(body?JSON.stringify(body):undefined);
  });
}
export async function rootSelfIpFixture(t, { brokenDial=false, legacyConnector=false, oldRoot=false }={}) {
  let runtime; t.after(()=>runtime?.stop());
  const source=await markerPair(t);
  const factory=(dist,options)=>createHttpApp(dist,{...options,allowSelfIpTargets:!oldRoot,allowAudienceMigration:true,
    appOriginTemplate:`http://{appId}.legacy.localhost:${new URL(options.connectOrigins[0]).port}`,
    namedAppZone:`http://apps.localhost:${new URL(options.connectOrigins[0]).port}`});
  const f=await nativeHttpFixture(t,{appOrigins:true,appFactory:factory});
  f.server.on('upgrade',(req,socket,head)=>{if(!f.app?.locals.appsService.handleUpgrade(req,socket,head))socket.destroy();});
  const owner=nativeIdentity('Self-IP owner'),other=nativeIdentity('Self-IP outsider'),account=await f.bootstrap(owner);await f.bootstrap(other);
  const identity={linkId:'self_ip_root_link_012345678901234567890',hostDeviceId:'self_ip_host',connectorId:'self_ip_connector'},token=randomBytes(32).toString('base64url'),wire=[];
  good((await f.http('/api/connectors/register',{method:'POST',token,body:JSON.stringify({linkId:identity.linkId,deviceId:identity.hostDeviceId,connectorId:identity.connectorId,scope:'Dev',protocol:2,capabilities:['apps']})})).body);
  let visibleAddresses=networkInterfaces();
  runtime=createLocalAppsRuntime({randomSecret:()=>randomBytes(32).toString('base64url'),digest:sha,
    createWebSocket:url=>{const ws=new WebSocket(url);ws.on('message',bytes=>wire.push(JSON.parse(bytes)));return ws;},
    httpRequest:options=>request(brokenDial?{...options,hostname:'127.0.0.1'}:options),
    encodeBase64:bytes=>Buffer.from(bytes).toString('base64'),decodeBase64:bytes=>Buffer.from(bytes,'base64'),
    ...(!legacyConnector?{fetch,networkInterfaces:()=>visibleAddresses}:{})},{serverUrl:f.origin,identity,token});
  runtime.start();await until(()=>runtime.status().connected,'actual connector authenticated');
  const claim=await runtime.claim();good(await f.call(owner,'apps.claim',{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,claimCode:claim.claimCode}));
  const call=(op,args={})=>f.call(owner,op,{expectedAccountId:account.accountId,...args});
  const inventory=async()=>until(async()=>{const listed=good(await call('apps.devices'));return listed.devices.find(value=>value.connectorId===identity.connectorId&&value.sourceEndpoints?.state==='ready');},'actual connector inventory');
  const args=async(extra={})=>({requestId:randomBytes(18).toString('hex'),hostDeviceId:identity.hostDeviceId,connectorId:identity.connectorId,name:'Self-IP marker app',
    source:{schema:'soty.app-http-source.v2',endpoint:{scope:'connector-self',scheme:'http',family:'IPv4',address:source.address},port:source.port,entryPath:'/identity'},
    grants:{accountIds:[],communityIds:[]},...(!legacyConnector&&!oldRoot?{inventoryWitness:(await inventory()).sourceEndpoints.inventoryWitness}:{}),...extra});
  async function session(appId,extra={}) {
    await until(async()=>good(await call('apps.inspect',{appId})).source.binding.state==='bound','real target binding');
    const launched=good(await call('apps.launch',{appId,...extra})),url=new URL(launched.launchUrl),response=await rawHttp(f.server.address().port,'/_soty/session',{host:url.host,origin:url.origin,method:'POST',body:{ticket:url.hash.slice(1)}});
    assert.equal(response.status,200,response.text);return {host:url.host,origin:url.origin,cookie:response.headers['set-cookie'][0].split(';')[0]};
  }
  return {...f,get app(){return f.app;},source,owner,other,account,identity,runtime,wire,ownerCall:call,args,inventory,session,
    removeInterfaces(){visibleAddresses={};},restoreInterfaces(){visibleAddresses=networkInterfaces();}};
}
