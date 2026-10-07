import {open,lstat,realpath} from 'node:fs/promises';
import {resolve,dirname,join} from 'node:path';
import {fields,check,deepFreeze} from '../server/wire.mjs';
import {parseSourceJson} from '../shared/strict-json.mjs';
import {selectedResourceProfile} from '../../apps/scoped-embed/resource-profile.mjs';
import {STANDARD_SELECTED_SOURCE_V2} from '../server/standard-profile.mjs';

const contexts=new WeakMap();
const utf8=bytes=>new TextDecoder('utf-8',{fatal:true}).decode(bytes);
function privateMode(stat){check(stat.isFile()&&!stat.isSymbolicLink()
  &&(process.platform!=='linux'||stat.uid===process.getuid()&&(stat.mode&0o777)===0o600),'source_install_private_file_denied',503);}
async function privateBytes(path,max){
  const before=await lstat(path);privateMode(before);check(before.size<=max&&await realpath(path)===path,'source_install_private_file_denied',503);
  const file=await open(path,'r');
  try{const current=await file.stat();privateMode(current);check(current.dev===before.dev&&current.ino===before.ino&&current.size<=max,'source_install_private_file_denied',503);
    const bytes=await file.readFile();check(bytes.length<=max,'source_install_private_file_denied',503);return bytes;}finally{await file.close();}
}
const name=value=>typeof value==='string'&&/^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/u.test(value)&&!value.includes('..');

/** Only a trusted CLI/constructor supplies this private operator-file path.
 * Neither an HTTP body nor .soty/author.json is accepted as authorization. */
export async function loadSourceOperatorConfiguration(path){
  check(typeof path==='string'&&resolve(path)===path,'source_install_config_invalid',503);
  const bytes=await privateBytes(path,65536);let cfg;
  try{cfg=parseSourceJson(utf8(bytes),{bytes:65536});}finally{bytes.fill(0);}
  cfg=fields(cfg,['schema','profile','connectorPort','listener','storage','rp','native','policy']);
  check(cfg.schema==='soty.ordinary-source.operator.v1','source_install_config_invalid',503);
  const profile=selectedResourceProfile(cfg.profile);
  check(profile.sourceProfile.id===STANDARD_SELECTED_SOURCE_V2.id&&profile.sourceProfile.version===2
    &&profile.sourceProfile.digest===STANDARD_SELECTED_SOURCE_V2.digest,'source_install_config_invalid',503);
  const listener=fields(cfg.listener,['host','port']);check(listener.host==='127.0.0.1'
    &&Number.isSafeInteger(listener.port)&&listener.port>=1024&&listener.port<=65535,'source_install_config_invalid',503);
  check(Number.isSafeInteger(cfg.connectorPort)&&cfg.connectorPort>=1024&&cfg.connectorPort<=65535&&cfg.connectorPort!==listener.port,'source_install_config_invalid',503);
  const storage=fields(cfg.storage,['directory','keyId','cipherKeyFile']);
  check(typeof storage.directory==='string'&&resolve(storage.directory)===storage.directory&&name(storage.keyId)&&name(storage.cipherKeyFile),'source_install_config_invalid',503);
  const native=fields(cfg.native,['realmId','resourceTitle']);check(typeof native.realmId==='string'&&/^[a-z][a-z0-9.-]{0,63}$/u.test(native.realmId)
    &&typeof native.resourceTitle==='string'&&native.resourceTitle.isWellFormed()&&native.resourceTitle.trim().length>0&&native.resourceTitle.length<=160,'source_install_config_invalid',503);
  const policy=fields(cfg.policy,['newEmptyGuest','linkedLogin']);check(Object.values(policy).every(x=>typeof x==='boolean'),'source_install_config_invalid',503);
  const rp=fields(cfg.rp,['clientSecretFile','transportKeyFile']);check(name(rp.clientSecretFile)&&name(rp.transportKeyFile),'source_install_config_invalid',503);
  check(new Set([storage.cipherKeyFile,rp.clientSecretFile,rp.transportKeyFile]).size===3,'source_install_config_invalid',503);
  const nativeUrl=new URL(profile.nativeOrigin);
  check(profile.nativeOrigin!==profile.embedOrigin&&nativeUrl.hostname!==new URL(profile.embedOrigin).hostname,'source_install_native_origin_invalid',503);
  check(nativeUrl.protocol==='https:'||['localhost','127.0.0.1'].includes(nativeUrl.hostname)
    &&Number(nativeUrl.port)===listener.port,'source_install_native_origin_invalid',503);
  const secretRoot=join(dirname(path),'secrets'),secretDirectory=await lstat(secretRoot);
  check(secretDirectory.isDirectory()&&!secretDirectory.isSymbolicLink()&&await realpath(secretRoot)===secretRoot
    &&(process.platform!=='linux'||secretDirectory.uid===process.getuid()&&(secretDirectory.mode&0o777)===0o700),'source_install_private_file_denied',503);
  const acquired=[];
  async function secret(file,kind){const raw=await privateBytes(join(secretRoot,file),4096);acquired.push(raw);let text;
    try{text=utf8(raw).replace(/\r?\n$/u,'');}catch{check(false,'source_install_secret_invalid',503);}
    check(/^[A-Za-z0-9_-]{43,128}$/u.test(text),'source_install_secret_invalid',503);
    if(kind==='key'){check(text.length===43,'source_install_secret_invalid',503);const key=Buffer.from(text,'base64url');
      check(key.length===32&&key.toString('base64url')===text,'source_install_secret_invalid',503);acquired.push(key);return key;}
    return text;
  }
  try{
    const cipherKey=await secret(storage.cipherKeyFile,'key'),transportKey=await secret(rp.transportKeyFile,'key'),clientSecret=await secret(rp.clientSecretFile,'client');
    const handle=Object.freeze(Object.create(null));
    contexts.set(handle,{config:deepFreeze(cfg),options:{profile:cfg.profile,connectorPort:cfg.connectorPort,listen:listener,
      databasePath:join(storage.directory,'native.sqlite'),realmId:native.realmId,keyId:storage.keyId,cipherKey,transportKey,
      rp:{issuer:profile.issuer,clientId:profile.clientId,clientSecret,redirectUri:profile.embedOrigin+'/api/embed/callback'},
      appLabel:native.resourceTitle,allowEmptyGuest:policy.newEmptyGuest,allowLinkedLogin:policy.linkedLogin},acquired,disposed:false});
    return handle;
  }catch(error){acquired.forEach(buffer=>buffer.fill(0));throw error;}
}
export function withSourceOperatorConfiguration(handle,action){const value=contexts.get(handle);check(value&&!value.disposed&&typeof action==='function','source_install_config_invalid',503);return action(value.options,value.config);}
export function sourceOperatorStatus(handle){return withSourceOperatorConfiguration(handle,(options,cfg)=>({schema:'soty.source-install-status.v1',
  configured:true,appId:options.profile.appId,sourceProfile:STANDARD_SELECTED_SOURCE_V2,authentication:'Basic300',
  nativeOrigin:options.profile.nativeOrigin,nativeReachability:new URL(options.profile.nativeOrigin).protocol==='https:'?'public-https-unverified':'same-machine-loopback',
  listenerLoopback:true,sourceNativePolicy:{newEmptyGuest:cfg.policy.newEmptyGuest,linkedLogin:cfg.policy.linkedLogin},
  jobsReady:false,longReady:false,connected:false}));}
export function disposeSourceOperatorConfiguration(handle){const value=contexts.get(handle);if(!value||value.disposed)return;value.disposed=true;
  value.acquired.forEach(buffer=>buffer.fill(0));value.options.rp.clientSecret='';contexts.delete(handle);}
