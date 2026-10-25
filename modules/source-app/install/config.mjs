import {open,lstat,realpath} from 'node:fs/promises';
import {resolve,dirname,join} from 'node:path';
import {fields,check,deepFreeze} from '../server/wire.mjs';
import {parseSourceJson} from '../shared/strict-json.mjs';
import {selectedResourceProfile} from '../../apps/scoped-embed/resource-profile.mjs';
import {STANDARD_SELECTED_SOURCE_V2,isRootSignedWorkspaceSource} from '../server/standard-profile.mjs';

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
  const workspace=cfg.schema==='soty.root-workspace.operator.v1';
  cfg=fields(cfg,['schema','profile','connectorPort','listener','storage','native','policy',workspace?'transportKeyFile':'rp']);
  check(workspace||cfg.schema==='soty.ordinary-source.operator.v1','source_install_config_invalid',503);
  const profile=selectedResourceProfile(cfg.profile);
  check(workspace?isRootSignedWorkspaceSource(profile.sourceProfile):profile.sourceProfile.id===STANDARD_SELECTED_SOURCE_V2.id&&profile.sourceProfile.version===2
    &&profile.sourceProfile.digest===STANDARD_SELECTED_SOURCE_V2.digest,'source_install_config_invalid',503);
  const listener=fields(cfg.listener,['host','port']);check(listener.host==='127.0.0.1'
    &&Number.isSafeInteger(listener.port)&&listener.port>=1024&&listener.port<=65535,'source_install_config_invalid',503);
  check(Number.isSafeInteger(cfg.connectorPort)&&cfg.connectorPort>=1024&&cfg.connectorPort<=65535&&cfg.connectorPort!==listener.port,'source_install_config_invalid',503);
  const storage=fields(cfg.storage,['directory','keyId','cipherKeyFile']);
  check(typeof storage.directory==='string'&&resolve(storage.directory)===storage.directory&&name(storage.keyId)&&name(storage.cipherKeyFile),'source_install_config_invalid',503);
  const native=fields(cfg.native,workspace?['resourceTitle','domainModule','uiDirectory']:['realmId','resourceTitle']);
  check(typeof native.resourceTitle==='string'&&native.resourceTitle.isWellFormed()&&native.resourceTitle.trim().length>0&&native.resourceTitle.length<=160,'source_install_config_invalid',503);
  if(workspace)check(typeof native.domainModule==='string'&&resolve(native.domainModule)===native.domainModule&&native.domainModule.endsWith('.mjs')
    &&typeof native.uiDirectory==='string'&&resolve(native.uiDirectory)===native.uiDirectory,'source_install_config_invalid',503);
  else check(typeof native.realmId==='string'&&/^[a-z][a-z0-9.-]{0,63}$/u.test(native.realmId),'source_install_config_invalid',503);
  const policy=fields(cfg.policy,workspace?['personalWorkspaces']:['newEmptyGuest','linkedLogin']);
  check(Object.values(policy).every(x=>typeof x==='boolean'),'source_install_config_invalid',503);
  const rp=workspace?{transportKeyFile:cfg.transportKeyFile}:fields(cfg.rp,['clientSecretFile','transportKeyFile']);
  check(name(rp.transportKeyFile)&&(workspace||name(rp.clientSecretFile)),'source_install_config_invalid',503);
  const keyFiles=[storage.cipherKeyFile,rp.transportKeyFile,...(workspace?[]:[rp.clientSecretFile])];
  check(new Set(keyFiles).size===keyFiles.length,'source_install_config_invalid',503);
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
    const cipherKey=await secret(storage.cipherKeyFile,'key'),transportKey=await secret(rp.transportKeyFile,'key');
    const clientSecret=workspace?undefined:await secret(rp.clientSecretFile,'client');
    const handle=Object.freeze(Object.create(null));
    contexts.set(handle,{config:deepFreeze(cfg),options:{profile:cfg.profile,connectorPort:cfg.connectorPort,listen:listener,
      databasePath:join(storage.directory,'native.sqlite'),keyId:storage.keyId,cipherKey,transportKey,appLabel:native.resourceTitle,
      ...(workspace?{domainModule:native.domainModule,uiDirectory:native.uiDirectory,personalWorkspaces:policy.personalWorkspaces}:
        {realmId:native.realmId,rp:{issuer:profile.issuer,clientId:profile.clientId,clientSecret,redirectUri:profile.embedOrigin+'/api/embed/callback'},
          allowEmptyGuest:policy.newEmptyGuest,allowLinkedLogin:policy.linkedLogin})},acquired,disposed:false});
    return handle;
  }catch(error){acquired.forEach(buffer=>buffer.fill(0));throw error;}
}
export function withSourceOperatorConfiguration(handle,action){const value=contexts.get(handle);check(value&&!value.disposed&&typeof action==='function','source_install_config_invalid',503);return action(value.options,value.config);}
export function sourceOperatorStatus(handle){return withSourceOperatorConfiguration(handle,(options,cfg)=>({schema:'soty.source-install-status.v1',
  configured:true,appId:options.profile.appId,sourceProfile:options.profile.sourceProfile,authentication:cfg.schema==='soty.root-workspace.operator.v1'?'RootSSO300':'Basic300',
  nativeOrigin:options.profile.nativeOrigin,nativeReachability:new URL(options.profile.nativeOrigin).protocol==='https:'?'public-https-unverified':'same-machine-loopback',
  listenerLoopback:true,sourceNativePolicy:cfg.schema==='soty.root-workspace.operator.v1'?{personalWorkspaces:cfg.policy.personalWorkspaces}:
    {newEmptyGuest:cfg.policy.newEmptyGuest,linkedLogin:cfg.policy.linkedLogin},
  jobsReady:false,longReady:false,connected:false}));}
export function disposeSourceOperatorConfiguration(handle){const value=contexts.get(handle);if(!value||value.disposed)return;value.disposed=true;
  value.acquired.forEach(buffer=>buffer.fill(0));if(value.options.rp)value.options.rp.clientSecret='';contexts.delete(handle);}
