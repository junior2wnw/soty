import {lstat,realpath,open,readFile} from 'node:fs/promises';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {check} from '../server/wire.mjs';
import {createOrdinaryAppStore} from '../examples/ordinary-app/store.mjs';
import {createOrdinaryAppServer} from '../examples/ordinary-app/index.mjs';
import {readOrdinaryFormat3} from '../examples/ordinary-app/reader3.mjs';
import {withSourceOperatorConfiguration} from './config.mjs';
import {probeInstalledSourceKey} from './key-probe.mjs';
import {createSqliteRootWorkspaceHost} from '../server/sqlite-root-workspace-host.mjs';
import {createRootWorkspaceServer} from '../server/workspace-server.mjs';
import {digest,syncResult} from '../server/wire.mjs';
import {createSourceReadinessObserver} from '../server/operation-observer.mjs';

const workspace=cfg=>cfg.schema==='soty.root-workspace.operator.v1';
async function publicFile(path){const stat=await lstat(path);check(stat.isFile()&&!stat.isSymbolicLink()&&await realpath(path)===path
  &&stat.size>0&&stat.size<=4194304,'source_install_public_file_denied',503);return readFile(path);}
async function workspaceHost(options,initialize=false){
  // This is operator-selected CODE on disk, not a module URL from a request,
  // public author manifest or model-supplied identity. Pin its bytes per load.
  const bytes=await publicFile(options.domainModule),module=await import(pathToFileURL(options.domainModule).href+'?sha256='+digest(bytes.toString('base64url')));
  check(typeof module.createDomain==='function'&&module.createDomain.constructor?.name!=='AsyncFunction','source_install_domain_invalid',503);
  return createSqliteRootWorkspaceHost({profile:options.profile,databasePath:options.databasePath,cipherKey:options.cipherKey,keyId:options.keyId,
    personalWorkspaces:options.personalWorkspaces,title:options.appLabel,domain:syncResult(module.createDomain()),initialize});
}

async function directory(path){const stat=await lstat(path);check(stat.isDirectory()&&!stat.isSymbolicLink()&&await realpath(path)===path
  &&(process.platform!=='linux'||stat.uid===process.getuid()&&(stat.mode&0o777)===0o700),'source_install_data_denied',503);}
export async function initializeInstalledSource(handle){return withSourceOperatorConfiguration(handle,async(options,cfg)=>{
  await directory(cfg.storage.directory);
  // Exclusive OS creation, rather than an exists-then-write race. Rejected
  // initialization leaves its exact empty/rolled-back file for operator review.
  let file;try{file=await open(options.databasePath,'wx',0o600);}catch(error){if(error.code==='EEXIST')check(false,'source_install_already_initialized',409);throw error;}
  await file.close();
  if(workspace(cfg)){const host=await workspaceHost(options,true);try{return{schema:'soty.source-install-init.v1',initialized:true,
    nativeReader:host.inspect().schema,nativePrincipalsCreated:0,nativeGrantsCreated:0,connected:false};}finally{host.close();}}
  const store=createOrdinaryAppStore({databasePath:options.databasePath,realmId:options.realmId,key:options.cipherKey,keyId:options.keyId,initialize:true,format:3});
  try{store.createResource({id:options.profile.resource.selection.nativeId,incarnationId:options.profile.resource.selection.incarnationId,
    title:cfg.native.resourceTitle,guestEmpty:cfg.policy.newEmptyGuest});store.db.exec('PRAGMA wal_checkpoint(TRUNCATE)');}
  finally{store.close();}
  return{schema:'soty.source-install-init.v1',initialized:true,nativeReader:readOrdinaryFormat3(options.databasePath,options.realmId).format,nativePrincipalsCreated:0,nativeGrantsCreated:0};
});}
export async function startInstalledSource(handle){return withSourceOperatorConfiguration(handle,async(options,cfg)=>{
  await directory(cfg.storage.directory);
  if(workspace(cfg)){
    const host=await workspaceHost(options),observer=createSourceReadinessObserver({directory:cfg.storage.directory,profile:options.profile,key:options.transportKey});let app;
    try{const publicDirectory=await lstat(options.uiDirectory);check(publicDirectory.isDirectory()&&!publicDirectory.isSymbolicLink()
      &&await realpath(options.uiDirectory)===options.uiDirectory,'source_install_public_file_denied',503);
      const bundled=typeof __SOTY_SOURCE_APP_BUNDLE__!=='undefined'&&__SOTY_SOURCE_APP_BUNDLE__===true;
      const [document,script,style,sdk]=await Promise.all([publicFile(join(options.uiDirectory,'embed.html')),publicFile(join(options.uiDirectory,'ui.js')),
        publicFile(join(options.uiDirectory,'ui.css')),readFile(new URL(bundled?'./browser.mjs':'../dist/browser.mjs',import.meta.url))]);
      app=createRootWorkspaceServer({profile:options.profile,transportKey:options.transportKey,connectorPort:options.connectorPort,host,
        ui:{document,script,style,sdk},listen:options.listen,onVerifiedOperation:observer.observe});await app.listen();
      return Object.freeze({...app,async close(){try{await app.close();}finally{observer.close();}}});
    }catch(error){try{if(app)await app.close();else host.close();}finally{observer.close();}throw error;}
  }
  readOrdinaryFormat3(options.databasePath,options.realmId);
  probeInstalledSourceKey({databasePath:options.databasePath,realmId:options.realmId,keyId:options.keyId,cipherKey:options.cipherKey});
  const app=await createOrdinaryAppServer({...options,initialize:false});
  try{await app.listen();return app;}catch(error){await app.close();throw error;}
});}
export function inspectInstalledSource(handle){return withSourceOperatorConfiguration(handle,(options,cfg)=>workspace(cfg)?workspaceHost(options).then(host=>{
  try{return{schema:'soty.source-reader-status.v1',reader:host.inspect(),readyToStart:true,connected:false,currentAuthority:'not_checked'};}finally{host.close();}
}):({schema:'soty.source-reader-status.v1',
  reader:readOrdinaryFormat3(options.databasePath,options.realmId),keyProbe:probeInstalledSourceKey({databasePath:options.databasePath,realmId:options.realmId,keyId:options.keyId,cipherKey:options.cipherKey}),readyToStart:true,connected:false}));}

export async function diagnoseInstalledSource(handle){return withSourceOperatorConfiguration(handle,async(options,cfg)=>{
  const reader=await inspectInstalledSource(handle);
  if(!workspace(cfg))return{schema:'soty.source-doctor.v1',reader,basis:'local_database',integrity:'not_instrumented',lastObservation:null,
    currentAuthority:'not_checked',clientAcknowledgement:'not_observed',connected:false,releaseReady:false};
  const observer=createSourceReadinessObserver({directory:cfg.storage.directory,profile:options.profile,key:options.transportKey});
  try{return{...observer.inspect(),reader};}finally{observer.close();}
});}
