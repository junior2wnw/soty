import {lstat,realpath,open} from 'node:fs/promises';
import {check} from '../server/wire.mjs';
import {createOrdinaryAppStore} from '../examples/ordinary-app/store.mjs';
import {createOrdinaryAppServer} from '../examples/ordinary-app/index.mjs';
import {readOrdinaryFormat3} from '../examples/ordinary-app/reader3.mjs';
import {withSourceOperatorConfiguration} from './config.mjs';
import {probeInstalledSourceKey} from './key-probe.mjs';

async function directory(path){const stat=await lstat(path);check(stat.isDirectory()&&!stat.isSymbolicLink()&&await realpath(path)===path
  &&(process.platform!=='linux'||stat.uid===process.getuid()&&(stat.mode&0o777)===0o700),'source_install_data_denied',503);}
export async function initializeInstalledSource(handle){return withSourceOperatorConfiguration(handle,async(options,cfg)=>{
  await directory(cfg.storage.directory);
  // Exclusive OS creation, rather than an exists-then-write race. Rejected
  // initialization leaves its exact empty/rolled-back file for operator review.
  let file;try{file=await open(options.databasePath,'wx',0o600);}catch(error){if(error.code==='EEXIST')check(false,'source_install_already_initialized',409);throw error;}
  await file.close();
  const store=createOrdinaryAppStore({databasePath:options.databasePath,realmId:options.realmId,key:options.cipherKey,keyId:options.keyId,initialize:true,format:3});
  try{store.createResource({id:options.profile.resource.selection.nativeId,incarnationId:options.profile.resource.selection.incarnationId,
    title:cfg.native.resourceTitle,guestEmpty:cfg.policy.newEmptyGuest});store.db.exec('PRAGMA wal_checkpoint(TRUNCATE)');}
  finally{store.close();}
  return{schema:'soty.source-install-init.v1',initialized:true,nativeReader:readOrdinaryFormat3(options.databasePath,options.realmId).format,nativePrincipalsCreated:0,nativeGrantsCreated:0};
});}
export async function startInstalledSource(handle){return withSourceOperatorConfiguration(handle,async(options,cfg)=>{
  await directory(cfg.storage.directory);readOrdinaryFormat3(options.databasePath,options.realmId);
  probeInstalledSourceKey({databasePath:options.databasePath,realmId:options.realmId,keyId:options.keyId,cipherKey:options.cipherKey});
  const app=await createOrdinaryAppServer({...options,initialize:false});
  try{await app.listen();return app;}catch(error){await app.close();throw error;}
});}
export function inspectInstalledSource(handle){return withSourceOperatorConfiguration(handle,options=>({schema:'soty.source-reader-status.v1',
  reader:readOrdinaryFormat3(options.databasePath,options.realmId),keyProbe:probeInstalledSourceKey({databasePath:options.databasePath,realmId:options.realmId,keyId:options.keyId,cipherKey:options.cipherKey}),readyToStart:true,connected:false}));}
