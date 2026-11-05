import {createHash} from 'node:crypto';
import {lstat,readFile,realpath,mkdtemp,rm} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {snapshotStorage} from './storage-snapshot.mjs';
import {readStorageFormat} from './storage-probe.mjs';
import {assertStorageReaderManifestCompatible} from './storage-guard.mjs';

export const readerBridgeReaders='{"version":11,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6,7,8,9,10,11,12,13,14],"notes":[1,2],"capabilities":[1,2,3],"appRegistration":[1],"feedback":[1],"humanIdentity":[1,2,3],"connect":[3,4,5],"world":[6,7],"canvasObjects":[1,2],"canvasWidgets":[1],"canvasVault":[1,2],"canvasPages":[1,2],"materials":[1,2],"invitations":[1,2],"referrals":[1],"appAgentDirectory":[1],"universalPublication":[1],"appsEndpoint":[],"appsFrameContinuity":[],"monitoring":[1]}}';
export const readerBridgePolicy=Object.freeze({
  schema:'soty.reader-bridge.build.v1',
  baseImage:'sha256:c5821dd11eb84b5467ff3c6826716dfc30b30a4ecdcff91a9266327825d47588',
  baseRevision:'330afe853dd8b40c13faa4d7256828b583d4ee3d',
  nodeVersion:'24.15.0',nodeSha256:'d1de76d8edf2fededf6f8b30d244e2c0529ac607923a018283b77e9c74bd932c',
  originalEntryBytes:2309,originalEntrySha256:'96306e5395873d4a618c29bd0567d4beca76801f4f3245eae51b0fb243517ca3',
  originalEntryName:'index.reader-bridge-original.js',snapshotMaxBytes:192*1024*1024,
});
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const requireThat=(ok,code)=>{if(!ok)throw Object.assign(new Error(code),{code});};
const overlaps=(a,b)=>a===b||a.startsWith(b+path.sep)||b.startsWith(a+path.sep);

export function assertReaderBridgeRuntime(version,binary){
  requireThat(version===readerBridgePolicy.nodeVersion&&sha(binary)===readerBridgePolicy.nodeSha256,'reader_bridge_runtime_invalid');
}
export function assertReaderBridgeOriginal(bytes){
  requireThat(bytes.length===readerBridgePolicy.originalEntryBytes&&sha(bytes)===readerBridgePolicy.originalEntrySha256,'reader_bridge_original_invalid');
}

// Library paths allow isolated synthetic fixtures. Production main below fixes
// /data, /tmp and the adjacent pinned original; there are no runtime overrides.
export async function runReaderBridge({dataDir,temporaryParent,loadOriginal}){
  requireThat(typeof loadOriginal==='function','reader_bridge_loader_invalid');
  const source=await realpath(dataDir),parent=await realpath(temporaryParent);
  for(const [given,resolved]of[[dataDir,source],[temporaryParent,parent]]){
    const state=await lstat(given);
    requireThat(state.isDirectory()&&!state.isSymbolicLink()&&path.resolve(given)===resolved,'reader_bridge_directory_invalid');
  }
  requireThat(!overlaps(source,parent),'reader_bridge_directory_invalid');
  const owned=await mkdtemp(path.join(parent,'soty-reader-bridge-'));
  const initial=await lstat(owned),resolvedOwned=await realpath(owned);
  requireThat(resolvedOwned===owned&&path.dirname(owned)===parent&&initial.isDirectory()&&!initial.isSymbolicLink()
    &&(process.platform==='win32'||(initial.mode&0o777)===0o700),'reader_bridge_directory_invalid');
  let format;
  try{
    const snapshot=await snapshotStorage(source,path.join(owned,'snapshot'),readerBridgePolicy.snapshotMaxBytes);
    format=assertStorageReaderManifestCompatible(readerBridgeReaders,await readStorageFormat(snapshot));
    requireThat(format.schema==='soty.storage-format.v10','storage_probe_invalid');
  }finally{
    // Only the fresh private directory created above can be removed. Cleanup
    // failure is fatal: the original application never starts with a leaked copy.
    const current=await lstat(owned);
    requireThat(current.isDirectory()&&!current.isSymbolicLink()&&current.dev===initial.dev&&current.ino===initial.ino
      &&await realpath(owned)===resolvedOwned&&path.dirname(owned)===parent,'reader_bridge_cleanup_invalid');
    await rm(owned,{recursive:true,force:false});
  }
  await loadOriginal();
  return format;
}

export async function startReaderBridge(){
  requireThat(process.env.DATA_DIR==='/data','storage_data_directory_unsupported');
  assertReaderBridgeRuntime(process.versions.node,await readFile(process.execPath));
  const original=new URL('./'+readerBridgePolicy.originalEntryName,import.meta.url);
  const verifyOriginal=async()=>{
    const state=await lstat(original);
    requireThat(state.isFile()&&!state.isSymbolicLink()&&state.nlink===1,'reader_bridge_original_invalid');
    assertReaderBridgeOriginal(await readFile(original));
  };
  await verifyOriginal();
  return runReaderBridge({dataDir:'/data',temporaryParent:'/tmp',loadOriginal:async()=>{
    await verifyOriginal();
    await import(original.href);
  }});
}

if(process.argv[1]&&pathToFileURL(path.resolve(process.argv[1])).href===import.meta.url){
  try{await startReaderBridge();}
  catch(error){
    const code=['storage_data_directory_unsupported','storage_format_unknown','storage_format_unreadable','storage_reader_incompatible',
      'storage_probe_invalid','reader_bridge_runtime_invalid','reader_bridge_original_invalid','reader_bridge_directory_invalid',
      'reader_bridge_cleanup_invalid'].includes(error?.code)?error.code:'reader_bridge_failed';
    process.stderr.write(JSON.stringify({ok:false,code})+'\n');process.exitCode=1;
  }
}
