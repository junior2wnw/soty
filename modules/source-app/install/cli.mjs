#!/usr/bin/env node
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {loadSourceOperatorConfiguration,sourceOperatorStatus,disposeSourceOperatorConfiguration} from './config.mjs';
import {initializeInstalledSource,startInstalledSource,inspectInstalledSource} from './runtime.mjs';
import {renderSourceNativePortal} from './native-portal.mjs';
import {createSourceAuthorDraft} from './author.mjs';
const safeError=error=>typeof error?.code==='string'&&/^[a-z][a-z0-9_]{0,79}$/u.test(error.code)?error.code:'source_install_failed';
export async function sourceInstallMain(argv=process.argv.slice(2)){
  let handle,app;
  try{
    if(argv[0]==='author'&&argv.length===3&&argv[1]==='--title'){
      console.log(JSON.stringify(await createSourceAuthorDraft({directory:process.cwd(),title:argv[2]})));return;
    }
    if(argv.length!==3||argv[1]!=='--config'||!['check','init','serve','reader','native-portal'].includes(argv[0]))throw Error('invalid_cli');
    handle=await loadSourceOperatorConfiguration(resolve(argv[2]));
    if(argv[0]==='check')console.log(JSON.stringify(sourceOperatorStatus(handle)));
    else if(argv[0]==='init')console.log(JSON.stringify(await initializeInstalledSource(handle)));
    else if(argv[0]==='reader')console.log(JSON.stringify(inspectInstalledSource(handle)));
    else if(argv[0]==='native-portal')process.stdout.write(renderSourceNativePortal(handle));
    else{app=await startInstalledSource(handle);console.log(JSON.stringify({schema:'soty.source-start.v1',listening:true,authentication:'Basic300',jobsReady:false,longReady:false,connected:false}));
      let closing=false;const close=async()=>{if(closing)return;closing=true;await app.close();disposeSourceOperatorConfiguration(handle);};
      for(const signal of ['SIGINT','SIGTERM'])process.once(signal,()=>{close().catch(()=>{process.exitCode=1;});});return;}
  }catch(error){console.log(JSON.stringify({ok:false,code:safeError(error)}));process.exitCode=1;}
  finally{if(!app)disposeSourceOperatorConfiguration(handle);}
}
if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url)await sourceInstallMain();
