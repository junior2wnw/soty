#!/usr/bin/env node
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {loadSourceOperatorConfiguration,sourceOperatorStatus,disposeSourceOperatorConfiguration} from './config.mjs';
import {initializeInstalledSource,startInstalledSource,inspectInstalledSource,diagnoseInstalledSource} from './runtime.mjs';
import {renderSourceNativePortal} from './native-portal.mjs';
import {createSourceAuthorDraft} from './author.mjs';
const safeError=error=>typeof error?.code==='string'&&/^[a-z][a-z0-9_]{0,79}$/u.test(error.code)?error.code:'source_install_failed';
export const SOURCE_INSTALL_HELP=Object.freeze({schema:'soty.source-cli-help.v1',commands:[
  'author --title TITLE [--template reading-list]','check --config PRIVATE_FILE','init --config PRIVATE_FILE',
  'serve --config PRIVATE_FILE','reader --config PRIVATE_FILE','doctor --config PRIVATE_FILE','native-portal --config PRIVATE_FILE'],
  modes:{'soty.ordinary-source.operator.v1':'Native/OIDC Basic300','soty.root-workspace.operator.v1':'Root signed workspace Basic300'},
  readiness:'check verifies configuration; reader verifies local database/key; doctor verifies only a historical server observation, never current authority',
  publication:'Use existing soty-app-release init/plan/verify and approved Root source prepare/promote; no automatic publication'});
export async function sourceInstallMain(argv=process.argv.slice(2)){
  let handle,app;
  try{
    if(argv.length===0||argv.length===1&&['--help','help'].includes(argv[0])){console.log(JSON.stringify(SOURCE_INSTALL_HELP));return;}
    if(argv[0]==='author'&&[3,5].includes(argv.length)&&argv[1]==='--title'&&(argv.length===3||argv[3]==='--template'&&argv[4]==='reading-list')){
      console.log(JSON.stringify(await createSourceAuthorDraft({directory:process.cwd(),title:argv[2],...(argv.length===5?{template:argv[4]}:{})})));return;
    }
    if(argv.length!==3||argv[1]!=='--config'||!['check','init','serve','reader','doctor','native-portal'].includes(argv[0]))throw Error('invalid_cli');
    handle=await loadSourceOperatorConfiguration(resolve(argv[2]));
    if(argv[0]==='check')console.log(JSON.stringify(sourceOperatorStatus(handle)));
    else if(argv[0]==='init')console.log(JSON.stringify(await initializeInstalledSource(handle)));
    else if(argv[0]==='reader')console.log(JSON.stringify(await inspectInstalledSource(handle)));
    else if(argv[0]==='doctor')console.log(JSON.stringify(await diagnoseInstalledSource(handle)));
    else if(argv[0]==='native-portal')process.stdout.write(renderSourceNativePortal(handle));
    else{app=await startInstalledSource(handle);console.log(JSON.stringify({schema:'soty.source-start.v1',listening:true,authentication:sourceOperatorStatus(handle).authentication,jobsReady:false,longReady:false,connected:false}));
      let closing=false;const close=async()=>{if(closing)return;closing=true;await app.close();disposeSourceOperatorConfiguration(handle);};
      for(const signal of ['SIGINT','SIGTERM'])process.once(signal,()=>{close().catch(()=>{process.exitCode=1;});});return;}
  }catch(error){console.log(JSON.stringify({ok:false,code:safeError(error)}));process.exitCode=1;}
  finally{if(!app)disposeSourceOperatorConfiguration(handle);}
}
if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url)await sourceInstallMain();
