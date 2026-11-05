// Standalone bounded developer runner, not auto-executed with module tests.
import {mkdtemp,mkdir,readFile,writeFile,cp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,resolve,basename} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
const repo=resolve(dirname(fileURLToPath(import.meta.url)),'../../..'),output=resolve(process.argv[2]??'');
if(!process.argv[2])throw new Error('explicit output directory required');
await mkdir(output,{recursive:false});const root=await mkdtemp(join(tmpdir(),'soty-monitoring-mutants-')),results=[];
const hash=value=>createHash('sha256').update(value).digest('hex');
const env=Object.fromEntries(['SystemRoot','TEMP','TMP','PATH','USERPROFILE','LOCALAPPDATA'].filter(key=>process.env[key]!==undefined).map(key=>[key,process.env[key]]));
try{
 for(const [name,anchor,replacement,target]of[
  ['disabled-ignored','if(!cfg.enabled){','if(false&&!cfg.enabled){','service'],
  ['missing-zero','value:!samples?null:','value:!samples?0:','service'],
  ['unweighted-mean',"def.kind==='distribution'?total/samples:last.last","def.kind==='distribution'?local.reduce((sum,row)=>sum+row.sum/row.samples,0)/local.length:last.last",'service'],
  ['early-admin-ack','const result=adminReceipt(action,request);',"const result={state:'committed',revision:args.expectedRevision+1};",'admin'],
  ['flush-boundary-omitted','sync(beforeCollectionChange(target));','void target;','host'],
 ]){
  const dir=join(root,name);await cp(join(repo,'modules','monitoring'),dir,{recursive:true});const file=join(dir,'server','index.mjs'),source=await readFile(file,'utf8');if(source.split(anchor).length!==2)throw new Error('exact mutation anchor drift: '+name);await writeFile(file,source.replace(anchor,replacement));
  let testFile=join(dir,'test',target+'.test.mjs'),childEnv=env;
  if(target==='host'){const host=join(repo,'server','monitoring-module.js');let code=await readFile(host,'utf8');code=code.replace("from '../modules/monitoring/server/index.mjs'",`from '${pathToFileURL(file).href}'`).replace(/from '([^']+)'/gu,(whole,specifier)=>specifier.startsWith('.')?`from '${new URL(specifier,pathToFileURL(host)).href}'`:whole);const bridge=join(dir,'host-bridge.mjs');await writeFile(bridge,code);testFile=join(repo,'modules','monitoring','test','host.test.mjs');childEnv={...env,SOTY_MONITORING_HOST_SOURCE:pathToFileURL(bridge).href};}
  const child=spawnSync(process.execPath,['--max-old-space-size=512','--test','--test-reporter=tap','--test-concurrency=1',testFile],{cwd:repo,env:childEnv,encoding:'utf8',timeout:20000});const log=child.stdout+child.stderr;await writeFile(join(output,name+'.log'),log);const fail=Number(log.match(/# fail (\d+)/u)?.[1]??0),skip=Number(log.match(/# skipped (\d+)/u)?.[1]??-1),instrumentation=/(?:SyntaxError|ReferenceError|ERR_MODULE_NOT_FOUND)/u.test(log);results.push({name,beforeSha256:hash(source),afterSha256:hash(source.replace(anchor,replacement)),exitCode:child.status,fail,skip,instrumentation,detected:child.status===1&&fail>0&&skip===0&&!instrumentation});
 }
 const file=join(repo,'server','monitoring-module.js'),original=await readFile(file,'utf8'),anchor="return apps().withAppAuthority({actor,appId:scope.appId,mode:'owner'},()=>callback());";if(original.split(anchor).length!==2)throw new Error('owner mutation anchor drift');let changed=original.replace(anchor,'return callback();');changed=changed.replace(/from '([^']+)'/gu,(whole,specifier)=>specifier.startsWith('.')?`from '${new URL(specifier,pathToFileURL(file)).href}'`:whole);const mutant=join(root,'owner-bypass.mjs');await writeFile(mutant,changed);
 const child=spawnSync(process.execPath,['--max-old-space-size=512','--test','--test-reporter=tap','--test-concurrency=1',join(repo,'modules','monitoring','test','host.test.mjs')],{cwd:repo,env:{...env,SOTY_MONITORING_HOST_SOURCE:pathToFileURL(mutant).href},encoding:'utf8',timeout:20000});const log=child.stdout+child.stderr;await writeFile(join(output,'owner-bypass.log'),log);const fail=Number(log.match(/# fail (\d+)/u)?.[1]??0),skip=Number(log.match(/# skipped (\d+)/u)?.[1]??-1),instrumentation=/(?:SyntaxError|ReferenceError|ERR_MODULE_NOT_FOUND)/u.test(log);results.push({name:'owner-bypass',beforeSha256:hash(original),afterSha256:hash(changed),exitCode:child.status,fail,skip,instrumentation,detected:child.status===1&&fail>0&&skip===0&&!instrumentation});
 await writeFile(join(output,'results.json'),JSON.stringify({results,allDetected:results.every(result=>result.detected)},null,2));console.log(JSON.stringify(results));if(!results.every(result=>result.detected))process.exitCode=1;
}finally{if(dirname(root)!==tmpdir()||!basename(root).startsWith('soty-monitoring-mutants-'))throw new Error('unsafe temp path');await rm(root,{recursive:true,force:true});}
