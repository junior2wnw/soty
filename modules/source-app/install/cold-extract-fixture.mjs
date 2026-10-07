// PRIVATE input stream; PUBLIC fixed code. Keys/archive metadata never enter
// stdout, files, args or environment. Native directory is a fresh physical
// named volume; this helper cannot choose an extraction destination.
import {mkdir,lstat,stat,open,readFile,readdir} from 'node:fs/promises';
import {createOrdinaryNativeRestorePorts} from '../../../deploy/connect/restore-backup.mjs';
import {SOURCE_COLD_NATIVE_REALM,sourceColdFailureCode} from './cold-original.mjs';
import {transferColdNativeThroughRam} from './cold-ram-transfer.mjs';
const nativeRestore=createOrdinaryNativeRestorePorts({realmId:SOURCE_COLD_NATIVE_REALM});
const ROOT='/target',namespace=ROOT+'/restore',dataRoot=namespace+'/data',configRoot=namespace+'/config';
async function mountId(path){const file=await open(path,'r');try{const text=await readFile('/proc/self/fdinfo/'+file.fd,'ascii');const match=text.match(/^mnt_id:\s*([1-9]\d*)$/m);if(!match)throw Error('mount');return Number(match[1]);}finally{await file.close();}}
async function pin(path){const info=await lstat(path,{bigint:true});if(!info.isDirectory()||info.uid!==1000n||(info.mode&0o7777n)!==0o700n)throw Error('custody');
  return{path,dev:info.dev,ino:info.ino,mountId:await mountId(path)};}
try{if(process.platform!=='linux'||process.getuid()!==1000)throw Error('platform');let size=0;const chunks=[];
  for await(const chunk of process.stdin){size+=chunk.length;if(size>131072)throw Error('input');chunks.push(chunk);}
  const bytes=Buffer.concat(chunks),input=JSON.parse(bytes.toString('utf8'));bytes.fill(0);chunks.forEach(chunk=>chunk.fill(0));
  if(Object.keys(input).sort().join(',')!=='expectedManifestSha256,expectedSha256,limits,privateKeyPem,sourceWitness,targetId')throw Error('shape');
  const root=await lstat(ROOT);if(!root.isDirectory()||root.isSymbolicLink()||root.uid!==1000||(root.mode&0o777)!==0o700||(await readdir(ROOT)).length!==0)throw Error('target');
  await mkdir(namespace,{mode:0o700});await mkdir(dataRoot,{mode:0o700});await mkdir(configRoot,{mode:0o700});
  const ns=await stat('/proc/self/ns/mnt',{bigint:true});
  const target={targetId:input.targetId,mountNamespace:{dev:ns.dev,ino:ns.ino},namespace:await pin(namespace),dataRoot:await pin(dataRoot),configRoot:await pin(configRoot)};
  const result=await transferColdNativeThroughRam(nativeRestore,input,target);input.privateKeyPem='';
  console.log(JSON.stringify(result));if(!result?.passed)process.exitCode=1;
}catch(error){console.log(JSON.stringify({passed:false,code:sourceColdFailureCode(error),productionReady:false}));process.exitCode=1;}
