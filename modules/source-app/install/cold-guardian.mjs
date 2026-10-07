// Operator-only public guardian. It admits one freshly reviewed packet/image
// and invokes only its fixed Source cold runner; no publisher URLs/commands.
import {readFile,lstat,realpath} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {resolve,dirname} from 'node:path';
import {pathToFileURL} from 'node:url';
import {LOCAL_LINUX_FEEDBACK_PLACEMENT as placement} from '../server/linux-feedback-local-placement.mjs';
import {runSourceCold} from './cold-runner.mjs';
import {SOURCE_COLD_PROFILE as profile} from './cold-profile.mjs';
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const check=value=>{if(!value)throw Error('source_cold_guard_refused');};
export async function runSourceColdGuardian(path){
  check(process.platform==='linux'&&process.getuid()===1000&&/^v24\./.test(process.version)&&resolve(path)===path);
  const directory=dirname(path),stat=await lstat(path);check(stat.isFile()&&!stat.isSymbolicLink()&&stat.uid===1000&&(stat.mode&0o777)===0o600&&stat.size<=8192&&await realpath(path)===path);
  const config=JSON.parse(await readFile(path,'utf8'));
  check(Object.keys(config).sort().join(',')==='coldManifestSha256,dockerCliSha256,imageId,nonce,packetManifestSha256,sourceCommit'
    &&/^[a-f0-9]{32}$/.test(config.nonce)&&/^sha256:[a-f0-9]{64}$/.test(config.imageId)&&/^[a-f0-9]{40}$/.test(config.sourceCommit)
    &&[config.coldManifestSha256,config.dockerCliSha256,config.packetManifestSha256].every(value=>/^[a-f0-9]{64}$/.test(value))
    &&directory===placement.lab+'/source-cold-'+config.nonce);
  check(config.imageId===profile.imageId&&config.sourceCommit===profile.sourceCommit&&config.packetManifestSha256===profile.packetManifestSha256);
  const parent=await lstat(placement.lab),folder=await lstat(directory);check(parent.isDirectory()&&folder.isDirectory()&&!parent.isSymbolicLink()&&!folder.isSymbolicLink()
    &&parent.uid===1000&&folder.uid===1000&&(parent.mode&0o777)===0o700&&(folder.mode&0o777)===0o700);
  const bytes=await readFile(directory+'/manifest.json');check(sha(bytes)===config.coldManifestSha256);
  const manifest=JSON.parse(bytes.toString('utf8'));check(manifest.schema==='soty.source-cold-packet.v1'&&manifest.nonce===config.nonce&&manifest.physicalVolumesRequired===2
    &&manifest.authenticationProved===false&&manifest.productionReady===false&&manifest.models===false);
  for(const [name,key] of [['fixture.mjs','fixtureSha256'],['extract.mjs','extractSha256'],['runner.mjs','runnerSha256'],['guardian.mjs','guardianSha256'],['entry.mjs','entrySha256'],['supervisor.mjs','supervisorSha256'],['stream-probe.mjs','streamProbeSha256']]){
    const file=directory+'/'+name,s=await lstat(file);check(s.isFile()&&!s.isSymbolicLink()&&s.size<=4194304&&sha(await readFile(file))===manifest[key]);}
  const cli=await lstat(profile.dockerBinary),socket=await lstat(profile.socket);
  check(cli.isFile()&&!cli.isSymbolicLink()&&sha(await readFile(profile.dockerBinary))===config.dockerCliSha256
    &&socket.isSocket()&&!socket.isSymbolicLink()&&socket.gid===1001&&(socket.mode&0o777)===0o660&&await realpath(profile.socket)===profile.socket);
  return runSourceCold({imageId:config.imageId,nonce:config.nonce,packetDirectory:directory,packetManifestSha256:config.packetManifestSha256,sourceCommit:config.sourceCommit});
}
if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url){try{check(process.argv.length===3);console.log(JSON.stringify(await runSourceColdGuardian(resolve(process.argv[2]))));}
catch{console.log(JSON.stringify({schema:'soty.source-cold-receipt.v1',passed:false,phase:'guardian',productionReady:false}));process.exitCode=1;}}
