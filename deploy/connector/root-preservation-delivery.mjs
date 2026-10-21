#!/usr/bin/env node
import { readFile,lstat,realpath,mkdir,writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const must=(condition,code)=>{if(!condition)throw Object.assign(new Error(code),{code});};

/** Source-only materialization. No Docker, process launch, configuration or keys. */
export async function materializeRootPreservationStage(directory){
  must(typeof directory==='string'&&path.isAbsolute(directory)&&!directory.includes('\0'),'delivery_target_invalid');
  const parent=path.dirname(directory);must(await realpath(parent)===path.resolve(parent),'delivery_target_invalid');
  const manifestBytes=await readFile(new URL('./root-preservation-qualification.manifest.json',import.meta.url)),manifest=JSON.parse(manifestBytes);
  must(manifest.schema==='soty.root-preservation-qualification-sources.v2'&&manifest.stockRevision==='cbeb42586ff501899652b2010188415ce5b68d16','delivery_manifest_invalid');
  const files=new Map(),root=new URL('../../',import.meta.url);
  for(const entry of [...manifest.files,...manifest.tests]){
    must(typeof entry.path==='string'&&path.posix.normalize(entry.path)===entry.path&&!entry.path.startsWith('../')&&!path.posix.isAbsolute(entry.path)
      &&!entry.path.includes('\\')&&!entry.path.includes('\0')&&!files.has(entry.path)&&['raw','git-lf'].includes(entry.representation),'delivery_manifest_invalid');
    const url=new URL(entry.path,root),stat=await lstat(url);must(stat.isFile()&&!stat.isSymbolicLink()&&stat.nlink===1,'delivery_source_invalid');
    const raw=await readFile(url),bytes=entry.representation==='git-lf'?Buffer.from(raw.toString('utf8').replaceAll('\r\n','\n')):raw;
    must(bytes.length===entry.bytes&&sha(bytes)===entry.sha256,'delivery_source_changed');files.set(entry.path,bytes);
  }
  const manifestPath='deploy/connector/root-preservation-qualification.manifest.json';must(!files.has(manifestPath),'delivery_manifest_invalid');
  files.set(manifestPath,manifestBytes);
  // Exclusive NEW target. A partial stage is retained on failure, never reused.
  await mkdir(directory,{mode:0o700});
  for(const [file,bytes] of files){const target=path.join(directory,file);await mkdir(path.dirname(target),{recursive:true,mode:0o700});await writeFile(target,bytes,{flag:'wx',mode:0o600});}
  for(const [file,bytes] of files)must(sha(await readFile(path.join(directory,file)))===sha(bytes),'delivery_readback_failed');
  return {ok:true,schema:'soty.root-preservation-delivery.v2',files:files.size,manifestSha256:sha(manifestBytes)};
}
if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href){
  try{must(process.argv.length===4&&process.argv[2]==='--output','delivery_arguments_invalid');process.stdout.write(JSON.stringify(await materializeRootPreservationStage(process.argv[3]))+'\n');}
  catch(error){process.stdout.write(JSON.stringify({ok:false,code:/^delivery_[a-z_]+$/u.test(error.code||'')?error.code:'delivery_failed'})+'\n');process.exitCode=1;}
}
