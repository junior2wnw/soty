#!/usr/bin/env node
import {execFileSync} from 'node:child_process';
import {readFile,mkdtemp,mkdir,writeFile,lstat,rename,unlink,realpath,readdir} from 'node:fs/promises';
import {join,resolve,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomBytes} from 'node:crypto';
import {SOURCE_SDK_FILES,SOURCE_SDK_TEXT_FILES,SOURCE_SDK_INDEX,SOURCE_SDK_LIMITS,sourceSdkArchiveName} from '../modules/source-app/package-contract.mjs';
import {createSourceSdkArchive,readSourceSdkArchive,sourceSdkReleaseIndex,sourceSdkHash,sourceSdkSourceName,sourceSdkRuntimeLock,verifySourceSdkContext,verifyPublicSourceSdk} from '../modules/source-app/public-release.mjs';

const root=resolve(dirname(fileURLToPath(import.meta.url)),'..');
const fail=code=>{throw Object.assign(new Error(code),{code});};
const need=(condition,code='source_sdk_release_invalid')=>{if(!condition)fail(code);};
const json=value=>Buffer.from(JSON.stringify(value,null,2)+'\n');

/** Explicit release action ONLY. Docker/prebuild invokes --check, which reads
 * the already frozen capsule and verifies source bytes without requiring Git. */
export async function buildSourceSdkRelease(){
  const git=args=>execFileSync('git',['-c','safe.directory='+root,...args],{cwd:root,windowsHide:true,maxBuffer:4194304});
  const sourceCommit=git(['rev-parse','HEAD']).toString('ascii').trim();need(/^[a-f0-9]{40}$/u.test(sourceCommit),'source_sdk_git_required');
  need(!git(['status','--porcelain','--untracked-files=all','--','modules/source-app','scripts/build-source-sdk-release.mjs','pnpm-lock.yaml']).toString('utf8').trim(),'source_sdk_freeze_required');
  const frozen=name=>{need(sourceSdkSourceName(name),'source_sdk_source_denied');return git(['show',sourceCommit+':'+name]);};
  const original=JSON.parse(frozen('modules/source-app/package.json'));
  const pkg=Object.fromEntries(['name','version','type','bin','engines','exports','dependencies'].map(key=>[key,original[key]]));pkg.files=SOURCE_SDK_FILES;
  const lock=sourceSdkRuntimeLock(pkg,frozen('pnpm-lock.yaml'));
  await mkdir(join(root,'output'),{recursive:true});const stage=await mkdtemp(join(root,'output/source-sdk-release-'));
  const {buildSourceAppPackage}=await import('../modules/source-app/scripts/build.mjs');
  const built=await buildSourceAppPackage({outdir:join(stage,'dist'),sourceCommit});
  const files=new Map([['package.json',json(pkg)],['npm-shrinkwrap.json',json(lock)]]);
  for(const item of built.outputs)files.set('dist/'+item.path,await readFile(join(stage,'dist',item.path)));
  files.set('dist/provenance.json',await readFile(join(stage,'dist/provenance.json')));
  for(const name of SOURCE_SDK_TEXT_FILES)files.set(name,frozen('modules/source-app/'+name));
  const sourceFiles=new Map(built.sourceFiles.map(item=>[item.path,item.sha256]));
  for(const name of ['pnpm-lock.yaml','scripts/build-source-sdk-release.mjs','deploy/connect/gpg-tar.mjs',
    'modules/source-app/scripts/build.mjs','modules/source-app/public-release.mjs',...SOURCE_SDK_TEXT_FILES.map(name=>'modules/source-app/'+name)])
    sourceFiles.set(name,sourceSdkHash(frozen(name)));
  const capsule={schema:'soty.source-sdk.capsule.v1',sourceCommit,packageVersion:pkg.version,
    files:[...files].map(([path,bytes])=>({path,sha256:sourceSdkHash(bytes),bytes:bytes.length})).sort((a,b)=>a.path<b.path?-1:1),
    sourceFiles:[...sourceFiles].map(([path,sha256])=>({path,sha256})).sort((a,b)=>a.path<b.path?-1:1),dependencyLockDigest:sourceSdkHash(files.get('npm-shrinkwrap.json'))};
  files.set('SDK-CAPSULE.json',json(capsule));
  const archive=createSourceSdkArchive(files);readSourceSdkArchive(archive);verifySourceSdkContext(root,archive);
  const index=sourceSdkReleaseIndex(archive);let destination=root;
  for(const part of ['public','sdk','source-app']){destination=join(destination,part);
    try{await mkdir(destination);}catch(error){if(error.code!=='EEXIST')throw error;}
    const stat=await lstat(destination);need(stat.isDirectory()&&!stat.isSymbolicLink()&&await realpath(destination)===destination,'source_sdk_public_layout_invalid');
    if(part==='sdk')need((await readdir(destination)).every(name=>name==='source-app'),'source_sdk_public_layout_invalid');}
  const existing=await readdir(destination);need(existing.every(name=>name==='index.json'||sourceSdkArchiveName('sdk/source-app/'+name)),'source_sdk_public_layout_invalid');
  for(const name of existing){const path=join(destination,name),stat=await lstat(path);need(stat.isFile()&&!stat.isSymbolicLink(),'source_sdk_public_layout_invalid');
    if(name!=='index.json'){need(stat.size<=SOURCE_SDK_LIMITS.archiveBytes,'source_sdk_archive_limit');const bytes=await readFile(path);
      need(name===sourceSdkHash(bytes)+'.tgz','source_sdk_archive_hash_mismatch');readSourceSdkArchive(bytes);}}
  need(existing.filter(name=>name.endsWith('.tgz')).length<SOURCE_SDK_LIMITS.archives||existing.includes(index.artifact.sha256+'.tgz'),'source_sdk_public_capacity');
  // Existing hash URLs are immutable. Never overwrite them with different bytes.
  const archivePath=join(destination,index.artifact.sha256+'.tgz');
  try{const prior=await lstat(archivePath);need(prior.isFile()&&!prior.isSymbolicLink()&&prior.size===archive.length,'source_sdk_immutable_conflict');
    need((await readFile(archivePath)).equals(archive),'source_sdk_immutable_conflict');}
  catch(error){if(error.code!=='ENOENT')throw error;await writeFile(archivePath,archive,{flag:'wx',mode:0o644});}
  const temporary=join(destination,'.index-'+randomBytes(8).toString('hex')+'.tmp');
  try{await writeFile(temporary,json(index),{flag:'wx',mode:0o644});await rename(temporary,join(root,'public',SOURCE_SDK_INDEX));}
  finally{await unlink(temporary).catch(error=>{if(error.code!=='ENOENT')throw error;});}
  return{ok:true,sourceCommit,packageVersion:pkg.version,artifact:index.artifact,productionReady:false};
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  try{const args=process.argv.slice(2);need(args.length===0||args.length===1&&args[0]==='--check','source_sdk_release_usage_invalid');
    const result=args.length?verifyPublicSourceSdk(root):await buildSourceSdkRelease();console.log(JSON.stringify(result));}
  catch(error){console.error(JSON.stringify({ok:false,code:typeof error.code==='string'&&/^[a-z][a-z0-9_]{0,90}$/u.test(error.code)?error.code:'source_sdk_release_failed'}));process.exitCode=1;}
}
