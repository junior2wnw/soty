import {createRequire} from 'node:module';
import {createHash} from 'node:crypto';
import {mkdir,readFile,writeFile,realpath} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {readerBridgePolicy,readerBridgeReaders} from './reader-bridge.mjs';

const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const refuse=code=>{throw Object.assign(new Error(code),{code});};
export async function buildReaderBridgeContext({sourceRoot,dependencyRoot=sourceRoot,outputDirectory,baseImageTag}){
  if(typeof baseImageTag!=='string'||!/^[a-z0-9][a-z0-9./_-]*:[a-zA-Z0-9][a-zA-Z0-9_.-]*$/u.test(baseImageTag))refuse('reader_bridge_base_tag_invalid');
  const root=await realpath(sourceRoot),out=path.resolve(outputDirectory);
  if(out===root||out.startsWith(root+path.sep))refuse('reader_bridge_output_invalid');
  const require=createRequire(path.join(path.resolve(dependencyRoot),'package.json')),esbuild=require('esbuild');
  const result=await esbuild.build({absWorkingDir:root,entryPoints:['deploy/connector/reader-bridge.mjs'],bundle:true,platform:'node',
    target:'node24.15',format:'esm',packages:'external',treeShaking:true,write:false,metafile:true,logLevel:'silent',
    define:{'process.env.SOTY_STORAGE_PROBE':'"0"'}});
  if(result.outputFiles.length!==1)refuse('reader_bridge_bundle_invalid');
  const external=[...new Set(Object.values(result.metafile.outputs).flatMap(value=>value.imports.filter(item=>item.external).map(item=>item.path)))];
  if(external.some(specifier=>!specifier.startsWith('node:')))refuse('reader_bridge_external_dependency');
  const sources=[];
  for(const name of Object.keys(result.metafile.inputs).sort()){
    if(path.isAbsolute(name)||name.split(/[\\/]/u).includes('..')||name.startsWith('node_modules/'))refuse('reader_bridge_source_invalid');
    const raw=await readFile(path.join(root,name));sources.push({path:name,bytes:raw.length,sha256:sha(raw)});
  }
  const template=(await readFile(path.join(root,'deploy/connector/reader-bridge.Dockerfile'),'utf8')).replaceAll('\r\n','\n');
  if(!template.includes('@@READERS@@')||!template.includes('@@BUNDLE_SHA256@@')||!template.includes('ARG REVISION\n')
    ||!template.includes('LABEL org.opencontainers.image.revision=${REVISION}')||!template.includes('RUN --network=none node '))refuse('reader_bridge_template_invalid');
  const bundle=result.outputFiles[0].contents,bundleSha256=sha(bundle);
  const dockerfile=template.replace('@@READERS@@',JSON.stringify(readerBridgeReaders)).replaceAll('@@BUNDLE_SHA256@@',bundleSha256);
  const manifest={...readerBridgePolicy,baseImageTag,readerManifest:JSON.parse(readerBridgeReaders),bundle:{file:'reader-bridge.bundle.mjs',bytes:bundle.length,sha256:bundleSha256},
    esbuildVersion:esbuild.version,externalImports:external,sourceClosure:sources,nodeOnlyClosure:true,
    dynamicOriginalImport:{file:readerBridgePolicy.originalEntryName,sha256:readerBridgePolicy.originalEntrySha256,bundled:false},
    rootRequiredChecks:['local tag resolves to exact baseImage digest before build','build REVISION is the exact reviewed bridge source commit','postbuild base filetree equality except wrapper+relocated original','inherited CMD ENTRYPOINT USER ENV HostConfig unchanged','actual pinned runtime reader negatives and full current-copy cold canary'],
    preparedOnly:true,dockerBuildOrRun:false};
  await mkdir(out,{mode:0o700});
  for(const [name,raw]of[['reader-bridge.bundle.mjs',bundle],['Dockerfile',dockerfile],['manifest.json',JSON.stringify(manifest,null,2)+'\n']])await writeFile(path.join(out,name),raw,{flag:'wx',mode:0o600});
  return manifest;
}

if(process.argv[1]&&pathToFileURL(path.resolve(process.argv[1])).href===import.meta.url){
  const args=process.argv.slice(2),options={sourceRoot:path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../..')};
  const names={'--source-root':'sourceRoot','--dependency-root':'dependencyRoot','--out':'outputDirectory','--base-image':'baseImageTag'};
  for(let i=0;i<args.length;i+=2){if(!names[args[i]]||!args[i+1]||options[names[args[i]]]!==undefined&&args[i]!=='--source-root')refuse('reader_bridge_arguments_invalid');options[names[args[i]]]=args[i+1];}
  if(!options.outputDirectory||!options.baseImageTag)refuse('reader_bridge_arguments_invalid');
  const manifest=await buildReaderBridgeContext(options);process.stdout.write(JSON.stringify({ok:true,outputDirectory:path.resolve(options.outputDirectory),baseImage:manifest.baseImage,bundle:manifest.bundle,sourceFiles:manifest.sourceClosure.length,dockerBuildOrRun:false})+'\n');
}
