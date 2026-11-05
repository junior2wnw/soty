import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {createRequire} from 'node:module';
import {readFile,writeFile} from 'node:fs/promises';
import {dirname,join,relative,resolve,isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';

const repository=resolve(dirname(fileURLToPath(import.meta.url)),'..');
const require=createRequire(import.meta.url),esbuild=require('esbuild');
const entry='modules/apps/scoped-embed/source-host.mjs';
const generator='scripts/build-maintained-source-ci-sdk.mjs';
const sdkFile='server/soty-source-host.mjs';
const sha=value=>createHash('sha256').update(value).digest('hex');
const json=value=>JSON.stringify(value,null,2)+'\n';
const fail=()=>{throw Error('maintained_source_sdk_overlay_mismatch');};
const need=value=>{if(!value)fail();};

// This build-only overlay reuses the maintained issuer; no independent clock
// rule or runtime path back into the Root checkout is embedded in the Source.
export async function buildMaintainedSourceCiSdk(root=repository){
  root=resolve(root);need(esbuild.version==='0.27.7');
  const built=await esbuild.build({entryPoints:[join(root,entry)],outfile:join(root,'source-ci-sdk.mjs'),absWorkingDir:root,
    bundle:true,write:false,metafile:true,platform:'node',format:'esm',target:'node24',external:['openid-client'],logLevel:'silent'});
  need(built.outputFiles.length===1);
  const names=Object.keys(built.metafile.inputs).map(name=>relative(root,resolve(root,name)).replaceAll('\\','/'));
  need(names.includes('modules/apps/scoped-embed/source-authority-client.mjs')
    &&names.every(name=>!isAbsolute(name)&&!name.startsWith('../')&&!name.includes('/node_modules/')));
  const exports=Object.values(built.metafile.outputs)[0].exports.slice().sort();
  assert.deepEqual(exports,['SCOPED_EMBED_LIMITS','SCOPED_EMBED_PROFILE','ScopedEmbedError','createSourceAuthorityClient',
    'createSourceCurrentSubjectPort','createSourceProofVerifier','scopedEmbedProfile','sourceConsentDigest'].sort());
  const inputs={};
  for(const name of [...new Set([...names,generator])].sort()){
    const text=new TextDecoder('utf-8',{fatal:true}).decode(await readFile(join(root,name)));
    const bytes=Buffer.from(text.replaceAll('\r\n','\n'));inputs[name]={bytes:bytes.length,sha256:sha(bytes)};
  }
  const bytes=Buffer.from(built.outputFiles[0].contents);
  return{bytes,inputs,entry,generator,esbuild:esbuild.version,output:{bytes:bytes.length,sha256:sha(bytes)}};
}

export async function verifyMaintainedSourceCiSdk(directory=join(repository,'test-fixtures/planner-source-ci'),root=repository){
  const manifest=JSON.parse(await readFile(join(directory,'SOURCE-PINS.json'),'utf8'));
  const overlay=manifest.sdkOverlay,built=await buildMaintainedSourceCiSdk(root);
  need(manifest.schema==='soty.maintained-source-ci-capsule.v2'&&overlay?.schema==='soty.maintained-source-sdk-overlay.v1'
    &&overlay.version===1&&overlay.inputEncoding==='utf-8-lf'&&overlay.entry===built.entry&&overlay.generator===built.generator&&overlay.esbuild===built.esbuild);
  assert.deepEqual(overlay.inputs,built.inputs,'the overlay must match the actual canonical input graph');
  assert.deepEqual(manifest.files[sdkFile],built.output,'the pinned SDK must match reproducible output');
  need((await readFile(join(directory,sdkFile))).equals(built.bytes));
  const old=await readFile(join(root,'modules/apps/test/fixtures/source-host-before-clock-margin.mjs'));
  need(overlay.baseFile.bytes===old.length&&overlay.baseFile.sha256===sha(old));
  return{passed:true,originalGitFiles:85,sdkOverlays:1,sourceRevision:manifest.sourceRevision,inputFiles:Object.keys(built.inputs).length,output:built.output};
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  const args=process.argv.slice(2);need(args.length===1&&['--write','--check'].includes(args[0]));
  const directory=join(repository,'test-fixtures/planner-source-ci');
  if(args[0]==='--write'){
    const manifest=JSON.parse(await readFile(join(directory,'SOURCE-PINS.json'),'utf8'));
    need(['soty.maintained-source-ci-capsule.v1','soty.maintained-source-ci-capsule.v2'].includes(manifest.schema)
      &&manifest.sourceRevision==='6c614feb489767266ce23676b2bc0df6342f8370'&&Object.keys(manifest.files).length===86);
    const built=await buildMaintainedSourceCiSdk(),baseFile=manifest.sdkOverlay?.baseFile??manifest.files[sdkFile];
    manifest.schema='soty.maintained-source-ci-capsule.v2';
    manifest.purpose='actual maintained Source in Root build-only tests;85 original Git files plus one declared maintained SDK overlay';
    manifest.files[sdkFile]=built.output;
    manifest.sdkOverlay={schema:'soty.maintained-source-sdk-overlay.v1',version:1,inputEncoding:'utf-8-lf',entry:built.entry,generator:built.generator,
      esbuild:built.esbuild,baseFile,inputs:built.inputs};
    await writeFile(join(directory,sdkFile),built.bytes);await writeFile(join(directory,'SOURCE-PINS.json'),json(manifest));
  }
  console.log(JSON.stringify(await verifyMaintainedSourceCiSdk(directory)));
}
