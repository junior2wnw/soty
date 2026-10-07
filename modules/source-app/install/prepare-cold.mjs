import {resolve,dirname,relative,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {mkdir,readFile,writeFile} from 'node:fs/promises';
import {createHash,randomBytes} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {createRequire} from 'node:module';
import {SOURCE_COLD_PROFILE} from './cold-profile.mjs';
const root=resolve(dirname(fileURLToPath(import.meta.url)),'../../..'),git=args=>execFileSync('git',['-c','safe.directory='+root,...args],{cwd:root,maxBuffer:4194304,windowsHide:true});
const sourceCommit=git(['rev-parse','HEAD']).toString('ascii').trim(),files=new Map(),sha=bytes=>createHash('sha256').update(bytes).digest('hex');
if(git(['status','--porcelain','--untracked-files=all','--','modules/source-app/install']).toString().trim())throw Error('source_cold_freeze_required');
const nonce=randomBytes(16).toString('hex'),directory=join(root,'output','source-cold-'+nonce);await mkdir(directory);
const require=createRequire(import.meta.url),esbuild=createRequire(require.resolve('vite'))('esbuild');if(esbuild.version!=='0.27.7')throw Error('source_cold_build_tool_required');
for(const [input,output] of [['cold-source-fixture.mjs','fixture.mjs'],['cold-extract-fixture.mjs','extract.mjs'],['cold-runner.mjs','runner.mjs'],['cold-guardian.mjs','guardian.mjs'],['cold-entry-fixture.mjs','entry.mjs'],['cold-supervisor.mjs','supervisor.mjs']])await esbuild.build({entryPoints:[join(root,'modules/source-app/install',input)],outfile:join(directory,output),bundle:true,platform:'node',target:output==='supervisor.mjs'?'node18':'node24',format:'esm',logLevel:'silent',
  plugins:[{name:'exact-source-cold',setup(build){build.onLoad({filter:/\.mjs$/},args=>{const path=relative(root,args.path).replaceAll('\\','/');
    if(path.startsWith('../'))throw Error('source_cold_path_denied');const bytes=git(['show',sourceCommit+':'+path]);files.set(path,sha(bytes));return{contents:bytes.toString('utf8'),loader:'js'};});}}]});
const manifest={schema:'soty.source-cold-packet.v1',nonce,sourceCommit,fixtureSha256:sha(await readFile(join(directory,'fixture.mjs'))),
  extractSha256:sha(await readFile(join(directory,'extract.mjs'))),runnerSha256:sha(await readFile(join(directory,'runner.mjs'))),
  guardianSha256:sha(await readFile(join(directory,'guardian.mjs'))),
  entrySha256:sha(await readFile(join(directory,'entry.mjs'))),supervisorSha256:sha(await readFile(join(directory,'supervisor.mjs'))),
  sourceFiles:[...files].sort(([a],[b])=>a.localeCompare(b)).map(([path,sha256])=>({path,sha256})),nativeReader:3,legacyReader:2,physicalVolumesRequired:2,
  synthetic:true,authenticationProved:false,models:false,productionReady:false};
await writeFile(join(directory,'manifest.json'),JSON.stringify(manifest,null,2)+'\n');console.log(JSON.stringify({directory,nonce,manifestSha256:sha(await readFile(join(directory,'manifest.json'))),sourceCommit,fixtureSha256:manifest.fixtureSha256,sourceFiles:files.size}));
await writeFile(join(directory,'operator.template.json'),JSON.stringify({coldManifestSha256:sha(await readFile(join(directory,'manifest.json'))),dockerCliSha256:'REPLACE_WITH_REVIEWED_CLI_SHA256',
  imageId:SOURCE_COLD_PROFILE.imageId,nonce,packetManifestSha256:SOURCE_COLD_PROFILE.packetManifestSha256,sourceCommit:SOURCE_COLD_PROFILE.sourceCommit},null,2)+'\n');
