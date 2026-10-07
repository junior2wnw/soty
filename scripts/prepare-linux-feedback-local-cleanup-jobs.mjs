import {createRequire} from 'node:module';
import {mkdir,readFile,writeFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {resolve,dirname,join,relative,isAbsolute} from 'node:path';
import {createHash,randomBytes} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {SYNTHETIC_FEEDBACK_IMAGE} from '../modules/source-app/server/linux-feedback-enforcer.mjs';
import {CLEANUP_LINUX_FEEDBACK_PLACEMENT} from '../modules/source-app/server/linux-feedback-enforcer.mjs';
import {digest} from '../modules/source-app/server/wire.mjs';

const root=resolve(dirname(fileURLToPath(import.meta.url)),'..'),nonce=randomBytes(16).toString('hex'),directory=join(root,'output','linux-feedback-local-cleanup-jobs-'+nonce);
const git=args=>execFileSync('git',['-c','safe.directory='+root,...args],{cwd:root,maxBuffer:4194304,windowsHide:true});
const sourceCommit=git(['rev-parse','HEAD']).toString('ascii').trim();if(!/^[a-f0-9]{40}$/u.test(sourceCommit))throw new Error('source_commit_required');
const frozenFiles=new Map(),sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const require=createRequire(import.meta.url);let esbuild;try{esbuild=require('esbuild');}catch{esbuild=createRequire(require.resolve('vite'))('esbuild');}
if(esbuild.version!=='0.27.7')throw new Error('pinned_esbuild_required');await mkdir(directory,{recursive:true});
const exactRoot=new Map([[resolve(root,'server/http-app.js'),'/app/server/http-app.js'],[resolve(root,'modules/connect/browser/client.mjs'),'/app/modules/connect/browser/client.mjs'],[resolve(root,'modules/apps/server/schema.mjs'),'/app/modules/apps/server/schema.mjs']]);
const plugin={name:'exact-image-root',setup(build){
  build.onResolve({filter:/^openid-client$/},()=>({path:'/app/node_modules/openid-client/build/index.js',external:true}));
  build.onResolve({filter:/\.(mjs|js)$/},args=>{if(!args.importer)return;const path=resolve(dirname(args.importer),args.path);if(exactRoot.has(path))return{path:exactRoot.get(path),external:true};});
  build.onLoad({filter:/\.(mjs|js)$/},args=>{
    const file=resolve(args.path),path=relative(root,file).replaceAll('\\','/');
    if(path==='..'||path.startsWith('../')||isAbsolute(path))throw new Error('source_path_required');
    const bytes=git(['show',sourceCommit+':'+path]);frozenFiles.set(path,bytes);return{contents:bytes.toString('utf8'),loader:'js'};
  });
}};
async function bundle(entry,name,target){await esbuild.build({absWorkingDir:root,entryPoints:[join(root,entry)],outfile:join(directory,name),bundle:true,platform:'node',target,format:'esm',external:['openid-client'],logLevel:'silent',plugins:[plugin]});}
await bundle('scripts/fixtures/linux-feedback-local-cleanup-jobs.mjs','fixture.mjs','node24');
await bundle('scripts/fixtures/linux-feedback-local-cleanup-supervisor.mjs','supervisor.mjs','node18');
const nativePath='scripts/fixtures/linux-feedback-native-revoke.mjs',nativeBytes=git(['show',sourceCommit+':'+nativePath]);frozenFiles.set(nativePath,nativeBytes);
await writeFile(join(directory,'native-revoke.mjs'),nativeBytes);
const sourceFiles=[...frozenFiles].sort(([a],[b])=>a.localeCompare(b)).map(([path,bytes])=>({path,sha256:sha(bytes)}));
const fixtureSha256=sha(await readFile(join(directory,'fixture.mjs'))),supervisorSha256=sha(await readFile(join(directory,'supervisor.mjs'))),nativeRevokeSha256=sha(nativeBytes);
const manifest={schema:'soty.source-feedback-local-linux-packet.v1',nonce,image:SYNTHETIC_FEEDBACK_IMAGE,placementDigest:digest(CLEANUP_LINUX_FEEDBACK_PLACEMENT),fixtureSha256,supervisorSha256,nativeRevokeSha256,sourceCommit,
  sourceFiles,externalRootImports:[...exactRoot.values(),'/app/node_modules/openid-client/build/index.js'],cases:7,synthetic:true,models:false,productionReady:false};
await writeFile(join(directory,'manifest.json'),JSON.stringify(manifest,null,2)+'\n');
process.stdout.write(JSON.stringify({directory,nonce,fixtureSha256,supervisorSha256,nativeRevokeSha256,manifestSha256:sha(await readFile(join(directory,'manifest.json'))),sourceFiles:sourceFiles.length})+'\n');
