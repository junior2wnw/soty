import {createRequire} from 'node:module';
import {mkdir,readFile,writeFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {resolve,dirname,join} from 'node:path';
import {createHash,randomBytes} from 'node:crypto';
import {SYNTHETIC_FEEDBACK_IMAGE} from '../modules/source-app/server/linux-feedback-enforcer.mjs';

const root=resolve(dirname(fileURLToPath(import.meta.url)),'..'),nonce=randomBytes(16).toString('hex'),directory=join(root,'output','linux-feedback-jobs-'+nonce);
const require=createRequire(import.meta.url);let esbuild;try{esbuild=require('esbuild');}catch{esbuild=createRequire(require.resolve('vite'))('esbuild');}
if(esbuild.version!=='0.27.7')throw new Error('pinned_esbuild_required');await mkdir(directory,{recursive:true});
const exactRoot=new Map([[resolve(root,'server/http-app.js'),'/app/server/http-app.js'],[resolve(root,'modules/connect/browser/client.mjs'),'/app/modules/connect/browser/client.mjs'],[resolve(root,'modules/apps/server/schema.mjs'),'/app/modules/apps/server/schema.mjs']]);
const result=await esbuild.build({entryPoints:[join(root,'scripts/fixtures/linux-feedback-jobs.mjs')],outfile:join(directory,'fixture.mjs'),bundle:true,platform:'node',target:'node24',format:'esm',external:['openid-client'],metafile:true,logLevel:'silent',
  plugins:[{name:'exact-image-root',setup(build){
    build.onResolve({filter:/^openid-client$/},()=>({path:'/app/node_modules/openid-client/build/index.js',external:true}));
    build.onResolve({filter:/\.(mjs|js)$/},args=>{if(!args.importer)return;const path=resolve(dirname(args.importer),args.path);if(exactRoot.has(path))return{path:exactRoot.get(path),external:true};});}}]});
const sha=bytes=>createHash('sha256').update(bytes).digest('hex'),sourceFiles=[];
for(const name of Object.keys(result.metafile.inputs).sort()){const file=resolve(name);sourceFiles.push({path:file.slice(root.length+1).replaceAll('\\','/'),sha256:sha(await readFile(file))});}
const fixtureSha256=sha(await readFile(join(directory,'fixture.mjs'))),manifest={schema:'soty.source-feedback-linux-packet.v1',nonce,image:SYNTHETIC_FEEDBACK_IMAGE,fixtureSha256,
  sourceFiles,externalRootImports:[...exactRoot.values(),'/app/node_modules/openid-client/build/index.js'],synthetic:true,models:false,productionReady:false};
await writeFile(join(directory,'manifest.json'),JSON.stringify(manifest,null,2)+'\n');process.stdout.write(JSON.stringify({directory,nonce,fixtureSha256,manifestSha256:sha(await readFile(join(directory,'manifest.json'))),sourceFiles:sourceFiles.length})+'\n');
