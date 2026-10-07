import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {resolve,dirname,join,relative,isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash,randomBytes} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {createRequire} from 'node:module';
import {buildSourceAppPackage} from '../scripts/build.mjs';

const root=resolve(dirname(fileURLToPath(import.meta.url)),'../../..'),require=createRequire(import.meta.url);
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
export async function prepareSourceInstallationPacket(){
  const git=args=>execFileSync('git',['-c','safe.directory='+root,...args],{cwd:root,maxBuffer:4194304,windowsHide:true});
  const sourceCommit=git(['rev-parse','HEAD']).toString('ascii').trim();
  const tracked=git(['status','--porcelain','--untracked-files=all','--','modules/source-app']).toString('utf8');
  if(tracked.trim())throw Error('source_install_freeze_required');
  const nonce=randomBytes(16).toString('hex'),directory=join(root,'output','source-installation-'+nonce),dist=join(directory,'dist');
  await mkdir(dist,{recursive:true});const built=await buildSourceAppPackage({outdir:dist,sourceCommit});
  const frozen=new Map(built.sourceFiles.map(item=>[item.path,item.sha256]));
  function source(path){const name=relative(root,path).replaceAll('\\','/');
    if(name==='..'||name.startsWith('../')||isAbsolute(name))throw Error('source_install_build_path_invalid');
    const bytes=git(['show',sourceCommit+':'+name]);frozen.set(name,sha(bytes));return bytes;}
  const plugin={name:'canonical-frozen-reader',setup(build){build.onLoad({filter:/\.(mjs|js)$/},args=>
    ({contents:new TextDecoder('utf-8',{fatal:true}).decode(source(resolve(args.path))),loader:'js'}));}};
  let esbuild;try{esbuild=require('esbuild');}catch{esbuild=createRequire(require.resolve('vite'))('esbuild');}
  if(esbuild.version!=='0.27.7')throw Error('source_install_build_tool_required');
  for(const name of ['reader','reader2','image-guard'])await esbuild.build({entryPoints:[join(root,'modules/source-app/install',name==='image-guard'?name+'.mjs':name+'-cli.mjs')],
    outfile:join(dist,name+'.mjs'),bundle:true,platform:'node',format:'esm',target:'node24',logLevel:'silent',plugins:[plugin]});
  const publicFiles=['Dockerfile','README.md','operator.template.json','compose.template.yaml'];
  for(const name of publicFiles)await writeFile(join(directory,name),source(join(root,'modules/source-app/install',name)));
  const files=[...publicFiles,...built.outputs.map(output=>'dist/'+output.path),'dist/reader.mjs','dist/reader2.mjs','dist/image-guard.mjs','dist/provenance.json'];
  const artifacts=await Promise.all(files.map(async path=>({path,sha256:sha(await readFile(join(directory,path)))})));
  const manifest={schema:'soty.source-installation-packet.v1',nonce,sourceCommit,baseImage:'sha256:8fd1a16e5239acbe8be0377489a9cd0cac7cf56e73c1be8e5f6a4762bc9e7725',
    baseRevision:'b02f6517346c2275462060b84b3de0fd75bd30aa',protocol:{name:'openid-client',version:'6.8.4'},nativeReader:3,legacyReader:2,
    artifacts,sourceFiles:[...frozen].sort(([a],[b])=>a.localeCompare(b)).map(([path,sha256])=>({path,sha256})),
    authentication:'Basic300',models:false,longReady:false,productionReady:false};
  await writeFile(join(directory,'manifest.json'),JSON.stringify(manifest,null,2)+'\n');
  return{directory,nonce,sourceCommit,manifestSha256:sha(await readFile(join(directory,'manifest.json'))),artifacts:artifacts.length,productionReady:false};
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url))console.log(JSON.stringify(await prepareSourceInstallationPacket()));
