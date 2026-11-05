import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {resolve,join,isAbsolute,dirname,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {verifyPublicSourceSdk,verifySourceSdkIndex,readSourceSdkArchive} from '../../../source-app/public-release.mjs';
import {createAuthorDraft} from '../../../app-contract/sdk.mjs';
import {readingListAssets} from './server.mjs';

const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const repo=fileURLToPath(new URL('../../../../',import.meta.url));
export async function prepareReadingListProject({directory,parentOrigin,port=53248,root=repo}){
 if(!isAbsolute(directory)||!Number.isInteger(port)||port<1024||port>65535)throw Error('reading_project_options_invalid');
 verifyPublicSourceSdk(root);
 const index=JSON.parse(await readFile(join(root,'public/sdk/source-app/index.json'),'utf8'));
 const packet=await readFile(join(root,'public',index.artifact.url.slice(1)));verifySourceSdkIndex(index,packet);
 const sdk=readSourceSdkArchive(packet),[glassCss,widgetContextScript]=await Promise.all([
  readFile(join(root,'vendor/optical-glass/src/ui-kit.css')),readFile(join(root,'vendor/optical-glass/src/widget-context.js'))]);
 await mkdir(directory,{mode:0o700});const sdkDirectory=join(directory,'.soty-sdk');await mkdir(sdkDirectory);
 for(const [name,bytes]of sdk.files){const path=resolve(sdkDirectory,name);
  const prefix=sdkDirectory+sep;
  if(!path.startsWith(prefix))throw Error('reading_sdk_path_invalid');await mkdir(dirname(path),{recursive:true});await writeFile(path,bytes,{flag:'wx'});}
 const assets=await readingListAssets({sdkDirectory,glassCss,widgetContextScript,parentOrigin});
 await mkdir(join(directory,'source'));await mkdir(join(directory,'source/assets'));await mkdir(join(directory,'.soty'));
 await writeFile(join(directory,'source/domain.mjs'),sdk.files.get('dist/templates/reading-list/domain.mjs'),{flag:'wx'});
 for(const [name,value]of Object.entries({document:assets.document,script:assets.script,style:assets.style}))await writeFile(join(directory,'source/assets',{document:'embed.html',script:'ui.js',style:'ui.css'}[name]),value,{flag:'wx'});
 await writeFile(join(directory,'.soty/author.json'),JSON.stringify(createAuthorDraft({title:'Список чтения'}),null,2)+'\n',{flag:'wx'});
 await writeFile(join(directory,'.soty/app.json'),JSON.stringify({schema:'soty.local-app.v1',name:'Список чтения',port,entryPath:'/embed'},null,2)+'\n',{flag:'wx'});
 await writeFile(join(directory,'operator.workspace.template.json'),sdk.files.get('install/operator.workspace.template.json'),{flag:'wx'});
 await writeFile(join(directory,'package.json'),JSON.stringify({name:'soty-reading-list-widget',private:true,type:'module',engines:{node:'>=24.15.0'},scripts:{
  check:'node .soty-sdk/dist/install.mjs check --config operator.private.json',init:'node .soty-sdk/dist/install.mjs init --config operator.private.json',start:'node .soty-sdk/dist/install.mjs serve --config operator.private.json'}},null,2)+'\n',{flag:'wx'});
 await writeFile(join(directory,'README.md'),await readFile(new URL('./README.md',import.meta.url)),{flag:'wx'});
 const provenance={schema:'soty.reading-widget.consumer.v1',sdk:{version:index.package.version,archiveSha256:sha(packet),sourceCommit:index.sourceCommit},
  domainSha256:sha(sdk.files.get('dist/templates/reading-list/domain.mjs')),glassCssSha256:sha(glassCss),glassWidgetContextSha256:sha(widgetContextScript),parentOrigin,port,entry:'/embed',grantsCreated:0,connected:false};
 await writeFile(join(directory,'consumer-provenance.json'),JSON.stringify(provenance,null,2)+'\n',{flag:'wx'});return provenance;
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const args=process.argv.slice(2);if(![4,6].includes(args.length)||args[0]!=='--out'||args[2]!=='--parent-origin'||args.length===6&&args[4]!=='--port')throw Error('usage: --out NEW_ABSOLUTE_DIRECTORY --parent-origin EXACT_ROOT_ORIGIN [--port PORT]');
 console.log(JSON.stringify(await prepareReadingListProject({directory:resolve(args[1]),parentOrigin:args[3],...(args[5]?{port:Number(args[5])}:{})})));
}
