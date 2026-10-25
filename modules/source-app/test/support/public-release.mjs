import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {resolve,dirname,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {buildSourceAppPackage} from '../../scripts/build.mjs';
import {SOURCE_APP_COPY_FILES,SOURCE_SDK_FILES,SOURCE_SDK_TEXT_FILES} from '../../package-contract.mjs';
import {createSourceSdkArchive,sourceSdkHash,sourceSdkRuntimeLock} from '../../public-release.mjs';

const root=resolve(dirname(fileURLToPath(import.meta.url)),'../../../..'),json=value=>Buffer.from(JSON.stringify(value,null,2)+'\n');
const canonical=bytes=>Buffer.from(new TextDecoder('utf-8',{fatal:true}).decode(bytes).replaceAll('\r\n','\n'));

/** Synthetic provenance label for FORMAT tests only. The separate producer
 * acceptance uses a real frozen Git commit and delivered public archive. */
export async function sdkFormatFixture(directory){
  const sourceCommit='a'.repeat(40),built=await buildSourceAppPackage({outdir:join(directory,'build')});
  const original=JSON.parse(await readFile(join(root,'modules/source-app/package.json'),'utf8'));
  const pkg=Object.fromEntries(['name','version','type','bin','engines','exports','dependencies'].map(key=>[key,original[key]]));pkg.files=SOURCE_SDK_FILES;
  const lock=sourceSdkRuntimeLock(pkg,await readFile(join(root,'pnpm-lock.yaml'))),lockBytes=json(lock),files=new Map([['package.json',json(pkg)],['package-lock.json',lockBytes],['npm-shrinkwrap.json',lockBytes]]);
  const sources=new Map();
  for(const item of built.sourceFiles){const bytes=canonical(await readFile(join(root,item.path)));sources.set(item.path,bytes);item.sha256=sourceSdkHash(bytes);}
  built.sourceCommit=sourceCommit;
  for(const item of built.outputs){let bytes=await readFile(join(directory,'build',item.path));if(Object.hasOwn(SOURCE_APP_COPY_FILES,item.path))bytes=canonical(bytes);
    files.set('dist/'+item.path,bytes);item.sha256=sourceSdkHash(bytes);}
  files.set('dist/provenance.json',json(built));
  for(const name of SOURCE_SDK_TEXT_FILES)files.set(name,canonical(await readFile(join(root,'modules/source-app',name))));
  for(const name of ['pnpm-lock.yaml','scripts/build-source-sdk-release.mjs','deploy/connect/gpg-tar.mjs','modules/source-app/scripts/build.mjs',
    'modules/source-app/public-release.mjs',...SOURCE_SDK_TEXT_FILES.map(name=>'modules/source-app/'+name)])sources.set(name,canonical(await readFile(join(root,name))));
  function archive(){const capsule={schema:'soty.source-sdk.capsule.v1',sourceCommit,packageVersion:pkg.version,
    files:[...files].filter(([path])=>path!=='SDK-CAPSULE.json').map(([path,bytes])=>({path,sha256:sourceSdkHash(bytes),bytes:bytes.length})).sort((a,b)=>a.path<b.path?-1:1),
    sourceFiles:[...sources].map(([path,bytes])=>({path,sha256:sourceSdkHash(bytes)})).sort((a,b)=>a.path<b.path?-1:1),dependencyLockDigest:sourceSdkHash(files.get('npm-shrinkwrap.json'))};
    files.set('SDK-CAPSULE.json',json(capsule));return createSourceSdkArchive(files);}
  async function context(destination){for(const [name,bytes]of sources){await mkdir(dirname(join(destination,name)),{recursive:true});await writeFile(join(destination,name),bytes);}}
  return{files,sources,archive,context,json,pkg,lock};
}
