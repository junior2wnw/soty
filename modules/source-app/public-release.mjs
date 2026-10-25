import {createHash} from 'node:crypto';
import {gzipSync,gunzipSync} from 'node:zlib';
import {lstatSync,readFileSync,realpathSync,readdirSync} from 'node:fs';
import {resolve,join,sep} from 'node:path';
import {TarInventory} from '../../deploy/connect/gpg-tar.mjs';
import {parseSourceJson} from './shared/strict-json.mjs';
import {SOURCE_APP_OUTPUTS,SOURCE_APP_COPY_FILES,SOURCE_SDK_TEXT_FILES,SOURCE_SDK_FILES,SOURCE_SDK_LIMITS,SOURCE_SDK_INDEX,sourceSdkArchiveName,sourceSdkVersion} from './package-contract.mjs';

const fail=code=>{throw Object.assign(new Error(code),{code});};
const need=(value,code='source_sdk_release_invalid')=>{if(!value)fail(code);};
export const sourceSdkHash=bytes=>createHash('sha256').update(bytes).digest('hex');
const hex=value=>typeof value==='string'&&/^[a-f0-9]{64}$/u.test(value);
const commit=value=>typeof value==='string'&&/^[a-f0-9]{40}$/u.test(value);
const exact=(value,keys)=>value&&typeof value==='object'&&!Array.isArray(value)
  &&Object.keys(value).sort().join(',')===[...keys].sort().join(',');
const parse=bytes=>parseSourceJson(new TextDecoder('utf-8',{fatal:true}).decode(bytes),{bytes:131072});
const canonicalText=bytes=>Buffer.from(new TextDecoder('utf-8',{fatal:true}).decode(bytes).replaceAll('\r\n','\n'));

/** Closed subset of the CURRENT pnpm v9 lock: openid-client and two leaves.
 * A different shape fails, rather than resolving a semver range or latest. */
export function sourceSdkRuntimeLock(pkg,bytes){
  need(pkg.name==='@soty/source-app'&&sourceSdkVersion(pkg.version)&&JSON.stringify(pkg.dependencies)==='{"openid-client":"6.8.4"}','source_sdk_package_invalid');
  const lines=new TextDecoder('utf-8',{fatal:true}).decode(bytes).replaceAll('\r\n','\n').split('\n');
  need(lines[0]==="lockfileVersion: '9.0'",'source_sdk_lock_shape_changed');
  function section(key){const positions=lines.flatMap((line,index)=>line===key+':'?[index]:[]);need(positions.length===1,'source_sdk_lock_shape_changed');
    let end=positions[0]+1;while(end<lines.length&&(!lines[end]||/^\s/u.test(lines[end])))end++;
    return lines.slice(positions[0]+1,end);}
  function entry(sectionName,key){const rows=section(sectionName),positions=rows.flatMap((line,index)=>line==='  '+key+':'||line==='  '+key+': {}'?[index]:[]);
    need(positions.length===1,'source_sdk_lock_shape_changed');let end=positions[0]+1;
    while(end<rows.length&&(!rows[end]||/^ {4}/u.test(rows[end])))end++;
    return{empty:rows[positions[0]].endsWith('{}'),rows:rows.slice(positions[0]+1,end).filter(line=>line.trim())};}
  const deps=entry('snapshots','openid-client@6.8.4');need(!deps.empty&&deps.rows.length===3&&deps.rows[0]==='    dependencies:','source_sdk_lock_shape_changed');
  const versions={};for(const line of deps.rows.slice(1)){const match=/^      (jose|oauth4webapi): ([0-9]+\.[0-9]+\.[0-9]+)$/u.exec(line);
    need(match&&!Object.hasOwn(versions,match[1]),'source_sdk_lock_shape_changed');versions[match[1]]=match[2];}
  need(Object.keys(versions).sort().join(',')==='jose,oauth4webapi','source_sdk_lock_shape_changed');
  const packages={'':{name:pkg.name,version:pkg.version,dependencies:pkg.dependencies,engines:pkg.engines}};
  for(const [name,version]of [['openid-client','6.8.4'],...Object.entries(versions).sort(([a],[b])=>a<b?-1:1)]){
    const value=entry('packages',name+'@'+version);need(!value.empty&&value.rows.length===1,'source_sdk_lock_shape_changed');
    const match=/^    resolution: \{integrity: (sha512-[A-Za-z0-9+/]{86}==)\}$/u.exec(value.rows[0]);need(match,'source_sdk_lock_shape_changed');
    if(name!=='openid-client')need(entry('snapshots',name+'@'+version).empty,'source_sdk_lock_shape_changed');
    packages['node_modules/'+name]={version,resolved:`https://registry.npmjs.org/${name}/-/${name}-${version}.tgz`,integrity:match[1],
      ...(name==='openid-client'?{dependencies:Object.fromEntries(Object.entries(versions).sort(([a],[b])=>a<b?-1:1))}:{})};
  }
  return{name:pkg.name,version:pkg.version,lockfileVersion:3,requires:true,packages};
}
const allowed=new Set(SOURCE_SDK_FILES);
const directories=new Set(['','package']);
for(const name of SOURCE_SDK_FILES){const parts=('package/'+name).split('/');while(parts.length>1){parts.pop();directories.add(parts.join('/'));}}
const cap={cipherBytes:SOURCE_SDK_LIMITS.archiveBytes,plainBytes:SOURCE_SDK_LIMITS.plainBytes,fileBytes:SOURCE_SDK_LIMITS.fileBytes,
  totalBytes:SOURCE_SDK_LIMITS.plainBytes,entries:SOURCE_SDK_LIMITS.files,pathBytes:16384,pathDepth:8,wallMs:10000};

const auxiliary=new Set(['pnpm-lock.yaml','scripts/build-source-sdk-release.mjs','deploy/connect/gpg-tar.mjs',
  'modules/app-contract/index.mjs','modules/app-contract/json.mjs','modules/app-contract/sdk.mjs',
  'modules/apps/scoped-embed/profile-dispatch.mjs','modules/apps/scoped-embed/profile.mjs',
  'modules/apps/scoped-embed/resource-profile.mjs','modules/apps/scoped-embed/resource-proof.mjs',
  'modules/apps/scoped-embed/resource-route-adapters.mjs','modules/apps/scoped-embed/source-authority-client.mjs',
  'modules/capabilities/server/validation.mjs','modules/feedback/server/media.mjs',
  'modules/source-rp/server/index.mjs','modules/source-rp/server/protocol.mjs','modules/source-rp/server/session-service.mjs']);
export function sourceSdkSourceName(name){
  return typeof name==='string'&&!name.split('/').some(part=>!part||part==='.'||part==='..')&&!name.includes('\\')
    &&(auxiliary.has(name)||name==='modules/source-app/package.json'||name==='modules/source-app/package-contract.mjs'
      ||name==='modules/source-app/public-release.mjs'||name==='modules/source-app/scripts/build.mjs'
      ||/^modules\/source-app\/(?:server|browser|shared)\/[a-z0-9-]+\.mjs$/u.test(name)
      ||/^modules\/source-app\/install\/[a-z0-9-]+\.mjs$/u.test(name)
      ||/^modules\/source-app\/examples\/(?:ordinary-app|reading-list)\/(?:assets\/)?[a-z0-9-]+\.(?:mjs|js|css|html)$/u.test(name)
      ||SOURCE_SDK_TEXT_FILES.some(file=>name==='modules/source-app/'+file)
      ||['modules/source-app/python/soty_native_entry.py','modules/source-app/python/requirements.txt'].includes(name));
}

/** Tiny deterministic USTAR writer for this fixed public file set only. The
 * independent maintained TarInventory validates it again before publication. */
export function createSourceSdkArchive(files){
  need(files instanceof Map&&[...files.keys()].sort().join('\n')===SOURCE_SDK_FILES.join('\n'));
  const blocks=[];
  const header=(name,size,type,mode)=>{const value=Buffer.alloc(512);
    need(Buffer.byteLength(name)<100);value.write(name,0,'ascii');
    const octal=(number,at,length)=>{const text=number.toString(8);need(text.length<length);value.write(text.padStart(length-1,'0')+'\0',at,'ascii');};
    octal(mode,100,8);octal(0,108,8);octal(0,116,8);octal(size,124,12);octal(0,136,12);value.fill(32,148,156);
    value[156]=type.charCodeAt(0);value.write('ustar\0',257,'ascii');value.write('00',263,'ascii');
    const sum=value.reduce((a,b)=>a+b,0);value.write(sum.toString(8).padStart(6,'0')+'\0 ',148,'ascii');return value;};
  for(const name of [...directories].sort())blocks.push(header(name?name+'/':'./',0,'5',0o755));
  let total=0;
  for(const [name,bytes]of [...files].sort(([a],[b])=>a<b?-1:a>b?1:0)){
    need(Buffer.isBuffer(bytes)&&bytes.length<=SOURCE_SDK_LIMITS.fileBytes);total+=bytes.length;need(total<=SOURCE_SDK_LIMITS.plainBytes);
    blocks.push(header('package/'+name,bytes.length,'0',name==='dist/install.mjs'?0o755:0o644),bytes,Buffer.alloc((512-bytes.length%512)%512));
  }
  blocks.push(Buffer.alloc(1024));const archive=gzipSync(Buffer.concat(blocks),{level:9});
  need(archive.length<=SOURCE_SDK_LIMITS.archiveBytes);return archive;
}

/** No extraction and no executable imports. Bounds apply BEFORE decompression
 * and the existing strict backup tar reader rejects links/traversal/duplicates. */
export function readSourceSdkArchive(archive){
  need(Buffer.isBuffer(archive)&&archive.length>0&&archive.length<=SOURCE_SDK_LIMITS.archiveBytes,'source_sdk_archive_limit');
  let plain;try{plain=gunzipSync(archive,{maxOutputLength:SOURCE_SDK_LIMITS.plainBytes});}catch{fail('source_sdk_archive_invalid');}
  const inventory=new TarInventory(cap);try{inventory.feed(plain);inventory.finish();}catch{fail('source_sdk_archive_invalid');}
  const files=new Map();
  for(let at=0;at<plain.length&&!plain.subarray(at,at+512).every(byte=>byte===0);){
    const header=plain.subarray(at,at+512),name=header.subarray(0,100).toString('ascii').replace(/\0.*$/su,''),type=String.fromCharCode(header[156]),size=parseInt(header.subarray(124,136).toString('ascii').replace(/\0.*$/su,'').trim(),8);
    need(['0','5'].includes(type)&&header.subarray(345,500).every(byte=>byte===0),'source_sdk_archive_invalid');
    if(type==='5')need(directories.has(name.replace(/\/$/u,'').replace(/^\.$/u,'')),'source_sdk_archive_entry_denied');
    else{need(name.startsWith('package/')&&allowed.has(name.slice(8))&&!files.has(name.slice(8)),'source_sdk_archive_entry_denied');
      files.set(name.slice(8),plain.subarray(at+512,at+512+size));}
    at+=512+Math.ceil(size/512)*512;
  }
  need([...files.keys()].sort().join('\n')===SOURCE_SDK_FILES.join('\n'),'source_sdk_archive_incomplete');
  const capsule=parse(files.get('SDK-CAPSULE.json'));
  need(exact(capsule,['schema','sourceCommit','packageVersion','files','sourceFiles','dependencyLockDigest'])
    &&capsule.schema==='soty.source-sdk.capsule.v1'&&commit(capsule.sourceCommit)&&sourceSdkVersion(capsule.packageVersion)
    &&hex(capsule.dependencyLockDigest)&&Array.isArray(capsule.files)&&capsule.files.length===SOURCE_SDK_FILES.length-1
    &&Array.isArray(capsule.sourceFiles)&&capsule.sourceFiles.length>0&&capsule.sourceFiles.length<=128);
  const seen=new Set();
  for(const item of capsule.files){need(exact(item,['path','sha256','bytes'])&&typeof item.path==='string'&&item.path!=='SDK-CAPSULE.json'
    &&files.has(item.path)&&!seen.has(item.path)&&hex(item.sha256)&&Number.isSafeInteger(item.bytes)
    &&item.bytes===files.get(item.path).length&&sourceSdkHash(files.get(item.path))===item.sha256);seen.add(item.path);}
  seen.clear();for(const item of capsule.sourceFiles){need(exact(item,['path','sha256'])&&sourceSdkSourceName(item.path)&&hex(item.sha256)&&!seen.has(item.path));seen.add(item.path);}
  for(const required of ['pnpm-lock.yaml','scripts/build-source-sdk-release.mjs','modules/source-app/package.json','modules/source-app/package-contract.mjs',
    'modules/source-app/public-release.mjs','modules/source-app/scripts/build.mjs',...SOURCE_SDK_TEXT_FILES.map(name=>'modules/source-app/'+name)])need(seen.has(required));
  const pkg=parse(files.get('package.json')),lock=parse(files.get('npm-shrinkwrap.json')),provenance=parse(files.get('dist/provenance.json'));
  need(exact(pkg,['name','version','type','bin','engines','exports','files','dependencies'])&&pkg.name==='@soty/source-app'&&pkg.version===capsule.packageVersion&&pkg.type==='module'
    &&exact(pkg.engines,['node'])&&pkg.engines.node==='>=24.15.0'&&exact(pkg.bin,['soty-source'])&&pkg.bin['soty-source']==='./dist/install.mjs'
    &&exact(pkg.exports,['./server','./browser','./example'])&&['server','browser','example'].every(name=>exact(pkg.exports['./'+name],['types','import'])
      &&pkg.exports['./'+name].types==='./types/'+name+'.d.mts'&&pkg.exports['./'+name].import==='./dist/'+name+'.mjs')
    &&JSON.stringify(pkg.files)===JSON.stringify(SOURCE_SDK_FILES)
    &&exact(pkg.dependencies,['openid-client'])&&pkg.dependencies['openid-client']==='6.8.4'&&pkg.scripts===undefined&&pkg.devDependencies===undefined);
  need(exact(lock,['name','version','lockfileVersion','requires','packages'])&&lock.name===pkg.name&&lock.version===pkg.version&&lock.lockfileVersion===3&&lock.requires===true
    &&exact(lock.packages,['','node_modules/openid-client','node_modules/jose','node_modules/oauth4webapi'])
    &&exact(lock.packages[''],['name','version','engines','dependencies'])&&lock.packages[''].name===pkg.name&&lock.packages[''].version===pkg.version
    &&JSON.stringify(lock.packages[''].engines)===JSON.stringify(pkg.engines)&&JSON.stringify(lock.packages[''].dependencies)===JSON.stringify(pkg.dependencies)
    &&sourceSdkHash(files.get('npm-shrinkwrap.json'))===capsule.dependencyLockDigest);
  for(const name of ['openid-client','jose','oauth4webapi']){const item=lock.packages['node_modules/'+name];
    need(exact(item,['version','resolved','integrity',...(name==='openid-client'?['dependencies']:[])])&&typeof item?.version==='string'&&/^\d+\.\d+\.\d+$/u.test(item.version)&&typeof item.integrity==='string'&&/^sha512-[A-Za-z0-9+/]{86}==$/u.test(item.integrity)
      &&item.resolved===`https://registry.npmjs.org/${name}/-/${name}-${item.version}.tgz`);}
  need(lock.packages['node_modules/openid-client'].version==='6.8.4'&&exact(lock.packages['node_modules/openid-client'].dependencies,['jose','oauth4webapi'])
    &&['jose','oauth4webapi'].every(name=>lock.packages['node_modules/openid-client'].dependencies[name]===lock.packages['node_modules/'+name].version));
  need(exact(provenance,['schema','packageVersion','esbuild','protocolDependency','sourceCommit','sourceFiles','outputs'])
    &&provenance.schema==='soty.source-app.build.v1'&&provenance.sourceCommit===capsule.sourceCommit&&provenance.packageVersion===pkg.version
    &&provenance.esbuild==='0.27.7'&&exact(provenance.protocolDependency,['name','version'])&&provenance.protocolDependency.name==='openid-client'&&provenance.protocolDependency.version==='6.8.4'
    &&Array.isArray(provenance.outputs)&&provenance.outputs.length===SOURCE_APP_OUTPUTS.length&&Array.isArray(provenance.sourceFiles));
  const sourceMap=new Map(capsule.sourceFiles.map(item=>[item.path,item.sha256]));
  for(const item of provenance.sourceFiles)need(exact(item,['path','sha256'])&&sourceSdkSourceName(item.path)&&sourceMap.get(item.path)===item.sha256);
  for(const name of SOURCE_SDK_TEXT_FILES)need(sourceMap.get('modules/source-app/'+name)===sourceSdkHash(files.get(name)));
  for(const [name,path]of Object.entries(SOURCE_APP_COPY_FILES))need(sourceMap.get('modules/source-app/'+path)===sourceSdkHash(files.get('dist/'+name)));
  seen.clear();for(const item of provenance.outputs){need(SOURCE_APP_OUTPUTS.includes(item.path)&&!seen.has(item.path)&&hex(item.sha256)
    &&sourceSdkHash(files.get('dist/'+item.path))===item.sha256);seen.add(item.path);}
  return{capsule,package:pkg,lock,provenance,files,sha256:sourceSdkHash(archive),bytes:archive.length};
}

export function sourceSdkReleaseIndex(archive){const value=readSourceSdkArchive(archive);return{
  schema:'soty.source-sdk.release.v1',status:'preview',package:{name:value.package.name,version:value.package.version,nodeEngine:value.package.engines.node,testedNode:'24.19.0'},
  sourceCommit:value.capsule.sourceCommit,artifact:{url:'/sdk/source-app/'+value.sha256+'.tgz',sha256:value.sha256,bytes:value.bytes},
  admission:{required:true,profileRegistry:'startup-static',maxProfiles:64},productionReady:false};}

export function verifySourceSdkIndex(index,archive){const expected=sourceSdkReleaseIndex(archive);
  need(JSON.stringify(index)===JSON.stringify(expected),'source_sdk_index_invalid');return expected;}

export function verifySourceSdkContext(root,archive){const value=readSourceSdkArchive(archive),base=resolve(root);
  for(const item of value.capsule.sourceFiles){const path=resolve(base,item.path);need(path.startsWith(base+sep)&&realpathSync(path)===path,'source_sdk_context_invalid');
    const stat=lstatSync(path);need(stat.isFile()&&!stat.isSymbolicLink()&&stat.size<=SOURCE_SDK_LIMITS.fileBytes,'source_sdk_context_invalid');
    need(sourceSdkHash(canonicalText(readFileSync(path)))===item.sha256,'source_sdk_context_changed');}
  const locked=sourceSdkRuntimeLock(value.package,readFileSync(join(base,'pnpm-lock.yaml')));
  need(JSON.stringify(locked)===JSON.stringify(value.lock),'source_sdk_dependency_lock_changed');
  need(JSON.parse(readFileSync(join(base,'modules/source-app/package.json'),'utf8')).version===value.package.version,'source_sdk_package_version_changed');
  return{ok:true,sourceCommit:value.capsule.sourceCommit,packageVersion:value.package.version,gitRequired:false,productionReady:false};}

export function verifyPublicSourceSdk(root,{requireContext=true}={}){
  const sdkRoot=join(resolve(root),'public/sdk'),directory=join(sdkRoot,'source-app');
  for(const current of [sdkRoot,directory]){const stamp=lstatSync(current);need(stamp.isDirectory()&&!stamp.isSymbolicLink()&&realpathSync(current)===current,'source_sdk_public_layout_invalid');}
  need(readdirSync(sdkRoot).join(',')==='source-app','source_sdk_public_layout_invalid');
  const names=readdirSync(directory);need(names.includes('index.json')&&names.length<=SOURCE_SDK_LIMITS.archives+1,'source_sdk_public_layout_invalid');
  for(const name of names){need(name==='index.json'||sourceSdkArchiveName('sdk/source-app/'+name),'source_sdk_public_layout_invalid');
    if(name==='index.json')continue;const file=join(directory,name),stamp=lstatSync(file);
    need(stamp.isFile()&&!stamp.isSymbolicLink()&&stamp.size<=SOURCE_SDK_LIMITS.archiveBytes,'source_sdk_public_layout_invalid');
    const bytes=readFileSync(file);need(name===sourceSdkHash(bytes)+'.tgz','source_sdk_archive_hash_mismatch');readSourceSdkArchive(bytes);}
  const path=join(resolve(root),'public',SOURCE_SDK_INDEX),stat=lstatSync(path);need(stat.isFile()&&!stat.isSymbolicLink()&&realpathSync(path)===path&&stat.size<=4096,'source_sdk_index_invalid');
  const index=parse(readFileSync(path));need(sourceSdkArchiveName(index?.artifact?.url?.slice(1))&&hex(index.artifact.sha256)
    &&index.artifact.url==='/sdk/source-app/'+index.artifact.sha256+'.tgz','source_sdk_index_invalid');
  const archivePath=join(resolve(root),'public',index.artifact.url.slice(1)),archiveStat=lstatSync(archivePath);
  need(archiveStat.isFile()&&!archiveStat.isSymbolicLink()&&realpathSync(archivePath)===archivePath&&archiveStat.size<=SOURCE_SDK_LIMITS.archiveBytes,'source_sdk_archive_limit');
  const archive=readFileSync(archivePath);verifySourceSdkIndex(index,archive);
  return requireContext?verifySourceSdkContext(root,archive):{ok:true,sourceCommit:index.sourceCommit,productionReady:false};
}
