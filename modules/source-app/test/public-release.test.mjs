import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm,writeFile,mkdir,readFile,symlink} from 'node:fs/promises';
import {join,resolve,dirname} from 'node:path';
import {tmpdir} from 'node:os';
import {gzipSync,gunzipSync} from 'node:zlib';
import {SOURCE_SDK_FILES,SOURCE_SDK_LIMITS} from '../package-contract.mjs';
import {readSourceSdkArchive,sourceSdkReleaseIndex,verifySourceSdkIndex,verifySourceSdkContext,verifyPublicSourceSdk,sourceSdkRuntimeLock} from '../public-release.mjs';
import {sdkFormatFixture} from './support/public-release.mjs';

async function fixture(t){const directory=await mkdtemp(join(tmpdir(),'sdk-public-unit-'));
  t.after(async()=>{assert.equal(dirname(resolve(directory)),resolve(tmpdir()));await rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:30});});
  return{directory,...await sdkFormatFixture(directory)};}
function mutateHeader(archive,mutation){const plain=gunzipSync(archive);for(let at=0;at<plain.length;){const header=plain.subarray(at,at+512);
  if(header.every(byte=>byte===0))break;const name=header.subarray(0,100).toString('ascii').replace(/\0.*$/su,''),size=parseInt(header.subarray(124,136).toString('ascii').replace(/\0.*$/su,''),8);
  if(name==='package/README.md'){mutation(header);header.fill(32,148,156);const sum=header.reduce((a,b)=>a+b,0);header.write(sum.toString(8).padStart(6,'0')+'\0 ',148,'ascii');return gzipSync(plain);}
  at+=512+Math.ceil(size/512)*512;}throw Error('fixture_header_missing');}

test('finite SDK format is deterministic and contains runtime, types, docs, templates and optional Python without private neighbours',async t=>{
  const f=await fixture(t),archive=f.archive(),value=readSourceSdkArchive(archive);assert.deepEqual([...value.files.keys()].sort(),SOURCE_SDK_FILES);
  assert.deepEqual(archive,f.archive());assert.equal(value.package.version,f.pkg.version);
  assert.equal(value.files.has('dist/python/soty_native_entry.py'),true);assert.equal(value.files.has('dist/templates/reading-list/domain.mjs'),true);
  assert.equal(value.package.scripts,undefined);assert.equal(value.package.devDependencies,undefined);
  const index=sourceSdkReleaseIndex(archive);assert.equal(index.productionReady,false);assert.equal(index.admission.maxProfiles,64);verifySourceSdkIndex(index,archive);
  assert.throws(()=>verifySourceSdkIndex({...index,sourceCommit:'b'.repeat(40)},archive));
  assert.throws(()=>verifySourceSdkIndex({...index,artifact:{...index.artifact,url:'/sdk/source-app/../private.tgz'}},archive));
});

test('links, traversal, private tar entries, missing files, malformed gzip and a compressed bomb are refused without extraction',async t=>{
  const f=await fixture(t),archive=f.archive();
  for(const name of ['package/private.key','package/install/operator.json','../.env']){
    const bad=mutateHeader(archive,header=>{header.fill(0,0,100);header.write(name,0,'ascii');});assert.throws(()=>readSourceSdkArchive(bad));
  }
  const link=mutateHeader(archive,header=>{header[156]='2'.charCodeAt(0);header.write('../../.env',157,'ascii');});assert.throws(()=>readSourceSdkArchive(link));
  assert.throws(()=>readSourceSdkArchive(Buffer.from('not gzip')));assert.throws(()=>readSourceSdkArchive(Buffer.alloc(SOURCE_SDK_LIMITS.archiveBytes+1)));
  assert.throws(()=>readSourceSdkArchive(gzipSync(Buffer.alloc(SOURCE_SDK_LIMITS.plainBytes+1))));
  f.files.delete('types/server.d.mts');assert.throws(()=>f.archive());
});

test('re-sealed metadata cannot add lifecycle code, unpinned production dependencies, private source paths or changed provenance',async t=>{
  const f=await fixture(t),original=new Map([...f.files].map(([name,bytes])=>[name,Buffer.from(bytes)]));
  function restore(){f.files.clear();for(const [name,bytes]of original)f.files.set(name,Buffer.from(bytes));}
  const pkg=JSON.parse(original.get('package.json'));f.files.set('package.json',f.json({...pkg,scripts:{postinstall:'synthetic-must-not-run'}}));assert.throws(()=>readSourceSdkArchive(f.archive()));restore();
  const lock=JSON.parse(original.get('npm-shrinkwrap.json'));lock.packages['node_modules/openid-client'].dependencies.jose='latest';
  f.files.set('npm-shrinkwrap.json',f.json(lock));assert.throws(()=>readSourceSdkArchive(f.archive()));restore();
  f.files.set('package-lock.json',Buffer.from('{}\n'));assert.throws(()=>readSourceSdkArchive(f.archive()),{code:'source_sdk_dependency_lock_changed'});restore();
  f.sources.set('modules/source-app/install/operator.json',Buffer.from('synthetic-private'));assert.throws(()=>readSourceSdkArchive(f.archive()));f.sources.delete('modules/source-app/install/operator.json');
  const proof=JSON.parse(original.get('dist/provenance.json'));proof.outputs[0].sha256='f'.repeat(64);f.files.set('dist/provenance.json',f.json(proof));assert.throws(()=>readSourceSdkArchive(f.archive()));
});

test('Gitless context verification checks actual canonical source bytes and the exact frozen production lock',async t=>{
  const f=await fixture(t),archive=f.archive(),context=join(f.directory,'gitless');await f.context(context);
  assert.equal(verifySourceSdkContext(context,archive).gitRequired,false);
  const path=join(context,'modules/source-app/server/bff.mjs');await writeFile(path,Buffer.concat([await readFile(path),Buffer.from('\n// changed\n')]));
  assert.throws(()=>verifySourceSdkContext(context,archive),{code:'source_sdk_context_changed'});
  const source=f.sources.get('modules/source-app/server/bff.mjs');await writeFile(path,source.toString('utf8').replaceAll('\n','\r\n'));
  assert.equal(verifySourceSdkContext(context,archive).ok,true,'Git text CRLF conversion is canonicalized; no Git command or fabricated current commit');
  const linked=join(context,'modules/source-app/server/wire.mjs');await rm(linked);await symlink(process.platform==='win32'?context:path,linked,process.platform==='win32'?'junction':'file');assert.throws(()=>verifySourceSdkContext(context,archive));
});

test('public index verifies in a Gitless tree; unknown files, symlinks and changed hash bytes cannot be published',async t=>{
  const f=await fixture(t),archive=f.archive(),index=sourceSdkReleaseIndex(archive),context=join(f.directory,'gitless');await f.context(context);
  const directory=join(context,'public/sdk/source-app');await mkdir(directory,{recursive:true});const file=join(directory,index.artifact.sha256+'.tgz');
  await writeFile(file,archive);await writeFile(join(directory,'index.json'),f.json(index));assert.equal(verifyPublicSourceSdk(context).ok,true);
  const privateFile=join(directory,'operator.json');await writeFile(privateFile,'synthetic-private');assert.throws(()=>verifyPublicSourceSdk(context),{code:'source_sdk_public_layout_invalid'});await rm(privateFile);
  await writeFile(file,Buffer.from('different'));assert.throws(()=>verifyPublicSourceSdk(context));await rm(file);
  await symlink(process.platform==='win32'?context:join(directory,'index.json'),file,process.platform==='win32'?'junction':'file');assert.throws(()=>verifyPublicSourceSdk(context));
});

test('runtime shrinkwrap derives only exact frozen openid-client and two leaves; duplicate or changed lock shapes fail closed',async t=>{
  const f=await fixture(t),bytes=f.sources.get('pnpm-lock.yaml'),lock=sourceSdkRuntimeLock(f.pkg,bytes);
  assert.equal(lock.packages['node_modules/openid-client'].version,'6.8.4');assert.equal(lock.packages['node_modules/jose'].version,'6.2.12');
  assert.equal(lock.packages['node_modules/oauth4webapi'].version,'3.8.8');assert.equal(Object.keys(lock.packages).length,4);
  for(const text of [bytes.toString().replace('lockfileVersion: \'9.0\'','lockfileVersion: \'future\''),
    bytes.toString().replace('  openid-client@6.8.4:\n    dependencies:\n      jose: 6.2.12','  openid-client@6.8.4:\n    dependencies:\n      jose: latest'),bytes.toString()+'\npackages:\n'])assert.throws(()=>sourceSdkRuntimeLock(f.pkg,Buffer.from(text)));
});
