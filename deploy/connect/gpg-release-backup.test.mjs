import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { spawn } from 'node:child_process';
import { mkdtemp,mkdir,writeFile,readFile,rm,lstat,symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { TarInventory,encryptTar,verifyTar,sha,keyBytes } from './gpg-tar.mjs';
import { backupStopped,restoreIsolated } from './gpg-release-backup.mjs';
import { listAllContainerMounts } from './gpg-container-inventory.mjs';

const KEY=Buffer.from('A'.repeat(64)),IMAGE='sha256:'+'1'.repeat(64),ID='2'.repeat(32),CONTAINER='3'.repeat(64);
const linux={skip:process.platform!=='linux',timeout:180000};

// Independent USTAR fixtures use the documented fixed header offsets. They do
// not call the production tar encoder; malformed archives can be GPG authentic.
function head(name,type,size=0,mode=0o600){
  const h=Buffer.alloc(512);h.write(name,0,100,'utf8');
  for(const [at,len,n] of [[100,8,mode],[108,8,0],[116,8,0],[124,12,size],[136,12,1]])h.write(n.toString(8).padStart(len-1,'0')+'\0',at,len,'ascii');
  h.fill(32,148,156);h[156]=type.charCodeAt(0);h.write('ustar\0',257);h.write('00',263);
  h.write(h.reduce((a,b)=>a+b,0).toString(8).padStart(6,'0')+'\0 ',148,8,'ascii');
  return h;
}
function tar(entries=[{name:'data.bin',bytes:Buffer.from('synthetic bytes')}]){
  const chunks=[head('./','5',0,0o700)];
  for(const e of entries){const bytes=e.bytes??Buffer.alloc(0);chunks.push(head(e.name,e.type??'0',bytes.length,e.mode??0o600),bytes,Buffer.alloc((512-bytes.length%512)%512));}
  chunks.push(Buffer.alloc(1024));return Buffer.concat(chunks);
}
function inventory(bytes,cap={}){const x=new TarInventory(cap);for(let at=0;at<bytes.length;at+=173)x.feed(bytes.subarray(at,at+173));return x.finish();}
function pax(k,v){const body=k+'='+v+'\n';let n=Buffer.byteLength(body)+2;for(;;){const s=n+' '+body;if(Buffer.byteLength(s)===n)return Buffer.from(s);n=Buffer.byteLength(s);}}
function withPax(record,name='data.bin'){
  const base=tar([]).subarray(0,512),bytes=Buffer.from('x');
  return Buffer.concat([base,head('pax','x',record.length),record,Buffer.alloc((512-record.length%512)%512),head(name,'0',1),bytes,Buffer.alloc(511),Buffer.alloc(1024)]);
}
async function child(args,{input,cwd,stdio=['pipe','pipe','pipe','pipe']}={}){
  const c=spawn(args[0],args.slice(1),{cwd,stdio});const pieces=[];
  c.stdout.on('data',b=>pieces.push(b));c.stderr?.resume();
  const done=new Promise((resolve,reject)=>{c.once('error',reject);c.once('close',n=>n===0?resolve():reject(Error('synthetic_command_failed')));});
  c.stdin?.on('error',()=>{});c.stdio[3]?.on('error',()=>{});
  if(c.stdio[3])c.stdio[3].end(Buffer.concat([KEY,Buffer.from('\n')]));
  c.stdin?.end(input);const timer=setTimeout(()=>c.kill('SIGKILL'),30000);
  try{await done;return Buffer.concat(pieces);}finally{clearTimeout(timer);}
}
async function ownedFixture(t){const dir=await mkdtemp(path.join(tmpdir(),'soty-gpg-synthetic-'));t.after(()=>rm(dir,{recursive:true,force:true}));return dir;}
async function rawCipher(t,bytes){
  const dir=await mkdtemp(path.join(tmpdir(),'soty-gpg-synthetic-')),home=path.join(dir,'gpg');await mkdir(home,{mode:0o700});
  t.after(async()=>{await child(['/usr/bin/gpgconf','--homedir',home,'--kill','gpg-agent'],{stdio:['ignore','pipe','ignore']}).catch(()=>{});await rm(dir,{recursive:true,force:true});});
  const cipher=await child(['/usr/bin/gpg','--no-options','--homedir',home,'--batch','--yes','--pinentry-mode','loopback','--no-symkey-cache','--passphrase-fd','3','--symmetric','--cipher-algo','AES256','--force-mdc','--compress-algo','none','--s2k-count','1024','--output','-'],{input:bytes});
  const file=path.join(dir,'fixture.gpg');await writeFile(file,cipher,{mode:0o600});return {file,cipher,expectedSha256:sha(cipher)};
}

test('standard NUL-space checksum, chunk boundaries and empty file are accepted',()=>{
  const p=inventory(tar([{name:'private.bin',bytes:Buffer.from('value')},{name:'empty.bin'}]));
  assert.equal(p.entries,3);assert.equal(p.fileBytes,5);
  assert.match(p.inventorySha256,/^[a-f0-9]{64}$/);assert.equal(JSON.stringify(p).includes('private.bin'),false);
  assert.notEqual(p.inventorySha256,inventory(tar([{name:'private.bin',bytes:Buffer.from('other')},{name:'empty.bin'}])).inventorySha256);
});
test('key validation refuses non-ASCII bytes masked by ASCII decoding',()=>{
  assert.equal(keyBytes(KEY),KEY);
  assert.throws(()=>keyBytes(Buffer.alloc(64,0xc1)),{code:'backup_key_invalid'});
  assert.throws(()=>keyBytes(Buffer.from('A'.repeat(63))),{code:'backup_key_invalid'});
});
for(const [label,name] of [['parent escape','../escape'],['absolute path','/escape'],['drive path','C:/escape'],['backslash','a\\b'],['interior empty path','a//b'],['control character','a\nb']]){
  test('tar refuses '+label,()=>assert.throws(()=>inventory(tar([{name}]))));
}
for(const type of ['1','2','3','4','6','7','g','L','K','S'])test('tar refuses unsupported entry type '+type,()=>assert.throws(()=>inventory(tar([{name:'entry',type}]))));
test('tar refuses missing root, duplicates, file parent, special permissions and bad checksum',()=>{
  assert.throws(()=>inventory(tar().subarray(512)));
  assert.throws(()=>inventory(tar([{name:'same'},{name:'./same'}])));
  assert.throws(()=>inventory(tar([{name:'parent'},{name:'parent/child'}])));
  assert.throws(()=>inventory(tar([{name:'file',mode:0o4600}])));
  const corrupt=tar();corrupt[0]^=1;assert.throws(()=>inventory(corrupt));
});
test('PAX path is applied, unknown keys and duplicate shadow values refuse',()=>{
  assert.equal(inventory(withPax(pax('path','valid'))).entries,2);
  assert.throws(()=>inventory(withPax(pax('path','../outside'))));
  assert.throws(()=>inventory(withPax(pax('GNU.sparse.map','0,1'))));
  assert.throws(()=>inventory(withPax(Buffer.concat([pax('path','one'),pax('path','two')]))));
  assert.throws(()=>inventory(withPax(pax('uid','-1'))));
});
test('tar detects truncation, nonzero trailing data and bounded totals/entries/depth',()=>{
  assert.throws(()=>inventory(tar().subarray(0,-1)));
  assert.throws(()=>inventory(Buffer.concat([tar(),Buffer.alloc(512,1)])));
  assert.throws(()=>inventory(tar(),{fileBytes:1}));
  assert.throws(()=>inventory(tar(),{totalBytes:1}));
  assert.throws(()=>inventory(tar(),{plainBytes:511}));
  assert.throws(()=>inventory(tar(),{entries:1}));
  assert.throws(()=>inventory(tar([{name:'one',type:'5'},{name:'one/two'}]),{pathDepth:1}));
});
test('actual GNU POSIX tar roundtrip supports UTF8 and long names',linux,async t=>{
  const dir=await ownedFixture(t),files=path.join(dir,'files');await mkdir(files,{mode:0o700});
  const long='д'.repeat(80)+'.bin';await writeFile(path.join(files,long),'synthetic');
  const bytes=await child(['/usr/bin/tar','--format=posix','--pax-option=delete=atime,delete=ctime','-C',files,'-cf','-','.'],{stdio:['ignore','pipe','pipe']});
  assert.equal(inventory(bytes).entries,2);
});
test('actual GPG authenticated encrypted stream roundtrip, no plaintext archive',linux,async t=>{
  const dir=await ownedFixture(t),file=path.join(dir,'only.gpg'),bytes=tar();
  const encrypted=await encryptTar({file,key:KEY,input:Readable.from([bytes])});
  const proof=await verifyTar({file,key:KEY,expectedSha256:encrypted.sha256});
  assert.equal(proof.authenticated,true);assert.equal(proof.plainSha256,sha(bytes));
  assert.equal(proof.inventorySha256,inventory(bytes).inventorySha256);
  assert.equal((await lstat(file)).mode&0o777,0o600);
  const cipher=await readFile(file);assert.equal(cipher.includes(Buffer.from('synthetic bytes')),false);
});
test('actual GPG baseline accepts independently encrypted tar',linux,async t=>{
  const artifact=await rawCipher(t,tar());
  assert.equal((await verifyTar({...artifact,key:KEY})).authenticated,true);
});
for(const kind of ['wrong key','modified ciphertext','truncated ciphertext','authenticated traversal','authenticated symlink','authenticated duplicate','authenticated oversized entry']){
  test('no Docker call before authentication/validation: '+kind,linux,async t=>{
    const bytes=kind==='authenticated traversal'?tar([{name:'../outside'}]):kind==='authenticated symlink'?tar([{name:'link',type:'2'}]):kind==='authenticated duplicate'?tar([{name:'same'},{name:'same'}]):tar();
    const artifact=await rawCipher(t,bytes);let key=KEY;
    if(kind==='wrong key')key=Buffer.from('B'.repeat(64));
    else if(kind==='modified ciphertext'){artifact.cipher[artifact.cipher.length-8]^=1;await writeFile(artifact.file,artifact.cipher);artifact.expectedSha256=sha(artifact.cipher);}
    else if(kind==='truncated ciphertext'){artifact.cipher=artifact.cipher.subarray(0,-8);await writeFile(artifact.file,artifact.cipher);artifact.expectedSha256=sha(artifact.cipher);}
    const calls=[],engine=new Proxy({},{get:(_target,method)=>async()=>{calls.push(method);throw Error('engine_must_not_be_called');}});
    await assert.rejects(restoreIsolated({...artifact,key,image:IMAGE,operationId:ID,cap:kind==='authenticated oversized entry'?{fileBytes:1}:{}},engine));
    assert.equal(calls.length,0);
  });
}
test('valid authentic archive reaches target fence, existing target gets no CREATE',linux,async t=>{
  const artifact=await rawCipher(t,tar()),calls=[];
  const engine={image:async()=>({Id:IMAGE}),request:async(method,url)=>{calls.push(method);return {Name:'already exists'};}};
  await assert.rejects(restoreIsolated({...artifact,key:KEY,image:IMAGE,operationId:ID},engine),{code:'restore_target_exists'});
  assert.deepEqual(calls,['GET']);
});
for(const kind of ['lost ownership label','competing volume mount','competing bind alias']){
  test('new target ownership fence refuses before extraction helper creation: '+kind,linux,async t=>{
    const artifact=await rawCipher(t,tar()),calls=[];let volume;
    const engine={
      image:async()=>({Id:IMAGE}),
      inspect:async()=>{throw Object.assign(Error('absent'),{code:'engine_http_404'});},
      create:async()=>{calls.push('CREATE_CONTAINER');throw Error('must_not_create');},
      request:async(method,url,body)=>{
        if(method==='POST'){calls.push('CREATE_VOLUME');volume={...body,Scope:'local',Mountpoint:'/synthetic/new-restore'};return volume;}
        if(url.startsWith('/volumes/')){
          if(!volume)throw Object.assign(Error('absent'),{code:'engine_http_404'});
          if(kind==='lost ownership label'){const labels={...volume.Labels};delete labels['io.soty.isolated-restore.owner'];return {...volume,Labels:labels};}
          return volume;
        }
        const mount=kind==='competing volume mount'?{Type:'volume',Name:volume.Name,Source:volume.Mountpoint}:kind==='competing bind alias'?{Type:'bind',Source:'/synthetic'}:null;
        return mount?[{Id:'4'.repeat(64),Mounts:[mount]}]:[];
      }
    };
    await assert.rejects(restoreIsolated({...artifact,key:KEY,image:IMAGE,operationId:ID},engine),{code:'restore_incomplete_quarantined'});
    assert.deepEqual(calls,['CREATE_VOLUME']);
  });
}
for(const kind of ['volume-name writer','bind alias writer','ancestor bind writer']){
  test('stopped source backup refuses '+kind+' before filesystem/CLI writes',async()=>{
    const mount={Type:'volume',Name:'synthetic-volume',Source:'/synthetic/data',Destination:'/data',RW:true};
    const original={Id:CONTAINER,Image:IMAGE,State:{Running:false},Config:{Env:['DATA_DIR=/data']},Mounts:[mount]};
    const writer=kind==='volume-name writer'?{...mount}:kind==='bind alias writer'?{Type:'bind',Source:'/synthetic/data',RW:true}:{Type:'bind',Source:'/synthetic',RW:true};
    const engine={inspect:async()=>original,request:async(method,url)=>url.startsWith('/volumes/')?{Name:mount.Name,Driver:'local',Scope:'local',Mountpoint:mount.Source}:([{Id:'4'.repeat(64),Mounts:[writer]}])};
    await assert.rejects(backupStopped({containerId:CONTAINER,image:IMAGE,operationId:ID,directory:'/must-not-create',key:KEY},engine),{code:'backup_other_writer_running'});
  });
}
test('artifact directory cannot write inside original data or bind paths',linux,async t=>{
  const dir=await ownedFixture(t),data=path.join(dir,'data'),bind=path.join(dir,'bind');
  await mkdir(data);await mkdir(bind);
  const mount={Type:'volume',Name:'synthetic-volume',Source:data,Destination:'/data',RW:true};
  const original={Id:CONTAINER,Image:IMAGE,State:{Running:false},Config:{Env:['DATA_DIR=/data']},Mounts:[mount,{Type:'bind',Source:bind,Destination:'/run/synthetic',RW:false}]};
  const engine={inspect:async()=>original,image:async()=>({Id:IMAGE}),request:async(method,url)=>url.startsWith('/volumes/')?{Name:mount.Name,Driver:'local',Scope:'local',Mountpoint:data}:[]};
  for(const source of [data,bind]){
    const target=path.join(source,'must-not-create');
    await assert.rejects(backupStopped({containerId:CONTAINER,image:IMAGE,operationId:ID,directory:target,key:KEY},engine),{code:'backup_directory_overlaps_source'});
    await assert.rejects(lstat(target),{code:'ENOENT'});
  }
});
test('artifact directory refuses symlinked parent before creation',linux,async t=>{
  const dir=await ownedFixture(t),data=path.join(dir,'data'),real=path.join(dir,'real'),alias=path.join(dir,'alias');
  await mkdir(data);await mkdir(real);await symlink(real,alias);
  const mount={Type:'volume',Name:'synthetic-volume',Source:data,Destination:'/data',RW:true};
  const original={Id:CONTAINER,Image:IMAGE,State:{Running:false},Config:{Env:['DATA_DIR=/data']},Mounts:[mount]};
  const engine={inspect:async()=>original,image:async()=>({Id:IMAGE}),request:async(method,url)=>url.startsWith('/volumes/')?{Name:mount.Name,Driver:'local',Scope:'local',Mountpoint:data}:[]};
  await assert.rejects(backupStopped({containerId:CONTAINER,image:IMAGE,operationId:ID,directory:path.join(alias,'must-not-create'),key:KEY},engine),{code:'backup_directory_invalid'});
  await assert.rejects(lstat(path.join(real,'must-not-create')),{code:'ENOENT'});
});
test('additional unarchived volume cannot disappear into a successful backup',async()=>{
  const original={Id:CONTAINER,Image:IMAGE,State:{Running:false},Config:{Env:['DATA_DIR=/data']},Mounts:[{Type:'volume',Destination:'/data',RW:true},{Type:'volume',Destination:'/other',RW:true}]};
  await assert.rejects(backupStopped({containerId:CONTAINER,image:IMAGE,operationId:ID,directory:'/must-not-create',key:KEY},{inspect:async()=>original}),{code:'backup_volume_profile_invalid'});
});

function checksum(header){header.fill(32,148,156);header.write(header.reduce((a,b)=>a+b,0).toString(8).padStart(6,'0')+'\0 ',148,8,'ascii');}
function placeholder(bytes,offset,raw,prefix=Buffer.alloc(0)){
  assert.ok(raw.length<=100&&prefix.length<=155);
  const copy=Buffer.from(bytes),h=copy.subarray(offset,offset+512);
  h.fill(0,0,100);h.fill(0,345,500);raw.copy(h,0);prefix.copy(h,345);checksum(h);return copy;
}
const fileHeaderOffset=record=>1024+Math.ceil(record.length/512)*512;

test('PAX metadata placeholder truncated inside UTF8 is not an extracted path',()=>{
  const record=pax('path','д'.repeat(80)+'.bin'),base=withPax(record);
  const truncated=Buffer.concat([Buffer.from('x'.repeat(99)),Buffer.from([0xd0])]);
  const changed=placeholder(base,512,truncated,Buffer.from([0xd0]));
  assert.equal(inventory(changed).inventorySha256,inventory(base).inventorySha256);
});
test('validated PAX path supersedes only the unconsumed USTAR name and prefix',()=>{
  const record=pax('path','valid.bin'),base=withPax(record),offset=fileHeaderOffset(record);
  for(const [raw,prefix] of [[Buffer.from([0xd0]),Buffer.from([0xd0])],[Buffer.from('../ignored'),Buffer.from('/ignored')]]){
    assert.equal(inventory(placeholder(base,offset,raw,prefix)).inventorySha256,inventory(base).inventorySha256);
  }
});
test('USTAR UTF8 and safe-path validation stays strict without an authoritative PAX path',()=>{
  const record=pax('mtime','1'),base=withPax(record),offset=fileHeaderOffset(record);
  assert.throws(()=>inventory(placeholder(base,offset,Buffer.from([0xd0]))));
  assert.throws(()=>inventory(placeholder(base,offset,Buffer.from('valid'),Buffer.from([0xd0]))));
  assert.throws(()=>inventory(placeholder(base,offset,Buffer.from('../outside'))));
});
test('unsafe or malformed authoritative PAX path refuses despite safe USTAR fallback',()=>{
  for(const actual of ['../outside','/outside','C:/outside','a\\b','bad\0value'])assert.throws(()=>inventory(withPax(pax('path',actual),'safe')));
  const invalid=pax('path','valid');invalid[invalid.indexOf(Buffer.from('valid'))]=0xd0;
  assert.throws(()=>inventory(withPax(invalid,'safe')));
});
test('ignored PAX placeholder does not waive checksum, magic or link admission',()=>{
  const base=withPax(pax('path','valid'));
  const badChecksum=Buffer.from(base);badChecksum[512]^=1;assert.throws(()=>inventory(badChecksum));
  for(const at of [257,157]){const invalid=Buffer.from(base),h=invalid.subarray(512,1024);h[at]=1;checksum(h);assert.throws(()=>inventory(invalid));}
});

// Maintained copy of the sealed independent three actual-builder ports.
{
function span(source, start, end) {
  const lo=source.indexOf(start), hi=source.indexOf(end,lo+start.length);
  assert.ok(lo>=0 && hi>lo, 'Actual Source anchors must exist');
  return source.slice(lo,hi);
}
const bad=code=>{throw Object.assign(Error(code),{code});};
const IMAGE='sha256:'+'1'.repeat(64), SOURCE='2'.repeat(64), OPERATION='3'.repeat(32);
function dockerLogDriver(args) {
  const stop=args.indexOf(IMAGE);assert.ok(stop>0, 'Pinned image must follow Docker run options');
  let driver;
  for(let i=0;i<stop;i++) {
    if(args[i]==='--log-driver')driver=args[++i];
    else if(args[i].startsWith('--log-driver='))driver=args[i].slice('--log-driver='.length);
  }
  return driver;
}

async function registerBackupLoggingRegressions({candidateFile,legacyArgsFile}) {
  const source=await readFile(candidateFile,'utf8'), legacy=await readFile(legacyArgsFile,'utf8');
  const archiveArgsSource=span(legacy,'export function archiveHelperArgs(', '\nexport async function encryptBackup(').replace(/^export /,'');
  const archiveHelperArgs=new Function(archiveArgsSource+'\nreturn archiveHelperArgs;')();
  const readonlySource=span(source,'async function readonlyArchive(', '\nfunction sourceState(');
  // Capture actual argv without spawning a process. Parent code is unchanged.
  for(const phase of ['volume','readback'])test('BK-LOG-01 readonly '+phase+' explicitly disables Docker plaintext logging',async()=>{
    let argv,spawns=0;
    const run=new Function('archiveHelperArgs','absentContainer','processDocker','stopOwned','HELPER',readonlySource+'\nreturn readonlyArchive;')(
      archiveHelperArgs,async()=>{},args=>{spawns++;argv=[...args];return {child:{stdin:{end(){}},kill(){}},done:Promise.resolve()};},async()=>{},'io.soty.backup.helper');
    const running=await run({},SOURCE,OPERATION,phase,IMAGE,{wallMs:1000},'4'.repeat(32));
    await running.cleanup();
    assert.equal(spawns,1,'Reach actual archive argv builder');
    assert.equal(dockerLogDriver(argv),'none','Plaintext tar stdout must never enter a Docker logging driver');
    assert.ok(argv.includes('--read-only'));
    assert.ok(argv.includes(SOURCE+':ro'));
  });
  test('BK-LOG-01 extraction CREATE explicitly disables Docker logs after authenticated first pass',async()=>{
    const constants=span(source,'const ID=', '\nfunction processDocker(');
    const fences=span(source,'async function absentContainer(', '\nasync function stopOwned(');
    const restoreSource=span(source,'export async function restoreIsolated(', '\nasync function stdinKey(').replace(/^export /,'');
    const actions=[];let volume,config,creates=0;
    // The trusted completed-auth port only lets this test reach actual CREATE
    // construction. It does not test real GPG or bypass production validation.
    const verifyTar=async()=>{actions.push('AUTH_COMPLETE');return {plainSha256:'5'.repeat(64),inventorySha256:'6'.repeat(64),entries:1,fileBytes:0};};
    const restore=new Function('path','bad','limits','verifyTar','randomBytes','listAllContainerMounts',constants+'\n'+fences+'\n'+restoreSource+'\nreturn restoreIsolated;')(
      path,bad,()=>({}),verifyTar,()=>Buffer.alloc(16,4),listAllContainerMounts);
    const engine={
      image:async image=>({Id:image}),
      inspect:async()=>{throw Object.assign(Error('absent'),{code:'engine_http_404'});},
      request:async(method,url,body)=>{
        if(method==='POST'&&url==='/volumes/create'){
          actions.push('CREATE_VOLUME');volume={...body,Scope:'local',Mountpoint:'/synthetic/new-restore'};return volume;
        }
        if(method==='GET'&&url.startsWith('/volumes/')){
          if(!volume)throw Object.assign(Error('absent'),{code:'engine_http_404'});
          return volume;
        }
        if(method==='GET'&&url==='/containers/json?all=1&limit=128')return [];
        throw Error('Unexpected port; no Docker process may start in this Source test');
      },
      create:async(_name,value)=>{actions.push('CREATE_CONTAINER');creates++;config=value;throw Error('TEST_STOP_AFTER_CAPTURE');},
    };
    await assert.rejects(restore({file:'/synthetic/authenticated.gpg',expectedSha256:'7'.repeat(64),image:IMAGE,operationId:OPERATION,key:Buffer.from('A'.repeat(64))},engine),{code:'restore_incomplete_quarantined'});
    assert.equal(creates,1,'Reach actual extraction configuration');
    assert.deepEqual(actions,['AUTH_COMPLETE','CREATE_VOLUME','CREATE_CONTAINER']);
    assert.equal(config.HostConfig.LogConfig?.Type,'none','Tar failures must not write private paths into Docker logs');
    assert.equal(Object.keys(config.HostConfig.LogConfig?.Config??{}).length,0);
    assert.equal(config.HostConfig.NetworkMode,'none');
    assert.equal(config.HostConfig.ReadonlyRootfs,true);
  });
}

await registerBackupLoggingRegressions({candidateFile:new URL("./gpg-release-backup.mjs",import.meta.url),legacyArgsFile:new URL("./backup.mjs",import.meta.url)});
test('BK-LOG-01 actual extraction inspection refuses logging before START',async()=>{
  const source=await readFile(new URL('./gpg-release-backup.mjs',import.meta.url),'utf8');
  const constants=span(source,'const ID=', '\nfunction processDocker(');
  const fences=span(source,'async function absentContainer(', '\nasync function stopOwned(');
  const restoreSource=span(source,'export async function restoreIsolated(', '\nasync function stdinKey(').replace(/^export /,'');
  let volume,config,starts=0,cleanups=0,creates=0,inspected=0;
  const extractId='8'.repeat(64);
  const restore=new Function('path','bad','limits','verifyTar','randomBytes','processDocker','stopOwned','listAllContainerMounts',constants+'\n'+fences+'\n'+restoreSource+'\nreturn restoreIsolated;')(
    path,bad,()=>({}),async()=>({}),()=>Buffer.alloc(16,4),()=>{starts++;throw Error('NO_PROCESS_PORT');},async()=>{cleanups++;},listAllContainerMounts);
  const engine={
    image:async()=>({Id:IMAGE}),
    inspect:async id=>{
      if(id!==extractId)throw Object.assign(Error('absent'),{code:'engine_http_404'});
      inspected++;return {Id:extractId,Image:IMAGE,State:{Running:false},Config:{Labels:config.Labels},HostConfig:{...config.HostConfig,LogConfig:{Type:'json-file',Config:{}}},Mounts:[{Type:'volume',Name:volume.Name,Destination:'/data',RW:true}]};
    },
    request:async(method,url,body)=>{
      if(method==='POST'){volume={...body,Scope:'local',Mountpoint:'/synthetic/new-restore'};return volume;}
      if(url.startsWith('/volumes/')){if(!volume)throw Object.assign(Error('absent'),{code:'engine_http_404'});return volume;}
      return [];
    },
    create:async(_name,value)=>{creates++;config=value;return {Id:extractId};}
  };
  await assert.rejects(restore({file:'/synthetic/authenticated.gpg',expectedSha256:'7'.repeat(64),image:IMAGE,operationId:OPERATION,key:KEY},engine),{code:'restore_incomplete_quarantined'});
  assert.equal(creates,1);assert.equal(inspected,1);assert.equal(cleanups,1);assert.equal(starts,0);
});
for(const owned of [true,false])test('BK-LOG-01 cleanup stops only owned helper with unexpected logging: '+owned,async()=>{
  const source=await readFile(new URL('./gpg-release-backup.mjs',import.meta.url),'utf8');
  const body=span(source,'async function stopOwned(', '\nasync function readonlyArchive(');
  const stop=new Function('bad','CONTAINER',body+'\nreturn stopOwned;')(bad,/^[a-f0-9]{64}$/);
  const label='io.soty.backup.helper',nonce='4'.repeat(32);let running=true,stops=0;
  const engine={inspect:async()=>({Id:SOURCE,Image:IMAGE,Config:{Labels:{[label]:owned?nonce:'wrong'}},HostConfig:{LogConfig:{Type:'json-file',Config:{}}},State:{Running:running}}),stop:async()=>{stops++;running=false;}};
  await assert.rejects(stop(engine,SOURCE,label,nonce,IMAGE),{code:'backup_helper_unresolved'});
  assert.equal(stops,owned?1:0);
});
}

// Genuine subprocess import of this exact helper closure on this executable.
{
const {pathToFileURL,fileURLToPath}=await import("node:url");
function registerBackupImportSmoke({nodeExecutable,candidateFile}) {
  test('BK-NODE-02 qualified genuine Node imports exact helper closure without runtime patches',async()=>{
    const url=pathToFileURL(path.resolve(candidateFile)).href;
    const child=spawn(nodeExecutable,['--input-type=module','-e','await import('+JSON.stringify(url)+');'],{stdio:['ignore','pipe','pipe']});
    let bytes=0,overflow=false;
    for(const stream of [child.stdout,child.stderr])stream.on('data',chunk=>{bytes+=chunk.length;if(bytes>65536){overflow=true;child.kill('SIGKILL');}});
    const timer=setTimeout(()=>child.kill('SIGKILL'),15000);
    let result;
    try{result=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',(code,signal)=>resolve({code,signal}));});}
    finally{clearTimeout(timer);}
    assert.equal(overflow,false,'Bound import diagnostic output');
    assert.equal(result.signal,null,'Import must complete normally');
    assert.equal(result.code,0,'Exact transitive helper import must succeed on the qualified genuine Node binary');
  });
}
registerBackupImportSmoke({nodeExecutable:process.execPath,candidateFile:fileURLToPath(new URL("./gpg-release-backup.mjs",import.meta.url))});
}

// Exact independent six-case Source port; original53 prefix is unchanged.
const {pathToFileURL,fileURLToPath}=await import("node:url");
// Actual candidate TarInventory; no GPG, Docker, crypto/provider fake or Source extraction.
export async function registerHighBitAdmissionRegressions({ tarModuleFile }) {
  const { TarInventory } = await import(pathToFileURL(path.resolve(tarModuleFile)).href);
  const consume = bytes => {
    const value = new TarInventory();
    for (let at = 0; at < bytes.length; at += 173) value.feed(bytes.subarray(at, at + 173));
    return value.finish();
  };
  const checksum = header => {
    header.fill(32, 148, 156);
    header.write(header.reduce((a, b) => a + b, 0).toString(8).padStart(6, '0') + '\0 ', 148, 8, 'ascii');
    return header;
  };
  const header = (name, type, size = 0) => {
    const value = Buffer.alloc(512); value.write(name, 0, 100, 'utf8');
    for (const [at, length, number] of [[100, 8, type === '5' ? 0o700 : 0o600], [108, 8, 0], [116, 8, 0], [124, 12, size], [136, 12, 1]])
      value.write(number.toString(8).padStart(length - 1, '0') + '\0', at, length, 'ascii');
    value[156] = type.charCodeAt(0); value.write('ustar\0', 257, 'ascii'); value.write('00', 263, 'ascii');
    return checksum(value);
  };
  const pax = (key, value) => {
    const body = Buffer.from(key + '=' + value + '\n'); let size = body.length + 2;
    for (;;) { const next = body.length + String(size).length + 1; if (next === size) return Buffer.concat([Buffer.from(String(size) + ' '), body]); size = next; }
  };
  const regular = () => Buffer.concat([header('./', '5'), header('safe', '0', 1), Buffer.from('x'), Buffer.alloc(511), Buffer.alloc(1024)]);
  const withPax = ({ corruptLength = false, fallback = 'safe', truncatedPlaceholder = false } = {}) => {
    const record = pax('path', 'safe');
    if (corruptLength) record[0] |= 0x80; // Same byte count, but not an ASCII decimal digit.
    const metadata = header('pax', 'x', record.length);
    if (truncatedPlaceholder) { metadata.fill(0x61, 0, 99); metadata[99] = 0xd0; checksum(metadata); }
    return Buffer.concat([header('./', '5'), metadata, record, Buffer.alloc((512 - record.length % 512) % 512), header(fallback, '0', 1), Buffer.from('x'), Buffer.alloc(511), Buffer.alloc(1024)]);
  };
  test('BK-HIGH-BIT setup genuine USTAR and PAX archives remain admitted', () => {
    assert.equal(consume(regular()).entries, 2); assert.equal(consume(withPax()).entries, 2);
  });
  test('BK-HIGH-BIT genuine PAX authority still permits an unused truncated UTF8 placeholder', () => {
    assert.equal(consume(withPax({ truncatedPlaceholder: true })).entries, 2);
  });
  for (const fallback of ['safe', '../escape']) test('BK-PAX-ASCII-01 reject non-ASCII PAX length before accepting path authority: ' + fallback, () => {
    const bytes = withPax({ corruptLength: true, fallback });
    assert.throws(() => consume(bytes), { code: 'backup_tar_invalid' }, 'A high-bit length byte is not a POSIX decimal length, even if ASCII decoding masks it into a digit');
  });
  for (const [label, offset] of [['magic', 257], ['version', 263]]) test('BK-MAGIC-ASCII-02 reject high-bit ' + label + ' despite a valid recomputed checksum', () => {
    const bytes = regular(); bytes[offset] |= 0x80; checksum(bytes.subarray(0, 512));
    assert.throws(() => consume(bytes), { code: 'backup_tar_invalid' }, 'USTAR magic/version are exact wire bytes; ASCII decoding must not discard their high bits');
  });
}

await registerHighBitAdmissionRegressions({tarModuleFile:fileURLToPath(new URL("./gpg-tar.mjs",import.meta.url))});

// Closed calls of the actual private sourceState plus its adjacent helper.
// Reading Source/importing sha never invokes Docker, GPG or a private inspect.
export async function registerMountOrderRegressions({ releaseModuleFile, tarModuleFile }) {
  const source = await readFile(releaseModuleFile, 'utf8');
  const first = source.indexOf('function sourceState(');
  const last = source.indexOf('\nasync function sourceFence(', first);
  assert.ok(first >= 0 && last > first, 'Actual sourceState anchors must exist');
  const { sha } = await import(pathToFileURL(path.resolve(tarModuleFile)).href);
  const checkpoint = new Function('sha', source.slice(first, last) + '\nreturn sourceState;')(sha);
  const data = Object.freeze({ Type: 'volume', Name: 'synthetic-data', Source: '/synthetic/data', Destination: '/data', RW: true,
    Driver: 'local', Mode: 'rw', Propagation: '', Extra: Object.freeze({ preserved: 'public-fixture' }) });
  const bind = Object.freeze({ Type: 'bind', Source: '/synthetic/config.json', Destination: '/run/synthetic.json', RW: false,
    Mode: 'ro', Propagation: 'rprivate' });
  const originalMounts = Object.freeze([data, bind]);
  const state = Object.freeze({ Running: false, StartedAt: '2026-01-01T00:00:00Z', FinishedAt: '2026-01-01T00:01:00Z', ExitCode: 0 });
  const value = { Id: '1'.repeat(64), Image: 'sha256:' + '2'.repeat(64), Config: { Env: ['DATA_DIR=/data', 'SYNTHETIC=public'] },
    HostConfig: { NetworkMode: 'bridge', RestartPolicy: { Name: 'no' } }, Mounts: originalMounts, State: state, RestartCount: 0 };
  test('BK-MOUNTS-ORDER-01 actual checkpoint accepts only mount order change without mutating its source', () => {
    const snapshot = JSON.stringify(value), baseline = checkpoint(value);
    assert.equal(checkpoint({ ...value, Mounts: Object.freeze([bind, data]) }), baseline,
      'An order-only Docker inspect change must preserve the stopped-source checkpoint');
    assert.equal(baseline, sha(JSON.stringify({ ...value, Mounts: [bind, data] })), 'Keep complete mount objects and every other checkpoint field');
    assert.equal(JSON.stringify(value), snapshot); assert.equal(value.Mounts, originalMounts);
    assert.equal(originalMounts[0], data); assert.equal(originalMounts[1], bind);
  });
  test('BK-MOUNTS-ORDER-02 actual checkpoint retains RW Source Destination and every other mount field', () => {
    const baseline = checkpoint(value);
    for (const [field, changed] of [['RW', false], ['Source', '/synthetic/other'], ['Destination', '/other'], ['Name', 'other-volume'],
      ['Propagation', 'changed'], ['Extra', { preserved: 'changed' }]]) {
      assert.notEqual(checkpoint({ ...value, Mounts: [{ ...data, [field]: changed }, bind] }), baseline, 'Semantic mount change must be retained: ' + field);
    }
    assert.notEqual(checkpoint({ ...value, Mounts: [data, bind, data] }), baseline, 'Duplicate mount multiplicity must not be dropped');
  });
  test('BK-MOUNTS-ORDER-03 exact identity config state and restart guards survive mount canonicalization', () => {
    const baseline = checkpoint(value);
    for (const changed of [{ Id: '3'.repeat(64) }, { Image: 'sha256:' + '4'.repeat(64) },
      { State: { ...state, Running: true } }, { State: { ...state, StartedAt: '2026-01-01T00:00:01Z' } },
      { State: { ...state, FinishedAt: '2026-01-01T00:01:01Z' } }, { RestartCount: 1 },
      { Config: { ...value.Config, Env: ['DATA_DIR=/data', 'SYNTHETIC=changed'] } },
      { HostConfig: { ...value.HostConfig, RestartPolicy: { Name: 'always' } } }]) {
      assert.notEqual(checkpoint({ ...value, ...changed }), baseline, 'Existing source checkpoint fields must still compare exactly');
    }
  });
}

await registerMountOrderRegressions({releaseModuleFile:fileURLToPath(new URL("./gpg-release-backup.mjs",import.meta.url)),tarModuleFile:fileURLToPath(new URL("./gpg-tar.mjs",import.meta.url))});
