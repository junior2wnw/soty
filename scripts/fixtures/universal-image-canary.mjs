// SYNTHETIC LINUX IMAGE GATE. Never mounts, inspects, starts or stops a serving container.
import assert from 'node:assert/strict';
import { lstat, realpath, mkdir, open, readFile, readdir } from 'node:fs/promises';
import { constants } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';

export const CANARY_BASE = '/home/ai2/codex-soty-universal-20261007-9f8dcd71';
export const CANARY_REVISION = '4a9a7d7f88a93402ac955169527896b1e665bb53';
export const CANARY_SOURCE = CANARY_BASE + '/source-4a9a7d7';
export const OLD_IMAGE = 'sha256:1c79c2a71aa3438472da8b197f03494908ee242e67fd76bb6a72d3da6340c7da';
export const CANARY_BASELINE_IMAGE='sha256:4cacf01e2e05897f67c6c12567a4af91dd91d8c4e0aafed605e3af19a30462d0';
export const CANARY_FEATURE_IMAGE='sha256:9283aea6485b5f4c08bad9416c7d9470db38bb64c19ea1a9e256295cc9960d74';
export const APPS6_BASELINE_IMAGE='sha256:c8754ba67f1883cb33417a970ef9947b0c1a499934d35f32f58f58a59c23c2e4';
export const CANARY_LABEL = 'io.soty.synthetic-universal-canary';
const imageId = value => typeof value === 'string' && /^sha256:[a-f0-9]{64}$/u.test(value);
const require = (ok, code) => { if (!ok) throw Object.assign(new Error(code), { code }); };
export const CANARY_HARNESS_FILES=Object.freeze(['universal-canary-bff.mjs','universal-canary-runtime.mjs','universal-image-canary.mjs']);
// Exact reviewed Gitarchive bytes. Windows core.autocrlf exported CRLF; a
// separate binary tar/Git comparison proved all four canonical source blobs.
// The runtime pins exact archive bytes and never normalizes its input.
export const CANARY_SOURCE_PINS=Object.freeze({
  'deploy/connector/docker-api.mjs':'ede9d311989b1e6dad15e2110b7ce7b55a16f14f3b2fec4fd91d471f8d61f413',
  'deploy/connector/storage-guard.mjs':'00b09bc0e95ae372fa5da0d338acaed38fce9e5440155dc63b7cb552489f3ed1',
  'deploy/connector/storage-probe.mjs':'a3c994e19e02808994f37a9abb3222bc65166fdd6fed2d4b988bfadc9275555c',
  'deploy/connector/storage-snapshot.mjs':'1db67a36458a3e525b4a292e4baedde68ceedb9e219c004a3b13696acefc2834',
  'scripts/agent-modules/local-apps.mjs':'ceec1381fdd0987a189e5386f901abc696b49b007ba4854c4317a11cf6204de0',
});
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
export function canaryHarnessManifest(entries) {
  require(Array.isArray(entries)&&entries.length===3&&entries.every((entry,index)=>entry&&Object.keys(entry).sort().join(',')==='path,sha256'
    &&entry.path===CANARY_HARNESS_FILES[index]&&/^[a-f0-9]{64}$/u.test(entry.sha256)),'canary_harness_manifest_invalid');
  return Object.freeze({schema:'soty.synthetic-image-harness.v1',applicationRevision:CANARY_REVISION,files:entries.map(entry=>({...entry}))});
}
export function canaryHarnessDigest(manifest) {
  require(manifest&&Object.keys(manifest).sort().join(',')==='applicationRevision,files,schema'&&manifest.schema==='soty.synthetic-image-harness.v1'
    &&manifest.applicationRevision===CANARY_REVISION,'canary_harness_manifest_invalid');return hash(Buffer.from(JSON.stringify(canaryHarnessManifest(manifest.files))));
}
export async function verifyCanaryHarness(directory,expectedDigest) {
  require(/^[a-f0-9]{64}$/u.test(expectedDigest||''),'canary_harness_pin_invalid');await checkDirectoryChain(directory,{privateLeaf:true});
  require((await readdir(directory)).sort().join(',')===['manifest.json',...CANARY_HARNESS_FILES].sort().join(','),'canary_harness_contents_invalid');
  const read=async(name,max)=>{const file=directory+'/'+name,before=await lstat(file,{bigint:true});require(before.isFile()&&!before.isSymbolicLink()&&before.nlink===1n
      &&before.uid===BigInt(process.getuid())&&(before.mode&0o777n)===0o600n&&before.size<=BigInt(max),'canary_harness_file_unsafe');const fd=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW);
    try {const current=await fd.stat({bigint:true});require(current.dev===before.dev&&current.ino===before.ino&&current.size===before.size,'canary_harness_file_changed');
      const bytes=await fd.readFile(),after=await fd.stat({bigint:true});require(bytes.length<=max&&after.mtimeNs===current.mtimeNs&&after.ctimeNs===current.ctimeNs,'canary_harness_file_changed');return bytes;
    }finally{await fd.close();}};
  const manifest=JSON.parse(await read('manifest.json',4096));require(canaryHarnessDigest(manifest)===expectedDigest,'canary_harness_pin_mismatch');
  for(const entry of manifest.files)require(hash(await read(entry.path,128*1024))===entry.sha256,'canary_harness_pin_mismatch');return manifest;
}
export async function verifyCanarySource(directory) {
  require(directory===CANARY_SOURCE,'canary_reviewed_source_required');const identity=await checkDirectoryChain(directory);
  for(const [name,expected]of Object.entries(CANARY_SOURCE_PINS)) {const file=directory+'/'+name;await checkDirectoryChain(path.posix.dirname(file));
    const before=await lstat(file,{bigint:true});require(before.isFile()&&!before.isSymbolicLink()&&before.nlink===1n&&before.size<=128n*1024n,'canary_source_file_unsafe');
    const fd=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW);try{const stat=await fd.stat({bigint:true});require(stat.dev===before.dev&&stat.ino===before.ino,'canary_source_file_changed');
      const bytes=await fd.readFile(),after=await fd.stat({bigint:true});require(bytes.length<=128*1024&&after.mtimeNs===stat.mtimeNs&&after.ctimeNs===stat.ctimeNs&&hash(bytes)===expected,'canary_source_pin_mismatch');
    }finally{await fd.close();}}
  return {exactGitSourceBytes:true,sourcePinnedFiles:Object.keys(CANARY_SOURCE_PINS).length,sourceLeafMode:Number(identity.mode&0o777n)};
}

export function checkedCanaryOptions(input) {
  require(input && ['baselineImage,featureImage,fixtureHarness,fixtureSource,harnessSha256,oldImage,ownedRoot,revision','baselineImage,featureImage,fixtureHarness,fixtureSource,harnessSha256,nonce,oldImage,ownedRoot,revision'].includes(Object.keys(input).sort().join(',')), 'canary_arguments_invalid');
  require(input.revision === CANARY_REVISION && input.fixtureSource === CANARY_SOURCE && input.oldImage === OLD_IMAGE, 'canary_reviewed_source_required');
  require(typeof input.ownedRoot === 'string' && new RegExp('^' + CANARY_BASE + '/canary-[a-f0-9]{32}$', 'u').test(input.ownedRoot), 'canary_owned_root_invalid');
  require(input.nonce===undefined||input.nonce===input.ownedRoot.slice(-32),'canary_owned_root_invalid');
  require(input.fixtureHarness===CANARY_BASE+'/harness-'+input.ownedRoot.slice(-32)&&/^[a-f0-9]{64}$/u.test(input.harnessSha256||''),'canary_harness_scope_invalid');
  require(imageId(input.baselineImage) && imageId(input.featureImage) && input.baselineImage !== input.featureImage
    && input.baselineImage===CANARY_BASELINE_IMAGE&&input.featureImage===CANARY_FEATURE_IMAGE, 'canary_image_invalid');
  return Object.freeze({ ...input, nonce: input.ownedRoot.slice(-32) });
}

export async function checkDirectoryChain(directory, { privateLeaf = false } = {}) {
  require(path.posix.isAbsolute(directory) && path.posix.normalize(directory) === directory, 'canary_path_invalid');
  const leaf=await lstat(directory,{bigint:true});
  let cursor = '/';
  for (const segment of directory.split('/').filter(Boolean)) {
    cursor = path.posix.join(cursor, segment);
    const stat = await lstat(cursor, { bigint: true });
    require(stat.isDirectory() && !stat.isSymbolicLink() && (stat.mode & 0o022n) === 0n
      && (stat.uid===0n||stat.uid===leaf.uid)&&await realpath(cursor) === cursor, 'canary_path_unsafe');
    if (cursor === directory && privateLeaf) require((stat.mode & 0o777n) === 0o700n&&stat.uid===BigInt(process.getuid()), 'canary_root_mode_invalid');
  }
  const stat = await lstat(directory, { bigint: true });
  return { dev: stat.dev, ino: stat.ino, uid: stat.uid, mode: stat.mode };
}

export function checkedImage(image, { id, legacy, revision = CANARY_REVISION }) {
  require(image?.Id === id && imageId(id) && image.Config?.Labels?.['org.opencontainers.image.revision'] === revision
    && image.Config.Labels['io.soty.universal.legacy'] === String(legacy), 'canary_image_pin_mismatch');
  let declaration;try{declaration=JSON.parse(image.Config.Labels['io.soty.storage.readers']);}catch{throw Object.assign(new Error('canary_reader_metadata_invalid'),{code:'canary_reader_metadata_invalid'});}
  require(declaration?.version===5&&declaration.readers&&Array.isArray(declaration.readers.humanIdentity),'canary_reader_metadata_invalid');const readers=declaration.readers;
  require(readers.humanIdentity?.includes(1) && readers.humanIdentity?.includes(2), 'canary_human_reader2_required');
  require(readers.apps?.includes(7), 'canary_apps_reader7_required');
  return id;
}

export function childConfiguration(options, { role, image, volume, readOnlyData = false, networkContainer, args = [], mode = 'helper' }) {
  const o = checkedCanaryOptions(options);
  require(/^[a-z][a-z0-9-]{0,39}$/u.test(role) && [o.baselineImage,o.featureImage].includes(image), 'canary_role_invalid');
  require(volume === 'soty-universal-canary-' + o.nonce + '-data' || volume === 'soty-universal-canary-' + o.nonce + '-restore', 'canary_volume_invalid');
  require(networkContainer === undefined || /^[a-f0-9]{64}$/u.test(networkContainer), 'canary_network_identity');
  require(['helper','baseline','feature'].includes(mode), 'canary_mode_invalid');
  require(args.every(value => typeof value === 'string' && value.length <= 4096), 'canary_command_invalid');
  return {
    Image:image, User:'0:0', WorkingDir:'/app', Entrypoint:['node'],
    Cmd:[o.fixtureHarness+'/universal-canary-runtime.mjs',role,'--owned-root',o.ownedRoot,'--fixture-source',o.fixtureSource,'--fixture-harness',o.fixtureHarness,'--harness-sha256',o.harnessSha256,...args],
    Env:['NODE_ENV=production','SOTY_UNIVERSAL_OPERATOR_ENABLED=1','SOTY_SYNTHETIC_CANARY=1',...mode==='baseline'?['SOTY_UNIVERSAL_APPS_ENABLED=false']:[]],
    Tty:false, Labels:{[CANARY_LABEL]:o.nonce,[CANARY_LABEL+'.role']:role,[CANARY_LABEL+'.revision']:o.revision},
    HostConfig:{Mounts:[{Type:'bind',Source:o.fixtureSource,Target:o.fixtureSource,ReadOnly:true},
      {Type:'bind',Source:o.fixtureHarness,Target:o.fixtureHarness,ReadOnly:true},
      {Type:'bind',Source:o.ownedRoot,Target:o.ownedRoot,ReadOnly:false},
      {Type:'volume',Source:volume,Target:'/data',ReadOnly:readOnlyData}],
      NetworkMode:networkContainer?'container:'+networkContainer:'none',RestartPolicy:{Name:'no'},ReadonlyRootfs:true,
      Memory:805306368,NanoCpus:2000000000,PidsLimit:96,CapDrop:['ALL'],SecurityOpt:['no-new-privileges'],
      Tmpfs:{'/tmp':'rw,nosuid,noexec,mode=1777,size=67108864'}},
    NetworkingConfig:{EndpointsConfig:{}},
  };
}

export function checkedOwnedContainer(inspect, { id, name, image, nonce, role }) {
  require(inspect?.Id === id && /^[a-f0-9]{64}$/u.test(id) && inspect.Name === '/'+name && inspect.Image === image
    && inspect.Config?.Labels?.[CANARY_LABEL] === nonce && inspect.Config.Labels[CANARY_LABEL+'.role'] === role
    && inspect.Config.Labels[CANARY_LABEL+'.revision'] === CANARY_REVISION, 'canary_container_identity');
  return inspect;
}

export function parseNativeTap(bytes) {
  require(Buffer.isBuffer(bytes) && bytes.length <= 256*1024, 'canary_native_output_limit');
  const text = bytes.toString('utf8'), count = key => Number(new RegExp('^# '+key+' ([0-9]+)$','mu').exec(text)?.[1]);
  require(count('tests') === 9 && count('pass') === 8 && count('fail') === 0 && count('skipped') === 1
    && !/^not ok /mu.test(text), 'canary_native_tests_failed');
  return Object.freeze({tests:9,pass:8,fail:0,skip:1,nativeTests:7});
}

function unframe(data, limit = 256*1024) {
  const out=[];let position=0,size=0;
  while(position<data.length) { require(position+8<=data.length && data[position]===1 && data[position+1]===0 && data[position+2]===0 && data[position+3]===0,'canary_output_invalid');
    const length=data.readUInt32BE(position+4);require(length<=data.length-position-8 && (size+=length)<=limit,'canary_output_invalid');
    out.push(data.subarray(position+8,position+8+length));position+=8+length; }
  return Buffer.concat(out,size);
}

async function controller(rawOptions) {
  require(process.platform === 'linux' && process.env.SOTY_SYNTHETIC_CANARY === '1', 'canary_linux_opt_in_required');
  const options=checkedCanaryOptions(rawOptions),root=options.ownedRoot;
  const rootIdentity=await checkDirectoryChain(root,{privateLeaf:true});
  const sourceMetadata=await verifyCanarySource(options.fixtureSource);
  await verifyCanaryHarness(options.fixtureHarness,options.harnessSha256);
  const {DockerApi}=await import(pathToFileURL(options.fixtureSource+'/deploy/connector/docker-api.mjs'));
  const {assertStorageCompatible}=await import(pathToFileURL(options.fixtureSource+'/deploy/connector/storage-guard.mjs'));
  const ownMarker=await open(root+'/canary-owner.json','wx',0o600);
  try {await ownMarker.writeFile(JSON.stringify({schema:'soty.synthetic-canary.v1',nonce:options.nonce,revision:options.revision}));await ownMarker.sync();}finally{await ownMarker.close();}
  const checkRoot=async()=>{const current=await checkDirectoryChain(root,{privateLeaf:true});require(current.dev===rootIdentity.dev&&current.ino===rootIdentity.ino&&current.uid===rootIdentity.uid&&current.mode===rootIdentity.mode,'canary_root_changed');
    await verifyCanaryHarness(options.fixtureHarness,options.harnessSha256);await verifyCanarySource(options.fixtureSource);};
  for(const directory of ['custody','private-state','receipts'])await mkdir(root+'/'+directory,{mode:0o700});
  const engine=new DockerApi({timeoutMs:120000}), prefix='soty-universal-canary-'+options.nonce;
  const baseline=await engine.image(options.baselineImage),features=await engine.image(options.featureImage),old=await engine.image(OLD_IMAGE),apps6=await engine.image(APPS6_BASELINE_IMAGE);
  checkedImage(baseline,{id:options.baselineImage,legacy:1});checkedImage(features,{id:options.featureImage,legacy:0});
  require(old.Id===OLD_IMAGE,'canary_old_image_identity');
  require(apps6.Id===APPS6_BASELINE_IMAGE,'canary_apps6_image_identity');
  const owned=[],volumes=[];let step=0,active;
  async function note(name,value) {await checkRoot();const file=await open(root+'/receipts/'+String(++step).padStart(2,'0')+'-'+name+'.json','wx',0o600);
    try{await file.writeFile(JSON.stringify({schema:'soty.synthetic-canary-step.v1',step:name,...value}));await file.sync();}finally{await file.close();}
    process.stdout.write(JSON.stringify({step:name,...value})+'\n');}
  async function volume(suffix) {const name=prefix+'-'+suffix;let absent=false;
    try{await engine.request('GET','/volumes/'+name);}catch(error){if(error.code==='engine_http_404')absent=true;else throw error;}
    require(absent,'canary_volume_exists');await checkRoot();
    const value=await engine.request('POST','/volumes/create',{Name:name,Driver:'local',Labels:{[CANARY_LABEL]:options.nonce,[CANARY_LABEL+'.revision']:options.revision}});
    require(value.Name===name&&value.Driver==='local'&&value.Labels?.[CANARY_LABEL]===options.nonce,'canary_volume_identity');volumes.push(name);return name;}
  async function create(role,image,volumeName,extra={}) {await checkRoot();const name=prefix+'-'+role;const body=childConfiguration(options,{role,image,volume:volumeName,...extra});
    // No retries of CREATE or START. An ambiguous outcome preserves all artifacts.
    const created=await engine.create(name,body);require(/^[a-f0-9]{64}$/u.test(created?.Id||''),'canary_container_identity');
    const record={id:created.Id,name,image,nonce:options.nonce,role};owned.push(record);checkedOwnedContainer(await engine.inspect(record.id),record);await checkRoot();await engine.start(record.id);return record;}
  async function state(record) {return checkedOwnedContainer(await engine.inspect(record.id),record);}
  async function job(role,volumeName,extra={}) {const record=await create(role,options.featureImage,volumeName,extra);const deadline=performance.now()+120000;
    let final;do{final=await state(record);if(!final.State.Running)break;await new Promise(done=>setTimeout(done,100));}while(performance.now()<deadline);
    require(final&&!final.State.Running&&final.State.ExitCode===0,'canary_job_failed');const out=await engine.helperOutput(record.id);
    require(out&&out.ok===true&&out.role===role,'canary_job_receipt_invalid');return out;}
  async function startEntry(role,image,volumeName,mode) {active=await create(role,image,volumeName,{mode,args:['--entry-mode',mode]});const deadline=performance.now()+15000;
    do {const current=await state(active);require(current.State.Running,'canary_entry_failed');let ready;
      try{ready=JSON.parse(await readFile(root+'/receipts/'+role+'-ready.json','utf8'));}catch(error){if(error.code!=='ENOENT')throw error;}
      if(ready) {require(Object.keys(ready).sort().join(',')==='mode,nonce,ready,role'&&ready.ready===true&&ready.role===role&&ready.mode===mode&&ready.nonce===options.nonce,'canary_ready_marker_invalid');
        // Exactly one fixed measurement after readiness. Ambiguous exec is never retried.
        const value=await engine.universalPreparedness(active.id);
      require(value.compiledLegacyMode===(mode==='baseline')&&value.universalConfigured===(mode==='feature')&&value.humanHttpEnabled===(mode==='feature'),'canary_runtime_mismatch');
      if(mode==='feature')require(value.human?.clientCount===2&&value.human?.renewal?.admissionEnabled===true
        &&value.human.renewal.maximumSessionSeconds===86400&&value.human.renewal.eligibleClientCount===2,'canary_renewal_not_prepared');
      if(mode==='feature')require(value.selected?.configured===false&&value.selected.migrationConfigured===true&&value.selected.profileCount===0,
        'canary_apps7_migration_not_prepared');
      await note(role,{runtimeMeasured:true,compiledLegacyMode:value.compiledLegacyMode,humanHttpEnabled:value.humanHttpEnabled,image});return;}
      await new Promise(done=>setTimeout(done,100)); }while(performance.now()<deadline);throw Object.assign(new Error('canary_entry_not_ready'),{code:'canary_entry_not_ready'});}
  async function stopEntry() {if(!active)return;const record=active;const before=await state(record);await checkRoot();if(before.State.Running)await engine.stop(record.id);
    const after=await state(record);require(!after.State.Running&&after.State.ExitCode===0,'canary_unclean_stop');active=null;}
  async function probe(role,volumeName) {
    require(active===null,'canary_probe_writer_active');
    const originalProbe=await readFile(options.fixtureSource+'/deploy/connector/storage-probe.mjs','utf8');
    const snapshot=await readFile(options.fixtureSource+'/deploy/connector/storage-snapshot.mjs','utf8');
    require(Buffer.byteLength(originalProbe)<=128*1024&&Buffer.byteLength(snapshot)<=16384
      &&originalProbe.split("await readStorageFormat('/data')").length===2,'canary_probe_limit');
    // Match the existing production offline guard. Original data stays RO;
    // the pinned host oracle opens a private cold copy in this child's tmpfs.
    const code=snapshot+'\n'+originalProbe.replace("await readStorageFormat('/data')", "await readStorageFormat(await snapshotStorage('/data', '/tmp/soty-storage-snapshot', 48*1024*1024))");
    const cfg=childConfiguration(options,{role,image:options.featureImage,volume:volumeName,readOnlyData:true});cfg.Cmd=['--input-type=module','-e',code];cfg.Env=['SOTY_STORAGE_PROBE=1'];
    const name=prefix+'-'+role,created=await engine.create(name,cfg),record={id:created.Id,name,image:options.featureImage,nonce:options.nonce,role};owned.push(record);
    checkedOwnedContainer(await engine.inspect(record.id),record);await engine.start(record.id);
    const deadline=performance.now()+15000;let current;do{current=await state(record);if(!current.State.Running)break;await new Promise(done=>setTimeout(done,100));}while(performance.now()<deadline);
    require(!current.State.Running&&current.State.ExitCode===0,'canary_independent_probe_failed');const value=await engine.helperOutput(record.id);
    assertStorageCompatible(baseline,value);assertStorageCompatible(features,value);return value;}
  try {
    await note('reviewed-harness',{applicationRevision:options.revision,harnessSha256:options.harnessSha256,sourceArchiveBytesUnmodified:true,...sourceMetadata});
    const data=await volume('data'),restore=await volume('restore');
    await job('initialize',data);await startEntry('baseline-one',options.baselineImage,data,'baseline');
    await job('signed-baseline',data,{networkContainer:active.id});await stopEntry();
    const first=await probe('probe-baseline',data);require(first.schema==='soty.storage-format.v3','canary_baseline_created_new_store');
    await note('baseline-no-new-stores',{format:first.schema,newStoresEmpty:true});
    await job('prepare-synthetic-human',data);await startEntry('feature-one',options.featureImage,data,'feature');
    await job('signed-feature',data,{networkContainer:active.id});await stopEntry();
    const format=await probe('probe-seven',data);require(format.schema==='soty.storage-format.v5'&&format.rooms===2&&format.apps===7&&format.notes===2&&format.capabilities===3
      &&format.appRegistration===1&&format.feedback===1&&format.humanIdentity===2,'canary_seven_store_format');
    let denied=false;try{assertStorageCompatible(old,format);}catch(error){denied=error.code==='storage_reader_incompatible';}require(denied,'canary_old_reader_accepted');
    let apps6Denied=false;try{assertStorageCompatible(apps6,format);}catch(error){apps6Denied=error.code==='storage_reader_incompatible';}require(apps6Denied,'canary_apps6_reader_accepted');
    await note('reader-gates',{...format,oldImage:OLD_IMAGE,oldReaderRefusedBeforeStart:true,oldContainerCreated:false,
      previousBaselineImage:APPS6_BASELINE_IMAGE,apps6ReaderRefusedBeforeStart:true,apps6ContainerCreated:false});
    await job('capture-evidence',data,{readOnlyData:true});await job('encrypted-backup',data,{readOnlyData:true});
    await startEntry('baseline-two',options.baselineImage,data,'baseline');await job('check-baseline',data,{networkContainer:active.id});await stopEntry();
    await job('compare-evidence',data,{readOnlyData:true});await note('cold-feature-to-baseline',{samePrivateEvidence:true,humanDisabled:true});
    await job('invalidate-original-config',data,{readOnlyData:true});await job('encrypted-restore',restore);await job('compare-restored',restore,{readOnlyData:true});await probe('probe-restore',restore);
    await startEntry('restored-feature',options.featureImage,restore,'feature');await job('check-restored-feature',restore,{networkContainer:active.id});await stopEntry();
    await job('compare-restored-after-read',restore,{readOnlyData:true});
    await note('encrypted-cold-restore',{samePrivateEvidence:true,realTwoRpUserinfo:true,versionedProductStores:7,retainedConnect:true,configurationRestoredFromCiphertext:true});
    const native=await create('native-tests',options.featureImage,restore,{readOnlyData:true});const deadline=performance.now()+30000;let end;
    do{end=await state(native);if(!end.State.Running)break;await new Promise(done=>setTimeout(done,100));}while(performance.now()<deadline);
    require(!end.State.Running&&end.State.ExitCode===0,'canary_native_tests_failed');
    const logs=await engine.request('GET','/containers/'+native.id+'/logs?stdout=true&stderr=false',undefined,true,270000);const counts=parseNativeTap(unframe(logs));
    await note('linux-private-port',{...counts});
    await note('complete',{ok:true,revision:options.revision,baselineImage:options.baselineImage,featureImage:options.featureImage,
      syntheticOnly:true,productionMutations:0,artifactsRetained:true,containerCount:owned.length,volumeCount:volumes.length});
  } finally {
    // Stop only exact already-recorded owned writers. Never delete data or retry an ambiguous helper mutation.
    if(active) {try {await stopEntry();}catch {process.stdout.write(JSON.stringify({ok:false,code:'canary_writer_stop_unconfirmed',artifactsRetained:true})+'\n');}}
    // START can have succeeded before its response was lost; its record was
    // already captured. Reconcile only those exact owned IDs, never issue START again.
    for(const record of owned) {try{const current=await state(record);if(current.State.Running){await checkRoot();await engine.stop(record.id);require(!(await state(record)).State.Running,'canary_writer_stop_unconfirmed');}}
      catch{process.exitCode=1;process.stdout.write(JSON.stringify({ok:false,code:'canary_owned_stop_unconfirmed',artifactsRetained:true})+'\n');}}
  }
}

export function parseControllerArgs(args) {
  const keys=new Map([['--owned-root','ownedRoot'],['--fixture-source','fixtureSource'],['--fixture-harness','fixtureHarness'],['--harness-sha256','harnessSha256'],['--baseline-image','baselineImage'],['--feature-image','featureImage'],['--old-image','oldImage'],['--expected-revision','revision']]);
  const out={};for(let index=0;index<args.length;index+=2){const key=keys.get(args[index]);require(key&&typeof args[index+1]==='string'&&!Object.hasOwn(out,key),'canary_arguments_invalid');out[key]=args[index+1];}
  return checkedCanaryOptions(out);
}
if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href) {
  try {await controller(parseControllerArgs(process.argv.slice(2)));}
  catch(error) {const code=/^canary_[a-z0-9_]{1,90}$/u.test(error?.code||'')?error.code:'canary_failed';process.stdout.write(JSON.stringify({ok:false,code,artifactsRetained:true})+'\n');process.exitCode=1;}
}
