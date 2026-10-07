import test from 'node:test';
import assert from 'node:assert/strict';
import {PassThrough,Writable,Readable} from 'node:stream';
import {createOrdinaryNativeRestorePorts} from '../../../deploy/connect/restore-backup.mjs';
import {projectColdExtractDiagnostic} from '../install/cold-ram-transfer.mjs';
test('native Source sender refuses Duplex before adoption; fs stream transport must not weaken the port',async()=>{
  const stream=new PassThrough({highWaterMark:65536}),ports=createOrdinaryNativeRestorePorts({realmId:'cold-synthetic'});
  const input={file:'/public-fixture/backup.enc',privateKeyPem:'synthetic-only',expectedSha256:'a'.repeat(64),expectedManifestSha256:'b'.repeat(64),
    sourceWitness:{generationId:'c'.repeat(32),checkpointSha256:'d'.repeat(64),inventorySha256:'e'.repeat(64)},
    limits:{archiveBytes:16777216,plaintextBytes:16777216,fileBytes:4194304,extractedBytes:8388608,entries:64,headers:128,pathBytes:4096,pathDepth:8,
      externalFiles:4,externalBytes:131072,wallMs:30000,idleMs:10000},output:stream};
  assert.equal(stream instanceof Writable,true);assert.equal(stream instanceof Readable,true);
  await assert.rejects(ports.send(input),error=>error.code==='restore_archive_invalid');assert.equal(stream.destroyed,false);stream.destroy();
});
test('send/receive diagnostics retain only bounded stages/classes/native booleans, no paths/keys/pins',()=>{
  const profiles=Object.fromEntries(['tmpfsPinned','outputWritable','outputNotDuplex','outputConstructed','outputEmitClose','outputAutoDestroy',
    'inputReadable','inputNotDuplex','inputConstructed','inputEmitClose','inputAutoDestroy','targetPinned'].map(key=>[key,true]));
  const value={schema:'soty.source-cold-extract.v2',passed:false,stage:'send',code:'restore_archive_invalid',sendPassed:false,receivePassed:false,
    cleanupUnknown:false,nodeVersion:'v24.15.0',profiles,productionReady:false,key:'PRIVATE_SYNTHETIC',path:'/private'};
  const safe=projectColdExtractDiagnostic(value);assert.equal(safe.stage,'send');assert.equal(safe.nodeVersion,'v24.15.0');
  assert.equal(JSON.stringify(safe).includes('PRIVATE_SYNTHETIC'),false);assert.equal(Object.hasOwn(safe,'path'),false);
  for(const delta of [{stage:'caller'},{code:'caller_secret'},{nodeVersion:'caller'},{profiles:{...profiles,outputNotDuplex:1}},{productionReady:true}])
    assert.equal(projectColdExtractDiagnostic({...value,...delta}),null);
  const getter=()=>{throw Error('getter_must_not_run');};
  assert.equal(projectColdExtractDiagnostic(Object.defineProperty({},'schema',{get:getter})),null);
  assert.equal(projectColdExtractDiagnostic({...value,profiles:Object.defineProperty({},'tmpfsPinned',{get:getter})}),null);
});
