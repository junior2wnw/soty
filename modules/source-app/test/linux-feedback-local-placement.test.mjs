import test from 'node:test';
import assert from 'node:assert/strict';
import {createSyntheticLinuxFeedbackProcessor,createSyntheticLocalLinuxFeedbackProcessor,SYNTHETIC_FEEDBACK_IMAGE} from '../server/linux-feedback-enforcer.mjs';
import {LOCAL_LINUX_FEEDBACK_PLACEMENT as placement} from '../server/linux-feedback-local-placement.mjs';
import {fixedLocalWslDockerArgs,fixedLocalWslHostDockerArgs,createLocalWslDockerCommandRunner,createLocalWslHostDockerCommandRunner} from '../server/linux-feedback-lifecycle.mjs';
import {digest} from '../server/wire.mjs';

test('local WSL placement pins fixed daemon/worker/cleanup policy without changing old Dev engine2',async()=>{
  const dev=createSyntheticLinuxFeedbackProcessor({directory:'/home/ai2/codex-soty-universal-20261007-9f8dcd71/source-feedback-jobs'});
  assert.deepEqual(dev.ref,{id:'local.synthetic-linux.success',version:2,digest:'4f32d83bd29982c78037b37e4f2e04241d701b5249e569704708725a781d03a3'});
  const local=createSyntheticLocalLinuxFeedbackProcessor({directory:placement.directory});assert.equal(local.ref.version,3);assert.notEqual(local.ref.digest,dev.ref.digest);
  assert.equal(local.placement,placement);assert.equal(local.image,SYNTHETIC_FEEDBACK_IMAGE);assert.equal(local.productionReady,false);
  assert.equal(local.ref.digest,digest({schema:'soty.synthetic-linux-feedback.v3',placement,image:local.image,workerSha256:local.workerSha256,scenario:'success',purpose:'ocr',
    maxBudget:{wallMs:60000,cpuMs:10000,cleanupMs:2000,scratchBytes:2097152,outputBytes:32768,mediaBytes:1048576,attachments:3,parallel:1}}));
  for(const extra of [{directory:'/tmp'},{directory:placement.directory,socket:'/tmp/docker.sock'},{directory:placement.directory,placement:{...placement}},
    {directory:placement.directory,image:SYNTHETIC_FEEDBACK_IMAGE},{directory:placement.directory,dockerBinary:'/bin/sh'}])assert.throws(()=>createSyntheticLocalLinuxFeedbackProcessor(extra));
  if(process.platform!=='linux')await assert.rejects(local.prepare(),error=>error.code==='source_feedback_processor_not_ready');
});

test('local daemon is an explicit fixed argv prefix; env cannot select another daemon/context',()=>{
  assert.deepEqual(fixedLocalWslDockerArgs(['image','inspect',SYNTHETIC_FEEDBACK_IMAGE]),['--host','unix:///run/soty-docker.sock','image','inspect',SYNTHETIC_FEEDBACK_IMAGE]);
  assert.deepEqual(fixedLocalWslHostDockerArgs(['image','ls']),['--host','unix://'+placement.socketHost,'image','ls']);
  for(const args of [['--host','tcp://foreign'],['-H','unix:///foreign'],['-Hunix:///foreign'],['-cforeign'],['--host=unix:///foreign'],['--context','foreign'],['--config','/tmp'],[],[{}]])assert.throws(()=>fixedLocalWslDockerArgs(args));
  assert.throws(()=>createLocalWslDockerCommandRunner('/bin/sh'));
  assert.throws(()=>createLocalWslHostDockerCommandRunner('/usr/bin/docker'));
});
