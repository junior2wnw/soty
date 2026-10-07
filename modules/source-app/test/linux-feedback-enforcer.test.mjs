import test from 'node:test';
import assert from 'node:assert/strict';
import {createSyntheticLinuxFeedbackProcessor} from '../server/linux-feedback-enforcer.mjs';
import {requireOrdinaryFeedbackJobs} from '../examples/ordinary-app/feedback-jobs.mjs';

test('fixed Linux constructor rejects arbitrary image/path/URL/command/engine JSON; Windows no-host stays not-ready',async()=>{
  for(const options of [{directory:'/tmp/anything'},{directory:'/home/ai2/codex-soty-universal-20261007-9f8dcd71/source-feedback-jobs',scenario:'../../shell'},
    {directory:'/home/ai2/codex-soty-universal-20261007-9f8dcd71/source-feedback-jobs',dockerBinary:'/bin/sh'},
    {directory:'/home/ai2/codex-soty-universal-20261007-9f8dcd71/source-feedback-jobs',command:'anything'}])assert.throws(()=>createSyntheticLinuxFeedbackProcessor(options));
  const one=createSyntheticLinuxFeedbackProcessor({directory:'/home/ai2/codex-soty-universal-20261007-9f8dcd71/source-feedback-jobs'}),two=createSyntheticLinuxFeedbackProcessor({directory:'/home/ai2/codex-soty-universal-20261007-9f8dcd71/source-feedback-jobs',scenario:'cpu'});
  assert.notEqual(one.ref.digest,two.ref.digest);assert.equal(one.productionReady,false);
  assert.equal(one.ref.version,2);
  if(process.platform!=='linux')await assert.rejects(one.prepare(),error=>error.code==='source_feedback_processor_not_ready');
  assert.throws(()=>requireOrdinaryFeedbackJobs({},{} ,'resource','incarnation'),error=>error.code==='ordinary_feedback_jobs_not_ready');
});
