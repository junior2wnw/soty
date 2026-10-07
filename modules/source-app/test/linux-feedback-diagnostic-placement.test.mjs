import test from 'node:test';
import assert from 'node:assert/strict';
import {createDiagnosticJobsRoot} from '../../../scripts/fixtures/linux-feedback-local-diagnostic-supervisor.mjs';
import {DIAGNOSTIC_LINUX_FEEDBACK_PLACEMENT as placement,createSyntheticDiagnosticLinuxFeedbackProcessor,createSyntheticLocalLinuxFeedbackProcessor} from '../server/linux-feedback-enforcer.mjs';
import {LOCAL_LINUX_FEEDBACK_PLACEMENT as historical} from '../server/linux-feedback-local-placement.mjs';
import {CLEANUP_LINUX_FEEDBACK_PLACEMENT as cleanup,createSyntheticCleanupLinuxFeedbackProcessor} from '../server/linux-feedback-enforcer.mjs';
import {createCleanupJobsRoot} from '../../../scripts/fixtures/linux-feedback-local-cleanup-supervisor.mjs';

test('separate diagnostic engine4 has one fixed fresh namespace; historical WSL engine3 remains exact',()=>{
  assert.equal(historical.directory,historical.lab+'/source-feedback-jobs');
  assert.equal(placement.directory,placement.lab+'/source-feedback-jobs-diag-c962047a89ef40c1b33a105d7e61a924');
  assert.equal(placement.socketHost,historical.socketHost);assert.equal(placement.socketGid,1001);
  assert.equal(createSyntheticLocalLinuxFeedbackProcessor({directory:historical.directory}).ref.version,3);
  const diagnostic=createSyntheticDiagnosticLinuxFeedbackProcessor({directory:placement.directory});assert.equal(diagnostic.ref.version,4);
  assert.equal(diagnostic.productionReady,false);
  for(const directory of [historical.directory,placement.directory+'-other','/tmp'])assert.throws(()=>createSyntheticDiagnosticLinuxFeedbackProcessor({directory}));
});
test('next cleanup engine5 cannot reuse either retained job root or accept a publisher-selected path',async()=>{
  assert.notEqual(cleanup.directory,placement.directory);assert.notEqual(cleanup.directory,historical.directory);
  assert.equal(cleanup.lifecycleCommit,'d2a621b525923d83250933505ae988b4cf3a32b6');
  assert.equal(createSyntheticCleanupLinuxFeedbackProcessor({directory:cleanup.directory}).ref.version,5);
  for(const directory of [placement.directory,historical.directory,'/tmp'])assert.throws(()=>createSyntheticCleanupLinuxFeedbackProcessor({directory}));
  let creates=0;const stat={isDirectory:()=>true,isSymbolicLink:()=>false,uid:1000,mode:0o700};
  await assert.rejects(createCleanupJobsRoot({async lstat(){return stat;},async realpath(path){return path;},async mkdir(path){assert.equal(path,cleanup.directory);creates++;throw Object.assign(Error('exists'),{code:'EEXIST'});}}),error=>error.code==='EEXIST');
  assert.equal(creates,1);
});

test('diagnostic job root refuses existing residue before any Docker create and never deletes or moves it',async()=>{
  let created=0,deleted=0,read=0;
  const stat={isDirectory:()=>true,isSymbolicLink:()=>false,uid:1000,mode:0o700};
  const host={async lstat(){return stat;},async realpath(path){return path;},async mkdir(path){assert.equal(path,placement.directory);created++;throw Object.assign(Error('exists'),{code:'EEXIST'});},
    async readdir(){read++;return ['packet-retained'];},async rm(){deleted++;},async rename(){deleted++;}};
  await assert.rejects(createDiagnosticJobsRoot(host),error=>error.code==='EEXIST');assert.equal(created,1);assert.equal(read,0);assert.equal(deleted,0);
  const operations=[];await createDiagnosticJobsRoot({...host,async mkdir(path){operations.push(path);},async readdir(path){assert.equal(path,placement.directory);return [];}});
  assert.deepEqual(operations,[placement.directory]);
  for(const change of [{uid:0},{mode:0o755},{isSymbolicLink:()=>true}])await assert.rejects(createDiagnosticJobsRoot({...host,async lstat(){return {...stat,...change};}}));
});
