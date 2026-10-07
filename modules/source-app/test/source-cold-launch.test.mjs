import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
import {createRequire} from 'node:module';
import {sourceColdLaunchDiagnostic} from '../install/cold-launch-diagnostic.mjs';
const root=resolve(dirname(fileURLToPath(import.meta.url)),'../../..');
const require=createRequire(import.meta.url),esbuild=createRequire(require.resolve('vite'))('esbuild');

test('actual bundled guardian emits only its closed preflight receipt, never nested backup CLI',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'soty-cold-cli-'));
  try{
    const entry=join(directory,'guardian.mjs');
    await esbuild.build({entryPoints:[join(root,'modules/source-app/install/cold-guardian.mjs')],outfile:entry,
      bundle:true,platform:'node',format:'esm',target:'node24',logLevel:'silent',define:{__SOTY_SOURCE_COLD_BUNDLE__:'true'}});
    const child=spawnSync(process.execPath,[entry,'/public-fixture-invalid/operator.json'],{encoding:'utf8',timeout:5000,maxBuffer:8192,env:{NODE_NO_WARNINGS:'1'}});
    assert.equal(child.status,1);assert.equal(child.signal,null);assert.equal(child.stderr,'');
    assert.deepEqual(JSON.parse(child.stdout),{schema:'soty.source-cold-receipt.v1',passed:false,phase:'guardian',productionReady:false});
    assert.equal(child.stdout.trim().split('\n').length,1);
  }finally{
    assert.equal(dirname(directory),resolve(tmpdir()));assert.match(directory.split(/[\\/]/u).at(-1),/^soty-cold-cli-/u);
    await rm(directory,{recursive:true,force:true,maxRetries:3,retryDelay:25});
  }
});

test('direct unbundled production backup CLI retains its previous closed validation failure',()=>{
  const child=spawnSync(process.execPath,[join(root,'deploy/connect/backup.mjs')],{encoding:'utf8',timeout:5000,maxBuffer:8192,env:{NODE_NO_WARNINGS:'1'}});
  assert.equal(child.status,1);assert.equal(child.signal,null);assert.equal(child.stdout,'');
  assert.deepEqual(JSON.parse(child.stderr),{ok:false,code:'backup_container_invalid'});
});

test('launch diagnostics expose only closed classes and never raw State.Error/CLI details',()=>{
  const error={linuxCliDiagnostic:{exitClass:'nonzero'},message:'SYNTHETIC_PRIVATE_VALUE'};
  const state={Status:'created',ExitCode:128,Error:'error mounting /private/SYNTHETIC_PRIVATE_VALUE: read-only file system'};
  assert.deepEqual(sourceColdLaunchDiagnostic(error,state),{cliExitClass:'nonzero',stateClass:'created',stateErrorClass:'mount_setup_failed',containerExitClass:'nonzero'});
  assert.equal(JSON.stringify(sourceColdLaunchDiagnostic(error,state)).includes('SYNTHETIC_PRIVATE_VALUE'),false);
  assert.deepEqual(sourceColdLaunchDiagnostic({linuxCliDiagnostic:{exitClass:'untrusted'}},{Status:'caller',ExitCode:'1',Error:'untrusted'}),
    {cliExitClass:'none',stateClass:'unknown',stateErrorClass:'other',containerExitClass:'unknown'});
  assert.equal(sourceColdLaunchDiagnostic(undefined,{Status:'exited',ExitCode:0,Error:''}).stateErrorClass,'none');
  const getter=()=>{throw Error('getter_must_not_run');};
  assert.doesNotThrow(()=>sourceColdLaunchDiagnostic(Object.defineProperty({},'linuxCliDiagnostic',{get:getter}),Object.defineProperty({},'Error',{get:getter})));
});
