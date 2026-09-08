#!/usr/bin/env node
import {readFile,open,unlink} from 'node:fs/promises';
import {DockerApi,SafeError} from './docker-api.mjs';
import {Rollout} from './rollout.mjs';
import {productionMaintenance,readiness} from './runtime.mjs';
import {journal} from './journal.mjs';
import {readApprovedPolicy} from './application-policy.mjs';
// This CLI never prints inspect objects, request payloads, helper logs or errors.
const opts={};for(let i=3;i<process.argv.length;i+=2){if(!process.argv[i]?.startsWith('--')||!process.argv[i+1])throw new SafeError('invalid_cli');opts[process.argv[i].slice(2)]=process.argv[i+1];}
const action=process.argv[2];let lock,lockPath;
try {
 if(!['prepare','promote'].includes(action)||!opts.journal)throw new SafeError('invalid_cli');
 const args={originalId:opts['original-id'],originalImage:opts['original-image'],candidateImage:opts['candidate-image'],revision:opts.revision,transaction:opts.transaction};
 if(opts['policy-file']||opts['policy-sha256'])args.applicationPolicy=await readApprovedPolicy(opts['policy-file'],opts['policy-sha256']);
 lockPath=opts.journal+'.lock';lock=await open(lockPath,'wx',0o600);
 const engine=new DockerApi();
 const run=new Rollout({engine,maintenance:productionMaintenance(engine),ready:readiness(opts['health-origin']||'http://127.0.0.1:18182'),record:state=>journal(opts.journal,state)});
 if(action==='prepare') {await run.prepare(args);}
 else {
   const prior=JSON.parse(await readFile(opts.journal,'utf8'));
   const approval=JSON.parse(await readFile(opts['reviewed-receipt'],'utf8'));
   if(approval.approved!==true||approval.legacyNoPendingWritesObserved!==true||approval.originalId!==args.originalId||approval.candidateId!==prior.candidateId||approval.configurationSha256!==prior.configurationSha256||approval.revision!==args.revision)throw new SafeError('supervised_receipt_mismatch');
   if((approval.applicationPolicySha256||null)!==(args.applicationPolicy?.sha256||null)||(prior.applicationPolicySha256||null)!==(args.applicationPolicy?.sha256||null))throw new SafeError('supervised_policy_receipt_mismatch');
   await run.guard(args);
   if(prior.phase!=='prepared'||prior.originalId!==args.originalId||prior.candidateImage!==args.candidateImage||prior.transaction!==args.transaction||prior.configurationSha256!==run.fingerprint||prior.modelReadinessSha256!==run.healthSha256)throw new SafeError('prepared_receipt_mismatch');
   run.candidate=await engine.inspect(prior.candidateId);
   run.validateCandidate(run.candidate,{stopped:true});
   const origin=new URL(opts['health-origin']||'http://127.0.0.1:18182');
   if(origin.protocol!=='http:'||origin.hostname!=='127.0.0.1'||!run.original.HostConfig.PortBindings?.['8080/tcp']?.some(b=>b.HostIp==='127.0.0.1'&&b.HostPort===origin.port))throw new SafeError('health_binding_mismatch');
   run.state=prior;await run.promote();
 }
 console.log(JSON.stringify({ok:true,...run.state}));
} catch(error){console.log(JSON.stringify({ok:false,code:error instanceof SafeError?error.code:'rollout_failed'}));process.exitCode=1;}
finally{if(lock){await lock.close();await unlink(lockPath);}}
