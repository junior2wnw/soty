import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import express from 'express';
import {attachConnectorApi} from '../server/connector-api.js';
import {modelReadiness} from '../deploy/connector/runtime.mjs';

const dir=await mkdtemp(path.join(tmpdir(),'soty-readiness-proof-'));
const api=attachConnectorApi(express(),{dataDir:dir,gonka:{apiKey:'synthetic-provider-key',applicationTokens:JSON.stringify({synthetic:'t'.repeat(48)}),applicationTokensFile:''}});
try {
 await api.store.ready;
 const health={ok:true,agentModelProxy:api.modelProxy,applicationModelProxy:api.applicationModelProxy};
 assert.equal(health.agentModelProxy.ready,true);
 assert.equal(health.applicationModelProxy.ready,true);
 const baseline=modelReadiness(health);
 for(const name of ['agentModelProxy','applicationModelProxy']){
  const actual=baseline[name],source=health[name];
  assert.equal(actual.ready,true);
  assert.equal(actual.model,source.model);
  assert.equal(actual.transport,source.transport);
  assert.equal(actual.upstreamModel,source.upstreamModel);
  assert.equal(actual.routing,'client-choice');
  assert.equal(actual.providerStrategy,source.providerStrategy);
  assert.deepEqual(actual.models,[...source.models].sort());
  assert.equal('providers' in actual,false);
 }
 assert.equal(baseline.applicationModelProxy.path,'/api/inference/v1/chat/completions');
 const noisy={...health,agentModelProxy:{...health.agentModelProxy,models:[...health.agentModelProxy.models].reverse()}};
 Object.defineProperty(noisy.agentModelProxy,'providers',{get(){throw new Error('dynamic provider snapshots must not be read');}});
 assert.deepEqual(modelReadiness(noisy),baseline);
 assert.notDeepEqual(modelReadiness({...health,agentModelProxy:{...health.agentModelProxy,routing:'fixed'}}),baseline);
 for(const broken of [{routing:17},{models:[]},{models:[health.agentModelProxy.model,health.agentModelProxy.model]}]){
  assert.throws(()=>modelReadiness({...health,agentModelProxy:{...health.agentModelProxy,...broken}}),/model_readiness_failed/);
 }
 assert.equal('policySha256' in health.applicationModelProxy,false);
 console.log(JSON.stringify({ok:true,defaultProxyHealth:true,stableRoutingProjection:true,dynamicCountersExcluded:true,noOptionalPolicyRequired:true}));
} finally {
 await api.store.close();
 assert.equal(path.dirname(dir),path.resolve(tmpdir()));
 assert.match(path.basename(dir),/^soty-readiness-proof-/);
 await rm(dir,{recursive:true,force:true});
}
