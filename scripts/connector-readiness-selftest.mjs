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
 assert.deepEqual(modelReadiness(health),{agentModelProxy:api.modelProxy,applicationModelProxy:api.applicationModelProxy});
 assert.equal('policySha256' in health.applicationModelProxy,false);
 console.log(JSON.stringify({ok:true,defaultProxyHealth:true,noOptionalPolicyRequired:true}));
} finally {
 await api.store.close();
 assert.equal(path.dirname(dir),path.resolve(tmpdir()));
 assert.match(path.basename(dir),/^soty-readiness-proof-/);
 await rm(dir,{recursive:true,force:true});
}
