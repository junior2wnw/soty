import test from 'node:test';
import assert from 'node:assert/strict';
import {generateKeyPairSync,sign} from 'node:crypto';
import {createConnectService,digestArgs} from '../../connect/server/index.mjs';
import {composeMonitoringExtension} from '../../../server/monitoring-module.js';
const origin='https://monitoring-compose.test';
async function connectFixture(t,extension){const key=generateKeyPairSync('ec',{namedCurve:'prime256v1'}),encryption=generateKeyPairSync('ec',{namedCurve:'prime256v1'});const service=createConnectService({databasePath:':memory:',projectId:'compose',allowedOrigins:[origin],extensions:[extension]});t.after(()=>service.close());async function call(op,args={}){const challenge=await service.handle({origin,op:'challenge',args:{operation:op,digest:digestArgs(args)}});assert.equal(challenge.ok,true);return service.handle({origin,op,args,proof:{challengeId:challenge.challengeId,publicJwk:key.publicKey.export({format:'jwk'}),signature:sign('sha256',Buffer.from(challenge.message),{key:key.privateKey,dsaEncoding:'ieee-p1363'}).toString('base64url')}});}assert.equal((await call('bootstrap',{label:'Synthetic composition',encryptionPublicJwk:encryption.publicKey.export({format:'jwk'})})).ok,true);return call;}
test('actual signed Core existing >64KiB result retains the original undefined default before and after composition',async t=>{
 const existing={operations:new Set(['fixture.large']),execute:()=>({value:'x'.repeat(70000)})};
 const before=await connectFixture(t,existing),beforeReply=await before('fixture.large');assert.equal(beforeReply.ok,true);assert.equal(beforeReply.value.length,70000);
 const monitoring={operations:new Set(['monitoring.describe']),execute:()=>({value:'small'})},composed=composeMonitoringExtension(existing,monitoring);
 const after=await connectFixture(t,composed),afterReply=await after('fixture.large');assert.equal(afterReply.ok,true,afterReply.error?.code);assert.equal(afterReply.value,beforeReply.value);
 assert.equal(Object.hasOwn(composed,'maxResultBytes'),false,'undefined preserves Core default instead of imposing a smaller new one');
});
test('an explicitly smaller existing result limit is not widened by the module union',async t=>{
 const existing={operations:new Set(['fixture.small']),maxResultBytes:2048,execute:()=>({value:'x'.repeat(3000)})},monitoring={operations:new Set(['monitoring.describe']),execute:()=>({value:'small'})};
 const before=await connectFixture(t,existing),original=await before('fixture.small');assert.equal(original.ok,false);assert.equal(original.error.code,'extension_result_too_large');
 const after=await connectFixture(t,composeMonitoringExtension(existing,monitoring)),result=await after('fixture.small');assert.equal(result.ok,false);assert.equal(result.error.code,original.error.code);assert.equal((await after('monitoring.describe')).ok,true);
});
test('monitoring branch has its own 64KiB cap while explicit existing bounds and operation snapshots remain intact',async t=>{
 const existing={operations:new Set(['fixture.large']),maxResultBytes:100000,execute:()=>({value:'x'.repeat(70000)})},monitoring={operations:new Set(['monitoring.describe']),execute:()=>({value:'x'.repeat(70000)})},composed=composeMonitoringExtension(existing,monitoring);
 assert.equal(composed.maxResultBytes,100000);const call=await connectFixture(t,composed);assert.equal((await call('fixture.large')).ok,true);const rejected=await call('monitoring.describe');assert.equal(rejected.ok,false);assert.equal(rejected.error.code,'monitoring_response_limit');
 existing.operations.add('fixture.late');assert.throws(()=>composed.execute({op:'fixture.late'}),{code:'monitoring_operation_unavailable'});
});
