import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, writeFile, mkdtemp, rm } from 'node:fs/promises';
import { createHash, generateKeyPairSync, randomUUID, sign } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { loadInstalledApplicationsEntry } from '../../../server/installed-applications-entry.js';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../../source-app/server/standard-profile.mjs';
import { loadInstalledExistingApplications } from '../../../server/installed-existing-applications.js';
import { createCurrentContextSourceApplication } from '../../../server/current-context-source.js';
import { createSelectedNativeSourceApplication } from '../../../server/selected-native-source.js';
import { appendFeedbackSourceApplications } from '../installed-catalog.mjs';
import { createConnectService, digestArgs } from '../../connect/server/index.mjs';
import { createCapabilitiesService } from '../../capabilities/server/index.mjs';
import { createNotesService } from '../../notes/server/index.mjs';
import { createPersonalAgentComposition } from '../../../server/personal-agent-composition.js';
import { captureExternalApplications, composeExternalApplications } from '../../../server/external-applications.js';
import { GLM_MODEL } from '../../personal-agent/provider/index.mjs';
const sha = value => createHash('sha256').update(value).digest('hex');
const api = {loadInstalledExistingApplications,createCurrentContextSourceApplication,createSelectedNativeSourceApplication};
const scope = letter => ({appId:'app-'+letter.repeat(32),target:{revision:1,digest:'b'.repeat(64)},sourceProfile:STANDARD_ROOT_WORKSPACE_SOURCE,
 resource:{registryId:'soty',environmentId:'synthetic',tenantId:'publisher',appId:'app-'+letter.repeat(32),resourceId:'feedback',selection:{kind:'soty.workspace.v1',nativeId:'feedback',incarnationId:'one'}}});

test('actual Root loader admits all Feedback rows alongside the previous catalog without granting authority', async () => {
  const fragment = await readFile(new URL('../installed-catalog.mjs',import.meta.url),'utf8');
  const folder = await mkdtemp(join(tmpdir(),'feedback-installed-')); let installed;
  try {
    // Exact-byte loader imports a data URL and allows default export only.
    const body=fragment.replaceAll('export function ','function ').replaceAll('export async function ','async function ')+`
      async function oldFactory(api) {return {...await api.loadInstalledExistingApplications({createRegistry:()=>[api.createCurrentContextSourceApplication(${JSON.stringify(scope('c'))})]}),close(){}};}
      export default async api => appendFeedbackSourceApplications(api,await oldFactory(api),${JSON.stringify(scope('a'))});`;
    const file=join(folder,'operator.mjs'); await writeFile(file,body);
    installed=await loadInstalledApplicationsEntry({SOTY_INSTALLED_APPLICATIONS_MODULE:file,SOTY_INSTALLED_APPLICATIONS_SHA256:sha(body)});
    assert.equal(installed.sourceReferences.length,11);
    assert.equal(installed.sourceReferences[0].capabilityId,'app.'+scope('c').appId+':current_context');
    assert.throws(()=>installed.sourceCredentialFor({reference:installed.sourceReferences.at(-1)}),{code:'authorization_required'});
    assert.throws(()=>installed.sourceCredentialFor({reference:installed.sourceReferences[0]}),{code:'authorization_required'});
    const rows=installed.httpOptions.externalApplications.slice(1);
    assert.equal(rows.length,10);
    assert.deepEqual(rows.slice(1).map(row=>row.catalog.effects),[[],['create'],[],[],[],['create'],['update'],['update'],['update']]);
    for(const row of rows.slice(1)) {
      const input=row.catalog.inputSchema.properties.input;
      assert.equal(Object.hasOwn(input.properties,'requestId'),false);
      assert.deepEqual(input.properties.operation.enum,[row.catalog.capabilityId.split(':feedback.')[1]]);
      assert.equal(row.catalog.inputSchema.required.includes('contextRef'),true);
    }
    assert.equal(rows[2].catalog.outputSchema.properties.outcome.enum[0],'committed');
    await assert.rejects(loadInstalledApplicationsEntry({SOTY_INSTALLED_APPLICATIONS_MODULE:file,SOTY_INSTALLED_APPLICATIONS_SHA256:'0'.repeat(64)}));
  } finally { await installed?.close(); await rm(folder,{recursive:true,force:true,maxRetries:5,retryDelay:40}); }
});

test('append preserves old refs, lifecycle, handlers and resolver behavior; new refs fail closed without breaking discovery', async () => {
  const before=await loadInstalledExistingApplications({createRegistry:()=>[createCurrentContextSourceApplication(scope('c'))]});
  const calls=[], credential={token:'synthetic-no-live-authority',audience:'https://fixture.invalid'}, close=()=>{},
    sourceCredentialFor=request=>{calls.push(request);return credential;}, handlers=Object.freeze([{handle:async()=>false,close}]);
  const old={...before,httpOptions:{...before.httpOptions,nativeSourceHandlers:handlers},close,sourceCredentialFor};
  const next=await appendFeedbackSourceApplications(api,old,scope('a'));
  assert.equal(next.close,close);
  assert.equal(next.httpOptions.nativeSourceHandlers,handlers);
  assert.deepEqual(next.sourceReferences.slice(0,old.sourceReferences.length),old.sourceReferences);
  assert.equal(next.httpOptions.externalApplications.length,old.httpOptions.externalApplications.length+10);
  assert.equal(old.httpOptions.externalApplications.length,1);
  for(const reference of next.sourceReferences.slice(old.sourceReferences.length))
    assert.throws(()=>next.sourceCredentialFor({reference}),{code:'authorization_required'});
  assert.equal(calls.length,0);
  const request={actor:{synthetic:true},reference:old.sourceReferences[0]};
  assert.equal(next.sourceCredentialFor(request),credential);assert.equal(calls[0],request);
  await assert.rejects(appendFeedbackSourceApplications(api,next,scope('a')),/already_installed/);
});

test('actual signed agent catalog discovery still finds an old granted Source with ten unconnected Feedback refs', async t => {
  const previous=await loadInstalledExistingApplications({createRegistry:()=>[createCurrentContextSourceApplication(scope('c'))]});
  let credential,connect,capabilities,personal;
  const installed=await appendFeedbackSourceApplications(api,{...previous,sourceCredentialFor:()=>credential},scope('a'));
  const entries=captureExternalApplications(installed.httpOptions.externalApplications), origin='https://feedback-catalog.fixture.invalid';
  const notes=createNotesService({databasePath:':memory:',projectId:'soty'}), apps={
    // Only the Native app target port is synthetic; discovery never reads data.
    withAppAuthority({actor,appId},callback){assert.ok(connect.isActorActive(actor));
      assert.ok([scope('a').appId,scope('c').appId].includes(appId));return callback({target:scope('c').target});}
  };
  capabilities=createCapabilitiesService({databasePath:':memory:',projectId:'soty',documentation:[],
    actorActive:actor=>connect.isActorActive(actor),catalog:entries.map(row=>row.catalog),
    readonlyQueries:composeExternalApplications(entries.filter(row=>row.readonly),{
      withConnectFence:callback=>connect.withAuthorityFence(callback),capabilities:()=>capabilities,apps:()=>apps})});
  const receipts=new Map(), modelBodies=[], tenant='synthetic_feedback_catalog', keyRef={};
  personal=createPersonalAgentComposition({getConnect:()=>connect,getServices:()=>({notes,apps,capabilities}),configuration:{
    enabled:true,historyEnabled:false,sourceReferences:installed.sourceReferences,sourceCredentialFor:installed.sourceCredentialFor,
    gateway:{gatewayOrigin:'https://synthetic-provider.invalid',billingTenant:tenant,keyRef,authorizeActor:()=>true,
      resolveCredential:async()=>({keyRef,tenantId:tenant,bearer:'synthetic-no-provider'}),
      readReceipt:async({requestId})=>({tenantId:tenant,request:receipts.get(requestId)}),
      fetchImpl:async(_url,init)=>{
        const body=JSON.parse(init.body);modelBodies.push(body);
        const evidence=body.messages.at(-1).role==='tool'?JSON.parse(body.messages.at(-1).content).evidence:null;
        const name=evidence?'finish_answer':'source_catalog', args=evidence?
          {answer:'Existing Source remains available',selectedEvidenceIds:evidence.map(row=>row.id),claims:[]}:{};
        const id=randomUUID();receipts.set(id,{id,status:'completed',model_requested:GLM_MODEL,model_resolved:GLM_MODEL,
          input_tokens:3,output_tokens:2,charge_urub:'0'});
        return Response.json({id,model:GLM_MODEL,choices:[{index:0,finish_reason:'tool_calls',message:{role:'assistant',
          tool_calls:[{id:'synthetic_catalog_call_'+modelBodies.length,type:'function',function:{name,arguments:JSON.stringify(args)}}]}}],
          usage:{prompt_tokens:3,completion_tokens:2,total_tokens:5}},{headers:{'x-soty-request-id':id}});
      }}
  }});
  connect=createConnectService({databasePath:':memory:',projectId:'soty',allowedOrigins:[origin],extensions:[notes,capabilities,personal]});
  t.after(()=>{personal.close();capabilities.close();notes.close();connect.close();});
  const identity=generateKeyPairSync('ec',{namedCurve:'prime256v1'}),publicJwk=identity.publicKey.export({format:'jwk'});
  const good=result=>{assert.equal(result.ok,true,result.error?.code);return result;};
  async function rpc(op,args={}) {
    const challenge=good(await connect.handle({origin,op:'challenge',args:{operation:op,digest:digestArgs(args)}}));
    return connect.handle({origin,op,args,proof:{challengeId:challenge.challengeId,publicJwk,
      signature:sign('sha256',Buffer.from(challenge.message),{key:identity.privateKey,dsaEncoding:'ieee-p1363'}).toString('base64url')}});
  }
  const owner=good(await rpc('bootstrap',{label:'Synthetic catalog owner',encryptionPublicJwk:publicJwk}));
  const principal=good(await rpc('access.principals.create',{expectedAccountId:owner.accountId,label:'Existing explicit Source grant'})).principal;
  const old=entries[0],grant=good(await rpc('access.grants.issue',{expectedAccountId:owner.accountId,principalId:principal.id,
    capabilities:[{capabilityId:old.contract.capabilityId,version:old.contract.version}],resources:old.catalog.resources,
    effects:[],recipients:old.catalog.recipients,expiresAt:Date.now()+600000,budget:{unit:'invocations',limit:10}})).grant;
  const issued=good(await rpc('access.credentials.issue',{expectedAccountId:owner.accountId,grantId:grant.id,audience:origin}));
  credential={token:issued.token,audience:origin};
  const result=good(await rpc('agent.turn',{requestId:'feedback_catalog_regression_001',prompt:'List my permitted Source tools'}));
  assert.equal(modelBodies.length,2);
  assert.deepEqual(result.evidence[0].data.items.map(row=>row.reference.capabilityId),[old.contract.capabilityId]);
  assert.equal(JSON.stringify(modelBodies).includes(issued.token),false);
});
