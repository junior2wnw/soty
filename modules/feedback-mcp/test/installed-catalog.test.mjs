import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, writeFile, mkdtemp, rm } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { loadInstalledApplicationsEntry } from '../../../server/installed-applications-entry.js';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../../source-app/server/standard-profile.mjs';
import { loadInstalledExistingApplications } from '../../../server/installed-existing-applications.js';
import { createCurrentContextSourceApplication } from '../../../server/current-context-source.js';
import { createSelectedNativeSourceApplication } from '../../../server/selected-native-source.js';
import { appendFeedbackSourceApplications } from '../installed-catalog.mjs';
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
    assert.equal(installed.sourceCredentialFor,undefined);
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

test('append preserves old refs, lifecycle, handlers and resolver; duplicate catalog is rejected', async () => {
  const before=await loadInstalledExistingApplications({createRegistry:()=>[createCurrentContextSourceApplication(scope('c'))]});
  const close=()=>{}, sourceCredentialFor=()=>undefined, handlers=Object.freeze([{handle:async()=>false,close}]);
  const old={...before,httpOptions:{...before.httpOptions,nativeSourceHandlers:handlers},close,sourceCredentialFor};
  const next=await appendFeedbackSourceApplications(api,old,scope('a'));
  assert.equal(next.close,close);assert.equal(next.sourceCredentialFor,sourceCredentialFor);
  assert.equal(next.httpOptions.nativeSourceHandlers,handlers);
  assert.deepEqual(next.sourceReferences.slice(0,old.sourceReferences.length),old.sourceReferences);
  assert.equal(next.httpOptions.externalApplications.length,old.httpOptions.externalApplications.length+10);
  assert.equal(old.httpOptions.externalApplications.length,1);
  assert.equal(next.sourceCredentialFor({reference:next.sourceReferences.at(-1)}),undefined);
  await assert.rejects(appendFeedbackSourceApplications(api,next,scope('a')),/already_installed/);
});
