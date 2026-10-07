import test from 'node:test';
import assert from 'node:assert/strict';
import { captureSelectedPreparedness,validateUniversalPreparedness } from '../universal-preparedness.mjs';
import { HIVE_SELECTED_SOURCE } from '../../apps/scoped-embed/resource-route-adapters.mjs';
const profile={schema:'soty.selected-human-embed.v2',appId:'app-'+'a'.repeat(32),connector:{linkId:'link',hostDeviceId:'device',connectorId:'connector'},target:{revision:2,digest:'1'.repeat(64)},sourceProfile:HIVE_SELECTED_SOURCE,
  resource:{registryId:'soty',tenantId:'root-owner',appId:'app-'+'a'.repeat(32),environmentId:'production',resourceId:'source:locator',selection:{kind:'hive.project.v1',nativeId:'Приватный Проект / Exact ',incarnationId:'source-uuid'}},
  issuer:'https://root.test/human-identity',clientId:'native-hive',embedOrigin:'https://embedded.root.test',nativeOrigin:'https://native.test',parentOrigin:'https://root.test'};
test('generic selected readiness measures v2 pins privately with distinct trusted v7/v8 migration flags',()=>{
  const old=captureSelectedPreparedness();assert.equal(Object.hasOwn(old,'resourceMigrationConfigured'),false);
  const dto=captureSelectedPreparedness({profiles:[profile],migrationConfigured:false,resourceMigrationConfigured:true});
  assert.equal(dto.migrationConfigured,false);assert.equal(dto.resourceMigrationConfigured,true);assert.equal(dto.profileCount,1);assert.ok(Object.isFrozen(dto));
  for(const value of [profile.resource.selection.nativeId,profile.resource.selection.incarnationId,profile.resource.tenantId,profile.clientId])assert.equal(JSON.stringify(dto).includes(value),false);
  assert.throws(()=>captureSelectedPreparedness({profiles:[profile],resourceMigrationConfigured:'true'}));
  assert.notEqual(captureSelectedPreparedness({profiles:[{...profile,resource:{...profile.resource,selection:{...profile.resource.selection,nativeId:'other'}}}]}).registryDigest,dto.registryDigest);
});
test('old v1 selected measurement preserves exact bytes and unknown readiness grants/fields stay rejected',()=>{
  assert.equal(JSON.stringify(captureSelectedPreparedness()),JSON.stringify(captureSelectedPreparedness({resourceMigrationConfigured:undefined})));
  const selected=captureSelectedPreparedness({resourceMigrationConfigured:true});
  assert.throws(()=>validateUniversalPreparedness({schema:'soty.universal-preparedness.v1',compiledLegacyMode:true,universalConfigured:false,reviewsConfigured:false,humanHttpEnabled:false,
    human:{configured:false},reviews:{configurationDigest:'0'.repeat(64),providerCount:0,bindingCount:0},selected}));
  assert.throws(()=>captureSelectedPreparedness({profiles:[{...profile,ready:true}]}));
});
