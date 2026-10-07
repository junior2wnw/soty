import test from 'node:test';
import assert from 'node:assert/strict';
import { createAppLauncher, validateAppLaunchBinding, sameAppLaunchBinding } from './app-launch.mjs';
const appId='app-'+ 'a'.repeat(32), domainId='dom_'+ 'b'.repeat(32), origin='https://app.fixture.invalid';
const entry={appId,domainId,origin,path:'/'}, url=origin+'/_soty/boot?path=%2F#'+ 't'.repeat(43);
const binding={schema:'soty.app-launch-binding.v1',policyEpoch:1,targetRevision:1,targetDigest:'c'.repeat(64),profile:'soty.relay-restricted.v1',bindingFloor:1};
function fixture(values){const calls=[],state={active:true};const launcher=createAppLauncher({target:{appId},accountId:'account-A',shellUrl:'https://shell.fixture.invalid',
  isCurrent:()=>state.active,request:async args=>{calls.push(args);const next=values.shift();return typeof next==='function'?next():{url,entry,...next};}});return{launcher,calls,state};}
test('a detached read-only binding is pinned from the original admitted response; retry carries no metadata selectors', async()=>{
  const input={...binding},f=fixture([{launchBinding:input},{launchBinding:{...binding}}]);await f.launcher.launch();input.targetRevision=9;
  assert.equal(f.launcher.binding().targetRevision,1);assert.equal(Object.isFrozen(f.launcher.binding()),true);
  assert.equal(await f.launcher.launch({requireSameBinding:true}),url);assert.deepEqual(Object.keys(f.calls[1]).sort(),['appId','domainId','expectedAccountId','path']);
});
test('policy/source/profile/floor drift refuses automatic response even for the same app/domain/name', async()=>{
  for(const change of [{policyEpoch:2},{targetRevision:2},{targetDigest:'d'.repeat(64)},{bindingFloor:2},{profile:'unknown'}]){
    const f=fixture([{launchBinding:binding},{launchBinding:{...binding,...change}}]);await f.launcher.launch();await assert.rejects(f.launcher.launch({requireSameBinding:true}),/app_launch_source_changed|invalid_app_launch_binding/);
    assert.equal(f.launcher.binding().targetRevision,1);
  }
});
test('legacy missing binding remains a working manual launch but cannot be guessed or upgraded by automatic recovery', async()=>{
  const f=fixture([{}, {launchBinding:binding}]);assert.equal(await f.launcher.launch(),url);assert.equal(f.launcher.binding(),null);
  await assert.rejects(f.launcher.launch({requireSameBinding:true}),/app_launch_binding_required/);assert.equal(f.calls.length,1);
  assert.equal(await f.launcher.launch(),url);assert.equal(f.launcher.binding(),null);
});
test('malformed or copied metadata never changes the resource identity or supplies authority', async()=>{
  for(const value of [{...binding,tenant:'caller'},{...binding,policyEpoch:-1},{...binding,targetDigest:'bad'},{...binding,bindingFloor:3}])assert.throws(()=>validateAppLaunchBinding(value),/invalid_app_launch_binding/);
  const f=fixture([{launchBinding:binding},()=>({url,entry:{...entry,appId:'app-'+ 'e'.repeat(32)},launchBinding:{...binding}})]);
  await f.launcher.launch();await assert.rejects(f.launcher.launch({requireSameBinding:true}),/invalid_app_entry/);assert.equal(sameAppLaunchBinding(binding,{...binding}),true,'JSON equality is provenance data, not a capability');
});
test('account/device controller invalidation during signed admission cannot deliver a late recovered iframe URL', async()=>{
  let resolve;const f=fixture([{launchBinding:binding},()=>new Promise(done=>{resolve=done;})]);await f.launcher.launch();const pending=f.launcher.launch({requireSameBinding:true});f.state.active=false;resolve({url,entry,launchBinding:binding});assert.equal(await pending,null);assert.equal(f.launcher.binding(),null);
});
