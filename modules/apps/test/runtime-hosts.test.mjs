import test from 'node:test';
import assert from 'node:assert/strict';
import { parseWidgetRuntimeHost, widgetRuntimeOrigin, captureWidgetRuntimeScope, widgetRuntimeScopeDigest,parseMainRuntimeHost,mainRuntimeOrigin,captureMainRuntimeScope } from '../server/runtime-hosts.mjs';

const widgetId='widget-11111111-2222-4333-8444-555555555555',appId='app-'+'a'.repeat(32);
const widget=()=>({widgetId,revision:1,state:'active',appId,entry:{domainId:'dom_'+'a'.repeat(32),path:'/app'},source:{revision:1,digest:'b'.repeat(64),profile:'soty.relay-restricted.v1'}});
const decision=()=>({subject:'account',actor:{accountId:'account-a',deviceId:'device-a'},appId,domainId:widget().entry.domainId,origin:'https://app.example',policyEpoch:1,targetRevision:1,targetDigest:'b'.repeat(64),profile:'soty.relay-restricted.v1',requiredBindingVersion:2});
const scope=(d=decision(),w=widget())=>captureWidgetRuntimeScope(d,w,w.entry.path);
const origin=(s=scope())=>widgetRuntimeOrigin(s.widgetId,widgetRuntimeScopeDigest(s).slice(0,24),'https://apps.example');

test('DNS selector is reserved flat62-character label and separates full current public scope',()=>{
 const first=origin(),parsed=parseWidgetRuntimeHost(new URL(first).hostname);
 assert.equal(new URL(first).hostname.split('.')[0].length,62);assert.equal(parsed.widgetId,widgetId);
 assert.equal(origin(),first,'same logical widget/principal/device/binding intentionally retains origin');
 for(const change of [d=>d.actor.accountId='account-b',d=>d.actor.deviceId='device-b',d=>d.policyEpoch++,d=>d.origin='https://other.example',d=>d.requiredBindingVersion=1]){
  const d=decision();change(d);assert.notEqual(origin(scope(d)),first);
 }
 const w=widget();w.revision++;assert.notEqual(origin(scope(decision(),w)),first);
 const d=decision(),v=widget();d.targetRevision++;v.source.revision++;assert.notEqual(origin(scope(d,v)),first);
 const d2=decision(),v2=widget();d2.targetDigest='c'.repeat(64);v2.source.digest=d2.targetDigest;assert.notEqual(origin(scope(d2,v2)),first);
 const captured=scope();assert.ok(Object.isFrozen(captured)&&Object.isFrozen(captured.entry)&&Object.isFrozen(captured.target));
});
test('malformed or stale metadata and unsafe selectors do not create origins',()=>{
 for(const mutate of [w=>w.state='removed',w=>w.revision=NaN,w=>w.appId='app-'+'c'.repeat(32),w=>w.source.digest='c'.repeat(64),w=>w.entry.path='/different']){
  const w=widget();mutate(w);assert.throws(()=>captureWidgetRuntimeScope(decision(),w,'/app'));
 }
 for(const host of ['app-w'+'1'.repeat(32)+'-'+'a'.repeat(23)+'.apps.example','APP-W'+'1'.repeat(32)+'-'+'a'.repeat(24)+'.apps.example','app-w'+'1'.repeat(32)+'-'+'a'.repeat(24)+'.apps.example:443','app-w'+'1'.repeat(32)+'-'+'a'.repeat(24)+'.apps..example'])assert.equal(parseWidgetRuntimeHost(host),null);
 assert.throws(()=>widgetRuntimeOrigin(widgetId,'a'.repeat(24),'https://apps.example/path'));
 assert.throws(()=>widgetRuntimeOrigin(widgetId,'a'.repeat(24),'http://apps.example'));
});

test('main runtime namespace separates principal/device/entry/target/policy while canonical entry remains metadata',()=>{
 const build=(d=decision(),path='/app')=>mainRuntimeOrigin(d.appId,widgetRuntimeScopeDigest(captureMainRuntimeScope(d,path)).slice(0,24),'https://apps.example');
 const first=build(),parsed=parseMainRuntimeHost(new URL(first).hostname);
 assert.equal(parsed.appId,appId);assert.equal(new URL(first).hostname.split('.')[0].length,62);assert.equal(build(),first);
 for(const change of[d=>d.actor.accountId='different',d=>d.actor.deviceId='different',d=>d.policyEpoch++,d=>d.targetRevision++,d=>d.targetDigest='d'.repeat(64),d=>d.origin='https://other.example',d=>d.requiredBindingVersion=1]){const current=decision();change(current);assert.notEqual(build(current),first);}
 assert.notEqual(build(decision(),'/other'),first);
 assert.equal(parseMainRuntimeHost(new URL(first).hostname.toUpperCase()),null);
 assert.throws(()=>captureMainRuntimeScope({...decision(),subject:'public'},'/app'));
});
