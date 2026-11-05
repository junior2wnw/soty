import test from 'node:test';
import assert from 'node:assert/strict';
import {createAppModuleCatalog,MONITORING_MODULE} from '../app-agent-directory-modules.js';
import {MONITORING_OPERATIONS} from '../../modules/monitoring/definitions.mjs';
import {createMonitoringDescriptor} from '../../modules/monitoring/descriptor.mjs';
import {DEFAULT_FEEDBACK_PROFILE} from '../../modules/feedback/server/profile.mjs';
test('monitoring is an installed reusable module descriptor; no app IDs or statistics grant is published',()=>{
 const catalog=createAppModuleCatalog({entries:()=>[MONITORING_MODULE]}).list();const module=catalog.modules.find(value=>value.id==='soty.monitoring');assert.deepEqual(module.operations,[...MONITORING_OPERATIONS]);assert.equal(module.documentation,'/agents/monitoring.md');assert.match(catalog.authority,/separate grants/);assert.equal(JSON.stringify(module).includes('app-'),false);
});
test('two independent author descriptors reuse one monitoring contract without root code/authority from JSON',()=>{
 const ref=id=>({id,version:1,digest:'c'.repeat(64)}),bindings=Object.fromEntries(['read','configure','admin','record'].map(right=>[right,ref('bindings:'+right)]));
 const build=name=>createMonitoringDescriptor({appId:name,namespace:name,title:name,source:{id:'sources:'+name,revision:1,digest:'b'.repeat(64)},auth:{mode:'shared-soty',profile:ref('auth:shared')},resource:ref(name+':metrics'),bindings,feedback:DEFAULT_FEEDBACK_PROFILE.feedback});
 const first=build('first'),second=build('second');assert.equal(first.capabilities.length,7);assert.equal(second.capabilities.length,7);assert.notEqual(first.app.id,second.app.id);assert.equal(first.capabilities[0].binding.id,second.capabilities[0].binding.id);assert.equal(JSON.stringify(first).includes('http://'),false);assert.throws(()=>createMonitoringDescriptor({appId:'forged'}),{code:'monitoring_descriptor_host_bindings_required'});
});
