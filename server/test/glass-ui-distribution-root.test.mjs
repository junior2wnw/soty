import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {nativeHttpFixture} from './support/native-capability-http.mjs';

test('a new external author discovers and downloads the actual shared Glass UI through the existing module catalog', {timeout:20000}, async t=>{
 const f=await nativeHttpFixture(t,{appOrigins:true});
 const discovery=await f.http('/.well-known/soty-app-platform');assert.equal(discovery.status,200);
 const catalog=await f.http('/api/app-agent-directory/modules');assert.equal(catalog.status,200);
 const module=catalog.body.modules.find(value=>value.id==='soty.optical-glass');
 assert.ok(module,'the reusable UI is discoverable without signing into a personal account');
 assert.equal(module.kind,'sdk');assert.equal(module.readiness,'available');assert.deepEqual(module.operations,[],'UI code never grants a platform operation');
 assert.equal(discovery.body.glassUi.documentation,module.documentation);
 const documentation=await fetch(f.origin+module.documentation);assert.equal(documentation.status,200);assert.match(await documentation.text(),/glass-ui__widget/u);
 assert.equal(discovery.body.glassUi.widgetContext,'/agents/sdk/glass-ui/src/widget-context.js');
 for(const name of ['ui-kit.css','ui-kit.js','site.js','liquid-glass.js','widget-context.js','widget-context.d.ts']){
  const url='/agents/sdk/glass-ui/src/'+name,response=await fetch(f.origin+url);assert.equal(response.status,200,url);
  assert.equal(response.headers.get('access-control-allow-origin'),'*');
  assert.equal(await response.text(),readFileSync(new URL('../../vendor/optical-glass/src/'+name,import.meta.url),'utf8'),'the consumer receives the maintained file, not a second implementation');
  const head=await fetch(f.origin+url,{method:'HEAD'});assert.equal(head.status,200);assert.equal((await head.arrayBuffer()).byteLength,0);
 }
 const css=await fetch(f.origin+discovery.body.glassUi.styles);assert.equal(css.status,200);assert.match(css.headers.get('content-type'),/text\/css/u);
 const denied=await fetch(f.origin+discovery.body.glassUi.styles,{method:'POST'});assert.equal(denied.status,405);
 const unknown=await fetch(f.origin+'/agents/sdk/glass-ui/private-config.json');assert.equal(unknown.status,404);
});
