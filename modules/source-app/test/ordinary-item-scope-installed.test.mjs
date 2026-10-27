import test from 'node:test';
import assert from 'node:assert/strict';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createOrdinaryInstalledFixture} from './support/ordinary-installed.mjs';

test('installed ordinary item scope uses current Native identity and survives Source restart',{timeout:120000},async t=>{
  const sdk=process.env.SOTY_SOURCE_TEST_SDK_DIRECTORY;
  const {createOrdinaryAppServer}=await import(sdk?pathToFileURL(join(sdk,'dist','example.mjs')).href:'../examples/ordinary-app/index.mjs');
  async function nativeFactory({realm,initialize}){
    const app=await createOrdinaryAppServer({...realm.options,initialize,...(initialize?{newResource:{title:'Synthetic selected project'}}:{})});
    if(initialize){app.store.createPrincipal('native-participant');app.store.grant('selected','native-participant','participant');
      realm.nativeToken=app.store.createNativeSession('native-participant');}
    return app;
  }
  const f=await createOrdinaryInstalledFixture(t,{realmIds:['board'],nativeFactory}),realm=f.realms[0];
  await realm.launch();await f.login(realm);
  async function read(operation){const response=await f.wire(realm.embedded+'/api/embed/query',{body:{requestId:'ordinary-scope-installed-0001',input:{operation}}});
    assert.equal(response.status,200,response.text);return response.value.data;}
  const original=await read('items.pending-scope');assert.match(original.scope,/^[a-f0-9]{64}$/u);
  const list=await read('items.list');assert.deepEqual(Object.keys(list).sort(),['items','requestId']);
  await realm.restart();assert.equal((await read('items.pending-scope')).scope,original.scope);
});
