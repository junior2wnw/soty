import test, {type TestContext} from 'node:test';
import assert from 'node:assert/strict';
import { startEmbedLogin } from './embed-login.mjs';

type Popup = {closed:boolean;close():void;location:{replace(value?:string):void}};
function fixture(t:TestContext, fetcher:typeof fetch, popup:Popup|null = { closed: false, close() { this.closed = true; }, location: { replace() {} } }) {
  const globals = new Map(['window', 'location', 'fetch'].map(key => [key, Object.getOwnPropertyDescriptor(globalThis, key)]));
  t.after(() => { for (const [key, value] of globals) if(value) Object.defineProperty(globalThis, key, value); else Reflect.deleteProperty(globalThis,key); });
  Object.defineProperty(globalThis,'window',{value:{open:()=>popup},writable:true,configurable:true});
  Object.defineProperty(globalThis,'location',{value:{origin:'https://selected.fixture.test'},writable:true,configurable:true});globalThis.fetch = fetcher;
  return popup;
}
const preparation = { schema: 'planner.embed-login-preparation.v1', csrf: 'c'.repeat(43), issuer: 'https://root.fixture.test/human-identity',
  clientId: 'selected-fixture', redirectUri: 'https://selected.fixture.test/api/embed/callback' };
const json = (body:unknown) => new Response(JSON.stringify(body), { headers: { 'content-type': 'application/json' } });
const url = () => {
  const value = new URL(preparation.issuer + '/authorize');
  for(const [key,item] of Object.entries({ client_id:preparation.clientId,redirect_uri:preparation.redirectUri,response_type:'code',code_challenge_method:'S256',state:'s'.repeat(43) }))value.searchParams.set(key,item);
  return value.href;
};
test('blocked popup performs no server preparation and preserves explicit retry', async t => {
  let requests = 0; fixture(t, async () => { requests++; assert.fail(); }, null);
  await assert.rejects(startEmbedLogin()); assert.equal(requests, 0);
});
test('unknown prepare acknowledgement closes the blank window and cancels only the captured CSRF intent', async t => {
  const inputs:unknown[]=[];const popup=fixture(t, async(_path, options)=>{
    if(!options?.method)return json(preparation);
    const value=JSON.parse(String(options.body));inputs.push(value);
    if(!value.cancel)throw new Error('synthetic unknown network acknowledgement');return json({cancelled:true});
  });
  await assert.rejects(startEmbedLogin());assert.equal(popup!.closed,true);
  assert.deepEqual(inputs,[{csrf:preparation.csrf},{csrf:preparation.csrf,cancel:true}]);
});
test('caller-independent issuer validation rejects a foreign redirect and cancels instead of navigating', async t=>{
  let navigations=0,cancels=0;const popup={closed:false,close(){this.closed=true;},location:{replace(){navigations++;}}};
  fixture(t,async(_path,options)=>{
    if(!options?.method)return json(preparation);
    if(JSON.parse(String(options.body)).cancel){cancels++;return json({cancelled:true});}
    const foreign=new URL(url());foreign.hostname='foreign.fixture.test';
    return json({schema:'planner.embed-login-authorization.v1',authorizationUrl:foreign.href});
  },popup);
  await assert.rejects(startEmbedLogin());assert.equal(navigations,0);assert.equal(cancels,1);assert.equal(popup.closed,true);
});
