import test from 'node:test';
import assert from 'node:assert/strict';
import { capturePlatformInvitation } from '../browser/platform-invitation.mjs';
test('invite fragment is cleared before routing and tab retry never exposes a token through query',()=>{
 const values=new Map(), token='a'.repeat(43); let replaced;
 const view={location:{hash:'#soty-invite='+token,pathname:'/',search:''},history:{state:{keep:true},replaceState(state,title,url){replaced={state,url};}},sessionStorage:{getItem:key=>values.get(key),setItem:(key,value)=>values.set(key,value),removeItem:key=>values.delete(key)}};
 const invitation=capturePlatformInvitation('scope',view);
 assert.equal(invitation.get(),token);assert.equal(replaced.url,'/');assert.deepEqual(replaced.state,{keep:true});
 view.location.hash='';const retry=capturePlatformInvitation('scope',view);assert.equal(retry.get(),token);
 retry.clear();assert.equal(values.size,0);
 view.location.hash='#soty-invite=invalid';assert.equal(capturePlatformInvitation('scope',view).get(),'');
});
