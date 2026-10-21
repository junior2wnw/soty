import test from 'node:test';
import assert from 'node:assert/strict';
import { parseConnectionIntent } from './connection-intent.mjs';
import { takeConnectionNavigation } from './connection-navigation.mjs';
const origin='https://shell.example';
test('cross-shell application navigation preserves the actual single-use invitation',()=>{
  const input=`${origin}/#app/app-${'a'.repeat(32)}?invitation=${'b'.repeat(64)}`;
  const intent=parseConnectionIntent(input,origin);
  const next=takeConnectionNavigation(intent,origin);
  assert.equal(next,input);
  assert.equal(parseConnectionIntent(next,origin).launch.takeInvitation(),'b'.repeat(64));
  assert.throws(()=>takeConnectionNavigation(intent,origin),/connection_invitation_expired/);
});
test('platform handoff is same-origin, closed and consumed once',()=>{
  let held=`${origin}/#soty-invite=${'a'.repeat(43)}`;
  const handoff={takePlatformInvitationLink(){const value=held;held=null;return value;}};
  assert.equal(takeConnectionNavigation({kind:'platform-invitation'},origin,handoff),`${origin}/#soty-invite=${'a'.repeat(43)}`);
  assert.throws(()=>takeConnectionNavigation({kind:'platform-invitation'},origin,handoff),/expired/);
  assert.throws(()=>takeConnectionNavigation({kind:'platform-invitation'},origin,{takePlatformInvitationLink:()=>`https://foreign.example/#soty-invite=${'a'.repeat(43)}`}),/expired/);
});
test('typed navigation cannot smuggle another query or actor flow',()=>{
  assert.equal(takeConnectionNavigation(parseConnectionIntent(`${origin}/#profile/person123`,origin),origin),`${origin}/#profile/person123`);
  assert.throws(()=>takeConnectionNavigation({kind:'contact',id:'a'.repeat(32)},origin),/unavailable/);
  assert.throws(()=>takeConnectionNavigation({kind:'profile',route:'#app/app-'+ 'a'.repeat(32)},origin),/invalid/);
});
