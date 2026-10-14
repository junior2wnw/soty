import test from 'node:test';
import assert from 'node:assert/strict';
import {parseAppLaunchRoute,formatAppLaunchRoute} from './app-launch.mjs';
import {mountAppInvitationCarrier} from './app-invitation-carrier.mjs';
const appId='app-'+ 'a'.repeat(32),token='b'.repeat(64);
test('invitation is one-use local data, absent from canonical route and HTTP target',()=>{
 const intent=parseAppLaunchRoute('#app/'+appId+'?invitation='+token);
 assert.equal(intent.route,'app/'+appId);assert.deepEqual(intent.target,{appId});assert.ok(!JSON.stringify(intent).includes(token));
 assert.equal(formatAppLaunchRoute(intent.target),intent.route);assert.equal(intent.takeInvitation(),token);assert.equal(intent.takeInvitation(),null);
 for(const suffix of ['&path=%2Fembed','&panel=discussion','&invitation='+token])assert.throws(()=>parseAppLaunchRoute('#app/'+appId+'?invitation='+token+suffix));
 assert.throws(()=>parseAppLaunchRoute('#app/'+appId+'?invitation=invalid'));
});
test('only exact current admitted app frame receives once; disposal/account invalidation denies',()=>{
 let listener;const sent=[],peerWindow={postMessage:(data,origin)=>sent.push({data,origin})};let current=true;
 const view={addEventListener:(_name,fn)=>listener=fn,removeEventListener:()=>listener=null};
 const peer={approved:true,appId,window:peerWindow,origin:'https://native.example'};
 const stop=mountAppInvitationCarrier({view,appId,take:()=>token,readPeer:()=>current?peer:null});
 const event={source:peerWindow,origin:peer.origin,data:{schema:'soty.app-invitation-ready.v1',appId,requestId:'c'.repeat(32)}};
 listener({...event,source:{}});listener({...event,origin:'https://foreign.example'});listener({...event,data:{...event.data,appId:'app-'+'d'.repeat(32)}});current=false;listener(event);assert.equal(sent.length,0);
 current=true;listener(event);listener(event);assert.equal(sent.length,1);assert.equal(sent[0].data.token,token);assert.equal(sent[0].origin,peer.origin);stop();assert.equal(listener,null);
 const stop2=mountAppInvitationCarrier({view,appId,take:()=>token,readPeer:()=>peer});const stale=listener;stop2();stale(event);assert.equal(sent.length,1);
});
