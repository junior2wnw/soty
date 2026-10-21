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


// Actual one-use AppLaunchIntent; peer/window ports only expose controlled admission.
function lazyCarrierFixture() {
 const original=parseAppLaunchRoute('#app/'+appId+'?invitation='+token),sent=[];
 let listener=null,takes=0,current=true,approved=true;
 const peerWindow={postMessage:(data,origin)=>sent.push({data,origin})};
 const peer={approved:true,appId,window:peerWindow,origin:'https://native.example'};
 const view={addEventListener:(_name,fn)=>listener=fn,removeEventListener:(_name,fn)=>{if(listener===fn)listener=null;}};
 const readPeer=()=>current?{...peer,approved}:null;
 const take=()=>{takes++;return original.takeInvitation();};
 const event={source:peerWindow,origin:peer.origin,data:{schema:'soty.app-invitation-ready.v1',appId,requestId:'c'.repeat(32)}};
 return {view,peer,event,original,sent,take,readPeer,get listener(){return listener;},get takes(){return takes;},setCurrent:value=>{current=value;},setApproved:value=>{approved=value;}};
}
test('unused invitation stays in its actual carrier at mount and disposal',()=>{
 const f=lazyCarrierFixture(),stop=mountAppInvitationCarrier({view:f.view,appId,take:f.take,readPeer:f.readPeer});
 assert.equal(f.takes,0);assert.equal(f.sent.length,0);stop();assert.equal(f.takes,0);
 assert.equal(f.original.takeInvitation(),token,'disposal did not redeem the unoffered invitation');
});
test('unknown schema and wrong or unapproved peers never take an invitation',()=>{
 const f=lazyCarrierFixture(),stop=mountAppInvitationCarrier({view:f.view,appId,take:f.take,readPeer:f.readPeer});
 const send=f.listener;
 for(const data of [{...f.event.data,schema:'foreign'}, {...f.event.data,extra:true}, {...f.event.data,approved:true}, {...f.event.data,requestId:'bad'}, {...f.event.data,appId:'app-'+'d'.repeat(32)}])send({...f.event,data});
 send({...f.event,source:{}});send({...f.event,origin:'https://foreign.example'});
 f.setApproved(false);send(f.event);f.setCurrent(false);send(f.event);
 assert.equal(f.takes,0);assert.equal(f.sent.length,0);stop();
});
test('a queued ready event after disposal cannot take an unused invitation',()=>{
 const f=lazyCarrierFixture(),stop=mountAppInvitationCarrier({view:f.view,appId,take:f.take,readPeer:f.readPeer}),queued=f.listener;
 stop();queued(f.event);assert.equal(f.takes,0);assert.equal(f.sent.length,0);assert.equal(f.listener,null);
 assert.equal(f.original.takeInvitation(),token);
});
test('one exact currently approved ready event takes and delivers the genuine carrier once',()=>{
 const f=lazyCarrierFixture(),stop=mountAppInvitationCarrier({view:f.view,appId,take:f.take,readPeer:f.readPeer});
 f.setCurrent(false);f.listener(f.event);assert.equal(f.takes,0);f.setCurrent(true);
 f.listener(f.event);f.listener({...f.event,data:{...f.event.data,requestId:'e'.repeat(32)}});
 assert.equal(f.takes,1);assert.equal(f.sent.length,1);
 assert.deepEqual(f.sent[0],{origin:f.peer.origin,data:{schema:'soty.app-invitation.v1',appId,requestId:'c'.repeat(32),token}});
 assert.equal(f.original.takeInvitation(),null);stop();
});
test('disposal during a fresh peer read and reentrant ready events cannot consume or replay',()=>{
 const cancelled=lazyCarrierFixture();let stop;
 stop=mountAppInvitationCarrier({view:cancelled.view,appId,take:cancelled.take,readPeer:()=>{stop();return cancelled.peer;}});
 cancelled.listener(cancelled.event);assert.equal(cancelled.takes,0);assert.equal(cancelled.sent.length,0);
 const f=lazyCarrierFixture();let nested;
 const close=mountAppInvitationCarrier({view:f.view,appId,take:()=>{nested(f.event);return f.take();},readPeer:f.readPeer});nested=f.listener;
 nested(f.event);assert.equal(f.takes,1);assert.equal(f.sent.length,1);close();
});
