import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {createScopedGatewayFixture} from './support/scoped-gateway-fixture.mjs';
const random=()=>randomBytes(32).toString('base64url');
const enabled=process.env.SOTY_PLANNER_MINIMUM_ACCESS_GATE==='1';

// Explicit, isolated, real wall-clock test. No Date/issuer/MAC clock override,
// native head editing, caller credentials, production data or model calls.
test('actual installed Source wall AT130 prepares fixed190; revoke and unknown keep Native authority closed',
  {skip:!enabled,timeout:230000,concurrency:3},async t=>{
    await Promise.all(['positive','revoke','unknown'].map(mode=>t.test(mode,async child=>{
      const f=await createScopedGatewayFixture({t:child,renewal:true}),initial=await f.launch();
      assert.equal((await f.nativeConsent(await f.login())).status,200);
      const created=f.planner().store.db.prepare('SELECT document FROM planner_soty_rp_heads').get();
      const before=JSON.parse(created.document),started=Date.now();
      await new Promise(done=>setTimeout(done,170000));
      assert.ok(Date.now()-started>=170000);
      const remaining=before.accessExpiresAt-Date.now();assert.ok(remaining>80000&&remaining<150000,'actual Source AT approximately130s');
      let tokenResponses=0,lost=false,revokePromise;
      const inspectIssuer=(req,res)=>{
        const path=String(req.url).split('?')[0];
        if(path==='/human-identity/token'){
          const end=res.end;res.end=function(chunk,...rest){
            if(res.statusCode===200){tokenResponses++;if(mode==='unknown'&&!lost){lost=true;res.socket?.destroy();return this;}}
            return end.call(this,chunk,...rest);
          };
        }
        if(mode==='revoke'&&path==='/human-identity/userinfo'&&!revokePromise){
          const end=res.end;res.end=function(chunk,...rest){
            revokePromise=f.wire.request(f.native+'/soty/disconnect',{fields:{}});
            void revokePromise.then(()=>end.call(this,chunk,...rest)).catch(()=>res.destroy());return this;
          };
        }
      };
      f.server.prependListener('request',inspectIssuer);
      const requestId=random(),args={appId:f.appId,handle:initial.scopedCloseHandle,requestId};
      const reply=await f.reader.client.extension('apps.scoped.renew',args);
      assert.equal((await f.wire.request(f.embedded+'/_soty/session',{body:{ticket:new URL(reply.launchUrl).hash.slice(1)}})).status,200);
      const response=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId}});
      f.server.removeListener('request',inspectIssuer);
      const head=()=>JSON.parse(f.planner().store.db.prepare('SELECT document FROM planner_soty_rp_heads').get().document);
      if(mode==='positive'){
        assert.equal(response.status,200);assert.equal(response.body?.ready,true);assert.equal(response.body?.renewable,true);
        const context=await f.reader.client.extension('apps.scoped.context',{appId:f.appId,handle:reply.scopedCloseHandle});
        assert.ok(context.expiresAt-Date.now()>190000);assert.ok(context.sourceSession.accessExpiresAt-Date.now()>190000);
        assert.equal(head().revision,1);assert.equal(head().sessionExpiresAt,before.sessionExpiresAt);assert.equal(tokenResponses,1);
        assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200);
        const extra=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:random(),minimumAccessRemainingMs:240000}});
        assert.ok(extra.status>=400);assert.equal(head().revision,1,'author JSON cannot force rotation');
      }else if(mode==='revoke'){
        assert.equal((await revokePromise).status,200);assert.ok(response.status>=400||response.body?.ready===false);
        assert.equal((await f.reader.client.extension('apps.scoped.context',{appId:f.appId,handle:reply.scopedCloseHandle})).sourceSession,undefined);
        assert.equal(f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_grants').get().n,0);
      }else{
        assert.equal(lost,true);assert.ok(response.status>=500);assert.equal(head().state,'unknown');assert.equal(tokenResponses,1);
        assert.equal((await f.reader.client.extension('apps.scoped.context',{appId:f.appId,handle:reply.scopedCloseHandle})).sourceSession,undefined);
        const retry=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId}});
        assert.equal(retry.status,200);assert.ok(retry.body?.ready===false||retry.body?.renewable===false);assert.equal(head().revision,0);assert.equal(tokenResponses,1,'old consumed RT never retried');
      }
    })));
  });
