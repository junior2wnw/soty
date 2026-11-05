import test from 'node:test';
import assert from 'node:assert/strict';
import { request } from 'node:http';
import {DatabaseSync} from 'node:sqlite';
import {join} from 'node:path';
import { createInvitationFixture } from './http-fixture.mjs';

const code = expected => error => { assert.equal(error.code, expected); return true; };
const form = [{ key: 'f_name', label: 'Имя', type: 'text', required: true }];
test('signed HTTP: private app invitation → form → director approval → actual app HTTP → immediate revoke', { timeout: 25000 }, async t => {
  const f = await createInvitationFixture(); t.after(f.close); const appId = f.appId;
  const current = await f.call(f.owner, 'apps.inspect', { appId });
  await f.call(f.owner, 'apps.publication.update', { appId, requestId: 'private-app', expectedPolicyEpoch: current.publication.policyEpoch,
    expectedTargetRevision: current.source.revision, activeDomainIds: [], launchPolicy: 'restricted', listed: false, audience: 'private' });
  await assert.rejects(f.call(f.member, 'apps.launch', { appId }), code('apps_access_denied'));
  await f.call(f.owner, 'invitations.configure', { appId, requestId: 'configure', expectedRevision: 0, enabled: true, mode: 'approval', description: 'Присоединяйтесь', fields: form });
  const { link } = await f.call(f.owner, 'invitations.issue', { appId, requestId: 'issue', label: 'Для команды', expiresAt: Date.now() + 86400000, maxUses: 10 });
  const publicPreview = await fetch(f.origin + '/api/invitations/preview', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ token: link.token }) });
  assert.equal(publicPreview.status, 200); assert.equal(publicPreview.headers.get('cache-control'), 'no-store');
  const preview = await publicPreview.json(); assert.equal(preview.title, 'Команда проекта'); assert.equal('ownerId' in preview, false);
  await assert.rejects(f.call(f.member, 'invitations.submit', { token: link.token, requestId: 'bad-form', formRevision: 1, answers: {}, consent: true }), code('invitation_required_field'));
  const { request: application } = await f.call(f.member, 'invitations.submit', { token: link.token, requestId: 'apply', formRevision: 1, answers: { f_name: 'Ирина' }, consent: true });
  assert.equal(application.state, 'pending');
  await assert.rejects(f.call(f.outsider, 'invitations.requests', { appId, state: 'pending' }), code('invitation_manager_required'));
  await f.call(f.owner, 'invitations.member.set', { appId, requestId: 'director', accountId: f.director.account.accountId, label: 'Алексей', role: 'director', active: true, expectedRevision: 0 });
  assert.equal((await f.call(f.director, 'invitations.inbox')).items[0].pendingCount, 1);
  await f.call(f.director, 'invitations.decide', { appId, requestId: 'approve', applicationId: application.id, expectedRevision: 1, decision: 'approved' });
  await f.restart(); f.reopenClient(f.member);
  assert.equal((await f.call(f.member,'invitations.status',{token:link.token})).state,'approved','persisted invitation survives Root restart and a fresh client');
  assert.equal((await f.call(f.member, 'apps.list')).apps[0].id, appId);
  assert.equal((await f.call(f.member, 'apps.directory.search', { scope: 'mine' })).apps[0].id, appId);
  assert.equal((await f.call(f.outsider, 'apps.directory.search', { scope: 'all' })).apps.length, 0);
  const launch = await f.call(f.member, 'apps.launch', { appId }), url = new URL(launch.launchUrl);
  const http = (path, method = 'GET', body, cookie) => new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port: new URL(f.origin).port, path, method,
      headers: { host: url.host, origin: url.origin, ...(body ? { 'content-type': 'application/json' } : {}), ...(cookie ? { cookie } : {}) } }, res => {
      const chunks = []; res.on('data', chunk => chunks.push(chunk)); res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString() }));
    }); req.on('error', reject); req.end(body);
  });
  const session = await http('/_soty/session', 'POST', JSON.stringify({ ticket: url.hash.slice(1) })); assert.equal(session.status, 200);
  const cookie = session.headers['set-cookie'][0].split(';')[0];
  const content = await http('/', 'GET', undefined, cookie); assert.equal(content.status, 200); assert.match(content.body, /Покупки/u);
  await f.call(f.director, 'invitations.member.set', { appId, requestId: 'remove-member', accountId: f.member.account.accountId, label: 'Ирина', role: 'member', active: false, expectedRevision: 1 });
  await assert.rejects(f.call(f.member, 'apps.launch', { appId }), code('apps_access_denied'));
  assert.equal((await http('/', 'GET', undefined, cookie)).status, 403);
  assert.equal((await f.call(f.member, 'apps.directory.search', { scope: 'mine' })).apps.length, 0);
  await f.call(f.owner, 'invitations.member.set', { appId, requestId: 'remove-director', accountId: f.director.account.accountId, label: 'Алексей', role: 'director', active: false, expectedRevision: 1 });
  await assert.rejects(f.call(f.director, 'invitations.get', { appId }), code('invitation_manager_required'));
  assert.equal((await f.call(f.owner, 'apps.inspect', { appId })).publication.audience, 'private');
});

test('signed HTTP: a bound private group invite admits only its recipient, persists, and a removed recipient cannot replay access',{timeout:25000},async t=>{
 const f=await createInvitationFixture();t.after(f.close);
 const {communityId}=await f.call(f.owner,'world.community.create',{requestId:'private_group',name:'Private group',joinPolicy:'invite'}),target={kind:'community',id:communityId};
 await f.call(f.owner,'invitations.configure',{target,requestId:'group_policy',expectedRevision:0,enabled:true,mode:'direct',description:'Join',fields:[]});
 const {link}=await f.call(f.owner,'invitations.issue',{target,requestId:'bound_group_link',label:'Сестра',recipientLabel:'Замдиректора',recipientAccountId:f.member.account.accountId,maxUses:1,expiresAt:Date.now()+86400000});
 await assert.rejects(f.call(f.outsider,'world.community.get',{communityId}));
 await assert.rejects(f.call(f.outsider,'invitations.submit',{token:link.token,requestId:'wrong_recipient',formRevision:1,answers:{},consent:true}),code('invitation_recipient_mismatch'));
 const initial=await f.call(f.member,'invitations.status',{token:link.token});assert.equal(initial.state,'none');
 const args={token:link.token,requestId:'correct_recipient',formRevision:1,answers:{},consent:true};
 const result=await f.call(f.member,'invitations.submit',args);assert.equal(result.admission.state,'admitted');assert.equal(result.request.state,'approved');
 assert.equal((await f.call(f.member,'world.community.get',{communityId})).community.membership.role,'member');
 await assert.rejects(f.call(f.member,'invitations.issue',{target,requestId:'label_is_not_manager',label:'No',maxUses:1,expiresAt:Date.now()+86400000}),code('invitation_manager_required'));
 assert.equal((await f.call(f.member,'invitations.status',{token:link.token})).state,'approved');assert.deepEqual(await f.call(f.member,'invitations.submit',args),result);
 await f.restart();f.reopenClient(f.member);assert.equal((await f.call(f.member,'invitations.status',{token:link.token})).state,'approved');assert.equal((await f.call(f.member,'world.community.get',{communityId})).community.membership.state,'active');
 const db=new DatabaseSync(join(f.directory,'data','invitations','invitations.sqlite'),{readOnly:true});
 try{assert.equal(db.prepare('SELECT uses FROM links WHERE id=?').get(link.id).uses,1);assert.equal(db.prepare('SELECT count(*) n FROM members WHERE app_id=?').get(communityId).n,0);}finally{db.close();}
 await f.call(f.owner,'world.membership.remove',{communityId,profileId:f.member.account.accountId});
 const reconciled=await f.call(f.member,'invitations.reconcile',{claimId:result.admission.claimId,requestId:'after_removal'});assert.equal(reconciled.admission.state,'removed');
 await assert.rejects(f.call(f.member,'world.community.get',{communityId}));assert.equal((await f.call(f.member,'invitations.status',{token:link.token})).state,'removed');
 await assert.rejects(f.call(f.outsider,'invitations.reconcile',{claimId:result.admission.claimId,requestId:'outside_reconcile'}),code('invitation_manager_required'));
});

test('signed HTTP: reusable group link maxUses, revoke and account fences survive real requests',{timeout:25000},async t=>{
 const f=await createInvitationFixture();t.after(f.close);
 const {communityId}=await f.call(f.owner,'world.community.create',{requestId:'reusable_group',name:'Shared group',joinPolicy:'invite'}),target={kind:'community',id:communityId};
 await f.call(f.owner,'invitations.configure',{target,requestId:'shared_policy',expectedRevision:0,enabled:true,mode:'direct',description:'Join',fields:[]});
 const {link}=await f.call(f.owner,'invitations.issue',{target,requestId:'shared_link',label:'Для команды',recipientLabel:'Команда',maxUses:2,expiresAt:Date.now()+86400000});
 const submit=person=>f.call(person,'invitations.submit',{token:link.token,requestId:'redeem_shared',formRevision:1,answers:{},consent:true});
 assert.equal((await submit(f.member)).admission.state,'admitted');assert.equal((await submit(f.director)).admission.state,'admitted');
 await assert.rejects(submit(f.outsider),code('invitation_unavailable'));
 await assert.rejects(f.call(f.member,'invitations.get',{target,expectedAccountId:f.owner.account.accountId}),code('invitation_account_changed'));
 await f.call(f.owner,'invitations.revoke',{target,requestId:'revoke_shared',linkId:link.id});
 assert.equal((await f.call(f.member,'world.community.get',{communityId})).community.membership.state,'active');
 await assert.rejects(f.call(f.outsider,'world.community.get',{communityId}));
});
