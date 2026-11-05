import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInvitationsService } from '../server.mjs';
import { fields, answers, invitationUrl, parseInvitationRoute } from '../model.mjs';

const appId = 'app-' + '1'.repeat(32), anotherApp = 'app-' + '2'.repeat(32);
const owner = { accountId: 'owner', deviceId: 'owner-device' }, director = { accountId: 'director', deviceId: 'director-device' },
  member = { accountId: 'member', deviceId: 'member-device' }, outsider = { accountId: 'outsider', deviceId: 'outsider-device' };
const form = [{ key: 'f_name', label: 'Имя', type: 'text', required: true }, { key: 'f_age', label: 'Возраст', type: 'number', required: false, min: 0, max: 130 }];
const code = expected => error => { assert.equal(error.code, expected); return true; };
function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-invitations-'));
  let time = 1000000, counter = 0, active = true, targetOwner = owner.accountId;
  const revoked = new Set(), changes = [];
  const options = { databasePath: join(directory, 'invitations.sqlite'), clock: () => time,
    actorActive: actor => !revoked.has(actor.accountId),
    withTarget({ appId: id }, callback) { if (!active || ![appId, anotherApp].includes(id)) throw Object.assign(new Error('unavailable'), { code: 'invitation_unavailable', status: 404 });
      return callback({ appId: id, ownerId: id === anotherApp ? outsider.accountId : targetOwner, title: 'Рабочая команда' }); },
    ownedTargets: actor => actor.accountId === owner.accountId ? [appId] : [], onAccessChanged: value => changes.push(value) };
  let service = createInvitationsService(options);
  t.after(() => { service.close(); rmSync(directory, { recursive: true, force: true }); });
  const call = (actor, op, args = {}) => service.execute({ actor, op, args: { expectedAccountId: actor.accountId, ...args } });
  const mutate = (actor, op, args) => call(actor, op, { requestId: `command.${++counter}`, ...args });
  const configure = (extra = {}) => mutate(owner, 'invitations.configure', { appId, expectedRevision: call(owner, 'invitations.get', { appId }).policy.revision,
    enabled: true, mode: 'approval', description: 'Представьтесь организатору', fields: form, ...extra });
  const issue = (extra = {}) => mutate(owner, 'invitations.issue', { appId, label: 'Команда', expiresAt: time + 86400000, maxUses: 0, ...extra }).link;
  const submit = (link, actor = member, extra = {}) => mutate(actor, 'invitations.submit', { token: link.token, formRevision: service.preview(link.token).formRevision,
    answers: { f_name: 'Ирина', f_age: 28 }, consent: true, ...extra }).request;
  return { call, mutate, configure, issue, submit, changes, revoked, get service() { return service; },
    restart() { service.close(); service = createInvitationsService(options); }, advance(value) { time += value; }, disable() { active = false; },
    transfer() { targetOwner = outsider.accountId; } };
}

test('approval grants only the applicant and persists with the same form, links, requests and membership', t => {
  const f = fixture(t); f.configure(); const link = f.issue();
  const preview = f.service.preview(link.token);
  assert.equal(preview.title, 'Рабочая команда'); assert.equal('ownerId' in preview, false); assert.equal('token' in preview, false);
  const request = f.submit(link); assert.equal(request.state, 'pending');
  assert.equal(f.service.hasAccess({ appId, ownerId: owner.accountId, accountId: member.accountId }), false);
  assert.throws(() => f.call(outsider, 'invitations.requests', { appId, state: 'pending' }), code('invitation_manager_required'));
  f.mutate(owner, 'invitations.decide', { appId, applicationId: request.id, expectedRevision: request.revision, decision: 'approved' });
  assert.equal(f.service.hasAccess({ appId, ownerId: owner.accountId, accountId: member.accountId }), true);
  assert.equal(f.service.hasAccess({ appId, ownerId: owner.accountId, accountId: outsider.accountId }), false);
  f.restart();
  assert.equal(f.call(owner, 'invitations.get', { appId }).links[0].token, link.token);
  assert.deepEqual(f.call(owner, 'invitations.request.get', { appId, applicationId: request.id }).request.answers, request.answers);
  assert.deepEqual(f.service.appIds(member.accountId), [appId]);
  assert.equal(f.call(member, 'invitations.status', { token: link.token }).state, 'approved');
});

test('retries recover a one-use direct invitation after expiry without duplicate consumption', t => {
  const f = fixture(t); f.configure({ mode: 'direct' }); const link = f.issue({ maxUses: 1 });
  const args = { requestId: 'stable-submit', token: link.token, formRevision: 1, answers: { f_name: 'Ирина' }, consent: true };
  const first = f.call(member, 'invitations.submit', args); f.advance(2 * 86400000);
  assert.deepEqual(f.call(member, 'invitations.submit', args), first);
  assert.equal(f.call(owner, 'invitations.get', { appId }).links[0].uses, 1);
  assert.equal(f.call(member, 'invitations.status', { token: link.token }).state, 'approved');
  assert.throws(() => f.call(member, 'invitations.submit', { ...args, answers: { f_name: 'Другое' } }), code('invitation_request_conflict'));
});

test('director can review but cannot appoint directors or retain management after revocation', t => {
  const f = fixture(t); f.configure(); const link = f.issue(); const request = f.submit(link);
  const directorArgs = { appId, accountId: director.accountId, label: 'Руководитель', role: 'director', active: true, expectedRevision: 0 };
  f.mutate(owner, 'invitations.member.set', directorArgs);
  assert.equal(f.call(director, 'invitations.inbox').items[0].pendingCount, 1);
  f.mutate(director, 'invitations.decide', { appId, applicationId: request.id, expectedRevision: 1, decision: 'approved' });
  assert.throws(() => f.mutate(director, 'invitations.member.set', { ...directorArgs, accountId: outsider.accountId }), code('invitation_owner_required'));
  f.mutate(owner, 'invitations.member.set', { ...directorArgs, active: false, expectedRevision: 1 });
  assert.throws(() => f.call(director, 'invitations.get', { appId }), code('invitation_manager_required'));
  assert.deepEqual(f.call(director, 'invitations.inbox').items, []);
  assert.equal(f.service.hasAccess({ appId, ownerId: owner.accountId, accountId: director.accountId }), false);
});

test('stale form, missing fields, no consent, wrong profile and cross-app decisions fail without consuming a link', t => {
  const f = fixture(t); f.configure(); const link = f.issue(); f.configure({ fields: [...form, { key: 'f_position', label: 'Должность', type: 'text', required: true }] });
  assert.throws(() => f.submit(link, member, { formRevision: 1 }), code('invitation_form_changed'));
  assert.throws(() => f.submit(link, member, { answers: { f_name: 'Ирина' } }), code('invitation_required_field'));
  assert.throws(() => f.submit(link, member, { consent: false }), code('invitation_consent_required'));
  assert.throws(() => f.call(member, 'invitations.get', { appId, expectedAccountId: owner.accountId }), code('invitation_account_changed'));
  assert.equal(f.call(owner, 'invitations.get', { appId }).links[0].uses, 0);
  const request = f.submit(link, member, { answers: { f_name: 'Ирина', f_position: 'Инженер' } });
  assert.throws(() => f.mutate(outsider, 'invitations.decide', { appId: anotherApp, applicationId: request.id, expectedRevision: 1, decision: 'approved' }), code('invitation_request_unavailable'));
});

test('limits, expiry, revoke, disabled policy and missing app deny new admissions', t => {
  const f = fixture(t); f.configure(); const link = f.issue({ maxUses: 1 }); f.submit(link);
  assert.throws(() => f.service.preview(link.token), code('invitation_unavailable'));
  const revoked = f.issue(); f.mutate(owner, 'invitations.revoke', { appId, linkId: revoked.id });
  assert.throws(() => f.submit(revoked), code('invitation_unavailable'));
  const expiring = f.issue(); f.advance(2 * 86400000); assert.throws(() => f.service.preview(expiring.token), code('invitation_unavailable'));
  const disabled = f.issue(); f.configure({ enabled: false }); assert.throws(() => f.service.preview(disabled.token), code('invitation_unavailable'));
  f.disable(); assert.throws(() => f.call(owner, 'invitations.get', { appId }), code('invitation_unavailable'));
});

test('request and link commands are idempotent and stale decisions cannot overwrite each other', t => {
  const f = fixture(t); f.configure(); const issue = { appId, requestId: 'same-link', label: 'Команда', expiresAt: 90000000, maxUses: 10 };
  assert.deepEqual(f.call(owner, 'invitations.issue', issue), f.call(owner, 'invitations.issue', issue));
  const link = f.call(owner, 'invitations.issue', issue).link, request = f.submit(link);
  assert.equal(f.submit(link).id, request.id); assert.equal(f.call(owner, 'invitations.get', { appId }).links[0].uses, 1);
  f.mutate(owner, 'invitations.decide', { appId, applicationId: request.id, expectedRevision: 1, decision: 'declined' });
  assert.throws(() => f.mutate(owner, 'invitations.decide', { appId, applicationId: request.id, expectedRevision: 1, decision: 'approved' }), code('invitation_revision_conflict'));
  assert.equal(f.submit(link).state, 'pending');
});

test('each invitation keeps its admission mode; the shared inbox lists summaries without exposing entire forms', t => {
  const f = fixture(t); f.configure(); const approval = f.issue(); f.configure({ mode: 'direct' }); const direct = f.issue();
  assert.equal(f.service.preview(approval.token).mode, 'approval'); assert.equal(f.service.preview(direct.token).mode, 'direct');
  assert.equal(f.submit(approval).state, 'pending');
  const inbox = f.call(owner, 'invitations.inbox');
  assert.equal(inbox.requests[0].appTitle, 'Рабочая команда'); assert.equal(inbox.requests[0].name, 'Ирина');
  assert.equal('answers' in inbox.requests[0], false); assert.equal('fields' in inbox.requests[0], false);
  assert.throws(() => f.call(outsider, 'invitations.request.get', { appId, applicationId: inbox.requests[0].id }), code('invitation_manager_required'));
});

test('removed members do not regain access by replaying an approved invitation; owner transfer invalidates old admission', t => {
  const f = fixture(t); f.configure({ mode: 'direct' }); const link = f.issue(); f.submit(link);
  f.mutate(owner, 'invitations.member.set', { appId, accountId: member.accountId, label: 'Ирина', role: 'member', active: false, expectedRevision: 1 });
  assert.equal(f.call(member, 'invitations.status', { token: link.token }).state, 'removed');
  assert.equal(f.service.hasAccess({ appId, ownerId: owner.accountId, accountId: member.accountId }), false);
  f.submit(link);
  assert.equal(f.service.hasAccess({ appId, ownerId: owner.accountId, accountId: member.accountId }), false);
  f.transfer(); assert.throws(() => f.service.preview(link.token), code('invitation_unavailable'));
  assert.equal(f.service.hasAccess({ appId, ownerId: outsider.accountId, accountId: member.accountId }), false);
});

test('field types, names and values reject malformed and unexpected data', () => {
  assert.throws(() => fields([...form, form[0]])); assert.throws(() => fields([{ ...form[0], type: 'script' }]));
  assert.throws(() => answers(form, { f_name: 'Ирина', f_age: 131 }));
  assert.throws(() => answers(form, { f_name: 'Ирина', extra: 'secret' }));
  const typed = fields([{ key: 'f_select', label: 'Отдел', type: 'select', required: true, options: ['Продажи', 'Поддержка'] },
    { key: 'f_check', label: 'Подтверждаю', type: 'checkbox', required: true }, { key: 'f_date', label: 'Дата', type: 'date', required: true }]);
  assert.throws(() => answers(typed, { f_select: 'Продажи', f_check: true, f_date: '2026-02-30' }));
  assert.deepEqual({ ...answers(typed, { f_select: 'Продажи', f_check: true, f_date: '2026-02-28' }) }, { f_select: 'Продажи', f_check: true, f_date: '2026-02-28' });
  const token = 'a'.repeat(64), url = invitationUrl('https://soty.example', token);
  assert.equal(parseInvitationRoute(new URL(url).hash).token, token);
  assert.equal(parseInvitationRoute('#invite/' + token + '?other=1'), null);
});

test('invitation pages preserve access to links beyond the first page', t => {
  const f = fixture(t); f.configure(); for (let index = 0; index < 51; index++) f.issue({ label: `Приглашение ${index}` });
  const first = f.call(owner, 'invitations.get', { appId }); assert.equal(first.links.length, 50); assert.equal(first.linkCount, 51);
  const next = f.call(owner, 'invitations.get', { appId, linkOffset: first.linksNextOffset });
  assert.equal(next.links.length, 1); assert.equal(next.linksNextOffset, null);
  assert.equal(new Set([...first.links, ...next.links].map(link => link.id)).size, 51);
});

test('the host admission proof rechecks current app owner and availability rather than old membership alone',t=>{
  const f=fixture(t);f.configure({mode:'direct'});const link=f.issue();f.submit(link);
  const [claimId]=f.service.listAdmissionClaims();assert.equal(f.service.readAdmission(claimId).state,'admitted');
  f.transfer();assert.equal(f.service.readAdmission(claimId).state,'removed');f.disable();assert.equal(f.service.readAdmission(claimId).state,'removed');
});

test('post-COMMIT membership invalidation includes the removed recipient rather than only the manager',t=>{
  const f=fixture(t);f.configure({mode:'direct'});const link=f.issue();f.submit(link);
  f.mutate(owner,'invitations.member.set',{appId,accountId:member.accountId,label:'Ирина',role:'member',active:false,expectedRevision:1});
  assert.ok(f.changes.at(-1).accountIds.includes(member.accountId));assert.deepEqual(f.changes.at(-1).target,{kind:'app',id:appId});
});
