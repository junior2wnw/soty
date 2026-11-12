import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createAppsService } from '../../apps/server/index.mjs';
import { connectorKey } from '../../apps/server/protocol.mjs';
import { feedbackInput } from '../../source-app/shared/feedback-wire.mjs';
import { createFeedbackService } from '../server/index.mjs';
import { DEFAULT_FEEDBACK_PROFILE } from '../server/profile.mjs';
import { validateFeedbackAttachments, FEEDBACK_MEDIA_LIMITS } from '../server/media.mjs';
import { validateRootFeedbackAttachments } from '../server/root-media.mjs';

const shortBytes = () => readFileSync(new URL('../../materials/test/fixtures/video.webm', import.meta.url));
const longBytes = () => readFileSync(new URL('./fixtures/ffmpeg-video-121s.webm', import.meta.url));
const video = (bytes = shortBytes()) => ({ kind: 'video', name: 'selected.webm', mimeType: 'video/webm', dataBase64: bytes.toString('base64') });
const code = expected => error => error?.code === expected && error.message === expected;
const validate = (item, options = {}) => validateRootFeedbackAttachments([item], options)[0];

test('an explicitly enabled host admits a real VP8/Opus clip without changing legacy admission or profile pins', () => {
  const bytes = shortBytes(), accepted = validate(video(bytes));
  assert.equal(accepted.kind, 'video'); assert.equal(accepted.mimeType, 'video/webm');
  assert.equal(accepted.width, 96); assert.equal(accepted.height, 64); assert.equal(accepted.durationMs, null);
  assert.equal(accepted.byteLength, bytes.length); assert.deepEqual(accepted.bytes, bytes); assert.equal(Object.isFrozen(accepted), true);
  assert.throws(() => validateFeedbackAttachments([video()]), code('feedback_media_unsupported'));
  assert.equal(DEFAULT_FEEDBACK_PROFILE.digest, '716516e51e0cab9261c737d13a0f494799c516d383a3b494d04ba606fe729a34');
  assert.equal(DEFAULT_FEEDBACK_PROFILE.feedback.captureProfile.digest, '94db961b2287b4231aedf0f8a954df7d9557432a8c052a30c29ec599f2c0f080');
  assert.throws(() => feedbackInput('submit', { requestId: 'source-video-negative', body: 'Private issue', attachments: [video()] }), code('source_feedback_input_invalid'));
});

test('video keeps the existing byte/count caps, closed DTO, MIME, track and actual VP8 frame validation', () => {
  const item = video(), bytes = shortBytes();
  assert.equal(FEEDBACK_MEDIA_LIMITS.totalAttachmentBytes, 1048576); assert.equal(FEEDBACK_MEDIA_LIMITS.maxAttachments, 3);
  assert.throws(() => validateRootFeedbackAttachments([item, item, item, item]), code('feedback_attachment_limit'));
  assert.throws(() => validate(video(Buffer.alloc(1048577))), code('feedback_attachment_limit'));
  assert.throws(() => validate(item, { limits: { totalAttachmentBytes: bytes.length - 1 } }), code('feedback_attachment_limit'));
  assert.throws(() => validate(item, { limits: { totalAttachmentBytes: 1048577 } }), code('feedback_media_limits_invalid'));
  assert.throws(() => validateRootFeedbackAttachments([item, item], { limits: { totalAttachmentBytes: bytes.length * 2 - 1 } }), code('feedback_attachment_limit'));
  assert.throws(() => validateRootFeedbackAttachments(Array(1)), code('feedback_attachment_invalid'));
  let getterCalls = 0;
  const accessor = { ...item }; Object.defineProperty(accessor, 'dataBase64', { enumerable: true, get() { getterCalls++; return item.dataBase64; } });
  assert.throws(() => validate(accessor), code('feedback_attachment_invalid')); assert.equal(getterCalls, 0);
  assert.throws(() => validate({ ...item, [Symbol('private')]: true }), code('feedback_attachment_invalid'));
  assert.throws(() => validate({ ...item, recipient: 'another-owner' }), code('feedback_attachment_invalid'));
  assert.throws(() => validate({ ...item, name: '../untrusted.webm' }), code('feedback_attachment_invalid'));
  assert.throws(() => validate({ ...item, dataBase64: item.dataBase64 + '\n' }), code('feedback_attachment_invalid'));
  assert.throws(() => validate({ ...item, kind: 'audio' }), code('feedback_media_unsupported'));
  assert.throws(() => validate({ ...item, mimeType: 'video/mp4' }), code('feedback_media_unsupported'));
  assert.throws(() => validate(video(Buffer.from('not a video'))), code('feedback_media_invalid'));
  assert.throws(() => validate(video(bytes.subarray(0, 32))), code('feedback_media_invalid'));
  const audio = readFileSync(new URL('./fixtures/chrome-recorder.webm', import.meta.url));
  assert.throws(() => validate(video(audio)), code('feedback_media_invalid'));
  const damaged = Buffer.from(bytes), keyframe = damaged.indexOf(Buffer.from([0x9d, 0x01, 0x2a])); assert.ok(keyframe > 0);
  damaged[keyframe] ^= 1;
  assert.throws(() => validate(video(damaged)), code('feedback_media_invalid'));
  const oversized = Buffer.from(bytes), dimension = oversized.indexOf(Buffer.from([0xb0, 0x81, 96])); assert.ok(dimension > 0);
  oversized[dimension + 2] = 97;
  assert.throws(() => validate(video(oversized)), code('feedback_media_invalid'));
});

test('an encoder-valid 121 second video below 1 MiB fails even when its declared duration is forged short', () => {
  const bytes = longBytes(); assert.ok(bytes.length < FEEDBACK_MEDIA_LIMITS.totalAttachmentBytes);
  assert.throws(() => validate(video(bytes)), code('feedback_video_duration_limit'));
  const forged = Buffer.from(bytes), duration = forged.indexOf(Buffer.from([0x44, 0x89, 0x88])); assert.ok(duration > 0);
  forged.writeDoubleBE(1, duration + 3);
  assert.throws(() => validate(video(forged)), code('feedback_video_duration_limit'));
});

function ownerFixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-feedback-video-'));
  const actors = Object.fromEntries(['owner-a', 'owner-b', 'reporter', 'first-admin'].map(accountId => [accountId, { accountId, deviceId: 'device-' + accountId }]));
  const active = new Set(Object.keys(actors));
  const appOptions = { databasePath: join(directory, 'apps.sqlite'), appOriginTemplate: 'https://{appId}.feedback.test',
    shellOrigins: ['https://shell.feedback.test'], actorActive: actor => active.has(actor?.accountId), withAuthorityFence: callback => callback() };
  let apps = createAppsService(appOptions), service;
  t.after(() => {
    service?.close(); apps.close();
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-feedback-video-/u);
    rmSync(directory, { recursive: true, force: true });
  });
  const setup = new DatabaseSync(appOptions.databasePath);
  const appIds = {};
  try {
    for (const accountId of ['owner-a', 'owner-b']) {
      const identity = { linkId: 'link-' + accountId, hostDeviceId: 'host-' + accountId, connectorId: 'connector-' + accountId };
      setup.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(connectorKey(identity), accountId, JSON.stringify(identity), 'Synthetic device', 1);
      appIds[accountId] = apps.execute({ op: 'apps.register', actor: actors[accountId], args: { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId,
        name: accountId, port: 42001, grants: { accountIds: Object.keys(actors).filter(id => id !== accountId), communityIds: [] } } }).app.id;
    }
  } finally { setup.close(); }
  const serviceOptions = { databasePath: join(directory, 'feedback.sqlite'), registryId: 'fixture', environmentId: 'test',
    actorActive: actor => active.has(actor?.accountId), withAppAuthority: (...args) => apps.withAppAuthority(...args) };
  const open = videoEnabled => createFeedbackService({ ...serviceOptions, videoEnabled });
  service = open(true);
  const call = (actor, operation, args) => service.execute({ op: 'apps.feedback.' + operation, actor: actors[actor], args });
  const context = (actor, appId) => call(actor, 'context', { appId }).context;
  return { call, context, actors, appIds, active, open, get service() { return service; },
    reopen() { service.close(); apps.close(); apps = createAppsService(appOptions); service = open(true); } };
}

test('real Apps ownership routes video to two distinct private inboxes and survives reopen without owner/admin leakage or duplicate effects', t => {
  const f = ownerFixture(t), appA = f.appIds['owner-a'], appB = f.appIds['owner-b'];
  assert.throws(() => f.open('true'), code('feedback_host_required'));
  const contextA = f.context('reporter', appA), contextB = f.context('reporter', appB);
  assert.notEqual(contextA.installationId, contextB.installationId); assert.equal(contextA.capabilities.video, true);
  assert.notEqual(contextA.captureProfile.id, DEFAULT_FEEDBACK_PROFILE.feedback.captureProfile.id);
  assert.equal(f.context('owner-a', appA).canManage, true); assert.equal(f.context('owner-b', appA).canManage, false);
  const input = { appId: appA, installationId: contextA.installationId, requestId: 'video-reporter-once', body: 'Private video issue', attachments: [video()] };
  assert.throws(() => f.call('reporter', 'submit', { ...input, ownerId: 'owner-b' }), code('feedback_invalid_arguments'));
  const receipt = f.call('reporter', 'submit', input).receipt;
  const selected = { appId: appA, installationId: contextA.installationId, ticketId: receipt.ticketId };
  const otherReceipt = f.call('reporter', 'submit', { ...input, appId: appB, installationId: contextB.installationId,
    requestId: 'other-owner-video-once', body: 'A separate private issue for owner B' }).receipt;
  const otherSelected = { appId: appB, installationId: contextB.installationId, ticketId: otherReceipt.ticketId };
  assert.equal(f.call('owner-b', 'list', { appId: appB, installationId: contextB.installationId }).tickets.length, 1);
  assert.deepEqual(f.call('owner-a', 'list', { appId: appB, installationId: contextB.installationId }).tickets, []);
  assert.throws(() => f.call('owner-a', 'get', otherSelected), code('feedback_ticket_unavailable'));
  const queue = actor => f.call(actor, 'list', { appId: appA, installationId: contextA.installationId }).tickets;
  assert.equal(queue('owner-a').length, 1); assert.equal(queue('reporter').length, 1);
  for (const foreign of ['owner-b', 'first-admin']) {
    assert.deepEqual(queue(foreign), []);
    assert.throws(() => f.call(foreign, 'get', selected), code('feedback_ticket_unavailable'));
    assert.throws(() => f.call(foreign, 'reply', { ...selected, requestId: 'forbidden-' + foreign, expectedRevision: 1, body: 'Unauthorized' }), code('feedback_ticket_unavailable'));
  }
  assert.throws(() => f.call('owner-b', 'get', { ...selected, appId: appB, installationId: contextB.installationId }), code('feedback_ticket_unavailable'));
  assert.throws(() => f.call('owner-a', 'get', { ...selected, appId: appB }), code('feedback_installation_mismatch'));
  const ownerTicket = f.call('owner-a', 'get', selected).ticket;
  assert.equal(ownerTicket.canManage, true); assert.equal(ownerTicket.attachments[0].dataBase64, input.attachments[0].dataBase64);
  const ownerReply = { ...selected, requestId: 'owner-response-once', expectedRevision: 1, body: 'A private owner response' };
  f.call('owner-a', 'reply', ownerReply);
  f.reopen();
  assert.equal(f.context('reporter', appA).installationId, contextA.installationId);
  assert.deepEqual(f.call('reporter', 'submit', input).receipt, receipt); assert.equal(f.call('reporter', 'submit', input).replayed, true);
  assert.equal(f.call('owner-a', 'reply', ownerReply).replayed, true);
  const recovered = f.call('reporter', 'get', selected).ticket;
  assert.equal(recovered.attachments[0].dataBase64, input.attachments[0].dataBase64); assert.equal(recovered.messages.length, 1);
  assert.equal(f.call('owner-b', 'get', otherSelected).ticket.attachments[0].dataBase64, input.attachments[0].dataBase64);
  assert.throws(() => f.call('owner-a', 'get', otherSelected), code('feedback_ticket_unavailable'));
  assert.deepEqual(queue('owner-b'), []); assert.deepEqual(queue('first-admin'), []);
  assert.throws(() => f.service.agent.execute({ op: 'apps.feedback.list', actor: f.actors['owner-a'], args: { appId: appA, installationId: contextA.installationId } }), code('unsupported_media_projection'));
  assert.throws(() => f.service.agent.execute({ op: 'apps.feedback.get', actor: f.actors['owner-a'], args: selected }), code('unsupported_media_projection'));
  const legacy = f.open(false);
  try {
    assert.equal(Object.hasOwn(legacy.execute({ op: 'apps.feedback.context', actor: f.actors.reporter, args: { appId: appB } }).context.capabilities, 'video'), false);
    assert.throws(() => legacy.execute({ op: 'apps.feedback.submit', actor: f.actors.reporter, args: { ...input, appId: appB, installationId: contextB.installationId } }), code('feedback_media_unsupported'));
  } finally { legacy.close(); }
  const ready = f.call('owner-a', 'status', { ...selected, requestId: 'owner-ready-once', expectedRevision: recovered.revision, status: 'ready_to_check' }).ticket;
  assert.throws(() => f.call('owner-a', 'accept', { ...selected, requestId: 'owner-cannot-accept', expectedRevision: ready.revision }), code('feedback_acceptance_required'));
  assert.equal(f.call('reporter', 'accept', { ...selected, requestId: 'reporter-accept-once', expectedRevision: ready.revision }).ticket.status, 'resolved');
  f.active.delete('reporter');
  assert.throws(() => f.call('reporter', 'get', selected), code('feedback_authentication_required'));
});
