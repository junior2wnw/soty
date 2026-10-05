import test from 'node:test';
import assert from 'node:assert/strict';
import { appBuilderProposal, appBuilderReceipt, appBuilderLaunchUrl, matchingAppBuilderRegistration } from './app-builder-result.mjs';

const pending = { hostDeviceId: 'device-1', connectorId: 'connector-1', jobId: 'job-1' };
const proposal = { schema: 'soty.local-app.v1', name: 'Личный проект', port: 5111, entryPath: '/board', sourceJobId: 'job-1' };
const result = (patch = {}, job = {}) => ({ job: { status: 'succeeded', result: { appProposal: { ...proposal, ...patch } }, ...job } });
test('an unleased queued ACK is valid while mismatched or malformed runtime receipts fail closed', () => {
  const job = { schema: 'soty.connector-job.v4', kind: 'agent', id: 'job-1', deviceId: pending.hostDeviceId, connectorId: '', status: 'queued', attempts: 0 };
  assert.equal(appBuilderReceipt(job, pending), true);
  assert.equal(appBuilderReceipt({ ...job, connectorId: pending.connectorId, status: 'running', attempts: 1 }, pending), true);
  for (const patch of [{ id: '' }, { deviceId: 'other-device' }, { schema: 'soty.connector-job.v3' }, { kind: 'terminal' }, { connectorId: 'other-connector' }, { status: 'leased' }, { status: 'running' }, { attempts: 1 }]) assert.equal(appBuilderReceipt({ ...job, ...patch }, pending), false);
});
test('only confirmed, succeeded, job-bound proposals become app drafts', () => {
  assert.deepEqual(appBuilderProposal(result(), pending), proposal);
  for (const job of [{ status: 'running' }, { status: 'failed' }, { executionUncertain: true }]) assert.equal(appBuilderProposal(result({}, job), pending), null);
  for (const patch of [{ sourceJobId: 'other-job' }, { schema: 'wrong' }, { name: ' ' }, { name: 'a'.repeat(65) }, { port: 49424 }, { port: 80 }, { entryPath: '//external' }, { entryPath: '/_soty/control' }, { entryPath: '/\u0000' }]) assert.equal(appBuilderProposal(result(patch), pending), null);
});
test('lost registration ACK recovery requires the exact owner, device and draft without changing access', () => {
  const app = { ...pending, ...proposal, id: 'app-1', ownerAccountId: 'owner-1', state: 'offline', grants: { accountIds: ['friend-1'], communityIds: [] } };
  assert.equal(matchingAppBuilderRegistration([app], pending, proposal, 'owner-1'), app);
  for (const patch of [{ ownerAccountId: 'other-owner' }, { hostDeviceId: 'other-device' }, { connectorId: 'other-connector' }, { port: 5112 }, { entryPath: '/' }, { name: 'Other' }, { state: 'revoked' }]) assert.equal(matchingAppBuilderRegistration([{ ...app, ...patch }], pending, proposal, 'owner-1'), null);
  assert.deepEqual(app.grants, { accountIds: ['friend-1'], communityIds: [] });
});
test('actual launch URLs require a separate trustworthy origin and never downgrade HTTPS', () => {
  assert.equal(appBuilderLaunchUrl('https://app.apps.example/board', 'https://soty.example/'), 'https://app.apps.example/board');
  assert.equal(appBuilderLaunchUrl('http://app.localhost:5331/', 'http://127.0.0.1:5331/'), 'http://app.localhost:5331/');
  for (const value of ['javascript:alert(1)', 'data:text/html,x', '/board', 'https://soty.example/board', 'https://u:p@app.apps.example/', 'http://app.apps.example/']) assert.equal(appBuilderLaunchUrl(value, 'https://soty.example/'), null);
});
