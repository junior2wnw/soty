import test from 'node:test';
import assert from 'node:assert/strict';
import { appBuilderAutomatic, appBuilderProposal, appBuilderReceipt, matchingAppBuilderRegistration } from './app-builder-result.mjs';

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
test('legacy read-only lookup requires the same owner, device and proposal without changing access', () => {
  const app = { ...pending, ...proposal, id: 'app-11111111111111111111111111111111', ownerAccountId: 'owner-1', state: 'offline', grants: { accountIds: ['friend-1'], communityIds: [] } };
  assert.equal(matchingAppBuilderRegistration([app], pending, proposal, 'owner-1'), app);
  for (const patch of [{ id: 'bad-route' }, { ownerAccountId: 'other-owner' }, { hostDeviceId: 'other-device' }, { connectorId: 'other-connector' }, { port: 5112 }, { entryPath: '/' }, { name: 'Other' }, { state: 'revoked' }]) assert.equal(matchingAppBuilderRegistration([{ ...app, ...patch }], pending, proposal, 'owner-1'), null);
  assert.deepEqual(app.grants, { accountIds: ['friend-1'], communityIds: [] });
});

test('automatic result opens only the exact successful job-bound server registration', () => {
  const id = '1234567890abcdef1234567890abcdef';
  const target = { ...pending, jobId: `job_${id}` };
  const job = { schema: 'soty.connector-job.v4', kind: 'agent', id: target.jobId, deviceId: target.hostDeviceId,
    connectorId: target.connectorId, status: 'succeeded', result: { appProposal: { ...proposal, sourceJobId: target.jobId } } };
  const ready = { job, task: { autoRegister: true }, autoRegistration: { state: 'ready', appId: `app-${id}`, appState: 'ready' } };
  assert.deepEqual(appBuilderAutomatic(ready, target), { state: 'ready', appId: `app-${id}`, appState: 'ready' });
  assert.deepEqual(appBuilderAutomatic({ ...ready, autoRegistration: { ...ready.autoRegistration, appState: 'new-state' } }, target), { state: 'ready', appId: `app-${id}`, appState: 'unknown' });
  for (const state of ['waiting', 'conflict', 'access-lost', 'revoked', 'failed']) assert.deepEqual(appBuilderAutomatic({ ...ready, autoRegistration: { state } }, target), { state });
  for (const value of [
    { ...ready, task: undefined }, { ...ready, task: { autoRegister: false } }, { ...ready, autoRegistration: undefined },
    { ...ready, autoRegistration: { ...ready.autoRegistration, appId: `app-${'f'.repeat(32)}` } },
    { ...ready, job: { ...job, id: `job_${'f'.repeat(32)}` } }, { ...ready, job: { ...job, deviceId: 'other' } },
    { ...ready, job: { ...job, connectorId: 'other' } }, { ...ready, job: { ...job, status: 'failed' } },
    { ...ready, job: { ...job, cancelRequested: true } }, { ...ready, job: { ...job, executionUncertain: true } },
    { ...ready, job: { ...job, result: null } },
  ]) assert.deepEqual(appBuilderAutomatic(value, target), { state: 'unconfirmed' });
});
