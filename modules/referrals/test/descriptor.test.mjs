import test from 'node:test';
import assert from 'node:assert/strict';
import { validateDescriptor, contractDigest } from '../../app-contract/index.mjs';
import { createReferralDescriptor, REFERRAL_RIGHTS } from '../descriptor.mjs';
import { REFERRAL_OPERATIONS } from '../server/index.mjs';

const pin = id => ({ id, version: 1, digest: contractDigest({ id, version: 1 }) });
const input = () => ({ appId: 'referral-provider', namespace: 'soty.referrals', source: { id: 'soty.referrals:source/main', revision: 1, digest: contractDigest({ fixture: 'source-v1' }) }, auth: { mode: 'shared-soty', profile: pin('identity:profile/soty') }, resource: pin('soty.referrals:resource/ledger'), bindings: { read: pin('soty.referrals:binding/read'), configure: pin('soty.referrals:binding/configure') }, feedback: { mode: 'required', provider: pin('feedback:provider'), captureProfile: pin('feedback:capture'), retentionProfile: pin('feedback:retention'), submitAudience: 'members', ticketVisibility: 'reporter-and-support' } });

test('real app-contract descriptor exposes separate read/configure bindings, never credit or payout', () => {
  const descriptor = createReferralDescriptor(input()); assert.deepEqual(validateDescriptor(descriptor), descriptor);
  assert.equal(descriptor.capabilities.length, REFERRAL_OPERATIONS.length); assert.equal(REFERRAL_RIGHTS.internalCredit, 'trusted-host-only');
  const configure = descriptor.capabilities.find(c => c.id.endsWith(':config.put'));
  assert.deepEqual(configure.effects, ['update']); assert.equal(configure.binding.id, 'soty.referrals:binding/configure');
  assert.ok(descriptor.capabilities.filter(c => c !== configure).every(c => c.effects[0] === 'read' && c.binding.id.endsWith('/read')));
  assert.ok(descriptor.capabilities.every(c => !/credit|payment|refund|payout/i.test(c.id)));
  assert.equal(JSON.stringify(descriptor).includes('http'), false, 'no fixed or private endpoint is embedded');
});

test('descriptor requires real host pins, shared identity, namespaced resources and valid digest', () => {
  assert.throws(() => createReferralDescriptor({ ...input(), auth: { mode: 'public' } }), e => e.code === 'referrals_shared_identity_required');
  assert.throws(() => createReferralDescriptor({ ...input(), bindings: {} }), e => e.code === 'referrals_descriptor_host_bindings_required');
  assert.throws(() => createReferralDescriptor({ ...input(), resource: pin('another-app:resource/ledger') }), e => e.code === 'resource_namespace');
  const broken = structuredClone(createReferralDescriptor(input())); broken.capabilities[0].effects = ['spend'];
  assert.throws(() => validateDescriptor(broken), e => e.code === 'capability_digest_mismatch');
});
