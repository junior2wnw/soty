import { pathToFileURL } from 'node:url';
import { createDescriptor } from '../sdk.mjs';
import { createAdmissionHost, beginAdmission, holdFeedback, createFeedbackReceipt, confirmFeedback, replayAdmission, contractDigest } from '../index.mjs';

export const pin = (id, digit) => ({ id, version: 1, digest: digit.repeat(64) });
/** Invented principals/resources only. No provider request or executable binding exists. */
export function fixtureConfiguration() {
  const provider = pin('fixture:feedback', 'b'), reviewProvider = pin('fixture:reviews', 'c');
  const capture = pin('fixture:capture/raster-audio', 'd'), retention = pin('fixture:retention/private', 'e');
  const source = { id: 'fixture.board:source/main', revision: 1, digest: 'a'.repeat(64) };
  const scope = { registryId: 'fixture.registry', tenantId: 'fixture.personal', appId: 'fixture-board', environmentId: 'fixture' };
  const binding = pin('fixture.board:binding/create', 'f');
  const subject = pin('fixture:subject/board', '8');
  const descriptor = createDescriptor({
    app: { id: scope.appId, namespace: 'fixture.board', title: 'Доска ✨', visibility: 'private', source, auth: { mode: 'public' } },
    capabilities: [{ id: 'fixture.board:create', version: 1,
      inputSchema: { type: 'object', properties: { title: { type: 'string', maxLength: 160 } }, required: ['title'], additionalProperties: false },
      outputSchema: { type: 'object', properties: { id: { type: 'string', maxLength: 96 } }, required: ['id'], additionalProperties: false },
      resources: [pin('fixture.board:resource/new', '1')], effects: ['create'], recipients: [pin('fixture.board:recipient/api', '2')], binding }],
    skills: [pin('fixture.board:skill/create', '3')], docs: [pin('fixture.board:docs/guide', '4')],
    feedback: { mode: 'required', provider, captureProfile: capture, retentionProfile: retention, submitAudience: 'members', ticketVisibility: 'reporter-and-support' },
    reviews: { mode: 'public-read', provider: reviewProvider, subjects: [subject] }
  });
  const hostConfig = {
    context: { scope, namespace: descriptor.app.namespace, ownerId: 'fixture.owner', authorityRevision: 1, visibility: 'private', source, auth: descriptor.app.auth },
    bindings: [{ ...binding, scope, source, capability: { id: descriptor.capabilities[0].id, version: 1, digest: descriptor.capabilities[0].digest } }],
    providers: [{ ...provider, kind: 'feedback', publicRead: false }, { ...reviewProvider, kind: 'reviews', publicRead: true }],
    profiles: [{ ...capture, kind: 'capture' }, { ...retention, kind: 'retention' }],
    skills: descriptor.skills, docs: descriptor.docs, placements: [],
    publicSubjects: [{ ...subject, provider: reviewProvider }]
  };
  return { descriptor, hostConfig };
}
export function runFixture() {
  const { descriptor, hostConfig } = fixtureConfiguration(), host = createAdmissionHost(hostConfig);
  const pending = beginAdmission(host, descriptor, 'fixture.request');
  const held = holdFeedback(host, pending);
  const receipt = createFeedbackReceipt(host, held, { installationId: 'fixture.inbox', receiptDigest: contractDigest({ fixture: 'confirmed' }) });
  const ready = confirmFeedback(host, held, receipt), replayed = replayAdmission(host, ready, descriptor, 'fixture.request');
  let conflict;
  try { replayAdmission(host, ready, { ...descriptor, app: { ...descriptor.app, title: 'Changed' } }, 'fixture.request'); } catch (e) { conflict = e.code; }
  return { prototype: true, providerCalls: 0, executedHandlers: 0, states: [pending.status, held.status, ready.status],
    replaySameObject: ready === replayed, conflict, gates: ready.plan.gates, productionAdmission: ready.plan.productionAdmission };
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) process.stdout.write(JSON.stringify(runFixture()) + '\n');
