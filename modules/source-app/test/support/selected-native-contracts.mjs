import { createSelectedNativeSourceApplication } from '../../../../server/selected-native-source.js';
import { createCurrentContextSourceApplication } from '../../../../server/current-context-source.js';
import { canonicalHash } from '../../../capabilities/server/validation.mjs';
import assert from 'node:assert/strict';

const object = properties => ({ type: 'object', additionalProperties: false, required: Object.keys(properties), properties });
export function selectedNativeApplications(profile, creation = { operation: 'items.create', titleField: 'title', artifactType: 'ordinary-item' }) {
  const scope = { appId: profile.appId, target: profile.target, resource: profile.resource, sourceProfile: profile.sourceProfile },
    resource = profile.resource.selection.kind === 'soty.workspace.v1' ? `workspace:${profile.appId}:${profile.resource.resourceId}` : 'items:' + profile.appId,
    base = { version: 1, appId: profile.appId, visibility: 'private', executionEnabled: true,
      resources: [resource], recipients: [resource] };
  return [createCurrentContextSourceApplication(scope), createSelectedNativeSourceApplication({ scope,
    catalog: { ...base, capabilityId: 'app.' + profile.appId + ':items.list', title: 'List Native items', description: 'Read existing items in the currently selected window.',
      effects: [], inputSchema: object({ operation: { type: 'string', maxLength: 40, enum: ['items.list'] } }),
      outputSchema: object({ requestId: { type: 'string', maxLength: 128 }, items: { type: 'array', maxItems: 100,
        items: object({ id: { type: 'string', maxLength: 128 }, title: { type: 'string', maxLength: 500 }, revision: { type: 'integer', minimum: 1 }, createdAt: { type: 'integer', minimum: 1 } }) } }) } }),
    createSelectedNativeSourceApplication({ scope, catalog: { ...base,
      capabilityId: 'app.' + profile.appId + ':items.create', title: 'Create Native item', description: 'Create one item only in the currently selected window.',
      effects: ['create'], inputSchema: object({ operation: { type: 'string', maxLength: 40, enum: [creation.operation] },
        [creation.titleField]: { type: 'string', minLength: 1, maxLength: 500 }, ...(creation.category ? { category: { type: 'string', maxLength: 40, enum: [creation.category] } } : {}) }),
      outputSchema: object({ id: { type: 'string', maxLength: 128 }, revision: { type: 'integer', minimum: 1 } }) },
      ...(profile.resource.selection.kind === 'soty.workspace.v1' ? {} : { projectReceipt(proof) {
        assert.equal(proof.outcome, 'committed'); const { id, revision } = proof.receipt;
        assert.equal(typeof id, 'string'); assert.equal(revision, 1);
        return { effects: [{ kind: 'created', resourceType: 'ordinary-item', resourceId: id, revision }],
          receipt: { verificationMethod: 'domain_read', artifacts: [{ type: 'ordinary-item', id, revision, sha256: canonicalHash(proof.receipt) }] } };
      } }) })];
}
