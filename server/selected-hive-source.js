import { createSelectedNativeSourceApplication } from './selected-native-source.js';
import { canonicalHash, AccessError } from '../modules/capabilities/server/validation.mjs';
import { captureQueryData } from '../modules/capabilities/server/readonly-queries.mjs';

const object = properties => ({ type: 'object', additionalProperties: false, required: Object.keys(properties), properties });
const text = maxLength => ({ type: 'string', minLength: 1, maxLength });

/** The existing selected HIVE profile only: bounded cell metadata and ONE
 * title patch. Native expectedRevision/requestId/selectedCommitFence retain
 * their meanings. No whole-field replacement, role or project creation. */
export function createSelectedHiveSourceApplications(raw) {
  const scope = captureQueryData(raw);
  if (scope.resource?.selection?.kind !== 'hive.project.v1') throw new AccessError('selected_native_source_denied');
  const resource = `app.${scope.appId}:hive.selected`, base = { appId: scope.appId, version: 1,
    visibility: 'private', executionEnabled: true, resources: [resource], recipients: [resource] };
  return Object.freeze([
    createSelectedNativeSourceApplication({ scope, catalog: { ...base,
      capabilityId: `app.${scope.appId}:hive.cells.list`, title: 'Ячейки текущего поля HIVE',
      description: 'Read bounded titles and IDs in the exact selected HIVE field; preserve private body and links.', effects: [],
      inputSchema: object({ operation: { ...text(40), enum: ['hive.cells.list'] } }),
      outputSchema: object({ title: text(240), revision: { type: 'integer', minimum: 1 },
        cells: { type: 'array', maxItems: 100, items: object({ id: text(128), title: { type: 'string', maxLength: 240 } }) } }) } }),
    createSelectedNativeSourceApplication({ scope, catalog: { ...base,
      capabilityId: `app.${scope.appId}:hive.cell.title`, title: 'Изменить заголовок ячейки HIVE',
      description: 'Patch one existing cell title using its read revision. Revision conflict or unknown requires original receipt/read; never resubmit automatically.', effects: ['update'],
      inputSchema: object({ operation: { ...text(40), enum: ['hive.cell.title'] },
        expectedRevision: { type: 'integer', minimum: 1, maximum: Number.MAX_SAFE_INTEGER - 1 },
        id: text(128), title: text(240) }),
      outputSchema: object({ projectId: text(128), revision: { type: 'integer', minimum: 1 } }) },
      projectReceipt(proof) {
        const { projectId, revision } = proof.receipt;
        return { effects: [{ kind: 'updated', resourceType: 'hive-project', resourceId: projectId, revision }],
          receipt: { verificationMethod: 'domain_read', artifacts: [{ type: 'hive-project', id: projectId, revision,
            sha256: canonicalHash(proof.receipt) }] } };
      } }),
  ]);
}
