import { createSelectedNativeSourceApplication } from './selected-native-source.js';
import { captureQueryData } from '../modules/capabilities/server/readonly-queries.mjs';
import { AccessError, canonicalHash, freezeDeep } from '../modules/capabilities/server/validation.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../modules/source-app/server/standard-profile.mjs';
import { validateHiveWorkflowNativeInput } from './hive-workflow-input.js';

const need = value => { if (!value) throw new AccessError('hive_workspace_source_invalid'); };
const object = (properties, required = Object.keys(properties)) => ({ type: 'object', additionalProperties: false, properties, required });
const text = (maxLength = 128, minLength = 1) => ({ type: 'string', minLength, maxLength });
const fixed = value => ({ ...text(), enum: [value] });
const integer = minimum => ({ type: 'integer', minimum, maximum: Number.MAX_SAFE_INTEGER - 1 });
const position = { type: 'number', minimum: -1000000, maximum: 1000000 };
const target = object({ kind: fixed('object'), id: text(96) });
const empty = object({});
const id = value => need(typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,128}$/u.test(value));
const title = value => need(typeof value === 'string' && value.isWellFormed() && value.length <= 240);
const record = value => need(value && typeof value === 'object' && !Array.isArray(value));

// Native v2 representation of the existing bounded workflow compiler. JSON
// Schema's closed subset has no oneOf, so the shared CODE validator enforces
// the exact payload for each op and the complete new-card/link relationships.
export const HIVE_WORKSPACE_ADDITIVE_INPUT = freezeDeep(object({
  operation: fixed('hive.operations'), expectedRevision: integer(1),
  operations: { type: 'array', minItems: 5, maxItems: 40, items: object({
    op: { ...text(), enum: ['object.add', 'occurrence.add', 'relation.add'] },
    object: object({ id: text(96), type: fixed('hive.process/action'), title: text(240), summary: { ...text(0, 0), enum: [''] },
      content: { type: 'array', maxItems: 0, items: text() }, properties: empty, extensions: empty }),
    viewId: text(96), occurrence: object({ id: text(102), target, x: position, y: position,
      width: { type: 'number', enum: [280] }, height: { type: 'number', enum: [240] } }),
    relation: object({ id: text(96), type: fixed('hive.process/flow'), endpoints: { type: 'array', minItems: 2, maxItems: 2,
      items: object({ role: { ...text(), enum: ['source', 'target'] }, target }) }, properties: empty, extensions: empty }),
  }, ['op']) },
}));

/** A bounded reduction of an already authorized Native read, not a new API.
 * The raw response must pass the existing 64KiB/4096-node transport capture
 * first. Large fields fail explicitly; no partial list is presented as full. */
export function hiveWorkspaceReadOutput(stored, input) {
  need(input.operation === 'hive.project.read'); record(stored); record(stored.project);
  const project = stored.project;
  need(project.schema === 'hive.document' && project.version === 2 && Number.isSafeInteger(stored.revision)
    && stored.revision >= 1 && stored.revision < Number.MAX_SAFE_INTEGER);
  id(project.id); title(project.title); need(project.title.length > 0); record(project.core); record(project.core.objects); record(project.views);
  const entries = Object.entries(project.core.objects); need(entries.length <= 100);
  const cells = entries.map(([key, item]) => { record(item); id(item.id); need(item.id === key); title(item.title);
    return { id: item.id, title: item.title }; }).sort((a, b) => a.id.localeCompare(b.id));
  const viewEntries = Object.entries(project.views); need(viewEntries.length <= 16);
  const views = viewEntries.map(([key, item]) => { record(item); id(item.id); need(item.id === key); title(item.title); record(item.placements);
    need(Object.keys(item.placements).length <= 100); return { id: item.id, title: item.title }; }).sort((a, b) => a.id.localeCompare(b.id));
  const insertionViews = [];
  // Root pins a project, not a Native editor view. Never choose the first of
  // several views. Their real IDs are available for an explicit human choice.
  if (views.length === 1) {
    let right = -40, top = 0;
    for (const placement of Object.values(project.views[views[0].id].placements)) {
      record(placement); const x = placement.x ?? 0, y = placement.y ?? 0, width = placement.width ?? 280;
      need([x, y, width].every(Number.isFinite) && width >= 0 && Math.abs(x) <= 1000000 && Math.abs(y) <= 1000000);
      right = Math.max(right, x + width); top = Math.min(top, y);
    }
    if (views[0].id.length <= 96 && right + 40 <= 1000000) insertionViews.push({ id: views[0].id, x: right + 40, y: top });
  }
  return { projectId: project.id, title: project.title, revision: stored.revision, cells, views, insertionViews };
}

/** Prepared additive catalogue rows for the reviewed HIVE workspace2 profile.
 * Append explicitly to the private registry; do not replace old rows, issue
 * grants or infer the installed profile from a factory name in module23. */
export function createHiveWorkspaceSourceApplications(rawScope) {
  const scope = captureQueryData(rawScope);
  need(scope.resource?.selection?.kind === 'soty.workspace.v1'
    && canonicalHash(scope.sourceProfile) === canonicalHash(STANDARD_ROOT_WORKSPACE_SOURCE));
  const resource = `workspace:${scope.appId}:${scope.resource.resourceId}`,
    base = { appId: scope.appId, version: 1, visibility: 'private', executionEnabled: true, resources: [resource], recipients: [resource] };
  return Object.freeze([
    createSelectedNativeSourceApplication({ scope, readOutput: hiveWorkspaceReadOutput, catalog: { ...base,
      capabilityId: `app.${scope.appId}:hive.workspace.cells.context`, title: 'Карточки и представления текущего HIVE',
      description: 'Read bounded card IDs/titles, real view IDs and the current project revision. An insertion point is supplied only for one unambiguous view. Multiple views need explicit human choice. No card bodies, accounts, assets or credentials are exposed. Large or malformed Native reads fail; these data grant no write authority.',
      effects: [], inputSchema: object({ operation: fixed('hive.project.read') }),
      outputSchema: object({ projectId: text(), title: text(240), revision: integer(1),
        cells: { type: 'array', maxItems: 100, items: object({ id: text(), title: text(240, 0) }) },
        views: { type: 'array', maxItems: 16, items: object({ id: text(), title: text(240, 0) }) },
        insertionViews: { type: 'array', maxItems: 1, items: object({ id: text(), x: position, y: position }) } }) } }),
    createSelectedNativeSourceApplication({ scope, validateNativeInput: validateHiveWorkflowNativeInput, catalog: { ...base,
      capabilityId: `app.${scope.appId}:hive.workspace.additive.batch`, title: 'Создать карточки и связи в текущем HIVE',
      description: 'Add 2–12 new cards and 1–16 links between them in ONE atomic Native batch (up to 40 operations). Use object.add then occurrence.add for each card, then relation.add. Occurrence id is cardId:first; size 280×240. First read real view IDs, revision and existing IDs. Use only one unambiguous insertion view or explicit human choice. Shared Root CODE checks exact payloads/references; Native checks ACL, revision and COMMIT. Preserve existing content. Unknown requires the ORIGINAL receipt, never a new mutation/requestId. Additive JSON only, not lossless import/export.',
      effects: ['update'], inputSchema: HIVE_WORKSPACE_ADDITIVE_INPUT,
      outputSchema: object({ outcome: fixed('committed') }) } }),
  ]);
}
