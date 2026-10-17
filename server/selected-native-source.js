import { types } from 'node:util';
import { createTrustedReadonlyQueryAdapter, captureQueryData } from '../modules/capabilities/server/readonly-queries.mjs';
import { EXTERNAL_ADAPTER_PROFILE } from '../modules/capabilities/server/external-adapters.mjs';
import { canonicalHash, freezeDeep, AccessError } from '../modules/capabilities/server/validation.mjs';
import { createCatalog } from '../modules/capabilities/server/catalog.mjs';
import { selectedResource } from '../modules/apps/scoped-embed/resource-profile.mjs';
import { STANDARD_SELECTED_SOURCE_V3, isWorkspaceSource } from '../modules/source-app/server/standard-profile.mjs';
import { contextAfter, workspaceMutationRef, workspaceWitness, workspaceReceipt } from '../modules/source-app/server/workspace-context.mjs';
import { HIVE_SELECTED_SOURCE, HIVE_SELECTED_KERNEL_SOURCE } from '../modules/apps/scoped-embed/resource-route-adapters.mjs';
import { selectedHiveWorkflowOperations } from './hive-workflow-input.js';

const installed = new WeakMap();
const need = value => { if (!value) throw new AccessError('selected_native_source_denied'); };
const fields = (value, required, optional = []) => need(value && !Array.isArray(value)
  && typeof value === 'object' && Object.keys(value).every(key => [...required, ...optional].includes(key))
  && required.every(key => Object.hasOwn(value, key)));
const same = (a, b) => canonicalHash(a) === canonicalHash(b);
const isHandle = value => typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value);
export const selectedNativeBindingId = appId => `app.${appId}:selected-native`;
export function isSelectedNativeContract(contract) {
  return contract?.binding?.id === selectedNativeBindingId(contract.appId)
    && (contract.binding.version === 1 && Object.keys(contract.inputSchema?.properties ?? {}).sort().join(',') === 'handle,input'
      || contract.binding.version === 2 && Object.keys(contract.inputSchema?.properties ?? {}).sort().join(',') === 'contextRef,handle,input');
}

/** Operator CODE supplies a bounded Native schema and, for effects, a typed
 * projection of an independently read Native receipt. A descriptor alone
 * grants no rights, selects no window and runs no adapter code. */
export function createSelectedNativeSourceApplication(raw) {
  need(raw && typeof raw === 'object' && !types.isProxy(raw) && !Array.isArray(raw)
    && [Object.prototype, null].includes(Object.getPrototypeOf(raw)));
  const descriptors = Object.getOwnPropertyDescriptors(raw), names = ['scope', 'catalog', 'projectReceipt', 'readOutput', 'validateNativeInput', 'guidance'];
  need(Reflect.ownKeys(descriptors).every(key => typeof key === 'string' && names.includes(key)
    && Object.hasOwn(descriptors[key], 'value')) && ['scope', 'catalog'].every(key => Object.hasOwn(descriptors, key)));
  const rawScope = descriptors.scope.value, rawCatalog = descriptors.catalog.value,
    projectReceipt = descriptors.projectReceipt?.value, readOutput = descriptors.readOutput?.value,
    validateNativeInput = descriptors.validateNativeInput?.value, guidance = descriptors.guidance?.value;
  const scope = captureQueryData(rawScope), catalog = captureQueryData(rawCatalog);
  fields(scope, ['appId', 'target', 'sourceProfile', 'resource']); fields(scope.target, ['revision', 'digest']);
  need(/^app-[a-f0-9]{32}$/u.test(scope.appId) && Number.isSafeInteger(scope.target.revision) && scope.target.revision > 0
    && /^[a-f0-9]{64}$/u.test(scope.target.digest));
  selectedResource(scope.resource); need(scope.resource.appId === scope.appId);
  const workspace = scope.resource.selection.kind === 'soty.workspace.v1' && isWorkspaceSource(scope.sourceProfile);
  const standard = workspace || scope.resource.selection.kind === 'soty.resource.v1' && same(scope.sourceProfile, STANDARD_SELECTED_SOURCE_V3),
    hive = scope.resource.selection.kind === 'hive.project.v1' && [HIVE_SELECTED_SOURCE, HIVE_SELECTED_KERNEL_SOURCE].some(pin => same(scope.sourceProfile, pin));
  need(standard || hive);
  fields(catalog, ['capabilityId', 'version', 'appId', 'title', 'description', 'visibility', 'executionEnabled', 'inputSchema', 'outputSchema', 'resources', 'effects', 'recipients']);
  need(catalog.appId === scope.appId && catalog.inputSchema.type === 'object' && Array.isArray(catalog.effects));
  if (workspace) need(same(catalog.resources, [`workspace:${scope.appId}:${scope.resource.resourceId}`])
    && same(catalog.recipients, catalog.resources));
  const effectful = catalog.effects.length > 0;
  // Reviewed CODE may reduce a bounded Native read to the exact public tool
  // schema. This grants no authority and never transforms mutations/receipts.
  need(readOutput === undefined || !effectful && standard && typeof readOutput === 'function' && !types.isProxy(readOutput)
    && !types.isAsyncFunction(readOutput) && !types.isGeneratorFunction(readOutput));
  need(validateNativeInput === undefined || effectful && workspace && typeof validateNativeInput === 'function' && !types.isProxy(validateNativeInput)
    && !types.isAsyncFunction(validateNativeInput) && !types.isGeneratorFunction(validateNativeInput));
  need(effectful && !workspace ? typeof projectReceipt === 'function' && !types.isProxy(projectReceipt)
    && !types.isAsyncFunction(projectReceipt) && !types.isGeneratorFunction(projectReceipt) : projectReceipt === undefined);
  freezeDeep(scope); freezeDeep(catalog);
  const sourceBinding = { id: selectedNativeBindingId(scope.appId), version: workspace ? 2 : 1,
    digest: canonicalHash({ schema: 'soty.selected-native-source.v1', scope, catalog }) };
  const wrapped = freezeDeep({ ...catalog, inputSchema: { type: 'object', additionalProperties: false, required: ['handle', 'input', ...(workspace ? ['contextRef'] : [])],
    properties: { handle: { type: 'string', minLength: 43, maxLength: 43 }, input: catalog.inputSchema,
      ...(workspace ? { contextRef: { type: 'string', minLength: 43, maxLength: 43 } } : {}) } },
    executionBinding: { kind: 'registered', handler: catalog.capabilityId, version: 1, binding: sourceBinding } });
  const registry = createCatalog([wrapped]), entry = registry.get(catalog.capabilityId, catalog.version);
  let host;
  // The existing composition holds original Root/App authority. Native current
  // authority is checked by the original handle before and after EACH HTTP call.
  const withAuthority = (_request, callback) => { need(host && typeof callback === 'function'); return callback(); };
  installed.set(withAuthority, { scope, bind(value) { need(!host); host = value; } });
  function input(raw) {
    const value = captureQueryData(raw); fields(value, ['handle', 'input', ...(workspace ? ['contextRef'] : [])]);
    need(isHandle(value.handle) && (!workspace || isHandle(value.contextRef)));
    need(!workspace || value.input.operation !== 'soty.context.select');
    registry.validateInput(entry, value); return value;
  }
  function validatedInput(request) {
    const value = input(request.input);
    if (validateNativeInput !== undefined) {
      need(host && host.withOwner(request, () => {
        const checked = validateNativeInput(value.input);
        if (types.isPromise(checked)) { void Promise.prototype.then.call(checked, () => {}, () => {}); need(false); }
        need(checked === true); return true;
      }) === true);
    }
    return value;
  }
  async function call(request, path, data, signal) {
    need(host && signal instanceof AbortSignal && !signal.aborted);
    const captured = validatedInput(request), response = await host.apps().callScopedNativeSource({ scope,
      handle: captured.handle, path, requestId: request.requestId, input: data, signal,
      ...(workspace ? { contextRef: captured.contextRef } : {}),
      withCurrentOwner: callback => host.withOwner(request, callback) });
    need(!signal.aborted && response.status === 200);
    const envelope = captureQueryData(response.data);
    if (hive) return envelope;
    fields(envelope, ['ok', 'data']); need(envelope.ok === true); return captureQueryData(envelope.data);
  }
  function hiveOperation(value) {
    if (value.operation === 'hive.workflow.create') return selectedHiveWorkflowOperations(value);
    const create = value.operation === 'hive.cell.create';
    fields(value, ['operation', 'expectedRevision', 'id', 'title', ...(create ? ['viewId', 'x', 'y'] : [])]);
    need((create || value.operation === 'hive.cell.title') && Number.isSafeInteger(value.expectedRevision) && value.expectedRevision >= 1
      && typeof value.id === 'string' && /^[A-Za-z0-9_.:-]{1,128}$/u.test(value.id)
      && typeof value.title === 'string' && value.title.trim() && value.title.length <= 240);
    if (create) {
      need(typeof value.viewId === 'string' && /^[A-Za-z0-9_.:-]{1,128}$/u.test(value.viewId)
        && [value.x, value.y].every(number => Number.isFinite(number) && Math.abs(number) <= 1000000));
      return [{ op: 'object.add', object: { id: value.id, type: 'hive.process/action', title: value.title,
        summary: '', content: [], properties: {}, extensions: {} } },
      { op: 'occurrence.add', viewId: value.viewId, occurrence: { id: `${value.id}:first`,
        target: { kind: 'object', id: value.id }, x: value.x, y: value.y, width: 280, height: 240 } }];
    }
    return [{ op: 'object.update', id: value.id, patch: { title: value.title } }];
  }
  const adapter = effectful ? Object.freeze({ profile: EXTERNAL_ADAPTER_PROFILE, withAuthority,
    async execute(request, signal) { const data = validatedInput(request); need(request.inputDigest === canonicalHash(data));
      if (hive) await call(request, '/api/embed/operations', { expectedRevision: data.input.expectedRevision, operations: hiveOperation(data.input) }, signal);
      else await call(request, '/api/embed/invoke', data.input, signal); },
    async readProof(request, signal) {
      const data = validatedInput(request), base = { requestId: request.requestId, inputDigest: request.inputDigest };
      need(request.inputDigest === canonicalHash(data));
      let proof; try {
        if (hive) {
          const operations = hiveOperation(data.input), changes = await call(request, '/api/embed/changes', { after: data.input.expectedRevision }, signal);
          fields(changes, ['projectId', 'changes']); need(changes.projectId === scope.resource.selection.nativeId && Array.isArray(changes.changes) && changes.changes.length <= 1);
          const change = changes.changes[0];
          if (!change) return { ...base, outcome: 'not_applied' };
          fields(change, ['revision', 'requestId', 'actor', 'operations', 'createdAt']);
          if (change.requestId !== request.requestId) return { ...base, outcome: 'unknown' };
          need(change.revision === data.input.expectedRevision + 1 && same(change.operations, operations));
          // Never use POST's latest/replayed revision as this request's receipt.
          proof = { requestId: request.requestId, inputDigest: canonicalHash(data.input), outcome: 'committed',
            receipt: { projectId: changes.projectId, revision: change.revision, operations: change.operations } };
        } else proof = await call(request, '/api/embed/receipt', { inputDigest: canonicalHash(data.input) }, signal);
      }
      catch { return { ...base, outcome: 'unknown' }; }
      fields(proof, ['requestId', 'inputDigest', 'outcome'], ['replayed', 'receipt', 'result', 'data', ...(workspace ? ['contextAfter', 'workspaceReceipt'] : [])]);
      need(proof.requestId === request.requestId && proof.inputDigest === canonicalHash(data.input)
        && ['committed', 'not_applied', 'unknown'].includes(proof.outcome));
      if (proof.outcome !== 'committed') return { ...base, outcome: proof.outcome };
      const projection = workspace ? workspaceReceipt(proof.workspaceReceipt) : projectReceipt(proof, data.input);
      if (types.isPromise(projection)) { void Promise.prototype.then.call(projection, () => {}, () => {}); need(false); }
      let output = captureQueryData(projection); fields(output, ['effects', 'receipt']);
      if (workspace) {
        const after = contextAfter(proof.contextAfter);
        need(after.mutationRef === workspaceMutationRef(request.requestId, proof.inputDigest, data.contextRef)
          && output.receipt.verificationMethod === 'domain_read' && output.receipt.artifacts.length > 0);
        output = { ...output, receipt: { ...output.receipt, artifacts: output.receipt.artifacts.map(artifact => ({ ...artifact,
          sha256: workspaceWitness({ after, artifact, requestId: request.requestId, inputDigest: proof.inputDigest, scope }) })) } };
      }
      // Core validates declared effects, artifacts, their exact correspondence
      // and current grant before spending the original durable reservation.
      return { ...base, outcome: 'committed', ...output };
    } }) : createTrustedReadonlyQueryAdapter({ withAuthority,
      async query(request, signal, current) {
        const data = input(request.input); need(current() === true);
        let result;
        if (hive) {
          fields(data.input, ['operation']); need(['hive.cells.list', 'hive.cells.context'].includes(data.input.operation));
          const stored = await call(request, '/api/embed/project', {}, signal), project = stored.project;
          need(project?.id === scope.resource.selection.nativeId && project.schema === 'hive.document' && project.version === 2
            && Number.isSafeInteger(stored.revision) && stored.revision >= 1 && typeof project.title === 'string'
            && project.core?.objects && typeof project.core.objects === 'object' && !Array.isArray(project.core.objects));
          const cells = Object.values(project.core.objects).map(item => {
            need(typeof item?.id === 'string' && typeof item.title === 'string'); return { id: item.id, title: item.title };
          }).sort((a, b) => a.id.localeCompare(b.id));
          need(cells.length <= 100); result = { title: project.title, revision: stored.revision, cells };
          if (data.input.operation === 'hive.cells.context') {
            need(project.views && typeof project.views === 'object' && !Array.isArray(project.views));
            const views = Object.values(project.views), insertionViews = [];
            // No current-view pin is carried by this profile. Multiple views
            // must never silently select the first; creation is then omitted.
            if (views.length === 1) {
              const view = views[0]; need(typeof view.id === 'string' && view.placements
                && typeof view.placements === 'object' && !Array.isArray(view.placements));
              const placements = Object.values(view.placements); need(placements.length <= 100);
              let right = -40, top = 0;
              for (const item of placements) {
                const x = item.x ?? 0, y = item.y ?? 0, width = item.width ?? 280;
                need([x, y, width].every(Number.isFinite) && width >= 0 && Math.abs(x) <= 1000000 && Math.abs(y) <= 1000000);
                right = Math.max(right, x + width); top = Math.min(top, y);
              }
              if (/^[A-Za-z0-9_.:-]{1,128}$/u.test(view.id) && right + 40 <= 1000000)
                insertionViews.push({ id: view.id, x: right + 40, y: top });
            }
            result.insertionViews = insertionViews;
          }
        } else result = await call(request, '/api/embed/query', data.input, signal);
        if (readOutput !== undefined) {
          need(current() === true);
          const projected = readOutput(result, data.input);
          if (types.isPromise(projected)) { void Promise.prototype.then.call(projected, () => {}, () => {}); need(false); }
          result = captureQueryData(projected);
        }
        need(current() === true); registry.validateOutput(entry, result); return result;
      } });
  return Object.freeze({ appId: scope.appId, target: scope.target, catalog: wrapped, adapter,
    ...(guidance === undefined ? {} : { guidance: captureQueryData(guidance) }) });
}

export function bindSelectedNativeSourceAdapter(adapter, { appId, target, withOwner, apps }) {
  const field = Object.getOwnPropertyDescriptor(adapter, 'withAuthority');
  const port = field && Object.hasOwn(field, 'value') ? installed.get(field.value) : undefined;
  if (!port) return false;
  need(port.scope.appId === appId && same(port.scope.target, target) && typeof withOwner === 'function' && typeof apps === 'function');
  port.bind(Object.freeze({ withOwner, apps })); return true;
}
