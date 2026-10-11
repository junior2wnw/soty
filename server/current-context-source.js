import { createTrustedReadonlyQueryAdapter, captureQueryData } from '../modules/capabilities/server/readonly-queries.mjs';
import { canonicalHash, freezeDeep, AccessError } from '../modules/capabilities/server/validation.mjs';
import { selectedResource } from '../modules/apps/scoped-embed/resource-profile.mjs';
import { HIVE_SELECTED_SOURCE, HIVE_SELECTED_KERNEL_SOURCE } from '../modules/apps/scoped-embed/resource-route-adapters.mjs';
import { STANDARD_SELECTED_SOURCE_V3, isWorkspaceSource } from '../modules/source-app/server/standard-profile.mjs';
import { workspaceContext, contextMetadata } from '../modules/source-app/server/workspace-context.mjs';

const ports = new WeakMap();
const need = value => { if (!value) throw new AccessError('current_context_denied'); };
const same = (a, b) => canonicalHash(a) === canonicalHash(b);
export const currentContextCapability = appId => `app.${appId}:current_context`;
const objectSchema = properties => ({ type: 'object', properties, required: Object.keys(properties), additionalProperties: false });

/** Installed CODE -> the existing externalApplications entry. No credential or
 * grant is minted here. Standard3 apps implement one Native read operation. */
export function createCurrentContextSourceApplication(raw) {
  const scope = captureQueryData(raw);
  need(Object.keys(scope).sort().join(',') === 'appId,resource,sourceProfile,target');
  need(/^app-[a-f0-9]{32}$/u.test(scope.appId));
  need(Object.keys(scope.target).sort().join(',') === 'digest,revision'
    && Number.isSafeInteger(scope.target.revision) && scope.target.revision > 0 && /^[a-f0-9]{64}$/u.test(scope.target.digest));
  selectedResource(scope.resource); need(scope.resource.appId === scope.appId);
  const workspace = scope.resource.selection.kind === 'soty.workspace.v1' && isWorkspaceSource(scope.sourceProfile);
  const standard = workspace || scope.resource.selection.kind === 'soty.resource.v1' && same(STANDARD_SELECTED_SOURCE_V3, scope.sourceProfile);
  need(standard || scope.resource.selection.kind === 'hive.project.v1'
    && [HIVE_SELECTED_SOURCE, HIVE_SELECTED_KERNEL_SOURCE].some(pin => same(pin, scope.sourceProfile)));
  freezeDeep(scope);
  let host;
  const withAuthority = (_request, callback) => { need(host); return callback(); };
  // The outer existing composition holds Root grant/owner/Apps fences. Native
  // selected authority is checked by the exact-handle Native read before data exists.
  ports.set(withAuthority, { scope, bind(value) { need(!host); host = value; } });
  const adapter = createTrustedReadonlyQueryAdapter({ withAuthority,
    async query(request, signal, current) {
      need(host && current() === true && !signal.aborted);
      const input = captureQueryData(request.input);
      need(Object.keys(input).join(',') === 'handle' && /^[A-Za-z0-9_-]{43}$/u.test(input.handle));
      const response = await host.apps().callScopedContextSource({ scope, handle: input.handle, requestId: request.requestId, signal,
        withCurrentOwner: callback => host.withOwner(request, callback) });
      need(current() === true && !signal.aborted && response.status === 200);
      const value = captureQueryData(response.data);
      if (standard) {
        need(value.ok === true && value.data && Object.keys(value.data).sort().join(',') === (workspace ? 'context,revision,role,schema,selection,title' : 'revision,role,schema,selection,title'));
        const data = value.data;
        need(data.schema === (workspace ? 'soty.current-context.v2' : 'soty.current-context.v1') && same(data.selection, scope.resource.selection)
          && ['owner', 'editor', 'viewer', 'reader'].includes(data.role)
          && typeof data.title === 'string' && data.title.trim() && data.title.length <= 240
          && !/[\u0000-\u001f\u007f]/u.test(data.title) && Number.isSafeInteger(data.revision) && data.revision >= 1);
        if (workspace) { contextMetadata({ title: data.title, role: data.role, revision: data.revision }); workspaceContext(data.context);
          need(data.context.expiresAt > Date.now() && data.context.expiresAt <= Date.now() + 300000); }
        return { title: data.title, role: data.role, revision: data.revision, ...(workspace ? { context: data.context } : {}) };
      }
      need(value.schema === 'hive.selected-context.v1' && value.version === 1
        && value.projectId === scope.resource.selection.nativeId && value.incarnationId === scope.resource.selection.incarnationId
        && value.access?.canRead === true && [null, 'owner', 'editor', 'viewer'].includes(value.access.role)
        && typeof value.title === 'string' && value.title.trim() && value.title.length <= 240
        && !/[\u0000-\u001f\u007f]/u.test(value.title) && Number.isSafeInteger(value.revision) && value.revision >= 1);
      return { title: value.title, role: value.access.role ?? 'reader', revision: value.revision };
    } });
  const capabilityId = currentContextCapability(scope.appId), resource = workspace ? `workspace:${scope.appId}:${scope.resource.resourceId}` : `${capabilityId}:selected`;
  return Object.freeze({ appId: scope.appId, target: scope.target, adapter,
    catalog: freezeDeep({ capabilityId, version: 1, appId: scope.appId, title: 'Контекст текущего проекта',
      description: 'Read bounded metadata of the exact current selected Native resource. No project body or write.',
      visibility: 'private', executionEnabled: true,
      inputSchema: objectSchema({ handle: { type: 'string', minLength: 43, maxLength: 43 } }),
      outputSchema: objectSchema({ title: { type: 'string', minLength: 1, maxLength: 240 },
        role: { type: 'string', maxLength: 16, enum: ['owner', 'editor', 'viewer', 'reader'] }, revision: { type: 'integer', minimum: 1 },
        ...(workspace ? { context: objectSchema({ ref: { type: 'string', minLength: 43, maxLength: 43 }, generation: { type: 'integer', minimum: 1 }, expiresAt: { type: 'integer', minimum: 1 } }) } : {}) }),
      resources: [resource], effects: [], recipients: [resource], executionBinding: { kind: 'registered', handler: capabilityId, version: 1,
        binding: { id: `${capabilityId}:readonly`, version: 1, digest: canonicalHash({ schema: 'soty.current-context-source.v1', scope }) } } }) });
}

export function bindCurrentContextSourceAdapter(adapter, { appId, target, withOwner, apps }) {
  const descriptor = Object.getOwnPropertyDescriptor(adapter, 'withAuthority');
  const port = descriptor && Object.hasOwn(descriptor, 'value') ? ports.get(descriptor.value) : undefined;
  if (!port) return false;
  need(port.scope.appId === appId && same(port.scope.target, target) && typeof withOwner === 'function' && typeof apps === 'function');
  port.bind(Object.freeze({ withOwner, apps })); return true;
}
