import { createCatalog } from '../modules/capabilities/server/catalog.mjs';
import { EXTERNAL_ADAPTER_PROFILE } from '../modules/capabilities/server/external-adapters.mjs';
import { createTrustedReadonlyQueryAdapter } from '../modules/capabilities/server/readonly-queries.mjs';
import { AccessError, canonicalHash, freezeDeep } from '../modules/capabilities/server/validation.mjs';
import { appId as checkedAppId } from '../modules/apps/server/protocol.mjs';
import { FEEDBACK_PROVIDER } from '../modules/feedback/server/profile.mjs';

const object = (properties, required = Object.keys(properties)) => ({ type: 'object', properties, required, additionalProperties: false });
const text = maxLength => ({ type: 'string', minLength: 1, maxLength });
const choice = values => ({ type: 'string', enum: values, maxLength: Math.max(...values.map(value => value.length)) });
const integer = (minimum, maximum) => ({ type: 'integer', minimum, maximum });
const identifier = text(128), appId = text(36), body = text(8000), revision = integer(1, Number.MAX_SAFE_INTEGER);
const attachment = object({ id: identifier, kind: choice(['image', 'audio']), name: text(160),
  mimeType: text(100), byteLength: integer(1, 1048576) });
const message = object({ id: identifier, kind: choice(['reporter', 'support']), body, createdAt: integer(0, Number.MAX_SAFE_INTEGER) });
const ticket = (summary = false) => object({ id: identifier, appId, installationId: identifier,
  body: { type: 'string', maxLength: summary ? 240 : 8000 },
  status: choice(['received', 'in_progress', 'needs_action', 'ready_to_check', 'resolved']), revision,
  createdAt: integer(0, Number.MAX_SAFE_INTEGER), updatedAt: integer(0, Number.MAX_SAFE_INTEGER),
  attachments: { type: 'array', items: attachment, maxItems: 3 },
  messages: { type: 'array', items: message, maxItems: summary ? 0 : 20 },
  canReply: { type: 'boolean' }, canManage: { type: 'boolean' }, canAccept: { type: 'boolean' } });
const artifact = object({ type: text(128), id: identifier, revision });
const proof = object({ requestId: identifier, inputDigest: text(64), outcome: choice(['committed', 'not_applied', 'unknown']),
  effects: { type: 'array', maxItems: 1, items: object({ kind: choice(['created', 'updated']), resourceType: text(128), resourceId: identifier, revision }) },
  receipt: object({ verificationMethod: choice(['domain_read']), artifacts: { type: 'array', maxItems: 1, items: artifact } }) },
  ['requestId', 'inputDigest', 'outcome']);
const scope = { appId, installationId: identifier }, selected = { ...scope, ticketId: identifier };
const definitions = [
  ['context', ['create'], object({ appId }), proof, 'Ensure the selected owned app feedback installation. May create installation metadata and a durable request receipt.'],
  ['submit', ['create'], object({ ...scope, body }), proof, 'Submit one text ticket to your existing app installation. This does not send media or change application code.'],
  ['list', [], object({ ...scope, limit: integer(1, 10), cursor: { type: 'string', maxLength: 768 } }, Object.keys(scope)),
    object({ tickets: { type: 'array', maxItems: 10, items: ticket(true) }, nextCursor: { type: 'string', maxLength: 768 } }),
    'Read at most ten summaries from the current placement owner queue. Attachments are metadata only.'],
  ['get', [], object({ ...selected, messageOffset: integer(0, 128), messageLimit: integer(1, 20) }, Object.keys(selected)),
    object({ ticket: ticket(), messagesPage: object({ offset: integer(0, 128), total: integer(0, 128), nextOffset: integer(-1, 128) }) }),
    'Read one owned app ticket and a byte-bounded conversation page. Use nextOffset until -1. Media bytes are never returned.'],
  ['reply', ['update'], object({ ...selected, expectedRevision: revision, body }), proof, 'Reply to a selected app ticket only as authorized by the human. Preserve the exact intention after a lost response.'],
  ['status', ['update'], object({ ...selected, expectedRevision: revision,
    status: choice(['in_progress', 'needs_action', 'ready_to_check']) }), proof,
    'Change an owned app ticket status. Ready to check is not resolved; the original reporter accepts separately.'],
  ['accept', ['update'], object({ ...selected, expectedRevision: revision }), proof, 'Accept only a ticket reported by this same account after ready_to_check. Ownership alone cannot accept another reporter result.'],
];
const binding = { id: 'soty.app-feedback', version: 1,
  digest: canonicalHash({ profile: 'soty.app-feedback-capabilities.v1', provider: FEEDBACK_PROVIDER, projection: 'metadata-pages-v1' }) };
export const APP_FEEDBACK_CATALOG = freezeDeep(definitions.map(([operation, effects, inputSchema, outputSchema, description]) => ({
  capabilityId: 'apps.feedback.' + operation, version: 1, appId: 'soty.apps', title: 'App feedback · ' + operation,
  description, visibility: 'private', executionEnabled: true, inputSchema, outputSchema,
  resources: ['apps.feedback'], effects, recipients: ['soty:app-feedback'],
  executionBinding: { kind: 'registered', handler: 'apps.feedback.' + operation, version: 1, binding },
})));
export const appFeedbackResource = value => 'apps.feedback:' + checkedAppId(value);
const check = (condition, code = 'access_denied') => { if (!condition) throw new AccessError(code); };

/** Root composition only. Seven fixed contracts reuse the existing coordinator,
 * delegated grants, Apps authority, Feedback tables and domain receipts. */
export function createAppFeedbackCapabilities({ capabilities, connect, apps, feedback }) {
  check([capabilities, connect, apps, feedback].every(port => typeof port === 'function'), 'host_auth_required');
  const registry = createCatalog(APP_FEEDBACK_CATALOG), entries = registry.listAll();
  const byId = new Map(entries.map(entry => [entry.capabilityId, entry]));
  const reference = entry => ({ capabilityId: entry.capabilityId, version: entry.version, digest: entry.digest });
  function inputFor(entry, input) {
    registry.validateInput(entry, input); checkedAppId(input.appId); return input;
  }
  function owner(request, input, callback) {
    const requiredResources = input === undefined ? undefined : [appFeedbackResource(input.appId)];
    return connect().withAuthorityFence(() => request.actor
      ? capabilities().withOwnerAuthority({ actor: request.actor, requiredResources }, callback)
      : capabilities().withSnapshotOwnerAuthority({ authorization: request.authorization, requiredResources }, callback));
  }
  function owned(request, input, callback) {
    try { return owner(request, input, actor => apps().withAppAuthority({ actor, appId: input.appId, mode: 'owner' }, () => callback(actor))); }
    catch (error) {
      if (['apps_owner_required', 'apps_access_denied', 'app_unavailable'].includes(error?.code)) throw new AccessError('access_denied');
      throw error;
    }
  }
  function native(entry, request, callback) {
    const input = inputFor(entry, request.input);
    check(request.authorization.capabilityId === entry.capabilityId && request.authorization.version === entry.version
      && request.authorization.capabilityDigest === entry.digest);
    return owner(request, input, actor => callback(feedback().agent, { op: entry.capabilityId, actor, args: input, requestId: request.requestId }));
  }
  function receiptProof(entry, request, receipt) {
    if (!receipt) return { requestId: request.requestId, inputDigest: request.inputDigest, outcome: 'not_applied' };
    const context = entry.capabilityId === 'apps.feedback.context';
    // Context records an ensure receipt. It does not claim that an already
    // provisioned installation was newly created by this agent request.
    const value = { type: context ? 'feedback_context' : 'feedback_ticket',
      id: context ? receipt.installationId : receipt.ticketId, revision: receipt.revision };
    return { requestId: request.requestId, inputDigest: request.inputDigest, outcome: 'committed',
      effects: [{ kind: entry.effects.includes('create') ? 'created' : 'updated', resourceType: value.type, resourceId: value.id, revision: value.revision }],
      receipt: { verificationMethod: 'domain_read', artifacts: [value] } };
  }
  const adapters = entries.map(entry => {
    const withAuthority = (request, callback) => {
      if (request.input !== undefined) return owned(request, inputFor(entry, request.input), callback);
      return owner(request, undefined, callback);
    };
    if (!entry.effects.length) return { contract: reference(entry), adapter: createTrustedReadonlyQueryAdapter({ withAuthority,
      query(request, signal, current) {
        check(!signal.aborted); current();
        const value = native(entry, request, (port, input) => port.execute(input));
        current(); return value;
      } }) };
    return { contract: reference(entry), adapter: { profile: EXTERNAL_ADAPTER_PROFILE, withAuthority,
      execute(request, signal) {
        check(!signal.aborted);
        return native(entry, request, (port, input) => receiptProof(entry, request, port.execute(input).receipt));
      },
      readProof(request, signal) {
        check(!signal.aborted);
        return native(entry, request, (port, input) => receiptProof(entry, request, port.readReceipt(input)));
      } } };
  });
  function selected(ref) {
    const entry = byId.get(ref?.capabilityId);
    if (entry) check(ref.version === entry.version && ref.digest === entry.digest);
    return entry;
  }
  function beforeCall({ actor, name, args }) {
    if (!['apps_invoke', 'apps_query'].includes(name)) return;
    const entry = selected(args.reference);
    if (!entry) return;
    const input = inputFor(entry, args.input);
    owned({ actor }, input, () => capabilities().authorize({ actor, capabilityId: entry.capabilityId, version: entry.version,
      input, action: 'invoke' }));
  }
  return Object.freeze({ catalog: APP_FEEDBACK_CATALOG,
    effects: adapters.filter((_, index) => entries[index].effects.length),
    queries: adapters.filter((_, index) => !entries[index].effects.length),
    beforeCall,
    recheck(request) {
      beforeCall(request);
      if (!['apps_invocation_get', 'apps_invocation_cancel'].includes(request.name)) return;
      const entry = byId.get(request.value.invocation.capabilityId);
      if (!entry || !entry.effects.length) return; // Read-query history contains no content, input or target locator.
      const dispatch = capabilities().invocations.inspectDispatch({ invocationId: request.value.invocation.invocationId });
      const input = inputFor(entry, dispatch.input);
      owned({ actor: request.actor }, input, () => true);
    }
  });
}
