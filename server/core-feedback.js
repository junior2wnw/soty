import { DatabaseSync } from 'node:sqlite';
import { createFeedbackService, FeedbackError } from '../modules/feedback/server/index.mjs';

// Apps validates /^app-[a-f0-9]{32}$/. This key is private storage scope,
// never an application, a registration or an authority supplied by a caller.
const CORE_SCOPE = 'soty.core.feedback';
const actions = ['context', 'submit', 'list', 'get', 'reply', 'status', 'accept'];
const operations = new Set(actions.map(action => `core.feedback.${action}`));
const requireThat = (ok, code, status = 400) => { if (!ok) throw new FeedbackError(code, status); };

export function coreFeedbackConfigurationFromEnvironment(env = {}) {
  requireThat([undefined, '', 'false', 'true'].includes(env.SOTY_CORE_FEEDBACK_ENABLED), 'core_feedback_configuration_invalid');
  return captureCoreFeedbackConfiguration({ enabled: env.SOTY_CORE_FEEDBACK_ENABLED === 'true', supportOwnerAccountId: env.SOTY_CORE_FEEDBACK_OWNER_ACCOUNT_ID ?? null });
}

export function captureCoreFeedbackConfiguration(configuration = { enabled: false }) {
  requireThat(configuration && typeof configuration === 'object' && !Array.isArray(configuration)
    && Object.keys(configuration).every(key => ['enabled', 'supportOwnerAccountId'].includes(key))
    && typeof configuration.enabled === 'boolean', 'core_feedback_configuration_invalid');
  const { enabled, supportOwnerAccountId = null } = configuration;
  requireThat(!enabled || typeof supportOwnerAccountId === 'string' && /^acct_[A-Za-z0-9_-]{32}$/u.test(supportOwnerAccountId), 'core_feedback_configuration_invalid');
  return Object.freeze({ enabled, supportOwnerAccountId });
}

/** Only the host installs this separate authority adapter. Apps keeps its
 * original authority, operations and immutable registrations unchanged. */
export function createCoreFeedback({ databasePath, connectDatabasePath, configuration, actorActive } = {}) {
  const { enabled, supportOwnerAccountId } = captureCoreFeedbackConfiguration(configuration);
  requireThat(typeof actorActive === 'function', 'core_feedback_host_required', 500);
  let engine, ownerReader, closed = false;
  function assertOwner() {
    const rows = ownerReader.prepare('SELECT tenant_id FROM feedback_installations WHERE app_id=? LIMIT 2').all(CORE_SCOPE);
    requireThat(rows.length < 2 && rows.every(row => row.tenant_id === supportOwnerAccountId), 'core_feedback_owner_change_requires_migration', 409);
  }
  try {
    if (enabled) {
      // Configuration can name only an account that already exists. It never
      // derives the recipient from the caller, browser or first administrator.
      let accounts;
      try {
        accounts = new DatabaseSync(connectDatabasePath, { readOnly: true });
        requireThat(accounts.prepare('SELECT 1 FROM accounts WHERE id=?').get(supportOwnerAccountId), 'core_feedback_owner_unavailable', 503);
      } catch { throw new FeedbackError('core_feedback_owner_unavailable', 503); }
      finally { accounts?.close(); }
      engine = createFeedbackService({ databasePath, actorActive,
        withAppAuthority({ actor, appId }, callback) {
          requireThat(appId === CORE_SCOPE, 'core_feedback_scope_invalid', 403);
          return callback(Object.freeze({ appId: CORE_SCOPE, accountId: actor.accountId, ownerId: supportOwnerAccountId,
            canManage: actor.accountId === supportOwnerAccountId, title: 'Соты', entry: null }));
        },
        withTransaction(run) {
          // The engine already holds BEGIN IMMEDIATE. Another process cannot
          // choose a different first owner between this check and installation.
          assertOwner(); return run();
        } });
      ownerReader = new DatabaseSync(databasePath, { readOnly: true });
      assertOwner();
    }
  } catch (error) { ownerReader?.close(); engine?.close(); throw error; }
  const projectTicket = value => { const { appId: _scope, ...ticket } = value; return { ...ticket, scope: 'core' }; };
  return Object.freeze({
    operations,
    status: () => ({ configured: Boolean(engine) && !closed }),
    // Keep this synchronous: signed Connect holds its active-device SQL fence
    // throughout the effect. Request receipts reconcile an unknown outer ACK.
    execute({ op, actor, args = {} }) {
      requireThat(!closed, 'core_feedback_unavailable', 503);
      requireThat(operations.has(op), 'unsupported_operation');
      requireThat(actorActive(actor) === true, 'feedback_authentication_required', 401);
      requireThat(args && typeof args === 'object' && !Array.isArray(args)
        && !['appId', 'owner', 'ownerId', 'recipient', 'supportOwnerAccountId', 'domainId', 'path'].some(key => Object.hasOwn(args, key)), 'core_feedback_invalid_arguments');
      const action = op.slice('core.feedback.'.length);
      if (!engine) {
        requireThat(action === 'context', 'core_feedback_unavailable', 503);
        requireThat(Object.keys(args).length === 0, 'core_feedback_invalid_arguments');
        return { readiness: 'unavailable', context: null };
      }
      const result = engine.execute({ op: `apps.feedback.${action}`, actor, args: { ...args, appId: CORE_SCOPE } });
      if (action === 'context') {
        const { appId: _scope, entry: _entry, ...context } = result.context;
        return { readiness: result.readiness, context: { ...context, scope: 'core', title: 'Соты', recipientLabel: 'Поддержка Сот' } };
      }
      return { ...result, ...(result.ticket ? { ticket: projectTicket(result.ticket) } : {}),
        ...(result.tickets ? { tickets: result.tickets.map(projectTicket) } : {}) };
    },
    close() { if (closed) return; closed = true; ownerReader?.close(); engine?.close(); }
  });
}
