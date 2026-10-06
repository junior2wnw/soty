import { createHash } from 'node:crypto';
import { appId, assertApps, AppsError, textId } from './protocol.mjs';
import { createEngagementTransaction } from './engagement-transaction.mjs';

export const directoryOperations = Object.freeze(['apps.directory.search', 'apps.directory.resolve']);
const schema = 'soty.app-directory.v1';
const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const exact = (value, allowed, required = []) => assertApps(value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).every(key => allowed.includes(key)) && required.every(key => Object.hasOwn(value, key)), 'invalid_arguments');
const folded = value => value.normalize('NFKD').replace(/\p{M}/gu, '').toLocaleLowerCase('ru');
const encode = (scope, row) => Buffer.from(JSON.stringify({ version: 1, scope, createdAt: row.created_at, id: row.id })).toString('base64url');
function afterCursor(value, scope) {
  if (value === undefined || value === null) return null;
  assertApps(typeof value === 'string' && value.length <= 768 && /^[A-Za-z0-9_-]+$/u.test(value), 'invalid_directory_cursor');
  try {
    const cursor = JSON.parse(Buffer.from(value, 'base64url').toString('utf8'));
    exact(cursor, ['version', 'scope', 'createdAt', 'id'], ['version', 'scope', 'createdAt', 'id']);
    assertApps(cursor.version === 1 && cursor.scope === scope && Number.isSafeInteger(cursor.createdAt) && cursor.createdAt >= 0, 'invalid_directory_cursor');
    appId(cursor.id); assertApps(encode(scope, { id: cursor.id, created_at: cursor.createdAt }) === value, 'invalid_directory_cursor');
    return cursor;
  } catch { throw new AppsError('invalid_directory_cursor'); }
}

/** No connector identities, grants, ports, private group IDs, tickets or human presence. */
export function createAppDirectory({ db, assertActor, withAuthorityFence, activeCommunityIds, canUse, resolveEntry }) {
  const run = createEngagementTransaction({ db, assertActor, withAuthorityFence, responseBytes: 128 * 1024,
    busyCode: 'apps_directory_busy', timeoutCode: 'apps_directory_timeout_invalid', responseCode: 'apps_directory_response_too_large' });
  db.function('apps_directory_match', { deterministic: true }, (name, rawQuery) => {
    const words = folded(name).match(/[\p{L}\p{N}]+/gu) ?? [];
    const terms = folded(rawQuery).match(/[\p{L}\p{N}]+/gu) ?? [];
    return Number((!rawQuery.trim() || terms.length > 0) && terms.every(term => words.some(word => word.startsWith(term))));
  });
  function projection(actor, app, visibility = 'all') {
    const own = app.owner_account_id === actor.accountId;
    const granted = canUse(actor, app);
    let entry, access;
    if (visibility !== 'public' && granted) {
      entry = resolveEntry({ actor, appId: app.id });
      if (entry) access = own ? 'owner' : 'granted';
    }
    if (!entry && visibility !== 'mine') {
      // Public discovery requires explicit listed+anyone and a currently bound
      // named address. Knowing an unlisted URL never makes it searchable.
      const address = db.prepare(`SELECT d.id FROM app_publications p JOIN app_publication_domains pd
        ON pd.app_id=p.app_id AND pd.owner_account_id=p.owner_account_id
        JOIN app_domains d ON d.id=pd.domain_id AND d.app_id=pd.app_id AND d.owner_account_id=pd.owner_account_id
        JOIN app_domain_zones z ON z.id=d.zone_id AND z.kind='named'
        WHERE p.app_id=? AND p.launch_policy='anyone' AND p.listed=1 AND d.role='alias' AND d.state='bound'
        ORDER BY d.created_at,d.id LIMIT 1`).get(app.id);
      if (address) { entry = resolveEntry({ actor, appId: app.id, domainId: address.id }); if (entry) access = 'public'; }
    }
    if (!entry) return null;
    return { id: app.id, name: app.name, ownerAccountId: app.owner_account_id, state: entry.status, createdAt: app.created_at, updatedAt: app.updated_at,
      access, canManage: own, entry: { appId: app.id, domainId: entry.domainId, origin: entry.origin, path: entry.path } };
  }
  function search(actor, args) {
    const rawQuery = args.query ?? '', visibility = args.scope ?? 'all', pageSize = args.limit ?? 30;
    assertApps(typeof rawQuery === 'string' && rawQuery.length <= 100 && rawQuery.isWellFormed()
      && !/[\u0000-\u001f\u007f]/u.test(rawQuery), 'invalid_directory_query');
    assertApps(['all', 'mine', 'public'].includes(visibility) && Number.isSafeInteger(pageSize) && pageSize >= 1 && pageSize <= 60, 'invalid_arguments');
    const query = rawQuery.normalize('NFC').trim(), scope = digest([schema, actor.accountId, actor.deviceId, query, visibility]);
    let after = afterCursor(args.cursor, scope), scanned = 0, exhausted = false;
    const apps = [], groups = typeof activeCommunityIds === 'function' ? activeCommunityIds(actor.accountId) : [];
    // Each call examines at most256 candidate rows. A short page can still have
    // a cursor when current authority removes candidates; never treat length as EOF.
    while (apps.length < pageSize && scanned < 256 && !exhausted) {
      const batchSize = Math.min(64, 256 - scanned);
      const rows = db.prepare(`SELECT a.* FROM local_apps a WHERE a.state='enabled'
        AND (? IS NULL OR a.created_at<? OR (a.created_at=? AND a.id>?))
        AND (?='' OR a.id=? OR apps_directory_match(a.name,?)=1)
        AND ((?<>'public' AND (a.owner_account_id=? OR a.id IN (
          SELECT app_id FROM local_app_grants WHERE kind='account' AND principal_id=?
          UNION SELECT app_id FROM local_app_grants WHERE kind='community' AND principal_id IN(SELECT value FROM json_each(?)))))
        OR (?<>'mine' AND EXISTS(SELECT 1 FROM app_publications p WHERE p.app_id=a.id AND p.launch_policy='anyone' AND p.listed=1)))
        ORDER BY a.created_at DESC,a.id LIMIT ?`).all(after?.createdAt ?? null, after?.createdAt ?? null,
      after?.createdAt ?? null, after?.id ?? '', query, query, query, visibility, actor.accountId, actor.accountId,
      JSON.stringify(groups), visibility, batchSize + 1);
      const batch = rows.slice(0, batchSize);
      for (let index = 0; index < batch.length; index++) {
        const row = batch[index]; scanned++; after = { createdAt: row.created_at, id: row.id };
        const value = projection(actor, row, visibility); if (value) apps.push(value);
        if (apps.length === pageSize) {
          exhausted = index === batch.length - 1 && rows.length <= batchSize;
          break;
        }
      }
      if (apps.length < pageSize) exhausted = rows.length <= batchSize;
    }
    return { schema, apps, nextCursor: exhausted || !after ? null : encode(scope, { created_at: after.createdAt, id: after.id }) };
  }
  return Object.freeze({ execute({ op, args, actor }) {
    if (op === 'apps.directory.search') {
      exact(args, ['query', 'scope', 'limit', 'cursor']);
      return run(actor, captured => search(captured, args));
    }
    exact(args, ['appIds'], ['appIds']);
    assertApps(Array.isArray(args.appIds) && args.appIds.length <= 60, 'invalid_directory_entities');
    const ids = args.appIds.map(appId);
    return run(actor, captured => ({ schema, items: ids.map(id => {
      const row = db.prepare("SELECT * FROM local_apps WHERE id=? AND state='enabled'").get(id);
      const app = row ? projection(captured, row) : null;
      return app ? { ref: { kind: 'app', id }, available: true, app } : { ref: { kind: 'app', id }, available: false };
    }) }));
  } });
}
