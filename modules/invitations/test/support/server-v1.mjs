import { DatabaseSync } from 'node:sqlite';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { INVITATION_SCHEMA, check, exact, identity, appIdentity, invitationToken, text, fields, answers } from './model.mjs';

export const invitationOperations = new Set(['invitations.get', 'invitations.configure', 'invitations.issue', 'invitations.revoke',
  'invitations.preview', 'invitations.submit', 'invitations.status', 'invitations.requests', 'invitations.decide',
  'invitations.members', 'invitations.member.set', 'invitations.inbox', 'invitations.request.get']);
const hash = value => createHash('sha256').update(value).digest('hex');
const uid = prefix => `${prefix}_${randomUUID().replaceAll('-', '')}`;
const positive = value => check(Number.isSafeInteger(value) && value > 0);
const revision = value => check(Number.isSafeInteger(value) && value >= 0);
const canonical = value => value && typeof value === 'object'
  ? Array.isArray(value) ? value.map(canonical) : Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])])) : value;
const encoded = value => JSON.stringify(canonical(value));
const blankPolicy = () => ({ enabled: false, mode: 'approval', description: '', fields: [], revision: 0 });

/** All mutations are signed Connect extensions. The host holds current Apps
 * authority through this SQLite commit. This store owns admission, never money. */
export function createInvitationsService({ databasePath, actorActive, withTarget, ownedTargets, onAccessChanged = () => {}, clock = Date.now }) {
  check(typeof databasePath === 'string' && typeof actorActive === 'function' && typeof withTarget === 'function'
    && typeof ownedTargets === 'function', 'invitation_configuration_invalid', 500);
  mkdirSync(dirname(databasePath), { recursive: true });
  const db = new DatabaseSync(databasePath); let closed = false;
  try {
    db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL;');
    const version = Number(db.prepare('PRAGMA user_version').get().user_version);
    const empty = !db.prepare("SELECT 1 FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' LIMIT 1").get();
    check(version === 1 || version === 0 && empty, 'invitation_schema_unsupported', 503);
    if (empty) db.exec(`BEGIN IMMEDIATE;
      CREATE TABLE policies(app_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,body TEXT NOT NULL,revision INTEGER NOT NULL);
      CREATE TABLE links(id TEXT PRIMARY KEY,app_id TEXT NOT NULL,owner_id TEXT NOT NULL,token TEXT NOT NULL UNIQUE,
        label TEXT NOT NULL,mode TEXT NOT NULL CHECK(mode IN('approval','direct')),issued_by TEXT NOT NULL,
        expires_at INTEGER NOT NULL,max_uses INTEGER NOT NULL,uses INTEGER NOT NULL DEFAULT 0,
        revoked INTEGER NOT NULL DEFAULT 0,created_at INTEGER NOT NULL,revoked_by TEXT,revoked_at INTEGER);
      CREATE INDEX links_app ON links(app_id,owner_id,created_at);
      CREATE TABLE requests(id TEXT PRIMARY KEY,app_id TEXT NOT NULL,owner_id TEXT NOT NULL,account_id TEXT NOT NULL,
        link_id TEXT NOT NULL REFERENCES links(id),state TEXT NOT NULL CHECK(state IN('pending','approved','declined')),
        form_json TEXT NOT NULL,answers_json TEXT NOT NULL,consent_at INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,
        revision INTEGER NOT NULL,decided_by TEXT,UNIQUE(app_id,owner_id,account_id));
      CREATE INDEX requests_app ON requests(app_id,owner_id,state,created_at);
      CREATE TABLE members(app_id TEXT NOT NULL,owner_id TEXT NOT NULL,account_id TEXT NOT NULL,label TEXT NOT NULL,
        role TEXT NOT NULL CHECK(role IN('member','director')),active INTEGER NOT NULL CHECK(active IN(0,1)),
        revision INTEGER NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(app_id,owner_id,account_id));
      CREATE INDEX members_account ON members(account_id,active,app_id);
      CREATE TABLE commands(actor_id TEXT NOT NULL,request_id TEXT NOT NULL,intent TEXT NOT NULL,result TEXT NOT NULL,
        created_at INTEGER NOT NULL,PRIMARY KEY(actor_id,request_id));
      PRAGMA user_version=1; COMMIT;`);
    const layout = db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
      .map(row => [row.type, row.name, row.tbl_name, typeof row.sql === 'string' ? row.sql.replace(/\s+/gu, ' ').trim() : null]);
    check(layout.length === 8 && hash(JSON.stringify(layout)) === '8aad8c6eb9c6f097818b39bb39c90c12f4d4b103794c0354b1f6a21b6a33a75b'
      && db.prepare('PRAGMA quick_check(1)').get().quick_check === 'ok' && !db.prepare('PRAGMA foreign_key_check').get(), 'invitation_store_invalid', 503);
  } catch (error) { db.close(); throw error; }
  const get = (sql, ...args) => db.prepare(sql).get(...args);
  const all = (sql, ...args) => db.prepare(sql).all(...args);
  const run = (sql, ...args) => db.prepare(sql).run(...args);
  function actorCheck(actor) {
    check(!closed, 'invitation_closed', 503);
    check(actor && actorActive(actor) === true, 'invitation_authentication_required', 401); identity(actor.accountId); identity(actor.deviceId);
  }
  function policy(target) {
    const row = get('SELECT * FROM policies WHERE app_id=? AND owner_id=?', target.appId, target.ownerId);
    return row ? { ...JSON.parse(row.body), revision: row.revision } : blankPolicy();
  }
  function membership(target, accountId) { return get('SELECT * FROM members WHERE app_id=? AND owner_id=? AND account_id=?', target.appId, target.ownerId, accountId); }
  const isOwner = (target, actor) => target.ownerId === actor.accountId;
  const isManager = (target, actor) => isOwner(target, actor) || membership(target, actor.accountId)?.active === 1 && membership(target, actor.accountId)?.role === 'director';
  function manage(target, actor) { check(isManager(target, actor), 'invitation_manager_required', 403); }
  function targetCall(id, actor, callback) {
    appIdentity(id); actorCheck(actor);
    return withTarget({ appId: id, actor }, target => { actorCheck(actor); return callback(target); });
  }
  function command(actor, op, args, callback) {
    const requestId = identity(args.requestId), intent = hash(encoded([op, args]));
    db.exec('BEGIN IMMEDIATE');
    try {
      actorCheck(actor);
      const previous = get('SELECT * FROM commands WHERE actor_id=? AND request_id=?', actor.accountId, requestId);
      if (previous) {
        check(previous.intent === intent, 'invitation_request_conflict', 409); db.exec('COMMIT'); return JSON.parse(previous.result);
      }
      check(get('SELECT count(*) AS n FROM commands WHERE actor_id=?', actor.accountId).n < 50000, 'invitation_storage_limit', 409);
      const result = callback(); actorCheck(actor);
      run('INSERT INTO commands VALUES (?,?,?,?,?)', actor.accountId, requestId, intent, JSON.stringify(result), clock());
      db.exec('COMMIT'); return result;
    } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  }
  function linkFor(token, available = true) {
    const row = get('SELECT * FROM links WHERE token=?', invitationToken(token));
    check(row && (!available || !row.revoked && row.expires_at > clock() && (!row.max_uses || row.uses < row.max_uses)), 'invitation_unavailable', 404);
    return row;
  }
  const projectRequest = row => ({ id: row.id, accountId: row.account_id, state: row.state, fields: JSON.parse(row.form_json),
    answers: JSON.parse(row.answers_json), createdAt: row.created_at, updatedAt: row.updated_at, revision: row.revision });
  function summarizeRequest(row) {
    const form = JSON.parse(row.form_json), values = JSON.parse(row.answers_json);
    const name = form.filter(field => /^(фамилия|имя|отчество)$/iu.test(field.label)).map(field => values[field.key]).filter(Boolean).join(' ').slice(0, 100) || 'Новый участник';
    const position = form.find(field => /^должность$/iu.test(field.label));
    return { id: row.id, accountId: row.account_id, state: row.state, name, position: position ? String(values[position.key] ?? '').slice(0, 100) : '', createdAt: row.created_at, revision: row.revision };
  }
  const projectMember = row => ({ accountId: row.account_id, label: row.label, role: row.role, active: Boolean(row.active), revision: row.revision });
  function checkedLink(token, actor, callback, available = true) {
    const link = linkFor(token, available);
    return withTarget({ appId: link.app_id, actor }, target => {
      check(target.ownerId === link.owner_id && (!available || policy(target).enabled), 'invitation_unavailable', 404);
      return callback(target, link);
    });
  }
  function preview(token) {
    check(!closed, 'invitation_closed', 503);
    return checkedLink(token, null, (target, link) => {
      const form = policy(target);
      return { schema: INVITATION_SCHEMA, appId: target.appId, title: target.title, description: form.description,
        fields: form.fields, formRevision: form.revision, mode: link.mode };
    });
  }
  function execute({ op, args = {}, actor }) {
    actorCheck(actor); check(invitationOperations.has(op));
    check(args.expectedAccountId === actor.accountId, 'invitation_account_changed', 401);
    const { expectedAccountId: _account, ...input } = args; args = input;
    if (op === 'invitations.preview') { exact(args, ['token']); return preview(args.token); }
    if (op === 'invitations.inbox') {
      exact(args, ['state', 'appId', 'offset'], []);
      const state = args.state ?? 'pending', offset = args.offset ?? 0;
      check(['pending', 'approved', 'declined'].includes(state) && Number.isSafeInteger(offset) && offset >= 0 && offset <= 10000);
      if (args.appId !== undefined) appIdentity(args.appId);
      const ids = new Set([...ownedTargets(actor), ...all("SELECT app_id FROM members WHERE account_id=? AND role='director' AND active=1", actor.accountId).map(row => row.app_id)]);
      const items = [];
      for (const id of ids) {
        try { targetCall(id, actor, target => {
          if (isManager(target, actor)) items.push({ appId: id, ownerId: target.ownerId, title: target.title,
            pendingCount: get("SELECT count(*) AS n FROM requests WHERE app_id=? AND owner_id=? AND state='pending'", id, target.ownerId).n });
        }); } catch (error) { if (error?.status !== 404) throw error; }
      }
      const selected = args.appId ? items.filter(item => item.appId === args.appId) : items;
      const rows = all(`SELECT r.* FROM requests r JOIN json_each(?) target
        ON r.app_id=json_extract(target.value,'$.appId') AND r.owner_id=json_extract(target.value,'$.ownerId')
        WHERE r.state=? ORDER BY r.created_at DESC,r.id LIMIT 21 OFFSET ?`, JSON.stringify(selected), state, offset);
      return { items: items.slice(0, 100).map(({ ownerId: _owner, ...item }) => item), managedCount: items.length,
        requests: rows.slice(0, 20).map(row => ({ ...summarizeRequest(row), appId: row.app_id, appTitle: items.find(item => item.appId === row.app_id).title })),
        nextOffset: rows.length > 20 ? offset + 20 : null };
    }
    if (op === 'invitations.submit' || op === 'invitations.status') {
      exact(args, op === 'invitations.submit' ? ['token', 'formRevision', 'answers', 'consent', 'requestId'] : ['token']);
      // A completed command remains recoverable after its link fills/expires.
      return checkedLink(args.token, actor, (target, link) => {
        const existing = () => get('SELECT * FROM requests WHERE app_id=? AND owner_id=? AND account_id=?', target.appId, target.ownerId, actor.accountId);
        if (op === 'invitations.status') {
          const member = membership(target, actor.accountId), request = existing();
          return { appId: target.appId, title: target.title, state: isOwner(target, actor) || member?.active ? 'approved' : request?.state === 'approved' ? 'removed' : request?.state ?? 'none', request: request ? projectRequest(request) : null };
        }
        return command(actor, op, args, () => {
          const current = existing();
          if (current && current.state !== 'declined') return { request: projectRequest(current) };
          linkFor(args.token); const form = policy(target);
          check(form.enabled, 'invitation_unavailable', 404);
          check(args.formRevision === form.revision, 'invitation_form_changed', 409);
          check(args.consent === true, 'invitation_consent_required');
          const values = answers(form.fields, args.answers), time = clock(), state = link.mode === 'direct' ? 'approved' : 'pending';
          check(get('SELECT count(*) AS n FROM requests WHERE app_id=?', target.appId).n < 10000 || current, 'invitation_storage_limit', 409);
          const requestId = current?.id ?? uid('req');
          run(`INSERT INTO requests VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(app_id,owner_id,account_id) DO UPDATE SET
            link_id=excluded.link_id,state=excluded.state,form_json=excluded.form_json,answers_json=excluded.answers_json,
            consent_at=excluded.consent_at,updated_at=excluded.updated_at,revision=requests.revision+1,decided_by=excluded.decided_by`,
          requestId, target.appId, target.ownerId, actor.accountId, link.id, state, JSON.stringify(form.fields), JSON.stringify(values), time, time, time, 1, state === 'approved' ? actor.accountId : null);
          run('UPDATE links SET uses=uses+1 WHERE id=?', link.id);
          if (state === 'approved') activateMember(target, actor.accountId, values, form.fields, time);
          return { request: projectRequest(existing()) };
        });
      }, false);
    }
    return targetCall(args.appId, actor, target => {
      manage(target, actor);
      if (op === 'invitations.get') {
        exact(args, ['appId', 'linkOffset'], ['appId']);
        const offset = args.linkOffset ?? 0; check(Number.isSafeInteger(offset) && offset >= 0 && offset <= 1000);
        const links = all('SELECT * FROM links WHERE app_id=? AND owner_id=? ORDER BY created_at DESC,id LIMIT 51 OFFSET ?', target.appId, target.ownerId, offset);
        return { schema: INVITATION_SCHEMA, appId: target.appId, title: target.title, owner: isOwner(target, actor), policy: policy(target),
          pendingCount: get("SELECT count(*) AS n FROM requests WHERE app_id=? AND owner_id=? AND state='pending'", target.appId, target.ownerId).n,
          linkCount: get('SELECT count(*) AS n FROM links WHERE app_id=? AND owner_id=?', target.appId, target.ownerId).n,
          linksNextOffset: links.length > 50 ? offset + 50 : null,
          links: links.slice(0, 50).map(row => ({ id: row.id, token: row.token, label: row.label, mode: row.mode, expiresAt: row.expires_at, maxUses: row.max_uses, uses: row.uses, revoked: Boolean(row.revoked) })) };
      }
      if (op === 'invitations.requests') {
        exact(args, ['appId', 'state', 'offset'], ['appId', 'state']); check(['pending', 'approved', 'declined'].includes(args.state));
        const offset = args.offset ?? 0; check(Number.isSafeInteger(offset) && offset >= 0 && offset <= 10000);
        const rows = all('SELECT * FROM requests WHERE app_id=? AND owner_id=? AND state=? ORDER BY created_at DESC,id LIMIT 21 OFFSET ?', target.appId, target.ownerId, args.state, offset);
        return { requests: rows.slice(0, 20).map(summarizeRequest), nextOffset: rows.length > 20 ? offset + 20 : null };
      }
      if (op === 'invitations.request.get') {
        exact(args, ['appId', 'applicationId']); identity(args.applicationId);
        const row = get('SELECT * FROM requests WHERE app_id=? AND owner_id=? AND id=?', target.appId, target.ownerId, args.applicationId);
        check(row, 'invitation_request_unavailable', 404); return { request: projectRequest(row) };
      }
      if (op === 'invitations.members') {
        exact(args, ['appId', 'offset'], ['appId']); const offset = args.offset ?? 0;
        check(Number.isSafeInteger(offset) && offset >= 0 && offset <= 10000);
        const rows = all('SELECT * FROM members WHERE app_id=? AND owner_id=? ORDER BY active DESC,updated_at DESC,account_id LIMIT 51 OFFSET ?', target.appId, target.ownerId, offset);
        return { owner: isOwner(target, actor), members: rows.slice(0, 50).map(projectMember), nextOffset: rows.length > 50 ? offset + 50 : null };
      }
      const result = command(actor, op, args, () => {
        if (op === 'invitations.configure') {
          exact(args, ['appId', 'requestId', 'expectedRevision', 'enabled', 'mode', 'description', 'fields']); revision(args.expectedRevision);
          check(args.expectedRevision === policy(target).revision, 'invitation_revision_conflict', 409);
          check(typeof args.enabled === 'boolean' && ['approval', 'direct'].includes(args.mode));
          const description = args.description === '' ? '' : text(args.description, 1200);
          const next = { enabled: args.enabled, mode: args.mode, description, fields: fields(args.fields) };
          run('INSERT INTO policies VALUES (?,?,?,?) ON CONFLICT(app_id) DO UPDATE SET owner_id=excluded.owner_id,body=excluded.body,revision=excluded.revision',
            target.appId, target.ownerId, JSON.stringify(next), args.expectedRevision + 1);
          return { policy: policy(target) };
        }
        if (op === 'invitations.issue') {
          exact(args, ['appId', 'requestId', 'label', 'mode', 'expiresAt', 'maxUses'], ['appId', 'requestId', 'label', 'expiresAt', 'maxUses']);
          const mode = args.mode ?? policy(target).mode; check(['approval', 'direct'].includes(mode));
          check(policy(target).enabled, 'invitation_disabled', 409);
          const time = clock();
          check(Number.isSafeInteger(args.expiresAt) && args.expiresAt > time && args.expiresAt <= time + 366 * 86400000
            && Number.isSafeInteger(args.maxUses) && args.maxUses >= 0 && args.maxUses <= 10000);
          check(get('SELECT count(*) AS n FROM links WHERE app_id=?', target.appId).n < 1000, 'invitation_storage_limit', 409);
          const link = { id: uid('inv'), token: randomBytes(32).toString('hex'), label: text(args.label, 80), mode, expiresAt: args.expiresAt, maxUses: args.maxUses, uses: 0, revoked: false };
          run('INSERT INTO links(id,app_id,owner_id,token,label,mode,issued_by,expires_at,max_uses,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)', link.id, target.appId, target.ownerId, link.token, link.label, mode, actor.accountId, link.expiresAt, link.maxUses, time);
          return { link };
        }
        if (op === 'invitations.revoke') {
          exact(args, ['appId', 'requestId', 'linkId']); identity(args.linkId);
          check(run('UPDATE links SET revoked=1,revoked_by=coalesce(revoked_by,?),revoked_at=coalesce(revoked_at,?) WHERE id=? AND app_id=? AND owner_id=?', actor.accountId, clock(), args.linkId, target.appId, target.ownerId).changes === 1, 'invitation_unavailable', 404);
          return { revoked: true };
        }
        if (op === 'invitations.decide') {
          exact(args, ['appId', 'requestId', 'applicationId', 'expectedRevision', 'decision']); identity(args.applicationId); positive(args.expectedRevision);
          check(['approved', 'declined'].includes(args.decision));
          const request = get('SELECT * FROM requests WHERE id=? AND app_id=? AND owner_id=?', args.applicationId, target.appId, target.ownerId);
          check(request, 'invitation_request_unavailable', 404);
          check(request.revision === args.expectedRevision && request.state === 'pending', 'invitation_revision_conflict', 409);
          run('UPDATE requests SET state=?,revision=revision+1,decided_by=?,updated_at=? WHERE id=?', args.decision, actor.accountId, clock(), request.id);
          if (args.decision === 'approved') activateMember(target, request.account_id, JSON.parse(request.answers_json), JSON.parse(request.form_json), clock());
          return { request: projectRequest(get('SELECT * FROM requests WHERE id=?', request.id)) };
        }
        exact(args, ['appId', 'requestId', 'accountId', 'label', 'role', 'active', 'expectedRevision']);
        identity(args.accountId); revision(args.expectedRevision);
        check(args.accountId !== target.ownerId && ['member', 'director'].includes(args.role) && typeof args.active === 'boolean');
        const member = membership(target, args.accountId);
        check(isOwner(target, actor) || member?.role === 'member' && args.role === 'member' && args.active === false, 'invitation_owner_required', 403);
        check((member?.revision ?? 0) === args.expectedRevision, 'invitation_revision_conflict', 409);
        check(member || args.active && args.role === 'director', 'invitation_member_unavailable', 404);
        run(`INSERT INTO members VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(app_id,owner_id,account_id) DO UPDATE SET
          label=excluded.label,role=excluded.role,active=excluded.active,revision=excluded.revision,updated_at=excluded.updated_at`,
        target.appId, target.ownerId, args.accountId, text(args.label, 100), args.role, Number(args.active), args.expectedRevision + 1, clock());
        return { member: projectMember(membership(target, args.accountId)) };
      });
      return result;
    });
  }
  function activateMember(target, accountId, values, form, time) {
    if (accountId === target.ownerId) return;
    const label = form.filter(field => /^(фамилия|имя|отчество)$/iu.test(field.label)).map(field => values[field.key]).filter(Boolean).join(' ').slice(0, 100) || 'Участник';
    const previous = membership(target, accountId);
    check(!previous || previous.active === 1, 'invitation_member_removed', 403);
    run(`INSERT INTO members VALUES (?,?,?,?,? ,1,1,?) ON CONFLICT(app_id,owner_id,account_id) DO UPDATE SET
      active=1,label=excluded.label,revision=members.revision+1,updated_at=excluded.updated_at`, target.appId, target.ownerId, accountId, label, previous?.role ?? 'member', time);
  }
  return Object.freeze({ operations: invitationOperations, preview,
    execute(request) {
      const result = execute(request);
      if (['invitations.submit', 'invitations.decide', 'invitations.member.set'].includes(request.op)) {
        const appId = request.args.appId ?? linkFor(request.args.token, false).app_id;
        onAccessChanged({ appId });
      }
      return result;
    },
    hasAccess({ appId, ownerId, accountId }) { return !closed && get('SELECT 1 FROM members WHERE app_id=? AND owner_id=? AND account_id=? AND active=1', appId, ownerId, accountId) !== undefined; },
    appIds(accountId) { return closed ? [] : all('SELECT DISTINCT app_id FROM members WHERE account_id=? AND active=1', accountId).map(row => row.app_id); },
    close() { if (!closed) { closed = true; db.close(); } },
  });
}
