import { DatabaseSync } from 'node:sqlite';
import { personalPrincipalId } from './personal-documents.mjs';
import { check, fields, syncResult } from './wire.mjs';

const SCHEMA = 'soty.app-access.v1';
const ROLES = new Set(['owner', 'editor', 'viewer']);
const META = 'CREATE TABLE soty_app_access_meta(id INTEGER PRIMARY KEY CHECK(id=1),schema TEXT NOT NULL) STRICT';
const RESOURCES = 'CREATE TABLE soty_app_access_resources(app_id TEXT NOT NULL,resource_id TEXT NOT NULL,PRIMARY KEY(app_id,resource_id)) STRICT';
const MEMBERS = `CREATE TABLE soty_app_access_memberships(app_id TEXT NOT NULL,resource_id TEXT NOT NULL,principal_id TEXT NOT NULL,issuer TEXT NOT NULL,subject TEXT NOT NULL,role TEXT NOT NULL CHECK(role IN('owner','editor','viewer')),revision INTEGER NOT NULL CHECK(revision>=1),active INTEGER NOT NULL CHECK(active IN(0,1)),PRIMARY KEY(app_id,resource_id,principal_id),UNIQUE(app_id,resource_id,issuer,subject),FOREIGN KEY(app_id,resource_id) REFERENCES soty_app_access_resources(app_id,resource_id)) STRICT`;
const TABLES = new Map([['soty_app_access_meta', META], ['soty_app_access_resources', RESOURCES], ['soty_app_access_memberships', MEMBERS]]);

function identity(value) {
  const result = fields(value, ['issuer', 'subject']);
  let issuer;
  try { issuer = new URL(result.issuer); } catch { check(false, 'app_access_identity_invalid'); }
  check(typeof result.issuer === 'string' && issuer.protocol === 'https:' && !issuer.username && !issuer.password
    && issuer.href === result.issuer && issuer.pathname === '/human-identity' && !issuer.search && !issuer.hash
    && typeof result.subject === 'string' && result.subject.length >= 1 && result.subject.length <= 128
    && !/[\u0000-\u001f\u007f]/u.test(result.subject), 'app_access_identity_invalid');
  return Object.freeze(result);
}
const resource = value => { check(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u.test(value), 'app_access_resource_invalid'); return value; };
const synchronous = action => check(typeof action === 'function' && !['AsyncFunction', 'AsyncGeneratorFunction', 'GeneratorFunction'].includes(action.constructor?.name), 'app_access_callback_invalid', 503);

/** App-owned ACL in the application's actual effect database. Identities must
 * come from its server verifier. This module neither authenticates JSON nor
 * grants platform/card owners access. Callbacks are trusted synchronous code. */
export function createAppAccess(options) {
  const { database, appId, initialize = false } = fields(options, ['database', 'appId'], ['initialize']);
  check(database instanceof DatabaseSync && typeof appId === 'string' && /^app-[a-f0-9]{32}$/u.test(appId) && typeof initialize === 'boolean', 'app_access_configuration_invalid', 503);
  check(!database.isTransaction, 'app_access_transaction_active', 503);
  let busy = false;
  const proofs = new WeakMap();
  const objects = () => database.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name GLOB 'soty_app_access_*' ORDER BY name").all();
  if (!objects().length) {
    check(initialize, 'app_access_not_initialized', 503);
    database.exec('BEGIN IMMEDIATE');
    try {
      // Another writer may have initialized the same optional schema first.
      if (!objects().length) {
        database.exec(`${META};${RESOURCES};${MEMBERS};`);
        database.prepare('INSERT INTO soty_app_access_meta VALUES(1,?)').run(SCHEMA);
      }
      database.exec('COMMIT');
    } catch (error) { if (database.isTransaction) database.exec('ROLLBACK'); throw error; }
  }
  const installed = objects();
  check(installed.length === TABLES.size && installed.every(row => row.type === 'table' && TABLES.get(row.name) === row.sql), 'app_access_schema_invalid', 503);
  const meta = database.prepare('SELECT * FROM soty_app_access_meta').all();
  check(meta.length === 1 && meta[0].id === 1 && meta[0].schema === SCHEMA, 'app_access_schema_invalid', 503);

  function transaction(action) {
    check(!busy && !database.isTransaction, 'app_access_transaction_active', 503);
    busy = true;
    try {
      database.exec('BEGIN IMMEDIATE');
      const result = syncResult(action());
      check(database.isTransaction, 'app_access_transaction_lost', 503);
      database.exec('COMMIT');
      return result;
    } catch (error) { if (database.isTransaction) database.exec('ROLLBACK'); throw error; }
    finally { busy = false; }
  }
  function membership(who, resourceId) {
    const row = database.prepare('SELECT * FROM soty_app_access_memberships WHERE app_id=? AND resource_id=? AND issuer=? AND subject=?')
      .get(appId, resourceId, who.issuer, who.subject);
    check(row?.active === 1 && row.principal_id === personalPrincipalId(who) && ROLES.has(row.role)
      && Number.isSafeInteger(row.revision) && row.revision >= 1, 'app_access_denied', 403);
    check(database.prepare('SELECT 1 FROM soty_app_access_resources WHERE app_id=? AND resource_id=?').get(appId, resourceId), 'app_access_denied', 403);
    return row;
  }
  function capture(verifiedIdentity, resourceId) {
    const who = identity(verifiedIdentity); resource(resourceId);
    const row = membership(who, resourceId);
    const proof = Object.freeze({ appId, resourceId, principalId: row.principal_id, role: row.role, revision: row.revision });
    proofs.set(proof, { who, resourceId, role: row.role, revision: row.revision });
    return proof;
  }
  function withAccess(proof, options, action) {
    const { roles } = fields(options, ['roles']);
    check(Array.isArray(roles) && roles.length >= 1 && roles.length <= 3 && roles.every(role => ROLES.has(role)) && new Set(roles).size === roles.length,
      'app_access_roles_invalid');
    synchronous(action);
    const held = proofs.get(proof);
    check(held, 'app_access_proof_invalid', 403);
    return transaction(() => {
      const row = membership(held.who, held.resourceId);
      check(row.revision === held.revision && row.role === held.role, 'app_access_changed', 403);
      check(roles.includes(row.role), 'app_access_denied', 403);
      return syncResult(action(Object.freeze({ database, appId, resourceId: held.resourceId, identity: held.who,
        principalId: row.principal_id, role: row.role, revision: row.revision })));
    });
  }
  function setMembership(ownerProof, targetIdentity, role) {
    const who = identity(targetIdentity);
    check(role === null || ROLES.has(role), 'app_access_role_invalid');
    return withAccess(ownerProof, { roles: ['owner'] }, context => {
      const prior = database.prepare('SELECT * FROM soty_app_access_memberships WHERE app_id=? AND resource_id=? AND issuer=? AND subject=?')
        .get(appId, context.resourceId, who.issuer, who.subject);
      check(prior || role !== null, 'app_access_member_missing', 404);
      if (prior?.active === 1 && prior.role === 'owner' && role !== 'owner') {
        const owners = database.prepare("SELECT count(*) AS n FROM soty_app_access_memberships WHERE app_id=? AND resource_id=? AND active=1 AND role='owner'")
          .get(appId, context.resourceId).n;
        check(owners > 1, 'app_access_last_owner', 409);
      }
      const principalId = personalPrincipalId(who), revision = prior ? prior.revision + 1 : 1;
      check(Number.isSafeInteger(revision), 'app_access_revision_exhausted', 503);
      if (prior) {
        check(prior.principal_id === principalId, 'app_access_schema_invalid', 503);
        database.prepare('UPDATE soty_app_access_memberships SET role=?,revision=?,active=? WHERE app_id=? AND resource_id=? AND principal_id=?')
          .run(role ?? prior.role, revision, role === null ? 0 : 1, appId, context.resourceId, principalId);
      } else database.prepare('INSERT INTO soty_app_access_memberships VALUES(?,?,?,?,?,?,?,1)')
        .run(appId, context.resourceId, principalId, who.issuer, who.subject, role, revision);
      return Object.freeze({ appId, resourceId: context.resourceId, principalId, role, revision, active: role !== null });
    });
  }
  return Object.freeze({
    createResource(verifiedIdentity, resourceId) {
      const who = identity(verifiedIdentity); resource(resourceId);
      return transaction(() => {
        check(!database.prepare('SELECT 1 FROM soty_app_access_resources WHERE app_id=? AND resource_id=?').get(appId, resourceId), 'app_access_resource_exists', 409);
        database.prepare('INSERT INTO soty_app_access_resources VALUES(?,?)').run(appId, resourceId);
        database.prepare("INSERT INTO soty_app_access_memberships VALUES(?,?,?,?,?,'owner',1,1)")
          .run(appId, resourceId, personalPrincipalId(who), who.issuer, who.subject);
        return capture(who, resourceId);
      });
    },
    capture,
    withAccess,
    setRole: (ownerProof, targetIdentity, role) => setMembership(ownerProof, targetIdentity, role),
    revoke: (ownerProof, targetIdentity) => setMembership(ownerProof, targetIdentity, null),
  });
}
