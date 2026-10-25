import { appId, assertApps } from './protocol.mjs';

export const APP_AUDIENCES = Object.freeze(['private', 'friends', 'link', 'public']);
export const AUDIENCE_FORMATS = Object.freeze({
  v9: Object.freeze({ base: 'v6', version: 9, marker: 'soty.apps-registry.v9' }),
  v10: Object.freeze({ base: 'v7', version: 10, marker: 'soty.apps-registry.v10' }),
  v11: Object.freeze({ base: 'v8', version: 11, marker: 'soty.apps-registry.v11' }),
});
export const COVER_APP_FORMATS = Object.freeze({
  v12: Object.freeze({ base: 'v6', audience: 'v9', version: 12, marker: 'soty.apps-registry.v12' }),
  v13: Object.freeze({ base: 'v7', audience: 'v10', version: 13, marker: 'soty.apps-registry.v13' }),
  v14: Object.freeze({ base: 'v8', audience: 'v11', version: 14, marker: 'soty.apps-registry.v14' }),
});
export const baseAppsSchema = state => COVER_APP_FORMATS[state]?.base ?? AUDIENCE_FORMATS[state]?.base ?? state;
export const audienceAppsSchema = state => COVER_APP_FORMATS[state]?.audience ?? state;
export const audienceColumns = Object.freeze({
  app_audiences: ['app_id', 'audience'],
  app_discussion_head_audiences: ['app_id', 'current_id', 'audience'],
  app_discussion_conversation_audiences: ['conversation_id', 'app_id', 'audience'],
});
export function audienceDdl() {
  return `CREATE TABLE app_audiences (
    app_id TEXT PRIMARY KEY NOT NULL REFERENCES local_apps(id),
    audience TEXT NOT NULL CHECK(audience IN ('private','friends','link','public')));
  CREATE TABLE app_discussion_head_audiences (
    app_id TEXT PRIMARY KEY NOT NULL REFERENCES app_discussion_heads(app_id),current_id TEXT NOT NULL UNIQUE,
    audience TEXT NOT NULL CHECK(audience IN ('private','friends','link','public')));
  CREATE TABLE app_discussion_conversation_audiences (
    conversation_id TEXT PRIMARY KEY NOT NULL,app_id TEXT NOT NULL,
    audience TEXT NOT NULL CHECK(audience IN ('private','friends','link','public')),
    FOREIGN KEY(app_id,conversation_id) REFERENCES app_discussion_conversations(app_id,id));`;
}
export function audienceGuards() {
  return [
    "CREATE TRIGGER app_discussion_audience_no_update BEFORE UPDATE ON app_discussion_conversation_audiences BEGIN SELECT RAISE(ABORT,'app_discussion_audience_immutable'); END",
    "CREATE TRIGGER app_discussion_audience_no_delete BEFORE DELETE ON app_discussion_conversation_audiences BEGIN SELECT RAISE(ABORT,'app_discussion_audience_immutable'); END",
    "CREATE TRIGGER app_discussion_audience_no_replace BEFORE INSERT ON app_discussion_conversation_audiences WHEN EXISTS(SELECT 1 FROM app_discussion_conversation_audiences WHERE conversation_id=NEW.conversation_id) BEGIN SELECT RAISE(ABORT,'app_discussion_audience_immutable'); END",
  ];
}
export function normalizeAppAudience(value) {
  assertApps(value === null || APP_AUDIENCES.includes(value), 'invalid_app_audience'); return value;
}
export function audiencePolicy(audience) {
  normalizeAppAudience(audience);
  return audience === null ? null : { launchPolicy: ['link', 'public'].includes(audience) ? 'anyone' : 'restricted', listed: audience === 'public' };
}
export function audienceMatchesPolicy(audience, launchPolicy, listed) {
  const expected = audiencePolicy(audience);
  return expected === null || expected.launchPolicy === launchPolicy && expected.listed === listed;
}
export function hasAudienceSchema(db) {
  return [9, 10, 11, 12, 13, 14].includes(Number(db.prepare('PRAGMA user_version').get().user_version));
}
export function readAppAudience(db, id) {
  if (!hasAudienceSchema(db)) return null;
  return normalizeAppAudience(db.prepare('SELECT audience FROM app_audiences WHERE app_id=?').get(appId(id))?.audience ?? null);
}
export function writeAppAudience(db, id, audience) {
  assertApps(db.isTransaction && hasAudienceSchema(db), 'apps_audience_migration_required', 503);
  appId(id); normalizeAppAudience(audience);
  if (audience === null) db.prepare('DELETE FROM app_audiences WHERE app_id=?').run(id);
  else db.prepare('INSERT INTO app_audiences VALUES (?,?) ON CONFLICT(app_id) DO UPDATE SET audience=excluded.audience').run(id, audience);
}
export function readConversationAudience(db, row) {
  if (!hasAudienceSchema(db)) return null;
  const id = row.id ?? row.current_id;
  const historical = db.prepare('SELECT audience FROM app_discussion_conversation_audiences WHERE conversation_id=? AND app_id=?').get(id, row.app_id);
  const current = historical ?? db.prepare('SELECT audience FROM app_discussion_head_audiences WHERE current_id=? AND app_id=?').get(id, row.app_id);
  return normalizeAppAudience(current?.audience ?? null);
}
export function writeHeadAudience(db, app, id, audience) {
  if (!hasAudienceSchema(db)) { assertApps(audience === null, 'apps_audience_migration_required', 503); return; }
  assertApps(db.isTransaction, 'apps_transaction_required', 500); normalizeAppAudience(audience);
  if (audience === null) db.prepare('DELETE FROM app_discussion_head_audiences WHERE app_id=?').run(app);
  else db.prepare('INSERT INTO app_discussion_head_audiences VALUES (?,?,?) ON CONFLICT(app_id) DO UPDATE SET current_id=excluded.current_id,audience=excluded.audience').run(app, id, audience);
}
export function materializeConversationAudience(db, row) {
  const audience = readConversationAudience(db, row);
  if (audience !== null) db.prepare('INSERT INTO app_discussion_conversation_audiences VALUES (?,?,?)').run(row.id, row.app_id, audience);
}
