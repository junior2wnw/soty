import { createHash } from 'node:crypto';
import { AppsError, assertApps, appId, appPort, requestPath, cleanGrants, textId } from './protocol.mjs';
import { canonicalOrigin, legacyZone, normalizeLegacyTemplate } from './domain-policy.mjs';
import { createLaunchPath } from './launch-path.mjs';
import { canonical } from '../scoped-embed/profile.mjs';
import { approvedEmbedProfile } from '../scoped-embed/profile-dispatch.mjs';

export const APPS_REGISTRY_SCHEMA = 'soty.apps-registry.v6';
export const APPS_SCOPED_REGISTRY_SCHEMA = 'soty.apps-registry.v7';
export const APPS_RESOURCE_REGISTRY_SCHEMA = 'soty.apps-registry.v8';
export const SCOPED_RUNTIME_PROFILE = 'soty.selected-human-embed.v1';
export const RESOURCE_RUNTIME_PROFILE = 'soty.selected-human-embed.v2';
export const selectedRuntimeProfile = value => [SCOPED_RUNTIME_PROFILE, RESOURCE_RUNTIME_PROFILE].includes(value);
export const supportedRuntimeProfile = value => [RUNTIME_PROFILE, SCOPED_RUNTIME_PROFILE, RESOURCE_RUNTIME_PROFILE].includes(value);
const v1Schema = 'soty.apps-registry.v1';
const v2Schema = 'soty.apps-registry.v2';
const v3Schema = 'soty.apps-registry.v3';
const v4Schema = 'soty.apps-registry.v4';
const v5Schema = 'soty.apps-registry.v5';
export const RUNTIME_PROFILE = 'soty.relay-restricted.v1';
const core = {
  apps_meta: ['key', 'value'],
  app_devices: ['connector_key', 'owner_account_id', 'identity_json', 'name', 'created_at'],
  local_apps: ['id', 'owner_account_id', 'connector_key', 'name', 'port', 'entry_path', 'grants_json', 'state', 'revision', 'created_at', 'updated_at'],
  local_app_grants: ['app_id', 'kind', 'principal_id'],
};
const domains = {
  app_domain_zones: ['id', 'kind', 'origin_template', 'suffix', 'scheme', 'port', 'created_at'],
  app_domain_heads: ['app_id', 'revision'],
  app_domains: ['id', 'zone_id', 'hostname', 'origin', 'slug', 'app_id', 'owner_account_id', 'role', 'state', 'created_at', 'retired_at'],
  app_domain_receipts: ['account_id', 'request_key', 'intent_hash', 'action', 'domain_id', 'committed_revision', 'created_at'],
};
const publications = {
  app_runtime_targets: ['app_id', 'revision', 'owner_account_id', 'connector_key', 'port', 'entry_path', 'profile', 'digest', 'created_at'],
  app_publications: ['app_id', 'owner_account_id', 'launch_policy', 'listed', 'policy_epoch', 'active_target_revision', 'exposure_ack_revision', 'exposure_ack_json', 'updated_at'],
  app_publication_domains: ['app_id', 'domain_id', 'owner_account_id'],
  app_publication_receipts: ['account_id', 'request_key', 'intent_hash', 'app_id', 'committed_epoch', 'value_json', 'created_at'],
};
const sources = {
  app_source_heads: ['app_id', 'required_binding_version'],
  app_source_receipts: ['account_id', 'request_key', 'intent_hash', 'app_id', 'committed_epoch', 'value_json', 'created_at'],
};
const saved = {
  app_saved_heads: ['account_id', 'revision'],
  app_saved_entries: ['account_id', 'app_id', 'domain_id', 'origin', 'path', 'label', 'saved_revision', 'updated_at'],
  app_saved_receipts: ['account_id', 'request_key', 'intent_hash', 'app_id', 'saved', 'committed_revision', 'created_at'],
};
const discussions = {
  app_discussion_heads: ['app_id', 'current_id', 'generation', 'owner_account_id', 'mode', 'grants_json', 'audience_hash', 'message_count', 'body_bytes', 'conversation_count', 'rate_at', 'rate_credit', 'updated_at'],
  app_discussion_conversations: ['id', 'app_id', 'generation', 'owner_account_id', 'mode', 'grants_json', 'audience_hash', 'created_at', 'message_seq', 'change_seq'],
  app_discussion_messages: ['id', 'app_id', 'conversation_id', 'seq', 'author_account_id', 'author_label', 'request_key', 'intent_hash', 'body', 'body_bytes', 'reply_to_id', 'created_at', 'removed_at', 'removed_by'],
  app_discussion_changes: ['conversation_id', 'seq', 'message_id', 'kind', 'created_at'],
  app_discussion_usage: ['id', 'head_count', 'conversation_count', 'message_count', 'body_bytes'],
  app_discussion_rates: ['account_id', 'at', 'credit'],
};
const scopedAdmissions = {
  app_scoped_embed_admissions: ['app_id', 'target_revision', 'target_digest', 'profile_digest', 'approved_pin_json', 'created_at'],
};
const digest = value => createHash('sha256').update(value).digest('hex');
export const domainZoneId = zone => `zone_${digest(zone.origin_template ?? zone.template).slice(0, 32)}`;

function normalizedSql(sql) {
  // Only cosmetic differences outside string literals are accepted. This is a
  // known-DDL recognizer, not a claim to understand arbitrary equivalent SQL.
  return sql.split(/('(?:[^']|'')*')/gu).map((part, index) => index % 2 ? part
    : part.replace(/\bIF\s+NOT\s+EXISTS\b/giu, '').replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
}
function definitions(sql) {
  return (Array.isArray(sql) ? sql : sql.split(';')).filter(statement => statement.trim()).map(statement => {
    const name = statement.match(/^\s*CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX|TRIGGER)\s+([A-Za-z_][A-Za-z0-9_]*)\b/u)?.[1];
    assertApps(name, 'apps_schema_definition_invalid', 500);
    return [name, normalizedSql(statement)];
  });
}

export function inspectAppsSchema(db) {
  const objects = db.prepare("SELECT type,name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' AND type IN ('table','index','view','trigger')").all();
  const version = Number(db.prepare('PRAGMA user_version').get().user_version);
  if (objects.length === 0 && version === 0) return 'empty';
  assertApps(objects.some(item => item.name === 'apps_meta' && item.type === 'table'), 'apps_schema_unsupported');
  const meta = db.prepare('PRAGMA table_info(apps_meta)').all();
  assertApps(JSON.stringify(meta.map(item => item.name)) === JSON.stringify(core.apps_meta), 'apps_schema_unsupported');
  const schema = db.prepare("SELECT value FROM apps_meta WHERE key='schema'").get()?.value;
  const state = schema === v1Schema && [0, 1].includes(version) ? 'v1'
    : schema === v2Schema && version === 2 ? 'v2' : schema === v3Schema && version === 3 ? 'v3'
      : schema === v4Schema && version === 4 ? 'v4' : schema === v5Schema && version === 5 ? 'v5'
        : schema === APPS_REGISTRY_SCHEMA && version === 6 ? 'v6'
          : schema === APPS_SCOPED_REGISTRY_SCHEMA && version === 7 ? 'v7'
            : schema === APPS_RESOURCE_REGISTRY_SCHEMA && version === 8 ? 'v8' : '';
  assertApps(state, 'apps_schema_unsupported');
  const modern = ['v3', 'v4', 'v5', 'v6', 'v7', 'v8'].includes(state), sourceVersion = ['v4', 'v5', 'v6', 'v7', 'v8'].includes(state), savedVersion = ['v5', 'v6', 'v7', 'v8'].includes(state);
  const expected = state === 'v1' ? core : { ...core, ...domains, ...(modern ? publications : {}), ...(sourceVersion ? sources : {}),
    ...(savedVersion ? saved : {}), ...(['v6', 'v7', 'v8'].includes(state) ? discussions : {}), ...(['v7', 'v8'].includes(state) ? scopedAdmissions : {}) };
  const sqlDefinitions = new Map([...definitions(coreDdl()), ...(state !== 'v1' ? definitions(domainDdl()) : []),
    ...(modern ? [...definitions(publicationDdl(['v7', 'v8'].includes(state), state === 'v8')), ...definitions(targetGuards())] : []),
    ...(sourceVersion ? [...definitions(sourceDdl()), ...definitions(sourceGuards())] : []),
    ...(savedVersion ? [...definitions(savedDdl()), ...definitions(savedGuards())] : []),
    ...(['v6', 'v7', 'v8'].includes(state) ? [...definitions(discussionDdl()), ...definitions(discussionGuards())] : []),
    ...(['v7', 'v8'].includes(state) ? [...definitions(scopedAdmissionDdl()), ...definitions(scopedAdmissionGuards())] : [])]);
  assertApps(objects.length === sqlDefinitions.size && objects.every(item => typeof item.sql === 'string'
    && sqlDefinitions.get(item.name) === normalizedSql(item.sql)), 'apps_schema_unsupported');
  for (const [name, columns] of Object.entries(expected)) {
    const actual = db.prepare(`PRAGMA table_info(${name})`).all();
    assertApps(JSON.stringify(actual.map(item => item.name)) === JSON.stringify(columns), 'apps_schema_unsupported');
    const primaryKeys = name === 'local_app_grants' ? ['app_id', 'kind', 'principal_id']
      : ['app_domain_receipts', 'app_publication_receipts', 'app_source_receipts', 'app_saved_receipts'].includes(name) ? ['account_id', 'request_key']
        : name === 'app_saved_entries' ? ['account_id', 'app_id'] : ['app_saved_heads', 'app_discussion_rates'].includes(name) ? ['account_id']
          : name === 'app_scoped_embed_admissions' ? ['app_id', 'target_revision']
          : name === 'app_discussion_changes' ? ['conversation_id', 'seq']
        : name === 'app_runtime_targets' ? ['app_id', 'revision']
          : name === 'app_publication_domains' ? ['app_id', 'domain_id']
            : [name === 'apps_meta' ? 'key' : name === 'app_devices' ? 'connector_key'
              : ['app_domain_heads', 'app_publications', 'app_source_heads', 'app_discussion_heads'].includes(name) ? 'app_id' : 'id'];
    for (const column of actual) {
      const integer = ['revision', 'created_at', 'updated_at', 'retired_at', 'committed_revision', 'port', 'listed',
        'policy_epoch', 'active_target_revision', 'exposure_ack_revision', 'committed_epoch', 'required_binding_version', 'saved_revision', 'saved',
        'generation', 'message_count', 'body_bytes', 'conversation_count', 'rate_at', 'rate_credit', 'message_seq', 'change_seq', 'seq', 'removed_at', 'head_count', 'at', 'credit', 'target_revision'].includes(column.name)
        && !(name === 'app_domain_zones' && column.name === 'port');
      assertApps(column.type.toUpperCase() === (integer || (name === 'app_discussion_usage' && column.name === 'id') ? 'INTEGER' : 'TEXT')
        && column.pk === primaryKeys.indexOf(column.name) + 1, 'apps_schema_unsupported');
    }
  }
  return state;
}

function coreDdl() {
  return `CREATE TABLE apps_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
    CREATE TABLE app_devices (connector_key TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,identity_json TEXT NOT NULL,name TEXT NOT NULL,created_at INTEGER NOT NULL);
    CREATE TABLE local_apps (id TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,connector_key TEXT NOT NULL REFERENCES app_devices(connector_key),name TEXT NOT NULL,port INTEGER NOT NULL,entry_path TEXT NOT NULL,grants_json TEXT NOT NULL,state TEXT NOT NULL,revision INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
    CREATE INDEX local_apps_owner ON local_apps(owner_account_id);
    CREATE TABLE local_app_grants (app_id TEXT NOT NULL REFERENCES local_apps(id) ON DELETE CASCADE,kind TEXT NOT NULL CHECK(kind IN ('account','community')),principal_id TEXT NOT NULL,PRIMARY KEY(app_id,kind,principal_id));
    CREATE INDEX local_app_grants_principal ON local_app_grants(kind,principal_id,app_id);`;
}

function domainDdl() {
  return `CREATE TABLE app_domain_zones (
      id TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN ('legacy','named')), origin_template TEXT NOT NULL UNIQUE,
      suffix TEXT NOT NULL, scheme TEXT NOT NULL CHECK(scheme IN ('https','http')), port TEXT NOT NULL, created_at INTEGER NOT NULL);
    CREATE TABLE app_domain_heads (app_id TEXT PRIMARY KEY REFERENCES local_apps(id),revision INTEGER NOT NULL CHECK(revision>=0));
    CREATE TABLE app_domains (
      id TEXT PRIMARY KEY,zone_id TEXT NOT NULL REFERENCES app_domain_zones(id),hostname TEXT NOT NULL UNIQUE,
      origin TEXT NOT NULL UNIQUE,slug TEXT,app_id TEXT NOT NULL REFERENCES local_apps(id),owner_account_id TEXT NOT NULL,
      role TEXT NOT NULL CHECK(role IN ('canonical','alias')),state TEXT NOT NULL CHECK(state IN ('bound','tombstone')),
      created_at INTEGER NOT NULL,retired_at INTEGER,
      CHECK((role='canonical' AND slug IS NULL AND state='bound' AND retired_at IS NULL) OR
        (role='alias' AND slug IS NOT NULL AND ((state='bound' AND retired_at IS NULL) OR (state='tombstone' AND retired_at IS NOT NULL)))));
    CREATE UNIQUE INDEX app_domain_canonical ON app_domains(app_id) WHERE role='canonical';
    CREATE INDEX app_domain_app ON app_domains(app_id,created_at,id);
    CREATE INDEX app_domain_owner ON app_domains(owner_account_id,role);
    CREATE TABLE app_domain_receipts (
      account_id TEXT NOT NULL,request_key TEXT NOT NULL,intent_hash TEXT NOT NULL,
      action TEXT NOT NULL CHECK(action IN ('claim','retire')),domain_id TEXT NOT NULL REFERENCES app_domains(id),
      committed_revision INTEGER NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(account_id,request_key));`;
}

function publicationDdl(scoped = false, resources = false) {
  return `CREATE UNIQUE INDEX local_apps_identity_owner ON local_apps(id,owner_account_id);
    CREATE UNIQUE INDEX app_devices_identity_owner ON app_devices(connector_key,owner_account_id);
    CREATE UNIQUE INDEX app_domains_identity_owner ON app_domains(id,app_id,owner_account_id);
    CREATE TABLE app_runtime_targets (
      app_id TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 9007199254740991),
      owner_account_id TEXT NOT NULL,connector_key TEXT NOT NULL,port INTEGER NOT NULL CHECK(port BETWEEN 1024 AND 65535),
      entry_path TEXT NOT NULL,profile TEXT NOT NULL CHECK(${resources ? "profile IN ('soty.relay-restricted.v1','soty.selected-human-embed.v1','soty.selected-human-embed.v2')" : scoped ? "profile IN ('soty.relay-restricted.v1','soty.selected-human-embed.v1')" : "profile='soty.relay-restricted.v1'"}),digest TEXT NOT NULL,
      created_at INTEGER NOT NULL,PRIMARY KEY(app_id,revision),
      FOREIGN KEY(app_id,owner_account_id) REFERENCES local_apps(id,owner_account_id),
      FOREIGN KEY(connector_key,owner_account_id) REFERENCES app_devices(connector_key,owner_account_id));
    CREATE TABLE app_publications (
      app_id TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,
      launch_policy TEXT NOT NULL CHECK(launch_policy IN ('restricted','anyone')),
      listed INTEGER NOT NULL CHECK(listed IN (0,1)),policy_epoch INTEGER NOT NULL CHECK(policy_epoch BETWEEN 1 AND 9007199254740991),
      active_target_revision INTEGER NOT NULL,exposure_ack_revision INTEGER,exposure_ack_json TEXT,updated_at INTEGER NOT NULL,
      FOREIGN KEY(app_id,owner_account_id) REFERENCES local_apps(id,owner_account_id),
      FOREIGN KEY(app_id,active_target_revision) REFERENCES app_runtime_targets(app_id,revision),
      FOREIGN KEY(app_id,exposure_ack_revision) REFERENCES app_runtime_targets(app_id,revision),
      CHECK(listed=0 OR launch_policy='anyone'),
      CHECK((exposure_ack_revision IS NULL AND exposure_ack_json IS NULL) OR (exposure_ack_revision IS NOT NULL AND exposure_ack_json IS NOT NULL)),
      CHECK(launch_policy='restricted' OR (exposure_ack_revision IS NOT NULL AND exposure_ack_revision=active_target_revision AND exposure_ack_json IS NOT NULL)));
    CREATE TABLE app_publication_domains (
      app_id TEXT NOT NULL,domain_id TEXT NOT NULL,owner_account_id TEXT NOT NULL,PRIMARY KEY(app_id,domain_id),
      FOREIGN KEY(app_id) REFERENCES app_publications(app_id),
      FOREIGN KEY(app_id,owner_account_id) REFERENCES local_apps(id,owner_account_id),
      FOREIGN KEY(domain_id,app_id,owner_account_id) REFERENCES app_domains(id,app_id,owner_account_id));
    CREATE TABLE app_publication_receipts (
      account_id TEXT NOT NULL,request_key TEXT NOT NULL,intent_hash TEXT NOT NULL,app_id TEXT NOT NULL,
      committed_epoch INTEGER NOT NULL CHECK(committed_epoch BETWEEN 2 AND 9007199254740991),value_json TEXT NOT NULL,created_at INTEGER NOT NULL,
      PRIMARY KEY(account_id,request_key),FOREIGN KEY(app_id,account_id) REFERENCES local_apps(id,owner_account_id));
    CREATE UNIQUE INDEX app_publication_receipt_epoch ON app_publication_receipts(app_id,committed_epoch);`;
}

function scopedAdmissionDdl() {
  return `CREATE TABLE app_scoped_embed_admissions (
    app_id TEXT NOT NULL,target_revision INTEGER NOT NULL CHECK(target_revision>=1),
    target_digest TEXT NOT NULL CHECK(length(target_digest)=64),profile_digest TEXT NOT NULL CHECK(length(profile_digest)=64),
    approved_pin_json TEXT NOT NULL CHECK(length(approved_pin_json) BETWEEN 1 AND 16384),created_at INTEGER NOT NULL,
    PRIMARY KEY(app_id,target_revision),FOREIGN KEY(app_id,target_revision) REFERENCES app_runtime_targets(app_id,revision));`;
}
function scopedAdmissionGuards() {
  return [
    "CREATE TRIGGER app_scoped_admission_no_update BEFORE UPDATE ON app_scoped_embed_admissions BEGIN SELECT RAISE(ABORT,'app_scoped_admission_immutable'); END",
    "CREATE TRIGGER app_scoped_admission_no_delete BEFORE DELETE ON app_scoped_embed_admissions BEGIN SELECT RAISE(ABORT,'app_scoped_admission_immutable'); END",
    "CREATE TRIGGER app_scoped_admission_no_replace BEFORE INSERT ON app_scoped_embed_admissions WHEN EXISTS(SELECT 1 FROM app_scoped_embed_admissions WHERE app_id=NEW.app_id AND target_revision=NEW.target_revision) BEGIN SELECT RAISE(ABORT,'app_scoped_admission_immutable'); END",
  ];
}

function targetGuards() {
  return [
    "CREATE TRIGGER app_runtime_target_no_update BEFORE UPDATE ON app_runtime_targets BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END",
    "CREATE TRIGGER app_runtime_target_no_delete BEFORE DELETE ON app_runtime_targets BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END",
  ];
}

function sourceDdl() {
  return `CREATE TABLE app_source_heads (
      app_id TEXT PRIMARY KEY REFERENCES local_apps(id),required_binding_version INTEGER NOT NULL CHECK(required_binding_version IN (1,2)));
    CREATE TABLE app_source_receipts (
      account_id TEXT NOT NULL,request_key TEXT NOT NULL,intent_hash TEXT NOT NULL,app_id TEXT NOT NULL,
      committed_epoch INTEGER NOT NULL CHECK(committed_epoch BETWEEN 2 AND 9007199254740991),value_json TEXT NOT NULL,created_at INTEGER NOT NULL,
      PRIMARY KEY(account_id,request_key),FOREIGN KEY(app_id,account_id) REFERENCES local_apps(id,owner_account_id));
    CREATE UNIQUE INDEX app_source_receipt_epoch ON app_source_receipts(app_id,committed_epoch);`;
}

function sourceGuards() {
  return [
    "CREATE TRIGGER app_source_head_no_downgrade BEFORE UPDATE ON app_source_heads WHEN NEW.app_id<>OLD.app_id OR NEW.required_binding_version<OLD.required_binding_version BEGIN SELECT RAISE(ABORT,'app_source_binding_downgrade'); END",
    "CREATE TRIGGER app_source_head_no_delete BEFORE DELETE ON app_source_heads BEGIN SELECT RAISE(ABORT,'app_source_head_required'); END",
    "CREATE TRIGGER app_source_head_no_replace_downgrade BEFORE INSERT ON app_source_heads WHEN EXISTS (SELECT 1 FROM app_source_heads WHERE app_id=NEW.app_id AND required_binding_version>NEW.required_binding_version) BEGIN SELECT RAISE(ABORT,'app_source_binding_downgrade'); END",
    "CREATE TRIGGER app_runtime_target_no_replace BEFORE INSERT ON app_runtime_targets WHEN EXISTS (SELECT 1 FROM app_runtime_targets WHERE app_id=NEW.app_id AND revision=NEW.revision) BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END",
  ];
}

function savedDdl() {
  return `CREATE TABLE app_saved_heads (
      account_id TEXT PRIMARY KEY NOT NULL,revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 9007199254740991));
    CREATE TABLE app_saved_entries (
      account_id TEXT NOT NULL REFERENCES app_saved_heads(account_id),app_id TEXT NOT NULL REFERENCES local_apps(id),
      domain_id TEXT NOT NULL REFERENCES app_domains(id),origin TEXT NOT NULL CHECK(length(origin) BETWEEN 1 AND 512),
      path TEXT NOT NULL CHECK(length(path) BETWEEN 1 AND 8192),label TEXT NOT NULL CHECK(length(label) BETWEEN 1 AND 64),
      saved_revision INTEGER NOT NULL CHECK(saved_revision BETWEEN 1 AND 9007199254740991),
      updated_at INTEGER NOT NULL CHECK(updated_at BETWEEN 0 AND 9007199254740991),PRIMARY KEY(account_id,app_id));
    CREATE UNIQUE INDEX app_saved_entry_revision ON app_saved_entries(account_id,saved_revision);
    CREATE TABLE app_saved_receipts (
      account_id TEXT NOT NULL REFERENCES app_saved_heads(account_id),request_key TEXT NOT NULL CHECK(length(request_key)=64),
      intent_hash TEXT NOT NULL CHECK(length(intent_hash)=64),app_id TEXT NOT NULL,saved INTEGER NOT NULL CHECK(saved IN (0,1)),
      committed_revision INTEGER NOT NULL CHECK(committed_revision BETWEEN 1 AND 9007199254740991),
      created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),PRIMARY KEY(account_id,request_key));
    CREATE UNIQUE INDEX app_saved_receipt_revision ON app_saved_receipts(account_id,committed_revision);`;
}

function savedGuards() {
  return [
    "CREATE TRIGGER app_saved_head_no_downgrade BEFORE UPDATE ON app_saved_heads WHEN NEW.account_id<>OLD.account_id OR NEW.revision<=OLD.revision BEGIN SELECT RAISE(ABORT,'app_saved_revision_not_increasing'); END",
    "CREATE TRIGGER app_saved_head_no_delete BEFORE DELETE ON app_saved_heads BEGIN SELECT RAISE(ABORT,'app_saved_head_required'); END",
    "CREATE TRIGGER app_saved_head_no_replace BEFORE INSERT ON app_saved_heads WHEN EXISTS (SELECT 1 FROM app_saved_heads WHERE account_id=NEW.account_id) BEGIN SELECT RAISE(ABORT,'app_saved_head_immutable'); END",
  ];
}

function discussionDdl() {
  return `CREATE TABLE app_discussion_heads (
      app_id TEXT PRIMARY KEY NOT NULL,current_id TEXT NOT NULL UNIQUE,generation INTEGER NOT NULL CHECK(generation BETWEEN 1 AND 9007199254740991),
      owner_account_id TEXT NOT NULL,mode TEXT NOT NULL CHECK(mode IN ('restricted','anyone')),grants_json TEXT NOT NULL,audience_hash TEXT NOT NULL,
      message_count INTEGER NOT NULL CHECK(message_count BETWEEN 0 AND 9007199254740991),body_bytes INTEGER NOT NULL CHECK(body_bytes BETWEEN 0 AND 9007199254740991),
      conversation_count INTEGER NOT NULL CHECK(conversation_count BETWEEN 0 AND 9007199254740991),
      rate_at INTEGER NOT NULL CHECK(rate_at BETWEEN 0 AND 9007199254740991),rate_credit INTEGER NOT NULL CHECK(rate_credit BETWEEN 0 AND 15000),
      updated_at INTEGER NOT NULL CHECK(updated_at BETWEEN 0 AND 9007199254740991),
      FOREIGN KEY(app_id,owner_account_id) REFERENCES local_apps(id,owner_account_id));
    CREATE TABLE app_discussion_conversations (
      id TEXT PRIMARY KEY NOT NULL,app_id TEXT NOT NULL REFERENCES app_discussion_heads(app_id),
      generation INTEGER NOT NULL CHECK(generation BETWEEN 1 AND 9007199254740991),owner_account_id TEXT NOT NULL,
      mode TEXT NOT NULL CHECK(mode IN ('restricted','anyone')),grants_json TEXT NOT NULL,audience_hash TEXT NOT NULL,
      created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
      message_seq INTEGER NOT NULL CHECK(message_seq BETWEEN 1 AND 9007199254740991),change_seq INTEGER NOT NULL CHECK(change_seq BETWEEN 1 AND 9007199254740991),
      FOREIGN KEY(app_id,owner_account_id) REFERENCES local_apps(id,owner_account_id));
    CREATE UNIQUE INDEX app_discussion_conversation_generation ON app_discussion_conversations(app_id,generation);
    CREATE UNIQUE INDEX app_discussion_conversation_app ON app_discussion_conversations(app_id,id);
    CREATE TABLE app_discussion_messages (
      id TEXT PRIMARY KEY NOT NULL,app_id TEXT NOT NULL,conversation_id TEXT NOT NULL,seq INTEGER NOT NULL CHECK(seq BETWEEN 1 AND 9007199254740991),
      author_account_id TEXT NOT NULL,author_label TEXT NOT NULL CHECK(length(author_label) BETWEEN 1 AND 80),
      request_key TEXT NOT NULL CHECK(length(request_key)=64),intent_hash TEXT NOT NULL CHECK(length(intent_hash)=64),
      body TEXT,body_bytes INTEGER NOT NULL CHECK(body_bytes BETWEEN 0 AND 16384),reply_to_id TEXT,
      created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),removed_at INTEGER CHECK(removed_at BETWEEN 0 AND 9007199254740991),removed_by TEXT,
      FOREIGN KEY(app_id,conversation_id) REFERENCES app_discussion_conversations(app_id,id),
      FOREIGN KEY(conversation_id,reply_to_id) REFERENCES app_discussion_messages(conversation_id,id),
      CHECK((body IS NOT NULL AND body_bytes>0 AND removed_at IS NULL AND removed_by IS NULL) OR
        (body IS NULL AND body_bytes=0 AND removed_at IS NOT NULL AND removed_by IS NOT NULL)));
    CREATE UNIQUE INDEX app_discussion_message_request ON app_discussion_messages(author_account_id,request_key);
    CREATE UNIQUE INDEX app_discussion_message_sequence ON app_discussion_messages(conversation_id,seq);
    CREATE UNIQUE INDEX app_discussion_message_conversation ON app_discussion_messages(conversation_id,id);
    CREATE INDEX app_discussion_message_app ON app_discussion_messages(app_id);
    CREATE TABLE app_discussion_changes (
      conversation_id TEXT NOT NULL,seq INTEGER NOT NULL CHECK(seq BETWEEN 1 AND 9007199254740991),message_id TEXT NOT NULL,
      kind TEXT NOT NULL CHECK(kind IN ('message','removed')),created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
      PRIMARY KEY(conversation_id,seq),FOREIGN KEY(conversation_id,message_id) REFERENCES app_discussion_messages(conversation_id,id));
    CREATE TABLE app_discussion_usage (
      id INTEGER PRIMARY KEY CHECK(id=1),head_count INTEGER NOT NULL CHECK(head_count BETWEEN 0 AND 9007199254740991),
      conversation_count INTEGER NOT NULL CHECK(conversation_count BETWEEN 0 AND 9007199254740991),
      message_count INTEGER NOT NULL CHECK(message_count BETWEEN 0 AND 9007199254740991),body_bytes INTEGER NOT NULL CHECK(body_bytes BETWEEN 0 AND 9007199254740991));
    CREATE TABLE app_discussion_rates (
      account_id TEXT PRIMARY KEY NOT NULL,at INTEGER NOT NULL CHECK(at BETWEEN 0 AND 9007199254740991),credit INTEGER NOT NULL CHECK(credit BETWEEN 0 AND 20000));`;
}

function discussionGuards() {
  return [
    "CREATE TRIGGER app_discussion_head_no_delete BEFORE DELETE ON app_discussion_heads BEGIN SELECT RAISE(ABORT,'app_discussion_head_required'); END",
    "CREATE TRIGGER app_discussion_head_no_replace BEFORE INSERT ON app_discussion_heads WHEN EXISTS (SELECT 1 FROM app_discussion_heads WHERE app_id=NEW.app_id) BEGIN SELECT RAISE(ABORT,'app_discussion_head_required'); END",
    "CREATE TRIGGER app_discussion_head_lineage BEFORE UPDATE ON app_discussion_heads WHEN NEW.app_id<>OLD.app_id OR NEW.owner_account_id<>OLD.owner_account_id OR NEW.generation<OLD.generation OR (NEW.generation=OLD.generation AND (NEW.current_id<>OLD.current_id OR NEW.mode<>OLD.mode OR NEW.grants_json<>OLD.grants_json OR NEW.audience_hash<>OLD.audience_hash)) OR (NEW.generation>OLD.generation AND (NEW.generation<>OLD.generation+1 OR NEW.current_id=OLD.current_id OR EXISTS (SELECT 1 FROM app_discussion_conversations WHERE id=NEW.current_id))) BEGIN SELECT RAISE(ABORT,'app_discussion_lineage_immutable'); END",
    "CREATE TRIGGER app_discussion_conversation_immutable BEFORE UPDATE OF id,app_id,generation,owner_account_id,mode,grants_json,audience_hash,created_at ON app_discussion_conversations BEGIN SELECT RAISE(ABORT,'app_discussion_audience_immutable'); END",
    "CREATE TRIGGER app_discussion_conversation_no_delete BEFORE DELETE ON app_discussion_conversations BEGIN SELECT RAISE(ABORT,'app_discussion_audience_immutable'); END",
    "CREATE TRIGGER app_discussion_conversation_no_replace BEFORE INSERT ON app_discussion_conversations WHEN EXISTS (SELECT 1 FROM app_discussion_conversations WHERE id=NEW.id OR (app_id=NEW.app_id AND generation=NEW.generation)) BEGIN SELECT RAISE(ABORT,'app_discussion_audience_immutable'); END",
    "CREATE TRIGGER app_discussion_message_immutable BEFORE UPDATE OF id,app_id,conversation_id,seq,author_account_id,author_label,request_key,intent_hash,reply_to_id,created_at ON app_discussion_messages BEGIN SELECT RAISE(ABORT,'app_discussion_message_immutable'); END",
    "CREATE TRIGGER app_discussion_message_no_delete BEFORE DELETE ON app_discussion_messages BEGIN SELECT RAISE(ABORT,'app_discussion_message_immutable'); END",
    "CREATE TRIGGER app_discussion_message_no_replace BEFORE INSERT ON app_discussion_messages WHEN EXISTS (SELECT 1 FROM app_discussion_messages WHERE id=NEW.id OR (author_account_id=NEW.author_account_id AND request_key=NEW.request_key) OR (conversation_id=NEW.conversation_id AND seq=NEW.seq)) BEGIN SELECT RAISE(ABORT,'app_discussion_message_immutable'); END",
    "CREATE TRIGGER app_discussion_message_redaction BEFORE UPDATE OF body,body_bytes,removed_at,removed_by ON app_discussion_messages WHEN OLD.removed_at IS NOT NULL OR NEW.body IS NOT NULL OR NEW.body_bytes<>0 OR NEW.removed_at IS NULL OR NEW.removed_by IS NULL BEGIN SELECT RAISE(ABORT,'app_discussion_redaction_required'); END",
    "CREATE TRIGGER app_discussion_change_immutable BEFORE UPDATE ON app_discussion_changes BEGIN SELECT RAISE(ABORT,'app_discussion_change_immutable'); END",
    "CREATE TRIGGER app_discussion_usage_no_delete BEFORE DELETE ON app_discussion_usage BEGIN SELECT RAISE(ABORT,'app_discussion_usage_required'); END",
    "CREATE TRIGGER app_discussion_usage_no_replace BEFORE INSERT ON app_discussion_usage WHEN EXISTS (SELECT 1 FROM app_discussion_usage WHERE id=NEW.id) BEGIN SELECT RAISE(ABORT,'app_discussion_usage_required'); END",
  ];
}

export function discussionAudienceDigest(ownerAccountId, mode, grants) {
  textId(ownerAccountId); assertApps(['restricted', 'anyone'].includes(mode), 'apps_registry_corrupt', 500);
  const normalized = cleanGrants(grants);
  return digest(JSON.stringify(['soty.app-discussion.audience.v1', ownerAccountId, mode, normalized.accountIds, normalized.communityIds]));
}

export function readDiscussionAudience(db, id) {
  const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(appId(id));
  const policy = db.prepare('SELECT owner_account_id,launch_policy FROM app_publications WHERE app_id=?').get(id);
  assertApps(app && policy?.owner_account_id === app.owner_account_id, 'apps_registry_corrupt', 500);
  let grants; try { grants = cleanGrants(JSON.parse(app.grants_json)); } catch { throw new AppsError('apps_registry_corrupt', 500); }
  return { app, ownerAccountId: app.owner_account_id, mode: policy.launch_policy, grants,
    hash: discussionAudienceDigest(app.owner_account_id, policy.launch_policy, grants) };
}

function validateDiscussionRows(db) {
  assertApps(!db.prepare('PRAGMA foreign_key_check').get(), 'apps_registry_corrupt', 500);
  const safe = (value, minimum = 0) => assertApps(Number.isSafeInteger(value) && value >= minimum, 'apps_registry_corrupt', 500);
  const identifier = (value, prefix) => assertApps(typeof value === 'string' && new RegExp(`^${prefix}_[a-f0-9]{32}$`, 'u').test(value), 'apps_registry_corrupt', 500);
  const hashValue = value => assertApps(typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value), 'apps_registry_corrupt', 500);
  function audience(row) {
    textId(row.owner_account_id); let grants;
    try { grants = cleanGrants(JSON.parse(row.grants_json)); } catch { throw new AppsError('apps_registry_corrupt', 500); }
    assertApps(JSON.stringify(grants) === row.grants_json && Buffer.byteLength(row.grants_json, 'utf8') <= 32768
      && discussionAudienceDigest(row.owner_account_id, row.mode, grants) === row.audience_hash, 'apps_registry_corrupt', 500);
  }
  const usage = db.prepare('SELECT * FROM app_discussion_usage').all();
  assertApps(usage.length === 1 && usage[0].id === 1, 'apps_registry_corrupt', 500);
  for (const column of ['head_count', 'conversation_count', 'message_count', 'body_bytes']) safe(usage[0][column]);
  const totals = { head_count: 0, conversation_count: 0, message_count: 0, body_bytes: 0 };
  for (const head of db.prepare('SELECT * FROM app_discussion_heads').iterate()) {
    totals.head_count++; appId(head.app_id); identifier(head.current_id, 'conv'); audience(head);
    safe(head.generation, 1); safe(head.rate_at); safe(head.rate_credit); safe(head.updated_at);
    const current = readDiscussionAudience(db, head.app_id);
    assertApps(head.owner_account_id === current.ownerAccountId && head.audience_hash === current.hash, 'apps_registry_corrupt', 500);
    const messages = db.prepare('SELECT count(*) AS n,coalesce(sum(body_bytes),0) AS bytes FROM app_discussion_messages WHERE app_id=?').get(head.app_id);
    const conversations = db.prepare('SELECT count(*) AS n FROM app_discussion_conversations WHERE app_id=?').get(head.app_id);
    assertApps(head.message_count === messages.n && head.body_bytes === messages.bytes && head.conversation_count === conversations.n, 'apps_registry_corrupt', 500);
    totals.message_count += messages.n; totals.body_bytes += messages.bytes; totals.conversation_count += conversations.n;
    const materialized = db.prepare('SELECT * FROM app_discussion_conversations WHERE id=?').get(head.current_id);
    assertApps(!materialized || (materialized.app_id === head.app_id && materialized.generation === head.generation
      && materialized.audience_hash === head.audience_hash), 'apps_registry_corrupt', 500);
  }
  for (const key of Object.keys(totals)) assertApps(totals[key] === usage[0][key], 'apps_registry_corrupt', 500);
  for (const conversation of db.prepare('SELECT * FROM app_discussion_conversations').iterate()) {
    identifier(conversation.id, 'conv'); audience(conversation); safe(conversation.generation, 1); safe(conversation.created_at);
    const head = db.prepare('SELECT * FROM app_discussion_heads WHERE app_id=?').get(conversation.app_id);
    assertApps(head && conversation.generation <= head.generation
      && (conversation.generation < head.generation || conversation.id === head.current_id), 'apps_registry_corrupt', 500);
    const messages = db.prepare('SELECT count(*) AS n,max(seq) AS maximum,sum(removed_at IS NOT NULL) AS removed FROM app_discussion_messages WHERE conversation_id=?').get(conversation.id);
    assertApps(messages.n > 0 && conversation.message_seq === messages.n && messages.maximum === messages.n
      && conversation.change_seq === messages.n + messages.removed, 'apps_registry_corrupt', 500);
    const changes = db.prepare('SELECT count(*) AS n,min(seq) AS minimum,max(seq) AS maximum FROM app_discussion_changes WHERE conversation_id=?').get(conversation.id);
    assertApps(changes.n > 0 && changes.maximum === conversation.change_seq && changes.maximum - changes.minimum + 1 === changes.n, 'apps_registry_corrupt', 500);
  }
  for (const message of db.prepare('SELECT * FROM app_discussion_messages').iterate()) {
    identifier(message.id, 'msg'); textId(message.author_account_id); hashValue(message.request_key); hashValue(message.intent_hash);
    safe(message.seq, 1); safe(message.created_at); safe(message.body_bytes);
    assertApps(typeof message.author_label === 'string' && message.author_label.trim() === message.author_label
      && message.author_label.length >= 1 && message.author_label.length <= 80 && message.author_label.isWellFormed()
      && !/[\u0000-\u001f\u007f]/u.test(message.author_label), 'apps_registry_corrupt', 500);
    if (message.removed_at === null) assertApps(typeof message.body === 'string' && message.body.trim().length > 0
      && message.body.length <= 4000 && message.body.isWellFormed() && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(message.body)
      && Buffer.byteLength(message.body, 'utf8') === message.body_bytes, 'apps_registry_corrupt', 500);
    else { safe(message.removed_at); textId(message.removed_by); assertApps(message.body === null && message.body_bytes === 0, 'apps_registry_corrupt', 500); }
    if (message.reply_to_id !== null) identifier(message.reply_to_id, 'msg');
  }
  assertApps(!db.prepare(`SELECT 1 FROM app_discussion_changes c JOIN app_discussion_messages m ON m.id=c.message_id
    WHERE (c.kind='removed' AND (m.removed_at IS NULL OR c.created_at<>m.removed_at)) OR (c.kind='message' AND c.created_at<>m.created_at) LIMIT 1`).get(), 'apps_registry_corrupt', 500);
  for (const rate of db.prepare('SELECT * FROM app_discussion_rates').iterate()) { textId(rate.account_id); safe(rate.at); safe(rate.credit); }
}

function validateSavedRows(db) {
  assertApps(!db.prepare('PRAGMA foreign_key_check').get(), 'apps_registry_corrupt', 500);
  assertApps(!db.prepare(`SELECT 1 FROM app_saved_entries e JOIN app_saved_heads h ON h.account_id=e.account_id
    JOIN app_domains d ON d.id=e.domain_id WHERE e.saved_revision>h.revision OR d.app_id<>e.app_id OR d.origin<>e.origin LIMIT 1`).get(), 'apps_registry_corrupt', 500);
  assertApps(!db.prepare(`SELECT 1 FROM app_saved_receipts r JOIN app_saved_heads h ON h.account_id=r.account_id
    WHERE r.committed_revision>h.revision LIMIT 1`).get(), 'apps_registry_corrupt', 500);
  assertApps(!db.prepare('SELECT 1 FROM app_saved_entries GROUP BY account_id HAVING count(*)>200 LIMIT 1').get()
    && !db.prepare('SELECT 1 FROM app_saved_receipts GROUP BY account_id HAVING count(*)>128 LIMIT 1').get(), 'apps_registry_corrupt', 500);
  for (const row of db.prepare('SELECT account_id,revision FROM app_saved_heads').iterate()) {
    textId(row.account_id); assertApps(Number.isSafeInteger(row.revision) && row.revision >= 1, 'apps_registry_corrupt', 500);
  }
  for (const row of db.prepare('SELECT * FROM app_saved_entries').iterate()) {
    appId(row.app_id); createLaunchPath(row.path);
    assertApps(typeof row.domain_id === 'string' && /^dom_[a-f0-9]{32}$/u.test(row.domain_id)
      && typeof row.label === 'string' && row.label.trim() === row.label && row.label.length > 0 && row.label.length <= 64
      && !/[\u0000-\u001f\u007f]/u.test(row.label) && Number.isSafeInteger(row.saved_revision) && row.saved_revision >= 1
      && Number.isSafeInteger(row.updated_at) && row.updated_at >= 0, 'apps_registry_corrupt', 500);
  }
  for (const row of db.prepare('SELECT * FROM app_saved_receipts').iterate()) {
    appId(row.app_id);
    assertApps(typeof row.request_key === 'string' && /^[a-f0-9]{64}$/u.test(row.request_key)
      && typeof row.intent_hash === 'string' && /^[a-f0-9]{64}$/u.test(row.intent_hash)
      && [0, 1].includes(row.saved) && Number.isSafeInteger(row.committed_revision) && row.committed_revision >= 1
      && Number.isSafeInteger(row.created_at) && row.created_at >= 0, 'apps_registry_corrupt', 500);
  }
}

export function runtimeTargetDigest(value) {
  return digest(JSON.stringify(['soty.runtime-target.v1', value.appId, value.revision, value.ownerAccountId,
    value.connectorKey, value.port, value.entryPath, value.profile]));
}

export function requiredBindingVersion(db, id) {
  const head = db.prepare('SELECT required_binding_version FROM app_source_heads WHERE app_id=?').get(id);
  assertApps(head && [1, 2].includes(head.required_binding_version), 'apps_registry_corrupt', 500);
  assertApps(head.required_binding_version === 2 || !db.prepare('SELECT 1 FROM app_runtime_targets WHERE app_id=? AND revision>1 LIMIT 1').get(id), 'apps_registry_corrupt', 500);
  return head.required_binding_version;
}

// Shared by initial migration and registration. Existing v4 records are only
// validated; missing state must never be silently repaired with a weaker floor.
export function ensureInitialPublication(db, app) {
  assertApps(db.isTransaction === true, 'apps_transaction_required', 500);
  const port = appPort(app.port), entryPath = requestPath(app.entry_path);
  assertApps(!entryPath.startsWith('/_soty/'), 'apps_registry_corrupt', 500);
  const device = db.prepare('SELECT owner_account_id FROM app_devices WHERE connector_key=?').get(app.connector_key);
  assertApps(device?.owner_account_id === app.owner_account_id, 'apps_registry_corrupt', 500);
  const target = { appId: app.id, revision: 1, ownerAccountId: app.owner_account_id, connectorKey: app.connector_key,
    port, entryPath, profile: RUNTIME_PROFILE };
  const targetDigest = runtimeTargetDigest(target);
  const existing = db.prepare('SELECT digest FROM app_runtime_targets WHERE app_id=? AND revision=1').get(app.id);
  const policy = db.prepare('SELECT app_id FROM app_publications WHERE app_id=?').get(app.id);
  const head = db.prepare('SELECT app_id FROM app_source_heads WHERE app_id=?').get(app.id);
  const anyTarget = existing || db.prepare('SELECT 1 FROM app_runtime_targets WHERE app_id=? LIMIT 1').get(app.id);
  if (anyTarget || policy || head) {
    assertApps(existing && policy && head, 'apps_registry_corrupt', 500);
    assertApps(existing.digest === targetDigest, 'apps_initial_target_changed', 409);
    requiredBindingVersion(db, app.id); return;
  }
  db.prepare('INSERT INTO app_runtime_targets VALUES (?,?,?,?,?,?,?,?,?)')
    .run(app.id, 1, app.owner_account_id, app.connector_key, port, entryPath, RUNTIME_PROFILE, targetDigest, app.created_at);
  db.prepare("INSERT INTO app_publications VALUES (?,?,'restricted',0,1,1,NULL,NULL,?)")
    .run(app.id, app.owner_account_id, app.updated_at);
  db.prepare('INSERT INTO app_source_heads VALUES (?,1)').run(app.id);
}

function validateSourceRows(db, { historical = false } = {}) {
  const marker = db.prepare("SELECT value FROM apps_meta WHERE key='schema'").get()?.value;
  const resources = marker === APPS_RESOURCE_REGISTRY_SCHEMA;
  const scoped = resources || marker === APPS_SCOPED_REGISTRY_SCHEMA;
  assertApps(!db.prepare('PRAGMA foreign_key_check').get(), 'apps_registry_corrupt', 500);
  for (const app of db.prepare('SELECT * FROM local_apps').iterate()) {
    const policy = db.prepare('SELECT * FROM app_publications WHERE app_id=?').get(app.id);
    assertApps(policy?.owner_account_id === app.owner_account_id, 'apps_registry_corrupt', 500);
    let initial = false, active = false, count = 0;
    for (const target of db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=?').iterate(app.id)) {
      count++;
      assertApps(target.owner_account_id === app.owner_account_id && Number.isSafeInteger(target.revision) && target.revision >= 1
        && (target.profile === RUNTIME_PROFILE || scoped && target.profile === SCOPED_RUNTIME_PROFILE || resources && target.profile === RESOURCE_RUNTIME_PROFILE) && target.digest === runtimeTargetDigest({ appId: app.id, revision: target.revision,
          ownerAccountId: app.owner_account_id, connectorKey: target.connector_key, port: target.port, entryPath: target.entry_path, profile: target.profile }), 'apps_registry_corrupt', 500);
      appPort(target.port); requestPath(target.entry_path);
      if ([SCOPED_RUNTIME_PROFILE, RESOURCE_RUNTIME_PROFILE].includes(target.profile)) {
        const admission = db.prepare('SELECT * FROM app_scoped_embed_admissions WHERE app_id=? AND target_revision=?').get(app.id, target.revision);
        assertApps(admission && admission.target_digest === target.digest, 'apps_scoped_admission_corrupt', 500);
        let profile;
        try { profile = approvedEmbedProfile(JSON.parse(admission.approved_pin_json)); } catch { throw new AppsError('apps_scoped_admission_corrupt', 500); }
        const { digest: derived, ...pin } = profile;
        assertApps(profile.schema === target.profile && derived === admission.profile_digest && canonical(pin) === admission.approved_pin_json
          && profile.appId === app.id && profile.resource.tenantId === app.owner_account_id
          && profile.target.revision === target.revision && profile.target.digest === target.digest
          && [profile.connector.linkId,profile.connector.hostDeviceId,profile.connector.connectorId].join('|') === target.connector_key
          && target.entry_path === '/embed', 'apps_scoped_admission_corrupt', 500);
        if (target.revision === policy.active_target_revision) assertApps(policy.launch_policy === 'restricted', 'apps_scoped_public_forbidden', 500);
      }
      if (target.revision === 1) {
        initial = true;
        assertApps(target.connector_key === app.connector_key && target.port === app.port && target.entry_path === app.entry_path, 'apps_initial_target_changed', 409);
      }
      if (target.revision === policy.active_target_revision) active = true;
    }
    assertApps(initial && active, 'apps_registry_corrupt', 500);
    if (historical) assertApps(count === 1 && policy.active_target_revision === 1, 'apps_source_history_unsupported', 409);
    else requiredBindingVersion(db, app.id);
  }
  if (scoped) assertApps(db.prepare('SELECT count(*) AS n FROM app_scoped_embed_admissions').get().n
    === db.prepare('SELECT count(*) AS n FROM app_runtime_targets WHERE profile IN (?,?)').get(SCOPED_RUNTIME_PROFILE, RESOURCE_RUNTIME_PROFILE).n, 'apps_scoped_admission_corrupt', 500);
}

function initializePublications(db) {
  assertApps(!db.prepare('PRAGMA foreign_key_check').get(), 'apps_registry_corrupt', 500);
  assertApps(!db.prepare(`SELECT 1 FROM app_domains d JOIN local_apps a ON a.id=d.app_id
    WHERE d.owner_account_id<>a.owner_account_id LIMIT 1`).get(), 'apps_registry_corrupt', 500);
  for (const app of db.prepare('SELECT * FROM local_apps').iterate()) {
    try {
      appId(app.id); textId(app.owner_account_id); cleanGrants(JSON.parse(app.grants_json));
      assertApps(['enabled', 'revoked'].includes(app.state) && Number.isSafeInteger(app.revision) && app.revision >= 1, 'apps_registry_corrupt', 500);
      assertApps(db.prepare('SELECT 1 FROM app_domain_heads WHERE app_id=?').get(app.id), 'apps_registry_corrupt', 500);
      ensureInitialPublication(db, app);
    } catch { throw new AppsError('apps_registry_corrupt', 500); }
  }
}

export function insertDomainZone(db, zone, timestamp) {
  const id = domainZoneId(zone);
  db.prepare('INSERT OR IGNORE INTO app_domain_zones VALUES (?,?,?,?,?,?,?)')
    .run(id, zone.kind, zone.template, zone.suffix, zone.scheme, zone.port, timestamp);
  return id;
}

// Called only inside the caller's app registration / migration transaction.
export function ensureCanonicalDomain(db, { id, owner_account_id: ownerAccountId, created_at: createdAt }, legacyTemplate) {
  assertApps(db.isTransaction === true, 'apps_transaction_required', 500);
  db.prepare('INSERT OR IGNORE INTO app_domain_heads VALUES (?,0)').run(id);
  if (!legacyTemplate) return;
  const zoneId = insertDomainZone(db, legacyZone(legacyTemplate), createdAt);
  const origin = canonicalOrigin(legacyTemplate, id);
  db.prepare("INSERT OR IGNORE INTO app_domains VALUES (?,?,?,?,NULL,?,?,'canonical','bound',?,NULL)")
    .run(`dom_${digest(`canonical:${id}`).slice(0, 32)}`, zoneId, new URL(origin).hostname, origin, id, ownerAccountId, createdAt);
  const recorded = db.prepare("SELECT origin,owner_account_id FROM app_domains WHERE app_id=? AND role='canonical'").get(id);
  assertApps(recorded?.origin === origin && recorded.owner_account_id === ownerAccountId, 'apps_canonical_origin_conflict', 409);
}

function validateAndRebuildGrants(db, visit) {
  assertApps(!db.prepare('PRAGMA foreign_key_check').get(), 'apps_registry_corrupt', 500);
  // The migration is atomic but does not materialize every app in process memory.
  db.exec('DELETE FROM local_app_grants');
  const insert = db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)');
  for (const row of db.prepare('SELECT * FROM local_apps').iterate()) {
    let grants;
    try {
      appId(row.id); textId(row.owner_account_id);
      assertApps(['enabled', 'revoked'].includes(row.state) && Number.isSafeInteger(row.revision) && row.revision >= 1, 'apps_registry_corrupt');
      grants = cleanGrants(JSON.parse(row.grants_json));
    } catch { throw new AppsError('apps_registry_corrupt', 500); }
    // grants_json remains the v1 authority; stale optimization rows never broaden access.
    for (const id of grants.accountIds) insert.run(row.id, 'account', id);
    for (const id of grants.communityIds) insert.run(row.id, 'community', id);
    visit(row);
  }
}

function migrateLegacyAppsSchema(db, { legacyTemplate = '', now = Date.now } = {}) {
  const normalizedTemplate = normalizeLegacyTemplate(legacyTemplate);
  // Inspect before any DDL or persistent PRAGMA. Unknown/future schemas are untouched.
  inspectAppsSchema(db);
  db.exec('BEGIN IMMEDIATE');
  try {
    const before = inspectAppsSchema(db); // another process may have migrated while this connection waited
    if (['v2', 'v3', 'v4', 'v5', 'v6'].includes(before)) {
      const pinned = db.prepare("SELECT value FROM apps_meta WHERE key='legacy_origin_template'").get();
      assertApps(pinned && pinned.value === normalizedTemplate, 'apps_origin_template_changed', 409);
      if (before === 'v6') {
        validateSourceRows(db);
        validateSavedRows(db);
        validateDiscussionRows(db);
        db.exec('COMMIT');
        return { schema: APPS_REGISTRY_SCHEMA, migrated: false, legacyTemplate: pinned.value };
      }
    }
    if (before === 'v3') validateSourceRows(db, { historical: true });
    if (['v4', 'v5'].includes(before)) validateSourceRows(db);
    if (before === 'v5') validateSavedRows(db);
    if (before === 'empty') db.exec(coreDdl());
    if (before === 'empty' || before === 'v1') {
      db.exec(domainDdl());
      const timestamp = now();
      if (normalizedTemplate) insertDomainZone(db, legacyZone(normalizedTemplate), timestamp);
      validateAndRebuildGrants(db, app => ensureCanonicalDomain(db, app, normalizedTemplate));
    }
    if (!['v3', 'v4', 'v5'].includes(before)) {
      db.exec(publicationDdl());
      for (const statement of targetGuards()) db.exec(statement);
    }
    if (!['v4', 'v5'].includes(before)) {
      db.exec(sourceDdl());
      for (const statement of sourceGuards()) db.exec(statement);
      if (before === 'v3') db.exec('INSERT INTO app_source_heads SELECT id,1 FROM local_apps');
      else initializePublications(db);
    }
    if (before !== 'v5') {
      db.exec(savedDdl());
      for (const statement of savedGuards()) db.exec(statement);
    }
    db.exec(discussionDdl());
    db.exec('INSERT INTO app_discussion_usage VALUES (1,0,0,0,0)');
    for (const statement of discussionGuards()) db.exec(statement);
    db.prepare("INSERT OR REPLACE INTO apps_meta(key,value) VALUES ('schema',?),('legacy_origin_template',?)")
      .run(APPS_REGISTRY_SCHEMA, normalizedTemplate);
    db.exec('PRAGMA user_version=6; COMMIT');
    return { schema: APPS_REGISTRY_SCHEMA, migrated: true, legacyTemplate: normalizedTemplate };
  } catch (error) {
    if (db.isTransaction) db.exec('ROLLBACK');
    throw error;
  }
}

/** Explicit startup capability migration. Old tuples/receipts are copied
 * byte-for-byte; the expanded CHECK requires a known-DDL table rebuild. No
 * request handler calls this function or changes a persisted reader epoch. */
function migrateLegacyAndScopedAppsSchema(db, { legacyTemplate = '', now = Date.now, allowScopedEmbedMigration = false } = {}) {
  assertApps(typeof allowScopedEmbedMigration === 'boolean', 'apps_scoped_migration_configuration_invalid', 503);
  const before = inspectAppsSchema(db);
  if (before === 'v7') {
    const pinned = db.prepare("SELECT value FROM apps_meta WHERE key='legacy_origin_template'").get();
    assertApps(pinned?.value === normalizeLegacyTemplate(legacyTemplate), 'apps_origin_template_changed', 409);
    validateSourceRows(db); validateSavedRows(db); validateDiscussionRows(db);
    return { schema: APPS_SCOPED_REGISTRY_SCHEMA, migrated: false, legacyTemplate: pinned.value };
  }
  const legacy = migrateLegacyAppsSchema(db, { legacyTemplate, now });
  if (!allowScopedEmbedMigration) return legacy;
  assertApps(!db.isTransaction && inspectAppsSchema(db) === 'v6', 'apps_scoped_migration_required', 503);
  const foreignKeys = Number(db.prepare('PRAGMA foreign_keys').get().foreign_keys);
  db.exec('PRAGMA foreign_keys=OFF;');
  try {
    db.exec('BEGIN IMMEDIATE');
    const state = inspectAppsSchema(db);
    if (state === 'v7') { db.exec('COMMIT'); return { schema: APPS_SCOPED_REGISTRY_SCHEMA, migrated: false, legacyTemplate: legacy.legacyTemplate }; }
    assertApps(state === 'v6', 'apps_schema_unsupported');
    validateSourceRows(db); validateSavedRows(db); validateDiscussionRows(db);
    const ddl = publicationDdl(true).split(';').find(value => value.trim().startsWith('CREATE TABLE app_runtime_targets'));
    assertApps(ddl, 'apps_schema_definition_invalid', 500);
    db.exec(ddl.replace('CREATE TABLE app_runtime_targets', 'CREATE TABLE scoped_runtime_targets_migration'));
    db.exec('INSERT INTO scoped_runtime_targets_migration SELECT * FROM app_runtime_targets');
    db.exec('DROP TRIGGER app_runtime_target_no_update;DROP TRIGGER app_runtime_target_no_delete;DROP TRIGGER app_runtime_target_no_replace;');
    db.exec('DROP TABLE app_runtime_targets;');
    db.exec(ddl);
    db.exec('INSERT INTO app_runtime_targets SELECT * FROM scoped_runtime_targets_migration;DROP TABLE scoped_runtime_targets_migration;');
    for (const statement of targetGuards()) db.exec(statement);
    for (const statement of sourceGuards().filter(value => value.includes('CREATE TRIGGER app_runtime_target_no_replace'))) db.exec(statement);
    db.exec(scopedAdmissionDdl()); for (const statement of scopedAdmissionGuards()) db.exec(statement);
    db.prepare("UPDATE apps_meta SET value=? WHERE key='schema'").run(APPS_SCOPED_REGISTRY_SCHEMA);
    db.exec('PRAGMA user_version=7;');
    assertApps(db.prepare('PRAGMA foreign_key_check').all().length === 0 && inspectAppsSchema(db) === 'v7', 'apps_scoped_migration_invalid', 500);
    validateSourceRows(db); validateSavedRows(db); validateDiscussionRows(db);
    db.exec('COMMIT');
    return { schema: APPS_SCOPED_REGISTRY_SCHEMA, migrated: true, legacyTemplate: legacy.legacyTemplate };
  } catch (error) {
    if (db.isTransaction) db.exec('ROLLBACK');
    throw error;
  } finally { db.exec(`PRAGMA foreign_keys=${foreignKeys};`); }
}

/** Explicit startup-only Apps8 upgrade. The generic resource shape does not
 * add a format epoch for each subsequent reviewed Source route adapter. */
export function migrateAppsSchema(db, options = {}) {
  const { legacyTemplate = '', now = Date.now, allowScopedEmbedMigration = false, allowSelectedResourceMigration = false } = options;
  assertApps(typeof allowScopedEmbedMigration === 'boolean' && typeof allowSelectedResourceMigration === 'boolean',
    'apps_scoped_migration_configuration_invalid', 503);
  const initial = inspectAppsSchema(db);
  if (initial === 'v8') {
    const pinned = db.prepare("SELECT value FROM apps_meta WHERE key='legacy_origin_template'").get();
    assertApps(pinned?.value === normalizeLegacyTemplate(legacyTemplate), 'apps_origin_template_changed', 409);
    validateSourceRows(db); validateSavedRows(db); validateDiscussionRows(db);
    return { schema: APPS_RESOURCE_REGISTRY_SCHEMA, migrated: false, legacyTemplate: pinned.value };
  }
  const previous = migrateLegacyAndScopedAppsSchema(db, { legacyTemplate, now,
    allowScopedEmbedMigration: allowScopedEmbedMigration || allowSelectedResourceMigration });
  if (!allowSelectedResourceMigration) return previous;
  assertApps(!db.isTransaction && inspectAppsSchema(db) === 'v7', 'apps_resource_migration_required', 503);
  const foreignKeys = Number(db.prepare('PRAGMA foreign_keys').get().foreign_keys);
  db.exec('PRAGMA foreign_keys=OFF;');
  try {
    db.exec('BEGIN IMMEDIATE');
    const state = inspectAppsSchema(db);
    if (state === 'v8') { db.exec('COMMIT'); return { schema: APPS_RESOURCE_REGISTRY_SCHEMA, migrated: false, legacyTemplate: previous.legacyTemplate }; }
    assertApps(state === 'v7', 'apps_schema_unsupported');
    validateSourceRows(db); validateSavedRows(db); validateDiscussionRows(db);
    const ddl = publicationDdl(true, true).split(';').find(value => value.trim().startsWith('CREATE TABLE app_runtime_targets'));
    assertApps(ddl, 'apps_schema_definition_invalid', 500);
    db.exec(ddl.replace('CREATE TABLE app_runtime_targets', 'CREATE TABLE resource_runtime_targets_migration'));
    db.exec('INSERT INTO resource_runtime_targets_migration SELECT * FROM app_runtime_targets');
    db.exec('DROP TRIGGER app_runtime_target_no_update;DROP TRIGGER app_runtime_target_no_delete;DROP TRIGGER app_runtime_target_no_replace;DROP TABLE app_runtime_targets;');
    db.exec(ddl);
    db.exec('INSERT INTO app_runtime_targets SELECT * FROM resource_runtime_targets_migration;DROP TABLE resource_runtime_targets_migration;');
    for (const statement of targetGuards()) db.exec(statement);
    for (const statement of sourceGuards().filter(value => value.includes('CREATE TRIGGER app_runtime_target_no_replace'))) db.exec(statement);
    db.prepare("UPDATE apps_meta SET value=? WHERE key='schema'").run(APPS_RESOURCE_REGISTRY_SCHEMA);
    db.exec('PRAGMA user_version=8;');
    assertApps(db.prepare('PRAGMA foreign_key_check').all().length === 0 && inspectAppsSchema(db) === 'v8', 'apps_resource_migration_invalid', 500);
    validateSourceRows(db); validateSavedRows(db); validateDiscussionRows(db); db.exec('COMMIT');
    return { schema: APPS_RESOURCE_REGISTRY_SCHEMA, migrated: true, legacyTemplate: previous.legacyTemplate };
  } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  finally { db.exec(`PRAGMA foreign_keys=${foreignKeys};`); }
}
