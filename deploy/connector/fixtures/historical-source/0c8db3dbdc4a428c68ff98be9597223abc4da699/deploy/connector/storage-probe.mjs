// This trusted host source is passed to a pinned helper's Node executable.
// Do not import application code: the candidate must not attest its own reader.
import { lstat, opendir } from 'node:fs/promises';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const fail = code => { throw Object.assign(new Error(code), { code }); };
const missing = async file => { try { return await lstat(file); } catch (e) { if (e.code === 'ENOENT') return null; throw e; } };
const roomTablesV2 = ['room_state', 'room_files', 'room_receipts', 'room_events', 'room_imports', 'room_deletions'];
const serviceFiles = new Set(['connector-store.json', 'connector-maintenance.json', 'connector-rollback.json', 'traffic-control.json', 'traffic-exit-pool.json']);
const appCore = {
  apps_meta: 'key,value',
  app_devices: 'connector_key,owner_account_id,identity_json,name,created_at',
  local_apps: 'id,owner_account_id,connector_key,name,port,entry_path,grants_json,state,revision,created_at,updated_at',
  local_app_grants: 'app_id,kind,principal_id',
};
const appDomains = {
  app_domain_zones: 'id,kind,origin_template,suffix,scheme,port,created_at',
  app_domain_heads: 'app_id,revision',
  app_domains: 'id,zone_id,hostname,origin,slug,app_id,owner_account_id,role,state,created_at,retired_at',
  app_domain_receipts: 'account_id,request_key,intent_hash,action,domain_id,committed_revision,created_at',
};
const appPublications = {
  app_runtime_targets: 'app_id,revision,owner_account_id,connector_key,port,entry_path,profile,digest,created_at',
  app_publications: 'app_id,owner_account_id,launch_policy,listed,policy_epoch,active_target_revision,exposure_ack_revision,exposure_ack_json,updated_at',
  app_publication_domains: 'app_id,domain_id,owner_account_id',
  app_publication_receipts: 'account_id,request_key,intent_hash,app_id,committed_epoch,value_json,created_at',
};
const appSources = {
  app_source_heads: 'app_id,required_binding_version',
  app_source_receipts: 'account_id,request_key,intent_hash,app_id,committed_epoch,value_json,created_at',
};
const appSaved = {
  app_saved_heads: 'account_id,revision',
  app_saved_entries: 'account_id,app_id,domain_id,origin,path,label,saved_revision,updated_at',
  app_saved_receipts: 'account_id,request_key,intent_hash,app_id,saved,committed_revision,created_at',
};
const appDiscussions = {
  app_discussion_heads: 'app_id,current_id,generation,owner_account_id,mode,grants_json,audience_hash,message_count,body_bytes,conversation_count,rate_at,rate_credit,updated_at',
  app_discussion_conversations: 'id,app_id,generation,owner_account_id,mode,grants_json,audience_hash,created_at,message_seq,change_seq',
  app_discussion_messages: 'id,app_id,conversation_id,seq,author_account_id,author_label,request_key,intent_hash,body,body_bytes,reply_to_id,created_at,removed_at,removed_by',
  app_discussion_changes: 'conversation_id,seq,message_id,kind,created_at',
  app_discussion_usage: 'id,head_count,conversation_count,message_count,body_bytes',
  app_discussion_rates: 'account_id,at,credit',
};
// Frozen host-side recognition of the two v3 immutable-target guards. Matching
// names alone would also admit a replaced trigger with different behavior.
const appTargetGuards = {
  app_runtime_target_no_update: { table: 'app_runtime_targets', sql: "CREATE TRIGGER app_runtime_target_no_update BEFORE UPDATE ON app_runtime_targets BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END" },
  app_runtime_target_no_delete: { table: 'app_runtime_targets', sql: "CREATE TRIGGER app_runtime_target_no_delete BEFORE DELETE ON app_runtime_targets BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END" },
};
// The v4 floor may only increase. These definitions are frozen independently of
// the application migration and do not attest that a runtime speaks binding v2.
const appSourceGuards = {
  app_source_head_no_downgrade: { table: 'app_source_heads', sql: "CREATE TRIGGER app_source_head_no_downgrade BEFORE UPDATE ON app_source_heads WHEN NEW.app_id<>OLD.app_id OR NEW.required_binding_version<OLD.required_binding_version BEGIN SELECT RAISE(ABORT,'app_source_binding_downgrade'); END" },
  app_source_head_no_delete: { table: 'app_source_heads', sql: "CREATE TRIGGER app_source_head_no_delete BEFORE DELETE ON app_source_heads BEGIN SELECT RAISE(ABORT,'app_source_head_required'); END" },
  app_source_head_no_replace_downgrade: { table: 'app_source_heads', sql: "CREATE TRIGGER app_source_head_no_replace_downgrade BEFORE INSERT ON app_source_heads WHEN EXISTS (SELECT 1 FROM app_source_heads WHERE app_id=NEW.app_id AND required_binding_version>NEW.required_binding_version) BEGIN SELECT RAISE(ABORT,'app_source_binding_downgrade'); END" },
  app_runtime_target_no_replace: { table: 'app_runtime_targets', sql: "CREATE TRIGGER app_runtime_target_no_replace BEFORE INSERT ON app_runtime_targets WHEN EXISTS (SELECT 1 FROM app_runtime_targets WHERE app_id=NEW.app_id AND revision=NEW.revision) BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END" },
};
// Apps5 is saved-only. Its monotonic account revision remains after removal;
// missing or replaced guards must not be mistaken for this accepted format.
const appSavedGuards = {
  app_saved_head_no_downgrade: { table: 'app_saved_heads', sql: "CREATE TRIGGER app_saved_head_no_downgrade BEFORE UPDATE ON app_saved_heads WHEN NEW.account_id<>OLD.account_id OR NEW.revision<=OLD.revision BEGIN SELECT RAISE(ABORT,'app_saved_revision_not_increasing'); END" },
  app_saved_head_no_delete: { table: 'app_saved_heads', sql: "CREATE TRIGGER app_saved_head_no_delete BEFORE DELETE ON app_saved_heads BEGIN SELECT RAISE(ABORT,'app_saved_head_required'); END" },
  app_saved_head_no_replace: { table: 'app_saved_heads', sql: "CREATE TRIGGER app_saved_head_no_replace BEFORE INSERT ON app_saved_heads WHEN EXISTS (SELECT 1 FROM app_saved_heads WHERE account_id=NEW.account_id) BEGIN SELECT RAISE(ABORT,'app_saved_head_immutable'); END" },
};
// Apps6 has independent discussion audiences and retained message tombstones.
// Literal guard recognition is format compatibility, not an ACL or row audit.
const appDiscussionGuards = {
  app_discussion_head_no_delete: { table: 'app_discussion_heads', sql: "CREATE TRIGGER app_discussion_head_no_delete BEFORE DELETE ON app_discussion_heads BEGIN SELECT RAISE(ABORT,'app_discussion_head_required'); END" },
  app_discussion_head_no_replace: { table: 'app_discussion_heads', sql: "CREATE TRIGGER app_discussion_head_no_replace BEFORE INSERT ON app_discussion_heads WHEN EXISTS (SELECT 1 FROM app_discussion_heads WHERE app_id=NEW.app_id) BEGIN SELECT RAISE(ABORT,'app_discussion_head_required'); END" },
  app_discussion_head_lineage: { table: 'app_discussion_heads', sql: "CREATE TRIGGER app_discussion_head_lineage BEFORE UPDATE ON app_discussion_heads WHEN NEW.app_id<>OLD.app_id OR NEW.owner_account_id<>OLD.owner_account_id OR NEW.generation<OLD.generation OR (NEW.generation=OLD.generation AND (NEW.current_id<>OLD.current_id OR NEW.mode<>OLD.mode OR NEW.grants_json<>OLD.grants_json OR NEW.audience_hash<>OLD.audience_hash)) OR (NEW.generation>OLD.generation AND (NEW.generation<>OLD.generation+1 OR NEW.current_id=OLD.current_id OR EXISTS (SELECT 1 FROM app_discussion_conversations WHERE id=NEW.current_id))) BEGIN SELECT RAISE(ABORT,'app_discussion_lineage_immutable'); END" },
  app_discussion_conversation_immutable: { table: 'app_discussion_conversations', sql: "CREATE TRIGGER app_discussion_conversation_immutable BEFORE UPDATE OF id,app_id,generation,owner_account_id,mode,grants_json,audience_hash,created_at ON app_discussion_conversations BEGIN SELECT RAISE(ABORT,'app_discussion_audience_immutable'); END" },
  app_discussion_conversation_no_delete: { table: 'app_discussion_conversations', sql: "CREATE TRIGGER app_discussion_conversation_no_delete BEFORE DELETE ON app_discussion_conversations BEGIN SELECT RAISE(ABORT,'app_discussion_audience_immutable'); END" },
  app_discussion_conversation_no_replace: { table: 'app_discussion_conversations', sql: "CREATE TRIGGER app_discussion_conversation_no_replace BEFORE INSERT ON app_discussion_conversations WHEN EXISTS (SELECT 1 FROM app_discussion_conversations WHERE id=NEW.id OR (app_id=NEW.app_id AND generation=NEW.generation)) BEGIN SELECT RAISE(ABORT,'app_discussion_audience_immutable'); END" },
  app_discussion_message_immutable: { table: 'app_discussion_messages', sql: "CREATE TRIGGER app_discussion_message_immutable BEFORE UPDATE OF id,app_id,conversation_id,seq,author_account_id,author_label,request_key,intent_hash,reply_to_id,created_at ON app_discussion_messages BEGIN SELECT RAISE(ABORT,'app_discussion_message_immutable'); END" },
  app_discussion_message_no_delete: { table: 'app_discussion_messages', sql: "CREATE TRIGGER app_discussion_message_no_delete BEFORE DELETE ON app_discussion_messages BEGIN SELECT RAISE(ABORT,'app_discussion_message_immutable'); END" },
  app_discussion_message_no_replace: { table: 'app_discussion_messages', sql: "CREATE TRIGGER app_discussion_message_no_replace BEFORE INSERT ON app_discussion_messages WHEN EXISTS (SELECT 1 FROM app_discussion_messages WHERE id=NEW.id OR (author_account_id=NEW.author_account_id AND request_key=NEW.request_key) OR (conversation_id=NEW.conversation_id AND seq=NEW.seq)) BEGIN SELECT RAISE(ABORT,'app_discussion_message_immutable'); END" },
  app_discussion_message_redaction: { table: 'app_discussion_messages', sql: "CREATE TRIGGER app_discussion_message_redaction BEFORE UPDATE OF body,body_bytes,removed_at,removed_by ON app_discussion_messages WHEN OLD.removed_at IS NOT NULL OR NEW.body IS NOT NULL OR NEW.body_bytes<>0 OR NEW.removed_at IS NULL OR NEW.removed_by IS NULL BEGIN SELECT RAISE(ABORT,'app_discussion_redaction_required'); END" },
  app_discussion_change_immutable: { table: 'app_discussion_changes', sql: "CREATE TRIGGER app_discussion_change_immutable BEFORE UPDATE ON app_discussion_changes BEGIN SELECT RAISE(ABORT,'app_discussion_change_immutable'); END" },
  app_discussion_usage_no_delete: { table: 'app_discussion_usage', sql: "CREATE TRIGGER app_discussion_usage_no_delete BEFORE DELETE ON app_discussion_usage BEGIN SELECT RAISE(ABORT,'app_discussion_usage_required'); END" },
  app_discussion_usage_no_replace: { table: 'app_discussion_usage', sql: "CREATE TRIGGER app_discussion_usage_no_replace BEFORE INSERT ON app_discussion_usage WHEN EXISTS (SELECT 1 FROM app_discussion_usage WHERE id=NEW.id) BEGIN SELECT RAISE(ABORT,'app_discussion_usage_required'); END" },
};
const normalizedSql = sql => typeof sql === 'string' ? sql.split(/('(?:[^']|'')*')/gu)
  .map((part, index) => index % 2 ? part : part.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('') : null;

// Historical v1 layouts, frozen independently of Notes/Capabilities code.
// Native v2 additions below do not redefine or migrate these historical tables.
const notesTables = {
  notes_meta: 'key,value',
  note_accounts: 'account_id,bytes,identities,active,archived,trashed',
  notes: 'rowid,account_id,id,title,body,items,preview,color,pinned,state,revision,bytes,created_at,updated_at',
  note_receipts: 'account_id,note_id,mutation_id,digest,result,revision',
  notes_fts: 'scope,title,body,items,notes_fts,rank',
  notes_fts_data: 'id,block',
  notes_fts_idx: 'segid,term,pgno',
  notes_fts_content: 'id,c0,c1,c2,c3',
  notes_fts_docsize: 'id,sz',
  notes_fts_config: 'k,v',
};
const notesIndexes = {
  notes_owner_order: { table: 'notes', sql: 'CREATE INDEX notes_owner_order ON notes(account_id,state,pinned DESC,updated_at DESC,id ASC)' },
  note_receipts_trim: { table: 'note_receipts', sql: 'CREATE INDEX note_receipts_trim ON note_receipts(account_id,note_id,revision DESC)' },
};
const notesFts = "CREATE VIRTUAL TABLE notes_fts USING fts5(scope,title,body,items,tokenize='unicode61 remove_diacritics 2',prefix='2 3')";
const capabilitiesTables = {
  cap_metadata: 'key,value',
  cap_contracts: 'capability_id,version,digest',
  cap_clients: 'id,account_id,label,state,policy_epoch,created_at,revoked_at',
  cap_principals: 'id,account_id,client_id,kind,label,state,creator_device_id,created_at,revoked_at',
  cap_grants: 'id,account_id,client_id,principal_id,parent_id,root_id,creator_device_id,capabilities_json,resources_json,effects_json,recipients_json,allow_delegation,max_depth,depth,not_before,expires_at,policy_epoch,created_at,revoked_at',
  cap_credentials: 'id,digest,account_id,client_id,principal_id,grant_id,audience,expires_at,created_at,revoked_at',
  cap_audit: 'id,account_id,kind,object_type,object_id,actor_type,actor_id,created_at',
  cap_budgets: 'root_grant_id,unit,limit_amount,reserved_amount,spent_amount',
  cap_budget_reservations: 'id,invocation_id,attempt_id,root_grant_id,unit,amount,actual_amount,disposition,request_digest,created_at,updated_at',
  cap_invocations: 'id,account_id,client_id,principal_id,grant_id,root_grant_id,policy_epoch,capability_id,capability_version,capability_digest,request_key,request_digest,internal_request_id,input_json,target_json,authorization_json,status,effect_state,cancel_requested,effects_json,reservation_id,job_id,created_at,updated_at,completed_at',
  cap_dispatch_intents: 'invocation_id,internal_request_id,state,created_at,updated_at',
  cap_receipts: 'invocation_id,value_json,digest,created_at',
};
const capabilitiesIndexes = {
  cap_clients_account: { table: 'cap_clients', sql: 'CREATE INDEX cap_clients_account ON cap_clients(account_id,created_at,id)' },
  cap_principals_account: { table: 'cap_principals', sql: 'CREATE INDEX cap_principals_account ON cap_principals(account_id,created_at,id)' },
  cap_grants_account: { table: 'cap_grants', sql: 'CREATE INDEX cap_grants_account ON cap_grants(account_id,created_at,id)' },
  cap_grants_root: { table: 'cap_grants', sql: 'CREATE INDEX cap_grants_root ON cap_grants(root_id,id)' },
  cap_credentials_grant: { table: 'cap_credentials', sql: 'CREATE INDEX cap_credentials_grant ON cap_credentials(grant_id,id)' },
  cap_audit_account: { table: 'cap_audit', sql: 'CREATE INDEX cap_audit_account ON cap_audit(account_id,created_at,id)' },
  cap_invocations_history: { table: 'cap_invocations', sql: 'CREATE INDEX cap_invocations_history ON cap_invocations(account_id,client_id,created_at,id)' },
  cap_dispatch_pending: { table: 'cap_dispatch_intents', sql: 'CREATE INDEX cap_dispatch_pending ON cap_dispatch_intents(state,created_at,invocation_id)' },
};

// Literal additive v2 definitions pinned to B1a 5e459abc6afa376861c2032226bd29f78bf0468d.
// No candidate application code runs in this independent host recognizer.
const notesNativeTables = {
  note_native_creates: "CREATE TABLE note_native_creates(\n  source_store_id TEXT NOT NULL\n    CHECK(length(source_store_id)=32 AND source_store_id NOT GLOB '*[^0-9a-f]*'),\n  invocation_id TEXT NOT NULL\n    CHECK(length(invocation_id) BETWEEN 1 AND 160\n      AND invocation_id NOT GLOB '*[^A-Za-z0-9_.:-]*'),\n  account_id TEXT NOT NULL,\n  note_id TEXT NOT NULL\n    CHECK(length(note_id)=66 AND substr(note_id,1,2)='n_'\n      AND substr(note_id,3) NOT GLOB '*[^0-9a-f]*'),\n  mutation_id TEXT NOT NULL\n    CHECK(length(mutation_id)=66 AND substr(mutation_id,1,2)='m_'\n      AND substr(mutation_id,3) NOT GLOB '*[^0-9a-f]*'),\n  input_digest TEXT NOT NULL\n    CHECK(length(input_digest)=64 AND input_digest NOT GLOB '*[^0-9a-f]*'),\n  capability_digest TEXT NOT NULL\n    CHECK(capability_digest='95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204'),\n  revision INTEGER NOT NULL CHECK(revision=1),\n  created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),\n  PRIMARY KEY(source_store_id,invocation_id),\n  UNIQUE(account_id,note_id),\n  UNIQUE(account_id,mutation_id),\n  FOREIGN KEY(account_id,note_id) REFERENCES notes(account_id,id)\n) STRICT",
};
const notesNativeGuards = {
  note_native_create_no_update: {"table":"note_native_creates","sql":"CREATE TRIGGER note_native_create_no_update\nBEFORE UPDATE ON note_native_creates BEGIN\n  SELECT RAISE(ABORT,'notes_native_proof_immutable');\nEND"},
  note_native_create_no_delete: {"table":"note_native_creates","sql":"CREATE TRIGGER note_native_create_no_delete\nBEFORE DELETE ON note_native_creates BEGIN\n  SELECT RAISE(ABORT,'notes_native_proof_immutable');\nEND"},
  note_native_create_no_replace: {"table":"note_native_creates","sql":"CREATE TRIGGER note_native_create_no_replace\nBEFORE INSERT ON note_native_creates\nWHEN EXISTS(SELECT 1 FROM note_native_creates\n  WHERE (source_store_id=NEW.source_store_id AND invocation_id=NEW.invocation_id)\n     OR (account_id=NEW.account_id AND note_id=NEW.note_id)\n     OR (account_id=NEW.account_id AND mutation_id=NEW.mutation_id))\nBEGIN SELECT RAISE(ABORT,'notes_native_proof_immutable'); END"},
  notes_identity_no_update: {"table":"notes_meta","sql":"CREATE TRIGGER notes_identity_no_update\nBEFORE UPDATE ON notes_meta\nWHEN OLD.key IN ('project_id','registry_id') OR NEW.key IN ('project_id','registry_id')\nBEGIN SELECT RAISE(ABORT,'notes_identity_immutable'); END"},
  notes_identity_no_delete: {"table":"notes_meta","sql":"CREATE TRIGGER notes_identity_no_delete\nBEFORE DELETE ON notes_meta WHEN OLD.key IN ('project_id','registry_id')\nBEGIN SELECT RAISE(ABORT,'notes_identity_immutable'); END"},
  notes_identity_no_replace: {"table":"notes_meta","sql":"CREATE TRIGGER notes_identity_no_replace\nBEFORE INSERT ON notes_meta\nWHEN NEW.key IN ('project_id','registry_id')\n AND EXISTS(SELECT 1 FROM notes_meta WHERE key=NEW.key)\nBEGIN SELECT RAISE(ABORT,'notes_identity_immutable'); END"},
};
const notesNativeProjections = {
  note_native_creates: "source_store_id,invocation_id,account_id,note_id,mutation_id,input_digest,capability_digest,revision,created_at",
};
const capabilitiesNativeTables = {
  cap_native_note_intents: "CREATE TABLE cap_native_note_intents(\n  invocation_id TEXT PRIMARY KEY,\n  account_id TEXT NOT NULL,\n  notes_store_id TEXT NOT NULL\n    CHECK(length(notes_store_id)=32 AND notes_store_id NOT GLOB '*[^0-9a-f]*'),\n  note_id TEXT NOT NULL\n    CHECK(length(note_id)=66 AND substr(note_id,1,2)='n_'\n      AND substr(note_id,3) NOT GLOB '*[^0-9a-f]*'),\n  mutation_id TEXT NOT NULL\n    CHECK(length(mutation_id)=66 AND substr(mutation_id,1,2)='m_'\n      AND substr(mutation_id,3) NOT GLOB '*[^0-9a-f]*'),\n  input_digest TEXT NOT NULL\n    CHECK(length(input_digest)=64 AND input_digest NOT GLOB '*[^0-9a-f]*'),\n  input_bytes INTEGER NOT NULL CHECK(input_bytes BETWEEN 1 AND 262144),\n  started_at INTEGER CHECK(started_at BETWEEN 0 AND 9007199254740991),\n  input_purged_at INTEGER CHECK(input_purged_at BETWEEN 0 AND 9007199254740991),\n  UNIQUE(account_id,note_id),\n  UNIQUE(account_id,mutation_id),\n  FOREIGN KEY(invocation_id,account_id) REFERENCES cap_invocations(id,account_id)\n) STRICT",
};
const capabilitiesNativeIndexes = {
  cap_invocations_native_identity: {"table":"cap_invocations","sql":"CREATE UNIQUE INDEX cap_invocations_native_identity\n  ON cap_invocations(id,account_id)"},
  cap_invocations_account_admission: {"table":"cap_invocations","sql":"CREATE INDEX cap_invocations_account_admission\n  ON cap_invocations(account_id,created_at,id)"},
  cap_invocations_principal_admission: {"table":"cap_invocations","sql":"CREATE INDEX cap_invocations_principal_admission\n  ON cap_invocations(account_id,principal_id,created_at,id)"},
  cap_invocations_nonterminal: {"table":"cap_invocations","sql":"CREATE INDEX cap_invocations_nonterminal\n  ON cap_invocations(account_id,principal_id,created_at,id)\n  WHERE status NOT IN ('succeeded','failed','cancelled')"},
};
const capabilitiesNativeGuards = {
  cap_native_note_admission: {"table":"cap_native_note_intents","sql":"CREATE TRIGGER cap_native_note_admission\nBEFORE INSERT ON cap_native_note_intents\nWHEN NOT EXISTS(SELECT 1 FROM cap_invocations i\n  WHERE i.id=NEW.invocation_id AND i.account_id=NEW.account_id\n    AND i.capability_id='notes.createDraft' AND i.capability_version=1\n    AND i.capability_digest='95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204'\n    AND i.job_id IS NULL AND i.input_json!='null')\nBEGIN SELECT RAISE(ABORT,'native_note_binding_invalid'); END"},
  cap_native_note_no_replace: {"table":"cap_native_note_intents","sql":"CREATE TRIGGER cap_native_note_no_replace\nBEFORE INSERT ON cap_native_note_intents\nWHEN EXISTS(SELECT 1 FROM cap_native_note_intents\n  WHERE invocation_id=NEW.invocation_id\n     OR (account_id=NEW.account_id AND note_id=NEW.note_id)\n     OR (account_id=NEW.account_id AND mutation_id=NEW.mutation_id))\nBEGIN SELECT RAISE(ABORT,'native_note_identity_immutable'); END"},
  cap_native_note_no_delete: {"table":"cap_native_note_intents","sql":"CREATE TRIGGER cap_native_note_no_delete\nBEFORE DELETE ON cap_native_note_intents\nBEGIN SELECT RAISE(ABORT,'native_note_identity_immutable'); END"},
  cap_native_note_update_guard: {"table":"cap_native_note_intents","sql":"CREATE TRIGGER cap_native_note_update_guard\nBEFORE UPDATE ON cap_native_note_intents\nWHEN NEW.invocation_id IS NOT OLD.invocation_id OR NEW.account_id IS NOT OLD.account_id\n  OR NEW.notes_store_id IS NOT OLD.notes_store_id OR NEW.note_id IS NOT OLD.note_id\n  OR NEW.mutation_id IS NOT OLD.mutation_id OR NEW.input_digest IS NOT OLD.input_digest\n  OR NEW.input_bytes IS NOT OLD.input_bytes\n  OR (OLD.started_at IS NOT NULL AND NEW.started_at IS NOT OLD.started_at)\n  OR (OLD.input_purged_at IS NOT NULL AND NEW.input_purged_at IS NOT OLD.input_purged_at)\n  OR (NEW.input_purged_at IS NOT NULL AND NOT EXISTS(\n    SELECT 1 FROM cap_invocations i JOIN cap_receipts r ON r.invocation_id=i.id\n    WHERE i.id=NEW.invocation_id AND i.input_json='null'\n      AND i.status IN ('succeeded','failed','cancelled')))\nBEGIN SELECT RAISE(ABORT,'native_note_identity_immutable'); END"},
  cap_native_note_input_guard: {"table":"cap_invocations","sql":"CREATE TRIGGER cap_native_note_input_guard\nBEFORE UPDATE OF input_json ON cap_invocations\nWHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=OLD.id)\n AND NEW.input_json IS NOT OLD.input_json\n AND (NEW.input_json!='null' OR NEW.status NOT IN ('succeeded','failed','cancelled')\n      OR NOT EXISTS(SELECT 1 FROM cap_receipts r WHERE r.invocation_id=OLD.id))\nBEGIN SELECT RAISE(ABORT,'native_note_input_immutable'); END"},
  cap_native_receipt_no_update: {"table":"cap_receipts","sql":"CREATE TRIGGER cap_native_receipt_no_update\nBEFORE UPDATE ON cap_receipts\nWHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=OLD.invocation_id)\nBEGIN SELECT RAISE(ABORT,'native_note_receipt_immutable'); END"},
  cap_native_receipt_no_delete: {"table":"cap_receipts","sql":"CREATE TRIGGER cap_native_receipt_no_delete\nBEFORE DELETE ON cap_receipts\nWHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=OLD.invocation_id)\nBEGIN SELECT RAISE(ABORT,'native_note_receipt_immutable'); END"},
  cap_native_receipt_no_replace: {"table":"cap_receipts","sql":"CREATE TRIGGER cap_native_receipt_no_replace\nBEFORE INSERT ON cap_receipts\nWHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=NEW.invocation_id)\n AND EXISTS(SELECT 1 FROM cap_receipts r WHERE r.invocation_id=NEW.invocation_id)\nBEGIN SELECT RAISE(ABORT,'native_note_receipt_immutable'); END"},
  cap_identity_no_update: {"table":"cap_metadata","sql":"CREATE TRIGGER cap_identity_no_update\nBEFORE UPDATE ON cap_metadata\nWHEN OLD.key IN ('project_id','registry_id') OR NEW.key IN ('project_id','registry_id')\nBEGIN SELECT RAISE(ABORT,'cap_identity_immutable'); END"},
  cap_identity_no_delete: {"table":"cap_metadata","sql":"CREATE TRIGGER cap_identity_no_delete\nBEFORE DELETE ON cap_metadata WHEN OLD.key IN ('project_id','registry_id')\nBEGIN SELECT RAISE(ABORT,'cap_identity_immutable'); END"},
  cap_identity_no_replace: {"table":"cap_metadata","sql":"CREATE TRIGGER cap_identity_no_replace\nBEFORE INSERT ON cap_metadata\nWHEN NEW.key IN ('project_id','registry_id')\n AND EXISTS(SELECT 1 FROM cap_metadata WHERE key=NEW.key)\nBEGIN SELECT RAISE(ABORT,'cap_identity_immutable'); END"},
};
const capabilitiesNativeProjections = {
  cap_native_note_intents: "invocation_id,account_id,notes_store_id,note_id,mutation_id,input_digest,input_bytes,started_at,input_purged_at",
};

async function checkedDatabaseFile(filename, info) {
  if (!info.isFile() || info.isSymbolicLink() || info.size < 100) fail('storage_format_unreadable');
  for (const suffix of ['-wal', '-shm', '-journal']) {
    const journal = await missing(filename + suffix);
    if (journal && (!journal.isFile() || journal.isSymbolicLink())) fail('storage_format_unreadable');
  }
}

function inspectDatabase(filename, inspect) {
  let db;
  try {
    // Read WAL normally. immutable=1 could silently ignore committed WAL data.
    db = new DatabaseSync(filename, { readOnly: true });
    db.exec('PRAGMA query_only=ON; PRAGMA busy_timeout=3000; BEGIN');
    return inspect(db);
  } catch (e) {
    if (e.code === 'storage_format_unknown' || e.code === 'storage_format_unreadable') throw e;
    fail('storage_format_unreadable');
  } finally { db?.close(); }
}

async function readRoomsFormat(dataDir) {
  const filename = path.join(dataDir, 'rooms-v2.sqlite');
  const info = await missing(filename);
  if (!info) {
    // An orphan journal is never interpreted as a fresh, empty volume.
    for (const suffix of ['-wal', '-shm', '-journal']) if (await missing(filename + suffix)) fail('storage_format_unreadable');
    let legacy = false;
    for await (const entry of await opendir(dataDir)) {
      if (!serviceFiles.has(entry.name) && /^[A-Za-z0-9_-]{16,96}\.json$/u.test(entry.name)) {
        if (!entry.isFile() || entry.isSymbolicLink()) fail('storage_format_unreadable');
        legacy = true;
      }
    }
    return legacy ? 1 : 'empty';
  }
  await checkedDatabaseFile(filename, info);
  return inspectDatabase(filename, db => {
    if (db.prepare('PRAGMA user_version').get().user_version !== 2) fail('storage_format_unknown');
    const tables = new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(r => r.name));
    if (roomTablesV2.some(name => !tables.has(name))) fail('storage_format_unreadable');
    // Prove the essential projection is readable, without enumerating contents.
    db.prepare('SELECT room_id,auth,closed_json,sequence,reserved_bytes,update_bytes FROM room_state LIMIT 0').all();
    return 2;
  });
}

async function readAppsFormat(dataDir) {
  const directory = path.join(dataDir, 'apps'), root = await missing(directory);
  if (!root) return 'empty';
  if (!root.isDirectory() || root.isSymbolicLink()) fail('storage_format_unreadable');
  const filename = path.join(directory, 'registry.sqlite'), info = await missing(filename);
  if (!info) {
    // Only an absent or actually empty Apps directory is a fresh store. An
    // orphan journal, backup or unknown file is evidence requiring review.
    for await (const entry of await opendir(directory)) fail('storage_format_unreadable');
    return 'empty';
  }
  await checkedDatabaseFile(filename, info);
  return inspectDatabase(filename, db => {
    const objects = db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' AND type IN ('table','view','trigger')").all();
    if (!objects.some(row => row.type === 'table' && row.name === 'apps_meta')) fail('storage_format_unreadable');
    const markers = db.prepare("SELECT value FROM apps_meta WHERE key='schema' LIMIT 2").all();
    const version = db.prepare('PRAGMA user_version').get().user_version;
    if (markers.length !== 1) fail('storage_format_unreadable');
    const format = markers[0].value === 'soty.apps-registry.v1' && [0, 1].includes(version) ? 1
      : markers[0].value === 'soty.apps-registry.v2' && version === 2 ? 2
        : markers[0].value === 'soty.apps-registry.v3' && version === 3 ? 3
          : markers[0].value === 'soty.apps-registry.v4' && version === 4 ? 4
            : markers[0].value === 'soty.apps-registry.v5' && version === 5 ? 5
              : markers[0].value === 'soty.apps-registry.v6' && version === 6 ? 6 : null;
    if (!format) fail('storage_format_unknown');
    const projections = { ...appCore, ...(format >= 2 ? appDomains : {}), ...(format >= 3 ? appPublications : {}),
      ...(format >= 4 ? appSources : {}), ...(format >= 5 ? appSaved : {}), ...(format === 6 ? appDiscussions : {}) };
    const guards = { ...(format >= 3 ? appTargetGuards : {}), ...(format >= 4 ? appSourceGuards : {}),
      ...(format >= 5 ? appSavedGuards : {}), ...(format === 6 ? appDiscussionGuards : {}) };
    if (objects.length !== Object.keys(projections).length + Object.keys(guards).length || objects.some(row =>
      row.type === 'table' ? !Object.hasOwn(projections, row.name)
        : row.type !== 'trigger' || !Object.hasOwn(guards, row.name) || row.tbl_name !== guards[row.name].table
          || normalizedSql(row.sql) !== normalizedSql(guards[row.name].sql))) fail('storage_format_unreadable');
    // Independent format recognition, not row/constraint integrity attestation.
    // Every identifier below is a trusted constant, never database contents.
    for (const [table, columns] of Object.entries(projections)) db.prepare(`SELECT ${columns} FROM ${table} LIMIT 0`).all();
    if (format >= 2) {
      const pinned = db.prepare("SELECT value FROM apps_meta WHERE key='legacy_origin_template' LIMIT 2").all();
      if (pinned.length !== 1 || typeof pinned[0].value !== 'string') fail('storage_format_unreadable');
    }
    return format;
  });
}

async function checkedStoreFile(dataDir, store, basename) {
  const directory = path.join(dataDir, store), root = await missing(directory);
  if (!root) return null;
  if (!root.isDirectory() || root.isSymbolicLink()) fail('storage_format_unreadable');
  const filename = path.join(directory, basename), info = await missing(filename);
  const allowed = new Set([basename, basename + '-wal', basename + '-shm', basename + '-journal']);
  for await (const entry of await opendir(directory)) {
    if (!info || !allowed.has(entry.name) || !entry.isFile() || entry.isSymbolicLink()) fail('storage_format_unreadable');
  }
  if (!info) return null;
  await checkedDatabaseFile(filename, info);
  return filename;
}

function recognizeNativeLayout(db, tables, indexes, { fts = false, strict = false, tableSql = {}, guards = {} } = {}) {
  const objects = db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'").all();
  if (objects.length !== Object.keys(tables).length + Object.keys(indexes).length + Object.keys(guards).length || objects.some(row => {
    if (row.type === 'table') return !Object.hasOwn(tables, row.name) || (Object.hasOwn(tableSql, row.name)
      && normalizedSql(row.sql) !== normalizedSql(tableSql[row.name]));
    const expected = row.type === 'index' ? indexes : row.type === 'trigger' ? guards : null;
    return !expected || !Object.hasOwn(expected, row.name) || row.tbl_name !== expected[row.name].table
      || normalizedSql(row.sql) !== normalizedSql(expected[row.name].sql);
  })) fail('storage_format_unreadable');
  if (fts && normalizedSql(objects.find(row => row.name === 'notes_fts')?.sql) !== normalizedSql(notesFts)) fail('storage_format_unreadable');
  const kinds = new Map(db.prepare('PRAGMA table_list').all().filter(row => row.schema === 'main').map(row => [row.name, row]));
  for (const [table, columns] of Object.entries(tables)) {
    // Only fixed host identifiers enter SQL. FTS hidden columns are checked but
    // never selected (rank/MATCH would execute an unnecessary text search).
    const info = db.prepare(`PRAGMA table_xinfo(${table})`).all();
    const kind = fts && table === 'notes_fts' ? 'virtual' : fts && table.startsWith('notes_fts_') ? 'shadow' : 'table';
    if (info.map(row => row.name).join(',') !== columns || kinds.get(table)?.type !== kind
      || kinds.get(table)?.strict !== Number(strict || Object.hasOwn(tableSql, table))
      || info.some(row => row.hidden !== (fts && table === 'notes_fts' && ['notes_fts', 'rank'].includes(row.name) ? 1 : 0))) fail('storage_format_unreadable');
    const projection = fts && table === 'notes_fts' ? 'scope,title,body,items' : columns;
    db.prepare(`SELECT ${projection} FROM ${table} LIMIT 0`).all();
  }
}

function recognizeNativeMetadata(db, table, expected, registry) {
  // A tiny bounded read is sufficient for these exact two/three-key maps. It
  // neither emits store identities nor treats a missing v2 identity as empty.
  const rows = db.prepare(`SELECT
    CASE WHEN typeof(key)='text' AND length(key)<=32 THEN key ELSE NULL END AS key,
    CASE WHEN typeof(value)='text' AND length(value)<=128 THEN value ELSE NULL END AS value
    FROM ${table} LIMIT 4`).all();
  if (rows.length !== Object.keys(expected).length + Number(registry)) fail('storage_format_unknown');
  const seen = new Set();
  for (const row of rows) {
    if (typeof row.key !== 'string' || typeof row.value !== 'string' || seen.has(row.key)) fail('storage_format_unknown');
    seen.add(row.key);
    if (registry && row.key === 'registry_id') {
      if (!/^[a-f0-9]{32}$/u.test(row.value)) fail('storage_format_unknown');
    } else if (!Object.hasOwn(expected, row.key) || row.value !== expected[row.key]) fail('storage_format_unknown');
  }
  if (registry && !seen.has('registry_id')) fail('storage_format_unknown');
}

async function readNotesFormat(dataDir) {
  const filename = await checkedStoreFile(dataDir, 'notes', 'notes.sqlite');
  if (!filename) return 'empty';
  return inspectDatabase(filename, db => {
    const version = db.prepare('PRAGMA user_version').get().user_version;
    if (version !== 1 && version !== 2) fail('storage_format_unknown');
    recognizeNativeMetadata(db, 'notes_meta', { lineage: `soty.notes.sqlite.v${version}`, project_id: 'soty' }, version === 2);
    recognizeNativeLayout(db, { ...notesTables, ...(version === 2 ? notesNativeProjections : {}) }, notesIndexes,
      { fts: true, ...(version === 2 ? { tableSql: notesNativeTables, guards: notesNativeGuards } : {}) });
    return version;
  });
}

async function readCapabilitiesFormat(dataDir) {
  const filename = await checkedStoreFile(dataDir, 'capabilities', 'capabilities.sqlite');
  if (!filename) return 'empty';
  return inspectDatabase(filename, db => {
    const version = db.prepare('PRAGMA user_version').get().user_version;
    if (version !== 1 && version !== 2) fail('storage_format_unknown');
    recognizeNativeMetadata(db, 'cap_metadata', { lineage: `soty.capabilities.sqlite.v${version}`,
      ...(version === 2 ? { project_id: 'soty' } : {}) }, version === 2);
    recognizeNativeLayout(db, { ...capabilitiesTables, ...(version === 2 ? capabilitiesNativeProjections : {}) },
      { ...capabilitiesIndexes, ...(version === 2 ? capabilitiesNativeIndexes : {}) },
      { strict: true, ...(version === 2 ? { tableSql: capabilitiesNativeTables, guards: capabilitiesNativeGuards } : {}) });
    return version;
  });
}

export async function readStorageFormat(dataDir = '/data') {
  const root = await lstat(dataDir);
  if (!root.isDirectory() || root.isSymbolicLink()) fail('storage_directory_invalid');
  const rooms = await readRoomsFormat(dataDir), apps = await readAppsFormat(dataDir);
  const notes = await readNotesFormat(dataDir), capabilities = await readCapabilitiesFormat(dataDir);
  return { ok: true, schema: 'soty.storage-format.v3', rooms, apps, notes, capabilities };
}

if (process.env.SOTY_STORAGE_PROBE === '1') {
  try { process.stdout.write(JSON.stringify(await readStorageFormat('/data'))); }
  catch (e) {
    const code = ['storage_directory_invalid', 'storage_format_unknown', 'storage_format_unreadable'].includes(e.code) ? e.code : 'storage_probe_failed';
    process.stdout.write(JSON.stringify({ ok: false, code })); process.exitCode = 1;
  }
}
