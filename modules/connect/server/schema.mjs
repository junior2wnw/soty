export const SCHEMA_VERSION = 3;
export const READER_EPOCH = 1;
const SCHEMA_LINEAGE = 'connect.sqlite.local.v1';

// These are additive migrations. Never recreate a database after an open/migration error.
export const MIGRATIONS = [
  `
    CREATE TABLE connect_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE accounts (id TEXT PRIMARY KEY, label TEXT NOT NULL, created_at INTEGER NOT NULL);
    CREATE TABLE installations (
      id TEXT PRIMARY KEY, public_jwk TEXT NOT NULL, encryption_jwk TEXT NOT NULL,
      account_id TEXT REFERENCES accounts(id), label TEXT NOT NULL,
      state TEXT NOT NULL CHECK (state IN ('pending','active','revoked')),
      created_at INTEGER NOT NULL, activated_at INTEGER, revoked_at INTEGER,
      enrolled_by TEXT
    );
    CREATE INDEX installations_account ON installations(account_id, state);
    CREATE TABLE challenges (
      id TEXT PRIMARY KEY, origin TEXT NOT NULL, operation TEXT NOT NULL,
      digest TEXT NOT NULL, message TEXT NOT NULL, expires_at INTEGER NOT NULL,
      consumed_at INTEGER
    );
    CREATE TABLE rate_limits (key TEXT PRIMARY KEY, bucket INTEGER NOT NULL, count INTEGER NOT NULL);
    CREATE TABLE cards (
      id TEXT PRIMARY KEY, account_id TEXT NOT NULL REFERENCES accounts(id),
      created_at INTEGER NOT NULL, revoked_at INTEGER
    );
    CREATE UNIQUE INDEX cards_active_account ON cards(account_id) WHERE revoked_at IS NULL;
    CREATE TABLE contact_requests (
      id TEXT PRIMARY KEY, sender_id TEXT NOT NULL REFERENCES accounts(id),
      recipient_id TEXT NOT NULL REFERENCES accounts(id), card_id TEXT NOT NULL REFERENCES cards(id),
      state TEXT NOT NULL CHECK (state IN ('pending','accepted','declined','cancelled','expired')),
      created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, relationship_id TEXT
    );
    CREATE INDEX contact_requests_participants ON contact_requests(sender_id, recipient_id, state);
    CREATE TABLE relationships (
      id TEXT PRIMARY KEY, first_id TEXT NOT NULL REFERENCES accounts(id),
      second_id TEXT NOT NULL REFERENCES accounts(id), state TEXT NOT NULL CHECK (state IN ('active','ended')),
      created_at INTEGER NOT NULL, ended_at INTEGER
    );
    CREATE UNIQUE INDEX relationships_active_pair ON relationships(first_id, second_id) WHERE state='active';
    CREATE TABLE blocks (
      owner_id TEXT NOT NULL REFERENCES accounts(id), peer_id TEXT NOT NULL REFERENCES accounts(id),
      created_at INTEGER NOT NULL, PRIMARY KEY(owner_id, peer_id)
    );
    CREATE TABLE enrollments (
      id TEXT PRIMARY KEY, device_id TEXT NOT NULL REFERENCES installations(id),
      label TEXT NOT NULL, encryption_jwk TEXT NOT NULL, start_digest TEXT NOT NULL,
      state TEXT NOT NULL CHECK (state IN ('pending','approved','finished','cancelled')),
      created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL,
      account_id TEXT REFERENCES accounts(id), approved_by TEXT, approved_at INTEGER, wrapped_key TEXT
    );
    CREATE INDEX enrollments_device ON enrollments(device_id, state);
    CREATE TABLE vaults (
      account_id TEXT PRIMARY KEY REFERENCES accounts(id), revision INTEGER NOT NULL,
      envelope TEXT NOT NULL, updated_at INTEGER NOT NULL
    );
    CREATE TABLE recovery_methods (
      id TEXT PRIMARY KEY, account_id TEXT NOT NULL REFERENCES accounts(id), verifier TEXT NOT NULL,
      wrapped_key TEXT NOT NULL, state TEXT NOT NULL CHECK (state IN ('pending','verified','consumed','superseded','expired')),
      created_by TEXT NOT NULL, created_at INTEGER NOT NULL, expires_at INTEGER,
      verified_at INTEGER, consumed_at INTEGER, consumed_by TEXT
    );
    CREATE UNIQUE INDEX recovery_active_account ON recovery_methods(account_id) WHERE state='verified';
    CREATE UNIQUE INDEX recovery_pending_account ON recovery_methods(account_id) WHERE state='pending';
    CREATE INDEX recovery_account ON recovery_methods(account_id);
  `,
  `
    ALTER TABLE enrollments ADD COLUMN finished_at INTEGER;
    ALTER TABLE enrollments ADD COLUMN receipt_until INTEGER;
    ALTER TABLE enrollments ADD COLUMN receipt_json TEXT;
    ALTER TABLE recovery_methods ADD COLUMN consumed_digest TEXT;
    ALTER TABLE recovery_methods ADD COLUMN receipt_until INTEGER;
    ALTER TABLE recovery_methods ADD COLUMN receipt_json TEXT;
    CREATE INDEX challenges_expiry ON challenges(expires_at);
    CREATE INDEX contact_requests_expiry ON contact_requests(state, expires_at);
  `,
  `
    CREATE TABLE contact_invitations (
      id TEXT PRIMARY KEY, relationship_id TEXT NOT NULL REFERENCES relationships(id),
      sender_id TEXT NOT NULL REFERENCES accounts(id), recipient_id TEXT NOT NULL REFERENCES accounts(id),
      url TEXT NOT NULL, label TEXT NOT NULL, created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL,
      dismissed_at INTEGER, invalidated_at INTEGER
    );
    CREATE INDEX invitations_recipient ON contact_invitations(recipient_id, invalidated_at, dismissed_at);
  `,
];

const REQUIRED_COLUMNS = {
  connect_meta: 'key value',
  accounts: 'id label created_at',
  installations: 'id public_jwk encryption_jwk account_id label state created_at activated_at revoked_at enrolled_by',
  challenges: 'id origin operation digest message expires_at consumed_at',
  rate_limits: 'key bucket count',
  cards: 'id account_id created_at revoked_at',
  contact_requests: 'id sender_id recipient_id card_id state created_at expires_at relationship_id',
  relationships: 'id first_id second_id state created_at ended_at',
  blocks: 'owner_id peer_id created_at',
  enrollments: 'id device_id label encryption_jwk start_digest state created_at expires_at account_id approved_by approved_at wrapped_key',
  vaults: 'account_id revision envelope updated_at',
  recovery_methods: 'id account_id verifier wrapped_key state created_by created_at expires_at verified_at consumed_at consumed_by',
};

function validateLayout(db, understoodVersion) {
  const required = { ...REQUIRED_COLUMNS };
  if (understoodVersion >= 2) {
    required.enrollments += ' finished_at receipt_until receipt_json';
    required.recovery_methods += ' consumed_digest receipt_until receipt_json';
  }
  if (understoodVersion >= 3) required.contact_invitations = 'id relationship_id sender_id recipient_id url label created_at expires_at dismissed_at invalidated_at';
  for (const [table, fields] of Object.entries(required)) {
    const columns = new Set(db.prepare(`PRAGMA table_info(${table})`).all().map((item) => item.name));
    if (!fields.split(' ').every((field) => columns.has(field))) throw new Error('connect_schema_layout_invalid');
  }
  for (const [table, name] of [['cards', 'cards_active_account'], ['relationships', 'relationships_active_pair'], ['recovery_methods', 'recovery_active_account'], ['recovery_methods', 'recovery_pending_account']]) {
    const index = db.prepare(`PRAGMA index_list(${table})`).all().find((item) => item.name === name);
    if (!index || index.unique !== 1 || index.partial !== 1) throw new Error('connect_schema_layout_invalid');
  }
}

function validateMetadata(db, projectId, readerEpoch, supportedVersion) {
  const version = Number(db.prepare('PRAGMA user_version').get().user_version);
  if (!Number.isInteger(version) || version < 0) throw new Error('connect_schema_invalid');
  if (version === 0) {
    if (db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1").get()) throw new Error('connect_schema_metadata_mismatch');
    return version;
  }
  const metadata = Object.fromEntries(db.prepare('SELECT key,value FROM connect_meta').all().map((row) => [row.key, row.value]));
  if (metadata.project_id !== projectId) throw new Error('connect_project_mismatch');
  if (!/^[1-9]\d*$/u.test(metadata.min_reader || '') || Number(metadata.min_reader) > readerEpoch) throw new Error('connect_reader_too_old');
  if (metadata.schema_lineage !== SCHEMA_LINEAGE || metadata.schema_version !== String(version)) throw new Error('connect_schema_metadata_mismatch');
  // A later additive writer may add tables/columns only while retaining this epoch's
  // semantics. Epoch changes, inconsistent metadata and missing invariants fail closed.
  validateLayout(db, Math.min(version, supportedVersion));
  return version;
}

export function migrateDatabase(db, projectId, { supportedVersion = SCHEMA_VERSION, readerEpoch = READER_EPOCH } = {}) {
  if (!Number.isInteger(supportedVersion) || supportedVersion < 1 || supportedVersion > SCHEMA_VERSION) throw new Error('connect_schema_invalid');
  db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;');
  validateMetadata(db, projectId, readerEpoch, supportedVersion);
  db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;');
  db.exec('BEGIN IMMEDIATE');
  try {
    // A second process may have migrated while this connection was waiting.
    const current = validateMetadata(db, projectId, readerEpoch, supportedVersion);
    for (let index = current; index < supportedVersion; index += 1) {
      db.exec(MIGRATIONS[index]);
      db.exec(`PRAGMA user_version=${index + 1}`);
      db.prepare('INSERT OR REPLACE INTO connect_meta(key,value) VALUES (?,?)').run('schema_version', String(index + 1));
      db.prepare('INSERT OR IGNORE INTO connect_meta(key,value) VALUES (?,?)').run('min_reader', String(READER_EPOCH));
      db.prepare('INSERT OR IGNORE INTO connect_meta(key,value) VALUES (?,?)').run('schema_lineage', SCHEMA_LINEAGE);
    }
    const storedProject = db.prepare("SELECT value FROM connect_meta WHERE key='project_id'").get();
    if (storedProject && storedProject.value !== projectId) throw new Error('connect_project_mismatch');
    db.prepare('INSERT OR IGNORE INTO connect_meta(key,value) VALUES (?,?)').run('project_id', projectId);
    validateMetadata(db, projectId, readerEpoch, supportedVersion);
    if (db.prepare('PRAGMA quick_check').all().some((row) => row.quick_check !== 'ok') || db.prepare('PRAGMA foreign_key_check').get()) throw new Error('connect_storage_corrupt');
    db.exec('COMMIT');
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
}
