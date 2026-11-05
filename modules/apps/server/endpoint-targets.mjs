import { createHash } from 'node:crypto';
import { isIP } from 'node:net';
import { assertApps } from './protocol.mjs';

export const SELF_IP_PROFILE = 'soty.relay-self-ip.v1';
export const ENDPOINT_STORAGE = 'soty.apps-self-ip-targets.v1';
export const ENDPOINT_SCHEMA = 'soty.runtime-target.v2';
export const SELF_ADDRESSES_SCHEMA = 'soty.connector-self-addresses.v1';
const hash = value => createHash('sha256').update(value).digest('hex');
const exact = (value, keys) => value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));

// Literal syntax is checked before URL parsing can normalize short, hexadecimal,
// decimal or encoded spellings. The selected connector additionally checks OS
// assignment immediately before each connection; this is not a LAN allowlist.
export function selfIPv4(value) {
  assertApps(typeof value === 'string' && isIP(value) === 4 && value.split('.').every(part => String(Number(part)) === part), 'apps_endpoint_invalid');
  const [a, b] = value.split('.').map(Number);
  assertApps(a !== 0 && a !== 127 && a < 224 && !(a === 169 && b === 254) && value !== '255.255.255.255', 'apps_endpoint_invalid');
  return value;
}
export function normalizeSelfEndpoint(value) {
  assertApps(exact(value, ['scope', 'scheme', 'family', 'address']) && value.scope === 'connector-self'
    && value.scheme === 'http' && value.family === 'IPv4', 'apps_endpoint_invalid');
  return Object.freeze({ scope: 'connector-self', scheme: 'http', family: 'IPv4', address: selfIPv4(value.address) });
}
export function endpointDigestTuple(value) {
  const endpoint = normalizeSelfEndpoint(value.endpoint);
  return [ENDPOINT_SCHEMA, value.appId, value.revision, value.ownerAccountId, value.connectorKey, value.profile,
    endpoint.scheme, endpoint.scope, endpoint.family, endpoint.address, value.port, value.entryPath];
}
export function readRuntimeEndpoint(db, target) {
  if (target.profile !== SELF_IP_PROFILE) return undefined;
  const row = db.prepare('SELECT * FROM app_runtime_endpoints WHERE app_id=? AND target_revision=?').get(target.app_id ?? target.appId, target.revision);
  assertApps(row && row.target_digest === target.digest, 'apps_endpoint_corrupt', 500);
  return normalizeSelfEndpoint({ scope: row.scope, scheme: row.scheme, family: row.family, address: row.address });
}
export function targetFromRow(db, row) {
  const endpoint = readRuntimeEndpoint(db, row);
  return { appId: row.app_id, revision: row.revision, ownerAccountId: row.owner_account_id, connectorKey: row.connector_key,
    port: row.port, entryPath: row.entry_path, profile: row.profile, digest: row.digest,
    ...(endpoint ? { schema: ENDPOINT_SCHEMA, endpoint } : {}) };
}
export function inventoryDigest({ connectorKey, channelId, revision, addresses }) {
  return hash(JSON.stringify([SELF_ADDRESSES_SCHEMA, connectorKey, channelId, revision, addresses.map(item => item.address)]));
}
export function normalizeSelfInventory(value, { connectorKey, channelId }) {
  assertApps(exact(value, ['schema', 'revision', 'addresses', 'digest']) && value.schema === SELF_ADDRESSES_SCHEMA
    && Number.isSafeInteger(value.revision) && value.revision >= 1 && Array.isArray(value.addresses)
    && value.addresses.length <= 32 && typeof value.digest === 'string' && /^[a-f0-9]{64}$/u.test(value.digest), 'apps_endpoint_inventory_invalid');
  const addresses = value.addresses.map(item => {
    assertApps(exact(item, ['family', 'address']) && item.family === 'IPv4', 'apps_endpoint_inventory_invalid');
    return Object.freeze({ family: 'IPv4', address: selfIPv4(item.address) });
  });
  assertApps(new Set(addresses.map(item => item.address)).size === addresses.length
    && addresses.every((item, index) => index === 0 || addresses[index - 1].address < item.address)
    && value.digest === inventoryDigest({ connectorKey, channelId, revision: value.revision, addresses }), 'apps_endpoint_inventory_invalid');
  return Object.freeze({ schema: SELF_ADDRESSES_SCHEMA, revision: value.revision, addresses: Object.freeze(addresses), digest: value.digest });
}

export const endpointColumns = {
  app_runtime_endpoints: ['app_id', 'target_revision', 'target_digest', 'scope', 'scheme', 'family', 'address'],
  app_endpoint_registration_receipts: ['account_id', 'request_key', 'intent_hash', 'app_id', 'created_at'],
};
export function endpointDdl() {
  return `CREATE TABLE app_runtime_endpoints (
    app_id TEXT NOT NULL,target_revision INTEGER NOT NULL CHECK(target_revision>=1),target_digest TEXT NOT NULL CHECK(length(target_digest)=64),
    scope TEXT NOT NULL CHECK(scope='connector-self'),scheme TEXT NOT NULL CHECK(scheme='http'),family TEXT NOT NULL CHECK(family='IPv4'),address TEXT NOT NULL CHECK(length(address) BETWEEN 7 AND 15),
    PRIMARY KEY(app_id,target_revision),FOREIGN KEY(app_id,target_revision) REFERENCES app_runtime_targets(app_id,revision));
  CREATE TABLE app_endpoint_registration_receipts (
    account_id TEXT NOT NULL,request_key TEXT NOT NULL,intent_hash TEXT NOT NULL CHECK(length(intent_hash)=64),app_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,PRIMARY KEY(account_id,request_key),FOREIGN KEY(app_id,account_id) REFERENCES local_apps(id,owner_account_id));`;
}
export function endpointGuards() {
  return ['app_runtime_endpoints', 'app_endpoint_registration_receipts'].flatMap(table => [
    `CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table} BEGIN SELECT RAISE(ABORT,'apps_endpoint_immutable'); END`,
    `CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table} BEGIN SELECT RAISE(ABORT,'apps_endpoint_immutable'); END`,
    `CREATE TRIGGER ${table}_no_replace BEFORE INSERT ON ${table} WHEN EXISTS(SELECT 1 FROM ${table} WHERE ${table === 'app_runtime_endpoints' ? 'app_id=NEW.app_id AND target_revision=NEW.target_revision' : 'account_id=NEW.account_id AND request_key=NEW.request_key'}) BEGIN SELECT RAISE(ABORT,'apps_endpoint_immutable'); END`,
  ]);
}
export function insertRuntimeEndpoint(db, target) {
  if (target.profile !== SELF_IP_PROFILE) return;
  const endpoint = normalizeSelfEndpoint(target.endpoint);
  db.prepare('INSERT INTO app_runtime_endpoints VALUES (?,?,?,?,?,?,?)').run(target.appId, target.revision, target.digest,
    endpoint.scope, endpoint.scheme, endpoint.family, endpoint.address);
}
