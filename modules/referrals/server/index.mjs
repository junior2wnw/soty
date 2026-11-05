import { DatabaseSync } from 'node:sqlite';
import { mkdirSync, realpathSync } from 'node:fs';
import { basename, dirname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { snapshot } from '../../app-contract/json.mjs';
import { initialize, SCHEMA_VERSION } from './schema.mjs';
import { check, keys, id, integer, currency, rates, hash, proportionalFloor, trustedEvent, ReferralsError } from './validation.mjs';
export { ReferralsError, SCHEMA_VERSION };
export const REFERRAL_OPERATIONS = Object.freeze(['referrals.config.get', 'referrals.config.put', 'referrals.attribution.get', 'referrals.ledger.list', 'referrals.balance.get']);
export const DEFAULT_RATES_BPS = Object.freeze([1000, 1000, 1000]);
export const ROUNDING = 'floor-per-beneficiary;cumulative-refund-of-original-commission';
const moduleRoot = dirname(dirname(fileURLToPath(import.meta.url)));
function physical(file) { try { return realpathSync(file); } catch (error) { if (error.code !== 'ENOENT') throw error; check(dirname(file) !== file, 'referrals_database_path_invalid'); return resolve(physical(dirname(file)), basename(file)); } }
const sync = result => { check(!result || typeof result.then !== 'function', 'referrals_async_authority_forbidden'); return result; };
const syncFunction = value => typeof value === 'function' && !['AsyncFunction', 'AsyncGeneratorFunction', 'GeneratorFunction'].includes(value.constructor?.name);

/** Connect boundary: actor comes only from the authenticated host, never a request body. */
export function createReferralsService({ databasePath, projectId, authorize, readTrustedEvent, clock = Date.now } = {}) {
  check(typeof databasePath === 'string' && databasePath.length > 0, 'referrals_database_path_required'); id(projectId);
  check(syncFunction(authorize) && syncFunction(clock), 'referrals_host_required');
  check(readTrustedEvent === undefined || syncFunction(readTrustedEvent), 'referrals_host_required');
  const file = databasePath === ':memory:' ? databasePath : resolve(databasePath);
  if (file !== ':memory:') {
    const fold = path => process.platform === 'win32' ? path.toLowerCase() : path;
    const root = fold(physical(moduleRoot));
    check(![file, physical(file)].some(path => fold(path) === root || fold(path).startsWith(root + sep)), 'referrals_database_must_be_outside_module');
    mkdirSync(dirname(file), { recursive: true, mode: 0o700 });
  }
  const db = new DatabaseSync(file); let closed = false, active = false, meta;
  try { meta = initialize(db, projectId); } catch (error) { db.close(); throw error; }
  const statements = new Map();
  const statement = sql => { if (!statements.has(sql)) statements.set(sql, db.prepare(sql)); return statements.get(sql); };
  const get = (sql, ...args) => statement(sql).get(...args), all = (sql, ...args) => statement(sql).all(...args), run = (sql, ...args) => statement(sql).run(...args);
  const now = () => integer(clock());
  function transaction(action, { write = true, beforeCommit = () => {} } = {}) {
    check(!closed, 'referrals_service_closed', 503); check(!active && !db.isTransaction, 'referrals_nested_transaction'); active = true;
    try {
      db.exec(write ? 'BEGIN IMMEDIATE' : 'BEGIN'); const result = sync(action()); sync(beforeCommit()); db.exec('COMMIT'); return result;
    } catch (error) { if (db.isTransaction) { try { db.exec('ROLLBACK'); } catch { /* Keep the original failure. */ } } throw error; }
    finally { active = false; }
  }
  function permission(actor, appId, right) { check(sync(authorize({ actor: Object.freeze({ ...actor }), appId, right })) === true, 'referrals_forbidden', 403); }
  function config(appId) {
    const row = get('SELECT * FROM referral_configs WHERE app_id=?', appId);
    return { appId, revision: row?.revision || 0, ratesBps: row ? JSON.parse(row.rates_json) : [...DEFAULT_RATES_BPS], rounding: ROUNDING, payoutsEnabled: false, updatedAt: row?.updated_at || 0 };
  }
  function replay(appId, kind, eventId, digest) {
    const receipt = get('SELECT digest,result FROM referral_receipts WHERE app_id=? AND kind=? AND event_id=?', appId, kind, eventId);
    if (!receipt) return null; check(receipt.digest === digest, 'referrals_event_reused', 409); return { ...JSON.parse(receipt.result), replayed: true };
  }
  function record(appId, kind, eventId, digest, result) { run('INSERT INTO referral_receipts VALUES(?,?,?,?,?)', appId, kind, eventId, digest, JSON.stringify(result)); return result; }
  function configure(args, actor) {
    keys(args, ['expectedAccountId', 'appId', 'mutationId', 'expectedRevision', 'ratesBps']);
    const mutationId = id(args.mutationId), expected = integer(args.expectedRevision), nextRates = rates(args.ratesBps);
    const digest = hash({ expectedRevision: expected, ratesBps: nextRates });
    const prior = replay(args.appId, 'config', mutationId, digest); if (prior) return prior;
    const current = config(args.appId); check(current.revision === expected, 'referrals_revision_conflict', 409);
    const revision = integer(expected + 1, 1), updatedAt = now(), json = JSON.stringify(nextRates);
    run('INSERT INTO referral_configs VALUES(?,?,?,?) ON CONFLICT(app_id) DO UPDATE SET revision=excluded.revision,rates_json=excluded.rates_json,updated_at=excluded.updated_at', args.appId, revision, json, updatedAt);
    run('INSERT INTO referral_config_versions VALUES(?,?,?,?,?)', args.appId, revision, json, actor.accountId, updatedAt);
    return record(args.appId, 'config', mutationId, digest, config(args.appId));
  }
  function attribution(event) {
    check(event.accountId !== event.parentAccountId, 'referrals_self_referral');
    const existing = get('SELECT parent_id FROM referral_attributions WHERE app_id=? AND account_id=?', event.appId, event.accountId);
    if (existing) { check(existing.parent_id === event.parentAccountId, 'referrals_attribution_immutable', 409); return { appId: event.appId, accountId: event.accountId, bound: true, ...(existing.parent_id === null ? {} : { parentAccountId: existing.parent_id }), alreadyBound: true }; }
    const ancestors = new Set([event.accountId]); let ancestor = event.parentAccountId;
    while (ancestor !== null) {
      check(!ancestors.has(ancestor), 'referrals_cycle'); ancestors.add(ancestor);
      check(ancestors.size <= 10000, 'referrals_attribution_depth_exceeded');
      const row = get('SELECT parent_id FROM referral_attributions WHERE app_id=? AND account_id=?', event.appId, ancestor);
      ancestor = row ? row.parent_id : null;
    }
    run('INSERT INTO referral_attributions VALUES(?,?,?,?)', event.appId, event.accountId, event.parentAccountId, now());
    return { appId: event.appId, accountId: event.accountId, bound: true, ...(event.parentAccountId === null ? {} : { parentAccountId: event.parentAccountId }) };
  }
  function ledger(event, beneficiaryId, level, amount, moneyCurrency, paymentId, createdAt) {
    run('INSERT INTO referral_ledger(app_id,beneficiary_id,currency,payment_id,event_kind,event_id,level,amount,created_at) VALUES(?,?,?,?,?,?,?,?,?)', event.appId, beneficiaryId, moneyCurrency, paymentId, event.kind, event.eventId, level, amount, createdAt);
    const previous = get('SELECT amount FROM referral_balances WHERE app_id=? AND beneficiary_id=? AND currency=?', event.appId, beneficiaryId, moneyCurrency)?.amount || '0';
    const total = BigInt(previous) + BigInt(amount); check(total >= 0n && total.toString().length <= 32, 'referrals_balance_limit');
    run('INSERT INTO referral_balances VALUES(?,?,?,?) ON CONFLICT(app_id,beneficiary_id,currency) DO UPDATE SET amount=excluded.amount', event.appId, beneficiaryId, moneyCurrency, total.toString());
  }
  function payment(event) {
    const snapshot = config(event.appId), createdAt = now(), credits = [];
    run('INSERT INTO referral_payments VALUES(?,?,?,?,?,0,?,?,?)', event.appId, event.eventId, event.payerAccountId, event.currency, event.eligibleAmountMinor, snapshot.revision, JSON.stringify(snapshot.ratesBps), createdAt);
    let account = event.payerAccountId;
    const seen = new Set([account]);
    for (let i = 0; i < snapshot.ratesBps.length; i++) {
      const row = get('SELECT parent_id FROM referral_attributions WHERE app_id=? AND account_id=?', event.appId, account);
      if (!row || row.parent_id === null) break; account = row.parent_id;
      check(!seen.has(account), 'referrals_cycle'); seen.add(account);
      const amountMinor = proportionalFloor(event.eligibleAmountMinor, snapshot.ratesBps[i], 10000), level = i + 1;
      run('INSERT INTO referral_commissions VALUES(?,?,?,?,?,0)', event.appId, event.eventId, level, account, amountMinor);
      ledger(event, account, level, amountMinor, event.currency, event.eventId, createdAt);
      credits.push({ beneficiaryAccountId: account, level, rateBps: snapshot.ratesBps[i], amountMinor });
    }
    return { appId: event.appId, paymentId: event.eventId, currency: event.currency, eligibleAmountMinor: event.eligibleAmountMinor, configRevision: snapshot.revision, ratesBps: snapshot.ratesBps, credits, payoutsEnabled: false };
  }
  function refund(event) {
    const original = get('SELECT * FROM referral_payments WHERE app_id=? AND payment_id=?', event.appId, event.paymentId);
    check(original, 'referrals_payment_not_found', 404);
    const total = BigInt(original.refunded_amount) + BigInt(event.refundAmountMinor);
    check(total <= BigInt(original.eligible_amount), 'referrals_refund_exceeds_payment', 409);
    const refundedAmountMinor = Number(total), createdAt = now(), reversals = [];
    for (const row of all('SELECT * FROM referral_commissions WHERE app_id=? AND payment_id=? ORDER BY level', event.appId, event.paymentId)) {
      const reversed = proportionalFloor(row.amount, refundedAmountMinor, original.eligible_amount), delta = reversed - row.reversed_amount;
      check(delta >= 0 && reversed <= row.amount, 'referrals_refund_invalid');
      run('UPDATE referral_commissions SET reversed_amount=? WHERE app_id=? AND payment_id=? AND level=?', reversed, event.appId, event.paymentId, row.level);
      ledger(event, row.beneficiary_id, row.level, -delta, original.currency, event.paymentId, createdAt);
      reversals.push({ beneficiaryAccountId: row.beneficiary_id, level: row.level, amountMinor: delta ? -delta : 0 });
    }
    run('UPDATE referral_payments SET refunded_amount=? WHERE app_id=? AND payment_id=?', refundedAmountMinor, event.appId, event.paymentId);
    return { appId: event.appId, refundId: event.eventId, paymentId: event.paymentId, refundAmountMinor: event.refundAmountMinor, refundedAmountMinor, currency: original.currency, reversals, payoutsEnabled: false };
  }
  const operations = new Set(REFERRAL_OPERATIONS);
  return {
    projectId, registryId: meta.registry_id, schemaVersion: SCHEMA_VERSION, supportedSchemaVersions: Object.freeze([SCHEMA_VERSION]), operations,
    execute({ op, args = {}, actor } = {}) {
      check(!closed, 'referrals_service_closed', 503); check(operations.has(op), 'unsupported_operation');
      args = snapshot(args);
      check(actor && typeof actor === 'object', 'authentication_required', 401);
      const principal = { accountId: id(actor.accountId), deviceId: id(actor.deviceId) };
      check(args?.expectedAccountId === principal.accountId, 'referrals_account_changed', 409); const appId = id(args.appId);
      const right = op === 'referrals.config.put' ? 'configure' : 'read'; permission(principal, appId, right);
      return transaction(() => {
        permission(principal, appId, right);
        if (op === 'referrals.config.put') return configure(args, principal);
        if (op === 'referrals.config.get') { keys(args, ['expectedAccountId', 'appId']); return config(appId); }
        if (op === 'referrals.attribution.get') {
          keys(args, ['expectedAccountId', 'appId']); const row = get('SELECT parent_id FROM referral_attributions WHERE app_id=? AND account_id=?', appId, principal.accountId);
          return { appId, accountId: principal.accountId, bound: Boolean(row), ...(row?.parent_id ? { parentAccountId: row.parent_id } : {}) };
        }
        if (op === 'referrals.balance.get') {
          keys(args, ['expectedAccountId', 'appId', 'currency']); const moneyCurrency = currency(args.currency);
          const amountMinor = get('SELECT amount FROM referral_balances WHERE app_id=? AND beneficiary_id=? AND currency=?', appId, principal.accountId, moneyCurrency)?.amount || '0';
          return { appId, currency: moneyCurrency, beneficiaryAccountId: principal.accountId, amountMinor };
        }
        keys(args, ['expectedAccountId', 'appId', 'cursor', 'limit'], ['expectedAccountId', 'appId']); const cursor = integer(args.cursor ?? 0), limit = integer(args.limit ?? 30, 1, 100);
        const rows = all('SELECT id,currency,event_kind,level,amount,created_at FROM referral_ledger WHERE app_id=? AND beneficiary_id=? AND id>? ORDER BY id LIMIT ?', appId, principal.accountId, cursor, limit + 1);
        const entries = rows.slice(0, limit).map(row => ({ entryId: row.id, currency: row.currency, kind: row.event_kind, level: row.level, amountMinor: row.amount, createdAt: row.created_at }));
        return { entries, ...(rows.length > limit ? { nextCursor: entries.at(-1).entryId } : {}) };
      }, { write: right === 'configure', beforeCommit: () => permission(principal, appId, right) });
    },
    internal: Object.freeze({
      readConfigReceipt({actor,appId,mutationId}) {
        id(appId);id(mutationId);permission(actor,appId,'configure');
        return transaction(()=>{
          permission(actor,appId,'configure');
          const receipt=get("SELECT result FROM referral_receipts WHERE app_id=? AND kind='config' AND event_id=?",appId,mutationId);
          return receipt?JSON.parse(receipt.result):null;
        },{write:false,beforeCommit:()=>permission(actor,appId,'configure')});
      },
      // Not a public operation. The host resolves the proof to authoritative event data.
      applyEvent({ context } = {}) {
        check(!closed, 'referrals_service_closed', 503); check(typeof readTrustedEvent === 'function', 'referrals_trusted_host_required', 403);
        const event = trustedEvent(sync(readTrustedEvent(context))), digest = hash(event);
        const verify = () => check(hash(trustedEvent(sync(readTrustedEvent(context)))) === digest, 'referrals_trusted_event_changed', 403);
        return transaction(() => {
          verify(); const previous = replay(event.appId, event.kind, event.eventId, digest); if (previous) return previous;
          const result = event.kind === 'attribution' ? attribution(event) : event.kind === 'payment' ? payment(event) : refund(event);
          return record(event.appId, event.kind, event.eventId, digest, result);
        }, { beforeCommit: verify });
      },
    }),
    close() { if (!closed) { check(!active && !db.isTransaction, 'referrals_nested_transaction'); statements.clear(); db.close(); closed = true; } },
  };
}
