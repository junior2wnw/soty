import { createDescriptor } from '../app-contract/sdk.mjs';
import { check } from './server/validation.mjs';

export const REFERRAL_RIGHTS = Object.freeze({ read: 'referrals.read', configure: 'referrals.configure', internalCredit: 'trusted-host-only' });
const text = maxLength => ({ type: 'string', maxLength });
const number = (minimum = 0) => ({ type: 'integer', minimum, maximum: Number.MAX_SAFE_INTEGER });
const bool = { type: 'boolean' };
const object = (properties, required = Object.keys(properties)) => ({ type: 'object', properties, required, additionalProperties: false });
const rateArray = { type: 'array', items: { type: 'integer', minimum: 0, maximum: 10000 }, minItems: 1, maxItems: 10 };
const config = object({ appId: text(128), revision: number(), ratesBps: rateArray, rounding: text(120), payoutsEnabled: { ...bool, enum: [false] }, updatedAt: number(), replayed: bool }, ['appId', 'revision', 'ratesBps', 'rounding', 'payoutsEnabled', 'updatedAt']);
const base = { expectedAccountId: text(128), appId: text(128) };
const schemas = {
  'config.get': [object(base), config],
  'config.put': [object({ ...base, mutationId: text(128), expectedRevision: number(), ratesBps: rateArray }), config],
  'attribution.get': [object(base), object({ appId: text(128), accountId: text(128), bound: bool, parentAccountId: text(128) }, ['appId', 'accountId', 'bound'])],
  'balance.get': [object({ ...base, currency: text(3) }), object({ appId: text(128), currency: text(3), beneficiaryAccountId: text(128), amountMinor: text(32) })],
  'ledger.list': [object({ ...base, cursor: number(), limit: { type: 'integer', minimum: 1, maximum: 100 } }, Object.keys(base)), object({ entries: { type: 'array', maxItems: 100, items: object({ entryId: number(1), currency: text(3), kind: { ...text(12), enum: ['payment', 'refund'] }, level: number(1), amountMinor: number(-Number.MAX_SAFE_INTEGER), createdAt: number() }) }, nextCursor: number(1) }, ['entries'])],
};
export const REFERRAL_SCHEMAS=Object.freeze(schemas);

/** Metadata only. Host supplies real source/auth/resource/binding pins and enforces grants. */
export function createReferralDescriptor({ appId, namespace = 'soty.referrals', title = 'Реферальная система', source, auth, resource, bindings, docs = [], skills = [], feedback } = {}) {
  check(auth?.mode === 'shared-soty', 'referrals_shared_identity_required');
  check(bindings?.read && bindings?.configure && resource, 'referrals_descriptor_host_bindings_required');
  return createDescriptor({
    app: { id: appId, namespace, title, visibility: 'private', source, auth },
    capabilities: Object.entries(schemas).map(([op, [inputSchema, outputSchema]]) => {
      const configure = op === 'config.put';
      return { id: `${namespace}:${op}`, version: 1, title: `${configure ? 'Настроить' : 'Прочитать'} реферальную систему: ${op}`,
        description: configure ? 'Требуется referrals.configure и проверка владельца приложения. Меняет только будущие начисления; выплаты выключены.' : 'Требуется referrals.read для приложения. Личные начисления и привязка читаются только для текущего аккаунта.',
        inputSchema, outputSchema, resources: [resource], effects: [configure ? 'update' : 'read'], recipients: [resource], binding: bindings[configure ? 'configure' : 'read'] };
    }),
    docs, skills, feedback, reviews: { mode: 'disabled' },
  });
}
