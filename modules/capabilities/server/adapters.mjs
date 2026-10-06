import { assert, identifier, integer } from './validation.mjs';

export const NOTES_CREATE_DRAFT_CONTRACT = Object.freeze({
  capabilityId: 'notes.createDraft', version: 1,
  digest: '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204'
});

const PORTS = ['readiness', 'admit', 'get', 'beginAttempt', 'execute', 'reconcile', 'reconcilePage', 'verifyContext'];
const REQUIRED = ['readiness', 'admit', 'get', 'execute', 'reconcile'];

function data(value, fields, code) {
  assert(value && typeof value === 'object' && !Array.isArray(value)
    && [Object.prototype, null].includes(Object.getPrototypeOf(value)), code);
  const properties = Object.getOwnPropertyDescriptors(value);
  assert(Object.getOwnPropertySymbols(value).length === 0
    && Object.keys(properties).every(name => fields.includes(name)
      && properties[name].enumerable && Object.hasOwn(properties[name], 'value')), code);
  return Object.fromEntries(Object.entries(properties).map(([name, property]) => [name, property.value]));
}

function contract(input) {
  const value = data(input, ['capabilityId', 'version', 'digest'], 'adapter_contract_invalid');
  assert(Object.keys(value).length === 3, 'adapter_contract_invalid');
  identifier(value.capabilityId, 'adapter_contract_invalid'); integer(value.version, 1, 1000000, 'adapter_contract_invalid');
  assert(typeof value.digest === 'string' && /^[a-f0-9]{64}$/u.test(value.digest), 'adapter_contract_invalid');
  return Object.freeze(value);
}

function ports(input) {
  const value = data(input, PORTS, 'adapter_ports_invalid');
  assert(REQUIRED.every(name => typeof value[name] === 'function'), 'adapter_ports_invalid');
  for (const callback of Object.values(value)) assert(typeof callback === 'function', 'adapter_ports_invalid');
  // Capture exact trusted functions once. Preserve the coordinator's branded actors,
  // contexts, errors, transactions and reconciliation; do not reinterpret arguments.
  return Object.freeze(Object.fromEntries(Object.entries(value).map(([name, callback]) => [name, callback.bind(input)])));
}

/** Trusted in-process wiring only. JSON metadata never supplies executable ports. */
export function createTrustedAdapterRegistry(entries = []) {
  assert(Array.isArray(entries) && entries.length <= 128, 'adapter_registry_limit');
  const byKey = new Map();
  for (const input of entries) {
    const value = data(input, ['contract', 'adapter'], 'adapter_entry_invalid');
    assert(Object.keys(value).length === 2, 'adapter_entry_invalid');
    const pin = contract(value.contract), key = `${pin.capabilityId}@${pin.version}`;
    assert(!byKey.has(key), 'adapter_version_conflict');
    byKey.set(key, Object.freeze({ contract: pin, adapter: ports(value.adapter) }));
  }
  const contracts = Object.freeze([...byKey.values()].map(entry => entry.contract));
  return Object.freeze({
    get(reference) {
      const pin = contract(reference), entry = byKey.get(`${pin.capabilityId}@${pin.version}`);
      assert(entry && entry.contract.digest === pin.digest, 'adapter_not_registered');
      return entry;
    },
    listContracts() { return contracts; }
  });
}

/** Wrapper for the existing native coordinator; it creates no new effect path. */
export function createNativeNotesAdapter(nativeNotes) {
  const adapter = ports(nativeNotes);
  assert(PORTS.every(name => typeof adapter[name] === 'function'), 'adapter_ports_invalid');
  return Object.freeze({ contract: NOTES_CREATE_DRAFT_CONTRACT, adapter });
}
