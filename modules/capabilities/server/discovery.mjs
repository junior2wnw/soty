import { createHash } from 'node:crypto';
import { AccessError, assert, canonicalHash, canonicalJson, exact, freezeDeep, identifier, integer, text } from './validation.mjs';
import { BUILTIN_DOCUMENTATION, assertPublicStrings, preparePublicDocumentation } from './documentation.mjs';

export const DISCOVERY_LIMITS = Object.freeze({ versions: 128, queryCodeUnits: 200, queryBytes: 800, queryTokens: 12,
  defaultPage: 10, maxPage: 20, cursorBytes: 512, itemBytes: 4096, pageBytes: 64 * 1024, detailBytes: 384 * 1024 });
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const ordinal = (a, b) => a.capabilityId < b.capabilityId ? -1 : a.capabilityId > b.capabilityId ? 1 : a.version - b.version;
const keyFor = (id, version) => `${id}@${version}`;
const normalize = value => value.normalize('NFKC').toLowerCase();
const fatalUtf8 = new TextDecoder('utf-8', { fatal: true });

function linksFor(capability) {
  const id = encodeURIComponent(capability.capabilityId), version = capability.version;
  const detail = `/api/capabilities/v1/catalog/${id}/versions/${version}`;
  return Object.freeze({ html: `/agents/capabilities/${id}/versions/${version}`, detail, contract: `${detail}/contract.json`,
    inputSchema: `${detail}/schemas/input`, outputSchema: `${detail}/schemas/output` });
}

function serialized(value, maxBytes) {
  try { return canonicalJson(value, { maxBytes }); }
  catch { throw new AccessError('projection_too_large'); }
}

function queryView(value) {
  text(value, { min: 0, max: DISCOVERY_LIMITS.queryCodeUnits, code: 'query_invalid' });
  assert(value.isWellFormed() && Buffer.byteLength(value, 'utf8') <= DISCOVERY_LIMITS.queryBytes, 'query_invalid');
  const normalized = normalize(value).trim(), tokens = normalized ? normalized.split(/\s+/u) : [];
  assert(tokens.length <= DISCOVERY_LIMITS.queryTokens, 'query_invalid');
  return { normalized, tokens, queryHash: canonicalHash({ query: normalized }) };
}

function encodeCursor(revision, queryHash, offset) {
  const cursor = Buffer.from(canonicalJson({ revision, queryHash, offset }), 'utf8').toString('base64url');
  assert(cursor.length <= DISCOVERY_LIMITS.cursorBytes, 'projection_too_large');
  return cursor;
}

function decodeCursor(cursor, revision, queryHash, total) {
  try {
    assert(typeof cursor === 'string' && cursor.length > 0 && cursor.length <= DISCOVERY_LIMITS.cursorBytes
      && /^[A-Za-z0-9_-]+$/u.test(cursor), 'cursor_invalid');
    const bytes = Buffer.from(cursor, 'base64url');
    assert(bytes.toString('base64url') === cursor, 'cursor_invalid');
    const source = fatalUtf8.decode(bytes), value = JSON.parse(source);
    exact(value, ['revision', 'queryHash', 'offset'], 'cursor_invalid');
    assert(canonicalJson(value) === source && value.revision === revision && value.queryHash === queryHash, 'cursor_invalid');
    return integer(value.offset, 0, total, 'cursor_invalid');
  } catch { throw new AccessError('cursor_invalid'); }
}

/** Immutable public snapshot; no actor view, per-query cache, schema fetch or I/O. */
export function createPublicDiscovery(options) {
  exact(options, ['catalog', 'documentation'], 'discovery_invalid');
  const { catalog, documentation = BUILTIN_DOCUMENTATION } = options;
  assert(catalog && ['listPublic', 'validateInput', 'validateOutput'].every(name => typeof catalog[name] === 'function'), 'discovery_invalid');
  const candidates = catalog.listPublic();
  assert(Array.isArray(candidates), 'discovery_invalid');
  // Do not validate private strings, include them in hashes, or consult get()
  // while answering a public lookup. Even a known private ID is simply absent.
  const publicEntries = candidates.filter(entry => entry?.visibility === 'public').sort(ordinal);
  assert(publicEntries.length <= DISCOVERY_LIMITS.versions, 'discovery_invalid');
  assert(Array.isArray(documentation), 'documentation_invalid');
  const publicKeys = new Set(publicEntries.map(entry => keyFor(entry.capabilityId, entry.version))), relevantDocumentation = new Map();
  // One constructor pass. Unused private/unknown sidecars have no public count,
  // byte budget or validation outcome; only selected public records are kept.
  for (const item of documentation) {
    if (!item || typeof item.capabilityId !== 'string' || !Number.isSafeInteger(item.version)) continue;
    const key = keyFor(item.capabilityId, item.version);
    if (!publicKeys.has(key)) continue;
    assert(!relevantDocumentation.has(key), 'documentation_invalid');
    relevantDocumentation.set(key, item);
  }
  const byKey = new Map(), index = [];
  for (const capability of publicEntries) {
    identifier(capability.capabilityId); integer(capability.version, 1, 1000000);
    const key = keyFor(capability.capabilityId, capability.version);
    assert(!byKey.has(key), 'discovery_invalid');
    assertPublicStrings(capability);
    const { executionEnabled, digest, charges: _charges, ...contract } = capability;
    assert(typeof executionEnabled === 'boolean', 'discovery_invalid');
    const canonical = canonicalJson(contract);
    assert(hash(canonical) === digest, 'discovery_invalid');
    const prepared = preparePublicDocumentation({ catalog, capability, source: relevantDocumentation.get(key) });
    const links = linksFor(capability);
    const summary = freezeDeep({ capabilityId: capability.capabilityId, version: capability.version, appId: capability.appId,
      title: prepared.documentation.locales.ru.title, summary: prepared.documentation.locales.ru.summary, digest, executionEnabled, links });
    // id-prefix is the longest possible match enum; every item must fit before
    // any request is served, including IDs requiring percent-encoded segments.
    serialized({ ...summary, match: 'id-prefix' }, DISCOVERY_LIMITS.itemBytes);
    const detail = freezeDeep({ scope: 'public', capability, documentation: prepared.documentation, links });
    serialized(detail, DISCOVERY_LIMITS.detailBytes);
    byKey.set(key, { detail, contract: Object.freeze({ canonicalJson: canonical, digest }), summary });
    index.push({ capabilityId: capability.capabilityId, version: capability.version, id: normalize(capability.capabilityId),
      haystack: normalize([capability.capabilityId, capability.title, capability.description, prepared.searchText].join('\n')), summary });
  }
  const revision = canonicalHash(index.map(item => ({ capabilityId: item.capabilityId, version: item.version, digest: item.summary.digest,
    executionEnabled: item.summary.executionEnabled, documentationRevision: byKey.get(keyFor(item.capabilityId, item.version)).detail.documentation.revision })));
  function selected(args, additionalKeys = []) {
    exact(args, ['capabilityId', 'version', ...additionalKeys]);
    identifier(args.capabilityId); integer(args.version, 1, 1000000);
    const value = byKey.get(keyFor(args.capabilityId, args.version));
    assert(value, 'not_found'); return value;
  }
  return Object.freeze({
    get(args) { return selected(args).detail; },
    contract(args) { return selected(args).contract; },
    schema(args) {
      const value = selected(args, ['kind']);
      assert(args.kind === 'input' || args.kind === 'output');
      return value.detail.capability[`${args.kind}Schema`];
    },
    search(args = {}) {
      exact(args, ['query', 'limit', 'cursor']);
      const { query = '', limit = DISCOVERY_LIMITS.defaultPage, cursor } = args;
      integer(limit, 1, DISCOVERY_LIMITS.maxPage);
      const { normalized, tokens, queryHash } = queryView(query);
      const matching = index.filter(item => tokens.every(token => item.haystack.includes(token)))
        .map(item => ({ item, match: !tokens.length ? 'browse' : item.id === normalized ? 'exact-id' : item.id.startsWith(normalized) ? 'id-prefix' : 'text' }));
      const rank = { 'exact-id': 0, 'id-prefix': 1, text: 2, browse: 3 };
      matching.sort((a, b) => rank[a.match] - rank[b.match] || ordinal(a.item, b.item));
      const offset = cursor === undefined || cursor === null ? 0 : decodeCursor(cursor, revision, queryHash, matching.length);
      const items = [];
      const result = next => ({ scope: 'public', revision, items, total: matching.length,
        cursor: next < matching.length ? encodeCursor(revision, queryHash, next) : null });
      for (let next = offset; next < matching.length && items.length < limit; next++) {
        items.push({ ...matching[next].item.summary, match: matching[next].match });
        // The probe budget is larger only to measure the candidate page before
        // deciding whether the last item belongs on the following page.
        if (Buffer.byteLength(serialized(result(next + 1), DISCOVERY_LIMITS.detailBytes), 'utf8') > DISCOVERY_LIMITS.pageBytes) {
          items.pop(); break;
        }
      }
      assert(items.length > 0 || offset === matching.length, 'projection_too_large');
      const page = result(offset + items.length);
      serialized(page, DISCOVERY_LIMITS.pageBytes);
      return freezeDeep(page);
    },
  });
}
