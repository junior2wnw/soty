import { fieldEntityKey, validateFieldEntity } from '../../modules/field/contract.mjs';
import type { FieldDocument, FieldEntityRef as FieldEntity } from '../../modules/field/contract.mjs';
import type { WorldCommunity, WorldProfile } from './types';

export interface DirectoryApi { request<T>(operation: string, args?: Record<string, unknown>): Promise<T> }
export interface DirectoryEntity {
  entity: FieldEntity; title: string; description?: string; symbol?: string; color?: string; avatarUrl?: string; avatarRevision?: number | null; coverKey?: string;
  source: 'public' | 'member' | 'owner' | 'builtin'; updatedAt?: number; unreadCount?: number; online?: boolean;
}
export interface DirectoryApp {
  id: string; name: string; ownerAccountId: string; state: 'ready' | 'starting' | 'stopped' | 'offline';
  createdAt: number; updatedAt: number; access: 'owner' | 'granted' | 'public'; canManage: boolean;
  entry: { appId: string; domainId: string; origin: string; path: string };
}
export interface DirectoryDevice { hostDeviceId: string; connectorId: string; name: string; online: boolean; claimed: boolean; bindingVersion?: number | null }
export interface DirectoryContact { kind: 'contact'; relationshipId: string; peerAccountId: string; label: string; createdAt: number }
export type DirectoryPerson = Pick<WorldProfile, 'profileId' | 'displayName' | 'avatarColor' | 'revision'> & Partial<WorldProfile>;
export type DirectoryRecord = WorldProfile | DirectoryPerson | WorldCommunity | DirectoryApp | DirectoryDevice | DirectoryContact;
export interface DirectoryPage { items: DirectoryEntity[]; cursor: string | null; status: 'ready' | 'partial' | 'offline'; errors: { source: string; code: string }[] }
interface WorldPage { people: WorldProfile[]; communities: WorldCommunity[]; nextCursor: string | null }
interface AppPage { apps: DirectoryApp[]; nextCursor: string | null }
type DirectorySource = 'communities' | 'people' | 'apps' | 'contacts' | 'devices';
interface FederationCursor { version: 2; query: string; kinds: string[]; mode: 'mine' | 'search'; accountId: string;
  sources: Partial<Record<DirectorySource, { cursor: string | null; done: boolean }>>; next: number; errors: { source: string; code: string }[] }
const code = (error: unknown): string => String((error as { code?: unknown })?.code ?? 'directory_network_unavailable');
const fail = (errorCode: string): never => { throw Object.assign(new Error(errorCode), { code: errorCode }); };

/** Account-bound metadata is ephemeral. The durable document contains identities only. */
export function createFieldDirectory({ api, accountId, isCurrent = () => true }: { api: DirectoryApi; accountId: string; isCurrent?: () => boolean }) {
  let disposed = false;
  const records = new Map<string, DirectoryRecord>();
  const check = (): void => { if (disposed || !isCurrent()) { records.clear(); fail('directory_account_changed'); } };
  const remember = (ref: FieldEntity, value: DirectoryRecord): void => {
    check(); const key = fieldEntityKey(ref); records.delete(key); records.set(key, value);
    if (records.size > 512) records.delete(records.keys().next().value!);
  };
  const request = async <T>(operation: string, args: Record<string, unknown>): Promise<T> => {
    check(); const result = await api.request<T>(operation, operation.includes('.directory.')
      ? { ...args, expectedAccountId: accountId } : args); check(); return result;
  };
  const person = (value: DirectoryPerson, source: DirectoryEntity['source'] = 'public'): DirectoryEntity => {
    const entity: FieldEntity = { kind: 'person', id: value.profileId }; remember(entity, value);
    return { entity, title: value.displayName, ...(value.bio ? { description: value.bio } : {}), color: value.avatarColor, source,
      ...(value.avatarUrl ? { avatarUrl: value.avatarUrl } : {}), ...(value.avatarRevision !== undefined ? { avatarRevision: value.avatarRevision } : {}) };
  };
  const community = (value: WorldCommunity): DirectoryEntity => {
    const entity: FieldEntity = { kind: 'community', id: value.communityId }; remember(entity, value);
    const admitted = ['active', 'invited', 'requested'].includes(value.membership?.state ?? '');
    return { entity, title: value.name, description: value.description, symbol: value.symbol, color: value.color,
      source: admitted ? 'member' : 'public', ...(value.membership?.state === 'active' ? { unreadCount: value.unreadCount } : {}) };
  };
  const app = (value: DirectoryApp): DirectoryEntity => {
    const entity: FieldEntity = { kind: 'app', id: value.id }; remember(entity, value);
    return { entity, title: value.name, source: value.access === 'granted' ? 'member' : value.access,
      updatedAt: value.updatedAt };
  };
  async function devices(): Promise<DirectoryEntity[]> {
    const response = await request<{ devices: DirectoryDevice[] }>('apps.devices', {});
    const values: DirectoryEntity[] = [];
    for (const value of response.devices) {
      const material = new TextEncoder().encode(JSON.stringify([value.hostDeviceId, value.connectorId]));
      const bytes = await crypto.subtle.digest('SHA-256', material); check();
      const id = `device-${[...new Uint8Array(bytes)].map(byte => byte.toString(16).padStart(2, '0')).join('')}`;
      const entity: FieldEntity = { kind: 'device', id }; remember(entity, value);
      values.push({ entity, title: value.name, symbol: 'laptop', source: 'owner', online: value.online === true });
    }
    return values;
  }
  async function contacts(): Promise<{ item: DirectoryEntity; record: DirectoryContact }[]> {
    const response = await request<{ contacts: Omit<DirectoryContact, 'kind'>[] }>('contacts.list', {});
    const toRecord = (value: Omit<DirectoryContact, 'kind'>): DirectoryContact => ({ kind: 'contact', relationshipId: value.relationshipId,
      peerAccountId: value.peerAccountId, label: value.label, createdAt: value.createdAt });
    const current = new Map(response.contacts.map(value => [value.peerAccountId, value]));
    for (const [key, record] of records) if ('kind' in record && record.kind === 'contact') {
      const next = current.get(record.peerAccountId);
      if (!next) records.delete(key); else records.set(key, toRecord(next));
    }
    return response.contacts.map(value => {
      const record = toRecord(value);
      const entity: FieldEntity = { kind: 'person', id: value.peerAccountId };
      return { record, item: { entity, title: value.label, description: 'В контактах', source: 'member' as const } };
    });
  }
  const encode = (value: FederationCursor): string => JSON.stringify(value);
  function readCursor(value: string | undefined, context: Omit<FederationCursor, 'sources' | 'next' | 'errors'>): FederationCursor | null {
    if (!value) return null;
    if (value.length > 4096) fail('invalid_directory_cursor');
    let parsed: FederationCursor;
    try { parsed = JSON.parse(value) as FederationCursor; } catch { return fail('invalid_directory_cursor'); }
    if (parsed.version !== 2 || parsed.accountId !== accountId || parsed.query !== context.query || parsed.mode !== context.mode
      || JSON.stringify(parsed.kinds) !== JSON.stringify(context.kinds) || !parsed.sources || typeof parsed.sources !== 'object'
      || !Number.isSafeInteger(parsed.next) || parsed.next < 0 || parsed.next > 4
      || !Array.isArray(parsed.errors) || parsed.errors.length > 5 || parsed.errors.some(item => !item || typeof item.source !== 'string' || typeof item.code !== 'string' || item.code.length > 128)
      || Object.entries(parsed.sources).some(([key, item]) => !['communities', 'people', 'apps', 'contacts', 'devices'].includes(key)
        || !item || typeof item.done !== 'boolean' || !(item.cursor === null || typeof item.cursor === 'string'))) fail('invalid_directory_cursor');
    return parsed;
  }
  async function page({ query = '', kinds = ['app', 'person', 'community'], cursor, limit = 30, mode }:
    { query?: string; kinds?: string[]; cursor?: string; limit?: number; mode: 'mine' | 'search' }): Promise<DirectoryPage> {
    check();
    if (typeof query !== 'string' || query.length > 100 || /[\uD800-\uDFFF]/u.test(query) || !Number.isSafeInteger(limit) || limit < 1 || limit > 60
      || !Array.isArray(kinds) || !kinds.length || kinds.some(kind => !['app', 'person', 'community', 'device'].includes(kind))) fail('invalid_directory_query');
    const selected = [...new Set(kinds)].sort(), normalized = query.normalize('NFC').trim();
    const context = { version: 2 as const, accountId, query: normalized, kinds: selected, mode };
    const prior = readCursor(cursor, context);
    const sources: DirectorySource[] = [selected.includes('community') ? 'communities' : null, selected.includes('person') ? mode === 'mine' ? 'contacts' : 'people' : null,
      selected.includes('app') ? 'apps' : null, mode === 'mine' && selected.includes('device') ? 'devices' : null]
      .filter((value): value is DirectorySource => value !== null);
    const continuation: FederationCursor = prior ?? { ...context, sources: Object.fromEntries(sources.map(source => [source, { cursor: null, done: false }])), next: 0, errors: [] };
    if (Object.keys(continuation.sources).length !== sources.length || sources.some(source => !continuation.sources[source])) fail('invalid_directory_cursor');
    const items: DirectoryEntity[] = [], errors: DirectoryPage['errors'] = [...continuation.errors];
    const order = sources.map((_, index) => (continuation.next + index) % sources.length);
    for (let position = 0; position < order.length && items.length < limit; position++) {
      const index = order[position]!, source = sources[index]!, entry = continuation.sources[source]!;
      if (entry.done) continue;
      const remainingSources = order.slice(position).filter(next => !continuation.sources[sources[next]!]!.done).length;
      const budget = Math.max(1, Math.floor((limit - items.length) / remainingSources));
      const after = entry.cursor ?? undefined;
      try {
        if (source === 'communities' || source === 'people') {
          const result = await request<WorldPage>('world.directory.search', { query: normalized, kind: source, scope: mode === 'mine' ? 'mine' : 'all',
            limit: budget, ...(after ? { cursor: after } : {}) });
          items.push(...result.communities.map(community), ...result.people.map(value => person(value)));
          entry.cursor = result.nextCursor; entry.done = result.nextCursor === null;
        } else if (source === 'apps') {
          const result = await request<AppPage>('apps.directory.search', { query: normalized, scope: mode === 'mine' ? 'mine' : 'all',
            limit: budget, ...(after ? { cursor: after } : {}) });
          items.push(...result.apps.map(app)); entry.cursor = result.nextCursor; entry.done = result.nextCursor === null;
        } else if (source === 'contacts') {
          const values = await contacts();
          let priorContact: { createdAt: number; relationshipId: string } | null = null;
          if (after) { try { priorContact = JSON.parse(after) as { createdAt: number; relationshipId: string }; } catch { fail('invalid_directory_cursor'); } }
          if (priorContact && (!Number.isSafeInteger(priorContact.createdAt) || typeof priorContact.relationshipId !== 'string')) fail('invalid_directory_cursor');
          const available = values.filter(value => !priorContact || value.record.createdAt > priorContact.createdAt
            || value.record.createdAt === priorContact.createdAt && value.record.relationshipId > priorContact.relationshipId);
          const batch = available.slice(0, budget); for (const value of batch) remember(value.item.entity, value.record);
          items.push(...batch.map(value => value.item));
          entry.done = batch.length >= available.length;
          const last = batch.at(-1)?.record; entry.cursor = !entry.done && last ? JSON.stringify({ createdAt: last.createdAt, relationshipId: last.relationshipId }) : null;
        } else {
          // The devices operation itself enforces ownership. Device names never
          // enter public search or an app's public/member projection.
          const available = await devices();
          const offset = after === undefined ? 0 : Number(after);
          if (!Number.isSafeInteger(offset) || offset < 0) fail('invalid_directory_cursor');
          const batch = available.slice(offset, offset + budget); items.push(...batch);
          entry.done = offset + batch.length >= available.length; entry.cursor = entry.done ? null : String(offset + batch.length);
        }
      } catch (error) {
        check(); if (code(error) === 'invalid_directory_cursor' || /account|PROFILE_CHANGED/u.test(code(error))) throw error;
        errors.push({ source, code: code(error).slice(0, 128) }); continuation.errors = [...errors]; entry.done = true;
      }
      continuation.next = (index + 1) % sources.length;
    }
    check();
    return { items, cursor: sources.some(source => !continuation.sources[source]!.done) ? encode(continuation) : null,
      status: errors.length ? items.length ? 'partial' : 'offline' : 'ready', errors };
  }
  async function resolve(refs: FieldEntity[]): Promise<{ items: DirectoryEntity[]; unavailable: FieldEntity[]; errors: DirectoryPage['errors'] }> {
    check(); if (!Array.isArray(refs) || refs.length > 256) fail('invalid_directory_entities');
    const unique = [...new Map(refs.map(ref => { const entity = validateFieldEntity(ref); return [fieldEntityKey(entity), entity]; })).values()];
    const items: DirectoryEntity[] = [], unavailable: FieldEntity[] = [], errors: DirectoryPage['errors'] = [];
    for (const kind of ['world', 'apps'] as const) {
      const candidates = unique.filter(ref => kind === 'world' ? ['person', 'community'].includes(ref.kind) : ref.kind === 'app');
      const selected = candidates.filter(ref => {
        const valid = kind === 'apps' ? /^app-[a-f0-9]{32}$/u.test(ref.id) : /^[A-Za-z0-9_-]{3,128}$/u.test(ref.id);
        if (!valid) { records.delete(fieldEntityKey(ref)); unavailable.push(ref); }
        return valid;
      });
      for (let offset = 0; offset < selected.length; offset += 60) {
        const batch = selected.slice(offset, offset + 60);
        try {
          const result = await request<{ items: { ref: FieldEntity; available: boolean; person?: DirectoryPerson; community?: WorldCommunity; app?: DirectoryApp; access?: DirectoryEntity['source'] }[] }>(
            kind === 'world' ? 'world.directory.resolve' : 'apps.directory.resolve', kind === 'world' ? { entities: batch } : { appIds: batch.map(ref => ref.id) });
          for (const item of result.items) {
            if (!item.available) { records.delete(fieldEntityKey(item.ref)); unavailable.push(item.ref); }
            else if (item.person) items.push(person(item.person, item.access ?? 'public'));
            else if (item.community) items.push(community(item.community));
            else if (item.app) items.push(app(item.app));
          }
        } catch (error) { check(); errors.push({ source: kind, code: code(error) }); batch.forEach(ref => { records.delete(fieldEntityKey(ref)); unavailable.push(ref); }); }
      }
    }
    const ownDevices = unique.filter(ref => ref.kind === 'device');
    if (ownDevices.length) {
      try { const current = await devices(); for (const ref of ownDevices) {
        const item = current.find(value => fieldEntityKey(value.entity) === fieldEntityKey(ref));
        if (item) items.push(item); else { records.delete(fieldEntityKey(ref)); unavailable.push(ref); }
      } } catch (error) { check(); errors.push({ source: 'devices', code: code(error) }); ownDevices.forEach(ref => { records.delete(fieldEntityKey(ref)); unavailable.push(ref); }); }
    }
    const privateContacts = unavailable.filter(ref => ref.kind === 'person');
    if (privateContacts.length) {
      try {
        const activeContacts = await contacts();
        for (const ref of privateContacts) {
          const known = activeContacts.find(value => value.record.peerAccountId === ref.id);
          if (known) { remember(known.item.entity, known.record); items.push(known.item); unavailable.splice(unavailable.findIndex(value => fieldEntityKey(value) === fieldEntityKey(ref)), 1); }
        }
      } catch (error) { check(); errors.push({ source: 'contacts', code: code(error) }); }
    }
    check(); return { items, unavailable, errors };
  }
  return Object.freeze({
    search: (options: { query?: string; kinds?: string[]; cursor?: string; limit?: number } = {}) => page({ ...options, mode: 'search' }),
    async loadMine({ document, cursor, limit = 30 }: { document?: FieldDocument; cursor?: string; limit?: number } = {}): Promise<DirectoryPage> {
      const result = await page({ mode: 'mine', kinds: ['community', 'app', 'person', 'device'], limit, ...(cursor ? { cursor } : {}) });
      if (document && !cursor) {
        const resolved = await resolve(document.shortcuts.map(shortcut => shortcut.entity));
        const merged = new Map([...result.items, ...resolved.items].map(item => [fieldEntityKey(item.entity), item]));
        result.items = [...merged.values()]; result.errors.push(...resolved.errors);
        if (result.errors.length) result.status = result.items.length ? 'partial' : 'offline';
      }
      return result;
    },
    resolve,
    getRecord(ref: FieldEntity): DirectoryRecord | null { check(); return records.get(fieldEntityKey(ref)) ?? null; },
    dispose(): void { disposed = true; records.clear(); },
  });
}
