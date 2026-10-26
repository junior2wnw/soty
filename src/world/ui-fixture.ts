import { mountWorldApp } from './app';
import { createDirectFixture } from './ui-fixture-direct';
import { openConnectionHub } from '../platform/connection-hub';
import { parseConnectionIntent } from '../platform/connection-intent.mjs';
import { resolveFieldAppArt } from './field-art.mjs';
import { createFieldDocument, validateFieldDocument, type FieldDocument } from '../../modules/field/contract.mjs';
import { el, button } from './dom';
import { createAppActions } from '../platform/local-apps';
import { mountAssistant } from '../platform/assistant';
import type { ConnectClient } from '../../modules/connect/browser/index.mjs';
import type { WorldProfile, WorldCommunity, WorldMessage, WorldAppRecord, WorldApi, WorldAssistantMountContext } from './types';
if (!import.meta.env.DEV)
    throw new Error('UI fixture is development-only');
const params = new URLSearchParams(location.search);
const oneFieldFixture = params.get('layout') === 'one-field';
const feedbackAccessSpecimen = params.get('appFeedback');
// An explicit, development-only specimen for normal assistant composition.
// It never calls a model, a computer or the production capabilities endpoint.
const assistantSpecimen = params.get('assistant');
if (assistantSpecimen === 'personal' || assistantSpecimen === 'device') {
    const fixtureFetch = window.fetch.bind(window);
    window.fetch = (input, init) => {
        const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.href : input.url, location.href);
        if (url.origin === location.origin && url.pathname === '/api/apps/capabilities') return Promise.resolve(new Response(JSON.stringify({ agentConfigured: true, personalAgent: { configured: assistantSpecimen === 'personal' } }), { headers: { 'content-type': 'application/json' } }));
        return fixtureFetch(input, init);
    };
}
const palette = params.get('theme') === 'light' ? 'light' : 'dark';
localStorage.setItem('soty.world.ui.v1', JSON.stringify({ themeMode: palette, themeBrightness: 50, view: 'mine', presentation: 'field', homePresentation: 'list', ...(params.get('motion') === 'off' ? { motion: false } : {}) }));
const self: WorldProfile = { profileId: oneFieldFixture ? 'qa-onefield-anna' : 'qa-field4-anna', displayName: 'Аня', bio: '', interests: [], avatarColor: '#a6b59a', revision: 1, discoverable: true,
    ...(oneFieldFixture ? { audience: 'public' as const, audienceWriteVersion: 1 as const } : {}) };
const people: WorldProfile[] = [self, { ...self, profileId: 'qa-person-tim', displayName: 'Тим', avatarColor: '#c4b0d8' }, { ...self, profileId: 'qa-person-mira', displayName: 'Мира', avatarColor: '#dba99b' }, { ...self, profileId: 'qa-person-sasha', displayName: 'Саша', avatarColor: '#b3c6a8' }, { ...self, profileId: 'qa-person-kirill', displayName: 'Кирилл', avatarColor: '#c7b995' }];
for (const person of people.slice(1)) delete person.audienceWriteVersion;
const qaPortraits = ['/src/world/fixtures/portraits/qa-anya.ce799ce936e5.webp', '/src/world/fixtures/portraits/qa-tim.879f6cbb19af.webp', '/src/world/fixtures/portraits/qa-mira.9e9577679d25.webp', '/src/world/fixtures/portraits/qa-sasha.4ba368e9d3be.webp', '/src/world/fixtures/portraits/qa-kirill.067cbf835a8d.webp'];
for (const [index, person] of people.entries()) person.avatarUrl = qaPortraits[index]!;
const groups: WorldCommunity[] = [
    { communityId: 'qa-studio', name: 'Студия', description: 'Место для наших идей', topics: ['творчество'], joinPolicy: 'invite', showcase: 'Придумываем, собираем и выпускаем свои проекты.', symbol: 'cells', color: 'sage', revision: 1, memberCount: 3, previewMembers: people, membership: { state: 'active', role: 'owner', pinned: true, muted: false, showInProfile: true, revision: 1 }, permissions: { canManage: true, canModerate: true, canWrite: true }, unreadCount: 0 },
    { communityId: 'qa-evening', name: 'После работы', description: 'Музыка и хорошая компания', topics: ['музыка', 'игры'], joinPolicy: 'open', showcase: 'Музыка, одна партия и люди, с которыми хорошо.', symbol: 'music', color: 'honey', revision: 1, memberCount: 3, previewMembers: people, membership: { state: 'active', role: 'member', pinned: false, muted: false, showInProfile: true, revision: 1 }, permissions: { canManage: false, canModerate: false, canWrite: true }, unreadCount: 2 },
    { communityId: 'qa-makers', name: 'Мастерская', description: 'Воплощаем идеи', topics: ['идеи'], joinPolicy: 'open', showcase: 'Обмениваемся идеями и делаем полезные вещи.', symbol: 'tools', color: 'coral', revision: 1, memberCount: 2, previewMembers: people.slice(1), membership: { state: 'active', role: 'member', pinned: false, muted: false, showInProfile: false, revision: 1 }, permissions: { canManage: false, canModerate: false, canWrite: true }, unreadCount: 0 }
];
for (const group of groups) group.previewMembers = group.previewMembers.slice(0, group.memberCount);
const apps: WorldAppRecord[] = [
    { appId: 'app-11111111111111111111111111111111', name: 'Тавыш', description: 'Музыка, в которой участвуешь', coverKey: 'tavysh', symbol: 'music', status: 'ready', ownerAccountId: self.profileId, communityId: 'qa-evening', grants: { accountIds: [], communityIds: ['qa-evening'] } },
    { appId: 'app-22222222222222222222222222222222', name: 'HIVE', description: 'Идеи становятся связями', coverKey: 'hive', symbol: 'cells', status: 'ready', ownerAccountId: self.profileId, communityId: 'qa-studio', grants: { accountIds: [], communityIds: ['qa-studio'] } },
    { appId: 'app-33333333333333333333333333333333', name: 'Фокус', description: 'Место для одной задачи', coverKey: 'focus', symbol: 'activity', status: 'ready', communityId: 'qa-evening' },
    { appId: 'app-44444444444444444444444444444444', name: 'Pulse', description: 'Главное в ваших цифрах', coverKey: 'pulse', symbol: 'activity', status: 'ready', ownerAccountId: self.profileId, communityId: 'qa-studio' }
];
apps[2] = { ...apps[2]!, name: 'Canvas', coverKey: 'canvas', symbol: 'brush', communityId: 'qa-studio' };
// Explicit synthetic UI state only. Real authorization, receipt and SQLite
// behavior are checked against Connect/World/Apps, outside this specimen.
type FixtureAudience = 'private' | 'friends' | 'link' | 'public';
const fixtureAudienceKey = 'soty.qa.one-field.audiences.v1';
const profileReceipts = new Map<string, { body: string; receipt: Record<string, unknown> }>();
const publicationReceipts = new Map<string, { body: string; result: Record<string, unknown> }>();
const fixturePublications = new Map<string, { audience: FixtureAudience | null; policyEpoch: number; launchPolicy: 'restricted' | 'anyone'; listed: boolean; activeDomainIds: string[]; activeTargetRevision: number }>();
for (const app of apps) fixturePublications.set(app.appId, { audience: 'private', policyEpoch: 1, launchPolicy: 'restricted', listed: false, activeDomainIds: [], activeTargetRevision: 1 });
if (oneFieldFixture) {
    try {
        const raw = localStorage.getItem(fixtureAudienceKey);
        if (raw && raw.length < 64_000) {
            const value = JSON.parse(raw);
            if (['private', 'friends', 'link', 'public'].includes(value.profile?.audience) && Number.isSafeInteger(value.profile.revision) && value.profile.revision >= 1)
                Object.assign(self, value.profile, { profileId: self.profileId, audienceWriteVersion: 1 });
            for (const [key, entry] of Array.isArray(value.profileReceipts) ? value.profileReceipts.slice(-30) : []) profileReceipts.set(key, entry);
            for (const [key, entry] of Array.isArray(value.publicationReceipts) ? value.publicationReceipts.slice(-30) : []) publicationReceipts.set(key, entry);
            for (const [key, entry] of Array.isArray(value.publications) ? value.publications : []) if (fixturePublications.has(key) && (entry.audience === null || ['private', 'friends', 'link', 'public'].includes(entry.audience)) && Number.isSafeInteger(entry.policyEpoch) && entry.policyEpoch >= 1) fixturePublications.set(key, entry);
        }
    } catch { /* No production state or authority exists in this DEV fixture. */ }
}
const persistFixtureAudience = (): void => {
    localStorage.setItem(fixtureAudienceKey, JSON.stringify({ profile: { audience: self.audience, revision: self.revision, discoverable: self.discoverable }, profileReceipts: [...profileReceipts].slice(-30), publicationReceipts: [...publicationReceipts].slice(-30), publications: [...fixturePublications] }));
};
const preset = createFieldDocument();
preset.contexts = [{ contextId: 'studio', title: 'Студия', x: 0, y: 0 }, { contextId: 'close', title: 'Близкие', x: 640, y: -80 }, { contextId: 'evening', title: 'После работы', x: 520, y: 340 }];
const shortcut = (id: string, entity: { kind: 'app' | 'person' | 'builtin' | 'device'; id: string }, contextId: string, slot: [number, number]): void => { preset.shortcuts.push({ shortcutId: id, entity, contextId, slot }); };
shortcut('studio-hive', { kind: 'app', id: apps[1]!.appId }, 'studio', [0, 0]);
shortcut('studio-canvas', { kind: 'app', id: apps[2]!.appId }, 'studio', [-25, 19]);
shortcut('studio-pulse', { kind: 'app', id: apps[3]!.appId }, 'studio', [6, 19]);
shortcut('studio-anna', { kind: 'person', id: self.profileId }, 'studio', [-24, -3]);
shortcut('studio-tim', { kind: 'person', id: people[1]!.profileId }, 'studio', [29, 5]);
shortcut('close-notes', { kind: 'builtin', id: 'notes' }, 'close', [0, 0]);
shortcut('close-mira', { kind: 'person', id: people[2]!.profileId }, 'close', [-16, -1]);
shortcut('close-sasha', { kind: 'person', id: people[3]!.profileId }, 'close', [16, 0]);
shortcut('evening-music', { kind: 'app', id: apps[0]!.appId }, 'evening', [0, 0]);
shortcut('evening-chess', { kind: 'builtin', id: 'chess' }, 'evening', [19, 0]);
shortcut('evening-kirill', { kind: 'person', id: people[4]!.profileId }, 'evening', [35, -1]);
const sha = async (value: unknown): Promise<string> => [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(value))))].map(byte => byte.toString(16).padStart(2, '0')).join('');
const fixtureFieldKey = oneFieldFixture ? 'soty.qa.one-field.v1' : 'soty.qa.unified-field.v4';
let fixtureField: { revision: number; document: FieldDocument; contentHash: string; updatedAt: number } | null = null;
const fieldReceipts = new Map<string, { revision: number; contentHash: string; committedAt: number }>();
// A loopback-only peer keeps the real launch boundary in UI checks. These
// deterministic specimen tickets carry no account or production authority.
const fixtureRuntime = new URL(new URLSearchParams(location.search).get('appOrigin') ?? 'http://127.0.0.1:5324');
if (fixtureRuntime.protocol !== 'http:' || !['127.0.0.1', 'localhost'].includes(fixtureRuntime.hostname) || fixtureRuntime.origin === location.origin || fixtureRuntime.href !== fixtureRuntime.origin + '/') throw new Error('Invalid UI fixture peer');
const fixtureApp = (app: WorldAppRecord) => ({ id: app.appId, name: app.name, ownerAccountId: app.ownerAccountId ?? 'qa-peer-owner', state: 'ready', createdAt: 1, updatedAt: 1, access: app.ownerAccountId === self.profileId ? 'owner' : 'public', canManage: app.ownerAccountId === self.profileId, ...(oneFieldFixture ? { publication: { ...fixturePublications.get(app.appId), activeNamedAddressCount: 0 } } : {}), entry: { appId: app.appId, domainId: `dom_${app.appId.slice(4)}`, origin: fixtureRuntime.origin, path: '/' } });
const fixtureEntry = (args: Record<string, unknown>) => {
    if (args.expectedAccountId !== self.profileId) throw Object.assign(new Error('Fixture account changed'), { code: 'authentication_required' });
    const app = apps.find(value => value.appId === args.appId);
    if (!app) throw Object.assign(new Error('Fixture app absent'), { code: 'app_not_found' });
    const entry = fixtureApp(app).entry;
    if (args.domainId !== undefined && args.domainId !== entry.domainId) throw Object.assign(new Error('Fixture domain absent'), { code: 'app_domain_not_found' });
    return { ...entry, path: typeof args.path === 'string' ? args.path : entry.path };
};
let fixtureSavedRevision = 0;
const fixtureSaved = new Map<string, Record<string, unknown>>();
const fixtureSavedReceipts = new Map<string, { args: string; result: unknown }>();
const now = Date.now();
const directSpecimen = params.get('direct') === '1';
const fixtureDirect = createDirectFixture(self, people[1]!, directSpecimen);
const history: Record<string, WorldMessage[]> = {};
for (const group of groups) {
    history[group.communityId] = params.has('history') ? Array.from({ length: 130 }, (_, index) => ({ messageId: `qa-msg-${group.communityId}-${index + 1}`, seq: index + 1, text: index === 127 ? 'Этот трек оставим?' : index === 128 ? 'Да, звучит отлично.' : index === 129 ? 'Добавила ещё один.' : `Идея ${index + 1}: давайте соберём всё в одном месте.`, author: people[index % 3]!, createdAt: now - (130 - index) * 70000 - (index < 65 ? 86400000 : 0), replyTo: null, removed: false })) : [
        { messageId: `qa-msg-${group.communityId}-1`, seq: 1, text: 'Этот трек оставим?', author: people[0]!, createdAt: now - 180000, replyTo: null, removed: false },
        { messageId: `qa-msg-${group.communityId}-2`, seq: 2, text: 'Да, звучит отлично.', author: people[1]!, createdAt: now - 120000, replyTo: null, removed: false },
        { messageId: `qa-msg-${group.communityId}-3`, seq: 3, text: 'Добавила ещё один.', author: people[2]!, createdAt: now - 60000, replyTo: null, removed: false }
    ];
}
const notes: Record<string, Record<string, unknown>> = {};
const fixtureJobId = 'job_1234567890abcdef1234567890abcdef', fixtureBuiltAppId = 'app-1234567890abcdef1234567890abcdef';
const proposal = { schema: 'soty.local-app.v1', name: 'Покупки', port: 5324, entryPath: '/', sourceJobId: fixtureJobId };
let fixtureAutomaticJob = false, fixtureRegistrationCalls = 0;
Object.assign(window, { qaAutoBuilder: () => ({ automaticJob: fixtureAutomaticJob, browserRegistrationCalls: fixtureRegistrationCalls }) });
// The specimen registry survives HMR/reload just like its field, so a valid
// newly registered ref is not replaced with an unavailable tile during QA.
const fixtureRegistryKey = 'soty.qa.app-registry.v1';
interface FixtureRegistration { appId: string; name: string; port: number; entryPath: string; body: string }
let registrations: FixtureRegistration[] = [];
try {
    const raw = localStorage.getItem(fixtureRegistryKey);
    if (raw && raw.length < 64_000) {
        const parsed: unknown = JSON.parse(raw);
        if (Array.isArray(parsed) && parsed.length <= 30 && parsed.every(value => value && /^app-[a-f0-9]{32}$/.test(value.appId) && typeof value.name === 'string' && value.name.length <= 64 && Number.isInteger(value.port) && value.port >= 1024 && value.port <= 65535 && typeof value.entryPath === 'string' && value.entryPath.startsWith('/') && typeof value.body === 'string' && value.body.length < 2048)) registrations = parsed;
    }
} catch { /* The explicit development specimen has no production storage. */ }
const addRegisteredProjection = (value: FixtureRegistration): void => {
    apps.push({ appId: value.appId, name: value.name, coverKey: 'notes', symbol: 'list', status: 'ready', ownerAccountId: self.profileId });
};
registrations.forEach(addRegisteredProjection);
// This DEV-only specimen uses the real settings and Access components. Its
// queue and keys are synthetic, in memory, and never sent to any server.
const accessPrincipals: Record<string, unknown>[] = [], accessGrants: Record<string, unknown>[] = [];
const feedbackAccess = (op: string, args: Record<string, unknown>): unknown => {
    if (!['1', 'legacy'].includes(feedbackAccessSpecimen ?? '')) return undefined;
    if (op === 'apps.feedback.context') {
        const app = apps.find(value => value.appId === args.appId && value.ownerAccountId === self.profileId);
        if (feedbackAccessSpecimen !== '1' || !app || Object.keys(args).join(',') !== 'appId') throw Object.assign(new Error('Synthetic queue unavailable'), { code: 'access_denied' });
        return { readiness: 'ready', context: { appId: app.appId, installationId: 'fbi_' + app.appId.slice(4), canManage: true, ticketVisibility: 'reporter-and-support' } };
    }
    if (op === 'agent.sources.list') return { accountId: self.profileId, deviceId: 'qa-notebook', apps: [] };
    if (op === 'oauth.connections.list') return { connections: [], nextCursor: null };
    if (!op.startsWith('access.')) return undefined;
    if (args.expectedAccountId !== self.profileId) throw Object.assign(new Error('Synthetic account changed'), { code: 'account_mismatch' });
    if (op === 'access.principals.list') return { principals: structuredClone(accessPrincipals), cursor: null };
    if (op === 'access.grants.list') return { grants: structuredClone(accessGrants.filter(value => value.principalId === args.principalId)), cursor: null };
    if (op === 'access.events.list') return { events: [], cursor: null };
    if (op === 'access.invocations.list') return { invocations: [], nextCursor: null };
    if (feedbackAccessSpecimen !== '1') throw Object.assign(new Error('Synthetic legacy backend'), { code: 'capability_disabled' });
    if (op === 'access.principals.create') {
        const id = 'qa_principal_' + (accessPrincipals.length + 1);
        const value = { id, accountId: self.profileId, clientId: 'qa_client_' + id, label: args.label, state: 'active', createdAt: Date.now(), revokedAt: null };
        accessPrincipals.push(value); return { principal: structuredClone(value) };
    }
    if (op === 'access.grants.issue') {
        const principal = accessPrincipals.find(value => value.id === args.principalId); if (!principal) throw Error('Synthetic principal absent');
        const id = 'qa_grant_' + (accessGrants.length + 1), budget = args.budget as { unit: string; limit: number };
        const value = { ...args, id, accountId: self.profileId, clientId: principal.clientId, parentGrantId: null, rootGrantId: id, depth: 0,
            createdAt: Date.now(), revokedAt: null, budget: { ...budget, reserved: 0, spent: 0, remaining: budget.limit, uncertain: 0 } };
        accessGrants.push(value); return { grant: structuredClone(value) };
    }
    if (op === 'access.credentials.issue') return { token: 'soty_cap_' + 'q'.repeat(43), credential: { id: 'qa_credential', grantId: args.grantId, audience: args.audience, expiresAt: args.expiresAt } };
    if (op === 'access.grants.revoke') { const value = accessGrants.find(item => item.id === args.grantId); if (value) value.revokedAt = Date.now(); return { grant: structuredClone(value) }; }
    if (op === 'access.principals.revoke') { const value = accessPrincipals.find(item => item.id === args.principalId); if (value) { value.state = 'revoked'; value.revokedAt = Date.now(); } return { principal: structuredClone(value) }; }
    throw Object.assign(new Error('Synthetic access operation absent'), { code: 'fixture_unsupported' });
};
const api: WorldApi = { async request<T>(op: string, args: Record<string, unknown> = {}): Promise<T> {
        const access = feedbackAccess(op, args); if (access !== undefined) return access as T;
        const direct = fixtureDirect(op, args); if (direct !== undefined) return direct as T;
        let result: unknown;
        const group = groups.find(item => item.communityId === args.communityId) || groups[0]!;
        switch (op) {
            case 'voice.orb.status':
                if (Object.keys(args).length) throw Object.assign(new Error('В локальном примере нет голосовых сессий'), { code: 'fixture_unsupported' });
                result = { schema: 'soty.voice-orb-status.v1', configured: false, missingBinding: 'not_enabled', voiceModel: 'gpt-realtime-2.1', brainModel: 'zai-org/GLM-5.3-Flash' };
                break;
            case 'contacts.list': result = { contacts: directSpecimen ? [{ relationshipId:'rel_qa_tim1111111111111111',peerAccountId:people[1]!.profileId,label:people[1]!.displayName,createdAt:now }] : [], requests: { incoming: [], outgoing: [] }, blocked: [], invitations: [] }; break;
            case 'world.field.get': {
                if (!fixtureField) {
                    if (oneFieldFixture) {
                        preset.contexts = [{ contextId: 'personal', title: 'Личное', x: 0, y: 0 }]; preset.shortcuts = [];
                        const refs = [
                            { kind: 'app', id: apps[2]!.appId }, { kind: 'person', id: self.profileId },
                            { kind: 'builtin', id: 'notes' }, { kind: 'app', id: apps[1]!.appId },
                            { kind: 'device', id: `device-${await sha(['qa-notebook', 'qa-connector'])}` },
                            { kind: 'builtin', id: 'chess' }, { kind: 'builtin', id: 'assistant' },
                        ] as const;
                        const slots: [number, number][] = [[0, 0], [0, -21], [21, -21], [-21, 0], [21, 0], [-21, 21], [0, 21]];
                        refs.forEach((ref, index) => shortcut(`one-field-${index}`, ref, 'personal', slots[index]!));
                    } else if (!preset.shortcuts.some(value => value.shortcutId === 'studio-notebook')) shortcut('studio-notebook', { kind: 'device', id: `device-${await sha(['qa-notebook', 'qa-connector'])}` }, 'studio', [-17, 34]);
                    const saved = localStorage.getItem(fixtureFieldKey);
                    try { if (saved) { const value = JSON.parse(saved); fixtureField = { ...value, document: validateFieldDocument(value.document) }; } } catch { /* Isolated controlled fixture only. */ }
                    fixtureField ??= { revision: 1, document: preset, contentHash: await sha(preset), updatedAt: 1 };
                }
                result = structuredClone(fixtureField); break;
            }
            case 'world.field.put': {
                if (args.expectedAccountId !== self.profileId) throw Object.assign(new Error('Fixture account changed'), { code: 'field_account_changed' });
                const document = validateFieldDocument(args.document), contentHash = await sha(document), id = String(args.requestId), prior = fieldReceipts.get(id);
                if (prior) { if (prior.contentHash !== contentHash) throw Object.assign(new Error('Fixture intent changed'), { code: 'field_request_conflict' }); result = { replayed: true, receipt: prior, current: fixtureField }; break; }
                if (Number(args.expectedRevision) !== (fixtureField?.revision ?? 0)) throw Object.assign(new Error('Fixture conflict'), { code: 'field_revision_conflict' });
                const receipt = { revision: Number(args.expectedRevision) + 1, contentHash, committedAt: Date.now() }; fieldReceipts.set(id, receipt);
                fixtureField = { revision: receipt.revision, document, contentHash, updatedAt: receipt.committedAt }; localStorage.setItem(fixtureFieldKey, JSON.stringify(fixtureField)); result = { replayed: false, receipt, current: fixtureField }; break;
            }
            case 'world.directory.search': {
                const query = String(args.query ?? '').toLocaleLowerCase('ru'); result = { people: args.kind === 'communities' || args.scope === 'mine' ? [] : people.filter(person => person.displayName.toLocaleLowerCase('ru').includes(query)), communities: args.kind === 'people' ? [] : groups.filter(group => group.name.toLocaleLowerCase('ru').includes(query)), nextCursor: null }; break;
            }
            case 'world.directory.resolve': {
                result = { items: (args.entities as { kind: string; id: string }[]).map(ref => ref.kind === 'person' ? { ref, available: true, person: people.find(person => person.profileId === ref.id), access: 'public' } : { ref, available: true, community: groups.find(group => group.communityId === ref.id) }) }; break;
            }
            case 'apps.directory.search': {
                const query = String(args.query ?? '').toLocaleLowerCase('ru'); result = { apps: apps.filter(app => app.name.toLocaleLowerCase('ru').includes(query)).map(fixtureApp), nextCursor: null }; break;
            }
            case 'apps.directory.resolve': {
                result = { items: (args.appIds as string[]).map(appId => { const app = apps.find(value => value.appId === appId), ref = { kind: 'app', id: appId }; return app ? { ref, available: true, app: fixtureApp(app) } : { ref, available: false }; }) }; break;
            }
            case 'world.profile.get':
                result = { profile: self };
                break;
            case 'world.profile.update': {
                if (args.requestId !== undefined || args.expectedAccountId !== undefined) {
                    if (args.expectedAccountId !== self.profileId) throw Object.assign(new Error('Fixture account changed'), { code: 'profile_account_changed' });
                    const requestId = String(args.requestId), body = JSON.stringify(args), previous = profileReceipts.get(requestId);
                    if (previous) {
                        if (previous.body !== body) throw Object.assign(new Error('Fixture request conflict'), { code: 'profile_request_conflict' });
                        result = { profile: self, receipt: previous.receipt, replayed: true }; break;
                    }
                    if (args.expectedRevision !== self.revision) throw Object.assign(new Error('Fixture revision changed'), { code: 'revision_conflict' });
                    if (!['private', 'friends', 'link', 'public'].includes(String(args.audience))) throw Object.assign(new Error('Fixture audience invalid'), { code: 'invalid_choice' });
                    self.audience = args.audience as FixtureAudience; self.discoverable = self.audience === 'public'; self.revision++;
                    const receipt = { schema: 'soty.world-profile-audience.receipt.v1', requestId, accountId: self.profileId, audience: self.audience, expectedRevision: args.expectedRevision, acceptedRevision: self.revision };
                    profileReceipts.set(requestId, { body, receipt }); persistFixtureAudience(); result = { profile: self, receipt, replayed: false }; break;
                }
                Object.assign(self, args, { revision: self.revision + 1 }); result = { profile: self }; break;
            }
            case 'world.profile.view': {
                const profile = people.find(item => item.profileId === args.profileId);
                if (!profile) throw Object.assign(new Error('Fixture profile absent'), { code: 'profile_not_found' });
                result = { profile, communities: groups, canRequestContact: profile !== self }; break;
            }
            case 'world.community.list':
                result = { communities: params.get('chats') === 'empty' ? [] : groups };
                break;
            case 'world.community.get':
                result = { community: group };
                break;
            case 'world.discovery.search': {
                const q = String(args.query || '').toLocaleLowerCase('ru');
                const communityResult = groups.filter(item => item.name.toLocaleLowerCase('ru').includes(q));
                const personResult = people.filter(item => item.displayName.toLocaleLowerCase('ru').includes(q));
                result = { communities: args.kind === 'people' ? [] : communityResult, people: args.kind === 'communities' ? [] : personResult, nextCursor: null, totals: { communities: communityResult.length, people: personResult.length } };
                break;
            }
            case 'world.membership.preferences':
                if (group.membership) {
                    Object.assign(group.membership, args, { revision: group.membership.revision + 1 });
                }
                result = { community: group };
                break;
            case 'world.membership.list':
                result = { members: people.map(profile => ({ profile, ...group.membership })), nextCursor: null };
                break;
            case 'world.chat.list': {
                const list = history[group.communityId] || [];
                const limit = Number(args.limit || 60);
                const filtered = list.filter(msg => (args.before === undefined || msg.seq < Number(args.before)) && (args.after === undefined || msg.seq > Number(args.after)));
                const page = args.after === undefined ? filtered.slice(-limit) : filtered.slice(0, limit);
                result = { messages: page, hasMore: filtered.length > limit };
                break;
            }
            case 'world.chat.send': {
                const list = history[group.communityId] ||= [];
                let message = list.find(item => item.messageId === args.clientId);
                if (!message) {
                    message = { messageId: String(args.clientId), seq: (list.at(-1)?.seq || 0) + 1, text: String(args.text), author: self, createdAt: Date.now(), replyTo: typeof args.replyTo === 'string' ? args.replyTo : null, removed: false };
                    list.push(message);
                }
                result = { message };
                break;
            }
            case 'world.chat.remove': {
                const message = history[group.communityId]?.find(item => item.messageId === args.messageId);
                if (message) {
                    message.removed = true;
                    message.text = '';
                }
                result = { removed: true };
                break;
            }
            case 'world.chat.read':
                group.unreadCount = 0;
                result = { unreadCount: 0 };
                break;
            case 'apps.devices':
                result = { devices: [{ hostDeviceId: 'qa-notebook', connectorId: 'qa-connector', name: 'Мой ноутбук', online: true, claimed: true }] };
                break;
            case 'apps.assistant.history':
                result = { jobs: [], nextCursor: null };
                break;
            case 'apps.list':
                result = { apps: apps.map(app => ({ id: app.appId, name: app.name, hostDeviceId: 'qa-notebook', connectorId: 'qa-connector', ownerAccountId: self.profileId, state: 'ready', port: app.name === 'Покупки' ? 5324 : 3000, entryPath: '/', grants: app.grants || { accountIds: [], communityIds: [] } })) };
                break;
            case 'apps.inspect': {
                const app = apps.find(value => value.appId === args.appId);
                if (!app) throw Object.assign(new Error('Приложение отсутствует в локальном примере'), { code: 'app_not_found' });
                result = { schema: 'soty.app-inspection.v1', checkedAt: Date.now(),
                    app: { id: app.appId, name: app.name, state: 'enabled', revision: 1, grants: app.grants || { accountIds: [], communityIds: [] } },
                    addresses: { revision: 1, claimOrigin: null, canonical: null, aliases: [], limits: { perApp: 8, perAccount: 80, usedByApp: 0, usedByAccount: 0 } },
                    publication: oneFieldFixture ? fixturePublications.get(app.appId)! : { policyEpoch: 1, launchPolicy: 'restricted', listed: false, activeDomainIds: [], activeTargetRevision: 1 },
                    source: { hostDeviceId: 'qa-notebook', connectorId: 'qa-connector', deviceName: 'Мой ноутбук', port: 5324, entryPath: '/', revision: 1, digest: 'a'.repeat(64), profile: 'soty.relay-restricted.v1', observation: { state: 'unknown', observedAt: null, freshUntil: null, evidence: null } },
                    actions: { canEdit: app.ownerAccountId === self.profileId, canPublish: oneFieldFixture && app.ownerAccountId === self.profileId, canPreview: false, canReserveName: false } };
                break;
            }
            case 'apps.publication.update': {
                const app = apps.find(value => value.appId === args.appId), publication = app && fixturePublications.get(app.appId);
                if (!oneFieldFixture || !app || !publication || app.ownerAccountId !== self.profileId || args.expectedAccountId !== self.profileId) throw Object.assign(new Error('Fixture app authority absent'), { code: 'app_owner_required' });
                const requestId = String(args.requestId), key = app.appId + ':' + requestId, body = JSON.stringify(args), previous = publicationReceipts.get(key);
                if (previous) {
                    if (previous.body !== body) throw Object.assign(new Error('Fixture request conflict'), { code: 'app_publication_request_conflict' });
                    result = { ...previous.result, replayed: true, current: { ...publication, appId: app.appId } }; break;
                }
                if (args.expectedPolicyEpoch !== publication.policyEpoch || args.expectedTargetRevision !== 1) throw Object.assign(new Error('Fixture policy changed'), { code: 'app_policy_conflict' });
                if (!['private', 'friends', null].includes(args.audience as 'private' | 'friends' | null) || args.launchPolicy !== 'restricted' || args.listed !== false || !Array.isArray(args.activeDomainIds) || args.activeDomainIds.length) throw Object.assign(new Error('Локальный пример не выдаёт публичные адреса'), { code: 'app_publication_domain_unavailable' });
                Object.assign(publication, { audience: args.audience, policyEpoch: publication.policyEpoch + 1 });
                const requestKeyHash = [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(requestId)))].map(byte => byte.toString(16).padStart(2, '0')).join('');
                const receipt = { schema: 'soty.app-publication-receipt.v2', namespace: 'apps.publication.update.v2', appId: app.appId, requestKeyHash, ...publication, targetRevision: 1, targetDigest: 'a'.repeat(64), profile: 'soty.relay-restricted.v1', exposureAck: null, committedAt: Date.now() };
                const accepted = { requestId, replayed: false, receipt, current: { ...publication, appId: app.appId } };
                result = accepted; publicationReceipts.set(key, { body, result: structuredClone(accepted) }); persistFixtureAudience(); break;
            }
            case 'apps.register': {
                fixtureRegistrationCalls++;
                const body = JSON.stringify(args), existing = registrations.find(value => value.port === Number(args.port));
                if (existing && existing.body !== body) throw Object.assign(new Error('Fixture port already registered'), { code: 'app_port_already_registered' });
                const value = existing ?? { appId: `app-${crypto.randomUUID().replaceAll('-', '')}`, name: String(args.name || 'Покупки'), port: Number(args.port), entryPath: String(args.entryPath || '/'), body };
                if (!existing) { localStorage.setItem(fixtureRegistryKey, JSON.stringify([...registrations, value])); registrations.push(value); addRegisteredProjection(value); }
                result = { app: { id: value.appId, name: value.name, ownerAccountId: self.profileId, hostDeviceId: 'qa-notebook', connectorId: 'qa-connector', port: value.port, entryPath: value.entryPath, state: 'ready', grants: { accountIds: [], communityIds: [] } } };
                break;
            }
            case 'apps.update': {
                const app = apps.find(item => item.appId === args.appId);
                if (app) {
                    app.name = String(args.name || app.name);
                    if (args.grants)
                        app.grants = args.grants as NonNullable<WorldAppRecord['grants']>;
                }
                result = {};
                break;
            }
            case 'apps.launch':
                { const entry = fixtureEntry(args);
                result = { launchUrl: `${entry.origin}/_soty/boot?path=${encodeURIComponent(entry.path)}#${'A'.repeat(43)}`, entry }; }
                break;
            case 'apps.entry.get':
                result = { entry: fixtureEntry(args) };
                break;
            case 'apps.saved.get':
                fixtureEntry(args);
                result = { revision: fixtureSavedRevision, entry: fixtureSaved.get(String(args.appId)) ?? null };
                break;
            case 'apps.saved.set': {
                const entry = fixtureEntry(args), old = fixtureSavedReceipts.get(String(args.requestId));
                if (old) {
                    if (old.args !== JSON.stringify(args)) throw Object.assign(new Error('Fixture saved intent changed'), { code: 'apps_saved_request_conflict' });
                    result = { ...(old.result as object), replayed: true }; break;
                }
                if (args.expectedRevision !== fixtureSavedRevision) throw Object.assign(new Error('Fixture saved changed'), { code: 'apps_saved_revision_conflict' });
                fixtureSavedRevision++;
                if (args.saved) {
                    const app = apps.find(value => value.appId === entry.appId)!;
                    fixtureSaved.set(entry.appId, { ...entry, label: app.name, savedRevision: fixtureSavedRevision, updatedAt: Date.now(), current: { name: app.name, status: 'ready', canManage: true } });
                } else fixtureSaved.delete(entry.appId);
                result = { requestId: args.requestId, replayed: false, receipt: { appId: entry.appId, saved: args.saved, revision: fixtureSavedRevision, committedAt: Date.now() }, current: { revision: fixtureSavedRevision, entry: fixtureSaved.get(entry.appId) ?? null } };
                fixtureSavedReceipts.set(String(args.requestId), { args: JSON.stringify(args), result: structuredClone(result) }); break;
            }
            case 'apps.saved.list':
                if (args.expectedAccountId !== self.profileId) throw Object.assign(new Error('Fixture account changed'), { code: 'authentication_required' });
                result = { revision: fixtureSavedRevision, entries: [...fixtureSaved.values()].sort((a, b) => Number(b.savedRevision) - Number(a.savedRevision)).slice(0, 20), nextCursor: null };
                break;
            case 'apps.agent.create':
                if (args.expectedAccountId !== self.profileId || args.hostDeviceId !== 'qa-notebook' || args.connectorId !== 'qa-connector') throw Object.assign(new Error('Локальный пример: другой владелец или компьютер'), { code: 'authentication_required' });
                fixtureAutomaticJob = args.autoRegister === true;
                result = { job: { schema: 'soty.connector-job.v4', kind: 'agent', id: fixtureJobId, deviceId: 'qa-notebook', connectorId: '', attempts: 0, status: 'queued', result: null } };
                break;
            case 'apps.agent.read':
                if (fixtureAutomaticJob && !apps.some(app => app.appId === fixtureBuiltAppId)) {
                    const created = { appId: fixtureBuiltAppId, name: proposal.name, port: proposal.port, entryPath: proposal.entryPath, body: JSON.stringify({ autoJob: fixtureJobId }) };
                    localStorage.setItem(fixtureRegistryKey, JSON.stringify([...registrations, created]));
                    registrations.push(created);
                    addRegisteredProjection(created);
                    fixturePublications.set(fixtureBuiltAppId, { audience: 'private', policyEpoch: 1, launchPolicy: 'restricted', listed: false, activeDomainIds: [], activeTargetRevision: 1 });
                }
                result = { job: { schema: 'soty.connector-job.v4', kind: 'agent', id: fixtureJobId, deviceId: 'qa-notebook', connectorId: 'qa-connector', attempts: 1, status: 'succeeded', result: { text: 'Приложение готово.', appProposal: proposal } },
                    ...(fixtureAutomaticJob ? { task: { autoRegister: true }, autoRegistration: { state: 'ready', appId: fixtureBuiltAppId, appState: 'ready' } } : {}),
                    done: true, cursor: 1, events: [{ seq: 1, type: 'progress', text: 'Готово' }] };
                break;
            case 'apps.agent.cancel':
                result = { job: { schema: 'soty.connector-job.v4', kind: 'agent', id: fixtureJobId, deviceId: 'qa-notebook', connectorId: 'qa-connector', attempts: 1, status: 'cancelled', result: null }, done: true };
                break;
            case 'notes.list':
                result = { notes: Object.entries(notes).map(([noteId, note]) => ({ noteId, title: note.title, preview: note.body, pinned: false, updatedAt: now })), nextCursor: null, usage: { count: 0 } };
                break;
            case 'notes.get':
                result = { note: notes[String(args.noteId)] || { noteId: args.noteId, title: '', body: '', items: [], color: 'plain', pinned: false, state: 'active', revision: 0 } };
                break;
            case 'notes.put':
                notes[String(args.noteId)] = { ...args, revision: Number(args.expectedRevision || 0) + 1, updatedAt: Date.now() };
                result = { noteId: args.noteId, revision: Number(args.expectedRevision || 0) + 1, updatedAt: Date.now() };
                break;
            case 'world.avatar.batch':
                result = { avatars: [] };
                break;
            default: throw Object.assign(new Error('Действие вне локальной проверки'), { code: 'fixture_unsupported' });
        }
        return result as T;
    } };
const fakeClient = { getLocalState: async () => ({ accountId: self.profileId, label: self.displayName, deviceId: 'qa-notebook', localRevision: 1, current: { revoked: false } }),
  extension: <T>(op: string, args?: Record<string, unknown>) => op === 'card.get'
    ? Promise.resolve({ cardId: 'card_qa1111111111111111111111111111' } as T) : api.request<T>(op, args) };
const assistant = (host: HTMLElement, context?: WorldAssistantMountContext) => mountAssistant(host, { client: fakeClient as unknown as ConnectClient,
  acquireVoiceCapture: () => null, connectDevice: () => action.connectDevice(), createApp: () => action.agentCreate(), resumeApp: target => action.agentCreate(undefined, target) }, context);
const action = createAppActions(fakeClient as unknown as ConnectClient, async () => { }, undefined, {
    readAgentCapabilities: async () => ({ agentConfigured: true, automaticAppRegistration: { schema: 'soty.automatic-app-registration-capability.v1', available: true } }),
    openRegisteredApp: async (appId, accountId, continueIf) => {
        if (accountId !== self.profileId) throw Error('connection_entry_changed');
        const intent = parseConnectionIntent(`#app/${appId}`, location.origin);
        if (!intent || intent.kind !== 'application') throw Error('connection_route_invalid');
        await world.followConnection(intent, undefined, continueIf);
    },
});
const toast = (text: string) => { const dialog = document.createElement('dialog'); dialog.className = 'sw-dialog'; dialog.append(el('p', '', text), button('Закрыть', undefined, '', () => dialog.close())); document.body.append(dialog); dialog.showModal(); dialog.addEventListener('close', () => dialog.remove(), { once: true }); };
const openConnections = async (): Promise<void> => {
  const hub = openConnectionHub({ client: fakeClient as unknown as ConnectClient, origin: location.origin, isCurrent: () => true,
    onIntent: async intent => {
      if (intent.kind !== 'profile' && intent.kind !== 'community' && intent.kind !== 'application')
        throw Error('Подтверждение доступа проверяется отдельно. Здесь только локальный UI пример.');
      await world.followConnection(intent); hub.close();
    },
  });
  await hub.closed;
};
const world = mountWorldApp(document.querySelector<HTMLElement>('#app')!, { api, fieldArt: entity => resolveFieldAppArt({ ...(entity.entity.kind === 'app' ? { coverKey: apps.find(app => app.appId === entity.entity.id)?.coverKey ?? 'generic' } : entity.coverKey ? { coverKey: entity.coverKey } : {}) }), localAccount: async () => ({ accountId: self.profileId, label: self.displayName }), listApps: async (communityId) => apps.filter(app => !communityId || app.communityId === communityId), listDevices: async () => [{ deviceId: 'qa-notebook', label: 'Мой ноутбук', state: 'online' }], openLegacy: tool => toast(`Проверка оболочки: ${tool || 'Инструменты'}`), openAccount: () => toast('Профиль: локальный пример'), openConnections, connectDevice: action.connectDevice, agentCreate: action.agentCreate, openAppBuilder: host => action.mountAppBuilder(host), openAssistant: assistant,
    ...(['1', 'legacy'].includes(feedbackAccessSpecimen ?? '') ? { accessAvailability: async () => ({ notesCreateEnabled: false, audience: location.origin,
        appFeedbackEnabled: feedbackAccessSpecimen === '1', mcpAudience: feedbackAccessSpecimen === '1' ? location.origin + '/mcp' : null }) } : {}) });
const marker = el('aside', 'sx-fixture-badge', 'Примеры · локальная проверка');
marker.setAttribute('aria-label', 'Изолированный UI пример. Данные вымышленные, операции локальные.');
document.body.append(marker);
const style = el('style');
style.textContent = '.sx-fixture-badge{position:fixed;bottom:3px;right:8px;z-index:1000;padding:2px 6px;border-radius:5px;background:var(--sw-bg);color:var(--sw-muted);font:9px system-ui,sans-serif;pointer-events:none;opacity:.8}';
document.head.append(style);
