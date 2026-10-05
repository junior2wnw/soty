import { mountWorldApp } from './app';
import { el, button } from './dom';
import { createAppActions } from '../platform/local-apps';
import type { ConnectClient } from '../../modules/connect/browser/index.mjs';
import type { WorldProfile, WorldCommunity, WorldMessage, WorldAppRecord, WorldApi } from './types';
if (!import.meta.env.DEV)
    throw new Error('UI fixture is development-only');
const params = new URLSearchParams(location.search);
const palette = params.get('theme') === 'light' ? 'light' : 'dark';
localStorage.setItem('soty.world.ui.v1', JSON.stringify({ themeMode: palette, themeBrightness: 50, view: 'mine', presentation: 'field', homePresentation: 'list' }));
const self: WorldProfile = { profileId: 'qa-person-anna', displayName: 'Аня', bio: '', interests: [], avatarColor: '#a6b59a', revision: 1, discoverable: true };
const people: WorldProfile[] = [self, { ...self, profileId: 'qa-person-tim', displayName: 'Тим', avatarColor: '#c4b0d8' }, { ...self, profileId: 'qa-person-mira', displayName: 'Мира', avatarColor: '#dba99b' }];
const groups: WorldCommunity[] = [
    { communityId: 'qa-studio', name: 'Студия', description: 'Место для наших идей', topics: ['творчество'], joinPolicy: 'invite', showcase: 'Придумываем, собираем и выпускаем свои проекты.', symbol: 'cells', color: 'sage', revision: 1, memberCount: 3, previewMembers: people, membership: { state: 'active', role: 'owner', pinned: true, muted: false, showInProfile: true, revision: 1 }, permissions: { canManage: true, canModerate: true, canWrite: true }, unreadCount: 0 },
    { communityId: 'qa-evening', name: 'После работы', description: 'Музыка и хорошая компания', topics: ['музыка', 'игры'], joinPolicy: 'open', showcase: 'Музыка, одна партия и люди, с которыми хорошо.', symbol: 'music', color: 'honey', revision: 1, memberCount: 3, previewMembers: people, membership: { state: 'active', role: 'member', pinned: false, muted: false, showInProfile: true, revision: 1 }, permissions: { canManage: false, canModerate: false, canWrite: true }, unreadCount: 2 },
    { communityId: 'qa-makers', name: 'Мастерская', description: 'Воплощаем идеи', topics: ['идеи'], joinPolicy: 'open', showcase: 'Обмениваемся идеями и делаем полезные вещи.', symbol: 'tools', color: 'coral', revision: 1, memberCount: 2, previewMembers: people.slice(1), membership: { state: 'active', role: 'member', pinned: false, muted: false, showInProfile: false, revision: 1 }, permissions: { canManage: false, canModerate: false, canWrite: true }, unreadCount: 0 }
];
const apps: WorldAppRecord[] = [
    { appId: 'app-11111111111111111111111111111111', name: 'Тавыш', description: 'Музыка, в которой участвуешь', coverKey: 'tavysh', symbol: 'music', status: 'ready', ownerAccountId: self.profileId, communityId: 'qa-evening', grants: { accountIds: [], communityIds: ['qa-evening'] } },
    { appId: 'app-22222222222222222222222222222222', name: 'HIVE', description: 'Идеи становятся связями', coverKey: 'hive', symbol: 'cells', status: 'ready', ownerAccountId: self.profileId, communityId: 'qa-studio', grants: { accountIds: [], communityIds: ['qa-studio'] } },
    { appId: 'app-33333333333333333333333333333333', name: 'Фокус', description: 'Место для одной задачи', coverKey: 'focus', symbol: 'activity', status: 'ready', communityId: 'qa-evening' },
    { appId: 'app-44444444444444444444444444444444', name: 'Pulse', description: 'Главное в ваших цифрах', coverKey: 'pulse', symbol: 'activity', status: 'ready', ownerAccountId: self.profileId, communityId: 'qa-studio' }
];
const now = Date.now();
const history: Record<string, WorldMessage[]> = {};
for (const group of groups) {
    history[group.communityId] = params.has('history') ? Array.from({ length: 130 }, (_, index) => ({ messageId: `qa-msg-${group.communityId}-${index + 1}`, seq: index + 1, text: index === 127 ? 'Этот трек оставим?' : index === 128 ? 'Да, звучит отлично.' : index === 129 ? 'Добавила ещё один.' : `Идея ${index + 1}: давайте соберём всё в одном месте.`, author: people[index % 3]!, createdAt: now - (130 - index) * 70000 - (index < 65 ? 86400000 : 0), replyTo: null, removed: false })) : [
        { messageId: `qa-msg-${group.communityId}-1`, seq: 1, text: 'Этот трек оставим?', author: people[0]!, createdAt: now - 180000, replyTo: null, removed: false },
        { messageId: `qa-msg-${group.communityId}-2`, seq: 2, text: 'Да, звучит отлично.', author: people[1]!, createdAt: now - 120000, replyTo: null, removed: false },
        { messageId: `qa-msg-${group.communityId}-3`, seq: 3, text: 'Добавила ещё один.', author: people[2]!, createdAt: now - 60000, replyTo: null, removed: false }
    ];
}
const notes: Record<string, Record<string, unknown>> = {};
const proposal = { schema: 'soty.local-app.v1', name: 'Покупки', port: 5324, entryPath: '/', sourceJobId: 'qa-agent-task' };
let draftRegistered = false;
const api: WorldApi = { async request<T>(op: string, args: Record<string, unknown> = {}): Promise<T> {
        let result: unknown;
        const group = groups.find(item => item.communityId === args.communityId) || groups[0]!;
        switch (op) {
            case 'world.profile.get':
                result = { profile: self };
                break;
            case 'world.profile.update':
                Object.assign(self, args, { revision: self.revision + 1 });
                result = { profile: self };
                break;
            case 'world.profile.view':
                result = { profile: people.find(item => item.profileId === args.profileId) || self, communities: groups, canRequestContact: true };
                break;
            case 'world.community.list':
                result = { communities: groups };
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
            case 'apps.list':
                result = { apps: apps.map(app => ({ id: app.appId, name: app.name, hostDeviceId: 'qa-notebook', connectorId: 'qa-connector', ownerAccountId: self.profileId, state: 'ready', port: app.name === 'Покупки' ? 5324 : 3000, entryPath: '/', grants: app.grants || { accountIds: [], communityIds: [] } })) };
                break;
            case 'apps.inspect': {
                const app = apps.find(value => value.appId === args.appId);
                if (!app) throw Object.assign(new Error('Приложение отсутствует в локальном примере'), { code: 'app_not_found' });
                result = { schema: 'soty.app-inspection.v1', checkedAt: Date.now(),
                    app: { id: app.appId, name: app.name, state: 'enabled', revision: 1, grants: app.grants || { accountIds: [], communityIds: [] } },
                    addresses: { revision: 1, claimOrigin: null, canonical: null, aliases: [], limits: { perApp: 8, perAccount: 80, usedByApp: 0, usedByAccount: 0 } },
                    publication: { policyEpoch: 1, launchPolicy: 'restricted', listed: false, activeDomainIds: [], activeTargetRevision: 1 },
                    source: { hostDeviceId: 'qa-notebook', connectorId: 'qa-connector', deviceName: 'Мой ноутбук', port: 5324, entryPath: '/', revision: 1, digest: 'a'.repeat(64), profile: 'soty.relay-restricted.v1', observation: { state: 'unknown', observedAt: null, freshUntil: null, evidence: null } },
                    actions: { canEdit: app.ownerAccountId === self.profileId, canPublish: false, canPreview: false, canReserveName: false } };
                break;
            }
            case 'apps.register':
                if (!draftRegistered) {
                    apps.push({ appId: 'qa-shopping', name: String(args.name || 'Покупки'), coverKey: 'notes', symbol: 'list', status: 'ready', ownerAccountId: self.profileId });
                    draftRegistered = true;
                }
                result = { app: { id: 'qa-shopping', name: 'Покупки', ownerAccountId: self.profileId, hostDeviceId: 'qa-notebook', connectorId: 'qa-connector', port: 5324, entryPath: '/', state: 'ready', grants: { accountIds: [], communityIds: [] } } };
                break;
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
                result = { launchUrl: `http://127.0.0.1:5324/?app=${encodeURIComponent(String(args.appId))}` };
                break;
            case 'apps.agent.create':
                if (args.expectedAccountId !== self.profileId || args.hostDeviceId !== 'qa-notebook' || args.connectorId !== 'qa-connector') throw Object.assign(new Error('Локальный пример: другой владелец или компьютер'), { code: 'authentication_required' });
                result = { job: { schema: 'soty.connector-job.v4', kind: 'agent', id: 'qa-agent-task', deviceId: 'qa-notebook', connectorId: '', attempts: 0, status: 'queued', result: null } };
                break;
            case 'apps.agent.read':
                result = { job: { schema: 'soty.connector-job.v4', kind: 'agent', id: 'qa-agent-task', deviceId: 'qa-notebook', connectorId: 'qa-connector', attempts: 1, status: 'succeeded', result: { text: 'Черновик готов. Проверьте, как всё работает.', appProposal: proposal } }, done: true, cursor: 1, events: [{ seq: 1, type: 'progress', text: 'Черновик готов' }] };
                break;
            case 'apps.agent.cancel':
                result = { job: { schema: 'soty.connector-job.v4', kind: 'agent', id: 'qa-agent-task', deviceId: 'qa-notebook', connectorId: 'qa-connector', attempts: 1, status: 'cancelled', result: null }, done: true };
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
const fakeClient = { getLocalState: async () => ({ accountId: self.profileId, label: self.displayName }), extension: <T>(op: string, args?: Record<string, unknown>) => api.request<T>(op, args) };
const action = createAppActions(fakeClient as unknown as ConnectClient, async () => { }, undefined, { readAgentCapabilities: async () => ({ agentConfigured: true }) });
const toast = (text: string) => { const dialog = document.createElement('dialog'); dialog.className = 'sw-dialog'; dialog.append(el('p', '', text), button('Закрыть', undefined, '', () => dialog.close())); document.body.append(dialog); dialog.showModal(); dialog.addEventListener('close', () => dialog.remove(), { once: true }); };
mountWorldApp(document.querySelector<HTMLElement>('#app')!, { api, localAccount: async () => ({ accountId: self.profileId, label: self.displayName }), listApps: async (communityId) => apps.filter(app => !communityId || app.communityId === communityId), listDevices: async () => [{ deviceId: 'qa-notebook', label: 'Мой ноутбук', state: 'online' }], openLegacy: tool => toast(`Проверка оболочки: ${tool || 'Инструменты'}`), openAccount: () => toast('Профиль: локальный пример'), connectDevice: () => toast('Устройство: локальный пример'), agentCreate: action.agentCreate, openAppBuilder: host => action.mountAppBuilder(host) });
const marker = el('aside', 'sx-fixture-badge', 'Примеры · локальная проверка');
marker.setAttribute('aria-label', 'Изолированный UI пример. Данные вымышленные, операции локальные.');
document.body.append(marker);
const style = el('style');
style.textContent = '.sx-fixture-badge{position:fixed;bottom:3px;right:8px;z-index:1000;padding:2px 6px;border-radius:5px;background:var(--sw-bg);color:var(--sw-muted);font:9px system-ui,sans-serif;pointer-events:none;opacity:.8}';
document.head.append(style);
