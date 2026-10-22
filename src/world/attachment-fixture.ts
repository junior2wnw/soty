import { createConnectClient } from '../../modules/connect/browser/index.mjs';
import { mountWorldApp } from './app';
import type { WorldApi, WorldProfile, WorldDirectThread } from './types';
if (!import.meta.env.DEV || location.hostname !== '127.0.0.1') throw Error('Disposable localhost fixture only');
const info = await fetch('/fixture/info').then(response => response.json()) as { projectId: string; communityId: string; peerAccountId: string };
const client = createConnectClient({ projectId: info.projectId, endpoint: location.origin + '/api/connect/rpc', dbName: 'soty-world7-browser-alice', capturePlatformInvitation: false });
const account = await client.bootstrap('Аня · локально');
let failNextChunkAck = false, failNextChatAck = false;
document.addEventListener('soty-fixture-lose-chunk-ack', () => { failNextChunkAck = true; });
document.addEventListener('soty-fixture-lose-chat-ack', () => { failNextChatAck = true; });
const api: WorldApi = { async request<T>(op: string, args: Record<string, unknown> = {}): Promise<T> {
  if (op === 'apps.list' || op === 'apps.catalog') return { apps: [] } as T;
  if (op === 'notes.list') return { notes: [] } as T;
  const result = await client.extension<T>(op, args, { expectedAccountId: account.accountId });
  if (op === 'world.profile.get') (result as { profile: WorldProfile }).profile.directMessagesVersion = 1;
  if (op === 'world.attachment.chunk' && failNextChunkAck) { failNextChunkAck = false; throw Object.assign(Error('Synthetic lost chunk ACK'), { code: 'NETWORK_ERROR' }); }
  if ((op === 'world.chat.send' || op === 'world.direct.chat.send') && failNextChatAck) { failNextChatAck = false; throw Object.assign(Error('Synthetic lost message ACK'), { code: 'NETWORK_ERROR' }); }
  return result;
} };
await api.request('world.profile.get'); await api.request('world.membership.join', { communityId: info.communityId });
let selected = info.communityId;
if (new URLSearchParams(location.search).get('kind') === 'direct') {
  let contact = (await client.contacts()).contacts.find(value => value.peerAccountId === info.peerAccountId);
  if (!contact) {
    const card = await client.card();
    const request = await fetch('/fixture/contact', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ cardId: card.cardId }) }).then(response => response.json()) as { requestId: string };
    await client.acceptContact(request.requestId); contact = (await client.contacts()).contacts.find(value => value.peerAccountId === info.peerAccountId);
  }
  if (!contact) throw Error('Fixture contact missing');
  selected = (await api.request<{ current: WorldDirectThread }>('world.direct.open', { expectedAccountId: account.accountId, peerAccountId: info.peerAccountId, expectedRelationshipId: contact.relationshipId, requestId: 'browser_direct_' + crypto.randomUUID() })).current.threadId;
}
history.replaceState({}, '', location.pathname + location.search + '#messages/' + selected);
document.body.dataset.fixtureConversation = selected;
mountWorldApp(document.querySelector<HTMLElement>('#app')!, { api, localAccount: async () => ({ accountId: account.accountId, label: 'Аня · локально' }),
  openLegacy: () => {}, openAccount: () => {}, connectDevice: () => {}, agentCreate: () => {}, listApps: async () => [], listDevices: async () => [] });
