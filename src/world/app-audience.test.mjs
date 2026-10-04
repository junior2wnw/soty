import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import * as audience from './app-audience.mjs';
import * as deployment from './app-deployment.mjs';

const owner = 'owner', grants = { accountIds: [], communityIds: [] };
const app = { appId: 'app-example', name: 'Project', status: 'offline', ownerAccountId: owner, grants,
  publication: { launchPolicy: 'anyone', activeNamedAddressCount: 2 } };
const compile = async filename => ts.transpileModule(await readFile(new URL(filename, import.meta.url), 'utf8'),
  { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const controllerCode = await compile('./app.ts'), cardCode = await compile('./application-card.ts');

class Element {
  children = []; attributes = {}; dataset = {}; open = true; isConnected = true;
  classList = { add() {} };
  constructor(tag, className = '', text = '') { Object.assign(this, { tag, className, textContent: text }); }
  append(...nodes) { this.children.push(...nodes); }
  addEventListener() {}
  setAttribute(name, value) { this.attributes[name] = value; }
  getAttribute(name) { return this.attributes[name] ?? null; }
  querySelector() { return null; }
  close() { this.open = false; }
}
const el = (...args) => new Element(...args);
const all = node => [node, ...node.children.flatMap(all)];
const ports = {
  './app-audience.mjs': audience,
  './app-deployment.mjs': deployment,
  './types': { worldColor: color => color },
  './dom': { el, button: (label, symbol, className) => el('button', className, label), iconButton: label => el('button', '', label) },
  './icons': { icon: symbol => Object.assign(el('svg'), { symbol }) },
};
function evaluate(code, extra = {}) {
  const module = { exports: {} };
  vm.runInNewContext(code, { module, exports: module.exports, require: name => ({ ...ports, ...extra })[name] ?? {},
    document: { activeElement: null }, location: { hash: '#mine' }, HTMLElement: Element, URLSearchParams });
  return module.exports;
}

test('audience separates named publication from direct grants, health, and unknown or foreign metadata', () => {
  const publicView = audience.describeAppAudience(app, owner);
  assert.equal(publicView.label, 'Именные ссылки: всем'); assert.equal(publicView.icon, 'external');
  assert.deepEqual(publicView.details, ['Именные ссылки: доступны всем.', 'Личные допуски: только вам.']);
  for (const status of ['ready', 'offline', 'stopped', 'starting']) assert.equal(audience.describeAppAudience({ ...app, status }, owner).publicNamed, true);
  for (const publication of [{ launchPolicy: 'anyone', activeNamedAddressCount: 0 }, { launchPolicy: 'restricted', activeNamedAddressCount: 2 }])
    assert.equal(audience.describeAppAudience({ ...app, publication }, owner).label, 'Личный доступ');
  assert.equal(audience.describeAppAudience({ ...app, status: 'revoked' }, owner).label, 'Доступ закрыт');
  for (const publication of [undefined, null, { launchPolicy: 'anyone', activeNamedAddressCount: -1 }, { launchPolicy: 'anyone', activeNamedAddressCount: '2' }])
    assert.equal(audience.describeAppAudience({ ...app, publication }, owner).label, 'Ваше приложение');
  assert.deepEqual(audience.describeAppAudience(app, 'reader'), { label: 'Вам доступно', icon: 'app', publicNamed: false, details: [] });
});

test('inspection summary excludes inactive, retired and non-selected aliases and all aliases of a closed app', () => {
  const snapshot = { app: { state: 'enabled' }, publication: { launchPolicy: 'anyone', activeDomainIds: ['a', 'b', 'c'] },
    addresses: { aliases: [{ id: 'a', active: true, state: 'bound' }, { id: 'b', active: false, state: 'bound' },
      { id: 'c', active: true, state: 'tombstone' }, { id: 'outside', active: true, state: 'bound' }] } };
  assert.deepEqual(audience.publicationFromInspection(snapshot), { launchPolicy: 'anyone', activeNamedAddressCount: 1 });
  snapshot.app.state = 'revoked'; assert.equal(audience.publicationFromInspection(snapshot).activeNamedAddressCount, 0);
});

test('actual controller settings update and subsequent list refresh use the same audience, including anyone with zero aliases', async () => {
  let settings;
  const { TestController } = evaluate(`${controllerCode}\nexports.TestController = WorldApplication;`, {
    './app-settings': { mountAppSettings: options => { settings = options; return { requestClose() {}, dispose() {} }; } },
  });
  const controller = Object.create(TestController.prototype);
  let projection = { id: app.appId, name: app.name, hostDeviceId: 'host', deviceName: 'Device', state: 'offline', ownerAccountId: owner,
    grants, publication: app.publication };
  const dialogs = [];
  Object.assign(controller, { deskAccount: owner, accountGeneration: 1, screenSequence: 1, destroyed: false, communities: [], apps: [], options: {},
    api: { request: async op => { if (op === 'apps.catalog') return { apps: [] }; assert.equal(op, 'apps.list'); return { apps: [projection] }; } },
    dialog() { const dialog = { element: el('dialog'), body: el('div'), close() {} }; dialogs.push(dialog); return dialog; } });
  controller.apps = await controller.loadApps();
  assert.equal(controller.apps[0].audience, 'Именные ссылки: всем');
  controller.openAppSettings(controller.apps[0]);
  for (const [launchPolicy, count] of [['anyone', 2], ['anyone', 0], ['restricted', 2]]) {
    const ids = Array.from({ length: count }, (_, i) => `domain-${i}`);
    settings.onChanged({ app: { name: app.name, state: 'enabled', grants }, source: { hostDeviceId: 'host', deviceName: 'Device', observation: { state: 'offline' } },
      publication: { launchPolicy, activeDomainIds: ids }, addresses: { aliases: ids.map(id => ({ id, active: true, state: 'bound' })) } });
    const locallyUpdated = controller.apps[0];
    projection = { ...projection, publication: { launchPolicy, activeNamedAddressCount: count } };
    const refreshed = (await controller.loadApps())[0];
    assert.equal(locallyUpdated.audience, refreshed.audience); assert.deepEqual(locallyUpdated.publication, refreshed.publication);
    assert.notEqual(refreshed.audience, 'Только вам');
  }
  controller.appStateLabel = () => 'Устройство не в сети';
  controller.inspectApplication({ ...controller.apps[0], publication: app.publication });
  const details = all(dialogs.at(-1).body).map(node => node.textContent);
  assert.ok(details.includes('Именные ссылки: доступны всем.')); assert.ok(details.includes('Личные допуски: только вам.'));
});

test('actual card shows public link glyph without lock and keeps independent community and chat controls', () => {
  const { createApplicationCard } = evaluate(cardCode);
  const options = { app: { ...app, audience: 'Только вам' }, accountId: owner, communities: [], pinned: false,
    open() {}, inspect() {}, openCommunity() {}, togglePin() { return true; } };
  const nodes = all(createApplicationCard(options));
  assert.ok(nodes.some(node => node.textContent === 'Именные ссылки: всем'));
  assert.ok(nodes.some(node => node.symbol === 'external')); assert.ok(!nodes.some(node => node.symbol === 'lock'));
  const group = { communityId: 'team', name: 'Actual community', membership: { state: 'active' } };
  const groupNodes = all(createApplicationCard({ ...options, app: { ...app, grants: { accountIds: [], communityIds: ['team'] } }, communities: [group] }));
  for (const label of ['Именные ссылки: всем', 'Actual community', 'Чат сообщества Actual community']) assert.ok(groupNodes.some(node => node.textContent === label), label);
});
