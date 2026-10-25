import test from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { createSourceWorkspaceClient } from '../browser/index.mjs';
import { STANDARD_WORKSPACE_SOURCE } from '../server/standard-profile.mjs';
import { createOrdinaryInstalledFixture } from './support/ordinary-installed.mjs';

// Two existing Native apps, actual installed connector + signed Root/OIDC/BFF
// and Native SQLite. Only the browser fetch/cookie jar and consent gestures are
// supplied by this local fixture; no anonymous market or external model call.
test('the same browser SDK drives board and library contracts, preserves one effect after lost ACK, and refuses a moved child', { timeout: 120000 }, async t => {
  const f = await createOrdinaryInstalledFixture(t, { workspace: true, sourceProfile: STANDARD_WORKSPACE_SOURCE,
    connectorScript: fileURLToPath(new URL('../../../scripts/soty-connector.mjs', import.meta.url)) });
  let sequence = 0;
  const id = () => 'browser-consumer-' + (++sequence);
  const clients = [];
  for (const realm of f.realms) {
    await realm.launch(); const login = await f.authorize(realm); await f.complete(login.callbackUrl);
    const paths = [];
    const client = createSourceWorkspaceClient({ href: realm.embedded + '/embed', fetch: async (path, init) => {
      paths.push(path);
      const response = await f.wire(realm.embedded + path, { method: init.method, body: JSON.parse(init.body), headers: Object.fromEntries(new Headers(init.headers)) });
      return new Response(response.text, { status: response.status, headers: response.headers });
    } });
    clients.push({ realm, client, paths });
    const selected = await client.connect({ requestId: id() });
    assert.equal(selected.title, realm.realmId + '-one');
    assert.equal((await client.query({ requestId: id(), input: { operation: 'items.list' } })).items.length, 0);
  }
  const [board, library] = clients;
  // Minimal board consumer: its operation/fields are not built into the SDK.
  const boardIntent = await board.client.prepareInvoke({ requestId: id(), input: { operation: 'items.create', title: 'One local board record' } });
  board.realm.dropPath = '/api/embed/invoke';
  await assert.rejects(board.client.invoke(boardIntent), error => error.effectUnknown === true);
  const recovered = await board.client.invoke(boardIntent);
  assert.equal(recovered.outcome, 'committed'); assert.equal(recovered.workspaceReceipt.receipt.artifacts[0].type, 'ordinary-item');
  assert.equal(board.paths.filter(path => path === '/api/embed/invoke').length, 1);
  assert.equal(board.paths.at(-1), '/api/embed/receipt');
  assert.equal(board.realm.instance.store.db.prepare('SELECT COUNT(*) AS n FROM native_items').get().n, 1);
  assert.equal((await board.client.query({ requestId: id(), input: { operation: 'items.list' } })).items[0].title, 'One local board record');
  // A second minimal consumer uses a different Native schema and artifact.
  const bookIntent = await library.client.prepareInvoke({ requestId: id(), input: { operation: 'books.add', label: 'A library book', category: 'book' } });
  const book = await library.client.invoke(bookIntent);
  assert.equal(book.workspaceReceipt.receipt.artifacts[0].type, 'library-book');
  assert.equal(library.realm.instance.store.db.prepare('SELECT COUNT(*) AS n FROM native_items').get().n, 1);
  const old = await board.client.prepareInvoke({ requestId: id(), input: { operation: 'items.create', title: 'Stay on original child' } });
  await board.client.selectContext({ requestId: id(), nativeId: 'board-two', generation: 20 });
  await assert.rejects(board.client.invoke(old), /source_client_context_changed/);
  assert.equal(board.realm.instance.store.db.prepare('SELECT COUNT(*) AS n FROM native_items').get().n, 1);
  assert.equal((await board.client.query({ requestId: id(), input: { operation: 'items.list' } })).items.length, 0);
  for (const { paths, client } of clients) {
    assert.equal(paths.every(path => ['/api/embed/session-continue', '/api/embed/query', '/api/embed/invoke', '/api/embed/receipt'].includes(path)), true);
    client.dispose();
  }
});
