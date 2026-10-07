import test from 'node:test';
import assert from 'node:assert/strict';
import {createOrdinaryInstalledFixture} from './support/ordinary-installed.mjs';
import {sourceContinuationAck} from '../../apps/scoped-embed/source-continuation.mjs';

async function continuation(f,realm,id){return f.wire(realm.embedded+'/api/embed/session-continue',{body:{requestId:id}});}

test('actual installed Basic ACK reports only usable AT/Source/Root bound and stays non-renewable',{timeout:120000},async t=>{
  const f=await createOrdinaryInstalledFixture(t),[realm]=f.realms;
  await realm.launch();await f.login(realm);
  const row=realm.instance.store.db.prepare('SELECT id_hash,expires_at FROM source_sessions').get();
  const session=await realm.instance.store.storage.readSession(row.id_hash);
  // A Source session can lawfully end before its raw AT. The closed ACK must
  // remain usable by the real installed broker, rather than report the AT alone.
  realm.instance.store.tx(()=>realm.instance.store.db.prepare('UPDATE source_sessions SET expires_at=? WHERE id_hash=?').run(Date.now()+120000,row.id_hash));
  const reply=await continuation(f,realm,'actual-basic-bound-0001');assert.equal(reply.status,200);
  const ack=sourceContinuationAck(reply.value);assert.equal(ack.ready,true);assert.equal(ack.renewable,false);
  assert.equal(ack.accessExpiresAt,ack.sessionExpiresAt);assert.ok(ack.accessExpiresAt<=row.expires_at);
  assert.ok(ack.accessExpiresAt<=session.expiresAt);assert.ok(ack.accessExpiresAt<=Date.now()+121000);
  const rows=realm.instance.store.db.prepare('SELECT count(*) n FROM source_sessions').get().n;
  const repeated=await continuation(f,realm,'actual-basic-bound-0001');assert.equal(repeated.status,200);assert.equal(repeated.value.receiptDigest,reply.value.receiptDigest);
  assert.equal(realm.instance.store.db.prepare('SELECT count(*) n FROM source_sessions').get().n,rows,'ACK never creates a session');
  assert.equal(realm.instance.store.db.prepare('SELECT count(*) n FROM native_consents').get().n,1);
});

test('Basic ACK refuses new Root slot, expired Source deadline and revoked Native session without recovery mutations', {timeout:120000},async t=>{
  const f=await createOrdinaryInstalledFixture(t),[first,second]=f.realms;
  await first.launch();await f.login(first);
  const firstCounts=first.instance.store.db.prepare('SELECT count(*) n FROM source_sessions').get().n;
  await first.launch();assert.equal((await continuation(f,first,'basic-new-slot-0001')).status,401,'old Basic proof cannot authorize the new Root slot');
  assert.equal(first.instance.store.db.prepare('SELECT count(*) n FROM source_sessions').get().n,firstCounts);
  await second.launch();await f.login(second);
  second.instance.store.tx(()=>second.instance.store.db.prepare('UPDATE source_sessions SET expires_at=?').run(Date.now()-1));
  assert.equal((await continuation(f,second,'basic-expired-0001')).status,401);
  await second.launch();await f.login(second);
  second.instance.store.revokeNativeSession(second.nativeToken);
  assert.equal((await continuation(f,second,'basic-revoked-0001')).status,403);
  assert.equal(second.instance.store.db.prepare('SELECT count(*) n FROM source_sessions').get().n,2);
});
