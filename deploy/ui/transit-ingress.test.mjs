import test from 'node:test';
import assert from 'node:assert/strict';
import { prepareTransitSubscriptionIngress, prepareTransitSubscriptionReferrer } from './transit-ingress.mjs';

const expression = 'path("/", "/assets/*", "/api/*") || (method("GET", "HEAD") && path_regexp("^/index$"))';
const disk = Buffer.from('root.example {\n @root expression `' + expression + '`\n}\n');
function fixture(bytes, { host = '4-2.xn--p1ai', drift = false, twice = false } = {}) {
  const expr = /`([^`]+)`/u.exec(bytes.toString())[1];
  const route = { match: [{ expression: { expr, name: 'root' } }], handle: [{ handler: 'reverse_proxy', upstreams: [{ dial: '127.0.0.1:18182' }] }] };
  return { logging: { logs: { default: { encoder: { format: 'filter' } } } }, apps: { http: { servers: { main: {
    routes: [{ match: [{ host: [host] }], handle: [{ handler: 'subroute', routes: [route, ...(twice ? [structuredClone(route)] : [])] }] }],
    ...(drift ? { changed: true } : {}),
  } } } } };
}
test('adds only the capability path to the existing Root expression', async () => {
  const result = await prepareTransitSubscriptionIngress({ currentDisk: disk, adapt: async bytes => fixture(bytes) });
  assert.equal(result.addedPath, '/transit/subscriptions/*');
  assert.match(result.candidate.toString(), /"\/api\/\*", "\/transit\/subscriptions\/\*"/u);
  assert.equal(result.changedPaths.length, 1); assert.equal(result.otherConfigurationIdentical, true);
  assert.deepEqual(result.before.logging, result.after.logging);
});
test('refuses another host or ambiguous Root boundaries', async () => {
  for (const options of [{ host: 'elsewhere.example' }, { twice: true }])
    await assert.rejects(prepareTransitSubscriptionIngress({ currentDisk: disk, adapt: async bytes => fixture(bytes, options) }), { code: 'transit_ingress_boundary_ambiguous' });
});
test('refuses repeated publication and unrelated adaptation drift', async () => {
  const first = await prepareTransitSubscriptionIngress({ currentDisk: disk, adapt: async bytes => fixture(bytes) });
  await assert.rejects(prepareTransitSubscriptionIngress({ currentDisk: first.candidate, adapt: async bytes => fixture(bytes) }), { code: 'transit_ingress_already_configured' });
  await assert.rejects(prepareTransitSubscriptionIngress({ currentDisk: disk, adapt: async bytes => fixture(bytes, { drift: bytes.toString().includes('/transit/') }) }), { code: 'ui_structure_changed' });
});
test('refuses disk ambiguity, missing API boundary and config substitution', async () => {
  for (const bytes of [Buffer.concat([disk, disk]), Buffer.from(disk.toString().replace('"/api/*"', '"/unrelated/*"')), Buffer.from(disk.toString() + '{$UNREVIEWED}')])
    await assert.rejects(prepareTransitSubscriptionIngress({ currentDisk: bytes, adapt: async value => fixture(value) }));
});
test('scopes deferred privacy before Caddy coalesced headers/encode/subroute without changing its chain', async () => {
  const scopedExpression=expression.replace('"/api/*"','"/api/*", "/transit/subscriptions/*"');
  const input=Buffer.from('root.example {\n header {\n  Referrer-Policy "strict-origin-when-cross-origin"\n  defer\n }\n @root expression `'+scopedExpression+'`\n}\n');
  const global={handle:[{handler:'headers',response:{set:{'Referrer-Policy':['strict-origin-when-cross-origin']},deferred:true}},{handler:'encode'},{handler:'subroute',routes:[]}]};
  const preceding={handle:[{handler:'headers',response:{delete:['X-Frame-Options']}}],match:[{not:[{path:['/embed/*']}]}]};
  const adapt=async bytes=>({apps:{http:{servers:{main:{routes:[{match:[{host:['4-2.xn--p1ai']}],handle:[{handler:'subroute',routes:[
    ...(bytes.toString().includes('@transit_subscription_policy')?[{match:[{path:['/transit/subscriptions/*']}],handle:[{handler:'headers',response:{set:{'Referrer-Policy':['no-referrer']},deferred:true}}]}]:[]),structuredClone(preceding),structuredClone(global)]}]}]}}}}});
  const result=await prepareTransitSubscriptionReferrer({currentDisk:input,adapt});
  const routes=result.after.apps.http.servers.main.routes[0].handle[0].routes;
  assert.deepEqual(routes[1],preceding);assert.deepEqual(routes[2],global);assert.deepEqual(routes[0].match,[{path:['/transit/subscriptions/*']}]);
  await assert.rejects(prepareTransitSubscriptionReferrer({currentDisk:result.candidate,adapt}),{code:'transit_referrer_already_configured'});
});
