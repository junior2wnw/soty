import test from 'node:test';
import assert from 'node:assert/strict';
import { extendCaddyfile } from './edge-config.mjs';

test('isolated app TLS adds a registry permission check and keeps existing site bytes intact', () => {
  const global = '\n\tadmin localhost:2019\n}\n', site = 'example.org {\n\treverse_proxy 127.0.0.1:9000\n}\n';
  const result = extendCaddyfile('{' + global + site);
  assert.ok(result.includes(global + site));
  assert.match(result, /ask http:\/\/127\.0\.0\.1:18182\/api\/apps\/tls-allow/);
  assert.match(result, /https:\/\/ \{/u);
  assert.match(result, /expression `host\('\*\.soty\.pochinit\.online'\)`/u);
  assert.throws(() => extendCaddyfile(result), /requires_review/);
});
test('existing TLS permission policies and unresolved service environment require explicit review', () => {
  for (const source of ['example.org {}', '{ on_demand_tls {} }', '{ }\n*.soty.pochinit.online {}', '{ }\nhttps:// {}', '{ email {$EMAIL} }']) assert.throws(() => extendCaddyfile(source), /requires_review/);
});
