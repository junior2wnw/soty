import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createSotyBffProtocol } from './soty-protocol';

test('maintained OIDC current-subject port distinguishes discovery/provider outage from revoked or mismatched subject', async (t) => {
  let discoveryStatus = 503,
    userStatus = 200,
    userSubject = 'synthetic_subject';
  let issuer = '';
  const server = createServer((req, res) => {
    res.setHeader('content-type', 'application/json');
    if (req.url?.includes('.well-known/openid-configuration')) {
      res.writeHead(discoveryStatus);
      res.end(
        JSON.stringify(
          discoveryStatus === 200
            ? {
                issuer,
                authorization_endpoint: issuer + '/authorize',
                token_endpoint: issuer + '/token',
                userinfo_endpoint: issuer + '/userinfo',
                jwks_uri: issuer + '/jwks',
                response_types_supported: ['code'],
                subject_types_supported: ['public'],
                id_token_signing_alg_values_supported: ['RS256'],
              }
            : { error: 'temporarily_unavailable' },
        ),
      );
      return;
    }
    if (req.url === '/human-identity/userinfo') {
      if (userStatus === 401) res.setHeader('www-authenticate', 'Bearer error="invalid_token"');
      res.writeHead(userStatus);
      res.end(
        JSON.stringify(
          userStatus === 200
            ? { sub: userSubject }
            : { error: userStatus === 401 ? 'invalid_token' : 'temporarily_unavailable' },
        ),
      );
      return;
    }
    res.writeHead(404);
    res.end('{}');
  });
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  const address = server.address();
  assert.ok(address && typeof address === 'object');
  issuer = `http://127.0.0.1:${address.port}/human-identity`;
  t.after(async () => {
    server.closeAllConnections();
    await new Promise<void>((done) => server.close(() => done()));
  });
  const port = createSotyBffProtocol({
    issuer,
    clientId: 'synthetic-client',
    clientSecret: 'a'.repeat(43),
    redirectUri: `http://localhost:${address.port}/api/embed/callback`,
  });
  const current = () => port.currentSubject('synthetic_fixture_token', 'synthetic_subject');
  await assert.rejects(
    current(),
    (error: any) => error.code === 'account_provider_unavailable' && error.status === 503,
  );
  discoveryStatus = 200;
  userStatus = 503;
  await assert.rejects(
    current(),
    (error: any) => error.code === 'account_provider_unavailable' && error.status === 503,
  );
  userStatus = 401;
  await assert.rejects(
    current(),
    (error: any) => error.code === 'authentication_required' && error.status === 401,
  );
  userStatus = 200;
  userSubject = 'another_subject';
  await assert.rejects(
    current(),
    (error: any) => error.code === 'authentication_required' && error.status === 401,
  );
  userSubject = 'synthetic_subject';
  assert.equal(await current(), 'synthetic_subject');
});
