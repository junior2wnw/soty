'use strict';
// Optional local Chromium check: PLAYWRIGHT_MODULE may point to an installed
// playwright package; PLAYWRIGHT_CHANNEL may select an installed browser.
// Exercises the production consent template and CSP with
// synthetic POST -> resume -> cross-origin callback redirects, without login.
const { createServer } = require('node:http');
const { readFileSync } = require('node:fs');
const { resolve } = require('node:path');
const { createHash } = require('node:crypto');
const assert = require('node:assert/strict');
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const listen = server => new Promise(done => server.listen(0, '127.0.0.1', done));
const stop = server => { server.closeAllConnections(); return new Promise(done => server.close(done)); };

async function main() {
  const source = readFileSync(resolve(__dirname, '../auth.mjs'), 'utf8');
  const html = source.match(/res\.end\(`(<!doctype html>[^`]+)`\)/u)?.[1]
    .replace('${escape(client.clientName ?? client.clientId)}', 'Local regression client')
    .replace('${escape(details.params.redirect_uri)}', 'Registered callback')
    .replace('${escape(context.csrfToken)}', 'synthetic-only');
  const csp = source.match(/'content-security-policy': `([^`]+)`/u)?.[1];
  assert.ok(html && !html.includes('${'));
  assert.ok(csp?.endsWith("form-action 'self' ${redirectOrigin}"));
  let active;
  const callback = createServer((_req, res) => { active.callbackSeen = true; res.end('local callback'); });
  const foreign = createServer((_req, res) => { active.foreignSeen = true; res.end('foreign callback'); });
  await listen(callback); await listen(foreign);
  const callbackOrigin = `http://127.0.0.1:${callback.address().port}`;
  const foreignOrigin = `http://127.0.0.1:${foreign.address().port}`;
  const server = createServer(async (req, res) => {
    if (req.method === 'POST') {
      for await (const _chunk of req) { /* synthetic form only */ }
      active.submitSeen = true; active.originMatches = req.headers.origin === origin;
      res.writeHead(303, { location: '/oauth/resume' }); res.end(); return;
    }
    if (req.url === '/oauth/resume') {
      active.resumeSeen = true;
      res.writeHead(303, { location: (active.variant === 'foreign-callback' ? foreignOrigin : callbackOrigin) + '/callback' }); res.end(); return;
    }
    res.setHeader('content-type', 'text/html; charset=utf-8');
    res.setHeader('referrer-policy', 'no-referrer');
    res.setHeader('content-security-policy', active.variant === 'self-only' ? csp.replace(' ${redirectOrigin}', '')
      : csp.replace('${redirectOrigin}', callbackOrigin));
    res.end(html);
  });
  await listen(server);
  const origin = `http://127.0.0.1:${server.address().port}`;
  let browser;
  try {
    browser = await chromium.launch({ headless: true, ...(process.env.PLAYWRIGHT_CHANNEL ? { channel: process.env.PLAYWRIGHT_CHANNEL } : {}) });
    const observations = [];
    for (const variant of ['self-only', 'registered-callback', 'foreign-callback']) {
      active = { variant, submitSeen: false, resumeSeen: false, callbackSeen: false,
        foreignSeen: false, cspViolation: false, originMatches: false };
      const page = await browser.newPage();
      await page.route('**/*', route => [origin, callbackOrigin, foreignOrigin].includes(new URL(route.request().url()).origin)
        ? route.continue() : route.abort());
      page.on('console', msg => { if (/form-action/iu.test(msg.text()) && /violat|refused/iu.test(msg.text())) active.cspViolation = true; });
      await page.goto(origin);
      await page.locator('button[value="approve"]').click({ noWaitAfter: true });
      await page.waitForTimeout(500);
      assert.equal(active.submitSeen, true); assert.equal(active.resumeSeen, true); assert.equal(active.originMatches, true);
      assert.equal(active.callbackSeen, variant === 'registered-callback'); assert.equal(active.foreignSeen, false);
      assert.equal(active.cspViolation, variant !== 'registered-callback');
      observations.push({ ...active }); await page.close();
    }
    console.log(JSON.stringify({ schema: 'feedback.consent-source-redirect-browser.v1', passed: true,
      scope: 'three-loopback-ports-only', sourceTemplateOnly: true, fullAuthIntegration: false,
      sourceSha256: createHash('sha256').update(source).digest('hex'), browserVersion: browser.version(), observations }));
  } finally { await browser?.close(); await stop(server); await stop(callback); await stop(foreign); }
}
main().catch(error => { console.error('local_consent_redirect_regression_failed: ' + error.message); process.exitCode = 1; });
