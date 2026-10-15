import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { fixtureConfiguration } from '../examples/fixture.mjs';

const cli = fileURLToPath(new URL('../cli.mjs', import.meta.url));
test('actual validate CLI escapes hostile metadata, preserves guides and only prepares a candidate', () => {
  const directory = mkdtempSync(join(tmpdir(), 'soty-guide-cli-'));
  try {
    mkdirSync(join(directory, 'guide'));
    const existing = join(directory, 'guide', 'index.html');
    writeFileSync(existing, 'Accepted existing guide');
    const descriptor = structuredClone(fixtureConfiguration().descriptor);
    descriptor.app.title = '<script>alert("x")</script> & App';
    const filename = join(directory, 'agent.json');
    writeFileSync(filename, JSON.stringify(descriptor));
    const invoke = () => spawnSync(process.execPath, [cli, 'validate', filename], { encoding: 'utf8' });
    const result = invoke();
    assert.equal(result.status, 0, result.stderr);
    const guide = JSON.parse(result.stdout).guide;
    assert.equal(guide.state, 'candidate');
    assert.equal(guide.routeInstalled, false);
    assert.equal(guide.published, false);
    const html = readFileSync(guide.path, 'utf8');
    assert.match(html, /&lt;script&gt;alert\(&quot;x&quot;\)&lt;\/script&gt; &amp; App/u);
    assert.doesNotMatch(html, /<script|<iframe|<form/u);
    assert.match(html, /fixture.board:skill\/create/u);
    assert.match(html, /fixture.board:create/u);
    assert.equal(readFileSync(existing, 'utf8'), 'Accepted existing guide');
    assert.equal(JSON.parse(invoke().stdout).guide.path, guide.path);
    writeFileSync(guide.path, 'Author edited this candidate');
    const replacement = JSON.parse(invoke().stdout).guide;
    assert.notEqual(replacement.path, guide.path);
    assert.equal(replacement.existingCandidatePreserved, true);
    assert.equal(readFileSync(guide.path, 'utf8'), 'Author edited this candidate');
    assert.equal(readFileSync(existing, 'utf8'), 'Accepted existing guide');
    const before = readdirSync(directory).sort();
    writeFileSync(filename, JSON.stringify({ ...descriptor, capabilities: [{ id: 'invalid' }] }));
    assert.notEqual(invoke().status, 0);
    assert.deepEqual(readdirSync(directory).sort(), before);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('host guide closed projection escapes text without interpreting URLs or exposing descriptor metadata', async () => {
  const { renderAppGuide } = await import('../guide.mjs');
  const view = { title: '<img src="https://internal.invalid/">', tools: ['<script>alert(1)</script>', 'javascript:alert(2) & "label"'], feedbackReady: true };
  const html = renderAppGuide(view);
  assert.match(html, /&lt;img src=&quot;https:\/\/internal.invalid\//);
  assert.match(html, /&lt;script&gt;/);
  assert.doesNotMatch(html, /<script|<img|<[^>]+(?:href|src)=/);
  assert.throws(() => renderAppGuide({ ...view, source: { connectorId: 'private' } }));
  assert.throws(() => renderAppGuide({ ...view, tools: [null] }));
});
