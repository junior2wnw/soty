import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const python = process.platform === 'win32' ? 'python' : 'python3';
const script = fileURLToPath(new URL('./archive-source.py', import.meta.url));
const git = (repo, args, options = {}) => execFileSync('git', ['-C', repo, ...args], { encoding: 'utf8', ...options }).trim();
const archive = (repo, output, commit, env = {}) => spawnSync(python, [script, '--repo', repo, '--commit', commit, '--output-root', output], { encoding: 'utf8', env: { ...process.env, ...env } });

async function fixture(t, { ignored = false } = {}) {
  const parent = await realpath(tmpdir()), root = await mkdtemp(join(parent, 'soty-archive-test-'));
  t.after(async () => {
    assert.equal(dirname(resolve(root)), resolve(parent));
    assert.match(basename(root), /^soty-archive-test-/u);
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  const repo = join(root, 'repo'), output = join(root, 'output-雪-😀'), template = join(root, 'template');
  await Promise.all([mkdir(repo), mkdir(output), mkdir(template)]);
  git(repo, ['init', '-q', `--template=${template}`]);
  for (const [key, value] of [['user.name','Synthetic test'],['user.email','test@example.invalid'],['commit.gpgsign','false'],['core.hooksPath',template],['core.autocrlf','true'],['core.eol','crlf'],['core.safecrlf','false']]) git(repo, ['config', key, value]);
  for (const [name, value] of Object.entries({ Dockerfile:'FROM scratch\n', '.dockerignore':'\n', 'package.json':'{}\n',
    'pnpm-lock.yaml':'lockfileVersion: 9\n', 'sample.txt':'one\r\ntwo\r\n', 'run.sh':'#!/bin/sh\necho ok\n', 'file-雪-😀.txt':'unicode\n',
    '.gitattributes': ignored ? 'excluded.txt export-ignore\n' : '', 'excluded.txt':'still tracked\n' })) await writeFile(join(repo, name), value);
  git(repo, ['add', '.']); git(repo, ['update-index', '--chmod=+x', 'run.sh']); git(repo, ['commit', '-qm', 'synthetic source']);
  return { root, repo, output, commit: git(repo, ['rev-parse', 'HEAD']) };
}

test('a CRLF checkout archives Git LF bytes and the Git executable bit with an exact receipt', { timeout: 60000 }, async t => {
  const f = await fixture(t);
  await writeFile(join(f.repo, 'sample.txt'), 'changed'); git(f.repo, ['checkout', '--', 'sample.txt']);
  assert.equal(await readFile(join(f.repo, 'sample.txt'), 'utf8'), 'one\r\ntwo\r\n');
  assert.equal(git(f.repo, ['show', `${f.commit}:sample.txt`]), 'one\ntwo');
  const result = archive(f.repo, f.output, f.commit, { PYTHONIOENCODING:'cp1251' }); assert.equal(result.status, 0, result.stderr);
  assert.equal(Buffer.from(result.stdout, 'utf8').every(byte => byte < 128), true);
  const folder = join(f.output, f.commit), tar = join(folder, 'source.tar'), bytes = await readFile(tar);
  const receipt = JSON.parse(await readFile(join(folder, 'source-receipt.json'), 'utf8'));
  assert.equal(JSON.parse(result.stdout).archive, tar);
  assert.equal(receipt.commit, f.commit); assert.equal(receipt.tree, git(f.repo, ['rev-parse', 'HEAD^{tree}']));
  assert.equal(receipt.archiveBytes, bytes.length); assert.equal(receipt.sha256, createHash('sha256').update(bytes).digest('hex'));
  const probe = JSON.parse(execFileSync(python, ['-c', 'import base64,json,sys,tarfile\nt=tarfile.open(sys.argv[1]); print(json.dumps({"text":base64.b64encode(t.extractfile("sample.txt").read()).decode(),"mode":t.getmember("run.sh").mode}))', tar], { encoding: 'utf8' }));
  assert.deepEqual(Buffer.from(probe.text, 'base64'), Buffer.from('one\ntwo\n')); assert.notEqual(probe.mode & 0o111, 0);
});

test('export-ignore cannot silently omit a tracked file', { timeout: 60000 }, async t => {
  const f = await fixture(t, { ignored: true }), result = archive(f.repo, f.output, f.commit);
  assert.notEqual(result.status, 0); assert.match(result.stderr, /Archive omitted 1 tracked entries; first: excluded\.txt/u);
  await assert.rejects(readFile(join(f.output, f.commit, 'source-receipt.json')), { code: 'ENOENT' });
});

test('repeating an archive never overwrites the existing tar or receipt', { timeout: 60000 }, async t => {
  const f = await fixture(t), first = archive(f.repo, f.output, f.commit); assert.equal(first.status, 0, first.stderr);
  const folder = join(f.output, f.commit), before = await Promise.all(['source.tar','source-receipt.json'].map(name => readFile(join(folder, name))));
  const again = archive(f.repo, f.output, f.commit); assert.notEqual(again.status, 0); assert.match(again.stderr, /archive-source:/u);
  const after = await Promise.all(['source.tar','source-receipt.json'].map(name => readFile(join(folder, name)))); assert.deepEqual(after, before);
});

test('symlinks and submodules are explicitly rejected', { timeout: 60000 }, async t => {
  for (const [mode, kind] of [['120000','blob'],['160000','commit']]) await t.test(mode, async t => {
    const f = await fixture(t), object = kind === 'commit' ? f.commit : git(f.repo, ['hash-object','-w','--stdin'], { input:'target\n' });
    git(f.repo, ['update-index','--add','--cacheinfo',`${mode},${object},unsupported`]); git(f.repo, ['commit','-qm','unsupported entry']);
    const result = archive(f.repo, f.output, git(f.repo, ['rev-parse','HEAD'])); assert.notEqual(result.status, 0);
    assert.match(result.stderr, new RegExp(`Unsupported tracked entry unsupported: only regular files are supported \\(got ${mode} ${kind}\\)`, 'u'));
  });
});
