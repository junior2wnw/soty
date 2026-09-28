// Isolated, repeatable observation; no timings here are pass/fail thresholds.
import { performance } from 'node:perf_hooks';
import { cpus, tmpdir, release } from 'node:os';
import { mkdtempSync, rmSync, statSync, mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createWorldService } from '../modules/world/server/index.mjs';

const samples = 50, warmups = 10;
const root = resolve(tmpdir()), temporary = mkdtempSync(join(root, 'soty-directory-bench-'));
const destination = resolve('output', 'discovery-scale-20260928', 'benchmark.json');
const owner = { accountId: 'acct_benchmark_owner', deviceId: 'dev_benchmark_owner', label: 'Владелец теста' };
const reader = { accountId: 'acct_benchmark_reader', deviceId: 'dev_benchmark_reader', label: 'Читатель теста' };
const report = { measuredAt: new Date().toISOString(), runtime: { node: process.version, platform: process.platform, architecture: process.arch,
  osRelease: release(), cpu: cpus()[0]?.model, logicalCpus: cpus().length }, samples, warmups,
  transport: 'direct synchronous World service, no HTTP/Connect/signature/browser', fixtures: [] };
const rounding = value => Number(value.toFixed(4));
function measure(name, run) {
  for (let index = 0; index < warmups; index++) run();
  const times = []; let result;
  for (let index = 0; index < samples; index++) { const start = performance.now(); result = run(); times.push(performance.now() - start); }
  const sorted = [...times].sort((a, b) => a - b);
  const measurement = { name, p50Ms: rounding(sorted[Math.ceil(samples * .5) - 1]), p95Ms: rounding(sorted[Math.ceil(samples * .95) - 1]),
    minMs: rounding(sorted[0]), maxMs: rounding(sorted.at(-1)), meanMs: rounding(times.reduce((sum, value) => sum + value, 0) / samples),
    samplesMs: times.map(rounding),
    ...(result?.totals ? { totals: result.totals, peopleInPage: result.people.length, communitiesInPage: result.communities.length } : {}),
    ...(result?.community ? { memberCount: result.community.memberCount, previews: result.community.previewMembers.length } : {}) };
  console.log(JSON.stringify({ name, p50Ms: measurement.p50Ms, p95Ms: measurement.p95Ms }));
  return measurement;
}
function cursorAt(query, kind, after) { return Buffer.from(JSON.stringify({ version: 1, scope: JSON.stringify([query, kind]), after })).toString('base64url'); }

try {
  for (const profileCount of [10_000, 50_000]) {
    const databasePath = join(temporary, `${profileCount}.sqlite`), service = createWorldService({ databasePath, projectId: 'directory-benchmark' });
    let db;
    try {
      const call = (op, args = {}) => service.execute({ op: 'world.' + op, args, actor: reader });
      service.execute({ op: 'world.profile.get', actor: owner }); call('profile.get');
      db = new DatabaseSync(databasePath); db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; BEGIN IMMEDIATE');
      const started = performance.now();
      const profile = db.prepare('INSERT INTO profiles(account_id,display_name,bio,interests,search_text,discoverable,created_at,updated_at) VALUES(?,?,?,?,?,1,1,1)');
      const community = db.prepare("INSERT INTO communities(id,owner_id,name,description,search_text,join_policy,created_at,updated_at) VALUES(?,?,?,?,?,'open',1,1)");
      const member = db.prepare("INSERT INTO memberships(community_id,account_id,role,state,joined_at,updated_at) VALUES(?,?,?,'active',?,1)");
      let groupCount = 0;
      for (let index = 0; index < profileCount; index++) {
        const name = `Каталог человек ${index}`, bio = index % 100 === 0 ? 'Метеорит прогулки музыка' : 'Прогулки фотография музыка';
        profile.run(`acct_bench_${index}`, name, bio, '["фото","музыка"]', `${name} ${bio} фото музыка`.toLowerCase());
        if (index % 20 === 19) {
          const groupId = `group_bench_${groupCount}`, groupName = `Каталог сообщество ${groupCount}`;
          const description = groupCount % 10 === 0 ? 'Метеорит общие проекты' : 'Общие проекты';
          community.run(groupId, owner.accountId, groupName, description, `${groupName} ${description}`.toLowerCase());
          member.run(groupId, owner.accountId, 'owner', 1);
          for (let offset = 0; offset < 4; offset++) member.run(groupId, `acct_bench_${index - offset}`, 'member', offset + 2);
          groupCount++;
        }
      }
      const largeMembers = Math.min(10_000, profileCount);
      community.run('group_bench_large', owner.accountId, 'Крупный клуб', '', 'крупный клуб');
      member.run('group_bench_large', owner.accountId, 'owner', 1);
      for (let index = 0; index < largeMembers; index++) member.run('group_bench_large', `acct_bench_${index}`, 'member', index + 2);
      db.exec('COMMIT; PRAGMA wal_checkpoint(TRUNCATE)');
      const setupMs = performance.now() - started;
      const maximum = db.prepare('SELECT MAX(seq) AS seq FROM world_directory').get().seq, middleSeq = Math.floor(maximum / 2);
      const search = args => call('discovery.search', args);
      const emptyCounts = db.prepare('SELECT kind,n FROM world_directory_counts');
      const boundedCount = db.prepare('SELECT COUNT(*) AS n FROM (SELECT 1 FROM world_directory_fts WHERE world_directory_fts MATCH ? LIMIT ?)');
      const memberCount = db.prepare("SELECT COUNT(*) AS n FROM memberships WHERE community_id=? AND state='active'");
      console.log(JSON.stringify({ profileCount, communities: groupCount + 1, setupMs: rounding(setupMs) }));
      const measurements = [
        measure('empty-mixed-page', () => search({ kind: 'all', limit: 30 })),
        measure('empty-people-page', () => search({ kind: 'people', limit: 30 })),
        measure('empty-community-page', () => search({ kind: 'communities', limit: 30 })),
        measure('empty-mixed-cursor-halfway', () => search({ kind: 'all', limit: 30, cursor: cursorAt('', 'all', middleSeq) })),
        measure('prefix-common-mixed-page', () => search({ query: 'катал', kind: 'all', limit: 30 })),
        measure('prefix-common-cursor-halfway', () => search({ query: 'катал', kind: 'all', limit: 30, cursor: cursorAt('катал', 'all', middleSeq) })),
        measure('prefix-selective-mixed-page', () => search({ query: 'метеор', kind: 'all', limit: 30 })),
        measure('prefix-miss', () => search({ query: 'несуществующиймаркер', kind: 'all', limit: 30 })),
        measure('directory-counters-sql-only', () => emptyCounts.all()),
        measure('prefix-common-capped-counts-sql-only', () => ({ people: boundedCount.get('kind : person AND search_text : ("катал"*)', 1001).n,
          communities: boundedCount.get('kind : community AND search_text : ("катал"*)', 1001).n })),
        measure('community-detail-five-members', () => call('community.get', { communityId: 'group_bench_0' })),
        measure('community-detail-large', () => call('community.get', { communityId: 'group_bench_large' })),
        measure('large-group-member-count-sql-only', () => memberCount.get('group_bench_large')),
      ];
      report.fixtures.push({ profileCount, communities: groupCount + 1, largeGroupMembers: largeMembers + 1, setupMs: rounding(setupMs),
        databaseBytes: statSync(databasePath).size, memoryRssBytes: process.memoryUsage().rss,
        sqliteVersion: db.prepare('SELECT sqlite_version() AS version').get().version, measurements });
    } finally { db?.close(); service.close(); }
  }
  mkdirSync(dirname(destination), { recursive: true }); writeFileSync(destination, JSON.stringify(report, null, 2) + '\n');
  console.log(JSON.stringify({ result: destination, fixtures: report.fixtures.length, measuredReads: report.fixtures.length * 13 * samples }));
} finally {
  const resolved = resolve(temporary);
  if (dirname(resolved) !== root || !resolved.startsWith(join(root, 'soty-directory-bench-'))) throw new Error('Refusing to remove an unexpected benchmark path');
  rmSync(resolved, { recursive: true, force: true });
}
