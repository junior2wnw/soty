import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { resolve, sep } from 'node:path';
import { pathToFileURL } from 'node:url';
import { mkdirSync, rmSync } from 'node:fs';
import { createServer } from 'node:net';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { createPlannerServer } from './main.ts';

async function fixture(host = '127.0.0.1') {
  const app = await createPlannerServer({ dbPath: ':memory:', port: 0, host, scheduler: false });
  const url = `http://127.0.0.1:${await app.listen()}`;
  const clients: Client[] = [];
  async function connect(token?: string) {
    const client = new Client({ name: 'mcp-test', version: '1.0.0' }, { capabilities: {} });
    await client.connect(
      new StreamableHTTPClientTransport(new URL('/mcp', url), {
        requestInit: { headers: token ? { Authorization: `Bearer ${token}` } : {} },
      }),
    );
    clients.push(client);
    return client;
  }
  async function http(
    path: string,
    body?: unknown,
    method = body === undefined ? 'GET' : 'POST',
    cookie?: string,
  ) {
    return fetch(url + path, {
      method,
      headers: {
        Origin: url,
        ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
        ...(cookie ? { Cookie: cookie } : {}),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  }
  async function close() {
    for (const client of clients) await client.close();
    await app.close();
  }
  return { app, url, connect, http, clients, close };
}
async function call(
  client: Client,
  name: string,
  args: Record<string, unknown> = {},
): Promise<Record<string, any> & { isError: boolean }> {
  const result = await client.callTool({ name, arguments: args });
  const json =
    (result.structuredContent as Record<string, any>) ??
    JSON.parse(
      (result.content as { type: string; text: string }[]).find((c) => c.type === 'text')!.text,
    );
  return { isError: result.isError === true, ...json };
}
async function ok(client: Client, name: string, args: Record<string, unknown> = {}) {
  const r = await call(client, name, args);
  assert.equal(r.isError, false, JSON.stringify(r));
  return r;
}
async function workspace(client: Client, name = 'MCP тест') {
  const r = await ok(client, 'planner_apply', {
    requestId: randomUUID(),
    operations: [
      {
        op: 'create',
        collection: 'workspaces',
        key: 'ws',
        data: { name, timezone: 'Asia/Yekaterinburg' },
      },
    ],
  });
  const read = await ok(client, 'planner_read', { collection: 'workspaces', query: name });
  return read.items[0].id as string;
}
test('real Streamable HTTP discovers tools, resources and prompts and executes a compact atomic batch', async () => {
  const f = await fixture();
  try {
    const c = await f.connect();
    const list = await c.listTools();
    assert.equal(list.tools.length, 8);
    assert(list.tools.every((t) => t.inputSchema.type === 'object'));
    assert.equal((await c.listResources()).resources.length, 2);
    const guide = await c.readResource({ uri: 'planner://guide' });
    assert.match(JSON.stringify(guide), /planner_apply/);
    const context = await c.readResource({ uri: 'planner://overview' });
    assert.match(JSON.stringify(context), /Asia\/Yekaterinburg/);
    assert.match(
      JSON.stringify(
        await c.getPrompt({ name: 'plan-anything', arguments: { goal: 'Поездка на неделю' } }),
      ),
      /Поездка/,
    );
    const ws = await workspace(c);
    const r = await ok(c, 'planner_apply', {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'objects',
          workspace: ws,
          key: 'trip',
          data: { title: 'Поездка', kind: 'process', start: '2027-06-01', end: '2027-06-08' },
        },
        {
          op: 'create',
          collection: 'objects',
          workspace: ws,
          data: { title: 'Идея без даты', parent: '$trip' },
        },
      ],
    });
    const read = await ok(c, 'planner_read', { workspace: ws, detail: true });
    assert.equal(read.items.length, 2);
    assert.equal(read.items.find((e: any) => e.title === 'Идея без даты').plan.start, null);
    assert.equal(
      read.items.find((e: any) => e.title === 'Поездка').plan.start,
      '2027-05-31T19:00:00.000Z',
    );
    assert(r.revision > 0);
  } finally {
    await f.close();
  }
});
test('MCP invalid input returns actionable structured errors, preserves atomic rollback and unknown dates', async () => {
  const f = await fixture();
  try {
    const c = await f.connect();
    const ws = await workspace(c);
    const before = f.app.store.read().revision;
    const r = await call(c, 'planner_apply', {
      requestId: randomUUID(),
      operations: [
        { op: 'create', collection: 'objects', workspace: ws, data: { title: 'Must roll back' } },
        {
          op: 'create',
          collection: 'objects',
          workspace: ws,
          data: { title: 'Invalid', start: '2027-02-30' },
        },
      ],
    });
    assert.equal(r.isError, true);
    assert.equal(r.error.code, 'invalid_date');
    assert(r.error.nextStep);
    assert.equal(f.app.store.read().revision, before);
    const unknown = await call(c, 'no_such_tool', {});
    assert.equal(unknown.error.code, 'unknown_tool');
    const offset = await call(c, 'planner_apply', {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'objects',
          workspace: ws,
          data: { title: 'Clock', start: '2027-03-01T09:00:00' },
        },
      ],
    });
    assert.equal(offset.error.code, 'offset_required');
  } finally {
    await f.close();
  }
});
test('MCP scenario previews, cascades, approvals and retries preserve baseline and reject stale changes', async () => {
  const f = await fixture();
  try {
    const c = await f.connect();
    const ws = await workspace(c);
    await ok(c, 'planner_apply', {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'objects',
          workspace: ws,
          key: 'a',
          data: { title: 'A', start: '2027-06-01', end: '2027-06-02' },
        },
        {
          op: 'create',
          collection: 'objects',
          workspace: ws,
          key: 'b',
          data: { title: 'B', start: '2027-06-02', end: '2027-06-03' },
        },
        { op: 'create', collection: 'dependencies', workspace: ws, data: { from: '$a', to: '$b' } },
      ],
    });
    const objects = (await ok(c, 'planner_read', { workspace: ws, detail: true })).items;
    const a = objects.find((e: any) => e.title === 'A');
    const b = objects.find((e: any) => e.title === 'B');
    const change = {
      workspace: ws,
      changes: [{ object: a.id, shiftMinutes: 1440, expectedVersion: a.version }],
    };
    const preview = await ok(c, 'planner_scenario', { action: 'preview', ...change });
    assert.equal(preview.preview.changes.length, 2);
    assert.equal(f.app.store.read().entities.find((e) => e.id === a.id)!.version, 1);
    const proposal = { action: 'create', ...change, name: 'Delay', requestId: randomUUID() };
    const created = await ok(c, 'planner_scenario', proposal);
    const replay = await ok(c, 'planner_scenario', proposal);
    assert.equal(replay.scenario.id, created.scenario.id);
    assert(replay.replayed);
    await ok(c, 'planner_scenario', {
      action: 'submit',
      scenario: created.scenario.id,
      requestId: randomUUID(),
    });
    const decision = {
      action: 'approve',
      scenario: created.scenario.id,
      requestId: randomUUID(),
      confirm: true,
    };
    await ok(c, 'planner_scenario', decision);
    assert((await ok(c, 'planner_scenario', decision)).replayed);
    const state = f.app.store.read();
    assert.deepEqual(state.entities.find((e) => e.id === a.id)!.baseline, a.baseline);
    assert.deepEqual(state.entities.find((e) => e.id === b.id)!.baseline, b.baseline);
    assert.equal(
      Date.parse(state.entities.find((e) => e.id === b.id)!.plan.start!),
      Date.parse('2027-06-02T19:00:00.000Z'),
    );
    const stale = await call(c, 'planner_scenario', {
      action: 'create',
      ...change,
      requestId: randomUUID(),
    });
    assert.equal(stale.error.code, 'version_conflict');
  } finally {
    await f.close();
  }
});
test('MCP JSON/CSV exports and atomic imports are idempotent and remap the full logical model', async () => {
  const f = await fixture();
  try {
    const c = await f.connect();
    const source = await workspace(c, 'Source');
    const target = await workspace(c, 'Target');
    await ok(c, 'planner_apply', {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'objects',
          workspace: source,
          data: { title: 'Unknown', tags: ['travel'] },
        },
        {
          op: 'create',
          collection: 'objects',
          workspace: source,
          data: {
            title: 'Repeat',
            start: '2027-06-01',
            recurrence: { frequency: 'week', count: 3 },
          },
        },
      ],
    });
    for (const format of ['json', 'csv']) {
      const exported = await ok(c, 'planner_transfer', {
        action: 'export',
        workspace: source,
        format,
      });
      const args = {
        action: 'import',
        workspace: target,
        format,
        content: exported.content,
        requestId: randomUUID(),
      };
      const imported = await ok(c, 'planner_transfer', args);
      assert.equal(imported.created.length, 2);
      assert((await ok(c, 'planner_transfer', args)).replayed);
    }
    const items = (await ok(c, 'planner_read', { workspace: target, detail: true })).items;
    assert.equal(items.length, 4);
    assert.equal(items.find((e: any) => e.title === 'Unknown').plan.start, null);
    assert.equal(items.find((e: any) => e.title === 'Repeat').recurrence.count, 3);
    const before = f.app.store.read().revision;
    const failed = await call(c, 'planner_transfer', {
      action: 'import',
      workspace: target,
      format: 'json',
      content: 'not-json',
      requestId: randomUUID(),
    });
    assert(failed.isError);
    assert.equal(f.app.store.read().revision, before);
  } finally {
    await f.close();
  }
});

test('real MCP client preserves exact nanosecond events through read, scenario and JSON transfer', async () => {
  const f = await fixture();
  try {
    const c = await f.connect();
    const source = await workspace(c, 'Exact source');
    const target = await workspace(c, 'Exact target');
    const start = '10000000000000000000000000';
    const end = '10000000000000000000000001';
    const created = await ok(c, 'planner_apply', {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'objects',
          workspace: source,
          data: {
            title: 'Exact one nanosecond',
            plan: {
              timezone: 'UTC',
              precision: 'exact',
              precise: {
                scale: 'unix-nanoseconds',
                start,
                end,
                resolutionNs: '1',
              },
            },
          },
        },
      ],
    });
    const id = created.results[0].id;
    const initial = (await ok(c, 'planner_read', { ids: [id], detail: true })).items[0];
    assert.equal(initial.plan.start, null);
    assert.equal(initial.plan.precise.start, start);
    assert.deepEqual(initial.baseline, initial.plan);
    const occurrences = await ok(c, 'planner_read', {
      collection: 'occurrences',
      workspace: source,
      fromNs: (BigInt(start) - 1n).toString(),
      toNs: (BigInt(end) + 1n).toString(),
    });
    assert.equal(occurrences.items[0].precise.start, start);

    const scenarioRequest = {
      action: 'create',
      workspace: source,
      name: 'Exact shift',
      requestId: randomUUID(),
      changes: [{ object: id, expectedVersion: initial.version, shiftNs: '10' }],
    };
    const proposal = await ok(c, 'planner_scenario', scenarioRequest);
    assert.equal(
      proposal.scenario.preview.changes[0].plan.precise.start,
      (BigInt(start) + 10n).toString(),
    );
    assert((await ok(c, 'planner_scenario', scenarioRequest)).replayed);
    await ok(c, 'planner_scenario', {
      action: 'submit',
      scenario: proposal.scenario.id,
      requestId: randomUUID(),
    });
    await ok(c, 'planner_scenario', {
      action: 'approve',
      scenario: proposal.scenario.id,
      confirm: true,
      requestId: randomUUID(),
    });
    const shifted = (await ok(c, 'planner_read', { ids: [id], detail: true })).items[0];
    assert.equal(shifted.plan.precise.start, (BigInt(start) + 10n).toString());
    assert.equal(shifted.baseline.precise.start, start);

    const savedTemplate = await ok(c, 'planner_template', {
      action: 'save',
      workspace: source,
      object: id,
      name: 'Exact pulse',
      requestId: randomUUID(),
    });
    assert.equal(savedTemplate.template.anchorNs, shifted.plan.precise.start);
    const templateAnchor = (BigInt(start) + 100n).toString();
    const appliedTemplate = await ok(c, 'planner_template', {
      action: 'apply',
      workspace: source,
      template: savedTemplate.template.id,
      anchorNs: templateAnchor,
      requestId: randomUUID(),
    });
    const templated = (
      await ok(c, 'planner_read', { ids: [appliedTemplate.created[0].id], detail: true })
    ).items[0];
    assert.equal(templated.plan.precise.start, templateAnchor);
    assert.equal(templated.plan.precise.end, (BigInt(templateAnchor) + 1n).toString());

    const exported = await ok(c, 'planner_transfer', {
      action: 'export',
      workspace: source,
      format: 'json',
    });
    assert.equal(JSON.parse(exported.content).schemaVersion, 3);
    const importRequest = {
      action: 'import',
      workspace: target,
      format: 'json',
      content: exported.content,
      requestId: randomUUID(),
    };
    const imported = await ok(c, 'planner_transfer', importRequest);
    assert((await ok(c, 'planner_transfer', importRequest)).replayed);
    const copy = (
      await ok(c, 'planner_read', {
        workspace: target,
        ids: [imported.created[0].id],
        detail: true,
      })
    ).items[0];
    assert.equal(copy.plan.precise.start, shifted.plan.precise.start);
    assert.equal(copy.baseline.precise.start, start);
    const ics = await call(c, 'planner_transfer', {
      action: 'export',
      workspace: source,
      format: 'ics',
    });
    assert(ics.isError);
    assert.equal(ics.error.code, 'ics_projection_unsupported');
  } finally {
    await f.close();
  }
});
test('MCP saves and instantiates process templates with typed fields and nested dependencies', async () => {
  const f = await fixture();
  try {
    const c = await f.connect();
    const ws = await workspace(c);
    await ok(c, 'planner_apply', {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'objects',
          workspace: ws,
          key: 'root',
          data: { title: 'Tour', kind: 'process', start: '2027-06-01', end: '2027-06-08' },
        },
        {
          op: 'create',
          collection: 'objects',
          workspace: ws,
          key: 'a',
          data: { title: 'Flight', parent: '$root', start: '2027-06-01', end: '2027-06-02' },
        },
        {
          op: 'create',
          collection: 'objects',
          workspace: ws,
          key: 'b',
          data: { title: 'Hotel', parent: '$root', start: '2027-06-02', end: '2027-06-08' },
        },
        { op: 'create', collection: 'dependencies', workspace: ws, data: { from: '$a', to: '$b' } },
      ],
    });
    const before = await ok(c, 'planner_read', { workspace: ws });
    const saved = await ok(c, 'planner_template', {
      action: 'save',
      workspace: ws,
      object: 'Tour',
      name: 'Tour kit',
      expectedRevision: before.revision,
      requestId: randomUUID(),
    });
    assert.equal(saved.template.items.length, 3);
    const args = {
      action: 'apply',
      workspace: ws,
      template: saved.template.id,
      start: '2027-07-01',
      title: 'July tour',
      requestId: randomUUID(),
    };
    const applied = await ok(c, 'planner_template', args);
    assert.equal(applied.created.length, 3);
    assert.equal(applied.dependencies.length, 1);
    assert((await ok(c, 'planner_template', args)).replayed);
    const after = await ok(c, 'planner_read', { workspace: ws });
    assert.equal(after.total, 6);
  } finally {
    await f.close();
  }
});
test('MCP signal controls refuse active resolution, persist snoozes and require a risk reason', async () => {
  const f = await fixture();
  try {
    const c = await f.connect();
    const signals = (await ok(c, 'planner_read', { collection: 'signals', workspace: 'team' }))
      .items;
    // Generate real current conditions, without depending on sample notification fixtures.
    f.app.scheduler.tick();
    const signal = (await ok(c, 'planner_read', { collection: 'signals', workspace: 'team' }))
      .items[0];
    assert(signal);
    const resolved = await call(c, 'planner_attention', {
      action: 'resolve',
      workspace: 'team',
      signal: signal.id,
      requestId: randomUUID(),
    });
    assert.equal(resolved.error.code, 'condition_active');
    const risk = await call(c, 'planner_attention', {
      action: 'accept-risk',
      workspace: 'team',
      signal: signal.id,
      requestId: randomUUID(),
    });
    assert.equal(risk.error.code, 'reason_required');
    const args = {
      action: 'snooze',
      workspace: 'team',
      signal: signal.id,
      until: new Date(Date.now() + 86400000).toISOString(),
      requestId: randomUUID(),
    };
    const snoozed = await ok(c, 'planner_attention', args);
    assert(snoozed.signal.snoozedUntil);
    assert((await ok(c, 'planner_attention', args)).replayed);
  } finally {
    await f.close();
  }
});
test('scoped MCP bearer keys are hashed, read-only and revocable, and cannot see other workspace data', async () => {
  const f = await fixture();
  try {
    const keyResponse = await f.http('/api/agent/keys', {
      name: 'Reader',
      workspaceIds: ['personal'],
      readOnly: true,
    });
    assert.equal(keyResponse.status, 201);
    const key = await keyResponse.json();
    const stored = f.app.store.db
      .prepare('SELECT token_hash FROM agent_keys WHERE id=?')
      .get(key.id) as { token_hash: string };
    assert.notEqual(stored.token_hash, key.token);
    assert.equal(stored.token_hash.length, 64);
    const c = await f.connect(key.token);
    const read = await ok(c, 'planner_read', { collection: 'overview' });
    assert.equal(read.workspaces.length, 1);
    assert.equal(read.workspaces[0].id, 'personal');
    const hidden = await call(c, 'planner_read', { collection: 'objects', workspace: 'team' });
    assert.equal(hidden.error.code, 'not_found');
    assert(!JSON.stringify(hidden).includes('Командная работа'));
    const write = await call(c, 'planner_apply', {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'objects',
          workspace: 'personal',
          data: { title: 'Forbidden' },
        },
      ],
    });
    assert.equal(write.error.code, 'read_only');
    const revoke = await f.http(`/api/agent/keys/${key.id}`, undefined, 'DELETE');
    assert.equal(revoke.status, 200);
    await assert.rejects(c.callTool({ name: 'planner_help', arguments: {} }));
    const listed = await (await f.http('/api/agent/keys')).json();
    assert.equal(listed.keys.length, 0);
    assert(!JSON.stringify(listed).includes(key.token));
  } finally {
    await f.close();
  }
});
test('public binding requires authentication for MCP and validates Origin even on GET', async () => {
  const f = await fixture('0.0.0.0');
  try {
    const unauth = await fetch(f.url + '/mcp', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'initialize',
        params: {
          protocolVersion: '2025-11-25',
          capabilities: {},
          clientInfo: { name: 'test', version: '1' },
        },
      }),
    });
    assert.equal(unauth.status, 401);
    for (const method of ['GET', 'POST']) {
      const hostile = await fetch(f.url + '/mcp', {
        method,
        headers: { Origin: 'https://hostile.invalid' },
      });
      assert.equal(hostile.status, 403);
    }
    const register = await f.http('/api/auth/register', {
      email: `${randomUUID()}@test.invalid`,
      password: randomUUID() + '!abc',
      displayName: 'Private MCP owner',
      claimLocal: false,
    });
    assert.equal(register.status, 201);
    const state = await register.json();
    const cookie = register.headers.get('set-cookie')!.split(';')[0];
    const key = await (
      await f.http(
        '/api/agent/keys',
        { name: 'Remote', workspaceIds: [state.workspaces[0].id], readOnly: false },
        'POST',
        cookie,
      )
    ).json();
    const c = await f.connect(key.token);
    const read = await ok(c, 'planner_read', { collection: 'overview' });
    assert.equal(read.workspaces.length, 1);
    assert(!JSON.stringify(read).includes('team'));
  } finally {
    await f.close();
  }
});
test('real stdio MCP transports the same tools and permissions without protocol noise', async () => {
  const f = await fixture();
  try {
    const key = await (
      await f.http('/api/agent/keys', { name: 'Stdio', workspaceIds: ['personal'], readOnly: true })
    ).json();
    const client = new Client({ name: 'stdio-test', version: '1' }, { capabilities: {} });
    f.clients.push(client);
    const transport = new StdioClientTransport({
      command: process.execPath,
      args: [
        '--import',
        pathToFileURL(resolve('node_modules/tsx/dist/loader.mjs')).href,
        resolve('server/mcp-stdio.ts'),
      ],
      env: { PLANNER_URL: f.url, PLANNER_MCP_TOKEN: key.token },
      stderr: 'pipe',
    });
    await client.connect(transport);
    assert.equal((await client.listTools()).tools.length, 8);
    const read = await ok(client, 'planner_read', { collection: 'workspaces' });
    assert.equal(read.items.length, 1);
    assert.equal(read.items[0].id, 'personal');
  } finally {
    await f.close();
  }
});
test('MCP universal templates preserve undated ideas, repeats, uncertainty and calendar clocks across DST', async () => {
  const f = await fixture();
  try {
    const c = await f.connect();
    const created = await ok(c, 'planner_apply', {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'workspaces',
          key: 'ws',
          data: { name: 'DST templates', timezone: 'America/New_York' },
        },
        {
          op: 'create',
          collection: 'objects',
          workspace: '$ws',
          key: 'course',
          data: {
            title: 'Course',
            kind: 'process',
            start: '2027-03-01',
            end: '2027-03-16',
            due: '2027-03-15',
            tags: ['learn'],
          },
        },
        {
          op: 'create',
          collection: 'objects',
          workspace: '$ws',
          data: {
            title: 'Weekly',
            parent: '$course',
            start: '2027-03-01T09:00:00-05:00',
            end: '2027-03-01T10:00:00-05:00',
            recurrence: {
              frequency: 'week',
              weekdays: [1],
              until: '2027-03-15',
              exceptions: ['2027-03-08'],
            },
          },
        },
        {
          op: 'create',
          collection: 'objects',
          workspace: '$ws',
          data: { title: 'Undated', parent: '$course' },
        },
        {
          op: 'create',
          collection: 'objects',
          workspace: '$ws',
          data: {
            title: 'Maybe',
            parent: '$course',
            kind: 'period',
            plan: {
              start: '2027-03-05',
              end: '2027-03-06',
              precision: 'approximate',
              earliest: '2027-03-04',
              latest: '2027-03-07',
            },
          },
        },
      ],
    });
    const ws = created.refs.ws;
    const source = (await ok(c, 'planner_read', { workspace: ws, detail: true })).items;
    const series = source.find((e: any) => e.title === 'Weekly');
    assert.equal(series.recurrence.until, '2027-03-15');
    assert.equal(series.recurrence.exceptions[0], '2027-03-08');
    const occurrences = await ok(c, 'planner_read', {
      collection: 'occurrences',
      workspace: ws,
      ids: [series.id],
      from: '2027-03-01',
      to: '2027-03-16',
    });
    assert.equal(occurrences.items.length, 2);
    assert.match(occurrences.items[1].start, /T09:00:00.*-04:00$/);
    assert.equal(occurrences.items[1].timezone, 'America/New_York');
    const saved = await ok(c, 'planner_template', {
      action: 'save',
      workspace: ws,
      object: 'Course',
      name: 'Reusable course',
      requestId: randomUUID(),
    });
    const applied = await ok(c, 'planner_template', {
      action: 'apply',
      workspace: ws,
      template: saved.template.id,
      start: '2027-03-08',
      title: 'New course',
      requestId: randomUUID(),
    });
    const items = (
      await ok(c, 'planner_read', {
        workspace: ws,
        ids: applied.created.map((e: any) => e.id),
        detail: true,
      })
    ).items;
    assert.equal(items.find((e: any) => e.title === 'Undated').plan.start, null);
    assert.equal(items.find((e: any) => e.title === 'Undated').plan.precision, 'unknown');
    const weekly = items.find((e: any) => e.title === 'Weekly');
    assert.equal(weekly.recurrence.until, '2027-03-22');
    assert.equal(weekly.recurrence.exceptions[0], '2027-03-15');
    const shifted = await ok(c, 'planner_read', {
      collection: 'occurrences',
      workspace: ws,
      ids: [weekly.id],
      from: '2027-03-08',
      to: '2027-03-23',
    });
    assert.equal(shifted.items.length, 2);
    assert(shifted.items.every((o: any) => o.start.includes('T09:00:00')));
    const maybe = items.find((e: any) => e.title === 'Maybe');
    assert.equal(maybe.plan.precision, 'approximate');
    assert.match(maybe.plan.earliest, /2027-03-11/);
    assert.match(maybe.plan.latest, /2027-03-14/);
    const root = items.find((e: any) => e.title === 'New course');
    assert.deepEqual(root.tags, ['learn']);
    assert.equal(root.actual, null);
    assert.deepEqual(root.baseline, root.plan);
  } finally {
    await f.close();
  }
});
test('MCP real file bytes, attachments, versions and durable deletion replays are consistent', async () => {
  const f = await fixture();
  try {
    const c = await f.connect();
    const ws = await workspace(c);
    const create = await ok(c, 'planner_apply', {
      requestId: randomUUID(),
      operations: [
        { op: 'create', collection: 'objects', workspace: ws, data: { title: 'Tickets' } },
      ],
    });
    const object = create.results[0].id;
    const args = {
      action: 'attach',
      object,
      name: 'Билет.txt',
      text: 'Пассажир: тест\nРейс: 42',
      expectedVersion: 1,
      requestId: randomUUID(),
    };
    const attached = await ok(c, 'planner_file', args);
    assert.equal(attached.object.version, 2);
    assert((await ok(c, 'planner_file', args)).replayed);
    const read = await ok(c, 'planner_file', {
      action: 'read',
      file: attached.file.id,
      format: 'text',
    });
    assert.equal(read.content, args.text);
    const hiddenKey = await (
      await f.http('/api/agent/keys', {
        name: 'Other space',
        workspaceIds: ['team'],
        readOnly: true,
      })
    ).json();
    const other = await f.connect(hiddenKey.token);
    const denied = await call(other, 'planner_file', { action: 'read', file: attached.file.id });
    assert.equal(denied.error.code, 'not_found');
    const revision = (await ok(c, 'planner_read', { workspace: ws })).revision;
    const deletion = {
      action: 'delete',
      file: attached.file.id,
      expectedRevision: revision,
      requestId: randomUUID(),
    };
    await ok(c, 'planner_file', deletion);
    assert((await ok(c, 'planner_file', deletion)).replayed);
    const state = (await ok(c, 'planner_read', { workspace: ws, detail: true })).items[0];
    assert.equal(state.version, 3);
    assert.equal(state.links.length, 0);
  } finally {
    await f.close();
  }
});
test('workspace-scoped keys cannot mutate account-wide settings and comments do not stale scenarios', async () => {
  const f = await fixture();
  try {
    const c = await f.connect();
    const ws = await workspace(c);
    const key = await (
      await f.http('/api/agent/keys', {
        name: 'Scoped editor',
        workspaceIds: [ws],
        readOnly: false,
      })
    ).json();
    const editor = await f.connect(key.token);
    const revision = f.app.store.read().revision;
    const global = await call(editor, 'planner_apply', {
      requestId: randomUUID(),
      expectedRevision: revision,
      operations: [{ op: 'settings', data: { notifications: { timezone: 'UTC' } } }],
    });
    assert.equal(global.error.code, 'scope_violation');
    const created = await ok(c, 'planner_apply', {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'objects',
          workspace: ws,
          data: { title: 'Move me', start: '2027-06-01' },
        },
      ],
    });
    const entity = created.results[0].id;
    const scenario = await ok(c, 'planner_scenario', {
      action: 'create',
      changes: [{ object: entity, expectedVersion: 1, shiftMinutes: 1440 }],
      requestId: randomUUID(),
    });
    await ok(c, 'planner_apply', {
      requestId: randomUUID(),
      operations: [{ op: 'comment', ref: entity, data: { text: 'Reviewed' } }],
    });
    const submitted = await ok(c, 'planner_scenario', {
      action: 'submit',
      scenario: scenario.scenario.id,
      requestId: randomUUID(),
    });
    assert.equal(submitted.scenario.state, 'pending');
  } finally {
    await f.close();
  }
});
test('stdio auto-starts a local server, persists state across client sessions and keeps stdout protocol-only', async () => {
  const probe = createServer();
  await new Promise<void>((r) => probe.listen(0, '127.0.0.1', r));
  const port = (probe.address() as { port: number }).port;
  await new Promise<void>((r) => probe.close(() => r()));
  const directory = resolve('output', 'mcp-autostart-tests', randomUUID());
  mkdirSync(directory, { recursive: true });
  const connect = async () => {
    const client = new Client({ name: 'autostart', version: '1' }, { capabilities: {} });
    await client.connect(
      new StdioClientTransport({
        command: process.execPath,
        args: [
          '--import',
          pathToFileURL(resolve('node_modules/tsx/dist/loader.mjs')).href,
          resolve('server/mcp-stdio.ts'),
        ],
        env: { PLANNER_PORT: String(port), PLANNER_DB_PATH: resolve(directory, 'planner.sqlite') },
        stderr: 'pipe',
      }),
    );
    return client;
  };
  let c: Client | undefined;
  try {
    c = await connect();
    const applied = await ok(c, 'planner_apply', {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'objects',
          workspace: 'personal',
          data: { title: 'Survives stdio restart' },
        },
      ],
    });
    const id = applied.results[0].id;
    await c.close();
    c = undefined;
    c = await connect();
    const result = await ok(c, 'planner_read', { ids: [id] });
    assert.equal(result.items[0].title, 'Survives stdio restart');
  } finally {
    if (c) await c.close();
    const allowed = resolve('output', 'mcp-autostart-tests');
    if (!directory.startsWith(allowed + sep)) throw new Error('Unsafe cleanup path');
    rmSync(directory, { recursive: true, force: true });
  }
});
test('template resource assignments survive cloning; website template creation uses the same universal model', async () => {
  const f = await fixture();
  try {
    const c = await f.connect();
    const ws = await workspace(c);
    await ok(c, 'planner_apply', {
      requestId: randomUUID(),
      operations: [
        {
          op: 'create',
          collection: 'resources',
          workspace: ws,
          key: 'room',
          data: { name: 'Room', kind: 'place' },
        },
        {
          op: 'create',
          collection: 'objects',
          workspace: ws,
          data: {
            title: 'Meeting',
            start: '2027-06-07T09:00:00+05:00',
            end: '2027-06-07T10:00:00+05:00',
            recurrence: { frequency: 'week', count: 3 },
            resources: ['$room'],
          },
        },
      ],
    });
    const meeting = (await ok(c, 'planner_read', { workspace: ws, detail: true })).items[0];
    const saved = await ok(c, 'planner_template', {
      action: 'save',
      workspace: ws,
      object: meeting.id,
      name: 'Room series',
      requestId: randomUUID(),
    });
    const applied = await ok(c, 'planner_template', {
      action: 'apply',
      workspace: ws,
      template: saved.template.id,
      start: '2027-07-07T09:00:00+05:00',
      requestId: randomUUID(),
    });
    const clone = (
      await ok(c, 'planner_read', { workspace: ws, ids: [applied.created[0].id], detail: true })
    ).items[0];
    assert.deepEqual(clone.allocations, meeting.allocations);
    assert.equal(clone.recurrence.count, 3);
    assert.deepEqual(applied.conflicts, []);
    const payload = {
      workspaceId: ws,
      name: 'Website series',
      anchorDate: meeting.plan.start,
      timezone: meeting.plan.timezone,
      items: [
        {
          key: 'meeting',
          title: meeting.title,
          typeId: meeting.typeId,
          kind: meeting.kind,
          offsetDays: 0,
          durationDays: 1 / 24,
          schedule: meeting.plan,
          recurrence: meeting.recurrence,
          allocations: meeting.allocations,
        },
      ],
      dependencies: [],
    };
    const response = await f.http('/api/templates', payload);
    assert.equal(response.status, 201);
    const state = await response.json();
    assert(state.templates.some((t: any) => t.name === 'Website series'));
    const before = f.app.store.read().revision;
    const malformed = await f.http('/api/templates', {
      ...payload,
      name: 'Bad template',
      items: [{ ...payload.items[0], parentKey: 'missing' }],
    });
    assert.equal(malformed.status, 400);
    assert.equal(f.app.store.read().revision, before);
    const named = await ok(c, 'planner_template', {
      action: 'rename',
      workspace: ws,
      template: saved.template.id,
      name: 'Renamed series',
      expectedRevision: before,
      requestId: randomUUID(),
    });
    assert.equal(named.template.name, 'Renamed series');
    const removed = await ok(c, 'planner_template', {
      action: 'delete',
      workspace: ws,
      template: saved.template.id,
      expectedRevision: named.revision,
      confirm: true,
      requestId: randomUUID(),
    });
    assert.equal(removed.deleted, saved.template.id);
  } finally {
    await f.close();
  }
});
