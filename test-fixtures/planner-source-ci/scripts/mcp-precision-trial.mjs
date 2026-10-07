import assert from 'node:assert/strict';
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { createPlannerServer } from '../server/main.ts';

const outputDirectory = resolve('output', 'agent-trials');
const database = resolve(outputDirectory, 'precision-mcp-sdk-trial.sqlite');
const receiptPath = resolve(outputDirectory, 'precision-mcp-sdk-receipt.json');
mkdirSync(outputDirectory, { recursive: true });
for (const suffix of ['', '-wal', '-shm']) rmSync(database + suffix, { force: true });

const app = await createPlannerServer({
  dbPath: database,
  port: 0,
  host: '127.0.0.1',
  scheduler: false,
});
const port = await app.listen();
const client = new Client({ name: 'precision-sdk-trial', version: '1.0.0' }, { capabilities: {} });
const checks = [];

async function call(name, args = {}) {
  const response = await client.callTool({ name, arguments: args });
  const value =
    response.structuredContent ??
    JSON.parse(response.content.find((item) => item.type === 'text').text);
  if (response.isError) throw new Error(`${name}: ${JSON.stringify(value)}`);
  return value;
}

try {
  await client.connect(new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`)));
  const tools = await client.listTools();
  assert(tools.tools.some((tool) => tool.name === 'planner_apply'));
  checks.push('official SDK discovery');

  const start = '10000000000000000000000000';
  const end = (BigInt(start) + 1n).toString();
  const created = await call('planner_apply', {
    requestId: randomUUID(),
    operations: [
      {
        op: 'create',
        collection: 'objects',
        workspace: 'personal',
        data: {
          title: 'SDK exact pulse',
          plan: {
            timezone: 'UTC',
            precision: 'exact',
            precise: { scale: 'unix-nanoseconds', start, end, resolutionNs: '1' },
          },
        },
      },
    ],
  });
  const objectId = created.results[0].id;
  const read = await call('planner_read', { ids: [objectId], detail: true });
  assert.equal(read.items[0].plan.precise.start, start);
  assert.equal(typeof read.items[0].plan.precise.start, 'string');
  assert.equal(read.items[0].plan.start, null);
  checks.push('decimal-string create/read outside calendar range');

  const occurrences = await call('planner_read', {
    collection: 'occurrences',
    workspace: 'personal',
    fromNs: (BigInt(start) - 1n).toString(),
    toNs: (BigInt(end) + 1n).toString(),
  });
  assert.equal(occurrences.items[0].precise.end, end);
  checks.push('exact occurrence viewport');

  const request = {
    action: 'create',
    workspace: 'personal',
    name: 'SDK exact shift',
    requestId: randomUUID(),
    changes: [{ object: objectId, expectedVersion: 1, shiftNs: '9' }],
  };
  const scenario = await call('planner_scenario', request);
  const replay = await call('planner_scenario', request);
  assert.equal(replay.scenario.id, scenario.scenario.id);
  assert.equal(replay.replayed, true);
  checks.push('scenario exact shift and durable replay');

  const exported = await call('planner_transfer', {
    action: 'export',
    workspace: 'personal',
    format: 'json',
  });
  const document = JSON.parse(exported.content);
  assert.equal(document.schemaVersion, 3);
  assert.equal(
    document.entities.find((entity) => entity.id === objectId).plan.precise.start,
    start,
  );
  checks.push('JSON v3 exact export');

  writeFileSync(
    receiptPath,
    JSON.stringify(
      {
        status: 'passed',
        completedAt: new Date().toISOString(),
        transport: 'Streamable HTTP via official TypeScript MCP SDK Client',
        isolatedDatabase: database,
        productionDatabaseTouched: false,
        checks,
        sample: { objectId, start, end, scenarioId: scenario.scenario.id },
      },
      null,
      2,
    ) + '\n',
  );
  console.log(receiptPath);
} finally {
  await client.close().catch(() => {});
  await app.close();
  for (const suffix of ['', '-wal', '-shm']) rmSync(database + suffix, { force: true });
}
