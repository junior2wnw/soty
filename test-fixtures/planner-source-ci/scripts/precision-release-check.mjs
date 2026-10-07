import assert from 'node:assert/strict';
import { writeFileSync } from 'node:fs';
import { connectPlanner, jsonResult } from './mcp-client.mjs';

// Read-only confirmation of the actual main service after the local update.
const origin = 'http://127.0.0.1:4317';
const client = await connectPlanner({ url: origin, name: 'precision-release-check' });
try {
  const health = await (await fetch(new URL('/health', origin))).json();
  const discovery = await client.listTools();
  const names = discovery.tools.map((tool) => tool.name).sort();
  assert.equal(health.version, '1.2.0');
  assert.equal(client.getServerVersion()?.version, '1.2.0');
  assert.equal(names.length, 8);
  const readSchema = discovery.tools.find((tool) => tool.name === 'planner_read')?.inputSchema;
  assert.ok(JSON.stringify(readSchema).includes('fromNs'));
  const help = jsonResult(await client.callTool({ name: 'planner_help', arguments: {} }));
  assert.equal(help.isError, false);
  assert.ok(JSON.stringify(help).includes('unix-nanoseconds'));
  const read = jsonResult(
    await client.callTool({
      name: 'planner_read',
      arguments: { collection: 'objects', limit: 200 },
    }),
  );
  assert.equal(read.isError, false);
  assert.equal(read.items.length, 22);
  const receipt = {
    checkedAt: new Date().toISOString(),
    version: health.version,
    tools: names,
    objects: read.items.length,
    preciseDiscovery: true,
    preciseHelp: true,
    writes: 0,
  };
  writeFileSync(
    'output/agent-trials/precision-main-mcp-receipt.json',
    JSON.stringify(receipt, null, 2) + '\n',
  );
  console.log(JSON.stringify(receipt));
} finally {
  await client.close();
}
