import { readFile } from 'node:fs/promises';
import { connectPlanner, jsonResult } from './mcp-client.mjs';
const client = await connectPlanner();
try {
  const [tool, file] = process.argv.slice(2);
  if (!tool || tool === '--list') {
    process.stdout.write(JSON.stringify(await client.listTools(), null, 2) + '\n');
  } else {
    const input = file ? JSON.parse(await readFile(file, 'utf8')) : {};
    const started = performance.now();
    const result = jsonResult(await client.callTool({ name: tool, arguments: input }));
    process.stdout.write(
      JSON.stringify(
        { ...result, durationMs: Math.round((performance.now() - started) * 10) / 10 },
        null,
        2,
      ) + '\n',
    );
    if (result.isError) process.exitCode = 1;
  }
} finally {
  await client.close();
}
