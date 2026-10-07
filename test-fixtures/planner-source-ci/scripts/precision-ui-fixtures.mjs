import { randomUUID } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import { connectPlanner, jsonResult } from './mcp-client.mjs';
import { rangeFromNs, isoToNs } from '../shared/precise-time.ts';
import { YEAR, MS } from '../shared/universal-timeline.ts';

const origin = process.env.PLANNER_URL ?? 'http://127.0.0.1:4319';
const preview = new URL(origin);
if (
  preview.protocol !== 'http:' ||
  !['127.0.0.1', 'localhost', '[::1]'].includes(preview.hostname) ||
  preview.port !== '4319'
) {
  throw new Error('UI fixtures are allowed only on isolated localhost preview port 4319.');
}
const client = await connectPlanner({ url: origin, name: 'precision-ui-qa' });
const anchor = isoToNs('2026-10-03T12:00:00+05:00');
if (anchor === null) throw new Error('Invalid QA calendar anchor.');
const fixtures = [
  ['signal', 'QA · Сигнал', anchor + 2n * MS, anchor + 5n * MS],
  ['response', 'QA · Ответ', anchor + 6n * MS, anchor + 8n * MS],
  ['pulse', 'QA · Импульс', anchor + 13n * MS, anchor + 13n * MS + 100n],
  ['deepA', 'QA · Эпоха A', -2_000_000n * YEAR, -1_700_000n * YEAR],
  ['deepB', 'QA · Цикл B', -1_600_000n * YEAR, -1_300_000n * YEAR],
  ['deepC', 'QA · Эпоха C', -1_200_000n * YEAR, -500_000n * YEAR],
];
try {
  const result = jsonResult(
    await client.callTool({
      name: 'planner_apply',
      arguments: {
        requestId: randomUUID(),
        operations: fixtures.map(([key, title, start, end]) => ({
          op: 'create',
          collection: 'objects',
          workspace: 'personal',
          key,
          data: {
            title,
            kind: 'period',
            plan: {
              ...rangeFromNs(start, end, 'Asia/Yekaterinburg'),
              precise: { ...rangeFromNs(start, end).precise, resolutionNs: '1' },
            },
          },
        })),
      },
    }),
  );
  if (result.isError) throw new Error(JSON.stringify(result));
  mkdirSync('output/qa-precision-20261003', { recursive: true });
  const receipt = {
    origin,
    database: 'output/qa-precision-20261003/planner.sqlite',
    productionWrites: false,
    anchorNs: anchor.toString(),
    refs: result.refs,
    results: result.results,
  };
  writeFileSync('output/qa-precision-20261003/ui-fixtures.json', JSON.stringify(receipt, null, 2));
  console.log(JSON.stringify({ created: result.results.length, productionWrites: false }));
} finally {
  await client.close();
}
