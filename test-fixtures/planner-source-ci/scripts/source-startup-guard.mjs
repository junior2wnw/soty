// Invoke on the selected release host before START; logs no payload/config values.
import { readLegacyPlannerRp } from './fixtures/planner-rp-v1-reader.literal.mjs';
import { readCompatiblePlannerRp } from './fixtures/planner-rp-v2-reader.literal.mjs';
const [reader, path, ...extra] = process.argv.slice(2);
if (!['1', '2'].includes(reader) || !path || extra.length)
  throw new Error('usage: node source-startup-guard.mjs <1|2> <database-path>');
try {
  const result = (reader === '1' ? readLegacyPlannerRp : readCompatiblePlannerRp)(path);
  process.stdout.write(JSON.stringify(result) + '\n');
} catch {
  process.stdout.write(
    JSON.stringify({ supported: false, code: 'planner_rp_reader_incompatible' }) + '\n',
  );
  process.exitCode = 78;
}
