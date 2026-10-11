// Test-only, explicitly preloaded in an owned child. Production modules never load this hook.
import { readFileSync, writeFileSync, lstatSync } from 'node:fs';
import { dirname, resolve, basename, isAbsolute } from 'node:path';
import { tmpdir } from 'node:os';
export function installFixtureClock(filename) {
  const ownedDirectory=dirname(resolve(filename)),temporaryRoot=resolve(tmpdir());
  if (!isAbsolute(filename) || !(dirname(ownedDirectory)===temporaryRoot||ownedDirectory===temporaryRoot)
    || basename(filename)!=='clock.txt' || !/^hive-(?:soty|selected)-actual-/u.test(basename(ownedDirectory))
    || !lstatSync(filename).isFile()) throw new Error('fixture_clock_path_invalid');
  const NativeDate = globalThis.Date;
  const offset = () => { const value = Number(readFileSync(filename, 'utf8'));
    if (!Number.isSafeInteger(value) || value < 0 || value > 25 * 60 * 60 * 1000) throw new Error('fixture_clock_invalid'); return value; };
  globalThis.Date = class FixtureDate extends NativeDate {
    constructor(...args) { if (args.length) super(...args); else super(NativeDate.now() + offset()); }
    static now() { return NativeDate.now() + offset(); }
  };
  return Object.freeze({ advance(milliseconds) { writeFileSync(filename, String(offset() + milliseconds)); },
    restore() { globalThis.Date = NativeDate; } });
}
if (process.env.HIVE_TEST_CLOCK_FILE) installFixtureClock(process.env.HIVE_TEST_CLOCK_FILE);
