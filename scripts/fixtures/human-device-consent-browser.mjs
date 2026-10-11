// Explicit synthetic browser gate. No live profile, config, account or source
// browser is used. CLI output (including auth URLs) stays private and bounded.
import { execFile } from 'node:child_process';
import { readFile, writeFile, mkdir, realpath } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { environment } from '../../modules/human-identity/test/support/renewal-fixture.mjs';

const cleanup = [], session = 'human-forget-' + process.pid;
let stage = 'configuration', fixture, cli, closed = false;
const requireGate = value => { if (!value) throw new Error('human_device_consent_browser_failed'); };
async function command(args) {
  let output, errors;
  try {
    const result = await new Promise((resolve, reject) => execFile(process.execPath, [cli, '-s=' + session, ...args], {
      cwd: fixture.directory, timeout: 30000, maxBuffer: 256 * 1024, encoding: 'buffer', windowsHide: true,
    }, (error, stdout, stderr) => { output = stdout; errors = stderr; if (error) reject(new Error('human_device_consent_browser_cli_failed'));
      else resolve(stdout); }));
    return /(?:^|\n)true(?:\r?\n|$)/u.test(result.toString('utf8'));
  } finally { output?.fill(0); errors?.fill(0); }
}
try {
  requireGate(process.argv.length === 4 && process.argv[2] === '--cli');
  cli = await realpath(process.argv[3]); requireGate(cli.replaceAll('\\', '/').endsWith('/node_modules/@playwright/cli/playwright-cli.js'));
  const metadata = JSON.parse(await readFile(join(dirname(cli), 'package.json'), 'utf8'));
  requireGate(metadata.name === '@playwright/cli' && metadata.version === '0.1.19');
  fixture = await environment({ after: fn => cleanup.push(fn) }, { deviceConsent: true });
  const config = join(fixture.directory, 'browser-config.json'), profile = join(fixture.directory, 'isolated-chrome-profile');
  await mkdir(profile); await writeFile(config, JSON.stringify({ browser: { browserName: 'chromium', launchOptions: { channel: 'chrome', headless: true } } }));
  stage = 'open'; await command(['open', fixture.rps[0].origin + '/login#forget', '--config=' + config, '--persistent', '--profile=' + profile]);
  const check = `location.hash === '#forget' && location.origin === '${fixture.origin}' && location.pathname.startsWith('/human-identity/interaction/') && document.title === 'Fixture root vault login'`;
  stage = 'redirect-fragment'; requireGate(await command(['eval', check]));
  stage = 'reload'; await command(['reload']); requireGate(await command(['eval', check]));
  stage = 'close'; await command(['close']); closed = true;
  console.log(JSON.stringify({ schema: 'soty.human-device-consent-browser.v1', passed: true, isolatedProfile: true, syntheticLoopbackIssuer: true,
    ordinaryRpLogin: true, issuerAuthorizeRedirect: true, forgetFragmentRetained: true, fragmentSurvivesReload: true,
    browserConsentUiTested: false, liveProfileUsed: false, authUrlsLogged: false, cliVersion: metadata.version }));
} catch {
  console.log(JSON.stringify({ schema: 'soty.human-device-consent-browser.v1', passed: false, stage, authUrlsLogged: false, liveProfileUsed: false }));
  process.exitCode = 1;
} finally {
  if (fixture && !closed) { try { await command(['close']); } catch { /* Only this named synthetic session. */ } }
  for (const close of cleanup.reverse()) await close();
}
