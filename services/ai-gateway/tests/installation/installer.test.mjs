import test from 'node:test';
import assert from 'node:assert/strict';
import { renderInstaller, renderPowerShell, validateBaseUrl } from '../../packages/installation/installer.mjs';
const input = { baseUrl: 'https://inference.example.test', claim: 'a'.repeat(43), model: 'MiniMaxAI/MiniMax-M2.7' };
test('installer round trips unicode, parses payload from last marker and keeps command line bounded', () => {
  const cmd = renderInstaller(input).toString();
  const marker = cmd.lastIndexOf(':SOTY_PAYLOAD');
  const ps = Buffer.from(cmd.slice(marker + 13).trim(), 'base64').toString('utf8');
  assert.equal(ps, renderPowerShell(input));
  const command = cmd.split('\r\n').find(l=>l.startsWith('powershell.exe'));
  assert.ok(command.length < 8191);
  assert.match(command, /LastIndexOf/);
  assert.ok(!command.includes(input.claim));
  assert.match(ps, /ProtectedData/);
  assert.match(ps, /Get-FileHash/);
  assert.match(ps, /\/setup\/redeem/);
  assert.ok(!ps.includes('__PAYLOAD__'));
});
test('no plaintext HTTP or credentials in endpoint, malicious inputs never become code', () => {
  for (const baseUrl of ['http://example.org', 'https://user:pass@example.org', 'https://example.org/?key=x', 'javascript:alert(1)']) {
    assert.throws(()=>renderInstaller({...input,baseUrl}));
  }
  assert.equal(validateBaseUrl('http://localhost:8080'), 'http://localhost:8080');
  assert.throws(()=>renderInstaller({...input,model:"bad'; Get-Item *"}));
  assert.throws(()=>renderInstaller({...input,claim:'x'}));
});
