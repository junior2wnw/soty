import { readFileSync } from 'node:fs';

export const INSTALLER_VERSION = '1.18.31';
const template = readFileSync(new URL('./windows.ps1', import.meta.url), 'utf8');

export function validateBaseUrl(value) {
  const url = new URL(value);
  if (url.username || url.password || url.search || url.hash || (url.protocol !== 'https:' && !(url.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)))) {
    throw new Error('Installation requires HTTPS or loopback');
  }
  return url.href.replace(/\/$/, '');
}

export function renderPowerShell({ baseUrl, claim, model }) {
  const url = validateBaseUrl(baseUrl);
  if (!/^[A-Za-z0-9_-]{43}$/.test(claim)) throw new Error('Invalid installation claim');
  if (typeof model !== 'string' || model.length > 180 || !/^[\w./:-]+$/.test(model)) throw new Error('Invalid model');
  const payload = Buffer.from(JSON.stringify({ baseUrl: url, claim, model })).toString('base64');
  return template.replace('__PAYLOAD__', payload);
}

// The command stays below cmd.exe's 8191-byte line limit. The payload is read
// from this file, never interpolated into a shell command or a URL query.
export function renderInstaller(input) {
  const encoded = Buffer.from(renderPowerShell(input), 'utf8').toString('base64');
  return Buffer.from([
    '@echo off', 'setlocal', 'set "SOTY_SETUP_FILE=%~f0"',
    'powershell.exe -NoLogo -NoProfile -Command "$s=[IO.File]::ReadAllText($env:SOTY_SETUP_FILE);$b=$s.Substring($s.LastIndexOf(\':SOTY_PAYLOAD\')+13).Trim();& ([ScriptBlock]::Create([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($b))))"',
    'set "SOTY_SETUP_RESULT=%errorlevel%"', 'if not "%SOTY_SETUP_RESULT%"=="0" pause',
    'exit /b %SOTY_SETUP_RESULT%', ':SOTY_PAYLOAD', encoded, '',
  ].join('\r\n'), 'utf8');
}
