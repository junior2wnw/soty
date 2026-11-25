import { execFile } from 'node:child_process';
import { createServer } from 'node:http';
import { lstat, mkdtemp, readFile, rm, unlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { promisify } from 'node:util';

const run = promisify(execFile);
const socketPath = '/run/feedback-voice/worker.sock';
const formats = new Map([['audio/webm', 'matroska'], ['audio/ogg', 'ogg']]);
const maxBytes = 1048576;
let busy = false;

async function transcribe(req, format, length) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > length) throw Error('invalid_audio_length');
    chunks.push(chunk);
  }
  if (size !== length) throw Error('invalid_audio_length');
  const dir = await mkdtemp('/tmp/feedback-voice-');
  try {
    const input = join(dir, 'recording'), wav = join(dir, 'audio.wav'), output = join(dir, 'transcript');
    await writeFile(input, Buffer.concat(chunks), { mode: 0o600 });
    await run('/usr/bin/ffmpeg', ['-hide_banner', '-loglevel', 'error', '-nostdin', '-protocol_whitelist', 'file',
      '-threads', '1', '-f', format, '-i', input, '-map', '0:a:0', '-vn', '-sn', '-dn',
      '-t', '120', '-ar', '16000', '-ac', '1', '-c:a', 'pcm_s16le', wav],
    { timeout: 15000, maxBuffer: 65536 });
    await run('/usr/local/bin/whisper-cli', ['-m', '/opt/feedback-voice/model.bin', '-f', wav,
      '-l', 'auto', '-t', '2', '-ng', '-nt', '-np', '-otxt', '-of', output], { timeout: 150000, maxBuffer: 65536 });
    const path = output + '.txt';
    const info = await lstat(path);
    if (info.size > 64000) throw Error('transcript_too_long');
    return (await readFile(path, 'utf8')).trim();
  } finally { await rm(dir, { recursive: true, force: true }); }
}

const server = createServer(async (req, res) => {
  const json = (status, body) => res.writeHead(status, { 'content-type': 'application/json', connection: 'close' }).end(JSON.stringify(body));
  if (req.method === 'GET' && req.url === '/health') return json(200, { ok: true });
  if (req.method !== 'POST' || req.url !== '/transcribe') return json(404, {});
  const length = Number(req.headers['content-length']), format = formats.get(req.headers['content-type']);
  if (!format || !Number.isSafeInteger(length) || length < 1 || length > maxBytes) return json(400, {});
  if (busy) return json(429, {});
  busy = true;
  try { json(200, { text: await transcribe(req, format, length) }); }
  catch { if (!res.destroyed) json(422, {}); }
  finally { busy = false; }
});
server.headersTimeout = 5000;
server.requestTimeout = 10000;
server.maxConnections = 8;
process.umask(0o077);
try {
  if (!(await lstat(socketPath)).isSocket()) throw Error('invalid_socket');
  await unlink(socketPath);
} catch (error) { if (error.code !== 'ENOENT') throw error; }
server.listen(socketPath);
