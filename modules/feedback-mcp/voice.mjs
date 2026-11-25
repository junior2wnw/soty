import { DatabaseSync } from 'node:sqlite';
import { request } from 'node:http';
import { existsSync } from 'node:fs';
import { join } from 'node:path';

function localWorker(socketPath, attachment, signal) {
  const bytes = Buffer.from(attachment.dataBase64, 'base64');
  if (!bytes.length || bytes.length > 1048576) throw Error('invalid_audio_length');
  return new Promise((resolve, reject) => {
    const call = request({ socketPath, path: '/transcribe', method: 'POST', signal, headers: {
      'content-type': attachment.mimeType, 'content-length': bytes.length,
    } }, async response => {
      try {
        if (response.statusCode !== 200) { response.resume(); throw Error('voice_worker_failed'); }
        let length = 0;
        const chunks = [];
        for await (const chunk of response) {
          length += chunk.length;
          if (length > 90000) throw Error('voice_response_too_long');
          chunks.push(chunk);
        }
        const { text } = JSON.parse(Buffer.concat(chunks).toString('utf8'));
        if (typeof text !== 'string' || !text.isWellFormed() || text.length > 16000) throw Error('voice_response_invalid');
        resolve(text.replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/gu, '').trim());
      } catch (error) { call.destroy(); reject(error); }
    });
    call.on('error', reject);
    call.end(bytes);
  });
}

// The caller verifies the current account and ticket ACL before every lookup,
// including cache hits. Only the private local worker receives recorded bytes.
export function createVoiceTranscripts(dataDirectory) {
  const socketPath = join(dataDirectory, 'voice-worker', 'worker.sock');
  const shutdown = new AbortController();
  const failures = new Map();
  let db, inFlight;
  function database() {
    if (!db) {
      db = new DatabaseSync(join(dataDirectory, 'voice-transcripts.sqlite'));
      db.exec(`PRAGMA journal_mode=DELETE; PRAGMA max_page_count=8192;
        CREATE TABLE IF NOT EXISTS transcripts(attachment_id TEXT PRIMARY KEY, text TEXT NOT NULL) STRICT;`);
    }
    return db;
  }
  return Object.freeze({
    get(attachment) {
      const id = attachment.id;
      if (!db && !existsSync(join(dataDirectory, 'voice-transcripts.sqlite')) && !existsSync(socketPath))
        return { status: 'unavailable' };
      const store = database();
      const cached = store.prepare('SELECT text FROM transcripts WHERE attachment_id=?').get(id);
      if (cached) return cached.text ? { status: 'ready', text: cached.text } : { status: 'no_speech' };
      if (inFlight) return { status: inFlight === id ? 'processing' : 'busy', retryAfterSeconds: 10 };
      if (!existsSync(socketPath)) return { status: 'unavailable' };
      if (failures.get(id) > Date.now()) return { status: 'failed' };
      inFlight = id;
      const signal = AbortSignal.any([shutdown.signal, AbortSignal.timeout(175000)]);
      void (async () => {
        try {
          const text = await localWorker(socketPath, attachment, signal);
          if (!shutdown.signal.aborted)
            store.prepare('INSERT OR IGNORE INTO transcripts VALUES(?,?)').run(id, text);
        } catch {
          for (const [key, expiry] of failures) if (expiry <= Date.now()) failures.delete(key);
          if (failures.size < 128) failures.set(id, Date.now() + 60000);
        } finally { inFlight = undefined; }
      })();
      return { status: 'processing', retryAfterSeconds: 10 };
    },
    close() { shutdown.abort(); db?.close(); },
  });
}
