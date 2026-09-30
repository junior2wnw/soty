// A bounded, sequential consumer of the existing SOTY_FILE stdout protocol.
// The callback resolves only after the relay ACK. That is not a recipient download receipt.
export function createCommandFileTransfer({ sendChunk, report, maxBytes, maxChunkBytes = 256000 }) {
  if (typeof sendChunk !== 'function' || typeof report !== 'function' || !Number.isSafeInteger(maxBytes) || maxBytes < 1) throw new TypeError('file_transfer_configuration');
  const streams = new Map();
  let tail = '', busy = false, closed = false;
  const fail = () => { throw new Error('file_transfer_incomplete'); };
  const decode = value => {
    if (!/^[A-Za-z0-9+/=_-]*$/u.test(value)) fail();
    const binary = atob(value.replaceAll('-', '+').replaceAll('_', '/'));
    return Uint8Array.from(binary, character => character.charCodeAt(0));
  };
  const json = value => JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(decode(value)));
  const validId = value => typeof value === 'string' && /^[A-Za-z0-9_-]{1,120}$/u.test(value);
  async function line(value) {
    if (!value.startsWith('SOTY_FILE_')) return false;
    if (value.startsWith('SOTY_FILE_BEGIN ')) {
      const meta = json(value.slice(16).trim());
      if (!meta || !validId(meta.id) || typeof meta.name !== 'string' || !meta.name.trim() || meta.name.length > 500
        || !Number.isSafeInteger(meta.size) || meta.size < 0 || meta.size > maxBytes
        || !Number.isSafeInteger(meta.total) || meta.total < 1 || meta.total > 8192
        || streams.has(meta.id) || streams.size >= 4) fail();
      const state = { fileId: meta.id, name: meta.name.replace(/[\\/:*?"<>|\u0000-\u001f]/gu, '_').slice(0, 120),
        type: typeof meta.type === 'string' ? meta.type.slice(0, 160) : 'application/octet-stream',
        size: meta.size, total: meta.total, autoDownload: meta.autoDownload === true,
        delivery: typeof meta.delivery === 'string' ? meta.delivery.slice(0, 80) : '', sent: 0, bytes: 0 };
      streams.set(meta.id, state); report({ state: 'started', name: state.name, size: state.size }); return true;
    }
    if (value.startsWith('SOTY_FILE_CHUNK ')) {
      const match = /^SOTY_FILE_CHUNK ([A-Za-z0-9_-]{1,120}) (\d{1,8}) ([+/=0-9A-Za-z]*)$/u.exec(value);
      if (!match || match[3].length > Math.ceil(maxChunkBytes / 3) * 4) fail();
      const state = streams.get(match[1]), index = Number(match[2]);
      if (!state || index !== state.sent || index >= state.total) fail();
      const chunk = decode(match[3]);
      if (chunk.byteLength > maxChunkBytes || state.bytes + chunk.byteLength > state.size) fail();
      await sendChunk(state.fileId, { name: state.name, type: state.type, size: state.size,
        autoDownload: state.autoDownload, delivery: state.delivery }, chunk, index, state.total);
      if (closed) fail();
      state.sent++; state.bytes += chunk.byteLength; return true;
    }
    if (value.startsWith('SOTY_FILE_END ')) {
      const end = json(value.slice(14).trim()), state = streams.get(end?.id);
      if (!state || state.sent !== state.total || state.bytes !== state.size) fail();
      // An optional sender hash is not presented as independently verified evidence.
      streams.delete(state.fileId); report({ state: 'stored', name: state.name, size: state.size }); return true;
    }
    fail();
  }
  return {
    async write(rawText, flush = false) {
      if (busy || closed || typeof rawText !== 'string' || rawText.length > 1000000) fail();
      busy = true;
      try {
        const lines = `${tail}${rawText}`.split('\n'); tail = lines.pop() || '';
        if (tail.length > 350000) fail();
        if (flush && tail) { lines.push(tail); tail = ''; }
        const visible = [];
        for (const value of lines) {
          if (value.length > 350000) fail();
          const clean = value.replace(/\r$/u, '');
          if (!await line(clean)) visible.push(clean);
        }
        if (flush && streams.size) fail();
        return visible.length ? `${visible.join('\n')}\n` : '';
      } catch (error) { closed = true; streams.clear(); tail = ''; throw error; }
      finally { busy = false; }
    },
    close() { closed = true; streams.clear(); tail = ''; },
    hasPending() { return busy || streams.size > 0; },
  };
}
