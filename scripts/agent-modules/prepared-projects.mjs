// This module is dependency-injected so the connector release can bundle it.
// It never executes project code or treats a pathname as authorization.
export function createPreparedProjectBrowser(deps) {
  const fail = code => { throw Object.assign(new Error(code), { code }); };
  const need = (ok, code) => { if (!ok) fail(code); };
  const validSegment = value => typeof value === 'string' && value.length > 0 && value.length <= 255
    && value.isWellFormed() && value !== '.' && value !== '..' && !/[\\/\u0000-\u001f\u007f]/u.test(value);
  const exact = (value, keys) => value && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).length === keys.length && Object.keys(value).every(key => keys.includes(key));
  const within = (root, target) => { const rel = deps.relative(root, target);
    return rel === '' || rel !== '..' && !rel.startsWith(`..${deps.sep}`) && !deps.isAbsolute(rel); };

  async function roots() {
    const configured = await deps.allowedRoots();
    need(Array.isArray(configured) && configured.length > 0 && configured.length <= 16, 'prepared_roots_unavailable');
    const values = [];
    for (const item of configured) {
      if (typeof item !== 'string' || !deps.isAbsolute(item)) continue;
      try {
        const root = await deps.realpath(item), stat = await deps.lstat(root);
        if (!stat.isDirectory() || stat.isSymbolicLink()) continue;
        const id = deps.digest(root);
        if (!values.some(value => value.id === id)) values.push({ id, root, label: deps.basename(root) || root });
      } catch { /* A missing configured root is not a new authorization. */ }
    }
    need(values.length > 0, 'prepared_roots_unavailable'); return values;
  }
  async function resolveLocation(location) {
    need(exact(location, ['rootId', 'segments']) && typeof location.rootId === 'string' && /^[a-f0-9]{64}$/u.test(location.rootId)
      && Array.isArray(location.segments) && location.segments.length <= 32 && location.segments.every(validSegment)
      && new TextEncoder().encode(JSON.stringify(location)).byteLength <= 4096, 'prepared_location_invalid');
    const root = (await roots()).find(value => value.id === location.rootId);
    need(root, 'prepared_root_changed'); let current = root.root;
    for (const segment of location.segments) {
      const next = deps.join(current, segment), info = await deps.lstat(next);
      need(info.isDirectory() && !info.isSymbolicLink(), 'prepared_directory_invalid');
      const resolved = await deps.realpath(next); need(within(root.root, resolved), 'prepared_directory_invalid'); current = resolved;
    }
    return { root, path: current };
  }
  async function manifest(path) {
    try {
      const folder = deps.join(path, '.soty'), dir = await deps.lstat(folder);
      need(dir.isDirectory() && !dir.isSymbolicLink(), 'prepared_manifest_invalid');
      const file = deps.join(folder, 'app.json'), before = await deps.lstat(file);
      need(before.isFile() && !before.isSymbolicLink() && before.size >= 2 && before.size <= 4096, 'prepared_manifest_invalid');
      const resolved = await deps.realpath(file); need(within(path, resolved), 'prepared_manifest_invalid');
      const handle = await deps.open(resolved, 'r'); let bytes;
      try {
        const opened = await handle.stat();
        need(opened.isFile() && opened.size === before.size && opened.ino === before.ino && opened.dev === before.dev, 'prepared_manifest_changed');
        const buffer = new Uint8Array(before.size + 1); let used = 0;
        while (used < buffer.length) { const read = await handle.read(buffer, used, buffer.length - used, null); if (!read.bytesRead) break; used += read.bytesRead; }
        const final = await handle.stat();
        need(used === before.size && final.size === before.size && final.mtimeMs === before.mtimeMs
          && final.ino === before.ino && final.dev === before.dev, 'prepared_manifest_changed');
        bytes = buffer.subarray(0, used);
      } finally { await handle.close(); }
      const after = await deps.lstat(file);
      need(after.isFile() && !after.isSymbolicLink() && after.size === before.size && after.mtimeMs === before.mtimeMs
        && after.ino === before.ino && after.dev === before.dev, 'prepared_manifest_changed');
      let text; try { text = new TextDecoder('utf-8', { fatal: true }).decode(bytes); } catch { fail('prepared_manifest_invalid'); }
      const value = deps.normalizeManifest(deps.parseJson(text, { bytes: 4096 }), deps.blockedPorts);
      return { value, digest: deps.digest(JSON.stringify(value)) };
    } catch (error) { if (error?.code === 'ENOENT') return null; throw error; }
  }
  async function list({ location = null, after = '' } = {}) {
    if (location === null) return { schema: 'soty.prepared-directories.v1', roots: (await roots()).map(({ id, label }) => ({ id, label })) };
    need(typeof after === 'string' && (after === '' || validSegment(after)), 'prepared_cursor_invalid');
    const selected = await resolveLocation(location), names = [], directory = await deps.opendir(selected.path); let count = 0;
    for await (const entry of directory) {
      need(++count <= 4096, 'prepared_directory_too_large');
      if (entry.isDirectory() && !entry.isSymbolicLink() && entry.name !== '.soty' && validSegment(entry.name)) names.push(entry.name);
    }
    names.sort((a, b) => a < b ? -1 : a > b ? 1 : 0);
    const matching = names.filter(name => name > after), page = []; let pageBytes = 0;
    for (const name of matching) {
      const bytes = new TextEncoder().encode(JSON.stringify(name)).byteLength;
      if (page.length === 100 || pageBytes + bytes > 16 * 1024) break;
      page.push(name); pageBytes += bytes;
    }
    let found = null, projectError = null;
    try { found = await manifest(selected.path); } catch { projectError = 'unreadable'; }
    let responding = false;
    if (found) responding = await deps.probe(found.value);
    // Recheck the directory after asynchronous enumeration/probing.
    const again = await resolveLocation(location); need(again.path === selected.path, 'prepared_directory_changed');
    return { schema: 'soty.prepared-directories.v1', location, entries: page,
      next: matching.length > page.length ? page.at(-1) : null,
      project: found ? { name: found.value.name, digest: found.digest, responding } : null, projectError };
  }
  async function inspect({ location, digest } = {}) {
    need(typeof digest === 'string' && /^[a-f0-9]{64}$/u.test(digest), 'prepared_request_invalid');
    const selected = await resolveLocation(location), found = await manifest(selected.path);
    need(found && found.digest === digest, 'prepared_manifest_changed');
    need(await deps.probe(found.value), 'prepared_app_not_responding');
    const again = await resolveLocation(location), latest = await manifest(again.path);
    need(again.path === selected.path && latest && latest.digest === digest, 'prepared_manifest_changed');
    return found.value;
  }
  return Object.freeze({ list, inspect });
}
