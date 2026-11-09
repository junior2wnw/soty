/** Upgrade only the managed projection of a historical profile. The issuer's
 * stable filename wins; old local JSON also gets a private credential-derived
 * file on the next explicit use, without changing peer keys or the saved file. */
export function createTransitCacheProjection({ normalize, hash }) {
 return function isolateLegacyTransitCache(input) {
  const profile = normalize(input), cache = profile.experimental?.cache_file;
  if (cache && !cache.path) {
    const identities = [...new Set(profile.outbounds.filter(row => ['vless', 'hysteria2'].includes(row.type))
      .map(row => JSON.stringify([row.type, row.uuid || row.password])))].sort();
    cache.path = 'transit-cache-' + hash('TRANSIT/LEGACY-PROFILE-CACHE/V1\0' + JSON.stringify(identities)) + '.db';
  }
  return profile;
 };
}

/** Call only below the Agent's already qualified private root (NTFS DACL on
 * Windows). Each probe owns its working directory, cache and configuration.
 * An unconfirmed process exit explicitly retains its directory for recovery. */
export function createTransitProbeWorkspace({ lstat, mkdtemp, realpath, rm, writeFile, join, relative, resolve, sep }) {
 return async function withTransitProbeWorkspace(parent, protectFile, operation) {
  const absolute = resolve(parent), info = await lstat(absolute);
  if (!info.isDirectory() || info.isSymbolicLink() || await realpath(absolute) !== absolute)
    throw new Error('transit-private-storage-failed');
  const root = await mkdtemp(join(absolute, 'probe-')), stamp = await lstat(root);
  let retained = false;
  try {
    const file = join(root, 'profile.json');
    return await operation({ root, retain() { retained = true; }, async write(profile) {
      await writeFile(file, JSON.stringify(profile) + '\n', { flag: 'wx', mode: 0o600 });
      await protectFile(file); return file;
    } });
  } finally {
    if (!retained) {
      const current = await lstat(root), local = relative(absolute, resolve(root));
      if (!local || local.startsWith('..' + sep) || local === '..' || resolve(root) !== await realpath(root)
        || !current.isDirectory() || current.isSymbolicLink() || current.dev !== stamp.dev || current.ino !== stamp.ino)
        throw new Error('transit-private-storage-failed');
      await rm(root, { recursive: true });
    }
  }
 };
}
