import { createDecipheriv, createHash, createPrivateKey, randomBytes, sign } from 'node:crypto';
import { closeSync, constants, fstatSync, openSync, readFileSync } from 'node:fs';
import path from 'node:path';

const domain = Buffer.from('TRANSIT/SOTY-PROFILE/V1\0');
const deny = code => { throw Object.assign(new Error(code), { code }); };
const exact = (value, fields) => value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).sort().join(',') === [...fields].sort().join(',');

export function loadTransitAccessConfiguration(env = {}, { dataDir } = {}) {
  const explicit = env.SOTY_TRANSIT_ACCESS_CONFIG_FILE;
  const filename = explicit || (dataDir ? path.join(dataDir, 'transit-access.json') : null);
  if (!filename) return null;
  let fd;
  try {
    fd = openSync(filename, constants.O_RDONLY | (constants.O_NOFOLLOW || 0));
    const stat = fstatSync(fd);
    if (!stat.isFile() || stat.size > 16_384) throw Error();
    const raw = readFileSync(fd, 'utf8'); if (Buffer.byteLength(raw) > 16_384) throw Error();
    const value = JSON.parse(raw);
    if (!exact(value, ['schema', 'endpoint', 'issuer', 'privateKey', 'deliveryKey', 'appId']) || value.schema !== 'soty.transit-access.v1') throw Error();
    const url = new URL(value.endpoint), issuer = new URL(value.issuer), key = createPrivateKey(value.privateKey);
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash || url.pathname !== '/v1/profile'
      || issuer.protocol !== 'https:' || issuer.origin !== value.issuer || key.asymmetricKeyType !== 'ed25519'
      || !/^app-[a-f0-9]{32}$/.test(value.appId)) throw Error();
    const deliveryKey = Buffer.from(value.deliveryKey, 'base64');
    if (deliveryKey.length !== 32 || deliveryKey.toString('base64') !== value.deliveryKey) throw Error();
    return Object.freeze({ endpoint: url.href, issuer: issuer.origin, key, deliveryKey, appId: value.appId });
  } catch (error) { if (!explicit && error.code === 'ENOENT') return null; deny('transit_access_configuration_invalid'); }
  finally { if (fd !== undefined) closeSync(fd); }
}

/** Uses Connect's original authenticated actor. No caller-supplied account or
 * device can select another person's profile. Lost replies are safe to retry. */
export function createTransitAccess({ configuration, getConnect, fetchImpl = fetch, now = Date.now }) {
  let closed = false;
  const abort = new AbortController();
  const current = (actor, args) => {
    if (closed) deny('transit_access_unavailable');
    if (!exact(args, ['expectedAccountId']) || args.expectedAccountId !== actor.accountId) deny('transit_access_context_changed');
    getConnect().withActorAuthorityFence(actor, () => true);
  };
  return Object.freeze({
    operations: new Set(['transit.access.status', 'transit.access.profile']), maxResultBytes: 256_000,
    async executeAsync({ op, actor, args }) {
      current(actor, args);
      if (op === 'transit.access.status') return { enabled: !!configuration, appId: configuration?.appId ?? null };
      if (op !== 'transit.access.profile' || !configuration) deny('transit_access_unavailable');
      const issuedAt = now(), body = JSON.stringify({ schema: 'soty.transit-access-request.v1', issuer: configuration.issuer,
        accountId: actor.accountId, deviceId: actor.deviceId, issuedAt, expiresAt: issuedAt + 20_000, nonce: randomBytes(16).toString('hex') });
      const signature = sign(null, Buffer.concat([domain, Buffer.from(body)]), configuration.key).toString('base64');
      let response;
      try {
        response = await fetchImpl(configuration.endpoint, { method: 'POST', redirect: 'error', cache: 'no-store',
          signal: AbortSignal.any([abort.signal, AbortSignal.timeout(15_000)]),
          headers: { 'content-type': 'application/json', 'x-transit-signature': signature }, body });
        current(actor, args);
        const parts = []; let length = 0;
        for await (const part of response.body) { length += part.length; if (length > 400_000) deny('transit_access_unavailable'); parts.push(part); }
        current(actor, args);
        const envelope = JSON.parse(Buffer.concat(parts));
        if (!response.ok) deny(envelope.error === 'access_capacity' ? 'transit_access_capacity' : 'transit_access_unavailable');
        if (envelope.schema !== 'transit.encrypted-profile.v1') deny('transit_access_unavailable');
        const decipher = createDecipheriv('aes-256-gcm', configuration.deliveryKey, Buffer.from(envelope.nonce, 'base64'));
        decipher.setAAD(Buffer.concat([Buffer.from('TRANSIT/PROFILE-DELIVERY/V1\0'), Buffer.from(body)]));
        decipher.setAuthTag(Buffer.from(envelope.tag, 'base64'));
        const plaintext = Buffer.concat([decipher.update(Buffer.from(envelope.ciphertext, 'base64')), decipher.final()]);
        if (plaintext.length > 240_000) deny('transit_access_unavailable');
        const value = JSON.parse(plaintext);
        if (value.schema !== 'transit.personal-profile.v1' || value.accountId !== actor.accountId || value.coreVersion !== '1.13.15'
          || value.mode !== 'free-best-effort' || createHash('sha256').update(JSON.stringify(value.profile)).digest('hex') !== value.sha256) deny('transit_access_unavailable');
        return { profile: value.profile, coreVersion: value.coreVersion, mode: value.mode };
      } catch (error) {
        current(actor, args);
        if (['transit_access_capacity', 'transit_access_context_changed'].includes(error.code)) throw error;
        deny('transit_access_unavailable');
      } finally { try { await response?.body?.cancel(); } catch {} }
    },
    close() { closed = true; abort.abort(); },
  });
}
