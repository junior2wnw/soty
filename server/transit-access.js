import { createDecipheriv, createHash, createPrivateKey, randomBytes, sign } from 'node:crypto';
import { closeSync, constants, fstatSync, openSync, readFileSync } from 'node:fs';
import path from 'node:path';

const profileRequest = Buffer.from('TRANSIT/SOTY-PROFILE/V1\0');
const profileDelivery = Buffer.from('TRANSIT/PROFILE-DELIVERY/V1\0');
const usageRequest = Buffer.from('TRANSIT/SOTY-USAGE/V1\0');
const usageDelivery = Buffer.from('TRANSIT/USAGE-DELIVERY/V1\0');
const subscriptionDelivery = Buffer.from('TRANSIT/SUBSCRIPTION-DELIVERY/V1\0');
const subscriptionProfileDelivery = Buffer.from('TRANSIT/SUBSCRIPTION-PROFILE-DELIVERY/V1\0');
const subscriptionFetchRequest = Buffer.from('TRANSIT/SOTY-SUBSCRIPTION-FETCH/V1\0');
const featuresRequest = Buffer.from('TRANSIT/SOTY-PROFILE-FEATURES/V1\0');
const featuresDelivery = Buffer.from('TRANSIT/PROFILE-FEATURES-DELIVERY/V1\0');
const subscriptionPrefix = '/transit/subscriptions/';
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
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
    const fields = ['schema', 'endpoint', 'issuer', 'privateKey', 'deliveryKey', 'appId'];
    if (value && Object.hasOwn(value, 'usageEnabled')) {
      if (typeof value.usageEnabled !== 'boolean') throw Error();
      fields.push('usageEnabled');
    }
    if (value && Object.hasOwn(value, 'subscriptionBaseUrl')) fields.push('subscriptionBaseUrl');
    if (!exact(value, fields) || value.schema !== 'soty.transit-access.v1') throw Error();
    const url = new URL(value.endpoint), issuer = new URL(value.issuer), key = createPrivateKey(value.privateKey);
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash || url.pathname !== '/v1/profile'
      || issuer.protocol !== 'https:' || issuer.origin !== value.issuer || key.asymmetricKeyType !== 'ed25519'
      || !/^app-[a-f0-9]{32}$/.test(value.appId)) throw Error();
    const deliveryKey = Buffer.from(value.deliveryKey, 'base64');
    if (deliveryKey.length !== 32 || deliveryKey.toString('base64') !== value.deliveryKey) throw Error();
    const subscriptionBaseUrl = value.subscriptionBaseUrl;
    if (subscriptionBaseUrl !== undefined && subscriptionBaseUrl !== issuer.origin + '/transit/subscriptions') throw Error();
    return Object.freeze({ endpoint: url.href, issuer: issuer.origin, key, deliveryKey, appId: value.appId,
      usageEnabled: value.usageEnabled, subscriptionBaseUrl });
  } catch (error) { if (!explicit && error.code === 'ENOENT') return null; deny('transit_access_configuration_invalid'); }
  finally { if (fd !== undefined) closeSync(fd); }
}

function validUsage(value) {
  if (value === null) return true;
  return exact(value, ['uploadBytes', 'downloadBytes', 'freshEntries', 'expectedEntries', 'latestObservationAt'])
    && [value.uploadBytes, value.downloadBytes].every(bytes => typeof bytes === 'string' && /^(0|[1-9]\d{0,23})$/.test(bytes))
    && Number.isSafeInteger(value.freshEntries) && Number.isSafeInteger(value.expectedEntries)
    && value.freshEntries >= 0 && value.expectedEntries > 0 && value.freshEntries <= value.expectedEntries
    && (value.latestObservationAt === null || (Number.isSafeInteger(value.latestObservationAt) && value.latestObservationAt >= 0));
}

/** Uses Connect's original authenticated actor. No caller-supplied account or
 * device can select another person's profile. Lost replies are safe to retry. */
export function createTransitAccess({ configuration, getConnect, fetchImpl = fetch, now = Date.now }) {
  let closed = false, inFlight = 0, windowStarted = now(), requests = 0;
  let featurePromise, featureValue = null, featureAt = 0, featureTtl = 0;
  const buckets = new Map(), abort = new AbortController();
  const current = (actor, args) => {
    if (closed) deny('transit_access_unavailable');
    if (!exact(args, ['expectedAccountId']) || args.expectedAccountId !== actor.accountId) deny('transit_access_context_changed');
    getConnect().withActorAuthorityFence(actor, () => true);
  };
  const exchange = async (pathname, payload, requestDomain, responseDomain, responseSchema, guard,
    retry = true, deadline = AbortSignal.any([abort.signal, AbortSignal.timeout(15_000)])) => {
    guard();
    const issuedAt = now(), body = JSON.stringify({ ...payload, issuer: configuration.issuer,
      issuedAt, expiresAt: issuedAt + 20_000, nonce: randomBytes(16).toString('hex') });
    const signature = sign(null, Buffer.concat([requestDomain, Buffer.from(body)]), configuration.key).toString('base64');
    let response;
    try {
      try {
        response = await fetchImpl(new URL(pathname, configuration.endpoint).href, { method: 'POST', redirect: 'error', cache: 'no-store',
          signal: deadline, headers: { 'content-type': 'application/json', 'x-transit-signature': signature }, body });
      } catch (error) {
        guard();
        // A restarted issuer can close an idle keep-alive socket. These are
        // idempotent control reads/URL management, never application payload.
        // Re-sign with a fresh nonce once, within the original total deadline.
        if (retry && !deadline.aborted && ['UND_ERR_SOCKET', 'ECONNRESET', 'ECONNREFUSED', 'EPIPE'].includes(error.cause?.code))
          return exchange(pathname, payload, requestDomain, responseDomain, responseSchema, guard, false, deadline);
        throw error;
      }
      guard();
      const parts = []; let length = 0;
      for await (const part of response.body) { length += part.length; if (length > 400_000) deny('transit_access_unavailable'); parts.push(part); }
      guard();
      const envelope = JSON.parse(Buffer.concat(parts));
      if (!response.ok) deny(envelope.error === 'access_capacity' ? 'transit_access_capacity'
        : envelope.error === 'access_subscription_not_found' ? 'transit_subscription_not_found' : 'transit_access_unavailable');
      if (!exact(envelope, ['schema', 'nonce', 'tag', 'ciphertext']) || envelope.schema !== responseSchema) deny('transit_access_unavailable');
      const decipher = createDecipheriv('aes-256-gcm', configuration.deliveryKey, Buffer.from(envelope.nonce, 'base64'));
      decipher.setAAD(Buffer.concat([responseDomain, Buffer.from(body)]));
      decipher.setAuthTag(Buffer.from(envelope.tag, 'base64'));
      const plaintext = Buffer.concat([decipher.update(Buffer.from(envelope.ciphertext, 'base64')), decipher.final()]);
      if (plaintext.length > 240_000) deny('transit_access_unavailable');
      return JSON.parse(plaintext);
    } finally { try { await response?.body?.cancel(); } catch {} }
  };
  const validProfile = value => value?.coreVersion === '1.13.15' && value.profile && typeof value.profile === 'object'
    && !Array.isArray(value.profile) && sha256(JSON.stringify(value.profile)) === value.sha256;
  const features = async () => {
    if (!configuration || closed) return null;
    if (featureTtl && now() >= featureAt && now() - featureAt < featureTtl) return featureValue;
    if (!featurePromise) featurePromise = (async () => {
      try {
        const value = await exchange('/v1/features', { schema: 'soty.transit-profile-features.v1' }, featuresRequest, featuresDelivery,
          'transit.encrypted-profile-features.v1', () => { if (closed) deny('transit_access_unavailable'); }, true,
          AbortSignal.any([abort.signal, AbortSignal.timeout(2500)]));
        if (!exact(value, ['schema', 'portableProfiles', 'subscriptions', 'usage', 'topology']) || value.schema !== 'transit.profile-features.v1'
          || ['portableProfiles', 'subscriptions', 'usage', 'topology'].some(name => typeof value[name] !== 'boolean')) deny('transit_access_unavailable');
        featureValue = Object.freeze(value); featureTtl = 30_000;
      } catch { featureValue = null; featureTtl = 2000; }
      featureAt = now(); return featureValue;
    })();
    try { return await featurePromise; } finally { featurePromise = null; }
  };
  const subscriptionBase = configuration?.subscriptionBaseUrl || (configuration ? configuration.issuer + '/transit/subscriptions' : null);
  const limited = tokenHash => {
    const time = now();
    if (time < windowStarted || time - windowStarted >= 60_000) { windowStarted = time; requests = 0; buckets.clear(); }
    if (++requests > 12_000 || inFlight >= 32) return true;
    const count = buckets.get(tokenHash) || 0;
    if (count >= 30) return true;
    // Bound memory even for random, unauthenticated URL probes.
    if (count === 0 && buckets.size >= 4096) return true;
    buckets.set(tokenHash, count + 1);
    return false;
  };
  return Object.freeze({
    operations: new Set(['transit.access.status', 'transit.access.profile', 'transit.access.usage',
      'transit.access.subscription', 'transit.access.subscription.rotate', 'transit.access.subscription.revoke']), maxResultBytes: 256_000,
    async executeAsync({ op, actor, args }) {
      current(actor, args);
      const available = await features(); current(actor, args);
      if (op === 'transit.access.status') return { enabled: !!configuration, appId: configuration?.appId ?? null,
        usageEnabled: configuration?.usageEnabled !== false && available?.usage === true, subscriptionEnabled: available?.subscriptions === true };
      const usage = op === 'transit.access.usage', actions = new Map([['transit.access.subscription', 'get'],
        ['transit.access.subscription.rotate', 'rotate'], ['transit.access.subscription.revoke', 'revoke']]), action = actions.get(op);
      if (!configuration || (usage ? configuration.usageEnabled === false || available?.usage !== true
        : action ? available?.subscriptions !== true : op !== 'transit.access.profile')) deny('transit_access_unavailable');
      try {
        const value = await exchange(action ? `/v1/subscription/${action}` : usage ? '/v1/usage' : available?.portableProfiles ? '/v1/profile/portable' : '/v1/profile',
          { schema: action ? `soty.transit-subscription-${action}.v1` : usage ? 'soty.transit-usage-request.v1' : 'soty.transit-access-request.v1',
            accountId: actor.accountId, deviceId: actor.deviceId },
          action ? Buffer.from(`TRANSIT/SOTY-SUBSCRIPTION-${action.toUpperCase()}/V1\0`) : usage ? usageRequest : profileRequest,
          action ? subscriptionDelivery : usage ? usageDelivery : profileDelivery,
          action ? 'transit.encrypted-subscription.v1' : usage ? 'transit.encrypted-usage.v1' : 'transit.encrypted-profile.v1', () => current(actor, args));
        current(actor, args);
        if (action) {
          if (!exact(value, ['schema', 'accountId', 'revoked', 'token']) || value.schema !== 'transit.personal-subscription.v1'
            || value.accountId !== actor.accountId || typeof value.revoked !== 'boolean'
            || (value.revoked ? value.token !== null : typeof value.token !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(value.token))) deny('transit_access_unavailable');
          return { subscription: { revoked: value.revoked, url: value.revoked ? null : `${subscriptionBase}/${value.token}/profile.json` } };
        }
        if (usage) {
          if (!exact(value, ['schema', 'accountId', 'usage']) || value.schema !== 'transit.personal-usage.v1'
            || value.accountId !== actor.accountId || !validUsage(value.usage)) deny('transit_access_unavailable');
          return { usage: value.usage };
        }
        if (value.schema !== 'transit.personal-profile.v1' || value.accountId !== actor.accountId
          || value.mode !== 'free-best-effort' || !validProfile(value)) deny('transit_access_unavailable');
        return { profile: value.profile, coreVersion: value.coreVersion, mode: value.mode };
      } catch (error) {
        current(actor, args);
        if (['transit_access_capacity', 'transit_access_context_changed'].includes(error.code)) throw error;
        deny('transit_access_unavailable');
      }
    },
    // Register before request collectors. Never include this secret-bearing URI
    // in application logs, metrics labels, errors or session persistence.
    handlesSubscription(req) { return typeof req.url === 'string' && req.url.startsWith(subscriptionPrefix); },
    async serveSubscription(req, res) {
      const reply = (status, value) => {
        if (res.destroyed) return;
        res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store, private',
          'referrer-policy': 'no-referrer', 'x-content-type-options': 'nosniff', 'content-security-policy': "default-src 'none'; frame-ancestors 'none'",
          ...(status === 429 ? { 'retry-after': '60' } : {}) });
        res.end(JSON.stringify(value));
      };
      const match = /^\/transit\/subscriptions\/([A-Za-z0-9_-]{43})\/profile\.json$/.exec(req.url);
      if (req.method !== 'GET' || !match || closed || !configuration) { reply(404, { error: 'not_found' }); return; }
      const tokenHash = sha256(match[1]);
      if (limited(tokenHash)) { reply(429, { error: 'rate_limited' }); return; }
      inFlight++;
      try {
        if ((await features())?.subscriptions !== true) { reply(503, { error: 'subscription_unavailable' }); return; }
        const value = await exchange('/v1/subscription/profile', { schema: 'soty.transit-subscription-fetch.v1', tokenHash },
          subscriptionFetchRequest, subscriptionProfileDelivery, 'transit.encrypted-subscription-profile.v1',
          () => { if (closed) deny('transit_access_unavailable'); });
        if (!exact(value, ['schema', 'coreVersion', 'profile', 'sha256']) || value.schema !== 'transit.subscription-profile.v1'
          || !validProfile(value)) deny('transit_access_unavailable');
        reply(200, value.profile);
      } catch (error) { reply(error.code === 'transit_subscription_not_found' ? 404 : 503,
        { error: error.code === 'transit_subscription_not_found' ? 'not_found' : 'subscription_unavailable' }); }
      finally { inFlight--; }
    },
    close() { closed = true; abort.abort(); buckets.clear(); },
  });
}
