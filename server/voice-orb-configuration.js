import { openSync, closeSync, fstatSync, readSync, constants } from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { fields, canonical, parseJson } from '../modules/personal-agent/provider/validation.mjs';
import { createOpenAiOrbTransport, voiceOrbError } from './voice-orb-transport.js';
import { createVoiceOrbQuota } from './voice-orb-quota.js';

const invalid = () => { throw voiceOrbError('voice_orb_installation_invalid'); };
const need = value => { if (!value) invalid(); };
const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,160}$/u.test(value);
function privateFile(filename) {
  need(typeof filename === 'string' && path.isAbsolute(filename) && filename.length <= 4096 && !filename.includes('\0'));
  const bytes = Buffer.alloc(65537); let fd;
  try {
    fd = openSync(filename, constants.O_RDONLY | (constants.O_NONBLOCK || 0) | (constants.O_NOFOLLOW || 0));
    const before = fstatSync(fd); need(before.isFile() && before.size > 0 && before.size <= 65536);
    need(process.platform === 'win32' || before.uid === process.getuid() && (before.mode & 0o077) === 0);
    let used = 0;
    while (used < bytes.length) { const count = readSync(fd, bytes, used, bytes.length - used, used); if (!count) break; used += count; }
    const after = fstatSync(fd); need(used > 0 && used <= 65536 && before.dev === after.dev && before.ino === after.ino
      && before.size === after.size && before.mtimeMs === after.mtimeMs && before.ctimeMs === after.ctimeMs);
    return parseJson(new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(0, used)), { maxBytes: 65536, maxDepth: 8, maxNodes: 4096 });
  } catch { invalid(); }
  finally { bytes.fill(0); if (fd !== undefined) closeSync(fd); }
}

/** Real index.js→startup.js binding. Reads ONLY its explicitly selected private
 * voice profile when operator enablement is1. No Native/KUF/Gonka credential or
 * budget fallback, no model discovery call, no automatic grant/key enrollment. */
export function loadVoiceOrbConfiguration(env = process.env, { dataDir, fetchImpl = fetch, socketFactory } = {}) {
  let quota;
  try {
    const flag = env.SOTY_VOICE_ORB_ENABLED ?? ''; need(['', '0', '1'].includes(flag));
    if (flag !== '1') return { voiceOrb: { enabled: false }, close() {} };
    need(typeof dataDir === 'string' && path.isAbsolute(dataDir));
    const value = privateFile(env.SOTY_VOICE_ORB_CONFIG_FILE);
    // V2 is an explicitly installed operator service, never a reinterpretation
    // of V1 user profiles. The previous reader rejects this closed schema.
    if (value?.schema === 'soty.voice-orb-installed.v2') {
      fields(value, ['schema', 'service', 'admission'], ['maxDurationMs']);
      const maxDurationMs = value.maxDurationMs ?? 180000;
      need(Number.isSafeInteger(maxDurationMs) && maxDurationMs >= 15000 && maxDurationMs <= 900000);
      fields(value.admission, ['policy', 'connectProjectId']);
      need(value.admission.policy === 'soty-authenticated-accounts' && value.admission.connectProjectId === 'soty');
      const entry = value.service;
      fields(entry, ['tenantId', 'serviceId', 'apiKey', 'projectId', 'authorizationId', 'validUntil', 'maxStarts', 'maxAccountStarts']);
      need(['tenantId', 'serviceId', 'authorizationId'].every(name => id(entry[name]))
        && typeof entry.apiKey === 'string' && /^[^\s\u0000-\u001f]{20,512}$/u.test(entry.apiKey)
        && typeof entry.projectId === 'string' && /^proj_[A-Za-z0-9_-]{1,150}$/u.test(entry.projectId)
        && Number.isSafeInteger(entry.maxStarts) && entry.maxStarts >= 1 && entry.maxStarts <= 1000000
        && Number.isSafeInteger(entry.maxAccountStarts) && entry.maxAccountStarts >= 1 && entry.maxAccountStarts <= entry.maxStarts);
      const expiresAt = Date.parse(entry.validUntil);
      need(typeof entry.validUntil === 'string' && Number.isFinite(expiresAt) && expiresAt > Date.now());
      // tenantId is this operator's billing namespace. It grants no app-data
      // tenant membership, model change, Notes permission or spending approval.
      quota = createVoiceOrbQuota({ databasePath: path.join(dataDir, 'voice-orb-quota.sqlite'), service: Object.freeze({ ...entry, expiresAt }) });
      const own = actor => quota.profileFor(actor);
      const transport = createOpenAiOrbTransport({ resolveApiKey: async ({ actor }) => own(actor).apiKey,
        resolveProject: async ({ actor }) => own(actor).projectId, fetchImpl, ...(socketFactory ? { socketFactory } : {}) });
      return { voiceOrb: { enabled: true, maxDurationMs, transport, connectProjectId: value.admission.connectProjectId,
        admitQuota: intent => quota.admit(intent), quotaStatus: ({ actor }) => quota.statusFor(actor) }, close: () => quota.close() };
    }
    fields(value, ['schema', 'profiles'], ['maxDurationMs']); need(value.schema === 'soty.voice-orb-installed.v1');
    const maxDurationMs = value.maxDurationMs ?? 180000;
    need(Number.isSafeInteger(maxDurationMs) && maxDurationMs >= 15000 && maxDurationMs <= 900000);
    need(Array.isArray(value.profiles) && value.profiles.length >= 1 && value.profiles.length <= 128);
    const profiles = new Map(), keyOwners = new Map(), projectOwners = new Map();
    for (const entry of value.profiles) {
      fields(entry, ['tenantId', 'accountId', 'deviceId', 'apiKey', 'projectId', 'authorizationId', 'validUntil', 'maxStarts']);
      need(['tenantId', 'accountId', 'deviceId', 'authorizationId'].every(name => id(entry[name]))
        && typeof entry.apiKey === 'string' && /^[^\s\u0000-\u001f]{20,512}$/u.test(entry.apiKey)
        && typeof entry.projectId === 'string' && /^proj_[A-Za-z0-9_-]{1,150}$/u.test(entry.projectId)
        && Number.isSafeInteger(entry.maxStarts) && entry.maxStarts >= 1 && entry.maxStarts <= 1000);
      const expiresAt = Date.parse(entry.validUntil); need(typeof entry.validUntil === 'string' && Number.isFinite(expiresAt));
      const key = canonical({ accountId: entry.accountId, deviceId: entry.deviceId }), owner = canonical({ accountId: entry.accountId, tenantId: entry.tenantId, projectId: entry.projectId });
      const fingerprint = createHash('sha256').update(entry.apiKey).digest('hex');
      need(!profiles.has(key) && (!keyOwners.has(fingerprint) || keyOwners.get(fingerprint) === owner)
        && (!projectOwners.has(entry.projectId) || projectOwners.get(entry.projectId) === owner));
      keyOwners.set(fingerprint, owner); projectOwners.set(entry.projectId, owner); profiles.set(key, Object.freeze({ ...entry, expiresAt }));
    }
    quota = createVoiceOrbQuota({ databasePath: path.join(dataDir, 'voice-orb-quota.sqlite'), profiles });
    const own = actor => quota.profileFor(actor);
    const transport = createOpenAiOrbTransport({ resolveApiKey: async ({ actor }) => own(actor).apiKey,
      resolveProject: async ({ actor }) => own(actor).projectId, fetchImpl, ...(socketFactory ? { socketFactory } : {}) });
    return { voiceOrb: { enabled: true, maxDurationMs, transport,
      admitQuota: intent => quota.admit(intent), quotaStatus: ({ actor }) => quota.statusFor(actor) }, close: () => quota.close() };
  } catch { quota?.close(); invalid(); }
}
