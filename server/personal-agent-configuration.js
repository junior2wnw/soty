import { openSync, closeSync, fstatSync, readSync, constants } from 'node:fs';
import path from 'node:path';
import { TextDecoder } from 'node:util';
import { fields, identifier, parseJson } from '../modules/personal-agent/provider/validation.mjs';
import { agentError } from './personal-agent-transport.js';

const invalid = () => { throw agentError('agent_gateway_configuration_invalid'); };
const need = value => { if (!value) invalid(); };
const field = (env, name) => { const value = env[name]; need(value === undefined || typeof value === 'string'); return value ?? ''; };
function privateFile(filename) {
  need(path.isAbsolute(filename) && filename.length <= 4096 && !filename.includes('\0'));
  const bytes = Buffer.alloc(65537); let fd;
  try {
    fd = openSync(filename, constants.O_RDONLY | (constants.O_NONBLOCK || 0) | (constants.O_NOFOLLOW || 0));
    const stat = fstatSync(fd); need(stat.isFile() && stat.size > 0 && stat.size <= 65536);
    need(process.platform === 'win32' || (stat.mode & 0o077) === 0);
    let used = 0;
    while (used < bytes.length) { const count = readSync(fd, bytes, used, bytes.length - used, used); if (!count) break; used += count; }
    need(used > 0 && used <= 65536);
    return parseJson(new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(0, used)), { maxBytes: 65536, maxDepth: 8, maxNodes: 2048 });
  } catch { invalid(); }
  finally { bytes.fill(0); if (fd !== undefined) closeSync(fd); }
}
async function receiptJson(response) {
  if (response.status !== 200 || !response.body) throw agentError('agent_accounting_unknown');
  const reader = response.body.getReader(); let size = 0; const chunks = [];
  try {
    while (true) {
      const { done, value } = await reader.read(); if (done) break;
      size += value.byteLength; if (size > 32768) throw agentError('agent_accounting_unknown'); chunks.push(value);
    }
    const body = Buffer.concat(chunks.map(value => Buffer.from(value)));
    const packet = parseJson(new TextDecoder('utf-8', { fatal: true }).decode(body), { maxBytes: 32768, maxDepth: 8, maxNodes: 1024 });
    fields(packet, ['request']);
    const request = { ...packet.request };
    // PostgreSQL bigint counters arrive as decimal strings from the management
    // API. Adapt that wire format once; the transport still checks exact usage.
    for (const name of ['input_tokens', 'output_tokens']) {
      const value = request[name];
      if (typeof value !== 'string') continue;
      const number = /^(?:0|[1-9]\d{0,15})$/u.test(value) ? Number(value) : NaN;
      if (!Number.isSafeInteger(number)) throw agentError('agent_accounting_unknown');
      request[name] = number;
    }
    return request;
  } catch { throw agentError('agent_accounting_unknown'); }
  finally { try { await reader.cancel(); } catch {} reader.releaseLock(); }
}

/** Actual process-entry binding. Default OFF; no environment dump or credential generation.
 * The file is an explicit installed host billing/account allowlist, never proof of a live
 * Root actor, local device, Source grant or commerce key. Those services check current authority. */
export function loadPersonalAgentConfiguration(env = process.env, { fetchImpl = fetch, accountAiMode = false } = {}) {
  try {
    const flag = field(env, 'SOTY_PERSONAL_AGENT_ENABLED'); need(['', '0', '1'].includes(flag));
    if (flag !== '1' && !accountAiMode) return { personalAgent: { enabled: false } };
    if (accountAiMode && !field(env, 'SOTY_PERSONAL_AGENT_CONFIG_FILE')) return { personalAgent: { enabled: true, historyEnabled: true } };
    const value = privateFile(field(env, 'SOTY_PERSONAL_AGENT_CONFIG_FILE'));
    fields(value, ['schema', 'gateway', 'accountIds'], ['localTargets', 'capacity', 'sourceAccess']); need(value.schema === 'soty.personal-agent-installed.v1');
    let agentSourceAccess;
    if (value.sourceAccess !== undefined) {
      fields(value.sourceAccess, ['schema', 'keyId', 'key']);
      need(value.sourceAccess.schema === 1 && typeof value.sourceAccess.keyId === 'string' && /^[\w.-]{1,64}$/u.test(value.sourceAccess.keyId)
        && typeof value.sourceAccess.key === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value.sourceAccess.key));
      const key = Buffer.from(value.sourceAccess.key, 'base64url');
      need(key.length === 32 && key.toString('base64url') === value.sourceAccess.key);
      agentSourceAccess = { key, keyId: value.sourceAccess.keyId };
    }
    let capacity;
    if (value.capacity !== undefined) {
      fields(value.capacity, [], ['maxAccountContexts', 'maxRetainedTurns']);
      need(Object.values(value.capacity).every(limit => Number.isSafeInteger(limit) && limit >= 1 && limit <= 65536));
      capacity = Object.freeze({ ...value.capacity });
    }
    if (accountAiMode) {
      const targets = new Map(), localTargets = value.localTargets ?? [];
      need(Array.isArray(localTargets) && localTargets.length <= 128);
      for (const entry of localTargets) {
        fields(entry, ['accountId', 'hostDeviceId', 'connectorId', 'cwd']); identifier(entry.accountId, 96); identifier(entry.hostDeviceId, 180); identifier(entry.connectorId, 180);
        need(!targets.has(entry.accountId) && typeof entry.cwd === 'string' && entry.cwd.length <= 2000 && entry.cwd === entry.cwd.trim()
          && !/[\u0000-\u001f]/u.test(entry.cwd) && (path.posix.isAbsolute(entry.cwd) || path.win32.isAbsolute(entry.cwd)));
        targets.set(entry.accountId, Object.freeze({ hostDeviceId: entry.hostDeviceId, connectorId: entry.connectorId, cwd: entry.cwd }));
      }
      // Preserve installed tools, encrypted Sources and capacity. Shared billing
      // secrets and account admission do not participate in account AI mode.
      return { ...(agentSourceAccess ? { agentSourceAccess } : {}), personalAgent: { enabled: true, historyEnabled: true,
        ...(capacity ? { capacity } : {}), ...(targets.size ? { localTargetFor: ({ actor }) => targets.get(actor.accountId) ?? null } : {}) } };
    }
    fields(value.gateway, ['origin', 'billingTenant', 'inferenceBearer', 'managementSession']);
    const gateway = value.gateway, origin = new URL(gateway.origin);
    if (agentSourceAccess) need(value.sourceAccess.key !== gateway.inferenceBearer && value.sourceAccess.key !== gateway.managementSession);
    need(origin.protocol === 'https:' && origin.origin === gateway.origin && !origin.username && !origin.password && !origin.search && !origin.hash);
    identifier(gateway.billingTenant, 160);
    need(typeof gateway.inferenceBearer === 'string' && /^[^\s\u0000-\u001f]{20,512}$/u.test(gateway.inferenceBearer)
      && typeof gateway.managementSession === 'string' && /^[A-Za-z0-9_-]{20,256}$/u.test(gateway.managementSession));
    need(Array.isArray(value.accountIds) && value.accountIds.length >= 1 && value.accountIds.length <= 128);
    const accounts = new Set(value.accountIds.map(id => identifier(id, 96))); need(accounts.size === value.accountIds.length);
    const targets = new Map(), localTargets = value.localTargets ?? [];
    need(Array.isArray(localTargets) && localTargets.length <= 128);
    for (const entry of localTargets) {
      fields(entry, ['accountId', 'hostDeviceId', 'connectorId', 'cwd']); identifier(entry.hostDeviceId, 180); identifier(entry.connectorId, 180);
      need(accounts.has(entry.accountId) && !targets.has(entry.accountId) && typeof entry.cwd === 'string' && entry.cwd.length <= 2000
        && entry.cwd === entry.cwd.trim() && !/[\u0000-\u001f]/u.test(entry.cwd)
        && (path.posix.isAbsolute(entry.cwd) || path.win32.isAbsolute(entry.cwd)));
      targets.set(entry.accountId, Object.freeze({ hostDeviceId: entry.hostDeviceId, connectorId: entry.connectorId, cwd: entry.cwd }));
    }
    const keyRef = Object.freeze({}), billingTenant = gateway.billingTenant;
    return { ...(agentSourceAccess ? { agentSourceAccess } : {}), personalAgent: { enabled: true, historyEnabled: true,
      ...(capacity ? { capacity } : {}),
      ...(targets.size ? { localTargetFor: ({ actor }) => targets.get(actor.accountId) ?? null } : {}),
      gateway: { gatewayOrigin: gateway.origin, billingTenant, keyRef,
        authorizeActor: ({ actor, billingTenant: tenant, keyRef: ref }) => tenant === billingTenant && ref === keyRef && accounts.has(actor.accountId),
        resolveCredential: async ref => { if (ref !== keyRef) throw agentError('agent_key_binding_invalid');
          return { keyRef, tenantId: billingTenant, bearer: gateway.inferenceBearer }; },
        async readReceipt({ requestId, billingTenant: tenant, keyRef: ref, signal }) {
          if (signal?.aborted) throw agentError('agent_cancelled');
          if (tenant !== billingTenant || ref !== keyRef || typeof requestId !== 'string'
            || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu.test(requestId)) throw agentError('agent_key_binding_invalid');
          // Existing gateway managementGuard +tenant-qualified SELECT independently scope this receipt.
          const response = await fetchImpl(`${gateway.origin}/api/requests/${requestId}`, { redirect: 'error', signal,
            headers: { accept: 'application/json', cookie: `soty_workspace=${gateway.managementSession}` } });
          return { tenantId: billingTenant, request: await receiptJson(response) };
        }, fetchImpl },
    } };
  } catch { invalid(); }
}
