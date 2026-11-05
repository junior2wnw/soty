// Frozen reader/writer before selection-result schema3; source SHA256 12e73ae6240fa3dc82830c7a312e1a4388ce5677391d8393d7fc827346a892bf

const fail = code => { throw Object.assign(new Error(code), { code }); };
const clone = value => structuredClone(value);
const appPattern = /^app-[a-f0-9]{32}$/u;
const audiences = ['private', 'link', 'public','community'];
const identifier = value => typeof value === 'string' && value.length > 0 && value.length <= 160 && !/[\u0000-\u001f]/u.test(value);
const check = (value, code = 'app_connect_invalid_state') => { if (!value) fail(code); };
const same = (left, right) => JSON.stringify(left) === JSON.stringify(right);

const selfIPv4 = value => {
  if (!/^(?:0|[1-9][0-9]{0,2})(?:\.(?:0|[1-9][0-9]{0,2})){3}$/u.test(value)) return false;
  const parts = value.split('.').map(Number), [a,b] = parts;
  return parts.every(item=>item<=255) && a!==0 && a!==127 && a<224 && !(a===169&&b===254);
};
/** A numeric IP still requires the selected owned connector's fresh OS inventory. */
export function parseAppConnectAddress(input) {
  check(typeof input === 'string' && input.trim().length <= 8192, 'app_connect_invalid_address');
  let text = input.trim(); if (/^\d+$/u.test(text)) text = `127.0.0.1:${text}`;
  let url; try { url = new URL(text.includes('://') ? text : `http://${text}`); } catch { fail('app_connect_invalid_address'); }
  check(!url.username && !url.password && !text.includes('#') && !/[\\\s]/u.test(text), 'app_connect_invalid_address');
  check(![...url.searchParams.keys()].some(key=>/^(?:token|key|secret|password|auth|authorization|code|session|api[_-]?key|access_token|refresh_token)$/iu.test(key)), 'app_connect_credentials_in_address');
  const rawHost = text.replace(/^[A-Za-z][A-Za-z0-9+.-]*:\/\//u,'').split(/[/?]/u)[0].split(':')[0];
  const loopback = ['localhost', '127.0.0.1'].includes(rawHost);
  check(loopback || selfIPv4(rawHost) && url.hostname === rawHost, 'app_connect_source_host_unsupported');
  check(url.protocol === 'http:', 'app_connect_source_protocol_unsupported');
  const port = Number(url.port); check(Number.isSafeInteger(port) && port >= 1024 && port <= 65535 && port !== 49424, 'app_connect_invalid_port');
  const entryPath = url.pathname + url.search; let decoded;
  try { decoded = decodeURIComponent(url.pathname); } catch { fail('app_connect_invalid_address'); }
  check(!decoded.startsWith('//') && !/[\\\u0000-\u0020\u007f]/u.test(decoded) && !/^\/_soty(?:\/|$)/u.test(decoded), 'app_connect_invalid_address');
  return { port, entryPath, address: `http://${loopback?'127.0.0.1':rawHost}:${port}${entryPath}`,
    ...(!loopback ? { profile:'soty.relay-self-ip.v1', endpoint:{scope:'connector-self',scheme:'http',family:'IPv4',address:rawHost} } : {}) };
}

function intent(input) {
  check(input && identifier(input.hostDeviceId) && identifier(input.connectorId), 'app_connect_device_required');
  check(typeof input.name === 'string' && input.name.trim().length > 0 && input.name.trim().length <= 64 && !/[\u0000-\u001f\u007f]/u.test(input.name), 'invalid_app_name');
  check(audiences.includes(input.audience), 'invalid_app_audience');
  check(input.audience!=='community'||identifier(input.communityId),'app_connect_community_required');
  const source = parseAppConnectAddress(input.address);
  return { name: input.name.trim(), hostDeviceId: input.hostDeviceId, connectorId: input.connectorId, ...source, audience: input.audience,...(input.audience==='community'?{communityId:input.communityId}:{}) };
}

/** No credential is stored. One unfinished connection per account survives reload. */
export function createAppConnectJournal({ accountId, storage, locks, randomId = () => crypto.randomUUID() }) {
  check(identifier(accountId) && storage && locks?.request, 'app_connect_storage_unavailable');
  const key = `soty.app-connect.v1:${accountId}`;
  function read() {
    let raw; try { raw = storage.getItem(key); } catch { fail('app_connect_storage_unavailable'); }
    if (raw === null) return null;
    try {
      check(raw.length < 32000); const value = JSON.parse(raw);
      check([1,2].includes(value?.schema) && value.accountId === accountId && identifier(value.id) && typeof value.dispatched === 'boolean' && typeof value.complete === 'boolean');
      const normalized = intent(value.intent); check(same(normalized, value.intent));
      check(value.schema===2 || normalized.endpoint===undefined);
      check(value.appId === undefined || appPattern.test(value.appId));
      check(value.appName === undefined || typeof value.appName === 'string');
      check(Object.keys(value).every(key=>['schema','accountId','id','intent','dispatched','complete','rejected','configured','appId','appName','shortcutId'].includes(key)));
      check(['rejected','configured'].every(key=>value[key]===undefined||typeof value[key]==='boolean'));
      check(!value.complete || appPattern.test(value.appId) && identifier(value.shortcutId));
      return clone(value);
    } catch { fail('app_connect_journal_corrupt'); }
  }
  const write = value => { try { const text = JSON.stringify(value); storage.setItem(key, text); check(storage.getItem(key) === text, 'app_connect_storage_unavailable'); } catch { fail('app_connect_storage_unavailable'); } return clone(value); };
  const update = (expected, apply) => locks.request(key, () => { const value = read(); check(value?.id === expected.id && same(value.intent, expected.intent), 'app_connect_journal_changed'); return write(apply(value)); });
  return {
    read,
    async begin(input) {
      const normalized = intent(input);
      return locks.request(key, () => {
        const existing = read();
        if (existing && !existing.complete && !existing.rejected) { check(same(existing.intent, normalized), 'app_connect_in_progress'); return existing; }
        if (existing?.complete && same(existing.intent, normalized)) return existing;
        return write({ schema: normalized.endpoint?2:1, accountId, id: randomId(), intent: normalized, dispatched: false, complete: false });
      });
    },
    dispatched: record => update(record, value => ({ ...value, dispatched: true })),
    rejected: record => update(record, value => ({ ...value, rejected: true })),
    registered: (record, app) => update(record, value => { check(appPattern.test(app.id) && (!value.appId || value.appId === app.id), 'app_connect_invalid_registration'); return { ...value, appId: app.id, appName: app.name }; }),
    configured: record => update(record,value=>({...value,configured:true})),
    complete: (record, shortcutId) => update(record, value => ({ ...value, complete: true, shortcutId })),
  };
}

