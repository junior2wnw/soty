import { appPublicationArgs, createAppSettingsState, dispatchAppSettingsIntent } from './app-settings-state.mjs';

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
  check(input.resultKind===undefined||['shortcut','selection','canvas'].includes(input.resultKind),'app_connect_invalid_result');
  check(input.resultKind!=='canvas'||identifier(input.canvasProjectId),'app_connect_canvas_required');
  const source = parseAppConnectAddress(input.address);
  return { name: input.name.trim(), hostDeviceId: input.hostDeviceId, connectorId: input.connectorId, ...source, audience: input.audience,...(input.audience==='community'?{communityId:input.communityId}:{}),...(['selection','canvas'].includes(input.resultKind)?{resultKind:input.resultKind}:{}),...(input.resultKind==='canvas'?{canvasProjectId:input.canvasProjectId}:{}) };
}
function canvasResult(value,requestId){check(value&&value.operationId===requestId&&identifier(value.objectId)&&identifier(value.shortcutId)&&['saved','pending'].includes(value.status)&&value.localDurable===true,'app_connect_invalid_canvas_result');return {operationId:requestId,objectId:value.objectId,shortcutId:value.shortcutId,status:value.status,localDurable:true};}

/** No credential is stored. One unfinished connection per account survives reload. */
export function createAppConnectJournal({ accountId, storage, locks, randomId = () => crypto.randomUUID() }) {
  check(identifier(accountId) && storage && locks?.request, 'app_connect_storage_unavailable');
  const key = `soty.app-connect.v1:${accountId}`;
  function read() {
    let raw; try { raw = storage.getItem(key); } catch { fail('app_connect_storage_unavailable'); }
    if (raw === null) return null;
    try {
      check(raw.length < 32000); const value = JSON.parse(raw);
      check([1,2,3,4].includes(value?.schema) && value.accountId === accountId && identifier(value.id) && typeof value.dispatched === 'boolean' && typeof value.complete === 'boolean');
      const normalized = intent(value.intent); check(same(normalized, value.intent));
      check(value.schema!==1 || normalized.endpoint===undefined);
      check((value.schema===3)===(normalized.resultKind==='selection'));
      check((value.schema===4)===(normalized.resultKind==='canvas'));
      check(value.appId === undefined || appPattern.test(value.appId));
      check(value.appName === undefined || typeof value.appName === 'string');
      check(Object.keys(value).every(key=>['schema','accountId','id','intent','dispatched','complete','rejected','configured','appId','appName','shortcutId','canvasResult','canvasSettled'].includes(key)));
      check(['rejected','configured'].every(key=>value[key]===undefined||typeof value[key]==='boolean'));
      check(!['selection','canvas'].includes(normalized.resultKind)||value.shortcutId===undefined);
      check(normalized.resultKind==='canvas'||value.canvasResult===undefined&&value.canvasSettled===undefined);
      if(normalized.resultKind==='canvas'){check(value.canvasSettled===undefined||typeof value.canvasSettled==='boolean');check(!value.canvasSettled||value.complete);if(value.complete)canvasResult(value.canvasResult,value.id);else check(value.canvasResult===undefined);}
      check(!value.complete || appPattern.test(value.appId) && (['selection','canvas'].includes(normalized.resultKind)?value.configured===true:identifier(value.shortcutId)));
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
        return write({ schema: normalized.resultKind==='canvas'?4:normalized.resultKind==='selection'?3:normalized.endpoint?2:1, accountId, id: randomId(), intent: normalized, dispatched: false, complete: false });
      });
    },
    dispatched: record => update(record, value => ({ ...value, dispatched: true })),
    rejected: record => update(record, value => ({ ...value, rejected: true })),
    registered: (record, app) => update(record, value => { check(appPattern.test(app.id) && (!value.appId || value.appId === app.id), 'app_connect_invalid_registration'); return { ...value, appId: app.id, appName: app.name }; }),
    configured: record => update(record,value=>({...value,configured:true})),
    complete: (record, shortcutId) => update(record, value => {check(!['selection','canvas'].includes(value.intent.resultKind)&&identifier(shortcutId),'app_connect_invalid_result');return{...value,complete:true,shortcutId};}),
    completeSelection: record => update(record,value=>{check(value.intent.resultKind==='selection'&&value.configured===true&&appPattern.test(value.appId),'app_connect_invalid_selection');return{...value,complete:true};}),
    completeCanvas:(record,result)=>update(record,value=>{check(value.intent.resultKind==='canvas'&&value.configured===true&&appPattern.test(value.appId),'app_connect_invalid_canvas_result');const creation=canvasResult(result,value.id);if(value.complete)check(same(value.canvasResult,creation),'app_connect_invalid_canvas_result');return{...value,complete:true,canvasResult:creation};}),
    settledCanvas:record=>update(record,value=>{check(value.intent.resultKind==='canvas'&&value.complete&&value.canvasResult,'app_connect_invalid_canvas_result');return{...value,canvasSettled:true};}),
  };
}

const knownRegistrationRejection = code => ['apps_device_not_owned', 'invalid_app_name', 'invalid_app_port', 'invalid_app_path', 'reserved_app_path', 'app_reserved_path', 'apps_limit_reached', 'app_port_already_registered', 'app_native_port_reserved', 'apps_grant_invalid','apps_community_admin_required','apps_endpoint_invalid','apps_endpoint_not_on_selected_device'].includes(code);

/** Register→real audience receipt→the caller's durable outcome. Every retry resumes the same intent. */
export function createAppConnectFlow({ api, accountId, journal, storage, locks, createPlacement,canvas, isCurrent = () => true, preferredContextId, initialAppIds = [], onStatus = () => {} }) {
  let flight;
  const current = () => check(isCurrent(), 'field_account_changed');
  const request = async (op, args) => { current(); const response = await api.request(op, { ...args, expectedAccountId: accountId }); current(); return response; };
  const inspect = appId => request('apps.inspect', { appId });
  const grants=source=>({accountIds:[],communityIds:source.audience==='community'?[source.communityId]:[]});
  const requireCanvas=projectId=>check(canvas&&identifier(canvas.projectId)&&canvas.projectId===projectId&&['prepare','complete','settle'].every(key=>typeof canvas[key]==='function'),'app_connect_canvas_required');
  function matches(app, source) { return app.ownerAccountId === accountId && app.hostDeviceId === source.hostDeviceId && app.port === source.port && app.entryPath === source.entryPath && same(app.endpoint??null,source.endpoint??null) && app.name === source.name && same(app.grants||{accountIds:[],communityIds:[]},grants(source)); }
  async function register(record) {
    const source = record.intent;
    if (record.dispatched && !source.endpoint) {
      onStatus('Проверяем прежнее подключение…'); const listed = await request('apps.list', {});
      const found = listed.apps.filter(app => matches(app, source)); check(found.length <= 1, 'app_connect_registration_ambiguous');
      if (found[0]) return journal.registered(record, found[0]);
    }
    record = await journal.dispatched(record); current(); onStatus('Подключаем приложение…');
    try {
      let inventoryWitness;
      if(source.endpoint){
        onStatus('Проверяем адрес на устройстве…');
        // Inventory is replaceable evidence, not part of the durable intent. An
        // exact replay can retrieve its receipt even if the device is now offline.
        try { const devices=await request('apps.devices',{});const device=devices.devices.find(value=>value.hostDeviceId===source.hostDeviceId&&value.connectorId===source.connectorId);inventoryWitness=device?.sourceEndpoints?.inventoryWitness; }
        catch(error){current();}
      }
      const response = await api.request(source.endpoint?'apps.registerEndpoint':'apps.register', { expectedAccountId: accountId, hostDeviceId: source.hostDeviceId, connectorId: source.connectorId, name: source.name,
        ...(source.endpoint?{requestId:record.id,source:{schema:'soty.app-http-source.v2',endpoint:source.endpoint,port:source.port,entryPath:source.entryPath},...(inventoryWitness?{inventoryWitness}:{})}:{port:source.port,entryPath:source.entryPath}),grants:grants(source) });
      check(appPattern.test(response?.app?.id) && typeof response.app.name === 'string', 'app_connect_invalid_registration');
      if(source.endpoint)check(response.registrationReceipt?.schema==='soty.app-endpoint-registration-receipt.v1'&&response.registrationReceipt.requestId===record.id&&response.registrationReceipt.appId===response.app.id&&response.registrationReceipt.committed===true&&matches(response.app,source),'app_connect_invalid_registration');
      // An exact late ACK may settle its own old-account journal, never a new account's UI.
      record = await journal.registered(record, response.app); current(); return record;
    } catch (error) { if (knownRegistrationRejection(error?.code)) await journal.rejected(record); throw error; }
  }
  async function configure(record) {
    onStatus('Настраиваем доступ…'); const state = createAppSettingsState({ accountId, appId: record.appId, storage, locks });
    const pending = state.read().pending;
    if (pending) {
      check(pending.op==='apps.domains.claim'&&pending.args.slug===`p-${record.appId.slice(4)}` || pending.op==='apps.publication.update'&&pending.args.audience===(record.intent.audience==='community'?null:record.intent.audience),'app_connect_settings_busy');
      const settled = await dispatchAppSettingsIntent({ state, expectedPending: pending, api, isCurrent }); check(settled.status === 'accepted', 'app_connect_pending_changed');
      if(pending.op==='apps.publication.update'){record=await journal.configured(record);check(settled.response.current.policyEpoch===settled.response.receipt.policyEpoch,'app_connect_publication_conflict');}
    }
    let snapshot = await inspect(record.appId); const source = record.intent;
    check(snapshot.schema==='soty.app-inspection.v1'&&snapshot.app.state==='enabled','app_connect_app_unavailable');
    check(snapshot.app.id === record.appId && snapshot.source.hostDeviceId === source.hostDeviceId && snapshot.source.connectorId === source.connectorId && snapshot.source.port === source.port && snapshot.source.entryPath === source.entryPath && same(snapshot.source.endpoint??null,source.endpoint??null), 'app_connect_source_changed');
    const desired = source.audience==='community'?null:source.audience, listed = desired === 'public', launchPolicy = desired === 'private'||desired===null ? 'restricted' : 'anyone';
    if (snapshot.publication.audience === desired && snapshot.publication.listed === listed && snapshot.publication.launchPolicy === launchPolicy) return snapshot;
    check(!record.configured,'app_connect_publication_conflict');
    let activeDomainIds = [...snapshot.publication.activeDomainIds];
    if (['link','public'].includes(desired) && !activeDomainIds.length) {
      const existing = snapshot.addresses.aliases.find(alias => alias.state === 'bound');
      if (!existing) {
        check(snapshot.actions.canReserveName && snapshot.addresses.claimOrigin, 'apps_named_zone_disabled');
        const result = await dispatchAppSettingsIntent({ state, op: 'apps.domains.claim', args: { appId: record.appId, expectedAccountId: accountId, slug: `p-${record.appId.slice(4)}`, expectedDomainsRevision: snapshot.addresses.revision }, api, isCurrent });
        check(result.status === 'accepted', 'app_connect_pending_changed'); snapshot = await inspect(record.appId);
      }
      const alias = snapshot.addresses.aliases.find(value => value.state === 'bound'); check(alias, 'app_publication_domain_required'); activeDomainIds = [alias.id];
    }
    const args = appPublicationArgs(snapshot, { launchPolicy, listed, audience: desired, activeDomainIds, exposureConfirmed: launchPolicy==='anyone' }, accountId);
    const accepted = await dispatchAppSettingsIntent({ state, op: 'apps.publication.update', args, api, isCurrent });
    check(accepted.status === 'accepted', 'app_connect_pending_changed');await journal.configured(record); snapshot = await inspect(record.appId);
    check(snapshot.publication.audience === desired && snapshot.publication.listed === listed && snapshot.publication.launchPolicy === launchPolicy, 'app_connect_publication_unconfirmed');
    return snapshot;
  }
  async function place(record) {
    current(); onStatus('Добавляем соту на полотно…');
    const placement = createPlacement(record.appId, { initialAppIds });
    try {
      let state = await placement.load(); current();
      if (state.pendingCount || state.persistence !== 'saved') { state = await placement.retry(); current(); }
      check(state.localDurable && !state.pendingCount && state.persistence === 'saved', 'app_connect_field_pending');
      const context = state.contexts.find(value => value.contextId === preferredContextId) || state.contexts.find(value => value.contextId === 'personal') || state.contexts[0];
      check(context, 'field_context_missing'); const result = await placement.place({ contextId: context.contextId, generation: state.generation }); current();
      check(result.status === 'saved' && result.shortcutId, 'app_connect_field_pending');
      const proof = await placement.load(); current();
      check(proof.persistence === 'saved' && proof.localDurable && !proof.pendingCount && proof.placements.some(item => item.shortcutId === result.shortcutId && item.contextId === context.contextId), 'app_connect_placement_unconfirmed');
      return { shortcutId: result.shortcutId, contextId: context.contextId };
    } finally { placement.dispose(); }
  }
  return {
    pending: () => journal.read(),
    async confirmSelection(result){
      current();const record=journal.read();
      check(result?.resultKind==='selection'&&record?.intent.resultKind==='selection'&&record.id===result.requestId&&record.appId===result.app?.id&&record.configured===true,'app_connect_invalid_selection');
      await journal.completeSelection(record);current();
    },
    run(input) {
      if (flight) return flight;
      flight = (async () => {
        current();
        const previous=journal.read();
        if(previous?.intent.resultKind==='canvas'&&previous.complete&&!previous.canvasSettled){requireCanvas(previous.intent.canvasProjectId);await canvas.settle(previous.id);current();await journal.settledCanvas(previous);current();}
        if(input.resultKind==='canvas')requireCanvas(canvas?.projectId);
        let record = await journal.begin(input.resultKind==='canvas'?{...input,canvasProjectId:canvas.projectId}:input); current();
        if(record.intent.resultKind==='canvas'){requireCanvas(record.intent.canvasProjectId);if(!record.complete)await canvas.prepare(record.id);current();}
        if (!record.appId) record = await register(record);
        const snapshot = await configure(record); current();record=await journal.configured(record);
        if(record.intent.resultKind==='canvas'){
          if(!record.complete){onStatus('Приложение подключено. Размещаем окно…');const creation=await canvas.complete({requestId:record.id,app:{id:record.appId,name:snapshot.app.name}});current();record=await journal.completeCanvas(record,creation);current();}
          if(!record.canvasSettled){await canvas.settle(record.id);current();record=await journal.settledCanvas(record);current();}
          return {resultKind:'canvas',requestId:record.id,app:{id:record.appId,name:snapshot.app.name},audience:snapshot.publication.audience,publication:snapshot.publication,creation:clone(record.canvasResult)};
        }
        if(record.intent.resultKind==='selection')return{resultKind:'selection',requestId:record.id,app:{id:record.appId,name:snapshot.app.name},audience:snapshot.publication.audience,publication:snapshot.publication};
        const placement = await place(record); current();
        record = await journal.complete(record, placement.shortcutId); current();
        return { resultKind:'shortcut',app: { id: record.appId, name: snapshot.app.name }, audience: snapshot.publication.audience, publication: snapshot.publication, ...placement };
      })();
      const release = () => { flight = null; }; flight.then(release, release); return flight;
    },
  };
}
