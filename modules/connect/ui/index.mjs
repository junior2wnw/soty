import { mountPlatformInvitations } from './platform-invitations.mjs';
import { watchDeviceEnrollment } from './device-enrollment.mjs';
import { mountCredentialLogin, mountCredentialSettings } from './credential-forms.mjs';
const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const date = value => value ? new Date(value).toLocaleString('ru-RU') : '—';
const activationSelection=state=>({accountId:state.accountId,deviceId:state.deviceId,localRevision:state.localRevision});
const materialElement = (tag, role) => { const node = document.createElement(tag); node.setAttribute('data-glass-role', role); node.setAttribute('data-glass-safe-area', 'off'); return node; };
const errors = {
  platform_invitation_required: 'Новые участники входят по приглашению. Откройте ссылку приглашения или подключите свой прежний профиль.',
  platform_invite_unavailable: 'Приглашение уже использовано, отозвано или просрочено. Попросите новую ссылку.',
  platform_invite_invalid: 'Ссылка приглашения повреждена. Откройте полную ссылку из сообщения.',
  'snapshot-text-too-large': 'В копии есть текст длиннее поддерживаемого Сотами размера. Импорт остановлен целиком, чтобы ничего не обрезать. Используйте исходное устройство для экспорта этого текста.',
  account_mismatch: 'Профиль изменился. Проверьте, какой аккаунт открыт, и повторите действие.',
  device_revoked: 'Доступ этого устройства завершён. Откройте профиль через другое устройство или файл восстановления.',
  vault_conflict: 'Сохранённая копия изменилась на другом устройстве. Сначала загрузите актуальную копию; ваши данные здесь не удалены.',
  revision_conflict: 'Данные изменились на другом устройстве. Обновите сведения и повторите действие.',
  enrollment_pending: 'Ожидаем подтверждения на вашем прежнем устройстве.',
  request_expired: 'Срок запроса закончился. Создайте новый QR.',
  enrollment_expired: 'Срок запроса закончился. Создайте новый QR.',
  active_profile_changed: 'Профиль изменился в другой вкладке. Проверьте его и повторите действие.',
  enrollment_preview_required: 'Сначала проверьте, какой профиль подтвердил подключение.',
  enrollment_account_mismatch: 'Подтверждён другой профиль. Проверьте подключение заново.',
  last_device: 'Сначала проверьте запасной способ входа или добавьте второе устройство.',
  recovery_invalid: 'Файл не подходит или уже использован. Проверьте, что выбран последний сохранённый файл.',
  network_error: 'Нет связи с сервером. Ваши данные на устройстве сохранены; повторите позже.',
  profile_change_cancelled: 'Профиль не изменён. Подключение можно продолжить позже.',
  weak_password: 'Выберите уникальный пароль от 15 символов.', weak_phrase: 'Выберите уникальную фразу от 20 символов.',
  invalid_login: 'Проверьте логин: от 3 до 80 символов без пробелов.', credential_unavailable: 'Не удалось войти. Проверьте логин, пароль или фразу.',
  credential_exists: 'Этот логин уже занят.', credential_rate_limited: 'Слишком много попыток. Подождите и повторите вход.',
  credential_pending: 'Предыдущая попытка ещё не завершена. Повторите её с теми же данными.', recovery_pending: 'Сначала завершите восстановление из файла.',
  storage_future_version: 'Данные созданы более новой версией. Обновите приложение — аккаунт не будет пересоздан.',
  storage_corrupt: 'Не удалось прочитать данные входа. Используйте восстановление; существующие данные не перезаписываются.'
};
function deviceLabel(){const p=globalThis.navigator?.userAgentData?.platform||globalThis.navigator?.platform;return p==='Win32'?'Windows':p==='MacIntel'?'Mac':p?.startsWith('Linux')?'Linux':p||'Браузер';}
function friendly(error) {
  const code = String(error?.code || '').toLowerCase();
  if (['unsupported_local_state', 'unsupported_database'].includes(code)) return errors.storage_future_version;
  if (['corrupt_local_state', 'corrupt_database'].includes(code)) return errors.storage_corrupt;
  if (code === 'enrollment_not_approved') return errors.enrollment_pending;
  if (code === 'last_device_requires_recovery') return errors.last_device;
  if (code === 'recovery_unavailable' || code === 'invalid_recovery_kit') return errors.recovery_invalid;
  return errors[code] || 'Не удалось завершить действие. Данные не сброшены. Повторите попытку или воспользуйтесь сохранённым способом входа.';
}
export function parseConnectLink(value, origin = globalThis.location?.origin) {
  try {
    if (typeof value !== 'string' || value.length > 4096 || !value.isWellFormed()) return null;
    const source = value.trim(), base = new URL(origin);
    if (base.origin !== origin || !['http:', 'https:'].includes(base.protocol)
      || /[\\\u0000-\u0020\u007f]/u.test(source) || !/^(?:https?:\/\/[^/?#]+)?\/?\?/iu.test(source) || source.includes('#')) return null;
    const url = new URL(source, origin);
    if (url.origin !== origin || url.pathname !== '/' || url.username || url.password
      || url.search.slice(1).split('&').some(part => !part)) return null;
    const kind = url.searchParams.get('connect');
    const field = kind === 'contact' ? 'card' : 'request', id = url.searchParams.get(field);
    if (!['contact', 'device'].includes(kind) || !id || !/^[A-Za-z0-9_-]{16,160}$/.test(id)
      || new Set(url.searchParams.keys()).size !== 2 || ['connect', field].some(name => url.searchParams.getAll(name).length !== 1)) return null;
    return { kind, id };
  } catch { return null; }
}
export function openConnectPanel({ client, label = 'Мой профиль', productName = 'Соты', qr, snapshot, restore, invitation, onRename, onContactCall, onContactMessage, onEditProfile, onOpenDataTools, beforeProfileChange,
  snapshotDescription = 'Комнаты и тексты. Вложения сохраняйте отдельно.', initialIntent = null, initialTab = 'profile', bootstrapOnOpen = true }) {
  if (typeof bootstrapOnOpen !== 'boolean') throw new TypeError('invalid_bootstrap_policy');
  if (beforeProfileChange !== undefined && typeof beforeProfileChange !== 'function') throw new TypeError('invalid_profile_change_guard');
  const previousFocus = document.activeElement,panelLifetime=new AbortController();
  const dialog = materialElement('dialog', 'overlay');
  const openingDevice = initialTab === 'connect' && !initialIntent;
  const approvingDevice = initialIntent?.kind === 'device';
  const deviceTitle = 'Открыть мои Соты здесь';
  const approvalTitle = 'Подключить ещё одно устройство';
  const initialTitle = openingDevice ? deviceTitle : approvingDevice ? approvalTitle : 'Входы и данные профиля';
  const initialProgress = openingDevice ? 'Готовим QR…' : approvingDevice ? 'Проверяем запрос…' : '';
  dialog.className = openingDevice ? 'connect-panel connect-entry' : 'connect-panel';
  dialog.setAttribute('aria-label', initialTitle);
  dialog.innerHTML = `<header><div><small>${escape(productName)}</small><h2>${initialTitle}</h2></div><button data-glass-role="control" data-glass-safe-area="off" type="button" data-close aria-label="Закрыть">×</button></header>
    <nav${initialProgress ? ' hidden' : ''} aria-label="Разделы профиля"><button data-glass-role="control" data-glass-safe-area="off" data-tab="profile">Профиль</button><button data-glass-role="control" data-glass-safe-area="off" data-tab="people">Люди</button><button data-glass-role="control" data-glass-safe-area="off" data-tab="devices">Устройства входа</button><button data-glass-role="control" data-glass-safe-area="off" data-tab="recovery">Восстановление</button><button data-glass-role="control" data-glass-safe-area="off" data-tab="data">Копии данных</button><button data-glass-role="control" data-glass-safe-area="off" data-tab="profiles">Выбрать профиль</button></nav>
    <p class="connect-message" role="status" aria-live="polite"></p><section class="connect-content" aria-busy="true">${initialProgress ? `<p role="status">${initialProgress}</p>` : ''}</section>`;
  document.body.append(dialog); dialog.showModal();
  const content = dialog.querySelector('.connect-content');
  const message = dialog.querySelector('.connect-message');
  let tab = ['profile', 'people', 'devices', 'recovery', 'data', 'profiles', 'connect'].includes(initialTab) ? initialTab : 'profile', busy = false, stopped = false, status, local, enrollment, recoveryKit, timer, intent = initialIntent;
  let platformInvitations = null,deviceEntry=null,entryLifetime=null,renderGeneration=0;
  const note = text => { if (!stopped) message.textContent = text; };
  const guard = fn => async event => {
    event?.preventDefault(); if (busy || stopped) return;
    busy = true; content.setAttribute('aria-busy', 'true');
    const buttons = [...content.querySelectorAll('button')].map(button => ({button,disabled:button.disabled})); buttons.forEach(({button}) => button.disabled = true);
    try {
      if (local?.accountId && (await client.getLocalState()).accountId !== local.accountId) {
        note(errors.active_profile_changed); await render(); return;
      }
      await fn(event);
    } catch (error) { note(friendly(error)); }
    finally { busy = false; content.setAttribute('aria-busy', 'false'); buttons.forEach(({button,disabled}) => { if (button.isConnected) button.disabled = disabled; });
      const target=event?.currentTarget;if(!stopped&&target?.isConnected&&!target.disabled&&(document.activeElement===document.body||document.activeElement===target))target.focus?.(); }
  };
  async function guardedIdentityMutation(toAccountId,mutate,validTarget=()=>true){
    const fromAccountId=local?.accountId??null,generation=renderGeneration;
    if(stopped)return false;
    if(beforeProfileChange&&await beforeProfileChange({fromAccountId,toAccountId})!==true){note('Профиль не изменён. Сохраните работу или закройте приложения.');return false;}
    if(stopped||generation!==renderGeneration)return false;
    const fresh=await client.getLocalState();if(stopped||generation!==renderGeneration)return false;
    if(fresh.accountId!==fromAccountId){note(errors.active_profile_changed);if(tab==='connect'){stopEntry();content.querySelector('[data-qr]')?.replaceChildren();}else await render();return false;}
    if(!validTarget(fresh)){note('Этот профиль или запрос больше недоступен. Проверьте выбор и повторите.');if(tab==='connect'){stopEntry();content.querySelector('[data-qr]')?.replaceChildren();}else await render();return false;}
    await mutate(fresh,entryLifetime?.signal??panelLifetime.signal);return true;
  }
  const on = (selector, fn) => content.querySelector(selector)?.addEventListener('click', guard(fn));
  const download = (filename, data) => {
    const url = URL.createObjectURL(new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' }));
    const a = document.createElement('a'); a.href = url; a.download = filename; a.click(); setTimeout(() => URL.revokeObjectURL(url), 2000);
  };
  const readFile = async selector => {
    const file = content.querySelector(selector)?.files?.[0];
    if (!file || file.size > 3_000_000) throw new Error('file_invalid');
    return JSON.parse(await file.text());
  };
  function stopEntry(){deviceEntry?.stop();deviceEntry=null;entryLifetime?.abort();entryLifetime=null;}
  function stop() { platformInvitations?.destroy(); stopped = true;panelLifetime.abort(); renderGeneration++;stopEntry();clearTimeout(timer); dialog.remove(); if (previousFocus?.isConnected) previousFocus.focus(); }
  dialog.querySelector('[data-close]').addEventListener('click', () => dialog.close());
  dialog.addEventListener('cancel',event=>{if(event.target===dialog)stopEntry();});
  dialog.addEventListener('close', stop);
  dialog.querySelectorAll('[data-tab]').forEach(button => button.addEventListener('click', guard(async () => { intent = null; tab = button.dataset.tab; await render(); })));
  async function refresh() {
    local = await client.getLocalState();
    if (local.accountId && tab !== 'profiles' && tab !== 'connect') {
      try {
        const raw = await client.status();
        status = { ...raw, account: { id: raw.accountId, label: raw.label }, devices: (raw.devices || []).map(d => ({ ...d, id: d.deviceId })) };
      } catch (error) { status = null; note(friendly(error)); }
    }
  }
  async function showQr(url,title,current=()=>!stopped){
    const host = content.querySelector('[data-qr]'); if (!host||!current()) return;
    const caption = document.createElement('p'); caption.textContent = title; host.append(caption);
    if (qr) {
      const src = await qr(url); if (!current()||!host.isConnected) return;
      const img = document.createElement('img'); img.src = src; img.alt = title; img.width = 224; img.height = 224; host.prepend(img);
    }
    const linkHost=tab==='connect'?content.querySelector('[data-other-ways]')||host:host;
    const input = materialElement('input', 'field'); input.readOnly = true; input.value = url; input.setAttribute('aria-label', 'Ссылка для подключения'); linkHost.append(input);
    const copy = materialElement('button', 'control'); copy.textContent = 'Скопировать ссылку'; copy.type = 'button'; host.append(copy);
    copy.addEventListener('click', guard(async () => {
      try { await navigator.clipboard.writeText(url); note('Ссылка скопирована.'); }
      catch {
        const alternatives = linkHost.closest('details'); if (alternatives) alternatives.open = true;
        input.focus(); input.select(); note('Скопируйте выделенную ссылку.');
      }
    }));
  }
  const link = (kind, id) => `${location.origin}/?connect=${kind}&${kind === 'contact' ? 'card' : 'request'}=${encodeURIComponent(id)}`;
  async function render() {
    platformInvitations?.destroy(); platformInvitations = null;
    clearTimeout(timer);stopEntry();const generation=++renderGeneration;
    if (stopped) return;
    await refresh();if(stopped||generation!==renderGeneration)return;
    const titles = { profile: 'Профиль', people: 'Контакты', devices: 'Устройства входа', recovery: 'Восстановление профиля', data: 'Копии данных', profiles: 'Выбрать профиль', connect: deviceTitle };
    const heading = intent?.kind === 'device' ? approvalTitle : titles[tab];
    dialog.querySelector('nav').hidden=tab==='connect'||intent?.kind==='device';dialog.className=tab==='connect'?'connect-panel connect-entry':'connect-panel';
    dialog.querySelector('h2').textContent = heading; dialog.setAttribute('aria-label', heading);
    dialog.querySelectorAll('[data-tab]').forEach(b => b.setAttribute('aria-current', b.dataset.tab === tab ? 'page' : 'false'));
    if (intent) { await renderIntent(); return; }
    if (tab === 'profiles') {
      content.innerHTML = '<h3>Ваши профили на этом устройстве</h3><p>Выберите, от чьего имени продолжить. Данные и разрешения каждого профиля хранятся отдельно.</p><div data-profile-list></div><button data-glass-role="control" data-glass-safe-area="off" data-existing>Открыть профиль с другого устройства</button><button data-glass-role="control" data-glass-safe-area="off" data-recovery>Восстановить из файла</button>';
      const known = new Map();
      for (const profile of local.profiles || []) if (!profile.revoked && (!known.has(profile.accountId) || profile.active)) known.set(profile.accountId, profile);
      if (!known.size) content.querySelector('[data-profile-list]').textContent = 'Сохранённых профилей пока нет.';
      for (const profile of known.values()) {
        const row = materialElement('button', 'control'); row.type = 'button'; row.dataset.profileId = profile.accountId;
        row.textContent = `${profile.label || 'Профиль'}${profile.active ? ' · текущий' : ''}`; row.disabled = Boolean(profile.active);
        row.addEventListener('click', guard(async () => {
          if(profile.accountId===(local?.accountId??null))return;
          if(await guardedIdentityMutation(profile.accountId,(fresh,signal)=>client.switchProfile(profile.accountId,{expectedState:activationSelection(fresh),signal}),fresh=>(fresh.profiles||[]).some(value=>value.accountId===profile.accountId&&!value.revoked))&&!stopped)dialog.close();
        }));
        content.querySelector('[data-profile-list]').append(row);
      }
      on('[data-existing]', async () => { tab = 'connect'; await render(); });
      on('[data-recovery]', async () => { tab = 'recovery'; await render(); });
      return;
    }
    if (!local.accountId && (tab === 'profile' || tab === 'people')) {
      content.innerHTML = '<h3>Открыть существующий профиль</h3><p>Подтвердите новое устройство с вашего прежнего устройства или используйте файл восстановления.</p><button data-glass-role="control" data-glass-safe-area="off" data-existing>Подключить по QR</button><button data-glass-role="control" data-glass-safe-area="off" data-recovery>Открыть файл восстановления</button>';
      on('[data-existing]', async () => { tab = 'connect'; await render(); });
      on('[data-recovery]', async () => { tab = 'recovery'; await render(); });
      return;
    }
    if (tab === 'profile') {
      const account = status?.account;
      content.innerHTML = `<h3>${escape(account?.label || label)}</h3><p>Ваш профиль сохраняется при обновлениях. У каждого подключённого устройства свой доступ.</p>
        ${typeof onEditProfile === 'function' ? '<button data-glass-role="control" data-glass-safe-area="off" data-edit-profile>Имя, фото и описание</button>' : `<label>Как вас увидят друзья<input data-glass-role="field" data-glass-safe-area="off" data-name maxlength="80" value="${escape(account?.label || label)}"></label><button data-glass-role="control" data-glass-safe-area="off" data-name-save>Сохранить имя</button>`}
        <button data-glass-role="control" data-glass-safe-area="off" data-card>Мой QR для знакомства</button><button data-glass-role="control" data-glass-safe-area="off" data-existing>Открыть мой профиль с другого устройства</button>
        <label>Открыть ссылку или QR из сообщения<input data-glass-role="field" data-glass-safe-area="off" data-link placeholder="Вставьте ссылку" autocomplete="off"></label><button data-glass-role="control" data-glass-safe-area="off" data-open-link>Продолжить по ссылке</button>
        <div data-qr></div><button data-glass-role="control" data-glass-safe-area="off" data-select-profile>Выбрать другой сохранённый профиль</button><p class="connect-muted">При добавлении друга вы остаётесь каждый со своим аккаунтом.</p>`;
      on('[data-card]', async () => { const card = await client.card(); content.querySelector('[data-qr]').replaceChildren(); await showQr(link('contact', card.cardId), 'Добавить меня в контакты'); });
      on('[data-name-save]', async () => { const name = content.querySelector('[data-name]').value, expectedAccountId = local.accountId; await client.rename(name, expectedAccountId); if (onRename) await onRename(name); note('Имя сохранено.'); await render(); });
      on('[data-edit-profile]', async () => { if (typeof onEditProfile !== 'function') return; onEditProfile(); dialog.close(); });
      on('[data-existing]', async () => { tab = 'connect'; await render(); });
      on('[data-open-link]', async () => { const next = parseConnectLink(content.querySelector('[data-link]').value); if (!next) { note('Это не ссылка подключения этого проекта.'); return; } intent = next; await render(); });
      on('[data-select-profile]', async () => { tab = 'profiles'; await render(); });
      const invitationHost = document.createElement('section'); content.append(invitationHost);
      platformInvitations = mountPlatformInvitations(invitationHost, { client, accountId: local.accountId, qr });
    } else if (tab === 'people') {
      const list = await client.contacts(); if (stopped) return;
      const contacts = list.contacts || [], incoming = list.requests?.incoming || [], outgoing = list.requests?.outgoing || [];
      content.innerHTML = `<h3>Люди в ${escape(productName)}</h3><p>Контакт помогает снова найти человека. Доступ к комнатам выдаётся отдельным приглашением.</p>
        <button data-glass-role="control" data-glass-safe-area="off" data-card>Показать мой QR</button><button data-glass-role="control" data-glass-safe-area="off" data-refresh>Обновить</button><div data-qr></div>
        <h4>Входящие запросы</h4><div data-incoming>${incoming.length ? '' : '<p class="connect-muted">Новых запросов нет.</p>'}</div>
        <h4>Контакты</h4><div data-contacts>${contacts.length ? '' : '<p class="connect-muted">Покажите другу свой QR, чтобы добавиться.</p>'}</div>
        <h4>Отправленные запросы</h4><div data-outgoing></div><h4>Приглашения</h4><div data-invitations></div>
        <details><summary data-glass-role="control" data-glass-safe-area="off">Нежелательные обращения и визитка</summary><div data-blocked></div><button data-glass-role="control" data-glass-safe-area="off" data-rotate>Сменить адрес моей визитки</button></details>`;
      const row = (container, text, actions) => { const el = document.createElement('div'); el.className = 'connect-row'; const span = document.createElement('span'); span.textContent = text; el.append(span); for (const [title, fn, accessibleName] of actions) { const b = materialElement('button', 'control'); b.textContent = title; b.type = 'button'; if (accessibleName) b.setAttribute('aria-label', accessibleName); b.addEventListener('click', guard(fn)); el.append(b); } content.querySelector(container).append(el); };
      for (const r of incoming) row('[data-incoming]', r.label || 'Новый контакт', [['Принять', async () => { await client.acceptContact(r.requestId); note('Вы добавили друг друга.'); await render(); }], ['Отклонить', async () => { await client.declineContact(r.requestId); await render(); }], ['Заблокировать', async () => { await client.blockContact(r.peerAccountId); await render(); }]]);
      for (const r of contacts) {
        const actions = [];
        if (typeof onContactMessage === 'function') actions.push(['Написать', async () => {
          await onContactMessage({ peerAccountId: r.peerAccountId, relationshipId: r.relationshipId, label: r.label || r.peerLabel || 'Контакт' });
          if (!stopped) dialog.close();
        }, `Написать ${r.label || r.peerLabel || 'контакту'}`]);
        if (typeof onContactCall === 'function') actions.push(['Слушать звонок', async () => {
          await onContactCall({ peerAccountId: r.peerAccountId, label: r.label || r.peerLabel || 'Контакт' });
          if (!stopped) dialog.close();
        }]);
        if (invitation) actions.push(['Пригласить в текущую комнату', async () => { const data = await invitation(); await client.sendContactInvite(r.relationshipId, data.url, data.label); note('Приглашение отправлено. Друг решит, присоединяться ли к комнате.'); }]);
        actions.push(['Удалить', async () => { if (!confirm('Удалить из контактов? Отдельные доступы к общим комнатам сохранятся.')) return; await client.removeContact(r.relationshipId); await render(); }], ['Заблокировать', async () => { await client.blockContact(r.peerAccountId); note('Обращения остановлены. Доступы к общим комнатам управляются отдельно.'); await render(); }]);
        row('[data-contacts]', r.label || r.peerLabel || 'Контакт', actions);
      }
      for (const r of outgoing) row('[data-outgoing]', `${r.label || r.toLabel || 'Контакт'} · ожидает ответа`, [['Отменить', async () => { await client.cancelContact(r.requestId); await render(); }]]);
      on('[data-card]', async () => { const c = await client.card(); content.querySelector('[data-qr]').replaceChildren(); await showQr(link('contact', c.cardId), 'Добавить меня в контакты'); });
      on('[data-refresh]', render);
      for (const r of list.invitations || []) row('[data-invitations]', `${r.peerLabel}: ${r.label}`, [['Открыть приглашение', async () => {
        const u = new URL(r.url); if (u.origin !== location.origin || u.pathname !== '/' || !u.searchParams.has('j')) throw new Error('invalid_invite'); location.assign(u.href);
      }], ['Убрать', async () => { await client.dismissContactInvite(r.invitationId); await render(); }]]);
      for (const r of list.blocked || []) row('[data-blocked]', r.label || 'Контакт', [['Разблокировать', async () => { await client.unblockContact(r.peerAccountId); note('Блокировка снята. Дружба автоматически не восстановлена.'); await render(); }]]);
      on('[data-rotate]', async () => { if (!confirm('Сменить адрес визитки? Старые QR и незавершённые запросы перестанут действовать. Контакты сохранятся.')) return; await client.rotateCard(); note('Адрес визитки изменён.'); await render(); });
    } else if (tab === 'connect') {
      content.innerHTML = `<p>Покажите этот QR устройству, где уже открыты ваши Соты. Подтвердите вход на нём, и здесь откроется то же полотно.</p>
        <div data-qr></div><p data-pairing-code class="connect-pairing-code"></p>
        <p data-entry-status role="status">Готовим QR…</p><button data-glass-role="control" data-glass-safe-area="off" data-new-qr hidden>Обновить QR</button>
        <details class="connect-other"><summary data-glass-role="control" data-glass-safe-area="off">Другой способ</summary><div data-other-ways></div>
          <label>Файл восстановления<input type="file" accept="application/json,.json" data-entry-file></label>
          <div data-credential-login></div><button data-glass-role="control" data-glass-safe-area="off" data-known-profiles>Сохранённые аккаунты</button></details>`;
      entryLifetime = new AbortController();
      const lifetime = entryLifetime, current = () => !stopped && dialog.open && generation === renderGeneration && !lifetime.signal.aborted;
      let qrGeneration=0,displayedRequestId=null;
      const begin = () => { qrGeneration++; deviceEntry?.stop(); deviceEntry = null;
        if (current()) content.querySelector('[data-entry-status]').textContent = 'Открываем аккаунт…'; };
      const complete = () => { if (current()) dialog.close(); };
      const resume = async () => { if (current()) await startQr(); };
      const denied=()=>{if(current()){content.querySelector('[data-entry-status]').textContent=errors.profile_change_cancelled;const button=content.querySelector('[data-new-qr]');button.hidden=false;button.textContent='Повторить';}};
      mountCredentialLogin(content.querySelector('[data-credential-login]'), { client, label: deviceLabel(), guard,begin,resume,activate:async action=>{let result=null;const allowed=await guardedIdentityMutation(null,async fresh=>{result=await action(fresh);},fresh=>!fresh.current?.revoked);return allowed?result:null;},onDenied:denied,signal: lifetime.signal, onComplete: complete });
      content.querySelector('[data-entry-file]').addEventListener('change', guard(async () => {
        const input = content.querySelector('[data-entry-file]');
        try {
          const kit = await readFile('[data-entry-file]'); if (!current()) return;
          begin();try{const allowed=await guardedIdentityMutation(null,fresh=>client.recover(kit,deviceLabel(),{signal:lifetime.signal,expectedState:activationSelection(fresh)}),fresh=>!fresh.current?.revoked);if(allowed)complete();else denied();}
          catch (error) { await resume(); throw error; }
        } finally { if (input) input.value = ''; }
      }));
      on('[data-known-profiles]', async () => { tab = 'profiles'; await render(); });
      let expired = false;
      on('[data-new-qr]',async()=>{if(expired){const fresh=await client.getLocalState();if(!current()||fresh.pendingEnrollment?.requestId!==displayedRequestId||typeof client.discardPendingEnrollmentIfCurrent!=='function'){note(errors.active_profile_changed);return;}await client.discardPendingEnrollmentIfCurrent(displayedRequestId,fresh,{signal:lifetime.signal});}await render();});
      async function startQr() {
        deviceEntry?.stop(); const ticket = ++qrGeneration, currentQr = () => current() && ticket === qrGeneration;
        const watcher = watchDeviceEnrollment({ client, label: deviceLabel(), isCurrent: currentQr,
          onRequest: async request => {
            displayedRequestId=request.requestId;
            if (!currentQr()) return;
            content.querySelector('[data-qr]').replaceChildren(); content.querySelector('[data-other-ways]').replaceChildren();
            await showQr(link('device', request.requestId), 'Вход в ваш аккаунт', currentQr);
            if (!currentQr()) return;
            content.querySelector('[data-pairing-code]').textContent = request.requestId.slice(-8).toUpperCase().match(/.{1,4}/g).join(' ');
          },
          onWaiting: (preview, error) => {
            if (currentQr()) content.querySelector('[data-entry-status]').textContent = preview?.account
              ? `Открываем аккаунт «${preview.account.label}»…` : error ? 'Нет связи. Проверим подтверждение, когда соединение восстановится.' : 'Ждём подтверждения на прежнем устройстве…';
          },
          activate:async(target,action)=>{let result=null;await guard(async()=>{const allowed=await guardedIdentityMutation(target.accountId,async fresh=>{result=await action(fresh);},fresh=>fresh.pendingEnrollment?.requestId===target.requestId&&!fresh.current?.revoked);if(!allowed)result=null;})();return result;},
          onComplete: complete,
          onError: error => { if (currentQr()) {
            expired = ['enrollment_expired', 'request_expired'].includes(String(error?.code).toLowerCase());
            content.querySelector('[data-entry-status]').textContent = friendly(error);
            const retry = content.querySelector('[data-new-qr]'); retry.hidden = false; retry.textContent = expired ? 'Обновить QR' : 'Повторить';
          } },
        });
        deviceEntry = watcher; await watcher.ready;
      }
      void startQr().catch(error => { if (current()) note(friendly(error)); });
    } else if (tab === 'devices') {
      content.innerHTML = `<h3>Доступ к этому профилю</h3><p>Здесь устройства входа, а не компьютеры с размещёнными приложениями. Чтобы открыть прежний профиль на этом устройстве, покажите QR телефону, где он уже открыт.</p>
        <label>Название этого устройства<input data-glass-role="field" data-glass-safe-area="off" data-device-label value="${escape(local.pendingEnrollment?.label || label)}" maxlength="64"></label>
        <button data-glass-role="control" data-glass-safe-area="off" data-enroll>Открыть прежний профиль здесь</button><div data-qr></div><div data-devices></div>
        <p class="connect-muted">Отключение прекращает доступ к профилю и сохранённым копиям. Уже открытые старые комнаты и скачанные данные требуют отдельного управления.</p>`;
      for (const d of status?.devices || []) {
        const row = document.createElement('div'); row.className = 'connect-row';
        const span = document.createElement('span'); span.textContent = `${d.label || 'Устройство'}${d.id === status.deviceId ? ' · это устройство' : ''}${d.revokedAt ? ' · отключено' : ''}`; row.append(span);
        if (!d.revokedAt) { const b = materialElement('button', 'control'); b.textContent = 'Отключить'; b.addEventListener('click', guard(async () => { if (!confirm('Завершить доступ этого устройства к профилю?')) return; await client.revokeDevice(d.id); note('Доступ устройства завершён.'); await render(); })); row.append(b); }
        content.querySelector('[data-devices]').append(row);
      }
      const showEnrollment = async request => {
        content.querySelector('[data-qr]').replaceChildren();
        await showQr(link('device', request.requestId), 'Открыть ваш профиль на этом устройстве');
        if (stopped) return;
        content.querySelector('[data-enroll]').hidden = true;
        const host = content.querySelector('[data-qr]');
        const code = document.createElement('p'); code.textContent = `Сравните на обоих устройствах код: ${request.requestId.slice(-8).toUpperCase().match(/.{1,4}/g).join(' ')}`; host.append(code);
        note(request.expiresAt <= Date.now() ? 'Срок QR закончился. Проверьте уже завершённое подтверждение или создайте новый QR.' : `Подтвердите на прежнем устройстве. Код действует до ${date(request.expiresAt)}.`);
        const details = document.createElement('p'); host.append(details);
        const finish = materialElement('button', 'control'); finish.textContent = 'Проверить подтверждение'; host.append(finish);
        let preview = null;
        finish.addEventListener('click', guard(async () => {
          if (!preview) {
            const checked = await client.previewEnrollment(request.requestId);
            if (!checked.account) { note('Подтверждения пока нет. Откройте этот QR на прежнем устройстве.'); return; }
            preview = checked;
            details.textContent = `Подтверждён профиль «${preview.account.label}» (${preview.account.accountId.slice(-8)}). Источник: ${preview.source.label}. Текущий локальный профиль сохранится отдельно. Если это не ваш профиль, создайте новый QR.`;
            finish.textContent = `Открыть профиль ${preview.account.label}`;
            note('Проверьте профиль и устройство, с которого пришло подтверждение.');
            return;
          }
          try { if(!await guardedIdentityMutation(preview.account.accountId,(fresh,signal)=>client.finishEnrollment(request.requestId,preview.account.accountId,{expectedState:activationSelection(fresh),signal}),fresh=>fresh.pendingEnrollment?.requestId===request.requestId))return; }
          catch (error) { preview = null; finish.textContent = 'Проверить подтверждение'; details.textContent = ''; throw error; }
          tab = 'recovery'; note('Профиль открыт. Прежний локальный профиль сохранён отдельно. Можно загрузить его сохранённые комнаты.'); await render();
        }));
        const restart = materialElement('button', 'control'); restart.textContent = 'Создать новый QR'; host.append(restart);
        restart.addEventListener('click', guard(async () => {
          if (!confirm('Оставить этот запрос и создать новый QR? Уже открытые профили сохранятся.')) return;
          const nextLabel = content.querySelector('[data-device-label]').value || label;
          await client.discardPendingEnrollment();
          enrollment = await client.startEnrollment(nextLabel); await showEnrollment(enrollment);
        }));
      };
      on('[data-enroll]', async () => {
        enrollment = await client.startEnrollment(content.querySelector('[data-device-label]').value || label);
        await showEnrollment(enrollment);
      });
      if (local.pendingEnrollment?.requestId) await showEnrollment(local.pendingEnrollment);
    } else if (tab === 'recovery') {
      const verified = status?.recovery?.verified === true;
      content.innerHTML = `<h3>Не потерять профиль</h3><p>${verified ? 'Запасной способ входа проверен.' : 'Пока доступ зависит от ваших устройств. Сохраните и проверьте файл восстановления.'}</p>
        ${local.accountId ? '<button data-glass-role="control" data-glass-safe-area="off" data-save-recovery>Создать файл восстановления</button>' : ''}<div data-recovery-confirm></div>
        <label>Открыть профиль из файла восстановления<input type="file" accept="application/json,.json" data-recover-file></label><button data-glass-role="control" data-glass-safe-area="off" data-recover>Восстановить профиль</button>
        <p class="connect-muted">Файл восстановления открывает ваш профиль. Это не копия содержимого приложений. Храните его в надёжном месте, отдельно от устройства. После использования создайте новый.</p>`;
      on('[data-save-recovery]', async () => {
        recoveryKit = await client.prepareRecovery(); download(`${productName}-recovery.json`, recoveryKit);
        content.querySelector('[data-recovery-confirm]').innerHTML = '<p>Проверьте сохранённый файл: выберите его с диска. Только после проверки он заменит прежний запасной способ.</p><input type="file" accept="application/json,.json" data-confirm-file><button data-glass-role="control" data-glass-safe-area="off" data-confirm>Проверить файл</button>';
        on('[data-confirm]', async () => { const kit = await readFile('[data-confirm-file]'); await client.confirmRecovery(kit); recoveryKit = null; note('Файл проверен. Запасной вход готов.'); await render(); });
        note('Файл подготовлен. Проверьте его, чтобы включить восстановление.');
      });
      on('[data-recover]', async () => { const kit = await readFile('[data-recover-file]'); if (!confirm('Открыть профиль из выбранного файла? Текущий профиль будет сохранён отдельно.')) return; if(!await guardedIdentityMutation(null,(fresh,signal)=>client.recover(kit,label,{expectedState:activationSelection(fresh),signal})))return; note('Профиль восстановлен. Загрузите копию данных и создайте новый файл восстановления.'); await render(); });
      if(local.accountId&&typeof client.credentialStatus==='function'){const credentials=document.createElement('section');credentials.className='connect-credential-settings';content.append(credentials);try{await mountCredentialSettings(credentials,{client,accountId:local.accountId,guard,isCurrent:()=>!stopped&&generation===renderGeneration,onSaved:note});}catch(error){if(error?.code==='unsupported_operation')credentials.remove();else if(!stopped&&generation===renderGeneration)note(friendly(error));}}
    } else if (tab === 'data') {
      content.innerHTML = `<h3>Сохранённая копия данных</h3><p>${escape(snapshotDescription)}</p>
        ${snapshot ? '<button data-glass-role="control" data-glass-safe-area="off" data-save>Сохранить текущие данные</button>' : ''}
        ${restore ? '<button data-glass-role="control" data-glass-safe-area="off" data-load>Загрузить сохранённую копию</button>' : ''}
        ${onOpenDataTools ? '<button data-glass-role="control" data-glass-safe-area="off" data-data-tools>Прежние комнаты и инструменты</button>' : ''}
        ${!snapshot && !restore ? '<p class="connect-muted">В этом разделе нет полной копии всех приложений. Каждое приложение управляет своими данными и экспортом.</p>' : ''}
        <p class="connect-muted">Копия данных и файл восстановления профиля — разные вещи. Файл входа создаётся в разделе «Восстановление».</p>`;
      on('[data-data-tools]', async () => { dialog.close(); await onOpenDataTools(); });
      on('[data-save]', async () => {
        const expectedAccountId = local.accountId;
        if (!expectedAccountId) throw new Error('profile_required');
        const payload = await snapshot();
        const result = await client.saveVault(payload, undefined, expectedAccountId);
        note(`Зашифрованная копия сохранена${result.revision ? ` · версия ${result.revision}` : ''}.`);
      });
      on('[data-load]', async () => {
        const expectedAccountId = local.accountId;
        if (!expectedAccountId) throw new Error('profile_required');
        const result = await client.loadVault(expectedAccountId);
        if (!result || result.payload === null) { note('Сохранённой копии пока нет.'); return; }
        const payload = result.payload ?? result;
        if (!confirm('Добавить комнаты из сохранённой копии? Совпадающие местные комнаты и их тексты сохранятся.')) return;
        if ((await client.getLocalState()).accountId !== expectedAccountId) throw Object.assign(new Error('profile_changed'), { code: 'ACTIVE_PROFILE_CHANGED' });
        await restore(payload); note('Копия добавлена. Существующие данные сохранены.');
      });
    }
  }
  function waitForConnectedDevice(target, accountId) {
    const generation = renderGeneration;
    const current = () => !stopped && dialog.open && generation === renderGeneration && local?.accountId === accountId;
    const check = async () => {
      if (!current()) return;
      if (status?.account?.id === accountId && status.devices.some(device => device.id === target.deviceId && !device.revokedAt)) {
        await render();
        if (!stopped && renderGeneration === generation + 1 && local?.accountId === accountId) {
          note('Устройство подключено. Ваши Соты открыты на обоих устройствах.');
          const done = materialElement('button', 'control'); done.type = 'button'; done.textContent = 'Готово';
          done.addEventListener('click', () => dialog.close(), { signal: panelLifetime.signal }); content.prepend(done); done.focus();
        }
        return;
      }
      if (Date.now() >= target.expiresAt) { note('Срок подключения истёк. Откройте новый QR на втором устройстве.'); return; }
      timer = setTimeout(async () => {
        if (!current()) return;
        try { await refresh(); await check(); }
        catch (error) { if (current()) note(friendly(error)); }
      }, 1200);
    };
    void check().catch(error => { if (!stopped) note(friendly(error)); });
  }
  async function renderIntent() {
    if (intent.kind === 'contact') {
      const card = await client.resolveCard(intent.id); if (stopped) return;
      content.innerHTML = `<h3>Добавить ${escape(card.label || 'человека')}?</h3><p>Ваш профиль: ${escape(status?.account?.label || label)}. Будут видны ваше имя и запрос знакомства в ${escape(productName)}.</p><button data-glass-role="control" data-glass-safe-area="off" data-request>Отправить запрос</button><button data-glass-role="control" data-glass-safe-area="off" data-cancel>Отмена</button>`;
      on('[data-request]', async () => { await client.requestContact(intent.id); intent = null; tab = 'people'; note('Запрос отправлен. Друг появится после принятия.'); await render(); });
    } else {
      const target = await client.inspectEnrollment(intent.id); if (stopped) return;
      const expectedAccountId = status?.account?.id;
      if (!expectedAccountId || target.accountId !== expectedAccountId) throw Object.assign(new Error('profile_changed'), { code: 'ACTIVE_PROFILE_CHANGED' });
      const pairingCode = target.requestId.slice(-8).toUpperCase().match(/.{1,4}/g).join(' ');
      content.innerHTML = `<h3>Открыть ваш профиль на устройстве?</h3><p>Получатель: ${escape(target.label || 'Новое устройство')}.</p><p>Сравните код <strong>${escape(pairingCode)}</strong> с экраном, который подключаете.</p><p>Откроется профиль ${escape(status.account.label)} (${escape(expectedAccountId.slice(-8))}) и его сохранённые данные. Подтверждайте только свой запрос.</p><button data-glass-role="control" data-glass-safe-area="off" data-approve>Открыть мой профиль</button><button data-glass-role="control" data-glass-safe-area="off" data-cancel>Отмена</button>`;
      on('[data-approve]', async () => {
        await client.approveEnrollment(target.requestId, expectedAccountId);
        intent = null; tab = 'devices'; note('Подтверждено. Ждём, пока второе устройство откроет Соты.');
        await render(); waitForConnectedDevice(target, expectedAccountId);
      });
    }
    on('[data-cancel]', async () => { intent = null; await render(); });
  }
  void guard(async () => {
    local = await client.getLocalState();
    if (!local.accountId && bootstrapOnOpen && tab !== 'profiles' && tab !== 'connect') { try { await guardedIdentityMutation(null,(fresh,signal)=>client.bootstrap(label,{expectedState:activationSelection(fresh),signal})); } catch (error) { note(friendly(error)); } }
    await render();
  })().catch(error => {
    if (stopped) return;
    content.replaceChildren(); content.setAttribute('aria-busy', 'false'); note(friendly(error));
  });
  return { element: dialog, close: () => dialog.close(), refresh: () => guard(render)() };
}
