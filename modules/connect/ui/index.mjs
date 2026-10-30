import { mountPlatformInvitations } from './platform-invitations.mjs';
import { watchDeviceEnrollment } from './device-enrollment.mjs';
import { mountCredentialLogin, mountCredentialSettings } from './credential-forms.mjs';
const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const date = value => value ? new Date(value).toLocaleString('ru-RU') : '—';
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
  storage_future_version: 'Данные созданы более новой версией. Обновите приложение — аккаунт не будет пересоздан.',
  storage_corrupt: 'Не удалось прочитать данные входа. Используйте восстановление; существующие данные не перезаписываются.',
  weak_password: 'Выберите уникальный пароль длиной от 15 символов.',
  weak_phrase: 'Выберите длинную уникальную фразу: от 20 символов на любом языке.',
  invalid_login: 'Проверьте логин: от 3 до 80 символов без пробелов.',
  credential_unavailable: 'Не удалось войти. Проверьте логин, пароль или фразу.',
  credential_exists: 'Этот логин уже занят. Выберите другой.',
  credential_rate_limited: 'Слишком много попыток. Подождите и повторите вход.',
  credential_pending: 'Предыдущая попытка входа ещё не завершена. Повторите её с теми же данными.',
  recovery_pending: 'Восстановление из файла ещё не завершено. Повторите его с тем же файлом.',
};
function deviceLabel() {
  const platform = globalThis.navigator?.userAgentData?.platform || globalThis.navigator?.platform;
  return platform === 'Win32' ? 'Windows' : platform === 'MacIntel' ? 'Mac' : platform?.startsWith('Linux') ? 'Linux' : platform || 'Браузер';
}
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
export function openConnectPanel({ client, label = 'Мой профиль', productName = 'Соты', qr, snapshot, restore, invitation, onRename, onContactCall, onContactMessage, onEditProfile, onOpenDataTools,
  snapshotDescription = 'Комнаты и тексты. Вложения сохраняйте отдельно.', initialIntent = null, initialTab = 'profile', bootstrapOnOpen = true }) {
  if (typeof bootstrapOnOpen !== 'boolean') throw new TypeError('invalid_bootstrap_policy');
  const previousFocus = document.activeElement;
  const dialog = document.createElement('dialog');
  dialog.className = 'connect-panel';
  dialog.setAttribute('aria-label', 'Входы и данные профиля');
  dialog.innerHTML = `<header><div><small>${escape(productName)}</small><h2>Входы и данные профиля</h2></div><button type="button" data-close aria-label="Закрыть">×</button></header>
    <nav aria-label="Разделы профиля"><button data-tab="profile">Профиль</button><button data-tab="people">Люди</button><button data-tab="devices">Устройства входа</button><button data-tab="recovery">Восстановление</button><button data-tab="data">Копии данных</button><button data-tab="profiles">Выбрать профиль</button></nav>
    <p class="connect-message" role="status" aria-live="polite"></p><section class="connect-content" aria-busy="true"></section>`;
  document.body.append(dialog); dialog.showModal();
  const content = dialog.querySelector('.connect-content');
  const message = dialog.querySelector('.connect-message');
  let tab = ['profile', 'people', 'devices', 'recovery', 'data', 'profiles', 'connect'].includes(initialTab) ? initialTab : 'profile', busy = false, stopped = false, status, local, recoveryKit, intent = initialIntent;
  let deviceEntry = null, entryLifetime = null, renderGeneration = 0;
  let platformInvitations = null;
  const note = text => { if (!stopped) message.textContent = text; };
  const guard = fn => async event => {
    event?.preventDefault(); if (busy || stopped || !dialog.open) return;
    busy = true; content.setAttribute('aria-busy', 'true'); content.inert = true; dialog.querySelector('nav').inert = true;
    const buttons = [...content.querySelectorAll('button')].map(button => [button, button.disabled]); buttons.forEach(([button]) => button.disabled = true);
    try {
      if (local?.accountId && (await client.getLocalState()).accountId !== local.accountId) {
        note(errors.active_profile_changed); await render(); return;
      }
      await fn(event);
    } catch (error) { note(friendly(error)); }
    finally { busy = false; content.setAttribute('aria-busy', 'false'); content.inert = false; dialog.querySelector('nav').inert = false;
      buttons.forEach(([button, disabled]) => { if (button.isConnected) button.disabled = disabled; }); }
  };
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
  function stopEntry() { deviceEntry?.stop(); deviceEntry = null; entryLifetime?.abort(); entryLifetime = null; }
  function stop() { platformInvitations?.destroy(); stopped = true; stopEntry(); dialog.remove(); if (previousFocus?.isConnected) previousFocus.focus(); }
  dialog.querySelector('[data-close]').addEventListener('click', () => { stopEntry(); dialog.close(); });
  dialog.addEventListener('cancel', event => { if (event.target === dialog) stopEntry(); });
  dialog.addEventListener('close', stop);
  dialog.querySelectorAll('[data-tab]').forEach(button => button.addEventListener('click', guard(async () => { intent = null; tab = button.dataset.tab; await render(); })));
  async function refresh() {
    local = await client.getLocalState();
    if (local.accountId && tab !== 'profiles' && tab !== 'connect' && intent?.kind !== 'device') {
      try {
        const raw = await client.status();
        status = { ...raw, account: { id: raw.accountId, label: raw.label }, devices: (raw.devices || []).map(d => ({ ...d, id: d.deviceId })) };
      } catch (error) { status = null; note(friendly(error)); }
    }
  }
  async function showQr(url, title, current = () => !stopped) {
    const host = content.querySelector('[data-qr]'); if (!host) return;
    if (!current()) return;
    const caption = document.createElement('p'); caption.textContent = title; host.append(caption);
    if (qr) {
      const src = await qr(url); if (!current() || !host.isConnected) return;
      const img = document.createElement('img'); img.src = src; img.alt = title; img.width = 224; img.height = 224; host.prepend(img);
    }
    const linkHost = tab === 'connect' ? content.querySelector('[data-other-ways]') || host : host;
    const input = document.createElement('input'); input.readOnly = true; input.value = url; input.setAttribute('aria-label', 'Ссылка для подключения'); linkHost.append(input);
    const copy = document.createElement('button'); copy.textContent = 'Скопировать ссылку'; copy.type = 'button'; linkHost.append(copy);
    copy.addEventListener('click', guard(async () => { await navigator.clipboard.writeText(url); note('Ссылка скопирована.'); }));
  }
  const link = (kind, id) => `${location.origin}/?connect=${kind}&${kind === 'contact' ? 'card' : 'request'}=${encodeURIComponent(id)}`;
  async function render() {
    platformInvitations?.destroy(); platformInvitations = null;
    stopEntry(); const generation = ++renderGeneration;
    if (stopped) return;
    await refresh(); if (stopped || generation !== renderGeneration) return;
    const titles = { profile: 'Профиль', people: 'Контакты', devices: 'Устройства входа', recovery: 'Восстановление профиля', data: 'Копии данных', profiles: 'Выбрать профиль', connect: 'Открыть прежний аккаунт' };
    const title = intent?.kind === 'device' ? 'Подтвердить вход' : titles[tab];
    dialog.querySelector('h2').textContent = title; dialog.setAttribute('aria-label', title);
    dialog.querySelector('nav').hidden = tab === 'connect' || intent?.kind === 'device';
    dialog.classList.toggle('connect-entry', tab === 'connect' || intent?.kind === 'device');
    dialog.querySelectorAll('[data-tab]').forEach(b => b.setAttribute('aria-current', b.dataset.tab === tab ? 'page' : 'false'));
    if (intent) { await renderIntent(); return; }
    if (tab === 'profiles') {
      content.innerHTML = '<h3>Ваши профили на этом устройстве</h3><p>Выберите, от чьего имени продолжить. Данные и разрешения каждого профиля хранятся отдельно.</p><div data-profile-list></div><button data-existing>Открыть профиль с другого устройства</button><button data-recovery>Восстановить из файла</button>';
      const known = new Map();
      for (const profile of local.profiles || []) {
        if (!profile.revoked && (!known.has(profile.accountId) || profile.active)) known.set(profile.accountId, profile);
      }
      if (!known.size) content.querySelector('[data-profile-list]').textContent = 'Сохранённых профилей пока нет.';
      for (const profile of known.values()) {
        const row = document.createElement('button'); row.type = 'button'; row.dataset.profileId = profile.accountId;
        row.textContent = `${profile.label || 'Профиль'}${profile.active ? ' · текущий' : ''}`; row.disabled = Boolean(profile.active);
        row.addEventListener('click', guard(async () => { await client.switchProfile(profile.accountId); dialog.close(); }));
        content.querySelector('[data-profile-list]').append(row);
      }
      on('[data-existing]', async () => { tab = 'connect'; await render(); });
      on('[data-recovery]', async () => { tab = 'recovery'; await render(); });
      return;
    }
    if (!local.accountId && (tab === 'profile' || tab === 'people')) {
      content.innerHTML = '<h3>Открыть существующий профиль</h3><p>Подтвердите новое устройство с вашего прежнего устройства или используйте файл восстановления.</p><button data-existing>Подключить по QR</button><button data-recovery>Открыть файл восстановления</button>';
      on('[data-existing]', async () => { tab = 'connect'; await render(); });
      on('[data-recovery]', async () => { tab = 'recovery'; await render(); });
      return;
    }
    if (tab === 'profile') {
      const account = status?.account;
      content.innerHTML = `<h3>${escape(account?.label || label)}</h3><p>Ваш профиль сохраняется при обновлениях. У каждого подключённого устройства свой доступ.</p>
        ${typeof onEditProfile === 'function' ? '<button data-edit-profile>Имя, фото и описание</button>' : `<label>Как вас увидят друзья<input data-name maxlength="80" value="${escape(account?.label || label)}"></label><button data-name-save>Сохранить имя</button>`}
        <button data-card>Мой QR для знакомства</button><button data-existing>Открыть мой профиль с другого устройства</button>
        <label>Открыть ссылку или QR из сообщения<input data-link placeholder="Вставьте ссылку" autocomplete="off"></label><button data-open-link>Продолжить по ссылке</button>
        <div data-qr></div><button data-select-profile>Выбрать другой сохранённый профиль</button><p class="connect-muted">При добавлении друга вы остаётесь каждый со своим аккаунтом.</p>`;
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
        <button data-card>Показать мой QR</button><button data-refresh>Обновить</button><div data-qr></div>
        <h4>Входящие запросы</h4><div data-incoming>${incoming.length ? '' : '<p class="connect-muted">Новых запросов нет.</p>'}</div>
        <h4>Контакты</h4><div data-contacts>${contacts.length ? '' : '<p class="connect-muted">Покажите другу свой QR, чтобы добавиться.</p>'}</div>
        <h4>Отправленные запросы</h4><div data-outgoing></div><h4>Приглашения</h4><div data-invitations></div>
        <details><summary>Нежелательные обращения и визитка</summary><div data-blocked></div><button data-rotate>Сменить адрес моей визитки</button></details>`;
      const row = (container, text, actions) => { const el = document.createElement('div'); el.className = 'connect-row'; const span = document.createElement('span'); span.textContent = text; el.append(span); for (const [title, fn, accessibleName] of actions) { const b = document.createElement('button'); b.textContent = title; b.type = 'button'; if (accessibleName) b.setAttribute('aria-label', accessibleName); b.addEventListener('click', guard(fn)); el.append(b); } content.querySelector(container).append(el); };
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
      content.innerHTML = `<p>Отсканируйте QR устройством, где уже открыт ваш аккаунт, и подтвердите вход. Здесь аккаунт откроется сам.</p>
        <div data-qr></div><p data-pairing-code class="connect-pairing-code"></p>
        <p data-entry-status role="status">Готовим QR…</p><button data-new-qr hidden>Обновить QR</button>
        <details class="connect-other"><summary>Другой способ</summary><div data-other-ways></div>
          <label>Файл восстановления<input type="file" accept="application/json,.json" data-entry-file></label>
          <div data-credential-login></div><button data-known-profiles>Сохранённые аккаунты</button></details>`;
      entryLifetime = new AbortController();
      const lifetime = entryLifetime, current = () => !stopped && dialog.open && generation === renderGeneration && !lifetime.signal.aborted;
      let qrGeneration = 0;
      const begin = () => { qrGeneration++; deviceEntry?.stop(); deviceEntry = null;
        if (current()) content.querySelector('[data-entry-status]').textContent = 'Открываем аккаунт…'; };
      const complete = () => { if (current()) dialog.close(); };
      const resume = async () => { if (current()) await startQr(); };
      mountCredentialLogin(content.querySelector('[data-credential-login]'), { client, label: deviceLabel(), guard, begin, resume, signal: lifetime.signal, onComplete: complete });
      content.querySelector('[data-entry-file]').addEventListener('change', guard(async () => {
        const input = content.querySelector('[data-entry-file]');
        try {
          const kit = await readFile('[data-entry-file]'); if (!current()) return;
          begin(); try { await client.recover(kit, deviceLabel(), { signal: lifetime.signal }); complete(); }
          catch (error) { await resume(); throw error; }
        } finally { if (input) input.value = ''; }
      }));
      on('[data-known-profiles]', async () => { tab = 'profiles'; await render(); });
      let expired = false;
      on('[data-new-qr]', async () => { if (expired) await client.discardPendingEnrollment(); await render(); });
      async function startQr() {
        deviceEntry?.stop(); const ticket = ++qrGeneration, currentQr = () => current() && ticket === qrGeneration;
        const watcher = watchDeviceEnrollment({ client, label: deviceLabel(), isCurrent: currentQr,
          onRequest: async request => {
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
      content.innerHTML = `<h3>Доступ к этому аккаунту</h3><p>Каждое устройство входит со своим ключом. Прежний аккаунт можно открыть здесь по QR.</p>
        <button data-enroll>Открыть прежний профиль здесь</button><div data-qr></div><div data-devices></div>
        <p class="connect-muted">Отключение прекращает доступ к профилю и сохранённым копиям. Уже открытые старые комнаты и скачанные данные требуют отдельного управления.</p>`;
      for (const d of status?.devices || []) {
        const row = document.createElement('div'); row.className = 'connect-row';
        const span = document.createElement('span'); span.textContent = `${d.label || 'Устройство'}${d.id === status.deviceId ? ' · это устройство' : ''}${d.revokedAt ? ' · отключено' : ''}`; row.append(span);
        if (!d.revokedAt) { const b = document.createElement('button'); b.textContent = 'Отключить'; b.addEventListener('click', guard(async () => { if (!confirm('Завершить доступ этого устройства к профилю?')) return; await client.revokeDevice(d.id); note('Доступ устройства завершён.'); await render(); })); row.append(b); }
        content.querySelector('[data-devices]').append(row);
      }
      on('[data-enroll]', async () => { tab = 'connect'; await render(); });
    } else if (tab === 'recovery') {
      const verified = status?.recovery?.verified === true;
      content.innerHTML = `<h3>Не потерять профиль</h3><p>${verified ? 'Файл восстановления проверен.' : 'Выберите запасной способ входа на случай, если прежнее устройство недоступно.'}</p>
        ${local.accountId ? '<button data-save-recovery>Создать файл восстановления</button>' : ''}<div data-recovery-confirm></div>
        <label>Открыть профиль из файла восстановления<input type="file" accept="application/json,.json" data-recover-file></label><button data-recover>Восстановить профиль</button>
        <p class="connect-muted">Файл восстановления открывает ваш профиль. Это не копия содержимого приложений. Храните его в надёжном месте, отдельно от устройства. После использования создайте новый.</p>`;
      on('[data-save-recovery]', async () => {
        recoveryKit = await client.prepareRecovery(); download(`${productName}-recovery.json`, recoveryKit);
        content.querySelector('[data-recovery-confirm]').innerHTML = '<p>Проверьте сохранённый файл: выберите его с диска. Только после проверки он заменит прежний запасной способ.</p><input type="file" accept="application/json,.json" data-confirm-file><button data-confirm>Проверить файл</button>';
        on('[data-confirm]', async () => { const kit = await readFile('[data-confirm-file]'); await client.confirmRecovery(kit); recoveryKit = null; note('Файл проверен. Запасной вход готов.'); await render(); });
        note('Файл подготовлен. Проверьте его, чтобы включить восстановление.');
      });
      on('[data-recover]', async () => { const kit = await readFile('[data-recover-file]'); if (!confirm('Открыть профиль из выбранного файла? Текущий профиль будет сохранён отдельно.')) return; await client.recover(kit, label); note('Профиль восстановлен. Загрузите копию данных и создайте новый файл восстановления.'); await render(); });
      if (local.accountId && typeof client.credentialStatus === 'function') {
        const credentials = document.createElement('section'); credentials.className = 'connect-credential-settings'; content.append(credentials);
        try { await mountCredentialSettings(credentials, { client, accountId: local.accountId, guard,
          isCurrent: () => !stopped && generation === renderGeneration, onSaved: note }); }
        catch (error) { if (!stopped && generation === renderGeneration) note(friendly(error)); }
      }
    } else if (tab === 'data') {
      content.innerHTML = `<h3>Сохранённая копия данных</h3><p>${escape(snapshotDescription)}</p>
        ${snapshot ? '<button data-save>Сохранить текущие данные</button>' : ''}
        ${restore ? '<button data-load>Загрузить сохранённую копию</button>' : ''}
        ${onOpenDataTools ? '<button data-data-tools>Прежние комнаты и инструменты</button>' : ''}
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
  async function renderIntent() {
    if (intent.kind === 'contact') {
      const card = await client.resolveCard(intent.id); if (stopped) return;
      content.innerHTML = `<h3>Добавить ${escape(card.label || 'человека')}?</h3><p>Ваш профиль: ${escape(status?.account?.label || label)}. Будут видны ваше имя и запрос знакомства в ${escape(productName)}.</p><button data-request>Отправить запрос</button><button data-cancel>Отмена</button>`;
      on('[data-request]', async () => { await client.requestContact(intent.id); intent = null; tab = 'people'; note('Запрос отправлен. Друг появится после принятия.'); await render(); });
    } else {
      const target = await client.inspectEnrollment(intent.id); if (stopped) return;
      const expectedAccountId = local.accountId;
      if (!expectedAccountId || target.accountId !== expectedAccountId) throw Object.assign(new Error('profile_changed'), { code: 'ACTIVE_PROFILE_CHANGED' });
      const pairingCode = target.requestId.slice(-8).toUpperCase().match(/.{1,4}/g).join(' ');
      content.innerHTML = `<h3>Подключить новое устройство?</h3><p>Ваш аккаунт: <strong>${escape(local.label || label)}</strong> (${escape(expectedAccountId.slice(-8))}).</p><p>Устройство: ${escape(target.label || 'Новое устройство')}.</p><p>Сравните код <strong class="connect-pairing-code">${escape(pairingCode)}</strong> с новым устройством. Подтверждайте только свой запрос.</p><button data-approve>Подтвердить вход</button><button data-cancel>Отмена</button>`;
      on('[data-approve]', async () => { await client.approveEnrollment(target.requestId, expectedAccountId); dialog.close(); });
    }
    on('[data-cancel]', async () => { intent = null; await render(); });
  }
  void guard(async () => {
    local = await client.getLocalState();
    if (!local.accountId && bootstrapOnOpen && tab !== 'profiles' && tab !== 'connect') { try { await client.bootstrap(label); } catch (error) { note(friendly(error)); } }
    await render();
  })();
  return { close: () => { stopEntry(); dialog.close(); }, refresh: () => guard(render)() };
}
