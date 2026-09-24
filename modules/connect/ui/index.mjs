const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const date = value => value ? new Date(value).toLocaleString('ru-RU') : '—';
const errors = {
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
  storage_corrupt: 'Не удалось прочитать данные входа. Используйте восстановление; существующие данные не перезаписываются.'
};
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
    const url = new URL(value, origin);
    if (url.origin !== origin) return null;
    const kind = url.searchParams.get('connect');
    const id = url.searchParams.get(kind === 'contact' ? 'card' : 'request');
    if (!['contact', 'device'].includes(kind) || !id || !/^[A-Za-z0-9_-]{16,160}$/.test(id)) return null;
    return { kind, id };
  } catch { return null; }
}
export function openConnectPanel({ client, label = 'Мой профиль', productName = 'Соты', qr, snapshot, restore, invitation, onRename,
  snapshotDescription = 'Комнаты и тексты. Вложения сохраняйте отдельно.', initialIntent = null }) {
  const previousFocus = document.activeElement;
  const dialog = document.createElement('dialog');
  dialog.className = 'connect-panel';
  dialog.setAttribute('aria-label', 'Профиль, люди и устройства');
  dialog.innerHTML = `<header><div><small>${escape(productName)}</small><h2>Профиль и подключения</h2></div><button type="button" data-close aria-label="Закрыть">×</button></header>
    <nav aria-label="Разделы профиля"><button data-tab="profile">Профиль</button><button data-tab="people">Люди</button><button data-tab="devices">Устройства</button><button data-tab="recovery">Сохранность</button></nav>
    <p class="connect-message" role="status" aria-live="polite"></p><section class="connect-content" aria-busy="true"></section>`;
  document.body.append(dialog); dialog.showModal();
  const content = dialog.querySelector('.connect-content');
  const message = dialog.querySelector('.connect-message');
  let tab = 'profile', busy = false, stopped = false, status, local, enrollment, recoveryKit, timer, intent = initialIntent;
  const note = text => { if (!stopped) message.textContent = text; };
  const guard = fn => async event => {
    event?.preventDefault(); if (busy || stopped) return;
    busy = true; content.setAttribute('aria-busy', 'true');
    const buttons = [...content.querySelectorAll('button')]; buttons.forEach(b => b.disabled = true);
    try {
      if (local?.accountId && (await client.getLocalState()).accountId !== local.accountId) {
        note(errors.active_profile_changed); await render(); return;
      }
      await fn(event);
    } catch (error) { note(friendly(error)); }
    finally { busy = false; content.setAttribute('aria-busy', 'false'); buttons.forEach(b => { if (b.isConnected) b.disabled = false; }); }
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
  function stop() { stopped = true; clearTimeout(timer); dialog.remove(); if (previousFocus?.isConnected) previousFocus.focus(); }
  dialog.querySelector('[data-close]').addEventListener('click', () => dialog.close());
  dialog.addEventListener('close', stop);
  dialog.querySelectorAll('[data-tab]').forEach(button => button.addEventListener('click', guard(async () => { intent = null; tab = button.dataset.tab; await render(); })));
  async function refresh() {
    local = await client.getLocalState();
    if (local.accountId) {
      try {
        const raw = await client.status();
        status = { ...raw, account: { id: raw.accountId, label: raw.label }, devices: (raw.devices || []).map(d => ({ ...d, id: d.deviceId })) };
      } catch (error) { status = null; note(friendly(error)); }
    }
  }
  async function showQr(url, title) {
    const host = content.querySelector('[data-qr]'); if (!host) return;
    const caption = document.createElement('p'); caption.textContent = title; host.append(caption);
    if (qr) {
      const src = await qr(url); if (stopped || !host.isConnected) return;
      const img = document.createElement('img'); img.src = src; img.alt = title; img.width = 224; img.height = 224; host.prepend(img);
    }
    const input = document.createElement('input'); input.readOnly = true; input.value = url; input.setAttribute('aria-label', 'Ссылка для подключения'); host.append(input);
    const copy = document.createElement('button'); copy.textContent = 'Скопировать ссылку'; copy.type = 'button'; host.append(copy);
    copy.addEventListener('click', guard(async () => { await navigator.clipboard.writeText(url); note('Ссылка скопирована.'); }));
  }
  const link = (kind, id) => `${location.origin}/?connect=${kind}&${kind === 'contact' ? 'card' : 'request'}=${encodeURIComponent(id)}`;
  async function render() {
    clearTimeout(timer);
    if (stopped) return;
    await refresh(); if (stopped) return;
    dialog.querySelectorAll('[data-tab]').forEach(b => b.setAttribute('aria-current', b.dataset.tab === tab ? 'page' : 'false'));
    if (intent) { await renderIntent(); return; }
    if (tab === 'profile') {
      const account = status?.account;
      content.innerHTML = `<h3>${escape(account?.label || label)}</h3><p>Ваш профиль сохраняется при обновлениях. У каждого подключённого устройства свой доступ.</p>
        <label>Как вас увидят друзья<input data-name maxlength="64" value="${escape(account?.label || label)}"></label><button data-name-save>Сохранить имя</button>
        <button data-card>Мой QR для знакомства</button><button data-existing>Открыть мой профиль с другого устройства</button>
        <label>Открыть ссылку или QR из сообщения<input data-link placeholder="Вставьте ссылку" autocomplete="off"></label><button data-open-link>Продолжить по ссылке</button>
        <div data-qr></div><div data-profiles></div><p class="connect-muted">При добавлении друга вы остаётесь каждый со своим аккаунтом.</p>`;
      on('[data-card]', async () => { const card = await client.card(); content.querySelector('[data-qr]').replaceChildren(); await showQr(link('contact', card.cardId), 'Добавить меня в контакты'); });
      on('[data-name-save]', async () => { const name = content.querySelector('[data-name]').value; await client.rename(name); if (onRename) await onRename(name); note('Имя сохранено.'); await render(); });
      on('[data-existing]', async () => { tab = 'devices'; await render(); });
      on('[data-open-link]', async () => { const next = parseConnectLink(content.querySelector('[data-link]').value); if (!next) { note('Это не ссылка подключения этого проекта.'); return; } intent = next; await render(); });
      for (const p of local.profiles || []) {
        if (p.active || p.revoked) continue;
        const b = document.createElement('button'); b.textContent = `Открыть сохранённый профиль: ${p.label}`;
        b.addEventListener('click', guard(async () => { await client.switchProfile(p.accountId); await render(); })); content.querySelector('[data-profiles]').append(b);
      }
    } else if (tab === 'people') {
      const list = await client.contacts(); if (stopped) return;
      const contacts = list.contacts || [], incoming = list.requests?.incoming || [], outgoing = list.requests?.outgoing || [];
      content.innerHTML = `<h3>Люди в ${escape(productName)}</h3><p>Контакт помогает снова найти человека. Доступ к комнатам выдаётся отдельным приглашением.</p>
        <button data-card>Показать мой QR</button><button data-refresh>Обновить</button><div data-qr></div>
        <h4>Входящие запросы</h4><div data-incoming>${incoming.length ? '' : '<p class="connect-muted">Новых запросов нет.</p>'}</div>
        <h4>Контакты</h4><div data-contacts>${contacts.length ? '' : '<p class="connect-muted">Покажите другу свой QR, чтобы добавиться.</p>'}</div>
        <h4>Отправленные запросы</h4><div data-outgoing></div><h4>Приглашения</h4><div data-invitations></div>
        <details><summary>Нежелательные обращения и визитка</summary><div data-blocked></div><button data-rotate>Сменить адрес моей визитки</button></details>`;
      const row = (container, text, actions) => { const el = document.createElement('div'); el.className = 'connect-row'; const span = document.createElement('span'); span.textContent = text; el.append(span); for (const [title, fn] of actions) { const b = document.createElement('button'); b.textContent = title; b.type = 'button'; b.addEventListener('click', guard(fn)); el.append(b); } content.querySelector(container).append(el); };
      for (const r of incoming) row('[data-incoming]', r.label || 'Новый контакт', [['Принять', async () => { await client.acceptContact(r.requestId); note('Вы добавили друг друга.'); await render(); }], ['Отклонить', async () => { await client.declineContact(r.requestId); await render(); }], ['Заблокировать', async () => { await client.blockContact(r.peerAccountId); await render(); }]]);
      for (const r of contacts) {
        const actions = [];
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
    } else if (tab === 'devices') {
      content.innerHTML = `<h3>Ваши устройства</h3><p>Чтобы открыть прежний профиль на этом устройстве, покажите QR телефону, где он уже открыт.</p>
        <label>Название этого устройства<input data-device-label value="${escape(local.pendingEnrollment?.label || label)}" maxlength="64"></label>
        <button data-enroll>Открыть прежний профиль здесь</button><div data-qr></div><div data-devices></div>
        <p class="connect-muted">Отключение прекращает доступ к профилю и сохранённым копиям. Уже открытые старые комнаты и скачанные данные требуют отдельного управления.</p>`;
      for (const d of status?.devices || []) {
        const row = document.createElement('div'); row.className = 'connect-row';
        const span = document.createElement('span'); span.textContent = `${d.label || 'Устройство'}${d.id === status.deviceId ? ' · это устройство' : ''}${d.revokedAt ? ' · отключено' : ''}`; row.append(span);
        if (!d.revokedAt) { const b = document.createElement('button'); b.textContent = 'Отключить'; b.addEventListener('click', guard(async () => { if (!confirm('Завершить доступ этого устройства к профилю?')) return; await client.revokeDevice(d.id); note('Доступ устройства завершён.'); await render(); })); row.append(b); }
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
        const finish = document.createElement('button'); finish.textContent = 'Проверить подтверждение'; host.append(finish);
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
          try { await client.finishEnrollment(request.requestId, preview.account.accountId); }
          catch (error) { preview = null; finish.textContent = 'Проверить подтверждение'; details.textContent = ''; throw error; }
          tab = 'recovery'; note('Профиль открыт. Прежний локальный профиль сохранён отдельно. Можно загрузить его сохранённые комнаты.'); await render();
        }));
        const restart = document.createElement('button'); restart.textContent = 'Создать новый QR'; host.append(restart);
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
    } else {
      const verified = status?.recovery?.verified === true;
      content.innerHTML = `<h3>Не потерять профиль</h3><p>${verified ? 'Запасной способ входа проверен.' : 'Пока доступ зависит от ваших устройств. Сохраните и проверьте файл восстановления.'}</p>
        <button data-save-recovery>Создать файл восстановления</button><div data-recovery-confirm></div>
        <label>Открыть профиль из файла восстановления<input type="file" accept="application/json,.json" data-recover-file></label><button data-recover>Восстановить профиль</button>
        <h4>Сохранённая копия данных</h4><p>${escape(snapshotDescription)}</p>
        ${snapshot ? '<button data-save>Сохранить текущие данные</button>' : ''}
        ${restore ? '<button data-load>Загрузить сохранённую копию</button>' : ''}
        <p class="connect-muted">Файл восстановления открывает ваш профиль. Храните его в надёжном месте, отдельно от устройства. После использования создайте новый.</p>`;
      on('[data-save-recovery]', async () => {
        recoveryKit = await client.prepareRecovery(); download(`${productName}-recovery.json`, recoveryKit);
        content.querySelector('[data-recovery-confirm]').innerHTML = '<p>Проверьте сохранённый файл: выберите его с диска. Только после проверки он заменит прежний запасной способ.</p><input type="file" accept="application/json,.json" data-confirm-file><button data-confirm>Проверить файл</button>';
        on('[data-confirm]', async () => { const kit = await readFile('[data-confirm-file]'); await client.confirmRecovery(kit); recoveryKit = null; note('Файл проверен. Запасной вход готов.'); await render(); });
        note('Файл подготовлен. Проверьте его, чтобы включить восстановление.');
      });
      on('[data-recover]', async () => { const kit = await readFile('[data-recover-file]'); if (!confirm('Открыть профиль из выбранного файла? Текущий профиль будет сохранён отдельно.')) return; await client.recover(kit, label); note('Профиль восстановлен. Загрузите копию данных и создайте новый файл восстановления.'); await render(); });
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
      const expectedAccountId = status?.account?.id;
      if (!expectedAccountId || target.accountId !== expectedAccountId) throw Object.assign(new Error('profile_changed'), { code: 'ACTIVE_PROFILE_CHANGED' });
      const pairingCode = target.requestId.slice(-8).toUpperCase().match(/.{1,4}/g).join(' ');
      content.innerHTML = `<h3>Открыть ваш профиль на устройстве?</h3><p>Получатель: ${escape(target.label || 'Новое устройство')}.</p><p>Сравните код <strong>${escape(pairingCode)}</strong> с экраном, который подключаете.</p><p>Откроется профиль ${escape(status.account.label)} (${escape(expectedAccountId.slice(-8))}) и его сохранённые данные. Подтверждайте только свой запрос.</p><button data-approve>Открыть мой профиль</button><button data-cancel>Отмена</button>`;
      on('[data-approve]', async () => { await client.approveEnrollment(target.requestId, expectedAccountId); intent = null; tab = 'devices'; note('Доступ разрешён. Проверьте имя профиля и завершите вход на новом устройстве.'); await render(); });
    }
    on('[data-cancel]', async () => { intent = null; await render(); });
  }
  void guard(async () => {
    local = await client.getLocalState();
    if (!local.accountId) { try { await client.bootstrap(label); } catch (error) { note(friendly(error)); } }
    await render();
  })();
  return { close: () => dialog.close(), refresh: () => guard(render)() };
}
