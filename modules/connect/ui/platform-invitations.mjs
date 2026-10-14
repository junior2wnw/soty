const el = (tag, text) => { const node = document.createElement(tag); if (text) node.textContent = text; return node; };
const friendly = error => ({ platform_invite_quota_exhausted: 'Все приглашения использованы.', platform_admin_required: 'Настройку меняет только администратор.', platform_policy_conflict: 'Настройка изменилась. Обновите список.', platform_invite_already_redeemed: 'По этому приглашению уже вошли.' }[error?.code] || 'Не удалось подтвердить действие. Повторите с тем же запросом.');
export function mountPlatformInvitations(host, { client, accountId, qr }) {
  let disposed = false, busy = false, sequence = 0;
  const key = `connect.platform-invites:${accountId}`;
  const load = () => { try { return JSON.parse(sessionStorage.getItem(key) || '{}'); } catch { return {}; } };
  const save = data => sessionStorage.setItem(key, JSON.stringify(data));
  const current = async () => !disposed && host.isConnected && (await client.getLocalState()).accountId === accountId;
  const call = (op, args = {}) => client.extension(op, args, { expectedAccountId: accountId });
  const note = el('p'); note.setAttribute('role', 'status');
  function button(title, action) {
    const button = el('button', title); button.type = 'button';
    button.addEventListener('click', async () => {
      if (busy || !await current()) return;
      busy = true; const buttons = [...host.querySelectorAll('button')];
      const prior = buttons.map(node => node.disabled); buttons.forEach(node => { node.disabled = true; });
      try { await action(); } catch (error) { if (await current()) note.textContent = friendly(error); }
      finally { busy = false; if (await current()) buttons.forEach((node,i) => { if (node.isConnected) node.disabled = prior[i]; }); }
    }); return button;
  }
  async function showLink(target, token) {
    const url = `${location.origin}/#soty-invite=${token}`;
    const input = el('input'); input.readOnly = true; input.value = url; input.setAttribute('aria-label', 'Приглашение в Соты'); target.append(input);
    target.append(button('Скопировать приглашение', async () => { await navigator.clipboard.writeText(url); note.textContent = 'Приглашение скопировано.'; }));
    if (qr) { const image = el('img'); image.alt = 'QR приглашения в Соты'; image.width = 224; image.height = 224;
      const src = await qr(url); if (!await current() || !target.isConnected) return; image.src = src; target.prepend(image); }
  }
  async function render() {
    const seq = ++sequence;
    try {
      const policy = await call('platform.admission.get'), list = await call('platform.invites.list');
      if (seq !== sequence || !await current()) return;
      host.replaceChildren(el('h3', 'Пригласить в Соты'), el('p', `Доступно ${list.remaining} из ${list.quota} приглашений`), note);
      note.textContent = '';
      const data = load();
      const issue = button(data.pending ? 'Повторить создание приглашения' : 'Создать приглашение', async () => {
        const data = load();
        if (!data.pending) {
          const bytes = crypto.getRandomValues(new Uint8Array(32));
          data.pending = { requestId: crypto.randomUUID(), token: btoa(String.fromCharCode(...bytes)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '') };
          save(data);
        }
        const response = await call('platform.invites.issue', data.pending);
        if (!await current()) return;
        data.links ??= {}; data.links[response.invitation.id] = data.pending.token; delete data.pending; save(data); await render();
      }); issue.disabled = list.remaining <= 0 && !data.pending; host.append(issue);
      host.append(button('Обновить приглашения', render));
      for (const invitation of list.invitations) {
        const active = !invitation.revokedAt && !invitation.redeemedAt && invitation.expiresAt > Date.now();
        const row = el('div'); row.className = 'connect-row';
        const state = invitation.redeemedAt ? 'Использовано' : invitation.revokedAt ? 'Отозвано' : active ? 'Ожидает входа' : 'Срок истёк';
        row.append(el('p', `${state} · до ${new Date(invitation.expiresAt).toLocaleString('ru-RU')}`));
        if (active) {
          row.append(button('Отозвать приглашение', async () => { await call('platform.invites.revoke', { requestId: `revoke_${invitation.id}`, invitationId: invitation.id }); const data = load(); if (data.links) delete data.links[invitation.id]; save(data); await render(); }));
          if (data.links?.[invitation.id]) row.append(button('Показать ссылку и QR', async () => { const target = el('div'); row.append(target); await showLink(target, data.links[invitation.id]); }));
        }
        host.append(row);
      }
      const details = el('details'), summary = el('summary', 'Вход в Соты'); details.append(summary);
      details.append(el('p', policy.inviteOnly ? 'Вход новых участников по приглашению включён.' : 'Вход по приглашению выключен. Новые участники могут войти свободно.'));
      if (policy.manageAllowed) {
        const label = el('label', 'Только по приглашению'); const toggle = el('input'); toggle.type = 'checkbox'; toggle.checked = policy.inviteOnly; toggle.setAttribute('aria-label', 'Только по приглашению'); label.append(toggle);
        const quotaLabel = el('label', 'Приглашений на человека'); const quota = el('input'); quota.type = 'number'; quota.min = '0'; quota.max = '100'; quota.value = String(policy.defaultQuota); quota.setAttribute('aria-label', 'Приглашений на человека'); quotaLabel.append(quota);
        details.append(label, quotaLabel, button('Сохранить режим входа', async () => {
          const data = load(); const intent = { expectedRevision: policy.revision, inviteOnly: toggle.checked, defaultQuota: Number(quota.value) };
          if (!data.policyPending || JSON.stringify(data.policyPending.intent) !== JSON.stringify(intent)) data.policyPending = { requestId: crypto.randomUUID(), intent };
          save(data); await call('platform.admission.update', { requestId: data.policyPending.requestId, ...intent }); delete data.policyPending; save(data); await render();
        }));
      }
      host.append(details);
    } catch (error) {
      if (disposed || seq !== sequence) return;
      host.replaceChildren(el('p', error?.code === 'unsupported_operation' ? 'Приглашения в Соты появятся после обновления сервера.' : 'Приглашения временно недоступны.'), button('Повторить загрузку приглашений', render));
    }
  }
  void render();
  return { destroy() { disposed = true; sequence++; host.replaceChildren(); } };
}
