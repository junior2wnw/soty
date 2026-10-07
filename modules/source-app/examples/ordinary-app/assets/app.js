import { createSourceFeedbackClient, createSourceFeedbackController } from './source-sdk.js';
const byId = id => document.getElementById(id), state = byId('state');
const requestId = () => 'ordinary-' + crypto.randomUUID();
let pendingWrite = null;
async function call(path, input) {
  const response = await fetch(path, { method: 'POST', credentials: 'same-origin', redirect: 'error', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ requestId: requestId(), input }) });
  if (!response.ok) throw new Error([401, 403].includes(response.status) ? 'login_required' : 'unknown'); return response.json();
}
async function render() {
  const response = await call('/api/embed/query', { operation: 'items.list' }); byId('items').replaceChildren();
  for (const item of response.data.items) { const row = document.createElement('li'); row.textContent = item.title; byId('items').append(row); }
}
try {
  const context = await fetch('/api/embed/context', { credentials: 'same-origin', redirect: 'error' });
  if (!context.ok || (await context.json()).ready !== true) throw new Error('login_required');
  await render(); state.textContent = 'Подключено к выбранному проекту'; byId('content').hidden = false;
} catch { state.textContent = 'Войдите через кнопку Сот. Доступ к данным проверит приложение.'; }
byId('create').addEventListener('submit', async event => {
  event.preventDefault(); const button = event.currentTarget.querySelector('button'); button.disabled = true;
  try {
    if (pendingWrite) {
      const response = await fetch('/api/embed/receipt', { method: 'POST', credentials: 'same-origin', redirect: 'error', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ requestId: pendingWrite.args.requestId, input: { inputDigest: pendingWrite.digest } }) });
      if (!response.ok) throw new Error('unknown'); const value = (await response.json()).data;
      if (value.outcome !== 'committed') throw new Error('unknown');
    } else {
      const input = Object.freeze({ operation: 'items.create', title: byId('title').value }), args = Object.freeze({ requestId: requestId(), input });
      const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(input)));
      pendingWrite = { args, digest: [...new Uint8Array(bytes)].map(byte => byte.toString(16).padStart(2, '0')).join('') };
      const response = await fetch('/api/embed/invoke', { method: 'POST', credentials: 'same-origin', redirect: 'error', headers: { 'content-type': 'application/json' }, body: JSON.stringify(args) });
      if (!response.ok) throw new Error('unknown');
    }
    pendingWrite = null; byId('title').value = ''; byId('title').disabled = false; button.textContent = 'Добавить запись'; await render(); state.textContent = 'Запись добавлена';
  } catch { byId('title').disabled = true; button.textContent = 'Проверить результат'; state.textContent = 'Ответ потерян. Проверим квитанцию без повторной записи.'; }
  finally { button.disabled = false; }
});
const feedback = createSourceFeedbackController({ api: createSourceFeedbackClient(), onChange(snapshot) {
  byId('recipient').textContent = snapshot.context?.recipientLabel ?? '';
  byId('feedback-state').textContent = snapshot.state === 'received' ? 'Обращение получено' : snapshot.state === 'unknown'
    ? 'Ответ потерян. Повторная отправка проверит то же обращение.' : snapshot.state === 'login_required' ? 'Нужно войти снова' : snapshot.state === 'sending' ? 'Отправляем…' : '';
  byId('feedback-send').disabled = snapshot.state === 'sending'; byId('feedback-body').disabled = snapshot.pending;
} });
byId('feedback-open').addEventListener('click', async () => { byId('feedback').hidden = false; await feedback.open(); });
byId('feedback-send').addEventListener('click', async () => { feedback.setDraft({ body: byId('feedback-body').value, attachments: [] }); if (await feedback.send()) byId('feedback-body').value = ''; });
addEventListener('pagehide', () => feedback.dispose(), { once: true });
