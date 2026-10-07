import { createSourceFeedbackClient, createSourceFeedbackController } from './source-sdk.js';
const byId = id => document.getElementById(id), state = byId('state');
const requestId = () => 'ordinary-' + crypto.randomUUID();
let pendingWrite = null, disposed = false, polling = null, pollUntil = 0, pollDelay = 700;
async function call(path, input) {
  const response = await fetch(path, { method: 'POST', credentials: 'same-origin', redirect: 'error', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ requestId: requestId(), input }) });
  if (!response.ok) throw new Error([401, 403].includes(response.status) ? 'login_required' : 'unknown'); return response.json();
}
async function render() {
  const response = await call('/api/embed/query', { operation: 'items.list' }); byId('items').replaceChildren();
  for (const item of response.data.items) { const row = document.createElement('li'); row.textContent = item.title; byId('items').append(row); }
}
async function refreshConnection() {
  try {
    const context = await fetch('/api/embed/context', { credentials: 'same-origin', redirect: 'error', signal: AbortSignal.timeout(10000) });
    if (!context.ok || (await context.json()).ready !== true || disposed) throw new Error('login_required');
    await render(); if (disposed) return false;
    state.textContent = 'Подключено к выбранному проекту'; byId('content').hidden = false; byId('connect').hidden = true; return true;
  } catch { if (!disposed) { state.textContent = 'Войдите через Соты. Доступ к выбранному проекту проверит приложение.'; byId('connect').hidden = false; byId('content').hidden = true; } return false; }
}
function pollConnection() {
  clearTimeout(polling);
  polling = setTimeout(async () => { if (disposed || Date.now() >= pollUntil) return;
    try { const response = await fetch('/api/embed/session-status', { credentials: 'same-origin', redirect: 'error', signal: AbortSignal.timeout(10000) });
      if ([401, 403].includes(response.status)) { pollUntil = 0; state.textContent = 'Подключение закрыто или доступ изменён. Откройте приложение заново из Сот.'; byId('content').hidden = true; return; }
      if (response.ok && (await response.json()).ready === true && await refreshConnection()) return; } catch {}
    if (!disposed) { pollDelay = Math.min(5000, Math.ceil(pollDelay * 1.6)); pollConnection(); }
  }, pollDelay);
}
byId('connect').addEventListener('click', async () => {
  const popup = window.open('about:blank', '_blank'); // user gesture before the request
  if (!popup) { state.textContent = 'Разрешите новую вкладку и нажмите «Войти через Соты» ещё раз.'; return; }
  byId('connect').disabled = true;
  try {
    const response = await fetch('/api/embed/login', { method: 'POST', credentials: 'same-origin', redirect: 'error',
      headers: { 'content-type': 'application/json' }, body: '{}', signal: AbortSignal.timeout(10000) });
    if (!response.ok) throw new Error('login_required'); const result = await response.json(), url = new URL(result.nativeUrl);
    if (disposed || result.schema !== 'soty.source-embed-auth.v1' || Object.keys(result).sort().join(',') !== 'nativeUrl,schema'
      || url.origin !== document.querySelector('main').dataset.nativeOrigin || url.pathname !== '/soty/connect'
      || [...url.searchParams.keys()].join(',') !== 'intent' || !/^[A-Za-z0-9_-]{43}$/.test(url.searchParams.get('intent'))) throw new Error('login_required');
    popup.location.replace(url.href); pollUntil = Date.now() + 300000; pollDelay = 700; pollConnection(); state.textContent = 'Подтвердите выбранный проект в открытой вкладке. Приложение проверит завершение входа.';
  } catch { popup.close(); if (!disposed) state.textContent = 'Вход пока не подтверждён. Нажмите «Войти через Соты» для явной проверки или нового входа.'; }
  finally { if (!disposed) byId('connect').disabled = false; }
});
await refreshConnection();
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
  byId('feedback-send').disabled = snapshot.state === 'sending' || snapshot.state === 'loading' || snapshot.state === 'login_required'
    || snapshot.context?.ready !== true || snapshot.context?.canSubmit !== true;
  byId('feedback-body').disabled = snapshot.pending;
  byId('feedback-file').disabled = snapshot.pending;
} });
byId('feedback-open').addEventListener('click', async () => { byId('feedback').hidden = false; await feedback.open(); });
byId('feedback-send').addEventListener('click', async () => {
  try{
    const file=byId('feedback-file').files[0],attachments=[];
    if(file){if(file.size>1048576)throw new Error('large');const bytes=new Uint8Array(await file.arrayBuffer());let binary='';for(const byte of bytes)binary+=String.fromCharCode(byte);
      attachments.push({kind:file.type.startsWith('audio/')?'audio':'image',name:file.name,mimeType:file.type,dataBase64:btoa(binary)});}
    if(!feedback.snapshot().pending)feedback.setDraft({ body: byId('feedback-body').value, attachments });
    if(await feedback.send()){byId('feedback-body').value='';byId('feedback-file').value='';}
  }catch{byId('feedback-state').textContent='Выберите PNG, JPEG, WebP, WebM или Ogg размером до 1 МиБ.';}
});
if(document.querySelector('main').dataset.processing==='true'){
  const {mountProcessingUi}=await import('/assets/processing.js');mountProcessingUi({isCurrent:()=>!disposed});
}
addEventListener('pagehide', () => { disposed = true; clearTimeout(polling); feedback.dispose(); }, { once: true });
