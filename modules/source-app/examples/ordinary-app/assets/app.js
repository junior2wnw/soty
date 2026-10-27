import { createSourceFeedbackClient, createSourceFeedbackController } from './source-sdk.js';
import { mountFeedbackInbox } from './feedback-inbox.js';
const byId = id => document.getElementById(id), state = byId('state');
const requestId = () => 'ordinary-' + crypto.randomUUID();
let pendingWrite = null, pendingKey = null, draftKey = null, pendingBlocked = false, writeBusy = false, disposed = false, polling = null, pollUntil = 0, pollDelay = 700;
const profileVersion = Number(document.querySelector('main').dataset.profileVersion);
if (![1, 2, 3].includes(profileVersion)) throw new Error('invalid_profile');
const feedbackApi = createSourceFeedbackClient({ profileVersion });
const inbox = mountFeedbackInbox({ host: byId('feedback-inbox'), api: feedbackApi, isCurrent: () => !disposed && !byId('content').hidden });
async function call(path, input) {
  const response = await fetch(path, { method: 'POST', credentials: 'same-origin', redirect: 'error', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ requestId: requestId(), input }) });
  if (!response.ok) throw new Error([401, 403].includes(response.status) ? 'login_required' : 'unknown'); return response.json();
}
async function render() {
  const response = await call('/api/embed/query', { operation: 'items.list' }); byId('items').replaceChildren();
  for (const item of response.data.items) { const row = document.createElement('li'); row.textContent = item.title; byId('items').append(row); }
}
async function itemScope() {
  const response = await call('/api/embed/query', { operation: 'items.pending-scope' });
  if (typeof response.data?.scope !== 'string' || !/^[a-f0-9]{64}$/u.test(response.data.scope)) throw Error('scope_invalid');
  return response.data.scope;
}
function restorePending(bindingDigest) {
  const key = 'ordinary-items-pending:' + bindingDigest, draft = 'ordinary-items-draft:' + bindingDigest; let raw, savedDraft;
  try { raw = sessionStorage.getItem(key); savedDraft = sessionStorage.getItem(draft); } catch { throw Error('pending_storage_unavailable'); }
  if (savedDraft !== null && (typeof savedDraft !== 'string' || savedDraft.length > 500 || !savedDraft.isWellFormed())) throw Error('pending_storage_invalid');
  let value = null;
  if (raw !== null) {
    try { value = JSON.parse(raw); } catch { throw Error('pending_storage_invalid'); }
    if (!value || Object.keys(value).sort().join(',') !== 'digest,requestId' || typeof value.requestId !== 'string'
      || !/^ordinary-[A-Za-z0-9-]{16,100}$/u.test(value.requestId) || typeof value.digest !== 'string'
      || !/^[a-f0-9]{64}$/u.test(value.digest)) throw Error('pending_storage_invalid');
  }
  pendingKey = key; draftKey = draft; pendingWrite = value; pendingBlocked = false;
  byId('title').value = savedDraft ?? ''; byId('title').readOnly = Boolean(value);
  byId('create').querySelector('button').textContent = value ? 'Проверить результат' : 'Добавить запись';
}
async function refreshConnection() {
  try {
    const context = await fetch('/api/embed/context', { credentials: 'same-origin', redirect: 'error', signal: AbortSignal.timeout(10000) });
    if (!context.ok || (await context.json()).ready !== true || disposed) throw new Error('login_required');
    await render(); if (disposed) return false;
    try { const scope = await itemScope(); if (disposed) return false; restorePending(scope); }
    catch { pendingKey = null; draftKey = null; pendingWrite = null; pendingBlocked = true; byId('title').value = ''; byId('title').readOnly = true; }
    const button = byId('create').querySelector('button'); button.disabled = pendingBlocked;
    state.textContent = pendingBlocked ? 'Не удалось проверить предыдущую запись на устройстве. Добавление недоступно.'
      : pendingWrite ? 'Результат предыдущей записи пока неизвестен. Проверьте сохранение.' : 'Подключено к выбранному проекту';
    byId('content').hidden = false; byId('connect').hidden = true; return true;
  } catch { if (!disposed) { inbox.invalidate(); state.textContent = 'Войдите через Соты. Доступ к выбранному проекту проверит приложение.'; byId('connect').hidden = false; byId('content').hidden = true; } return false; }
}
function pollConnection() {
  clearTimeout(polling);
  polling = setTimeout(async () => { if (disposed || Date.now() >= pollUntil) return;
    try { const response = await fetch('/api/embed/session-status', { credentials: 'same-origin', redirect: 'error', signal: AbortSignal.timeout(10000) });
      if ([401, 403].includes(response.status)) { pollUntil = 0; inbox.invalidate(); state.textContent = 'Подключение закрыто или доступ изменён. Откройте приложение заново из Сот.'; byId('content').hidden = true; return; }
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
byId('title').addEventListener('input', () => {
  if (!draftKey || pendingWrite) return;
  try { sessionStorage.setItem(draftKey, byId('title').value); }
  catch { state.textContent = 'Черновик не удалось сохранить на этой вкладке.'; }
});
byId('create').addEventListener('submit', async event => {
  event.preventDefault(); if (writeBusy || pendingBlocked || !pendingKey) return; writeBusy = true;
  const button = event.currentTarget.querySelector('button'); button.disabled = true;
  try {
    const scope = await itemScope();
    if (disposed || 'ordinary-items-pending:' + scope !== pendingKey) throw Error('profile_changed');
    if (pendingWrite) {
      const response = await fetch('/api/embed/receipt', { method: 'POST', credentials: 'same-origin', redirect: 'error', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ requestId: pendingWrite.requestId, input: { inputDigest: pendingWrite.digest } }) });
      if (!response.ok) throw new Error('unknown'); const value = (await response.json()).data;
      if (value.outcome !== 'committed' || value.requestId !== pendingWrite.requestId || value.inputDigest !== pendingWrite.digest) throw new Error('unknown');
    } else {
      const input = Object.freeze({ operation: 'items.create', title: byId('title').value }), args = Object.freeze({ requestId: requestId(), input });
      const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(input)));
      const reference = { requestId: args.requestId, digest: [...new Uint8Array(bytes)].map(byte => byte.toString(16).padStart(2, '0')).join('') };
      try { sessionStorage.setItem(draftKey, input.title); sessionStorage.setItem(pendingKey, JSON.stringify(reference)); }
      catch { throw Error('pending_storage_unavailable'); }
      pendingWrite = reference;
      const response = await fetch('/api/embed/invoke', { method: 'POST', credentials: 'same-origin', redirect: 'error', headers: { 'content-type': 'application/json' }, body: JSON.stringify(args) });
      if (!response.ok) throw new Error('unknown'); const value = (await response.json()).data;
      if (value.outcome !== 'committed' || value.requestId !== reference.requestId || value.inputDigest !== reference.digest) throw new Error('unknown');
    }
    try { sessionStorage.removeItem(draftKey); sessionStorage.removeItem(pendingKey); } catch { throw Error('pending_storage_unavailable'); }
    pendingWrite = null; byId('title').value = ''; byId('title').readOnly = false; button.textContent = 'Добавить запись';
    try { await render(); state.textContent = 'Запись добавлена'; } catch { state.textContent = 'Запись подтверждена, но список не обновлён.'; }
  } catch (error) {
    if (pendingWrite) { byId('title').readOnly = true; button.textContent = 'Проверить результат'; state.textContent = 'Результат пока неизвестен. Черновик сохранён на этой вкладке.'; }
    else state.textContent = error?.message === 'pending_storage_unavailable' ? 'Не удалось сохранить запрос на устройстве. Запись не отправлена.'
      : error?.message === 'profile_changed' ? 'Профиль изменился. Откройте приложение заново.' : 'Не удалось подготовить запись. Повторите попытку.';
  } finally { writeBusy = false; button.disabled = pendingBlocked; }
});
const feedback = createSourceFeedbackController({ api: feedbackApi, onChange(snapshot) {
  byId('recipient').textContent = snapshot.context?.recipientLabel ?? '';
  byId('feedback-state').textContent = snapshot.state === 'received' ? 'Обращение получено' : snapshot.state === 'unknown'
    ? 'Ответ потерян. Повторная отправка проверит то же обращение.' : snapshot.state === 'login_required' ? 'Нужно войти снова' : snapshot.state === 'sending' ? 'Отправляем…' : '';
  byId('feedback-send').disabled = snapshot.state === 'sending' || snapshot.state === 'loading' || snapshot.state === 'login_required'
    || snapshot.context?.ready !== true || snapshot.context?.canSubmit !== true;
  byId('feedback-body').disabled = snapshot.pending;
  byId('feedback-file').disabled = snapshot.pending;
} });
byId('feedback-open').addEventListener('click', async () => { byId('feedback').hidden = false; await feedback.open(); });
byId('feedback-inbox-open').addEventListener('click', async () => { byId('feedback-inbox').hidden = false; await inbox.open(); });
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
addEventListener('pagehide', () => { disposed = true; clearTimeout(polling); feedback.dispose(); inbox.dispose(); }, { once: true });
