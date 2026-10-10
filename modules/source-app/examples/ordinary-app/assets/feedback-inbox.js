import { createSourceFeedbackInbox } from './source-sdk.js';

const labels = { received: 'Принято', in_progress: 'В работе', needs_action: 'Нужно ваше действие',
  ready_to_check: 'Проверьте результат', resolved: 'Решено' };
const messages = { loading: 'Загружаем обращения…', sending: 'Сохраняем…', login_required: 'Доступ изменился. Войдите снова.',
  unavailable: 'Не удалось загрузить. Обновите список.', conflict: 'Обращение изменилось. Обновите его; ваш текст сохранён.',
  unknown: 'Ответ не получен. Проверьте ту же отправку; новый запрос не создаётся.' };
export function mountFeedbackInbox({ host, api, isCurrent }) {
  let mediaUrls = [];
  const clearMedia = () => { mediaUrls.forEach(url => URL.revokeObjectURL(url)); mediaUrls = []; };
  const make = (tag, text = '') => { const node = document.createElement(tag); node.textContent = text; return node; };
  const heading = make('h2', 'Обращения'), info = make('p'), refresh = make('button', 'Обновить список');
  const list = make('ul'), more = make('button', 'Ещё обращения'), detail = make('section');
  info.setAttribute('role', 'status'); refresh.type = more.type = 'button';
  host.replaceChildren(heading, refresh, info, detail, list, more);
  const inbox = createSourceFeedbackInbox({ api, isCurrent, onChange(value) {
    clearMedia();
    const busy = ['loading', 'sending'].includes(value.state);
    info.textContent = messages[value.state] ?? (value.tickets.length ? '' : 'Обращений пока нет.');
    refresh.disabled = busy || value.pending; more.hidden = !value.nextCursor; more.disabled = busy || value.pending;
    list.replaceChildren(); detail.replaceChildren();
    for (const ticket of value.tickets) {
      const row = make('li'), button = make('button', `${labels[ticket.status]} · ${ticket.body.slice(0, 100)}`);
      button.type = 'button'; button.disabled = busy || value.pending;
      button.addEventListener('click', () => void inbox.select(ticket.id)); row.append(button); list.append(row);
    }
    if (!value.ticket) return;
    const ticket = value.ticket;
    detail.append(make('h3', labels[ticket.status]), make('p', ticket.body));
    for (const media of ticket.attachments) {
      const label = make('p', `Вложение: ${media.name}`); detail.append(label);
      if (!media.dataBase64) continue;
      const binary = atob(media.dataBase64), bytes = Uint8Array.from(binary, char => char.charCodeAt(0));
      const url = URL.createObjectURL(new Blob([bytes], { type: media.mimeType })); mediaUrls.push(url);
      const preview = make(media.kind === 'audio' ? 'audio' : 'img'); preview.src = url;
      if (media.kind === 'audio') { preview.controls = true; preview.preload = 'metadata'; preview.setAttribute('aria-label', media.name); }
      else { preview.alt = media.name; preview.loading = 'lazy'; }
      detail.append(preview);
    }
    for (const message of ticket.messages) {
      const row = make('p', `${message.kind === 'support' ? 'Владелец' : 'Автор'}: ${message.body}`); detail.append(row);
    }
    if (ticket.canReply) {
      const label = make('label', ticket.canManage ? 'Ответ владельца' : 'Ваше уточнение'), body = make('textarea');
      body.maxLength = 8000; body.value = value.replyDraft; body.disabled = busy || value.pending;
      body.addEventListener('input', () => inbox.setReply(body.value)); label.append(body);
      const reply = make('button', 'Отправить ответ'); reply.type = 'button'; reply.disabled = busy || value.pending;
      reply.addEventListener('click', () => void inbox.write('reply')); detail.append(label, reply);
    }
    if (ticket.canManage) for (const [status, title] of [['in_progress', 'Взять в работу'], ['needs_action', 'Запросить уточнение'], ['ready_to_check', 'Передать на проверку']]) {
      const button = make('button', title); button.type = 'button'; button.disabled = busy || value.pending || status === ticket.status;
      button.addEventListener('click', () => void inbox.write('status', status)); detail.append(button);
    }
    if (ticket.canAccept) {
      const accept = make('button', 'Подтверждаю: исправлено'); accept.type = 'button'; accept.disabled = busy || value.pending;
      accept.addEventListener('click', () => void inbox.write('accept')); detail.append(accept);
    }
    if (value.pending) {
      const check = make('button', 'Проверить ту же отправку'); check.type = 'button'; check.disabled = busy;
      check.addEventListener('click', () => void inbox.write()); detail.append(check);
    }
    const read = make('button', 'Обновить обращение'); read.type = 'button'; read.disabled = busy || value.pending;
    read.addEventListener('click', () => void inbox.select(ticket.id)); detail.append(read);
  } });
  refresh.addEventListener('click', () => void inbox.open()); more.addEventListener('click', () => void inbox.more());
  return Object.freeze({ ...inbox, dispose() { clearMedia(); inbox.dispose(); } });
}
