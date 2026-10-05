const validDate = value => {
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date : null;
};

/** Local calendar days keep separators correct across DST and year boundaries. */
export function chatDayKey(value) {
  const date = validDate(value);
  return date ? `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}` : '';
}

export function chatDayLabel(value, now = Date.now()) {
  const date = validDate(value), today = validDate(now);
  if (!date || !today) return '';
  if (chatDayKey(date) === chatDayKey(today)) return 'Сегодня';
  const yesterday = new Date(today); yesterday.setDate(yesterday.getDate() - 1);
  if (chatDayKey(date) === chatDayKey(yesterday)) return 'Вчера';
  return date.toLocaleDateString('ru-RU', { day: 'numeric', month: 'long', ...(date.getFullYear() === today.getFullYear() ? {} : { year: 'numeric' }) });
}

export function chatListTime(value, now = Date.now()) {
  const date = validDate(value);
  if (!date) return '';
  return chatDayKey(date) === chatDayKey(now)
    ? date.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })
    : date.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', ...(date.getFullYear() === new Date(now).getFullYear() ? {} : { year: '2-digit' }) });
}

/** Only adjacent, live messages from the same author form a visual cluster. */
export function canGroupMessages(previous, current) {
  return !!previous && !previous.removed && !current.removed && !current.replyTo
    && previous.author.profileId === current.author.profileId
    && chatDayKey(previous.createdAt) !== '' && chatDayKey(previous.createdAt) === chatDayKey(current.createdAt)
    && current.createdAt >= previous.createdAt && current.createdAt - previous.createdAt < 5 * 60_000;
}

export function shouldSendOnEnter(event) {
  return event.key === 'Enter' && !event.shiftKey && !event.altKey && !event.ctrlKey && !event.metaKey
    && !event.isComposing && event.keyCode !== 229;
}

export function chatPreview(message) {
  if (!message) return '';
  return message.removed ? 'Сообщение удалено' : message.text.replace(/\s+/gu, ' ').trim();
}
