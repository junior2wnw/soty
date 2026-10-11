import { analyzeTag, recordDescription } from './diagnostics.mjs';

export function diagnosticCard(result, ui, actions) {
  const { el, icon, button } = ui, analysis = analyzeTag(result);
  const card = el('section', 'tag-insight'); card.dataset.status = analysis.iphone.status;
  const head = el('div', 'insight-head'), badge = el('span', 'insight-badge'); badge.append(icon(analysis.iphone.status === 'supported' ? 'check' : 'help'));
  const copy = el('div'); copy.append(el('h3', '', analysis.iphone.title), el('p', 'muted small', analysis.iphone.detail)); head.append(badge, copy);
  const tools = el('div', 'insight-actions');
  tools.append(button('Диагностика', 'chip', 'text-button', actions.details), button('Отчёт', 'download', 'text-button', actions.export));
  card.append(head, tools);
  if (analysis.iphone.repairUrl && analysis.iphone.status !== 'supported') card.append(button('Подготовить обычную ссылку', 'link', 'button button-wide', () => actions.repair(analysis.iphone.repairUrl)));
  return card;
}
export function diagnosticDetails(result, ui) {
  const { el } = ui, analysis = analyzeTag(result), host = el('div', 'diagnostic-details');
  const summary = el('dl', 'diagnostic-fields');
  for (const [label, value] of [['Источник', analysis.origin === 'file' ? 'Отчёт из файла' : 'Чтение на этом телефоне'], ['NDEF-записи', String(analysis.recordCount)], ['Оценка размера NDEF', analysis.estimatedNdefBytes === null ? 'Недоступна' : '≈ ' + analysis.estimatedNdefBytes + ' Б'], ['Модель чипа', 'Браузер не сообщает'], ['Тип технологии', 'Браузер не сообщает'], ['Полная память', 'Браузер не сообщает'], ['Защита от записи', 'Браузер не сообщает']]) summary.append(el('dt', '', label), el('dd', '', value));
  host.append(summary);
  for (const [index, record] of result.records.entries()) {
    const info = recordDescription(record), row = el('section', 'diagnostic-record');
    row.append(el('h3', '', (index + 1) + '. ' + info.label));
    const fields = el('dl', 'diagnostic-fields');
    for (const [label, value] of [['Тип Web NFC', record.recordType], ['Стандартный TNF', info.tnf === null ? 'Не определён' : info.tnf + ' · по типу записи'], ['Данные API', info.byteLength === null ? 'Вложенные записи / неизвестно' : info.byteLength + ' Б'], ...(record.mediaType ? [['MIME', record.mediaType]] : []), ...(record.encoding ? [['Кодировка', record.encoding]] : []), ...(record.lang ? [['Язык', record.lang]] : [])]) fields.append(el('dt', '', label), el('dd', '', value));
    row.append(fields); host.append(row);
  }
  const limits = el('p', 'muted small', 'Это данные NDEF из Web NFC. Они не являются дампом памяти чипа или полным радиопакетом. По номеру метки нельзя надёжно определить модель.');
  host.append(limits);
  return host;
}
