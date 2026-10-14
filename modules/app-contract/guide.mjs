import { lstatSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { validateDescriptor, contractDigest } from './index.mjs';

const escape = value => String(value).replace(/[&<>"']/gu, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
const refs = values => values.length ? `<ul>${values.map(ref => `<li><code>${escape(ref.id)}</code> · версия ${ref.version} · <code>${escape(ref.digest)}</code></li>`).join('')}</ul>` : '<p>Не объявлены.</p>';

/** A local candidate only: no route installation, registry admission or grant. */
export function prepareDescriptorGuide(input, descriptorFile) {
  const descriptor = validateDescriptor(input), digest = contractDigest(descriptor);
  const html = `<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'"><title>${escape(descriptor.app.title)} — руководство</title><style>body{font:16px/1.6 system-ui;max-width:760px;margin:40px auto;padding:0 20px}code{overflow-wrap:anywhere}li{margin:12px 0}</style><main>
<h1>${escape(descriptor.app.title)}</h1><p>Описание возможностей приложения для человека и помощника. Подготовлено из проверенного описания приложения; подключение и доступ проверяются при запуске.</p>
<h2>Действия</h2>${descriptor.capabilities.length ? `<ul>${descriptor.capabilities.map(cap => `<li><strong>${escape(cap.title ?? cap.id)}</strong>${cap.description ? `<p>${escape(cap.description)}</p>` : ''}<p>Идентификатор: <code>${escape(cap.id)}</code>. Версия: ${cap.version}. Изменения: ${cap.effects.map(escape).join(', ') || 'не заявлены'}.</p></li>`).join('')}</ul>` : '<p>Действия помощника не объявлены. Используйте интерфейс приложения.</p>'}
<h2>Навыки и документация</h2><h3>Навыки</h3>${refs(descriptor.skills)}<h3>Документация</h3>${refs(descriptor.docs)}
<p>Это ссылки на точные версии в каталоге Сот. Подключённые MCP/API и обработчики выбираются только из текущего разрешённого каталога. Описание не создаёт новый MCP и не разрешает выполнение действий.</p>
<h2>Обратная связь</h2><p>Обратная связь обязательна. Обращение видно автору обращения и допущенной поддержке. Отправка: ${descriptor.feedback.submitAudience === 'public' ? 'публичная аудитория' : 'участники'}. Используйте обратную связь в подключённом приложении; наличие этого описания не подтверждает готовность очереди.</p>
<p>Помощник может подготовить исправление в пределах своих прав. Выпуск подтверждает автор приложения.</p>
<h2>Доступ</h2><p>Права проверяются приложением при каждом действии. Старый аккаунт связывается только после подтверждения его владельца. Это руководство не даёт доступ к чужим данным.</p>
<p>Версия описания: <code>${digest}</code>.</p></main></html>\n`;
  const directory = dirname(resolve(descriptorFile));
  let path = join(directory, `soty-guide.${digest}.html`), preserved = false;
  try {
    const stat = lstatSync(path);
    if (stat.isFile() && !stat.isSymbolicLink() && stat.size === Buffer.byteLength(html) && readFileSync(path, 'utf8') === html)
      return { path, state: 'candidate', routeInstalled: false, published: false, descriptorDigest: digest };
    preserved = true;
    path = join(directory, `soty-guide.${digest}.${randomUUID()}.html`);
  } catch (error) { if (error.code !== 'ENOENT') throw error; }
  writeFileSync(path, html, { flag: 'wx', mode: 0o600 });
  return { path, state: 'candidate', routeInstalled: false, published: false, descriptorDigest: digest, ...(preserved ? { existingCandidatePreserved: true } : {}) };
}
