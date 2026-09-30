# P3-D4-A — читаемый composer обсуждения

30.09.2026. Узкий авторский freeze после R `e00f3a0`; основание — [D4 browser plan](p3-engagement-browser-plan.md). Изменены только `src/world/app-discussion.ts` и `src/world/app-engagement.css`. Модель draft/pending, backend, sample, stage и independent fixture не менялись. Коммита/деплоя не было.

## Проблема и выбор

Root воспроизвёл на настоящих 320×760: поле шириной около 200.125 px имеет высоту 48 px, clientHeight 46, scrollHeight 66. Восстановленный текст «Черновик D3 — ещё не отправлять» переносится на вторую строку, но поле остаётся однострочным. `render()` восстанавливал value без пересчёта высоты.

В проекте найдены локальные `resizeComposer` в `main.ts`, `fitComposer` в `app.ts` и `fitTextarea` в `notes.ts`; общего экспортируемого helper нет. Их лимиты и редакторский контекст различаются. Для D4-A добавлена локальная функция рядом с настоящим discussion textarea. Другие редакторы не переписывались и общий framework не вводился.

Используется реальная раскладка textarea через `scrollHeight`, а не длина строки/число символов. Поле сохраняет DOM identity. Автоподбор не пишет value, draft revision, selection, focus или storage и не вызывает API.

## Браузерные основания

Проверены первичные источники на 30.09.2026:

- [CSSOM View, `scrollHeight`](https://www.w3.org/TR/cssom-view-1/#dom-element-scrollheight), Working Draft 16.09.2025: размер scrolling area; без layout box возвращается 0. Поэтому hidden/нулевая ширина не используются как измерение. Для нашего border-box к scrollHeight добавляются верхняя и нижняя границы из computed CSS.
- [CSS UI 4, user resize](https://www.w3.org/TR/css-ui-4/#resize): нативное изменение размера записывает px height в inline style, min/max продолжают ограничивать размер. Это позволяет отличить manual preference от последнего значения, выставленного autosize, без pointer-coordinate эвристики.
- [Resize Observer, processing model](https://www.w3.org/TR/resize-observer-1/#html-event-loop): изменение размеров в callback может вызвать повторную доставку/loop error. Observer реагирует на изменение фактической ширины либо чужого inline height; собственный height не запускает бесконечный пересчёт. Запись выполняется через один queued animation frame.
- [`field-sizing` в CSS Form Control Styling Level 1](https://drafts.csswg.org/css-forms-1/#field-sizing), Editor’s Draft 17.09.2026: content sizing при фиксированной ширине зависит от строк текста; свойство перенесено из CSS UI 4. [Документация Chrome](https://developer.chrome.com/docs/css-ui/css-field-sizing) отдельно показывает необходимость ограничений размера. Наличие свойства в текущем draft не доказывает его поддержку у всех установленных клиентов. В этой правке `field-sizing` не является зависимостью: применяется JS-механизм на existing textarea.

## Поведение и границы

- Restore/remount, реальный ввод, удаление и обычный render планируют измерение после обновления DOM. Один animation frame объединяет несколько уведомлений. Если текст, ширина и влияющие CSS metrics не изменились, запись размеров пропускается.
- Minimum, maximum и border берутся из computed CSS. Сохранены текущие пределы: **48–160 px**, для landscape `min-width:480px / max-height:440px` — **44–88 px**. После верхнего предела текст прокручивается внутри textarea; controls не вытесняются неограниченным ростом поля.
- Для shrink измерение выполняется с временными `height:auto`/`overflow-y:hidden`; затем устанавливается ограниченная высота и `overflow-y:auto`. Значение/выделение не заменяются. На input не восстанавливается старый scrollTop поверх нового caret; ввод в конце текста оставляет конец видимым.
- При non-input изменении сохраняются собственные scrollTop/scrollLeft textarea с естественным браузерным clamp. Позиция истории снимается непосредственно перед actual fit: если она была у конца (погрешность 2 px), остаётся у конца; при чтении середины сохраняется её scrollTop. Queued fit не использует старую позицию, снятую до ручной прокрутки.
- ResizeObserver следит за настоящей шириной поля, поэтому изменение ширины host без window resize также учитывается. Window/visual viewport resize позволяют применить новые CSS limits. Загрузка шрифта отдельно инвалидирует метрики, даже если имя font-family не меняется.
- `resize:vertical` сохранён. Отличающийся от нашего inline height — явный manual override в пределах CSS. Он не перезаписывается автосжатием при каждом вводе; в новой ориентации ограничивается новым max, а при возвращении широкий предел позволяет восстановить выбранную высоту. Предпочтение живёт только до remount; без него новый composer снова использует autosize.
- Hidden/0-width поле не получает ложную нулевую высоту; reveal/render/observer возобновляют измерение. Каждый callback проверяет текущий mount/account/visibility через существующий foreground guard. Hide/dispose отменяет queued RAF; dispose отключает observer и resize/font listeners. Позднее fonts-ready уведомление ничего не делает после dispose.

## Проверка

### Дополнительный D4 finding — фокус при выборе разговора

После первого freeze root воспроизвёл в настоящем браузере на 320 px: «Ранее» → «Разговор 1 · Личное» успешно меняет URL/историю, но клавиатурный фокус сразу уходит в `document.body`. Причина подтверждена чтением source: `updateSelection()` очищает feed, а `renderArchives()` удаляет старые archive buttons; обратная кнопка «Текущее обсуждение» скрывается после успешного перехода.

Исправление ограничено `app-discussion.ts`: постоянный h2 получает `tabIndex=-1`. Только явный выбор из активной archive/current кнопки синхронно переводит фокус на этот заголовок с `preventScroll`, до первого await/flush. Поздний ответ вообще не вызывает focus, поэтому последующий Tab/клик пользователя не перехватывается. Программный `updateSelection()` и выбор при другом активном элементе фокус не меняют. Textarea/runtime не пересоздаются; старые ACL/projections не сохраняются ради удержания кнопок.

Точный повтор уже выбранного контекста — no-op без запроса, URL callback и изменения фокуса. Проверяется фактический context, а не только `requestedConversationId`: после смены аудитории unpinned запрос остаётся без ID, но старое обсуждение уже архивное, и переход «Текущее» должен продолжать работать. При отказе flush заголовок остаётся стабильным, существующий status показывает ошибку, URL callback не вызывается. Асинхронного возврата фокуса к исчезнувшему opener нет.

Critic подготовил отдельный actual mounted сценарий `?run=1&case=archive-focus`: удержанный context response при current→archive и archive→current, connected/visible/named focus во время загрузки, отсутствие позднего перехвата после перехода пользователя на Refresh, программный выбор без focus, сохранение собственного draft и ноль send. Root выполнил его, затем включил в полный mounted gate: **17/17 PASS** отдельно на 320×760, 667×375 и 1280×720 на версии `bd621a672da1f16c537e2ebe04e9091902a6773c0d1d6c221f3314c214a0f682`. В receipt focus остаётся на H2 во время/после перехода; после явного переноса пользователем на Refresh поздний ответ оставляет его на Refresh. Это реальная DOM fixture, не отдельное доказательство всех native Enter/Space/IME сочетаний.

Перед этим срезом root внёс copy-правку: при `ownerAdministrative=true` соответствующий текст имеет приоритет над `mode=archive`. Владелец текущего разговора больше не видит ложное «разговор завершён» лишь из-за административного read-only режима. Автор этого receipt данную строку не редактировал. Root наблюдал корректный banner, read-only и отсутствие iframe; critic проверил существующий административный fixture.

### Авторские проверки

Автор выполнил:

- `node node_modules/typescript/bin/tsc -p tsconfig.json --noEmit` — **PASS**.
- `git diff --check -- src/world/app-discussion.ts src/world/app-engagement.css` — **PASS** (Git отдельно сообщает штатную LF→CRLF нормализацию).
- Read-through: autosize не меняет domain/draft API, не создаёт нового textarea, не вызывает отправку/focus/selection setters; manual/auto heights разделены; callbacks отменяются или ограждены lifecycle. Единственный новый focus вызов относится к явному выбору разговора до await, а не к autosize/render/ответу API.

Mirror unit tests с подставным scrollHeight не добавлялись: они не доказывают перенос строки настоящим браузером. Независимый reviewer владеет actual mounted regression в `app-engagement.test.ts/.html`: restored multiline без input event, ввод/удаление, host-only narrowing/widening, 4000 chars/cap/internal scroll, identity/focus/selection, history bottom/middle, hidden/reveal, manual clamp и dispose. Root повторил полный **17/17 PASS** на каждом из трёх viewport; автор прочитал сохранённый 320 receipt и сверил final source SHA. В restored320 textarea имеет height68/client66/scroll66 вместо обрезанной второй строки; insertion89, deletion48, long text capped160/internal scroll. Нативный ручной drag не приписывается fixture, которая задаёт manual inline height.

Доказательства root: `output/implementation-20260930/p3-d4-final-fixture-320.json`, `p3-d4-final-fixture-667.json`, `p3-d4-final-fixture-1280.json`. Автор не запускал параллельную браузерную сессию и не выдаёт чужой прогон за свой.

### Финальная однострочная дельта root

После указанного 17/17×3 root добавил только `!state?.loading && !switching` к условию «Доступных архивов нет». Пока новый context/архив загружается, временно пустая проекция больше не выдаётся за окончательное отсутствие архивов. Данные, autosize и управление фокусом не менялись. Critic дополнил held-response assertion; root повторил изолированный сценарий на **667×375: 1/1 PASS**. Сохранён `output/implementation-20260930/p3-d4-final-focus-delta-667.json`; автор прочитал этот receipt и сверил final SHA `eb8a6d6e6fafdae66070852128a82ef6575c8e6bbd38adf99a7fc6dadb70f449` с файлом. Прежние 17/17×3 остаются доказательством исторического среза `bd621a…`, а не заявлением о новом полном повторе.

По сообщению root итоговый общий gate: **World 824 tests / 819 PASS / 0 FAIL / 5 явных skips, 86.88 s; Connect 70/70; typecheck/build PASS**. Это прогоны root, автор отчёта их не дублировал. В этой финализации изменён только настоящий документ.

Не утверждается поддержка всех browser/OS keyboard/IME комбинаций или испытание физического телефона. Эта правка ограничена геометрией existing composer; данные и права обсуждения остаются в принятой D3 модели. Обнаруженные реальные browser findings должны быть закрыты до общего D4 checkpoint.

## Frozen hashes

| Файл | SHA256 |
|---|---|
| `src/world/app-discussion.ts` | `eb8a6d6e6fafdae66070852128a82ef6575c8e6bbd38adf99a7fc6dadb70f449` |
| `src/world/app-engagement.css` | `b0577540c7fbc09aa7ae9022e96497177da7e8925967a3610b75fc79fa4aede4` |

После этого freeze production-файлы не меняются без конкретного finding. Browser/independent результат атрибутируется своему исполнителю отдельно от авторского typecheck.
