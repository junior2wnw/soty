# P4 — подключения приложений в существующей панели доступа

01.10.2026. Авторский UI срез, source frozen после **11/11 PASS**. Backend, AS, consent, маршруты и схема не меняются этим автором. Реальный OAuth flow и геометрию проверяет root отдельно.

## Область

- `src/world/access-panel.ts` — bounded списки, projection validation, подтверждение/проверка отзыва, lifecycle.
- `src/world/access-panel.css` — две группы в существующей нейтральной панели, раскрываемые подробности и focusable busy controls.
- `src/world/access-panel.test.html` — прежний dev-only fixture с дополнительными вымышленными сценариями.
- `src/world/access-panel-oauth.test.mjs` — actual transpiled controller с синтетическими DOM/dialog/API ports.
- Эта квитанция.

`app.ts`, consent, host/domain source и общие dialog helpers не редактируются. Root отдельно добавил optional `managedBy:'oauth'` только для exact owned principal→connection relation. Legacy поле отсутствует. Его backend gate нельзя приписывать UI-тестам автора.

## Один экран и точные субъекты

Во вкладке «Клиенты» показаны «Подключённые приложения» и «Доступ по ключу». У двух API собственные bounded pages по20 записей. Principal с `managedBy:'oauth'` не получает вторую generic карточку/выдачу grants; имена, profile и порядок страниц не используются для сопоставления. Два подключения одного профиля остаются разными connectionId и бюджетами. «Действия» и «История» прежние, без нового OAuth-журнала.

`oauth.connections.list` отдаёт безопасную текущую projection. `active` берётся из ответа; false без revokedAt не переименовывается в guessed «истёк срок» или «отозван владельцем». Дата expiry показана как дата, resource — как текст. Client profile преобразуется в известное имя либо нейтральное «Внешнее приложение». Tokens/provider IDs/Notes contents не запрашиваются и не отображаются.

При недоступности/неподдерживаемом API/ошибке чтения успешный пустой список не симулируется. Вместо старых connection cards показываются marked principals текущей страницы с явным «Подробности недоступны» и safety revoke по точному principal ID. Этот путь вызывает только `access.principals.revoke`: не читает grants, не выдаёт ключи/права. После успешного connlist используется только его карточка. На principal-странице из одних marked записей navigation сохраняется; пользователь может перейти к следующей странице клиентов. Бесконечного обхода/объединения всех страниц нет.

## Отзыв и неизвестный результат

Диалог захватывает kind, ID, display label и cursor показанного субъекта до await. Передаётся `expectedAccountId`. Нельзя заменить цель свежим первым элементом списка. Подтверждённый exact ACK проверяется по literal полям, затем локальный revoked overlay исключает восстановление active карточки ответом чтения, начатого до ACK. Это не выдуманная дата revokedAt и не копия исторической квитанции в текущее состояние.

Неизвестный/неверный ACK не считается успешным. Он сохраняется только в памяти этого mount и ведёт к «Проверить подключение». Проверка делает один bounded list-read с прежним cursor, не повторяет revoke. Exact row с подтверждённым revoke закрывает диалог. Active/missing row не доказывает, что прежнее действие не было выполнено: текст это сообщает; следующий **явный** click может повторить отзыв только того же ID. Отсутствие на одной странице не считается удалением/отзывом. После reload требуется новое серверное чтение; durable pending/crash recovery не обещаны для этого идемпотентного safety action.

Созданные записки и история не отменяются отзывом. Начатое действие могло успеть выполниться; его результат проверяется в общей вкладке «Действия». Никакого автоматического повторения mutation.

## Жизненный цикл и клавиатура

Все новые ответы проходят account/dispose/request-generation fence. `ACTIVE_PROFILE_CHANGED` добавлен к уже существующей очистке панели при отказе account admission. Receipt после dispose не возвращает DOM старого аккаунта.

Новые busy controls остаются focusable с `aria-disabled` и синхронными handler guards. Escape/cancel и header-close не закрывают диалог во время запроса. Новое начальное focus на «Закрыть» — синхронно при открытии; после await фокус не захватывается. При обычном закрытии используется существующий `createDialog` returnTarget: актуальная summary/заголовок этой карточки либо текущая panel, только пока account/mount/view актуальны. Поздний пользовательский focus не отменяется.

Раскрытые connection details сохраняются при render/refresh; refresh и pending pagination не делают активную кнопку native disabled. Отдельных таймеров, rAF focus или новой модальной системы нет.

## Проверки и открытые границы

На Windows Node24.21.0 выполнен `node --test --test-concurrency=1 src/world/access-panel-oauth.test.mjs`: **11 tests, 11 PASS, 0 FAIL, 0 SKIP, 496.491 ms**. Проверяются одинаковые labels и независимые connections, AS-unavailable fallback, lost ACK/readback, missing subject/captured repeat, неправильные receipts, late read после ACK, account/dispose, два независимых pagers, disclosure/focus/busy и старый key revoke. Wrong/coercible ACK не подтверждает отзыв. Unknown profile `constructor` проверен через actual renderer: нейтральный текст вместо унаследованного object property.

Первый11-case run также PASS; `tsc -p tsconfig.json --noEmit` завершился exit0 за2.70s без stdout. После него self-review заменил собственный object lookup имени на два literal comparisons и уточнил success copy, чтобы не обещать сохранность записки, которую пользователь уже удалил. Final11 повторён после этих двух изменений. Отдельного typecheck log-файла нет (пустой pipeline output); final integrated typecheck/build принадлежит root. `git diff --check` по изменённым source PASS.

Freeze — SHA256 фактических worktree bytes, не подмена canonical Git hashes:

| Файл | Bytes | SHA256 |
|---|---:|---|
| `src/world/access-panel.ts` | 65 566 | `1c4c23c34da2838a6517ae5787ab082729daa1fa2e40c56a20b0455fad1f7c30` |
| `src/world/access-panel.css` | 15 223 | `f5a31f059155b8f454b855a58421e40f57ae15a82a420742b10d1f00b7a92788` |
| `src/world/access-panel.test.html` | 11 374 | `abd22006d23064e1a7fc11bf53389df4fc837dcc4f688f77a96fa5ca41c39eeb` |
| `src/world/access-panel-oauth.test.mjs` | 21 877 | `d438c966f1492716f00b52a83889b8de556f44c8c566505d2a4e0d37774f432d` |
| `output/implementation-20260930/p4-oauth-access-panel-author.log` | 1 285 | `6ba5fb912af3999bf53e58753fc7a81a92dd7ffa63b7c7726cea5352ff4b17d9` |

Dev fixture: `/src/world/access-panel.test.html` на существующем dev server root (объявлен `http://127.0.0.1:5481` с watch:null; автор второй server не запускал). Сценарии в select: приложения+ключи; подробности недоступны; принятый отзыв с потерянным ответом; wrong ACK; pending1s; независимые страницы. Данные полностью вымышлены, реальная БД не вызывается. Root может проверить320×760/667×375, Tab/Enter/Escape, сохранение focus на pending и возврат после подтверждения. Синтетический DOM не доказывает native focus/scroll/layout, настоящий signed Connect или внешний OAuth flow. Эти gates, независимый review и actual AS-absent UI composition остаются открытыми на момент авторского freeze.
