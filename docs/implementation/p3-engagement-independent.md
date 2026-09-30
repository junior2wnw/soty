# P3-D3 — независимая приёмка интерфейса сохранений и обсуждений

Дата: 30 сентября 2026. Рабочая копия: `soty-platform`, исходный принятый D2 — `4ff4359`. Основание — [подплан D](p3-engagement-plan.md), [серверный контракт обсуждений](p3-discussion-contract.md) и независимая D2 приёмка. Этот документ обновляется по мере исполнения проверок; начальная запись не является сдачей D3/D4.

## Текущий статус

**Ограниченный D3 code/client gate пройден.** Независимый signed HTTP вход: **5/5 PASS**. Client acceptance: **21/21 PASS** — 4 deferred launcher и 17 сценариев с настоящими Apps/World SQLite/API и управляемой доставкой ответа/local storage. Mounted UI fixture: **15/15 PASS** на фактических viewport **320×760, 667×375 и 1280×720**. Все три сохранённых DOM evidence прочитаны reviewer. В перечисленных кодовых сценариях воспроизведённых blockers после исправлений не осталось. **Отдельная ошибка MutationObserver в общем browser log остаётся неатрибутированной; console-clean gate не пройден.** Это не полный D4 browser/runtime/physical gate.

Реализация автора, его тесты и production этим reviewer не изменялись. Приёмка пишет только свои новые test/fixture/report файлы. Новые API не подменяют уже принятые D1/D2 проверки данных и аудитории.

## Проверяемый пользовательский контракт

- Сохранение относится к тому exact domain/path, через который человек открыл приложение. Это не разрешение, не копия приложения, не вступление в группу и не подписка. Если в библиотеке уже другой вход того же приложения, его замена требует явного действия.
- Успешный `apps.launch` возвращает safe entry из того же допуска. Поздний metadata lookup не может изменить адрес успешно открытого iframe. Offline lookup нужен только после неуспешного admission; он не создаёт разговор, сохранение, профиль, ticket или session.
- App discussion, community chat и личный помощник имеют разные контексты. Текущая аудитория разговора важнее способа входа: canonical автора не делает публичное обсуждение приватным.
- Draft и pending принадлежат account/app/conversation/exact entry. Новый разговор пуст; старый текст остаётся в прежнем контексте. Retry отправляет именно показанный неизменяемый intent, а не свежий общий slot другого окна.
- Смена панели, доступного архива и Back в том же exact entry сохраняют iframe и его состояние. Другая учётная запись или настоящий другой вход уничтожают старый controller. Source revision не означает автоматическую перезагрузку приложения.
- Потеря права чтения очищает чужие messages/author projections, но не уничтожает собственный локальный draft. Собственная квитанция после lost ACK не даёт права снова показать недоступное тело сообщения.

## Выполненные проверки

### Настоящий signed HTTP вход — 5/5 PASS

Команда: `node --test --test-concurrency=1 server/test/app-entry-http.test.mjs`.

Проверяются production `createHttpApp`, подписанный Connect client, настоящие Apps/World SQLite, аутентифицированный v2 connector и loopback HTTP источники. Только browser identity storage заменён memory adapter. Финальный прогон этой группы: 5 PASS, 0 FAIL, 0 SKIP, примерно 2.84 s на Windows/Node24.13.1.

1. Safe entry содержит ровно appId/domainId/origin/path; соответствует origin и boot query того же launch. Unicode, query, percent-encoding и SPA fragment сохранены.
2. Offline lookup публичного alias работает у аккаунта без World profile. Не появляются saved/discussion heads. Private canonical не подставляется; wrong expectedAccountId запрещён.
3. После настоящего source promotion новый default path изменился, а повтор с ранее разрешённым явным domain/path открывает именно прежний вход, с новым ticket.
4. Retired alias запрещён владельцу и посетителю, хотя другой alias работает. Domain другого приложения не принимается. External и reserved пути отклоняются прежними typed validators.
5. Занятые Apps/World writers дают соответствующий503; после освобождения повтор чтения работает. Профиль и engagement rows не создаются.

Два ранних падения были ошибками независимой fixture: повторный register на том же source port законно возвращал прежний appId; reserved path использует существующий `app_reserved_path`, а не общий `invalid_app_path`. Fixture теперь имеет отдельный loopback source и явно утверждает разные appId; отрицательные проверки не удалены. Production исправлений из этих падений не требовалось.

### Deferred launcher — 4/4 PASS

Команда: `node --test --test-concurrency=1 src/world/app-engagement.acceptance.test.mjs`.

1. Первый запуск и немедленное внешнее открытие разделяют одно первоначальное разрешение entry. Изменение default во время ожидания не меняет вход; каждому открытию выдаётся свой ticket.
2. После ошибки первого admission ожидающий external launch ждёт offline resolver и использует его exact path.
3. Success без entry или с подменёнными app/domain/path/origin/extra metadata отвергается без позднего fallback; пустая вкладка закрывается.
4. Account A→B→A с новым generation не применяет старый launch и не перенаправляет старую пустую вкладку.

Эти проверки используют настоящий launcher и контролируемые Promise/popup adapters. Они не доказывают browser popup policy, iframe DOM lifecycle или правильность generation callback в `app.ts`; это отдельный интеграционный gate.

### Saved/discussion client — 17/17 PASS

Тот же файл и команда. Последний прогон всего файла после helper freeze: **21 PASS, 0 FAIL, 0 SKIP**, 4.01 s. Fixture создаёт настоящие Apps6 и World SQLite, регистрирует приложение через настоящий WebSocket claim, затем выполняет реальные service operations в World authority fence. Контролируются только local storage/locks и момент доставки уже полученного ответа. Эти проверки не называют memory adapter настоящим Web Locks или IndexedDB доказательством.

- Lost save ACK → reload → remove в другом окне → exact retry: историческая `saved:true` квитанция сосуществует с `current.entry:null`; приложение не сохраняется заново.
- Замена account pending A на B не позволяет старому retry отправить B; поздний ACK A не очищает B. Storage failure до durable admission даёт ноль dispatch. Другой вход требует явного replacement.
- Поздний matching ACK может завершить только исходный account record; после смены аккаунта результат имеет `stale`, без принятого ответа для чужого экрана.
- Old-audience pending после reload/rotation/revoke повторяется с прежним requestId/conversation/exact entry. Серверная квитанция не возвращает недоступный body; новый разговор и его draft отдельны.
- ACK не стирает более новый typed draft; неверная квитанция оставляет pending. Два draft writer сохраняют оба текста и требуют явного разрешения конфликта. Ошибка записи и смена аудитории, пока ждём lock, дают ноль отправок.
- Local recovery ограничен account/app/conversation/domain/path/origin и доступен без нового server history grant. Сброс пагинации saved не смешивает ревизии и не стирает pending.
- 35 загруженных сообщений, ещё32 новых и удаление самого старого вне tail: tombstone приходит через changes. Настоящий перезапуск сервиса инвалидирует cursor; local draft/pending остаются точными.
- Late old-conversation response не заменяет новый разговор. Отказ `archives` после revoke очищает messages; задержанный `history` другой категории не возвращает их. Native account refusal из `poll` также инвалидирует поздний `archives`.
- Send ACK не выдумывает порядок перед ранее не прочитанным сообщением: authoritative changes восстанавливают порядок. Поздний create ACK не воскрешает удалённый body.
- У discussion pending также проверена замена другим окном: сервер принимает A, ACK удерживается; после явного abandon A появляется B. Late ACK A не очищает B, прежний retry A даёт ноль API; сервер действительно содержит только A.

Найден и исправлен автором реальный дефект: native `authentication_required` отсутствовал в списках authoritative denial у saved library и discussion feed. Настоящий сервис при несовпадении expected account отказывал, но клиент мог оставить прежнюю projection до внешнего account observer. Независимые проверки сначала RED, после точечного добавления native кода GREEN. Network failure по-прежнему помечает ранее загруженное как stale; это другой класс ошибки. Дополнительный whole-feed generation fence автора независимо проверен двумя противоположными категориями запросов.

Проверенные frozen helper SHA-256:

- `app-saved-state.mjs`: `ce34485aa287bbe74680e5e5c44546e5dba29aae300e197953819b04e991c077`.
- `app-discussion-state.mjs`: `cbae17634d662bcd2b7d95cefc92a056e9107630a8e7b23eb92f370f0dbb5113`.

### Mounted development fixture — 15/15 на трёх размерах

`src/world/app-engagement.test.html?run=1` импортирует настоящий `mountAppStage`, saved/library/discussion components и их helpers. Typecheck PASS. 15 сценариев проверяют состояние iframe при panel/archive/Back, два null→exact entry перехода, success без корректного DTO, focus/selection, смену аудитории, storage recovery, отказ перехода с откатом URL, unknown ACK remount, показанный retry после замены другим окном, отказ чтения, cursor reset, account ABA, недоступное сохранение, plaintext/имена/ширину.

Runtime iframe получает `srcdoc` до insertion и остаётся настоящим browsing context. Только в пределах отдельного fixture-сценария перехватывается создание iframe; `finally` восстанавливает `document.createElement`. Ручной стенд сохраняет перехват до его закрытия. Fake external origin не запрашивается. Это доказывает DOM/context lifetime при браузерном исполнении, но не runtime HTTP/WS, session/cookie или connector.

Первый root browser запуск остановился из-за ошибки самого стенда: `intent.route` был передан в `history.replaceState` без `#`. Исправлена единая fixture `applyRoute`, production и assertions не изменены. Каждый сценарий теперь показывает конкретный await-шаг и выполняется изолированно; ошибка не скрывает остальные результаты.

Root browser воспроизвёл два продуктовых перехода:

1. После archive→close→Back stage сравнивал `undefined` presentation fields и пропускал `updateSelection`; сохранялась прежняя archive selection. Root изменил условие повторного открытия. В повторе iframe/context и возврат current draft прошли.
2. После server-side audience rotation `updateSelection({})` доверял старому `context.isCurrent` и не перечитывал текущий разговор. Fixture увидела прежний текст на месте нового current composer. Это не доказательство отправки в новую аудиторию: сеть не вызвана, но интерфейс неверно завершал явный выбор current. Автор разделил запрошенную selection и ранее resolved conversation; повтор этого сценария PASS, assertion не ослаблен.
3. Свежий прогон с удержанием context response на40ms подтвердил потерю focus/selection: `feed.load` очищает context, UI скрывает composer, браузер успевает перевести фокус. С мгновенной synthetic Promise такой layout gap можно пропустить. Автор сохранил собственный composer видимым/readOnly на время проверки, без прежней server authority и без позднего focus вызова. Новый повтор PASS: во время удержания отдельно утверждаются visible+readOnly, disabled send, очищенные server messages и прежний own text; после ответа — тот же textarea, focus и selection.
4. Source review показал, что отказ `updateSelection` из-за volatile draft оставлял новый archive URL при прежнем разговоре. Root восстанавливает последнюю показанную presentation через replace и оставляет панель для копирования. Добавленный actual stage сценарий временно вызывает quota failure только для точного случайного fixture-account key; переход не читает archive, URL возвращён, draft остаётся в памяти. Prototype восстанавливается в `finally`, затем draft успешно flush. PASS.

Первый содержательный root browser прогон был **13/14 PASS** с ошибкой аудитории. Следующий свежий прогон после её исправления и с40ms response gap — снова **13/14 PASS**, теперь ошибка только focus/selection. PASS предыдущего мгновенного refresh не применяется к этому сценарию. Финальные расширенные повторы дали **15/15 PASS** на [320×760](../../output/implementation-20260930/p3-d3-fixture-320.txt), [667×375](../../output/implementation-20260930/p3-d3-fixture-667.txt) и [1280×720](../../output/implementation-20260930/p3-d3-fixture-1280.txt). Включены все проверки admin/late-entry, iframe, quota, ACK/retry, authority cleanup, cursor reset, account ABA и plaintext. Их «ошибок0» означает только отсутствие событий, пойманных listener самого стенда после начала его модуля, а не чистую общую консоль.

### Отдельная неустановленная browser ошибка

Root обнаружил на reload в `dev.logs` сообщение `Uncaught TypeError: MutationObserver.observe parameter 1 is not of type Node`, без URL/stack. Нельзя ни приписать его продукту, ни списать на extension/tool без источника. Для диагностики добавлен только passive inline collector до module imports; он не изменяет MutationObserver, не подавляет ошибки, сохраняет ограниченные source/stack без query/hash и не подставляет текущий URL вместо отсутствующего filename.

На свежем1280×720 после collector: вновь **15/15 PASS**, ранний page-realm collector0; [DOM результат](../../output/implementation-20260930/p3-d3-fixture-early-errors.txt) прочитан reviewer. При этом root снова увидел отдельную MutationObserver error в общем log в10:21:56Z. Read-only in-memory bundling фактического fixture entry показал22 исходных inputs, отсутствие `avatars.ts` и строки `MutationObserver` в собранном коде. Этот граф не включает browser/tool isolated worlds и внешние injection scripts. Hypothesis «до HTML или другой realm» остаётся гипотезой. Дальнейшая атрибуция и общий console-clean gate остаются открытыми; blind injection и изменение product source не выполнялись.

### Проверенные версии и регрессия

| Файл | SHA-256 |
| --- | --- |
| `src/world/app-engagement.acceptance.test.mjs` | `7c830e7e34e20c5b1527e9d736f7c7d7827559cf4efd9f7d2176cc2e4e687f1e` |
| `server/test/app-entry-http.test.mjs` | `372d63bd3df1a6dcd980b86f58b0eb3dc149970ddafe673a11909d00974fd91d` |
| `src/world/app-engagement.test.ts` | `26c0c3677698c03d384f8f8195e7e558e089205ab8b5fe8b5ce2723ae060fe32` |
| `src/world/app-engagement.test.html` (с ранним collector) | `66ff0345a595010f3c0473367f595350971294a086a77c1b25c2057241a16c1e` |
| `src/world/app-stage.ts` | `7c067f2abc8fb888de00c2214e739de3d989be2969b60aae900457a5acfdbd3d` |
| `src/world/app-discussion.ts` | `bc0b6fca3637e6c94431f9d74c9e25883b2140c5af657a846470096a570f4468` |
| `src/world/app-saved.ts` | `34d0b45bc0edb80f7aa4eb8ddf966fede0119d5c2fef9d213dc021e30e79265a` |
| `src/world/app-library.ts` | `eecf77b384f98829a985eaaa1080eab86a1bd555fee1eedd625e518b71d6de1c` |

Root сообщил final targeted66/66 (controller/lifecycle/launch/entry/context/independentHTTP), полный world751:747 PASS/4 прежних skip, Connect70/70 и typecheck/build PASS. Это root regression receipt, а не повтор всех этих команд независимым reviewer. Последние узкие stage/UI исправления отдельно прошли mounted fixture и targeted root проверки; helper source после21/21 не менялся.

## Оставшийся ограниченный acceptance-подплан

| Граница | Решающий сценарий | Обязательный результат | Статус |
| --- | --- | --- | --- |
| Saved receipt/current | Lost ACK, remove в другом окне, retry старого save | Историческая квитанция не становится текущим сохранением; нет resurrection/auto-rebase | Client PASS |
| Pending identity | Показан A, shared slot заменён B до клика или пока ждём lock | Ноль dispatch B от действия для A; clear только exact matching record | Client и mounted fixture PASS |
| Durable failure | Quota/storage failure перед admission, поздний ACK после нового draft | Сеть не вызывается без сохранённого intent; новый draft не стирается | Client и mounted fixture PASS |
| Аудитория | Private draft/pending, затем новый public conversation | Новый draft пуст, old intent не отправляется в новый разговор | Client и mounted fixture PASS |
| Cursor reset | Истёкший cursor/restart, удаление за пределами tail | Snapshot обновляется; draft/pending остаются; удалённый текст не возвращается | Client + actual service PASS |
| Потеря права | Delayed history/ACK после revoke, retired alias и account ABA | Нет чужого текста/профиля, нет origin fallback; своё сохранение можно убрать | Client/HTTP и mounted fixture PASS |
| Frame lifecycle | App→discussion→archive→Back в том же exact entry | Тот же iframe object и отсутствие лишних launch requests | Mounted srcdoc fixture PASS |
| UI состояния | Loading/empty/error/current/archive/admin | Нет выдуманных hidden totals, false «отправлено», writable archive или полной profile projection | Перечисленные mounted сценарии PASS |
| Доступность | 320/667/1280, длинный plaintext, keyboard/focus, умеренный live log | Именованные controls, нет горизонтального overflow в fixture, обновление не крадёт focus/selection | Перечисленные DOM assertions PASS; human/screen reader и screenshot visual не проверены |

Для mounted dev fixture API ответы будут явно synthetic; действия DOM не выдаются за реальные pointer/keyboard/browser проверки. Настоящий browser gate root должен быть подписан отдельно, включая точные viewport и ограничения. Нового пользователя, физический телефон и screen reader этот этап сам по себе не имитирует.

## Границы вывода

D2 доказательства аудитории/receipts/SQLite остаются в своём отчёте; Ban, read watermark, подписки, уведомления, публичный пилот и production не входят в D3. D4 настоящий runtime idle/network, две реальные вкладки/устройства, installed PWA и human/screen-reader приёмка не подменяются fixture. Remount с memory adapter не объявляется настоящим page reload/IndexedDB proof. Root сообщил о недоступных CUA mouse/screenshot инструментах: изображений и визуального screenshot PASS нет. Неатрибутированная console error остаётся отдельным наблюдением, даже при15 зелёных assertions. Нет заявления универсальной доступности или защиты от всех возможных гонок; каждый PASS относится к перечисленному сценарию и проверенной версии исходников.
