# P3-D4 — независимая проверка browser evidence

30.09.2026. Основание: принятый R `e00f3a0`, [D4 plan](p3-engagement-browser-plan.md), [D2 contract](p3-discussion-contract.md). Это отдельное продолжение [D3 приёмки](p3-engagement-independent.md), не перенос прежних результатов на изменённые исходники. Reviewer меняет только dev fixture и эту квитанцию; production, браузер и signed QA исполняют другие участники.

**Узкий verdict:** autosize и исправленный archive/current focus прошли independent source review и actual mounted17/17 на320/667/1280. Reviewer прочитал сохранённые результаты и перечисленные реальные screenshots; CUA исполнял root. Новых consequential blockers в проверенных изменениях не осталось. Этот вывод не закрывает весь D4 или отдельные external gates.

## Геометрия настоящего textarea

Исходный root finding — восстановленный двухстрочный draft при48px/client46/scroll66 — воспроизводимая потеря читаемости. Локальный fit сохраняет тот же textarea и считает его реальный scrollHeight с границами в пределах computed min/max. Его callbacks ограждены mount/account/visibility, RAF отменяется, observer/listeners снимаются. Изменение высоты не вызывает API, не меняет value/draft revision/focus/selection. Source review не обнаружил нового consequential blocker в этом механизме. Общие community/Notes редакторы не меняются.

В `src/world/app-engagement.test.ts` добавлен независимый mounted сценарий. Это настоящий production component и CSS в браузере, без подставного scrollHeight. Synthetic только backend и явно injected storage/locks. Сценарий проверяет восстановление без input event; вставку/удаление; width-only narrow/wide без window resize; предел4000 символов и внутреннюю прокрутку; тот же DOM, selection/focus; history end/middle; hidden/0-width/reveal; inline manual preference; отмену RAF после dispose; отсутствие send и sizing-only storage changes.

Root исполнил `app-engagement.test.html?run=1&case=autosize` на фактических1280×720: **1/1 PASS**. Reviewer прочитал [сохранённые метрики](../../output/implementation-20260930/p3-d4-composer-fixture.json):

| Шаг | Высота / clientHeight / scrollHeight | Значение результата |
| --- | --- | --- |
| Восстановление двух строк |68 /66 /66|Обе строки помещаются без input event; selection2..8 и focus сохранены |
| Вставка / удаление |89 /87 /87 →48 /46 /46|Поле растёт и освобождает пустую высоту |
| Только сужение / расширение host |48 →110 →48|Перенос определяется настоящей шириной; selection4..11 остаётся |
|4000 символов |160 /158 /2145|Высота ограничена; текст прокручивается внутри поля |
| История у конца / в середине |end gap0; midpoint1389|Рост поля не уводит историю от конца и не перематывает середину |
| Явный inline размер |104 /102 /102|Обычный render/input не отменяет выбранную высоту |

Inline height здесь имитирует результат UA resize, а не доказывает физическое перетаскивание мышью. Synthetic input events не подменяют native keyboard/IME. Изменение ширины host не является само по себе испытанием поворота физического телефона.

Root отдельно сообщил полный **16/16 PASS** на1280 до следующей focus correction. Reviewer просмотрел [настоящий320 screenshot](../../output/implementation-20260930/p3-d4-composer-320.png): обе строки видимы, Send и нижняя навигация не перекрыты. Root measurement68/client66/scroll66 соответствует изображению. Финальные повторы после focus/admin copy corrections перечислены ниже; прежний16-case результат их не подменяет.

## Focus finding на переходе в архив

Root реальным действием обнаружил: current→«Ранее»→archive удаляет focused archive button во время очистки feed, после ответа activeElement оказывается BODY. Read-only source review подтвердил причинную цепочку `updateSelection`→invalidate→renderArchives→remove. Сохранение прежних private projections ради кнопки не требуется.

Автор изменил только явный `choose`: если активен archive/current opener, до первого await фокус переходит на постоянный h2 с `tabIndex=-1` и preventScroll. Поздние ответы вообще не фокусируют. Programmatic selection или действие при другом активном элементе не забирает фокус. No-op также проверяет фактический context: одного прежнего requested ID недостаточно после смены аудитории. Source review этого решения завершён без нового blocker.

Новый independent case `?run=1&case=archive-focus` удерживает context response40ms, чтобы браузер действительно успел выполнить layout. Он проверяет обе стороны перехода, connected/visible/readable focus во время ожидания, отсутствие его позднего перемещения, точный current draft, readonly archive и0send. Дополнительно моделируется перенос фокуса на постоянный Refresh во время запроса; ответ не должен его забирать. Programmatic `updateSelection` тоже сохраняет текущий фокус. Root исполнил [isolated case1280](../../output/implementation-20260930/p3-d4-archive-focus-fixture.json) — **1/1 PASS**; reviewer прочитал метрики. Native Enter/Space проверяются отдельно, DOM activation не выдаётся за них.

Reviewer повторил `node node_modules/typescript/bin/tsc -p tsconfig.json --noEmit` после fixture changes — PASS. `git diff --check -- src/world/app-engagement.test.ts` — PASS, отдельно штатное предупреждение LF→CRLF. Heavy suites не запускались параллельно root.

Root нашёл ещё отдельную copy неточность: administrative current view имеет `mode:archive` и `isCurrent:true`, но прежний banner называл его завершённым разговором. Existing admin fixture приведён к этому реальному DTO; он требует0iframe/0composer и запрещает false-closed copy только для ещё current разговора. Root переставил приоритет `ownerAdministrative` перед `mode:archive`; reviewer прочитал узкую строку, права не изменены. Для настоящего исторического `isCurrent:false` тест не запрещает объяснение завершённого разговора.

Финальные17-case browser receipts после обеих corrections прочитаны reviewer:

| Window viewport из fixture | Результат | Restore / insertion / deletion | Long-text cap |
| --- | --- | --- | --- |
|[320×760](../../output/implementation-20260930/p3-d4-final-fixture-320.json)|17/17 PASS|68 →89 →48px|160px; scrollHeight6345|
|[667×375](../../output/implementation-20260930/p3-d4-final-fixture-667.json)|17/17 PASS|62 →83 →44px|88px; scrollHeight2139|
|[1280×720](../../output/implementation-20260930/p3-d4-final-fixture-1280.json)|17/17 PASS|68 →89 →48px|160px|

В320/667 receipts отдельное root-поле `viewport[0]` равно305/652, тогда как сценарий пишет настоящий `window.innerWidth`320/667 и собственные измерения поля. Эти числа не смешаны; проверки выполнялись по реальной ширине textarea, а не по запрошенному размеру инструмента. Во всех трёх focus metrics показывают H2 «Обсуждение» в pending и после ответа; после пользовательского переноса остаётся BUTTON «Обновить обсуждение». Ошибок0 относится к listener стенда, не ко всей browser console. Assertions не ослаблялись ради PASS.

После полных17×3 root настоящим Enter на667 обнаружил отдельное misleading empty: во время context loading показывалось «Доступных архивов нет» с прежним archiveLoaded. Root добавил только `!state?.loading && !switching` к условию пустого статуса; reviewer прочитал эту строку, данные/права/геометрия не меняются. В existing held-focus case добавлена одна assertion перед переносом фокуса: pending не сообщает empty archive. Typecheck/diff-check PASS. Финальный [изолированный browser repeat667×375](../../output/implementation-20260930/p3-d4-final-focus-delta-667.json) — **1/1 PASS** с новой assertion; reviewer прочитал JSON и сверил hashes `eb8a6d6e…`/`a2dc9c66…`. Полные17×3 выше относятся к предыдущей версии; повтор всей матрицы после этой однострочной copy-дельты не заявляется.

Root отдельно сообщил настоящий keyboard путь на667: Enter по archive→H2, Space по current→H2, затем Tab→Refresh. [Native DOM receipt](../../output/implementation-20260930/p3-d4-real-keyboard-667.json) фиксирует конечный BUTTON «Обновить обсуждение», actual667×375, overflow:false, composer44/client42/scroll42. Reviewer просмотрел фактический [screenshot `real-discussion-667`](../../output/implementation-20260930/p3-d4-real-discussion-667.png): золотая рамка focus вокруг Refresh видима, поле и Send помещаются. Это отдельно от synthetic DOM activation fixture; экранный диктор или физическая мобильная клавиатура этим не проверены.

## Точный вход и отзыв прав

Reviewer локально разобрал root signed receipts, выводя только safe projections и результаты сравнений:

- [Initial public](../../output/implementation-20260930/p3-d4-reader-initial-public.json): canonical context —400 `app_unavailable`; выбранный public alias —200/public/canPost=true. Entry содержит только appId/domainId/origin/path.
- [Replay второго device](../../output/implementation-20260930/p3-d4-reader-replay-second-device.json) до отзыва возвращает accepted receipt и body, пока чтение разрешено.
- [После public→restricted](../../output/implementation-20260930/p3-d4-reader-revoked.json): ordinary context/entry —400 `app_unavailable`; exact replay —200/replayed=true, **тот же messageId/conversationId**, message:null. Это подтверждение собственного прошлого эффекта, не новая отправка. Saved entry сохраняет exact domain/origin/path, current:null.
- [После retire адреса](../../output/implementation-20260930/p3-d4-reader-retired-entry.json): ordinary entry/context отказаны; saved current:null, прежние tuple и собственный label сохранены. Новый адрес не подставлен.

Signed requests исполнил root; reviewer не заявляет второй независимый сетевой прогон. Название QA client «phone» не означает физический телефон. Native Connect semantic400 здесь соответствует принятому контракту; оно не переименовано в403 ради ожидаемой HTTP-конвенции.

Reviewer отдельно потребовал не выводить отказ новой отправки из успешного replay. Root добавил второй QA actor с ключами только в памяти: [до UI-отзыва](../../output/implementation-20260930/p3-d4-new-send-before-revoke.json) accepted/replayed=false; [после отзыва](../../output/implementation-20260930/p3-d4-new-send-after-revoke.json) новый requestId того же actor —400 `app_unavailable`, прежний exact replay —200/replayed=true/messageReturned=false. Reviewer сравнил account и receipt локально: actor тот же, receipt ID/conversation прежние. [Read-only Apps SQL count](../../output/implementation-20260930/p3-d4-new-send-count.json) равен1. Поэтому здесь подтверждены оба разных поведения: запрет нового эффекта и доступ к собственной минимальной прошлой квитанции.

[Private archive320](../../output/implementation-20260930/p3-d4-private-archive-320.png) просмотрен: readonly объяснение, старое сообщение и собственный retained draft показаны отдельно, composer отсутствует. Reviewer заметил несоответствие подписи другого screenshot и его public composer. Root подтвердил: HMR reload между retire ACK и снимком показал owner canonical, а не отказанный alias. Файл переименован в [owner new public conversation320](../../output/implementation-20260930/p3-d4-owner-new-public-conversation-320.png). Этот снимок **не доказывает retired UI cleanup**; доступ owner к общему public conversation корректен. Signed alias denial сохраняет силу независимо от прежней неверной подписи.

Последующий [exact retired-entry JSON](../../output/implementation-20260930/p3-d4-retired-entry-ui.json) и [320 screenshot](../../output/implementation-20260930/p3-d4-retired-entry-ui-320.png) независимо прочитаны:320×760, iframe0, visibleComposers0; показано generic unavailable, прежняя public audience/history отсутствует. Это уже отдельное доказательство отказанного выбранного входа.

[Source-off desktop1280×800](../../output/implementation-20260930/p3-d4-source-off-discussion-desktop.png) тоже просмотрен: runtime сообщает разрыв соединения, рядом остаётся самостоятельное public discussion и собственный новый draft. Root исполнил stop/restart источника и явный refresh, затем reload/Back/Forward с сохранением нового draft; это его browser evidence, а не сетевой прогон reviewer. Одна фотография не доказывает все API операции при offline.

## Ожидаемая узкая D4-C матрица

| Сценарий | Обязательная граница |
| --- | --- |
| Reader без grant, public alias D/private canonical C | D с query/SPA hash разрешён, C отказан; save сохраняет D/path без canonical fallback |
| Public→restricted | Новый conversation; прежние server body/labels убираются после отказа; own draft остаётся отдельно readonly; exact accepted replay не создаёт новый пост |
| Restricted S→public P→restricted S2 | Разные IDs; public basis не открывает S; для restricted archive нужны current grant AND исходный predicate |
| Новая группа / уход участника / потеря publisher admin | Новая группа не видит прежний restricted archive; динамический World отзыв закрывает последующее чтение, даже если app сейчас anyone |
| Retire D при работающем C/другом alias | Ordinary D отказан; saved unavailable остаётся собственным и удаляемым, без свежих private fields и origin substitution |
| Owner после revoke/retire | Только явный administrative read/redact с entry:null; no send/launch, ordinary route не повышается до administrative |
| Connector offline | При сохранённых ACL entry/save/discussion доступны, runtime честно offline; source/name-only не меняют conversation |
| Два actor states | Второй device того же account доказывает retry/scope, другой account — privacy; эти проверки не взаимозаменяемы |

Это expected matrix для D4 исполнения, а не дополнительные непроведённые PASS. Серверные D1/D2 proofs остаются в своих receipt; старые backend tests не переписывались.

## Проверенные версии и оставшиеся границы

| Файл/срез | SHA256 |
| --- | --- |
| Discussion autosize до focus correction |e20cbaa44a30c3fe74cd3e415bd38c8d9d5f2da01fe2915d91ecce08e93b6424|
| Discussion после focus correction, source reviewed |d4729d24a38ed907d0faa98b156b401d8190f764305ab0415e63e68272b4b83f|
| Discussion после root admin copy correction;17×3 |bd621a672da1f16c537e2ebe04e9091902a6773c0d1d6c221f3314c214a0f682|
| Discussion после root loading/empty copy delta, source reviewed |eb8a6d6e6fafdae66070852128a82ef6575c8e6bbd38adf99a7fc6dadb70f449|
| `src/world/app-engagement.css` |b0577540c7fbc09aa7ae9022e96497177da7e8925967a3610b75fc79fa4aede4|
| `src/world/app-engagement.test.ts`,17cases, actual admin DTO;17×3 |f11c7b5f37d5664d3a422be39b90ea5dc1b2aaa14de93a2944382127cfef8fdf|
| `src/world/app-engagement.test.ts`, дополнительная pending-empty assertion |a2dc9c66f49bc01a56f6202e4e2f6406182c786a4978ba69e03da7a702339b01|
| `src/world/app-engagement.test.html`, unchanged |66ff0345a595010f3c0473367f595350971294a086a77c1b25c2057241a16c1e|

Эти geometry/focus PASS не закрывают всю доступность или D4. Остальные native keyboard/browser сценарии и общий checkpoint фиксирует root в [browser evidence](p3-engagement-browser-evidence.md). Не заявлены физический телефон, screen reader, installed PWA, новый независимый человек, все IME/browser combinations или общий WCAG AA. Отдельная MutationObserver ошибка уже воспроизведена root на script-free iframe вне product source; её атрибуция ограничена [environment audit](p3-browser-environment-audit.md), ошибка не подавлялась. Production rollout, DNS/TLS, Linux fallback и backup restore — отдельные gates.
