# P2-I — закрытие окна и возврат к логическому месту

30.09.2026. Реализация принятого [узкого preflight](p2-dialog-focus-preflight.md). Исходный browser RED принадлежит root: на 320 px после «Связи и доступ» → «Название и доступ» → сохранение/закрытие прежний opener удалялся поздним обновлением главной, фокус оставался на BODY.

## Выполненная дельта

`createDialog` теперь имеет один idempotent finalizer. Управляемый `close()` сначала вызывает нативное закрытие, затем синхронно удаляет окно, вызывает cleanup/render и только после этого разрешает цель возврата. Поздний нативный `close` не вызывает эти действия второй раз. Header X использует тот же путь. Новых таймеров, rAF, очереди microtask или повторных попыток focus нет.

Дополнительный `DialogReturnTarget` содержит `isCurrent()` и `resolve()`. `WorldDialog.close({restoreFocus:false})` подавляет прикладной возврат при передаче управления. Это не отменяет краткий нативный возврат, предусмотренный самим браузером внутри `HTMLDialogElement.close()`.

Нативный Escape/requestClose сохраняет действующий cancel veto. Для нативного queued fallback фиксируются последующие pointer/key/click и смена focus. Если другой шаг уже забрал управление, `onClose({interrupted:true})` всё равно освобождает ресурсы, но settings/group callback не обновляет старый экран. После callback новый активный элемент или другое окно также запрещают возврат. Родительский dialog при вложенном окне допускается только для цели внутри него.

В `WorldApplication` возврат ограничен account ID **и account generation**, screenSequence, фактическим hash, activeRoute и живым экземпляром. A→B→A не восстанавливает старое право возврата. `inspectApplication` передаёт один ticket в settings; для карточки порядок: surviving opener → visible same-app inspect action → same-app launch → текущий main. Имена не используются как ключ. В карточку добавлены `data-app-action="inspect"` и `data-app-id`. Проверяются connected, focusability, CSS visibility, hidden/inert, disabled и закрытые details. Общие World dialogs при исчезнувшем opener получают main только в прежнем допустимом экране.

Self-review выявил отдельную ветку: `renderGroup()` сам вызывает `cleanScreen()` и меняет screenSequence. По согласованию root только после синхронного cleanup/render **той же** группы создаётся новый target к живому main нового экрана. До render обязательны актуальный исходный ticket/account/screen/route, отсутствие handoff/interruption; после — прежние account generation, группа и route. Старый ticket остаётся неактуальным. Карточки группы приходят асинхронно: фокус их не ждёт и повторно к ним не переносится. Эта граница применена к settings и существующему management-dialog cleanup, без изменения редакторов или загрузчиков.

Настройки над работающим приложением обновляют metadata прежним `onUpdated`; iframe не заменяется ради focus. Его stage уже до открытия settings закрывает меню и фокусирует постоянную кнопку «Действия с приложением», поэтому именно она является реальным surviving opener.

По отдельному согласованию root изменён только узкий контракт формы `AppSettingsOptions.onClose(afterClose?)`: continuation передаётся хосту, а не выполняется отдельно после него. Хост делает dispose, исключает старый repaint/return при handoff, затем вызывает ещё актуальный continuation один раз. Preview проходит тем же suppress-return путём. Условия busy, dirty, confirmation и сохранения данных не переписывались. Единственный production consumer — `app.ts`; других mounted options callbacks, теряющих этот аргумент, поиск не обнаружил.

## Проверка порядка существующих listeners

| Consumer | Cleanup после изменения |
| --- | --- |
| app settings | dispose и очистка host refs перед разрешением возврата; normal main metadata repaint синхронный; preview/route/forced close без прежнего repaint |
| appearance | destroy theme controls и unsubscribe PWA в одном callback |
| visibility | сброс `visibilityOpen` в callback |
| group management | repaint только при актуальном account/screen/group и отсутствии intervening action |
| invite | остановка debounce и увеличение request generation в callback |
| saved/library/discussion | прежние constructor callbacks очищают только собственный dialog ref; повторное закрытие безопасно |
| notes actions/purge | прежние conditional ref callbacks; editor/receipt правила не менялись |
| access panel | прежние clearSecret/dirty/ref cleanup; busy `cancel.preventDefault()` и disabled X сохранены |

Все прежние поздние `addEventListener('close', ...)` в `app.ts` переведены в управляемые constructor callbacks. Единственный такой listener в `src/world/*.ts` теперь находится внутри shared finalizer. Обычные native event listeners не выдаются за синхронные события браузера.

## Выполненные проверки

Последовательный локальный запуск:

```text
node --test --test-concurrency=1 src/world/dialog-focus.test.mjs src/world/app-account-lifecycle.test.mjs src/world/app-audience.test.mjs
28 tests / 28 PASS / 0 FAIL / 0 SKIP (1.976 s)
node node_modules/typescript/bin/tsc -p tsconfig.json --noEmit
PASS
git diff --check
PASS (сообщения о нормализации LF/CRLF не являются ошибками whitespace)
```

Новые 10 cases исполняют настоящие transpiled `dialogs.ts`, `app.ts`, `application-card.ts`. Browser native focus/close queue и settings form там заменены явными тестовыми портами. Проверены callback order, once-only cleanup, nested/follow-up modal, native cancel veto, native intervening focus, rename/unchanged chain, ABA/route/destroy, handoff/preview, toolbar/runtime identity, same-app/main fallback и отдельный same-group target без оживления прежнего ticket. Это контролируемые unit/integration проверки, **не доказательство native browser поведения**.

Существующий `app-audience.test.mjs` получил лишь недостающий `location.hash` в VM dependency port после появления route fence. Assertions и данные проекций не ослаблялись. Соседние 14 lifecycle и 4 audience теста прошли.

## Actual mounted gate для root

Dev-only страница: `http://localhost:5420/src/world/dialog-focus.test.html?run=1` (или тот же путь текущего Vite origin). `#qa-results[data-test-status]` принимает `running`, `passed`, `failed`; `data-passed` — счёт только после полного успеха. Девять сценариев: rename/main rerender; surviving opener; два Escape + native cancel + discard; shared follow-up modal; native veto/intervening focus; preview + toolbar/iframe continuity; A→B→A; подтверждённый и отменённый route leave; same-group main с удержанным ответом списка карточек и последующим пользовательским focus. Кнопка «Открыть пример для клавиатуры» оставляет настоящее окно для ручной проверки.

Страница монтирует настоящие `mountWorldApp`, `mountAppSettings`, `createDialog`. API синтетический. Storage только этого JS realm направлен в memory до импорта приложения; существующие записи не читаются/не перезаписываются. Runtime — srcdoc, не настоящий HTTP/WS источник. Test-only delay/rAF дают нативной очереди и layout завершиться; production возврат их не использует.

На момент авторского freeze браузерный стенд **не запускался автором**: единственный browser owner — root. Открыты actual mounted gate на 320 и desktop, настоящие Enter/Space/Tab/Shift+Tab/Escape, native CloseWatcher и проверка отсутствия отложенного возврата после собственного home rAF. Физическое устройство, installed PWA и сочетания экранных дикторов/браузеров остаются внешними gates. Общий world suite и browser acceptance не приписываются этому авторскому запуску.

Root сообщил предварительный CUA run `8/8 PASS` на `127.0.0.1:5420`, до добавления same-group correction и девятого сценария. Это отдельное атрибутированное свидетельство, не итоговая приёмка финального девятого среза.

## Итоговая локальная приёмка интегратора

Финальный девятый срез выполнен root в настоящем Chromium через CUA: **9/9 PASS** при 320×760, 667×375 и 1280×800. На всех трёх ширинах `overflow:false`. JSON-квитанции — `output/implementation-20260930/p2-dialog-focus-{320,667,1280}.json`. Это native DOM с явным synthetic API, а не доказательство сетевого сохранения. [Независимый reviewer](p2-dialog-focus-independent.md) отдельно повторил 28/28 focused cases и сверил исходники с авторскими hash.

Дополнительно root прошёл обычную оболочку существующего локального QA-аккаунта: Enter на «Связи и доступ» → Enter на «Название и доступ» → изменение названия → Enter «Сохранить название» → подтверждённое новое название → Escape. При 320×760 действительно сохранённое переименование вернуло focus на новую кнопку той же карточки приложения после rerender; при 1280×800 тем же путём восстановлено прежнее название «Покупки · второе окно». В обоих случаях `activeTag:BUTTON`, ожидаемый `aria-label`, `dialogs:0`, `overflow:false`. Доступ, источник, аудитории и черновики не менялись. Квитанции — `p2-dialog-main-{320,1280}.json`, изображение — `p2-dialog-main-focus-desktop.png` в той же output-директории. Проверка типов root прошла; лог `p2-dialog-types-root.log`.

Узкая найденная D4 ошибка возврата focus локально закрыта. Физический телефон, installed PWA, screen readers и весь оставшийся P2-I перечень этим не объявляются пройденными. Итоговый общий набор соседней P4 интеграции будет записан отдельно; production не менялся.

## Первичные источники

[WAI-ARIA APG Dialog Modal Pattern](https://www.w3.org/WAI/ARIA/apg/patterns/dialog-modal/), прочитан 30.09.2026: при исчезнувшем invoking element возврат выбирается по логике workflow. Это обоснование цели, не заявление полного WCAG соответствия.

[WHATWG HTML, close the dialog](https://html.spec.whatwg.org/multipage/interactive-elements.html#close-the-dialog), Living Standard, snapshot страницы от 29.09.2026, прочитан 30.09.2026: нативный возврат и последующее queued close различаются по порядку. Прикладной finalizer учитывает этот порядок, не подменяет native cancel алгоритм.

## SHA-256 авторского среза

| Файл | SHA-256 |
| --- | --- |
| src/world/dialogs.ts | fc2f9ae4b42be350af47191cbb8e61c46bc1ac4e28fece28e45cb807f31c2772 |
| src/world/app.ts | 3c21524b70516ef5ff59c5076b0955235368b5e7926467095e18e5b3ae21745c |
| src/world/application-card.ts | ce2b93ccf7de948c36577bfec54ce58c41dc3402c78e5f10ddb861c72ce80681 |
| src/world/app-settings.ts | 585ff1b366f64a4a7838496c3056512db3bd84e239d3857900f75bdae5401b03 |
| src/world/app-settings.types.ts | 7d9af6f536531a5d9789b96681c6e7e2bf1af7a5e6deb963a7c9870ead283c10 |
| src/world/dialog-focus.test.mjs | a1a96dc81babb59337cb8facbbbe18378e85e17818eb078dd61530c1d61afc17 |
| src/world/dialog-focus.test.ts | 61686d126444c200a615f311484040a74c073459ee31731f19b947576dde8522 |
| src/world/dialog-focus.test.html | 6444e9c7cd45e4143b97b25103bcf093e81bd2d8766b803d49dd2828c1ddb551 |
| src/world/app-audience.test.mjs | f67ada82a7c93d541bf8b8f5e3d5b48b6c155f377b1ed66cb4ea0f482339808e |
