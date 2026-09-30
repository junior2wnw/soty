# P2-I — независимая проверка закрытия окон

30.09.2026. Read-only review финального авторского среза, отдельный от [implementation receipt](p2-dialog-focus-implementation.md). Production и авторские tests не изменялись. Проверены `dialogs.ts`, lifecycle и inspect/settings/group paths в `app.ts`, узкий контракт `app-settings.ts`/types, action key карточки, controlled tests и actual mounted fixture. Дополнительно прочитан существующий отложенный возврат главной в `apps-home.ts`.

**Вердикт:** в рассмотренных путях блокирующего расхождения между принятым контрактом и кодом не найдено. Независимый повтор 28 focused tests прошёл; root отдельно получил 9/9 actual mounted на трёх размерах. Проверка настоящей клавиатуры в обычном приложении остаётся отдельной от fixture.

## Что проверено по исполнению и исходникам

| Граница | Наблюдаемое основание |
| --- | --- |
| Cleanup до выбора новой цели | Управляемый `close()` вызывает native close, затем единый `finish`. Внутри `finished` выставляется до callback, снимаются capture listeners, удаляется dialog, выполняется dispose/render, только затем вызывается `resolve`. Поздний native `close` не повторяет cleanup или focus. |
| Последующее действие пользователя | Queued native fallback отмечает intervening pointer/key/click/focus. Cleanup остаётся обязательным, но получает `interrupted`; settings/group не перерисовывают старый экран. Новое активное control или другое открытое окно после callback запрещают прикладной возврат. |
| Inspect → settings | Исходный ticket передаётся второму окну; промежуточное закрытие подавляет custom return. Сопоставление replacement использует app ID и action, а не изменяемое название. Порядок — surviving opener, same-app inspect, same-app launch, main прежнего экрана. |
| Scope и ABA | Ticket проверяет account ID вместе с account generation, screenSequence, фактический hash, activeRoute и живой controller. `transitionAccount` увеличивает generation до cleanup; A→B→A не оживляет прежний ticket. Forced close после смены screen/account не выполняет старый metadata repaint. |
| Handoff и dirty form | X, Escape и native cancel settings продолжают идти через `requestClose`. Новый `onClose(afterClose?)` переносит continuation в host: сначала dispose, затем ещё актуальное продолжение. Handoff и preview не возвращают фокус в старую карточку и не запускают старый repaint. |
| Group repaint | Исходный ticket проверяется до `renderGroup`. После синхронного render выдаётся отдельный target к main лишь для той же account generation/group/route. Старый ticket не становится current; async карточки не выбираются новой целью после ответа. |
| Работающее приложение | `onUpdated` обновляет metadata существующего stage; возврат может использовать постоянную toolbar-кнопку. Изменения P2-I не пересоздают iframe ради focus. Controlled case проверяет identity iframe и его ввода. |
| Фокусируемость | Проверяются connected, disabled/aria-disabled, hidden/inert, layout, CSS visibility и закрытые details. Явный tabindex=-1 разрешён для workflow main. Вложенное окно возвращается внутрь ещё открытого родителя; новое modal не перекрывается поздним возвратом. |

Новый механизм не записывает собственные focus-команды в `homeState.focusId/focusControl` и не добавляет rAF/таймер ожидания карточек. Существующий `apps-home.ts` rAF остаётся отдельным каналом восстановления главной: controlled `dialog-focus.test.mjs` подменяет её render портом и сам по себе этот browser порядок не доказывает. Actual mounted rename scenario намеренно переводит фокус наружу после закрытия и ждёт frame/native queue; финальный результат этого сценария следует брать из root browser receipt.

## Независимый запуск

```text
node --test --test-concurrency=1 src/world/dialog-focus.test.mjs src/world/app-account-lifecycle.test.mjs src/world/app-audience.test.mjs
28 tests / 28 PASS / 0 FAIL / 0 SKIP
duration_ms: 2091.9099
```

Новые 10 cases исполняют transpiled production `dialogs.ts`, `app.ts` и `application-card.ts`, но native DOM/close queue и settings form представлены управляемыми портами. Они подтверждают порядок callback, once-only cleanup, rejection stale scope, continuation и selected DOM identity в этом harness. Остальные 14 lifecycle и 4 audience cases прошли тем же независимым запуском. Новых тестов, повторяющих implementation, не добавлялось.

Прочитана dev-only `dialog-focus.test.html`/`.ts`: девять scenarios монтируют настоящие World/settings/dialogs, используют synthetic API, memory storage и srcdoc runtime. Программные `focus/click` и `KeyboardEvent` внутри fixture не являются отдельным доказательством настоящего Enter/Space/Tab/Shift+Tab/Escape. Сам fixture в этом review браузером не запускался.

После независимого source/test gate root сообщил финальный actual mounted результат. Файлы результата перечитаны мной: `output/implementation-20260930/p2-dialog-focus-320.json` — 320×760, `p2-dialog-focus-667.json` — 667×375, `p2-dialog-focus-1280.json` — 1280×800; каждый содержит `9/9 PASS` и `overflow:false`. Это настоящие native DOM/layout/очереди с synthetic API, а не signed account/connector опыт. Таким образом, финальная group correction и отсутствие позднего home-frame возврата подтверждены root fixture на том же проверенном срезе. Физическую клавиатуру, экранный диктор или все browser implementations из этих JSON не выводим.

## Границы заключения

- `restoreFocus:false` подавляет прикладной возврат, но не обещает отменить собственное краткое восстановление браузером внутри native `close()`.
- Итоговый 9-case mounted gate принадлежит root и атрибутирован выше. Предварительные 8/8 до group correction не подменяют финальные девять cases. Настоящая клавиатура и визуальная различимость focus в обычном приложении отдельно проверяются root.
- Installed PWA, физическое устройство, разные browser/CloseWatcher реализации и экранные дикторы этим запуском не проверялись. Полное соответствие WCAG не заявляется.
- Проверка не распространяется на все старые async actions любых окон World; просмотренная дельта не меняет серверные права, draft persistence или правила подтверждения несохранённой формы.

## Проверенные SHA-256

Файлы перечитаны и hashes вычислены после независимого запуска; совпадают с авторским freeze.

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
| src/world/app-account-lifecycle.test.mjs | 08d58657a1151bc5c7c324faa049a02ec0d9269a563d1488f49cad3936f2bad6 |
| src/world/app-audience.test.mjs | f67ada82a7c93d541bf8b8f5e3d5b48b6c155f377b1ed66cb4ea0f482339808e |
