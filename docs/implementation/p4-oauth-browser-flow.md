# P4-C1 — настоящий браузер, завершение OAuth и PWA

Дата: 2026-10-01. Root продолжает принятый master P0–P8. Этот срез проверяет реальный HTTP/PWA, а не объявляет завершёнными MCP, CLI, модельный пилот или production release.

## Причина и исправление

В первоначальном изолированном стенде настоящий PWA аккаунт подписал approve, но клиент не получил callback и Note не появилась. HTTP tests с явным Origin не воспроизводили вычисление заголовка браузером.

Root проверил две гипотезы отдельными native HTML forms. Удаление формы сразу после `submit()` само по себе не отменило POST: два отдельных ручных нажатия старого варианта дали два POST, новый вариант — один. Это **не** доказательство, что удержание формы исправило исходный OAuth сбой. Evidence: `output/implementation-20260930/p4-oauth-form-navigation-counts.json`.

Причинная browser-проба, меняющая только Referrer-Policy, дала `no-referrer → Origin:null` и `same-origin → Origin:same-origin`. Evidence: `p4-oauth-form-origin.json` в той же папке. Это соответствует [WHATWG Fetch, append a request Origin header](https://fetch.spec.whatwg.org/#append-a-request-origin-header) и [Mozilla, effect on Origin](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Referrer-Policy#effect_on_the_origin_header). Для обычной HTML POST navigation это отличается от fetch CORS requests, которыми пользовался HTTP helper.

Настоящий экран согласия и Provider resume200 account-switch autoform используют `same-origin`. Strict Origin admission сохранён; чужой и literal `null` по-прежнему закрыты. Context/token API и redirects сохраняют `no-referrer`; оба error writers явно восстанавливают эту политику. Same-origin policy не отправляет Referer внешнему callback: в настоящем flow получено `callbackRefererPresent:false`.

Нативный Back после успешного подключения выявил второй UX дефект: consumed consent GET показывал raw JSON. Только прошедший прежние transport/path/method/body проверки consent GET теперь использует общий fixed error document с нейтральным «Запрос подключения недоступен». Исходный HTTP status сохранён; context/complete остаются JSON. Экран не отражает UID, code, state, nonce или bearer и не содержит script/form. Этот статус не выдаётся за отсутствие уже созданной записки.

## Завершение навигации в UI

Новый общий helper сохраняет форму до ухода страницы, блокировки form-action или 8-секундного предела. Делает ровно один POST, очищает форму/timer/listeners, не копирует blocked URI в ошибки. `pagehide` означает только уход страницы, **не** ACK OAuth. Неподтверждённый возврат переводит экран в чтение состояния запроса; автоматического approve или повторного POST нет. Поздний отказ после смены аккаунта/закрытия не перерисовывает UI и не перехватывает фокус.

Финальный focused navigation/controller gate: **22/22 PASS, 0SKIP**, 774.2403ms (`p4-oauth-navigation-policy-tests.log`). Typecheck и стандартный prebuild/build PASS (`p4-oauth-navigation-policy-types.log`, `p4-oauth-navigation-policy-build.log`). Это DOM/port acceptance; настоящий PWA путь подтверждается отдельно ниже.

Первые два настоящих HTTP policy cases прошли causal RED → GREEN на неизменённых tests. Затем добавлен один consumed-document case с настоящими signed complete/code/token; прежние два сохранены побайтово. Итог **3/3 PASS, 0SKIP**, 2448.0821ms; изменённый expired Provider resume **1/1 PASS**, 1016.0563ms. Проверены неизменные durable rows после literal null-Origin, настоящий A→B session switch с сохранённым refresh A и фиксированный HTML вместо JSON без изменения authority. [Авторская квитанция](p4-oauth-browser-policy.md), [независимый audit](p4-oauth-browser-policy-independent.md).

## Браузерная приёмка

Первый confirmed-Origin runtime5491/5492 (`170c1ac95ebf7bbeb9856ff63658bdda`) прошёл настоящий signed browser approve/callback/native201; одна Note была доступна в PWA. Прерывание завершило его process и test-client memory. Ready в прежнем status является историческим. Root RO snapshot `interrupted-snapshot.json`, SHA `025700b4ad46d33f84fee1fdc0f5b1ef8f93c76f7adba01676aeee499fa44519`, подтверждает accounts1, Invocation1, Note1/proof1/revision1. Правка/replay/revoke этому run **не приписаны**. Старые данные сохранены; эфемерные OAuth keys не заменены на новые поверх старой базы.

Полная приёмка прошла на новом owned fresh runtime5493/5494 (`f951cfb90e480b7fa119c79d652b25b0`, PID48260). [Устройство стенда и safe zero smoke](p4-oauth-pwa-stand.md): настоящий `createHttpApp`, fresh Notes2/Caps3, актуальный dist/source; identity/decision/token/Note не seed. До первого browser шага все выбранные SQL/OAuth counts0.

1. PWA создала настоящий тестовый аккаунт через свой обычный клиент. Root запросил подключение и нажал «Разрешить» на320×760. Completion POST имел exact Origin; клиент получил один callback, один token exchange и native201.
2. Защищённая ссылка открыла исходный текст в PWA. Root изменил body, дождался «Сохранено». Refresh вручную дал200; exact повтор прежнего key/input дал200/reused=true с теми же Invocation/Note и historical revision1, без текущего body.
3. Перезагрузка PWA сохранила правку. В «Доступы и действия» видно одно завершённое создание и остаток19 из20. На настоящем320×760 раскрыты разрешения и отозвано точное подключение с полным ID в confirmation; status стал «Отключено».
4. Ручной повтор клиента после отзыва дал401/authorization_required. Его ранее полученная квитанция остаётся помеченной как последняя подтверждённая; нового результата сервер не выдаёт. Владелец по-прежнему открывает свою отредактированную Note.
5. Native Back вернулся к consumed consent и показал исправленный нейтральный HTML с безопасной домашней ссылкой. Это обычный reload; persisted BFCache не объявлен проверенным. Существующий `pageshow.persisted→reload()` сохранён.

Root финальный RO snapshot `var/p4-oauth-pwa-final/runs/f951cfb90e480b7fa119c79d652b25b0/completed-snapshot.json`, SHA `9b4231e8284252a199a506ddaf416ed1e4e379d91ffb93c83c5b937521c61d01`: accounts1, Invocation1, **Note1/proof1/revision2**, ожидаемая правка совпала; connection1/revoked; root budget20/spent1/reserved0. Client1start/1callback/1exchange/1refresh, native attempts3: initial201, exact replay200, post-revoke401. Callback без Referer. Reads выполнены отдельными RO transactions, не одним cross-store snapshot; body в artifact не включён.

Native screenshots и DOM/geometry checks: consent320×760 (scrollWidth320, обе decision кнопки48px), настоящая access/revoke panel320×760, Notes и история1280×720, светлая и нейтральная graphite темы. Графитовый фон actual `rgb(16,17,18)`; owner Note содержит сохранённую правку после отзыва. Скриншоты находятся в tool trace; локальные image filenames не выдумываются. Реальные Codex/OpenCode executables, MCP, модельный набор D1 и HTTPS выпуск остаются следующими отдельными gates.
