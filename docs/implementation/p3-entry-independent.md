# P3-B3 — независимая приёмка входа в приложение

Дата: 2026-09-30. База: checkpoint B2 `cd97cb8`, [подплан входа и транспорта](p3-runtime-transport-plan.md), [независимая приёмка B2](p3-runtime-independent.md).

**Серверный и transport gate: PASS в указанном объёме.** Добавлены 10 независимых B3-сценариев с настоящими HTTP/WS соединениями. Полный файл B2+B3: **40/40 PASS, 0 skip**, около34s, включая неизменённые реальные30s head/ACK timeouts. Проверки не используют production данные или настоящие пользовательские credentials. Это отдельный вывод от браузерной приёмки cookie, popup, focus и установленной PWA.

Независимый проверяющий изменял только `modules/apps/test/app-runtime.acceptance.test.mjs` и этот документ. Сервер, renderer и frontend реализовали другие участники.

## Проверенные сценарии B3

| Проверка | Наблюдаемый результат |
| --- | --- |
| Точный launch/recovery path | Query в `/_soty/boot?path=…#ticket` сохраняет local path и query без пересборки; fragment содержит одноразовый ticket. GET boot не отражает ticket в HTML и не создаёт cookie. После истечения ticket новый явный launch возвращает тот же path. |
| Старый A и новый B | Действующая cookieA не подтверждает новую sessionB. Проверка с nonceB без cookie или с cookieA отклоняется. Только cookieB+nonceB возвращает точный echo; повтор GET в пределах срока разрешён. Bare GET с cookieA по-прежнему проверяет A без заявления о приёме B. |
| Nonce и текущие права | Empty/foreign/duplicate nonce header отклонён. На29999ms проверка проходит, на30000ms — нет; сама cookie остаётся действительной до своего срока. Membership revoke отклоняет даже совпадающие cookie+nonce. Nonce без cookie не открывает private runtime. |
| Адресность reset | DELETE закрывает только streams предъявленных sessions с точным app/domain/origin. Другая session того же аккаунта на том же адресе, другая alias-session, другой аккаунт и уже открытый anonymous stream продолжают работать. |
| Явный публичный вход | DELETE допускает absent/unknown/duplicate cookies лишь при свежем anonymous допуске active public alias. Две clear-cookie записи покрывают Partitioned и unpartitioned host cookies. Missing/null/foreign/duplicate Origin не очищает ничего. Private/canonical/inactive/tombstone не становятся публичными; отказ не удаляет действующую private session. |
| Private top navigation | Только GET+navigate+document без действующей session получает302 на фиксированную оболочку и route с app/domain/local path. Query `returnUrl` остаётся обычными данными локального пути; forwarded host не задаёт destination. Авторизованный запрос получает runtime. |
| Запрет скрытых переходов | iframe, fetch, HEAD, unsafe HTTP, foreign Origin, duplicate Fetch Metadata и reserved/invalid paths не получают302. Имя приложения, owner, device и port не попадают в ответы отказа. |
| Signed public без fallback | Недействительная account cookie на public alias даёт status без runtime200, автоматического очистителя и shell redirect. Только отдельный явный DELETE и следующий запрос без cookie создают публичный вход. |
| Managed HTML/CSP | Query с `</script>` остаётся данными; в HTML один разрешённый script, его nonce совпадает с CSP и меняется между ответами. Нет `allow-popups`, `allow-top-navigation` или `unsafe-inline` для scripts. No-store/no-referrer сохранены. Неоднозначный boot query отклонён; inactive503/tombstone410/unknown404 не превращаются в recovery redirect. Boot без path не раскрывает private entryPath. |
| Нормализация и совместимость | Server launch/boot отвергают `/x/..//double` и encoded-dot вариант. `/#/dashboard`, `/board?tag=a%2Bb#item` и encoded `%23` в query сохраняются точно как локальная app navigation. |

Deadline/nonce проверки используют предусмотренные управляемые часы сервиса. Проверка old-A/new-B воспроизводит предъявление cookie на настоящем HTTP, но не объявляется доказательством решения конкретного браузера сохранить Set-Cookie.

## Найденные расхождения и решения

1. **Неодинаковая нормализация path.** Сервер принимал `/x/..//double`, тогда как renderer и оболочка отвергали выданный recovery target. Серверная проверка согласована с остальными; добавлена отрицательная исполняемая регрессия.
2. **Предотвращена потеря hash-SPA.** Первоначально новый frontend запрещал raw fragment в path. Это излишне строго для существующей локальной навигации `/#/dashboard`. После совместного пересмотра hash-SPA сохранены, а origin/reserved/normalized-double-slash ограничения остались. Positive cases проверяют именно эту совместимость.
3. **Более точный status после delayed ticket body.** Два прежних B2 assertions ожидали общий401/403 после изменения alias. B3 перечитывает адрес перед выводом ошибки: деактивация даёт503, retire410. Независимый тест теперь требует именно эти статусы; отсутствие Set-Cookie, Location и connector open остаётся обязательным. Произвольный4xx/5xx не принят как замена проверки.

Первый B3 запуск против старой B2 реализации имел1/8 PASS и7 expected failures: отсутствовали query path, nonce, DELETE и private302. Они не записаны как вновь найденные дефекты. После реализации и корректировки статусов полный набор проходит.

## Read-only review renderer и оболочки

Первоначально повторены14/14 `runtime-pages.test.mjs`; после финальных визуальных поправок повторены16/16 вместе с19/19 launcher tests, суммарно35/35 без skips. Их DOM/Fetch harness выполняет настоящий сгенерированный script в Node VM, но является моделью браузерного окружения. Подтверждены очистка fragment до запроса, один POST, exact nonce echo перед навигацией, отсутствие автоматического reset/retry, сохранение ошибки при timeout, защита от поздних ответов после pagehide/BFCache и отсутствие попыток открыть shell внутри iframe. Script/HTML данные экранируются; raw error не выводится пользователю.

Источник `app-launch.mjs` и интеграция `app.ts` прочитаны отдельно: actor+target закреплены на время запроса, поздний ответ после account/screen change не применяется, popup создаётся в пользовательском событии до await и получает новый ticket, уже открытая отдельная вкладка не закрывается при dispose. Необязательные сведения из списка приложений не блокируют прямой named launch и не дают доступа к чату.

Перед браузерной приёмкой найден focus gap: keyboard Retry/Refresh пересоздавал сфокусированную кнопку вместе со screen. Автор добавил захват факта, что фокус был внутри старого app stage, и фокусировку нового `h2` только на синхронном mount до await. Финальный source прочитан: поздний ответ не меняет фокус. Повторены19/19 launcher tests и12/12 actual-connector/legacy integrations; source-level замечание закрыто. Фактический keyboard/focus позднее проверил root в отдельном [браузерном gate](p3-entry-browser.md), который не подменяет независимый сетевой опыт.

Финальный read-only review охватил компактный framed renderer и landscape toolbar. `data-framed` определяется сравнением `window.top === window.self` и влияет на компоновку, не выдаёт прав. При высоте до280px и ширине от480px скрыты только повторный бренд/декоративная подпись; смысловой статус, явный public reset и инструкция использовать внешнюю панель остаются. Прокрутка не запрещена. Формулировка «Доступ изменился» применяется лишь при разрешённом сервером public recovery; она не вызывает reset автоматически. Toolbar в коротком landscape имеет высоту56px и цели не меньше44px; скрытая подпись аккаунта компенсирована явными `aria-label` и `title`. Ticket, Origin, nonce/CSP, sandbox и сетевой admission этим срезом не менялись.

Независимо просмотрены сохранённые root снимки `p3-entry-framed-final.png` и `p3-entry-landscape-667-final.png`: в первом видны полный статус, кнопка и инструкция; во втором toolbar оставляет область самому приложению. Числа667×199 и643×157.73/no-scroll взяты из браузерных измерений root, а не выведены из VM tests. Повторный полный40-case network suite после чисто визуального среза не запускался: hash сервера и независимого suite остались прежними. Новых material blockers в этом срезе не выявлено.

## Точная граница deep links

- `apps.launch(path)` и trusted shell route с encoded path сохраняют query **и client fragment**. Это рабочий способ передать private hash-SPA deep link.
- HTTP request path+query сохраняются в302 к shell. Исходный raw fragment URL вида `https://private-app/#/dashboard` сервер не получает.302 с собственным `#launch` не восстанавливает невидимый серверу fragment.
- Browser interstitial для захвата такого fragment не входит в зафиксированный B3 контракт. Раздел C публикации должен выдавать trusted `#launch` link с encoded path для private hash-SPA, когда у получателя ещё нет session. Public URL fragment браузер сохраняет естественным образом.

## Команды и проверенный срез

```text
node --test --test-name-pattern="B3:" modules/apps/test/app-runtime.acceptance.test.mjs
10 tests; 10 pass; 0 fail; 0 skip

node --test modules/apps/test/app-runtime.acceptance.test.mjs
40 tests; 40 pass; 0 fail; 0 skip; duration 34.00s

node --test modules/apps/test/runtime-pages.test.mjs src/world/app-launch.test.mjs
35 tests; 35 pass; 0 fail; 0 skip; renderer16 + launcher19

node --test src/world/app-launch.test.mjs modules/apps/test/app-runtime.test.mjs modules/apps/test/apps.test.mjs
31 tests; 31 pass; 0 fail; 0 skip

node --check modules/apps/test/app-runtime.acceptance.test.mjs
git diff --check -- modules/apps/test/app-runtime.acceptance.test.mjs
```

Syntax/diff проверены; предупреждение Git о будущей нормализации LF→CRLF не является ошибкой. Проверенный незакоммиченный B3-срез поверх `cd97cb8`:

| Файл | SHA-256 |
| --- | --- |
| `modules/apps/server/index.mjs` | `ffcfe092de53b82990f4623ec6bb6b6944ff4d3d03e964778f9b86a29245a4ab` |
| `modules/apps/server/runtime-pages.mjs` | `b3e9beeddb9eff78069252a66f2fc7204829b070e1318c3c5c6fd6ec8a0520dd` |
| `modules/apps/test/app-runtime.acceptance.test.mjs` | `3be26bb01961f5d2e4c4351bc6deecac0b22327f03bffc974a0bc22dce3aa21b` |
| `src/world/app.ts` | `0d3f36c269dd12f818248b90f1391fb25922a52c888b429f67b847786767e564` |
| `src/world/app-launch.mjs` | `b0da902718383e69af02d9248a20193b898b9c0a329ccc3477b5e38205bdb0ab` |
| `src/world/application.css` | `8a00905b23f281005b3a220a3af3fe50851e73464a902cf50937d8dc87f6dd3e` |

## Что этот gate не утверждает

Отдельный [прогон root](p3-entry-browser.md) подтверждает на локальном Chromium canonical/named HTTP+WS, точный query, новую внешнюю вкладку, явный public reset, offline/retry, keyboard focus и компактные размеры. Его финальный общий результат —380tests/377pass/0fail/3opt-in skip, typecheck/build PASS, dev3/3; это заявленные root результаты, не повторённый здесь общий прогон. Node VM и просмотр PNG не заменяют браузерное выполнение.

Настоящее запрещение cookies настройкой браузера, смена самой identity в браузере, popup-blocker поведение реального браузера, Safari/Firefox, физический телефон, screen reader и установленная PWA в B3 этим опытом не подтверждены. Account/screen races и blocked-popup branches проверены helper tests, old-cookie/new-cookie mismatch — настоящим HTTP suite. Public DNS/TLS, готовность release image, внешний restore/fallback, настоящие пользовательские устройства и совместимость всех браузеров по-прежнему не объявлены пройденными. Reset не откатывает уже выполненные действия приложения и не отзывает чужие sessions; private история/чат не становятся публичными от открытия app.
