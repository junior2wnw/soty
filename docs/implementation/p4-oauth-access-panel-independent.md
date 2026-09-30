# P4 — независимая проверка панели подключений

2026-10-01. Проверен существующий AccessPanel с точки зрения выбора точного доступа для отзыва, неизвестного результата и смены аккаунта. Это UI/controller gate. Работоспособность AS, signed Connect, полный OAuth и соответствие клиентам он не доказывает.

## Причинная находка: одинаковые подключения

У двух записей могут совпадать профиль, resource, минута создания и бюджет. Изначально карточки и подтверждение показывали только эти данные, хотя mutation внутри уже захватывала правильный ID. Человек не получал устойчивого обозначения выбранной записи.

Root принял узкую поправку: suffix12 как короткий ориентир, полный selectable «Код подключения» в подробностях и подтверждении. Суффикс не объявляется уникальным. В резервном пути показывается отдельный «Код доступа» из `principal.id`; связи с connection ID не выдумываются. Код различает серверные записи, но не сообщает, на каком компьютере работает клиент.

В новом `src/world/access-panel-identity.acceptance.test.mjs` два независимых сценария:

1. Полностью одинаковые описания и намеренно одинаковый suffix12 у двух разных connections. Требуются полные коды. Пока modal второй записи открыт, pending refresh возвращает обратный порядок; подтверждение должно вызвать отзыв только прежнего второго ID и сохранить sibling.
2. API подробностей недоступен, два одинаково названных managed principals. Карточка и modal показывают полный код доступа; единственная mutation — точный `access.principals.revoke`. Нет запросов grants/credentials и фиктивных connection IDs.

На исходном UI оба теста причинно упали: **2 tests, 0 PASS, 2 FAIL, 0 SKIP, 424.7644 ms**. Первый — на отсутствии обозначения полного connection subject, второй — на отсутствии полного principal ID в резервной карточке. Лог: `output/implementation-20260930/p4-oauth-access-panel-identity-red.log`, SHA256 `1f637f96915941540c30a44eb779eb05b1a3c90cfa5c90ca14b827fa95b71ee7`.

Проверенный исходный production pin: `access-panel.ts` — `1c4c23c34da2838a6517ae5787ab082729daa1fa2e40c56a20b0455fad1f7c30`; CSS — `f5a31f059155b8f454b855a58421e40f57ae15a82a420742b10d1f00b7a92788`. Исправление принадлежит root. После его изменения root выполнил общий прогон прежних11 и новых2: **13/13 PASS, 0 FAIL, 0 SKIP, 926.9957 ms**. Независимый reviewer прочитал полный итоговый лог и исправленный source; повтор этого прогона не выполнялся. Все assertions двух причинных тестов сохранены.

## Стенд и независимость

По отдельному разрешению root существующий harness перенесён из `access-panel-oauth.test.mjs` в `src/world/test-support/access-panel.mjs`. Оба файла тестов используют обычные explicit imports; runtime extraction чужого файла не используется. Блок всех прежних 11 tests при переносе побайтно сохранён, изменены только imports и путь к component в общем helper.

Исполняется действительно transpiled `access-panel.ts`, но DOM/dialog/API ports синтетические. Это meaningful проверка отображаемых ID и фактического captured mutation target, а не native geometry/focus/Connect proof. Общий harness не дублирует assertions и не заменяет production rendering собственной копией.

Команда независимого RED на Windows Node24.21.0:

```text
node --test --test-concurrency=1 src/world/access-panel-identity.acceptance.test.mjs
```

## Остальной lifecycle review

- `managedBy:'oauth'` приходит от точной серверной связи. Имена/profile не используются для объединения; marked principal не превращается в legacy key card. У списков собственные страницы по20 и cursors, без скрытого обхода всех страниц. Ошибка подробностей переводит в явно обозначенный safety fallback.
- Modal захватывает kind/ID/cursor до await. Exact ACK проверяется без coercion. Wrong/lost ACK не подтверждает отзыв и не вызывает автоматический повтор. Один bounded readback ищет только захваченный субъект; отсутствие записи на странице не выдаётся за подтверждение. Следующий повтор возможен лишь по явному действию с тем же ID.
- Подтверждённый revoke invalidates старые list requests; поздний ответ не оживляет карточку. Account admission передаётся через `expectedAccountId`; отказ и dispose очищают данные и не позволяют позднему ACK перерисовать иной аккаунт.
- Refresh сохраняет раскрытие и текущую логическую позицию фокуса. Восстановление выполняется в момент синхронного render/закрытия с проверкой актуального mount, а не отложенным focus после await. Busy controls сохраняют фокус и проверяют guard в handler.
- Идемпотентное неизвестное отключение хранится только в памяти mount. Durable outbox/точная история неизвестного ACK после перезапуска не заявлены. Резервный отзыв principal использует существующую authority boundary, не выдаёт новый ключ и не пытается обойти недоступность AS.

Новых blockers этого среза после исправления различимости source review не выявил. Короткий код используется только как ориентир, полный ID остаётся DOM text, а captured target/revoke протокол не изменён. Прежние авторские **11/11** и root native IAB proof отзыва одного sibling при ширине320 — отдельные свидетельства, не результаты двух новых тестов.

## Отдельная native граница

Root обнаружил на667×375, что после lost ACK выросший текст modal оставлял видимой лишь часть focused confirm. Его отдельное исправление — footer sibling scrollable body только для connection modal. Прочитанный source действительно ограничивает grid классом этого modal; legacy dialogs и capture/guards остаются прежними. Затем root отдельно перенёс сам error status из scroll body в footer перед кнопками: прежний 13/13 выше выполнен **до этой второй дельты**.

На окончательном source root повторил13 AccessPanel cases вместе с10 прежними dialog-focus: **23/23 PASS, 0 FAIL, 0 SKIP, 1877.11 ms**. Reviewer прочитал полный лог `p4-oauth-access-panel-final.log`; оба причинных identity tests и harness неизменны. Это проверяет controller regression, но synthetic DOM не измеряет геометрию.

Отдельная атрибуция root native IAB: после переноса кнопок на667 они целиком видимы с высотой44px. На окончательной status/footer версии320 dark root сообщил видимый error и обе44px кнопки в границах modal, successful explicit read после lost ACK, сохранённый sibling и возврат на summary выбранной карточки; desktop1280 — отсутствие horizontal overflow и просмотр screenshot. Reviewer не управлял браузером и не присваивает эти результаты собственному Node gate. Два controller cases не объявляются самостоятельным доказательством всей responsive/accessibility матрицы.

## Проверенный окончательный pin

SHA256 фактических worktree bytes после root identity и обеих footer дельт:

| Файл | SHA256 |
|---|---|
| `src/world/access-panel.ts` | `ad123d5f6311018b39ed226b34e4932e91931ef8fab89b58471ebeb2d8504bac` |
| `src/world/access-panel.css` | `38bb2e33f61a130ecd67e5e827ab67cb06b55f3f09c9fc81f6bd189aa02faabe` |
| `src/world/test-support/access-panel.mjs` | `8733b098d5a9f5c262a9d6e2c6d5929b0bfac21df65ef8ebf2ff74965bf647c7` |
| `src/world/access-panel-oauth.test.mjs` | `b3805a80d5bfd6edca14f0a9bcf221949b390ebb9fc0cc3273a0c8dc1fff0dcc` |
| `src/world/access-panel-identity.acceptance.test.mjs` | `aef94faadbf2162314f94054351b925aa887d2ef5fbb2122d03648f98597c594` |
| `output/implementation-20260930/p4-oauth-access-panel-identity-green.log` | `dc08d6384c4e54160f46c2762e07d6b0d50d55771879ea9a1fa62b8685001aaa` |
| `output/implementation-20260930/p4-oauth-access-panel-final.log` | `a3e2d8f34f1562a37a77ec28cff6eb35d7083c326c6aae1ee4d4b85bc08f87ba` |

Совместная команда root:

```text
node --test --test-concurrency=1 src/world/access-panel-oauth.test.mjs src/world/access-panel-identity.acceptance.test.mjs src/world/dialog-focus.test.mjs
```

Итог подтверждает устранение неоднозначности UI и соответствующие controller regressions после последней status/footer дельты. Полной OAuth/CLI/MCP или production readiness здесь не заявлено.
