# P3-D1 — интеграция личной библиотеки

Дата:30 сентября2026. Основание — [контракт D0](p3-engagement-plan.md). Этот документ описывает серверный подэтап; новые пользовательские кнопки относятся к D3, browser cycle — D4.

## Граница интеграции

`createHttpApp` передаёт Apps сервису host-owned `withAuthorityFence`, который вызывает синхронный `world.withCommunityAuthorityFence`. Подписанный Connect удерживает собственную транзакцию, затем берётся короткий World writer fence, затем bounded Apps transaction. World rows при этом не изменяются; отсутствие профиля не вызывает скрытую регистрацию. Привилегированный host method не выставлен как RPC. World execute/close/nesting/async под fence запрещены.

Saved registry проверяет actor до входа и внутри транзакции. `expectedAccountId` проверяется прежней общей оболочкой до dispatch. Отсутствующий host fence запрещает только новые saved operations; старые Apps API остаются совместимыми. Ошибка до эффекта не подтверждает сохранение. Ошибка после Apps COMMIT может оставлять неизвестный исход ответа: повторяется исходный requestId, а durable receipt отделена от свежего `current`.

`engagement-access.mjs` разрешает только точный выбранный domain, а при отсутствии domain — canonical. Он использует фактическую публикацию и grant-проверку. Удалённый/неактивный/private вход возвращает generic unavailable; ошибки БД, аутентификации и программирования не превращаются в отсутствие доступа. Текущий DTO содержит только name/status/canManage, без устройства, connector, grants, route pins или branded runtime decision. Сохранённый собственный label остаётся snapshot, не свежим приватным заголовком.

`launch-path.mjs` используется и launch, и save. Проверяется путь приложения и фактическая длина закодированного bootstrap path ≤8192. Он не изменяет connector protocol или release bundle. Offline/starting/stopped не являются отказом доступа: сохранение доступного адреса не требует работающего ноутбука.

## Почему именно такая блокировка

[SQLite isolation](https://www.sqlite.org/isolation.html) описывает отдельный снимок read transaction в WAL и сериализацию writer transactions. Из этого следует наше конкретное решение: обычный World read snapshot недостаточен, чтобы concurrent membership revoke не вклинился до Apps effect. Короткий World `BEGIN IMMEDIATE` удерживает predicate до конца Apps transaction. Это не атомарный commit нескольких изменяемых баз: World не меняется, а Connect и Apps имеют отдельные границы.

World busy policy100ms и отдельная короткая Apps busy policy ограничивают ожидаемую задержку конкуренции; они не дают жёсткого100ms wall-clock deadline. Синхронная секция не содержит network/await. Лимиты200 записей,128 квитанций,50 строк и256KiB ответа ограничивают объём одной операции. Проверка мощности production и масштабирования относится к нагрузочным gates, а не выводится из unit tests.

## Проверки

- Root helper + host fence + независимый World process + signed HTTP:16/16PASS. Проверены обычная/асинхронная ошибка, nested/close/World mutation, повторное использование, первый профиль, реальный concurrent revoke и восстановление normal busy policy. Log: `p3-d1-root-focused.log`.
- TypeScript:PASS (`p3-d1-typecheck.log`).
- Общий world:646 tests,642PASS,0FAIL,4 прежних explicit opt-in skip (`p3-d1-world-root.log`). Пропуски: immutable connector bundle integration, real inference job, connector health registration scenario,512MB relay. В этом серверном этапе их повторно не включали; не засчитываем их как пройденные.
- Connect70/70PASS (`p3-d1-connect-root.log`); production buildPASS (`p3-d1-build-root.log`). Connector1.4.0 bundle не изменился:189731bytes, SHA256 `f4ba827e556a21db97d6ac073272eaa293fde07f151ceb9c9b81b7814836c5ac`.
- Независимые saved/World/signed HTTP20/20PASS, включая действительно enrolled второе устройство и настоящий второй процесс World writer. Авторские новые model/schema21/21PASS. Подробности и ограничения — [модель](p3-saved-model.md) и [независимая проверка](p3-saved-independent.md).
- Root reader5critical5/5PASS (`p3-d1-reader-root.log`); отдельный deploy175tests/173PASS/2prior skip. Старый настоящий Apps4 migrator отказывает на Apps5 WAL без изменения main/WAL. [Reader report](p3-apps-v5-reader.md) отличает host format recognition от service integrity и реального Linux image.

Локальная D1 приёмка завершена. Первый общий прогон обнаружил восемь устаревших ожиданий latest4/future5: изменены только current-migrator expectations на5/6, исторические входные fixtures сохранены. Независимый signed HTTP RED обнаружил400 вместо503 для двух временных busy codes; узкий mapping `apps_saved_busy`/`world_authority_busy` исправлен, прежние semantic errors остаются400. Handler не повторяет mutation автоматически. Дополнительный model review привёл saved path validators к общему реальному launch limit; тест длинной страницы теперь использует допустимые пути, а не невозможный для запуска Unicode адрес.

Новые элементы UI не добавлялись: старые представления не переиспользованы для будущей библиотеки. Приёмка текущего UI остаётся C2-C/D; её не выдаём за проверку новых сохранений. Production не изменён; Linux exact candidate/fallback, encrypted isolated restore, домен и публичный пилот остаются внешними gates.
