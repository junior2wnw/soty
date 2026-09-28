# Технический аудит перед выпуском — 28 сентября 2026

Проверка выполнена по исходникам и изолированным временным данным. Production и рабочие пользовательские данные не изменялись. Визуальный аудит ведётся отдельно. Этот документ не утверждает, что весь продукт защищён от любых атак или выдерживает произвольное число одновременных пользователей.

## Подтверждённые дефекты и исправления

**P1: некорректный WebSocket Upgrade завершал весь HTTP-процесс.** В реальный `server/index.js`, запущенный отдельным дочерним процессом с временным `DATA_DIR`, передан request target `http://[`. До исправления: `ERR_INVALID_URL` из `modules/apps/server/index.mjs`, exit code 1. Парсинг находился перед обработчиком исключений. Исправлены оба входа: общий upgrade ingress и самостоятельно используемый application gateway. Теперь запрос получает HTTP 400; последующий `/health` отвечает 200. Это проверяет не только функцию парсинга, но и живой сервер.

**P1: legacy recovery позволял неограниченно заполнять диск анонимными PUT.** В отдельном Express mount три запроса без Authentication и Origin создали три постоянных файла (HTTP 200). Старый wire намеренно использует секретный lookup и зашифрованный envelope; требование новой Connect-авторизации сломало бы существующий путь восстановления. Исправление сохраняет wire и действительные резервные копии:

- До JSON parsing применяются ограничения по реальному peer и глобальные бюджеты, ограниченная таблица rate counters, не более четырёх одновременных записей и восьми чтений. `X-Forwarded-For` не меняет peer без явно настроенного Express trust proxy.
- Каталог ограничен 1 GiB и 10 000 recovery-файлами; перед каждой сериализованной записью учитываются реальные файлы и рост заменяемого envelope. Рестарт не сбрасывает дисковую квоту. Остаток свободного диска должен составлять минимум 64 MiB сверх временной новой копии.
- Квоты на объём, количество и дисковый запас можно задать через `SOTY_ACCOUNT_TRANSFER_MAX_BYTES`, `SOTY_ACCOUNT_TRANSFER_MAX_FILES`, `SOTY_ACCOUNT_TRANSFER_MIN_FREE_BYTES`. Некорректная конфигурация отклоняется при старте. Размер одного envelope сохраняет прежний предел.
- Запись использует уникальный временный файл, `fsync`, атомарный rename; на POSIX синхронизируется каталог. Незавершённая операция удерживает concurrency slot даже после отключения клиента.
- Выдача существующей копии и уменьшающая перезапись работают и после снижения квоты ниже уже занятого объёма. **Действительные recovery-файлы не имеют TTL.** Истекают только rate buckets и оставленные временные `.tmp` старше суток.
- Пределы возвращают фиксированные ошибки 429/503/507; malformed/oversized JSON — 400/413 без stack trace. Ответы не кэшируются.

Эта файловая квота соответствует текущему размещению: один application process на data volume. Она ограничивает возможный ущерб ресурсу, но не заменяет защиту от распределённого злоупотребления на сетевой границе. Несколько процессов на одном volume потребуют межпроцессного атомарного quota store; такой режим не заявляется.

Финальный review также выявил аналогичный унаследованный дефект в локальном connector: обработчик ошибки повторно разбирал malformed URL и выбрасывал исключение уже из `catch`. Реальный дочерний connector на временном `DATA_DIR` обрывал raw HTTP запрос `http://[`; регрессия падала с `ECONNRESET`. Теперь URL разбирается один раз перед dispatch, malformed target получает 400, а следующий `/health` отвечает 200. Loopback-привязка ограничивала эту проблему локальным ingress.

Регрессии: `node --test server/test/upgrade-ingress.test.mjs server/test/account-transfer.test.mjs` — **9 passed**. Проверены сырой Upgrade в реальный сервер, malformed HTTP в реальный connector, standalone gateway, квота после рестарта, конкурентные записи, увеличение существующего envelope, чтение/уменьшение при сниженной квоте, отсутствие подмены peer заголовком, истечение rate counters, лимит неполных тел, abort, отсутствие TTL у старой копии и очистка только staging-файлов.

## Что дополнительно проверено

- **Connect:** происхождение actor из активной подписывающей установки; proof связан с Origin/операцией/аргументами/project/expiry/challenge; replay и revoked-device отклоняются; шифрованный vault и recovery не выбирают чужой account через payload. `connect:test`: **64 passed**.
- **World:** приватность до пагинации/FTS; invite-only не доступен по угаданному ID; роль и текущее членство проверяются при каждом чтении/записи чата; перенос владельца и moderation проверены транзакционно.
- **Notes:** owner boundary, CAS, потерянный ACK и поздний ответ, конфликтующие вкладки, quota failure и offline outbox проверены существующими сервисными и browser-state тестами. Тексты не объявляются сквозно зашифрованными.
- **Apps:** разные origins, одноразовые короткие launch tickets, host-only HttpOnly Secure Partitioned cookie; live membership/device/grant checks; revocation завершает HTTP/WS; loopback-target не принимает произвольный host; credentials не передаются пользовательскому приложению; websocket fragmentation и размеры ограничены.
- **PWA:** только shell/assets кэшируются; API не берётся из service-worker cache. Обновление требует подтверждения сохранения от всех вкладок; новая вкладка между prepare/commit блокирует активацию. Состояние offline-ready проверяет наличие всех shell/module assets.
- **Каталог:** FTS5 и keyset пагинация ограничивают выдачу; счётчики популярных запросов возвращаются как `1000+`. Стоимость проекции очень большой группы и синхронного SQLite остаётся предметом нагрузочного измерения; прежний benchmark на 50k записей не доказывает столько же concurrent users.
- `world:test` перед P1-правками: **94 passed, 2 skipped**; `dev:test`: **2 passed**. Два opt-in OpenCode/runtime теста не запускались; доступность реальной inference этим прогоном не доказана.

## Границы и оставшиеся наблюдения

Унаследованный `server/room-store.js` сохраняет каждый открытый room в process Map без eviction после ухода последнего peer. Это P2 по длительной эксплуатации: рост retained rooms может увеличивать RSS; конкретный production memory leak/скорость роста здесь не измерялись. Изменение lifecycle старых зашифрованных комнат требует отдельной регрессии сохранения, reconnect и pending join, поэтому в срочные исправления оно не включено.

## Интеграция с актуальной production-веткой

Release worktree основана на `69f9940` из `codex/connect-release-20260925`, а не на старом локальном `a2ccecc`. Новые World/Apps/Notes и extensions Connect совмещены с современным runtime по трём версиям исходников. Сохранены production inference/model-routing, delegated grants, durable request ledger, SQLite worker с fencing владельца и graceful close, maintenance/readiness, signed release feed и recovery экспорт updater. Production `gonka-proxy.js`, identity-модули и операторские deployment-файлы не заменялись старыми копиями.

Account-owned задания используют `soty.connector-job.v4`. Владелец, request digest и запрошенные connector/device обязательны; старый room-link API не выдаёт и не отменяет такое задание. Оно использует тот же production durable ledger для повторной отправки и восстановления после потерянного ACK. Чтение, отмена, выдача результата и runtime status остаются подписанными; проверка разрешения повторяется внутри очереди изменения состояния. Регрессии проверяют reopen после закрытия SQLite owner, v4 binding и отделение legacy/delegated/account-owned путей.

**Граница rollback:** после появления v4 в live data пригоден только код, умеющий читать v4. Старый production reader отказывает в connector storage, а HTTP-сервер остаётся доступен; `storage-ready` возвращает 503. Это безопасный отказ, а не успешно работающий rollback. Старый immutable image нельзя объявить исправным только по `/health`. Обычный откат сохраняет актуальные данные и требует совместимого reader. Восстановление старого snapshot — отдельный, явно выбранный сценарий recovery; оно не входит в обычный откат и не должно стирать новые задания/результаты.

Прогон новой интеграции выявил несовместимость унаследованного `process.execArgv` с Worker в Node 24 test runner. Worker теперь получает только аргументы module loader/preload (`--import`, `--require`, loader, conditions), сохраняя fault-injection preload и исключая процессные V8/TLS/runner-флаги. Production тест отказа SQLite ROLLBACK подтвердил, что preload действительно исполняется; отключения теста или обхода fault injection нет.

Проверено в release worktree:

- Backend World/Apps/Notes/server: **81 passed, 2 opt-in skipped** до отдельной root-правки TLS allow. Connect: **64 passed**.
- Production selftests: inference/model routing, connector store, durable protocol/lost ACK/replay/cancellation, persistence/crash windows/second owner/failed rollback, queued migration, readiness и route isolation — успешно.
- `agent:release:selftest`: сборка и настоящий сценарий установки новой версии с подтверждением supervisor restart — успешно. Выпуск connector **1.3.0**, SHA-256 `1b6bedbbcffb0c14fbc9f1be2581aa25b39a7b9a62d5b91226b1a0469e701033`. Проверка автообновления поддерживает minor/major границы с нулевым patch; проверенный переход synthetic 1.2.0 → 1.3.0. Runtime сверяется с итоговым manifest, а не с этим историческим значением при будущей пересборке.
- `connect:release:test`: **60 passed, 1 opt-in Docker skipped**, включая 15 новых tests rebase на реальном Git, сохранение sequence, отказ от older signed release через штатный controller, locks, race, pending recovery, image identity и прекращение до публикации config после записи обоих журналов.

Для смены pinned whole-app source добавлен `deploy/connect/rebase-host.mjs`: старые host/module locks, запрет pending recovery, повторные проверки старого/нового Git и module tree, реально запущенного image и revision labels, сохранение antirollback и неизменяемых старых журналов, атомарная публикация нового config последней. Он только подготавливает новую generation и не выполняет Docker mutations или переключение systemd. Эксплуатационные условия и отдельная проверка отсутствия активных заданий описаны в `deploy/connect/CONTROLLER.md`. Production deploy не выполнялся в рамках этого технического подзадания.
