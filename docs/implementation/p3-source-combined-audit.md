# P3-C2-D — совместная проверка смены источника

Дата: 2026-09-30. Основание: C2-A `efe0d9f`, C2-B `5a4cb1d`, C2-C `60876fb` на хэшах ниже. Рабочая копия — `soty-platform`.

**Вывод: в проверенной области C2 остающихся материальных блокеров не найдено.** Неизменяемая конфигурация, текущая привязка устройства, серверное переключение и клиентское подтверждение образуют согласованный путь. Это локальный checkpoint смены источника, а не разрешение production-выпуска или утверждение о готовности всей платформы.

Этот проход — чтение текущего кода, его diff и сопоставление уже выполненных проверок. Автор документа участвовал в реализации модели, manager и клиентского состояния, поэтому не называет повторное чтение своих файлов независимой приёмкой. Независимые результаты принадлежат отдельным рецензентам; настоящий браузер и общий прогон — интегратору. Новых функций или изменений исходников в C2-D не внесено; повтор всех прежних запусков не выполнялся.

## Сквозные инварианты

| Граница | Проверенное поведение и место реализации |
| --- | --- |
| Постоянная конфигурация | Digest связывает app, revision, owner, connector, port, raw entryPath и profile. UPDATE/DELETE/REPLACE не меняют сохранённый target; source head нельзя понизить или удалить. `modules/apps/server/schema.mjs:169`, `:178`, `:183`. Это идентичность маршрута, не хэш кода или данных приложения. |
| Миграция | Genuine Apps3 с единственным target1 получает head1; неподдерживаемая noninitial история Apps3 отклоняется до записи. Повторное открытие Apps4 проверяет данные, не восстанавливает отсутствующую head как1. `schema.mjs:217`, `:294`. Reader дополнительно проверяет формат перед START, но не заменяет проверку строк сервисом. |
| Активный источник | Рабочий маршрут читается через `app_publications.active_target_revision`, а не исторические поля `local_apps`. Register и promote проверяют занятость connector+port внутри write transaction. `index.mjs:57`, `:304`; `sources.mjs:68`, `:185`. |
| Подготовка | Выбор, actor и target захвачены до await; HEAD/ACK выполняются вне SQLite write transaction. После await повторяются actor, CAS, текущий канал и срок. Подготовка ничего не публикует и не направляет пользовательский трафик к кандидату. `sources.mjs:132`; `runtime-bindings.mjs:240`, `:259`. |
| Подтверждение переключения | Owner проверяется до lookup receipt. Retained receipt ищется до transient preparation, CAS и expiry. Новая операция атомарно меняет target pointer, floor2, epoch, политику/точное whole-port согласие и receipt; затем снова проверяет authority/proof до COMMIT. `sources.mjs:185`; `publications.mjs:194`. |
| COMMIT и подключение | Успешный COMMIT не означает, что новое устройство уже приняло маршрут. Callback перечитывает действующий target, закрывает устаревшие допуски/потоки и согласовывает bindings; до точного нового ACK выдаётся отказ/ожидание, без fallback к старому upstream. Replay старого receipt тоже согласовывает **текущий** маршрут. `index.mjs:64`, `:177`, `:243`; `runtime-bindings.mjs:210`. |
| Точное соединение | Reference связан с actual channel, socket, channelId, syncId и pins. После reconnect и A→B→A старые ACK/remove/open не приобретают силу снова. Connector проверяет свой captured context и binding до и после асинхронной работы. `runtime-bindings.mjs:38`, `:138`, `:162`, `:218`; `scripts/agent-modules/local-apps.mjs:22`, `:165`, `:208`. |
| Readmodel | Negotiated version, ACK маршрута и свежий HTTP ответ показаны как разные факты. Inspection передаёт точный текущий target в `getState`; ACK предыдущего target не превращает новый в bound. Прежнее наблюдение также не подходит к новым revision/digest. `index.mjs:143`, `:151`; `inspection.mjs:104`; `runtime-bindings.mjs:228`. |
| Intent браузера | Один существующий account/app-scoped pending slot и Web Lock обслуживают C1 и source.promote. На диск попадают exact args и ограниченный expectedSource, без proof/readiness/TTL. Перед первой записью fresh proof повторно проверяется внутри lock; сети под lock нет. `src/world/app-settings-state.mjs:102`, `:132`. |
| Повтор и квитанция | Retry требует именно показанный `expectedPending`; внутри lock сравнивается вся нормализованная запись. Отсутствие/замена дают ноль API-вызовов. Receipt сопоставляется с request/preparation hashes, CAS, target pins, floor, политикой и consent до удаления pending. Исторический receipt и новый current не обязаны указывать на один target. `app-settings-state.mjs:64`, `:153`, `:186`. |
| История и черновики | История — одна страница20 неизменяемых конфигураций с ограниченным cursor, не журнал резервных копий. Автоматический observe не отменяет независимое чтение истории; explicit reset/dispose отменяют. Новые epoch/pins сохраняют dirty draft с конфликтом, не перебазируют его. `app-source-state.mjs:128`, `:158`, `:229`; `app-settings-state.mjs:247`. |

## Сочетания, важные для пользователя

1. **Legacy приложение без переключения.** Head1 продолжает работать через connector v1. Новое source.prepare требует v2. Список устройств не выдаёт legacy observation за подтверждение revision; интерфейс объясняет необходимость обновить коннектор. Уже существующие адреса, имя, grants и чат не мигрируют в новую сущность.
2. **Первое переключение и возврат к target1.** Оба меняют policy epoch; первое повышает floor до2, возврат не понижает его. Legacy sync исключает head2 (`index.mjs:255`), runtime отказывает на неподходящем канале. Исторический target1 после возврата получает fresh syncId и новое публичное согласие, если остаётся anyone.
3. **Временный offline или legacy reconnect после переключения.** Транспортный отказ не уничтожает ещё действующую DB-authority cookie/ticket. После возвращения v2 та же авторизация работает при неизменных правах/epoch. Реальный отзыв, смена epoch/target/floor по-прежнему инвалидируют её; retained session не разрешает HTTP без текущего binding. Это разделение закреплено `index.mjs:177`, `:198` и независимым сетевым тестом.
4. **Другой законный SQLite writer.** Пока старый процесс ещё держит ACK A, inspection уже показывает committed B как pending/unknown; запуск и HTTP к A не разрешаются. Callback и heartbeat согласовывают актуальную БД без сброса ACK соседнего неизменившегося приложения. Немедленного уведомления всех процессов при partition здесь не обещается.
5. **Name-only против изменения доступа.** Имя не конфликтует с source policy CAS. Grants/revoke/retire активного alias меняют общий epoch и отклоняют устаревшее переключение. Dirty C1 publication блокирует только новое source действие; узкий resetPublication сохраняет name/groups/slug и source выбор. Retained pending retry не зависит от нового черновика публикации.
6. **Потерянный ответ, expiry или перезапуск.** Уже отправленный exact intent повторяется без новой подготовки. Если receipt ещё сохранён, возвращается прежний факт и отдельный current; новый switch не исполняется. После prune последних64 receipts исходный CAS не перебазируется автоматически. Если первая операция вообще не была принята, истёкшая ephemeral preparation не восстанавливается из localStorage.
7. **Две вкладки и поздний ACK.** Кнопка повторяет показанную запись, а не последнюю случайную запись slot. Поздний receipt A не удаляет B; несовпавший receipt не очищает pending. Stale account/dispose не отображает ответ в другом окне/аккаунте. Local abandon только завершает локальную проверку и не отменяет возможный серверный COMMIT.
8. **Долгое чтение согласия.** Истёкшая карточка остаётся читаемой. Обычная «Проверить» не переключает. Явное «Перепроверить и переключить» получает новый preparationId и возвращает intent только при прежних full tuple/pins/CAS/audience/listed/consent и текущем поколении. Drift/hidden/edit/отказ — новый просмотр или stale, без promotion. Это конкретное улучшение взаимодействия, не декларация полной WCAG AA.
9. **История после собственного или внешнего изменения.** Финальный `app-settings.ts:158` перечитывает открытую историю только после принятого source receipt и успешного inspection. Прямое действие `:231` делает inspection→history без reset/rebase; `:575` запрещает выбор stale/current строки. Readback/history failure не возвращает подтверждённый COMMIT в pending. C1 observe получает completion только для реально завершённого C1 действия; остальные name/grants/publication drafts остаются прежними.
10. **Запущенное приложение после смены источника.** Toolbar обновляет метаданные без пересоздания iframe (`src/world/app.ts:1195`); прежние потоки прекращаются сервером, а новое открытие остаётся явным действием. Возврат к A показывает данные, которые продолжал хранить сам A. Это не перенос данных A→B и не восстановление резервной копии.

## Доказательства и исправленные дефекты

Числа ниже относятся к своим срезам и пересекающимся наборам. Их нельзя складывать в число уникальных тестов.

| Срез | Полученное свидетельство | Граница |
| --- | --- | --- |
| C2-A | [Авторская модель](p3-source-model.md):78/78. [Независимая модель](p3-source-model-independent.md):36/36, в том числе12 новых независимых случаев. | Настоящая SQLite, fault rollback, два writer, reopen/retention; proof-provider тогда synthetic, не сетевой ACK. |
| Apps4 reader | [Отдельный gate](p3-apps-v4-reader.md): полный164/162 pass/2 skip; старый настоящий Apps3 migrator отклоняет main3+WAL4 без изменения bytes. | Docker lifecycle synthetic; этот результат не Linux exact-image/backup restore proof. |
| C2-B | [Интеграция](p3-source-runtime-integration.md), [wiring review](p3-source-wiring-review.md), [независимая сеть](p3-source-runtime-independent.md): signed HTTP3, channel4, независимые service+connector9 и standalone bundle1. | Настоящие локальные HTTP/WS/SQLite и exact standalone bytes; физические устройства и публичный TLS не проверены. |
| C2-C состояние/сеть | [Независимый gate](p3-source-ui-independent.md):49/49; авторские source+C1 helper42/42; итоговый составной повтор интегратора91/91. | В независимых client тестах реальный service/connector/receipts, но управляемые storage/locks/clock. |
| C2-C браузер | [Журнал root](p3-source-settings-browser.md) и финальный receipt интегратора: публичное A→B, возврат B→A после expiry, недоступный порт, narrow reset, конфликт двух окон с сохранением имени, private A→B и автоистория3 rows без stale. Component fixture10/10;320×760 и667×375 без горизонтального overflow. | Браузер реальный; fixture synthetic и отдельно от реальных upstream опытов. Не телефон, screen reader или установленная PWA. |
| Финальная сборка | По финальному receipt root после history regression: world598 total/594 pass/0 fail/4 явных прежних skip; typecheck/build PASS на UI4c5e893. | Пропуски не считаются пройденными проверками; общий gate выполнял интегратор, не автор этого документа. |

Зелёные ранние прогоны не скрывают найденные позже дефекты:

- **Retention после legacy reconnect.** Реальный RED был200→503→403 той же cookie при неизменном epoch; после отделения transport от DB authority —200→503→200. Существующий ticket также покрыт постоянным независимым regression. Подробности — wiring review.
- **Подмена повторяемого pending другой вкладкой.** Реальный store/service probe отправлял B по кнопке A. Теперь mandatory expectedPending проверяется под lock до API; независимые tests и обе браузерные retry-кнопки проверены.
- **Неточные scalar shapes.** Массив с единственной валидной digest/ID строкой проходил RegExp coercion. Нормализаторы требуют строку; неверный descriptor/receipt не записывает intent и не снимает pending.
- **Пустая история после reopen.** Реальный браузер и RED helper probe показали, что обычный render→clean observe отменял in-flight history. `resetDraft` отделён от explicit reset. Авторский regression и независимый held-response service scenario проходят; root повторил initial/latest историю в браузере.
- **Устаревший процессный текст и открытая история после COMMIT.** Финальная UI-ветка завершает readback после успешного inspection и отдельно перечитывает открытую историю. Ошибка inspection оставляет честное подтверждение COMMIT; последующее чтение завершает сообщение. Эти последние два component сценария входят в финальные10/10.

Новый дополнительный RED finding в текущем C2-D проходе не получен. Root отдельно отметил небольшой C1 текстовый долг: после принятого обычного publication update может остаться «Читаем состояние». Actual effect/current state верны; это не source failure и не основание повторить команду. Его исправление оставлено следующему точечному UI-проходу.

## Оставшиеся границы

- Immutable target фиксирует маршрут. HEAD200–499 означает ограниченный ответ выбранного loopback endpoint, включая401/404; это не attestation неизменности программы, безопасность её API или функциональная готовность всех страниц.
- Unsent draft хранится в памяти окна. Guard ухода и beforeunload не обещают восстановление после kill/crash. Отправленный intent сохранён отдельно; очистка браузерного хранилища самим пользователем уничтожает локальную запись.
- Подготовка живёт в памяти процесса и текущего канала30s. После reload/reconnect без retained successful receipt нужна явная новая проверка. Старый prepared proof нельзя воскресить по адресу или состоянию UI.
- История конфигураций не удаляется автоматически и не ограничивается lifetime quota; чтение страниц ограничено. Это не доказательство эксплуатационной ёмкости для миллиардов приложений. Receipts ограничены последними64 source epochs на app.
- Публичная runtime зона, DNS/TLS/site isolation, Apps4-compatible candidate и fallback, управляемая остановка старого writer, свежий backup/изолированное restore и exact-image Linux Apps4 read-only WAL probe остаются отдельными release gates. Ранее проверенный Rooms canary их не заменяет.
- Проверки не обещают отсутствие произвольного стороннего writer на host, мгновенную остановку уже отправленных внешних действий при partition, перенос проектных данных или full browser/device matrix. В проверенной области нет автоматического возврата на старый источник ради сокрытия ошибки.

## Достаточный финальный набор для C2-C

1. Exact helper/receipt/retry/history regression + independent actual service/readmodel/C1 acceptance: **42+49 PASS**, итог91. Покрывает последние history и strict-shape изменения без повторного запуска всех старых модельных fixtures.
2. Общий world после последней history правки: **598/594/0/4**; typecheck/build — **PASS**. Проверка current readmodel и прежнего v1/v2 runtime входит в общий набор и независимые13 network/readmodel случаев.
3. Финальный UI на одном frozen source: **10/10 component**, реальные A→B→A, two-window CAS, explicit retry/review и узкие viewport проверки — **пройдены root**. Автоистория не подтверждается одной проверкой типов.
4. Сверка diff и хэшей — выполнена в этом read-only проходе. `schema.mjs`, `sources.mjs`, `publications.mjs`, `scripts/agent-modules/local-apps.mjs`, `storage-probe.mjs`, `storage-guard.mjs`, корневой Dockerfile относительно `5a4cb1d` не изменены. Новый прогон всего deploy reader/сборки connector только ради C2-C не нужен; прежние внешние gates остаются открыты.

## Зафиксированный source

Файловые SHA256 проверены этим проходом; последующее CRLF/LF преобразование Git может менять файловый hash.

| Файл | SHA256 |
| --- | --- |
| `src/world/app-source-state.mjs` | `fd5cdb8c8006a8b1ccea5bf940975175623171dde0af1083ed8535ad6507b744` |
| `src/world/app-settings-state.mjs` | `aa1f1da6285256221ea4fa7f72f28d7c9f64d5e2137ec4b3548400eaf2acf142` |
| `src/world/app-settings.ts` | `4c5e89349568a5ec417b1e3428f16c56eaa62eaeb151272d7da5b6e5d82c9893` |
| `src/world/app-settings.css` | `7333588152f528ea113d9503d5d4798e69ac5d27ae5bbf2e5a00e0bafb3a2ae1` |
| `modules/apps/server/index.mjs` | `2306e9341c422c5900e2f3c627b2f3c9e51d97209a61861795b975d1022364cc` |
| `modules/apps/server/inspection.mjs` | `3c0aea1020d117a9aea7c0de32364f773908e14f0ce0591146195d8a2b5b5684` |
| `modules/apps/server/runtime-bindings.mjs` | `bcad87d0b44c0bf35381d3cb32861330f6238c6a79ab3f404867979e2fa58695` |
