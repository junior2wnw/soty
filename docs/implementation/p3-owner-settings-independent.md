# P3-C1 — независимая приёмка настроек владельца

2026-09-30. Основание: B3 `3a710ac` и [контракт C1](p3-owner-settings-contract.md). **Backend и чистый frontend state gate: PASS в описанном ниже объёме.** Независимые файлы содержат14 серверных и13 клиентских успешных сценариев; связанный backend повтор inspection14 + source-observation3 + independent14 завершился31/31 PASS, без skips. DOM и browser gate ещё не приняты. Новых прав, источников исполнения или production-настроек этот документ не вводит.

Независимая область записи: `modules/apps/test/app-settings.acceptance.test.mjs`, дополнительно назначенный `src/world/app-settings.acceptance.test.mjs` и этот отчёт. Код авторов не изменяется. Browser gate выполняет root отдельно.

## Уточнения до реализации

- `apps.inspect` доступен только текущему владельцу, даже если другой аккаунт имеет grant либо приложение доступно анонимно. Проверка владельца предшествует source callback и проекции метаданных.
- `expectedAccountId` проходит уже существующий внешний guard `execute`, который проверяет и удаляет его до operation-specific exact-arguments validation. Это не новый аргумент внутреннего inspector.
- Предпросмотр не ограничен canonical: named-only конфигурация с активным допустимым alias сохраняет возможность явного запуска. При наличии canonical UI предпочитает его; каждый alias запускается с точным domain ID.
- У revoked app все разрешённые действия false и все `shareUrl` null, включая canonical. ID/origin/история адресов остаются фактами владения, а не обещанием работающей ссылки.
- Свежесть legacy observation: строго `now < observedAt + 45000`. На границе45s и позже — unknown с прежними `observedAt/freshUntil/evidence`. Offline не сохраняет наблюдение закрытого channel: оба времени null. Revoked — unknown/null. Connector-v1 не подтверждает revision кода, успешную пользовательскую задачу либо DNS/TLS.
- Финальное additive поле `checkedAt` берётся из серверных часов после синхронного observer. Проверено с внедрёнными часами: на44,999ms остаётся1ms свежести, повторный inspect не начинает новые45s. Чистый клиентский helper дополнительно проверен на вычитание полной длительности запроса; DOM countdown с monotonic часами остаётся browser/UI gate.
- Небезопасный старый entryPath не уничтожает весь inspect: источник остаётся видимым, ссылки null, preview недоступен. Редактирование имени и закрытие publication остаются разрешены; нельзя лишать владельца аварийного restrict/deactivate из-за неисправного пути.
- Name-only update сохраняет отсутствующие в команде grants, даже если владелец больше не администрирует ранее выданное сообщество. Явная отправка grants требует текущих полномочий. CAS проверяется внутри записи; конфликт не меняет имя, grants, revision или policy epoch.
- `invalidateAccess` уже перепроверяет и сохраняет действующие holders; это не безусловное закрытие потоков. Первоначальное предположение reviewer о закрытии после любого rename было неверно. Приёмка проверит наблюдаемый результат: rename при неизменных правах сохраняет допуск, реальная потеря прав его закрывает.

## Независимые сценарии

| Группа | Вход и возмущение | Наблюдаемый инвариант | Статус |
| --- | --- | --- | --- |
| Owner privacy | Текущий owner, чужой owner, допущенный member, посетитель публичного app и отозванный actor запрашивают один app | Только owner получает grants/source/aliases; чужой account и неверный expectedAccountId отказывают. Source callback privacy дополнительно проверена повторённым inspection suite | pass: service guard; signed HTTP отдельно |
| Согласованное чтение | Второй настоящий SQLite service меняет name, alias/квоту и publication между SELECTs inspector | Весь старый inspect, включая grants/адреса/квоты, остаётся одним WAL snapshot; следующее чтение видит новые согласованные данные | pass |
| CAS name/grants | Два Worker writers одновременно освобождаются barrier и записывают одинаковый expectedRevision | Ровно одна запись; проигравший получает app_revision_conflict. Имя/grants не смешиваются; revision и epoch меняются только по принятому намерению | pass |
| Старые community grants | Владелец потерял admin ранее допущенного сообщества; меняет только имя, затем явно отправляет прежние grants | Rename сохраняет grants и epoch. Явная неподтверждённая выдача прав отказывает без записи | pass |
| Непрерывный доступ | Выданный ticket и настоящий WS echo переживают rename; затем grants удаляются | Старый ticket всё ещё обменивается, WS отвечает после rename; изменение прав закрывает WS и отвергает прежнюю cookie | pass |
| Freshness | Production connector получает HTTP404/ошибку probe; часы44,999/45,000ms; disconnect/reconnect с удержанным новым probe | responding/unreachable истекают; checkedAt и клиентский remaining helper не продлевают свидетельство. Новый channel до ответа unknown; offline имеет null-время | pass: API/transport/helper; DOM countdown pending |
| Адреса и hash-SPA | Canonical/private alias/active anyone alias/inactive/retired/revoked, entryPath `/#/dashboard` и `/board?tag=a%2Bb#item` | Закрытые ссылки сохраняют полный encoded path; active public — origin+полный path. Inactive503/retired410 на настоящем HTTP; revoked links null | pass: DTO/HTTP; Clipboard pending |
| Named-only и отключённые claims | Canonical отсутствует, active alias существует; второй service отключает новые claims при retained zone | Preview action остаётся, claimOrigin null/canReserveName false, реальная claim-команда отказывает | pass |
| Повреждённый прежний путь | Реальная register допускает historical `/x/..//not-a-safe-launch`, который runtimePath не принимает | Inspect/редактирование доступны, ссылок/preview нет; emergency restricted с listed=false проходит | pass |
| Частичный успех | Claim принят, publication конфликтует; новый claim уже публичного app | Адрес остаётся bound и не публикуется сам. После lost claim ACK клиент повторяет exact request, не создавая второго имени; UI-отображение отдельно | pass: API/client state; DOM pending |
| Lost ACK и история | Повтор принятого envelope после restrict/revoke, затем после64 новых receipts | Historical anyone receipt не заменяет current restricted/revoked. После prune старый CAS отказывает; клиент сохраняет исходный pending без нового ID/epoch | pass: backend/client state |
| Durable client intent | Deferred locks/API, remount, смена account, два экземпляра state, поздний ACK после нового pending | Storage failure запрещает dispatch. Повтор использует тот же envelope/ID. Поздний успех возвращает stale без response и закрывает лишь matching старый pending; новый pending сохраняется | pass: pure state/real service; browser lifecycle pending |
| Emergency restrict | Existing listed=true/anyone и неисправный источник; владелец закрывает публичность | Backend принимает restricted/listed=false без preview. Клиентский helper формирует именно этот payload с прежними target/CAS pins; явное согласие требуется для anyone | pass: API/helper; DOM pending |

## Способ проверки и границы доказательств

Новый acceptance harness создаёт собственную временную SQLite базу и приложения через настоящие service operations, включая connector claim/register. Используется настоящий `createLocalAppsRuntime`, loopback HTTP404 process и WebSocket echo server. Production connector выполняет HEAD, а не подставной callback готовности. При reconnect новый HEAD намеренно удерживается до проверки unknown. Эффект45s проверяется внедрёнными серверными часами; это не45s физического ожидания или измерение мобильной памяти.

Конкуренция воспроизведена двумя настоящими Worker с отдельными Apps service/SQLite connections, одновременно отпущенными shared barrier. Отдельный WAL snapshot сценарий записывает новые name/alias/publication вторым service между SELECTs первого inspector. Для этого read-model подключён к настоящим registry на том же SQLite connection; авторский fixture не импортируется.

Service actorActive/membership callbacks используют синтетические идентификаторы. Внешний expectedAccountId guard реально вызывается, однако криптографическая подпись Connect/его HTTP transport этим fixture не проверяется. Для ticket/session/app HTTP/WS используется настоящая сеть. Production данные, пользовательские credentials и браузер не используются.

Client-state acceptance импортирует фактический замороженный `app-settings-state.mjs`. Его отдельная fixture поднимает собственный Apps service/SQLite и реальный WS channel для claim/register; затем вызывает настоящее service API для publication/claim/retire. Receipts не собраны вручную и не заимствованы из авторских fixtures. Передача этих управляющих команд здесь — прямой вызов service, не подписанный Connect HTTP. Сеть намеренно представлена controllable API promises после фактического commit; хранилище — синхронный Map adapter с квотой/отказом чтения, locks — очередь с управляемым barrier. Это проверяет persisted-before-dispatch и конкретные before/after-await границы, но не DOM, настоящий browser Web Locks/LocalStorage, Clipboard или память устройства.

Root отдельно проверяет:320px/desktop, клавиатуру и focus, Back/закрытие/возвращение, реальную выдачу имени и аудитории, clipboard fallback, частичный успех, повтор после потери ответа, private/named preview на действительном QA connector. Полный C1 gate не доказывает C2 source switch, постоянный hosting, каталог, app conversations, публичный DNS/TLS или production rollout.

## Независимые клиентские результаты

13/13 тестов чистого state прошли на настоящих service receipts:

1. Принятый publication с потерянным ACK сохраняет полный pending. Новый state на том же storage повторяет тот же op/args/requestId. После другого restrict результат содержит historical anyone receipt и current restricted отдельно.
2. Два state экземпляра одного account/app, отпущенные через lock barrier, не записывают два несовместимых pending. Повтор идентичного намерения использует прежний ID.
3. Старый accepted ACK после explicit abandon и нового pending возвращает superseded, не стирая новую команду.
4. Смена аккаунта во время ожидания local lock приводит к0 API calls. После уже committed запроса matching ACK может закрыть свой старый scoped pending, но результат stale не содержит response для отображения новому аккаунту.
5. Отказ storage write/read либо отсутствие lock исключает dispatch. Отказ записи ACK после серверного commit оставляет старый pending; повтор получает replay с прежним ID.
6. Девять искажений реального receipt — requestId/hash/app/epoch/active IDs/exposure digest/target digest/current epoch/current app — не очищают pending. Настоящий ответ после них принимается.
7. Lost claim ACK не создаёт второе имя после remount. Последующий publication conflict не удаляет уже сохранённый адрес и не перебазирует pending.
8. Lost retire ACK повторяется после remount; domain revision увеличивается ровно один раз, tombstone остаётся.
9. После64 более новых publication receipts старый client retry получает CAS conflict и сохраняет исходный pending/локальную revision; новых ID/epoch/publication эффектов нет.
10. Новое серверное наблюдение не стирает редактируемые name/grants/publication consent/slug и не меняет их CAS bases. Реальные stale update/publication команды отказывают; только explicit reset принимает текущую версию.
11. ACK прежнего имени не стирает более новый введённый текст. Name payload не посылает grants; grants payload сохраняет существующие личные account grants.
12. Anyone требует explicit exposure confirmation. Переход с listed public на restricted формирует listed=false, сохраняет target/epoch pins и действительно принимается сервером.
13. Freshness helper использует server checkedAt минус полный request elapsed, возвращает1ms перед истечением и0 на границе; unknown/offline/невалидное elapsed не продлеваются.

Отдельные lifecycle findings коллеги — acknowledgement как ложный no-op dirty и выбранный retired alias без видимого разрешения конфликта — исправлены автором до этого freeze. Эти два сценария принадлежат его helper/UI review и здесь не выдаются за собственную независимую браузерную проверку. `dispatchAppSettingsIntent` может бросить late rejection до финального isCurrent; защита DOM/toasts в catch остаётся обязанностью view и проверяется отдельно.

## Исполняемые результаты

```text
node --test modules/apps/test/app-settings.acceptance.test.mjs
14 tests; 14 pass; 0 fail; 0 skip

node --test modules/apps/test/app-settings.acceptance.test.mjs modules/apps/test/app-inspection.test.mjs modules/apps/test/source-observation.test.mjs
31 tests; 31 pass; 0 fail; 0 skip; 2.39s

node --test src/world/app-settings.acceptance.test.mjs
13 tests; 13 pass; 0 fail; 0 skip; 1.36s
```

Во время разработки собственного harness исправлены две ошибки теста: дополнительный SQLite connection закрывался после попытки удалить его Windows-каталог; сравнение активного alias использовало позицию в списке при одинаковом timestamp вместо stable domain ID. Это ошибки fixture, не implementation findings. Повтор после исправлений зелёный.

Квитанция отклонённой очистки временной fixture:

- Точный путь: `C:\Users\Junio\AppData\Local\Temp\soty-settings-independent-ta3z3p`.
- Запрошенная операция: однократное рекурсивное удаление именно этого каталога после проверки абсолютного целевого пути. Исходная строка shell-команды не сохранена в доступном сокращённом контексте; её буквальное написание здесь не реконструируется.
- Ответ автоматической проверки разрешений: `blocked by policy`. Более подробной причины в доступном результате не было.
- Каталог создан независимым тестовым harness для синтетической SQLite fixture; пользовательские данные и credentials в него не помещались. Он не является экспортным артефактом проекта и не используется последующими проверками.
- После отказа удаление не повторялось, способы обхода не искались. Каталог оставлен на месте; существование/содержимое заново ради этой квитанции не проверялись.

Проверенный backend-срез:

| Файл | SHA-256 |
| --- | --- |
| `modules/apps/server/index.mjs` | `f4287cda116cef970276c1edd7d4b97beabdaec116065eac1d02c544173c07e6` |
| `modules/apps/server/inspection.mjs` | `5961855a4cb60b58c69f3f57799580a180dec9cb1cdf706666d0fbc854d6d029` |
| `modules/apps/server/source-observation.mjs` | `e4849936144c251bce7a3708e7b031414cc918b0a3d8bc2736b87bb95e5e9dfb` |
| `modules/apps/server/protocol.mjs` | `445c092e6243e190dd4e166c2ea1554ec42f0afb61db45cd6e80191d6774dd0f` |
| `modules/apps/test/app-settings.acceptance.test.mjs` | `9ae95442e0005dbf56204d49df7ab230eb24ea51e0fe26dd8d66ba81b9b16862` |
| `src/world/app-settings-state.mjs` | `9ebb0e2db8191c3f84e83e8a14c4f489fd95c415ceda34c5189805ce843299d7` |
| `src/world/app-settings.acceptance.test.mjs` | `5cba3bcfa4c1434f0c00f0ca7d063fefef3c4387cd6478375b9486b6b81150b7` |

## Итог

**Backend и чистый state C1 приняты в указанной локальной границе:14/14 независимых серверных,13/13 независимых клиентских; backend related31/31.** После согласованных уточнений новых material blockers этих областей не обнаружено. **DOM lifecycle, UI/Clipboard/keyboard и browser gate pending**; C1 целиком ещё не принят. Результат не подтверждает C2, hosting, каталог, production DNS/TLS или release/restore.
