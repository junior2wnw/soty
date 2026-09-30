# P3-D2 — независимая приёмка обсуждений приложений

Дата: 30 сентября 2026. Основание: принятый D1 `3794f01`, [центральный план](p3-engagement-plan.md) и [контракт D2](p3-discussion-contract.md).

**Итог: PASS в ограниченной локальной серверной области.** Независимые 20 тестов прошли, без пропусков. Проверены реальные SQLite, World authority, WebSocket claim, подписанный Connect HTTP и отдельные процессы. После read-only аудита и исправления найденной утечки длины курсора открытых существенных блокеров в этой области не осталось. Это не приёмка интерфейса D3/D4, промышленной нагрузки или публичного выпуска.

Рецензент изменил только два новых независимых тестовых файла и этот документ. Реализацию исправляли её владельцы; production, пользовательские данные и ранее оставленные временные каталоги не менялись.

## Воспроизводимый прогон

```text
node --test modules/apps/test/app-discussion.acceptance.test.mjs server/test/app-discussion-http.test.mjs
```

Последний прогон на Windows, Node24.13.1: **20/20 PASS, 0 FAIL, 0 SKIP**, 5805 ms. Из них 15 сценариев реального Apps/World service и 5 сценариев production HTTP composition. Стандартное предупреждение Node об экспериментальном `node:sqlite` не является ошибкой теста. Syntax checks обоих новых файлов также прошли.

Финальный прогон выполнен после FINAL MODEL FREEZE автора. Последнее уточнение независимого теста отдельно меняет domainId и replyTo при прежнем requestId; оба изменения конфликтуют. Модельные хеши совпадают с freeze.

## Что именно доказано

| Группа | Наблюдаемый результат |
| --- | --- |
| Первый пользователь и offline source | Новый Connect аккаунт пишет без скрытого создания World profile. Обсуждение работает после подтверждённого сервером отключения runtime. Подпись автора — минимальный snapshot; последующее изменение закрытого World profile не обогащает старое сообщение. |
| Неизменяемые аудитории | Restricted→public→restricted→public даёт четыре разных ID. Возврат к прежнему режиму не переиспользует историю. Настоящее изменение grants ротирует и публичный разговор; rename, одинаковые grants и publication no-op не ротируют. Вход через canonical с grant-допуском всё равно показывает `audience:public`, если разговор публичный. |
| Исторические закрытые права | Нужны current grant и original predicate. Старый direct-grant участник может читать архив, получив нынешний допуск через другую группу. Новый участник другой группы не получает этот архив. Вступление в первоначальную группу действует по согласованной динамической политике; выход и потеря publisher admin прекращают соответствующий допуск. Администратор World community не становится модератором app discussion. |
| Exact entry и административный доступ | Retired адрес запрещён даже при работающем соседнем alias. Нет переключения origin. Отдельный administrative owner context возвращает `entry:null`, позволяет читать/удалять после app revoke и запрещает отправку. Чужой actor и отозванная установка владельца не получают обхода. |
| Повтор и удаление | Accepted requestId глобален для аккаунта между apps/conversations. Exact retry после потери доступа возвращает прежнюю минимальную квитанцию, не private body/history. Изменение app/conversation/domain/path/body/reply с прежним ID конфликтует. Tombstone сохраняет fingerprint; retry не воскрешает тело. Автор удаляет своё известное сообщение без восстановления read-доступа. |
| Reply и разделение с World | Reply относится к тому же разговору и хранит ID, не копию текста. После redaction старый body не остаётся в reply projection. Настоящее сообщение World не появляется в App history. |
| Snapshot и изменения | Начальный cursor затем доставляет удаление сообщения, уже находящегося вне tail. History возвращает tombstone. Retention gap и restart дают явный `resetRequired`, не ложное «сообщений нет». |
| Cursor и byte budget | Cursor нельзя перенести к другому actor, устройству, разговору или entry path. На 50 сообщениях примерно по 12 KiB UTF-8 фактические context/history/changes JSON остаются ≤256 KiB; страницы не теряют и не дублируют сообщения. |
| Скрытые архивы | 210 недоступных материализованных разговоров не создают пустые continuation pages, totals или hints. Видимая страница содержит только разрешённые архивы. Отдельная регрессия проверяет постоянную длину курсора при невидимых пустых ротациях. |
| Квоты и сохранность | Trusted lower overrides проверяют head/conversation/message/body admission. Tombstones остаются занятыми identity slots. Полная квота не блокирует exact retry, read, own erase, owner redaction, restrict и revoke. Reopen с лимитом ниже уже записанного объёма не объявляет прежние данные corrupt. |
| Retention после снижения настройки | После window4→reopen с `changesRetained:1` прежние 4 события не очищаются простым чтением. Следующее допустимое удаление сокращает окно, а свежий cursor получает его tombstone. |
| Два Apps writer | Два отдельных процесса с настоящими SQLite/World services одновременно отправляют один intent: ровно одно сообщение, одинаковая квитанция, один первоначальный приём и один replay. |
| Signed HTTP и World revoke | Реальный client подписывает операции; второй client enrolment того же аккаунта читает и повторяет исходную отправку. Подписанный send удерживает Connect→World→Apps. Второй процесс World не может отозвать membership между разрешением и Apps COMMIT; после освобождения следующая операция запрещена, а собственный старый retry остаётся минимальным. |
| Отказ и неизвестный ACK | Искусственный сбой доверенного host callback после настоящего Apps COMMIT не откатывает сообщение; retry читает квитанцию. Настоящие занятые World/Apps SQLite возвращают HTTP503, Apps DB/WAL не меняются при отказе, после освобождения тот же intent принимается. Реальный burst получает HTTP429; replay/remove/revoke остаются доступны. |

Service fixture использует самостоятельные actors и проверку активности установки; доказательство реальной подписи, enrollment и HTTP mapping находится в отдельном HTTP файле. Настоящий WebSocket выполняет claim, но connector не запускает inference и не служит фиктивным доказательством доступности приложения.

## Выявленное и исправленное

### Длина зашифрованного курсора раскрывала разрядность скрытого поколения

Независимый тест оставлял неизменными два видимых архива, выполнял 105 пустых смен аудитории и повторял ту же видимую страницу. `entries` совпадали, но длина курсора менялась **223→225** символов: JSON с `upper/before` шифровался без выравнивания. Это небольшая утечка метаданных, а не утечка тела или точного числа сообщений; она противоречила условию не выдавать скрытые промежуточные поколения.

Автор исправил encoding: plaintext block ровно512 байт с проверкой размера; decoder принимает ровно540 байт IV+ciphertext+tag. Scope, проверка ACL и reset после restart сохранены. Независимый тест сначала был RED, после исправления — GREEN и включён в полный 20/20.

```text
node --test --test-name-pattern="opaque archive cursor size" modules/apps/test/app-discussion.acceptance.test.mjs
```

Padding закрывает именно продемонстрированный канал длины. Этот отчёт не утверждает constant-time ответы или отсутствие всех timing side channels.

### Исправление независимого fixture, а не продукта

Первый прогон был 18/19: fixture ожидал `offline` сразу после клиентского WebSocket close и видел промежуточное `unknown`. Добавлено ожидание обработки закрытия сервером, после которого прежний assertion `offline` остаётся обязательным. Поведение продукта не ослаблялось; эффект обсуждения при подтверждённом offline по-прежнему проверяется.

## Read-only аудит реализации

- `schema.mjs`: latest head сверяется с текущим normalized audience hash/owner; historical descriptor проверяется по собственному hash и FK. SQL guards запрещают изменение/удаление/replace исторической аудитории и message identity. Redaction монотонен. Counters сверяются с фактическими rows/body bytes; message sequence и change sequence учитывают tombstones, retained changes проверяются как непрерывный хвост. Lower runtime overrides не переписывают валидную прежнюю базу.
- `discussions.mjs`: ordinary access разрешает exact entry; administrative и own erase имеют отдельные узкие пути. Replay lookup и fingerprint идут до изменяемого ACL, но после действительной аутентификации. Reply projection повторно проверяет read. Архивы фильтруются целиком в bounded наборе, затем пагинируются; нет страницы-продолжения из одних скрытых строк. Обработка create-event читает текущее тело/tombstone, поэтому catch-up не возвращает уже удалённый текст.
- `engagement-transaction.mjs`: сохранён синхронный порядок Connect→World→Apps, однократный callback, повторная проверка actor после Apps BEGIN, temporary busy timeout100 с восстановлением прежнего значения, response byte check перед COMMIT. Ошибка внешнего host release после COMMIT является неизвестным ответом, не обещанием rollback.
- Root bridge: audience hook находится внутри транзакций publication update, init, source promotion, grants change и revoke. Same-audience изменения сравниваются по descriptor. World helper принимает только relevant IDs≤65536, работает только под fence, выполняет один SQL JOIN, не создаёт profile и не выдаётся как RPC. HTTP adapter явно отображает discussion/world busy в503 и discussion rate в429; прежние semantic RPC errors сохраняют native HTTP400 и typed code.

Отдельные результаты root, не пересчитанные как дополнительные независимые тесты: policy integration + World7/7, D1 saved/fence/actual HTTP regression34/34. Автор сообщил модельные36/36. Исторический Apps5→6 reader/migration/rollback gate принадлежит отдельному рецензенту и не подменяется этими20 сценариями.

## Точные байты проверенного среза

| Файл | SHA256 |
| --- | --- |
| `modules/apps/server/discussions.mjs` | `3709b3473759ff48ed76b07efd41861f5b8c81db9e3d48e02b2725d31f97f878` |
| `modules/apps/server/schema.mjs` | `0dba9ad9c9e0427f56bb34c207242680e247d9f39dc96a9852802309c6fa5786` |
| `modules/apps/server/engagement-transaction.mjs` | `c8d9ed2139d6dc1e8ca4a8c61ed0680a67c19fdc4eb690b94bafe13f1ffcaffe` |
| `modules/apps/server/index.mjs` | `14785f17c0bd897e5d8b76f1acbed7320959cbe4e7659a7e1d43222c880e247d` |
| `modules/apps/server/publications.mjs` | `b5f06fa320700ad676eb37289aa404586252895a020b8810fcb6e708410c26a3` |
| `modules/world/server/index.mjs` | `a9e25c612f0903ad018c3fae186980948c214047a7a57c6ea3816ab0678a6b05` |
| `modules/connect/server/http.mjs` | `5283d9af0c37620c70f74ba62013b85ec06f71b93305f90b608a0ca543e3d4cf` |
| `server/http-app.js` | `a041cd2afeb7afd933f645d7db94e7f9b588fb0c96fe5bcb68306cc44512924c` |
| `modules/apps/test/app-discussion.acceptance.test.mjs` | `868a7afe64f755da8b501b111af27fa16ed0fa55303220724d2a39e5fad39dc7` |
| `server/test/app-discussion-http.test.mjs` | `870ea29d1870821c6ccbe541bd75214b42bdeff1d4400bcb98abf674ea2d7b5d` |

Перед commit root убрал из HTTP test две пустые строки, включая лишнюю строку в конце файла, обнаруженную `git diff --cached --check`. Логика не менялась; `node --check` и staged whitespace check прошли. SHA итогового HTTP test: `bc79250a6975282347eca2a7d19c18e54cfbd4b0b0c28f32d60c994869a0f5e0`; таблица выше сохраняет байты независимого прогона.

## Незакрытые внешние границы

- D3/D4: реальный UI, сохранение локального draft/pending при reset, поздние ответы после account/route changes, focus, экранный диктор и мобильный цикл. Серверная переменная или память fixture не выдавались за проверку сохранности браузерного ввода.
- Полные defaults в1M messages/8192 heads/8192 conversations не заполнялись. Проверены настоящие эффекты с lower admission limits,210 скрытых архивов и ограниченные большие страницы. Это не производственный capacity/RAM/latency benchmark1000×64 и не гарантия hard disk cap: SQLite/WAL, индексы, snapshots и tombstones занимают место сверх live body quota.
- Перезапуск сбрасывает cursor key; несколько независимых server instances не имеют общего непрерывного cursor. Клиент обязан применять честный reset flow.
- Физическая потеря питания/диска, Linux Docker recovery, публичные DNS/TLS и эксплуатационная готовность остаются прежними отдельными gates. Сбой host callback после COMMIT проверяет application ACK semantics, не заменяет power-loss test.
- P3-E: ban, abuse handling, уведомления, прочая модерация и публичный admission не считаются реализованными. D2 не обещает анонимный чат, доступ внешнего агента без разрешения или публичный rollout.

Независимый D2 sign-off относится к этим исходникам и перечисленным серверным сценариям. Общий checkpoint и последующий выпуск остаются у root после общего regression gate.
