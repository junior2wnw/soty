# P4-B1 — независимый review хранения и восстановления

Дата: 2026-09-30. База: P4-A `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e`. Это приёмка **контракта для реализации**, а не выполненной Notes2/Capabilities2 migration или внешнего native API. Production, исторические fixtures и авторские tests reviewer не менял. Тяжёлые suites не запускал.

После перечисленных ниже согласованных поправок новых блокеров самого дизайна не осталось. Можно реализовывать additive schema и изолированные проверки. Для разрешения v2-записи и native execution остаются отдельные обязательные gates; label, отключённый handler и зелёный существующий world suite их не заменяют.

## 1. Что проверено

Прочитаны `p4-external-agent-preflight.md`, `p4-storage-guard-preflight.md`, `p4-native-storage-contract.md`, master §12, текущие Notes schema/domain/validation, Capabilities schema/access/invocations/index, синхронная signed Connect composition, storage probe/guard, оба rollout/recovery пути и механизм cold backup. Дополнительно проверены узкие root изменения WS lifecycle, связанных tests и minimum runtime.

Фиксация проверенных контрактов:

| Файл | SHA256 |
| --- | --- |
| `p4-native-storage-contract.md` | `f218a909dae863c43a4081f081843f1c7a92d0ae03ab05ed5f130b8083f859c4` |
| `p4-storage-guard-preflight.md` | `e932530b7d908a05e7b938604ff27717328747d36cb93fa69a3a50cb4b7c4a83` |
| `p4-runtime-baseline.md` | `c844c939ee936d5897dd0f5a12c985b49252ec83e03067c7d613c97a7dfc743c` |

Новые domain SQL/API в первом файле остаются proposal: миграция, native effect и crash recovery этим review не исполнены. Bridge реализация началась параллельно; её новая локальная suite и Linux-проверка не входят в данный verdict.

## 2. Найденные разрывы и согласованные решения

### Первый bridge тоже может лишить штатного rollback

Оба текущих controller возвращают **тот же старый container/image**. Новый strict manifest v3 отклонит старый image с label v2 даже при Notes1/Capabilities1. Проверка только candidate позволяла остановить serving writer, после чего candidate failure оставлял бы недопустимый автоматический fallback.

Root принял обязательный preflight actual old image до STOP и иных serving mutations. В прочитанном root diff обеих orchestration зон exact old image ID и reader declaration проверены заранее; fresh полный `guardStart(old, {running:true})` добавлен непосредственно перед STOP. Это правильный порядок по source review. Его новые executable bridge tests здесь не объявляются пройденными.

Первый production переход требует отдельно проверенного bootstrap/recovery route. Переклеить label старому образу или сослаться на другой собранный fallback недостаточно. Второй, отдельный переход — от bridge `[1]` к реально serving reader `[1,2]` с default-off migration; только затем разрешается writer2. Ранний reader2 без effect reconciler может безопасно удержать незавершённое, но не обещает закончить восстановление.

### Durable marker должен менять все пути settlement

В текущем Caps `pending + no job + no effect` используется как доказательство отсутствия dispatch. Если native marker либо `dispatching` впервые записан в транзакции, которая откатилась после Notes COMMIT, generic cancel/revoke способен вернуть budget за реально созданную записку.

В §6 теперь закреплены отдельный **Caps COMMIT до Notes**, одновременно `started_at` и `dispatching`, и запрет generic shortcuts для native intent во всех `requestCancel`, `reconcileAuthorization`, `recordResult`/settlement и job dispatch путях. До marker cancel тоже обязан атомарно сохранить terminal receipt и удалить live dispatch input. Это нельзя реализовать только обёрткой нового executor.

### Lost ACK не должен исчезать после operational disable

Текущий `invocations.admit` сначала вызывает invoke authorization, включающую `executionEnabled`, и лишь потом ищет client key. Уже созданная записка + потерянный ответ + выключенный handler оставляли бы клиента без Invocation ID и без exact retry результата.

Native ordering уточнён: current actor/read ACL, account/client scope и полный fingerprint → existing key → только для **нового** intent execution readiness и новые quota/rate checks. Disabled handler не запускает unresolved intent; replay возвращает прежний Invocation/receipt. Отозванный caller по-прежнему не получает внешнюю историю. Terminal failed/cancelled никогда не переоткрывается при возвращении grant/лимита/handler; новая сознательная попытка требует нового key.

### Старый отказ по version не равен отсутствию записи

Оба исторических migrator меняют journal pragmas до проверки неизвестной версии; Notes также начинает write transaction. Последнее закрытие RW connection может checkpoint/cleanup. Поэтому нужен реальный before/after main/WAL/sidecar опыт на exact historical code, а главным барьером служит запрет START до исполнения старого образа.

Новые storage/native контракты правильно разделяют это. В исходном external preflight §7 оставались формулировки о произвольном prebuilt fallback и безусловном byte-unchanged old refusal. Root исправил обе после finding; итоговый текст независимо перечитан и согласуется с новыми контрактами. Исторический код нельзя переписать ради обещания отсутствия записи.

## 3. Минимальный целостный storage контракт

- Bridge manifest/probe/start receipt — strict v3 с **четырьмя независимыми** readers: Rooms `[1,2]`, Apps `[1,2,3,4,5,6]`, Notes `[1]`, Capabilities `[1]`. Ни label2, ни старый helper/START receipt не повышаются автоматически. Unknown/extra fields и future Notes/Cap2 на bridge не принимаются. Connect/World остаются отдельными startup formats и входят в согласованный backup.
- RO probe использует pinned host source, exact runtime image, обычное чтение committed WAL, проверенную standard local-volume topology. `immutable`, candidate imports, repair или вывод content/credentials в receipt недопустимы. `LIMIT 0` и known objects проверяют формат, а не целостность всех rows, FTS, counters или cross-store effects.
- `allowNativeMigration=false` действительно создаёт/оставляет v1. Совместимый reader открывает уже существующий v2 без downgrade, автоматически не чинит partial schema и не генерирует отсутствующий registry ID. Unknown формат распознаётся до persistent pragmas. Под своим write lock migrator повторяет admission и postconditions.
- Неполная пара Notes2/Caps1 или Notes1/Caps2 после crash допускает обычные функции, но native недоступен. Ни одна DB не изображает общий commit второй. Project ID в Caps — новое trusted связывание исторической Caps1, не доказательство её неизвестного прошлого происхождения.
- Notes proof и Cap native binding добавляются отдельными таблицами. Прежний v1 DDL и обычные Notes receipts не переписываются. SQL guards закрывают update/delete/REPLACE критических identities/proofs/receipts; recognizer проверяет точные guards и row invariants, а не только имена таблиц.
- Stable IDs закреплены в admission: `(capRegistryId, invocationId)`, Notes store ID, account, original input/contract digests и note/mutation IDs. Proof сохраняется в одной Notes transaction с созданием и переживает обычные 32 receipts, изменения и purge. Replay не вызывает Notes.get и не восстанавливает удалённую записку.
- Native attempts сериализуются в порядке **Connect authority fence → Caps write transaction → Notes transaction**, без await/network. Marker предшествует этому effect window отдельным durable COMMIT. Proof lookup предшествует новым mutable quota checks; live authorization для нового эффекта повторяется до Notes COMMIT. Все writers и internal reconciliation обязаны использовать тот же порядок.
- После Notes COMMIT неопределённость Caps не превращается в no-effect. Matching proof завершает spent/receipt даже после revoke; это внутреннее восстановление, не новый внешний read scope. Ошибка чтения, несовпавший store или malformed proof оставляют held, а не освобождают budget. Доказанное отсутствие проверяется под тем же сериализующим Caps lock.
- Terminal receipt, settlement и purge retained input коммитятся вместе. В live Caps tables не остаётся второй body. У unresolved input сохраняется. Удаление из live таблиц не обещает уничтожения прежних WAL/backup bytes.

Create-only внешний результат — исторические note ID/revision1 и неизменный эффект. Он не сообщает нынешние наличие, trash/deleted state или текст. Root master поправлен в соответствии с этой границей. Авторизованный PWA проверяет текущее состояние отдельно при открытии ссылки.

Store IDs обнаруживают другую incarnation, **не** более старый snapshot того же store. Несогласованный restore Caps/Notes/Connect нельзя автоматически трактовать как отсутствие эффекта. Нужны согласованный cold backup, проверенный restore и incident path; общего exactly-once через произвольный rollback файлов контракт не обещает.

## 4. Независимый малый Unicode probe

На изолированном `var/toolchains/node-v24.21.0-win-x64/node.exe` выполнены только `:memory:` SQL `SELECT` с bound strings, без таблиц и файлов. Фактические версии — Node `v24.21.0`, SQLite `3.53.4`.

| Исходная строка | Полученный UTF-8 hex | Точное равенство JS-строке |
| --- | --- | --- |
| Одинокий high surrogate | `EFBFBD` | false |
| Одинокий low surrogate | `EFBFBD` | false |
| Emoji U+1F600 | `F09F9880` | true |
| U+00E9 | `C3A9` | true |
| `e` + combining acute | `65CC81` | true |

Это подтверждает риск изменения текста и на новом runtime; это не migration/concurrency/production proof. Явный native admission reject до ledger для непредставимого Unicode обоснован. Pinned `@1` UTF-16 validator/digest остаются прежними; никаких normalization, replacement или truncation. Отдельный Notes document byte limit учитывает server defaults. Sidecar обязан описать эту admission границу до enable.

## 5. Решающая независимая приёмка после реализации

Эти сценарии **ещё не выполнены для Notes2/Capabilities2**. Они ограничивают необходимый gate, а не добавляют новые product APIs.

| Сценарий | Наблюдаемое требование |
| --- | --- |
| Exact historical v1, empty, v2, partial и mixed pair; default false / true | Старые notes/FTS/receipts/ledger сохраняются; IDs стабильны; default не мигрирует; partial/unknown не repair; native не работает на половине пары |
| Unknown marker/guard/index/project/registry, REPLACE и missing proof invariants | Fail до persistent mutation где это обещано новым reader; unknown не становится empty; legacy rejection измерен отдельно по main/WAL bytes |
| First bridge: candidate v3, actual old v2; после baseline — v2 format только в committed WAL | В первом случае нет STOP/serving mutation; во втором incompatible old не запускается ни rollback helper, ни START/restart/recovery |
| Lost CREATE/START response, pending v2 receipt, уже restored journal и новый несовместимый WAL | Fresh v3 guard; нет повторного START/второго writer; journal retained для recovery, старый receipt не authority |
| Crash после admission/marker/Notes COMMIT/Caps receipt; затем cancel/revoke | Один note/proof; spent=1 при совершённом эффекте; невыполненный marker не позволяет generic false release; ошибка чтения не доказывает no-effect |
| Два процесса с одним key, revoke creator/root/child, последний budget slot, busy каждого DB | Линеаризуемый допуск, стабильные IDs, не более одного эффекта и расхода; hold при ambiguity; сохранён прежний busy policy после операции |
| Edit >32, trash/purge, disabled handler, exact/changed retry и terminal retry после regrant | Исторический receipt без текущего тела/existence; никакого resurrection; changed fingerprint конфликт; disabled exact replay доступен только при живом read ACL |
| Подмена Notes store и несогласованный restore того же registry ID | Different incarnation fail-closed; старый same-ID snapshot не объявляется обнаруженным автоматически; нет ложного no-effect/released/нового create |
| Output/receipt/purge fault; Unicode/byte boundary; admission/storage quota exhausted | Не потерян unknown input; terminal атомарен; reject до admission где положено; replay/revoke/reconcile не заблокированы новым quota; ошибки без body/secrets |

После локального gate отдельно нужны Linux RO-WAL/topology/FTS на exact image, реальный SAME-image fallback и cold backup/restore согласованного набора. Только затем допустим production migration admission. Native `executionEnabled` требует отдельного B2 effect/recovery gate.

## 6. Узкий root WS и runtime follow-up

На Node24.21 root воспроизвёл зависшую WS capacity после `terminate()`: upgraded TCP socket мог остаться half-open, а завершение `for await` только отправляло source `end`. Прочитанный fix освобождает stream через идемпотентный `closeStream(..., 'app_client_closed')` после прочитанных chunks. Map/quota удаляются до cancel; relay/timers/pending writes закрываются прежним единым путём. Parser/heartbeat protocol и тайминги не менялись.

Это не выдаёт bare TCP EOF за clean WebSocket Close. Close control frames по-прежнему проходят через relay; clean close определяется завершённым handshake, а затем закрывается TCP. Такой предел соответствует [RFC6455 §7.1](https://www.rfc-editor.org/rfc/rfc6455#section-7.1). Source review нового lifecycle blocker не выявил.

Capacity regression теперь ждёт **реальный remote cancel точного stream ID**, а не только локальный close или искусственный sleep. Два новых actual-connector tests проверяют клиентский и source Close: обе стороны получают code1000/reason `finished`, освобождён ровно один stream, сосед реально продолжает echo. Это meaningful отрицательная граница для fix, не зеркало одной строки реализации.

Смежный `safe()` в независимом discussion test исправлен с substring `/bio/` на exact JSON field names. Случайный opaque encrypted cursor действительно мог содержать `bio`. Отдельные assertions на реальные private profile/body/World fixture values и точный Message DTO сохранены; privacy invariant не ослаблен.

Проверенные SHA256:

| Файл | SHA256 |
| --- | --- |
| `modules/apps/server/index.mjs` | `dd0db6887b3b50423c4493fb94b11966ff20ddf2b9be10bbe296c4d962344b22` |
| `modules/apps/test/app-runtime.acceptance.test.mjs` | `e37f34a8738b24e8f34321ff22dc6867681c3bc140f315c2b2bd56bb69486c07` |
| `modules/apps/test/websocket-liveness.acceptance.test.mjs` | `b0ecd0ae41444ce4aef51e2da0b733e614d09c25ccb0e82648c0d8e16bca3ff8` |
| `modules/apps/test/app-discussion.acceptance.test.mjs` | `e94619724faeaf2ab3d9e803c98a01ff53bcdd21d2c3235ca5186bd7f14af923` |

Root выполнил итоговый world на изолированном Node24.21.0: **858 tests / 853 PASS / 0 FAIL / 5 существующих skips**, 94.081 s. Reviewer прочитал финальный summary `output/implementation-20260930/p4-b1-node2421-world-final.log`; это атрибутированный root run, а не независимый повтор. Ранее сообщённый Cap/Connect176 PASS также root evidence. Старые неудачные прогоны не считаются текущим gate.

Прочитаны README/development/Connect README/master, оба `package.json` root изменения и итоговый `p4-runtime-baseline.md`. Минимум Node24.15.0 подтверждён официальным [release changelog с SQLite3.51.3](https://nodejs.org/en/blog/release/v24.15.0); [SQLite описывает исправление WAL-reset race](https://www.sqlite.org/wal.html#walresetbug). `engines` и название image не заменяют actual `sqlite_version()` и tests конкретной сборки. Никакая порча production данных этим review не установлена.

Docker pin metadata с `NODE_VERSION=24.15.0`, которую сообщил root, не является Linux execution proof. Actual Windows24.21/SQLite3.53.4 и прошедшая текущая world suite не доказывают Notes2/Capabilities2 migration, RO-WAL exact-container behavior или готовый native fallback.

## 7. Статус передачи

Контракт можно фиксировать как основание B1 реализации с принятыми уточнениями. Открытые gates — executable schema/row/migration acceptance, bridge/rollback composition, Linux exact-image и согласованный restore, затем B2 native effects. До них migration остаётся default-off, Notes2/Capabilities2 reader не объявляется по намерению, внешнее native исполнение не считается готовым.
