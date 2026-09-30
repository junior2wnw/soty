# P3-C2-A — модель смены источника

2026-09-30. Авторский локальный gate. Независимая приёмка модели — отдельный этап. Source API в этом срезе **не подключён** к HTTP/Connect; сетевой протокол и интерфейс относятся к C2-B/C2-C.

## Постоянные данные

`schema.mjs` распознаёт точные исторические Apps1/2/3 и Apps4. Новые базы имеют marker `soty.apps-registry.v4` и `user_version=4`. Apps4 добавляет две таблицы, сохраняя прежние определения publication/domain/target:

- `app_source_heads`: `app_id` — PK/FK на приложение; `required_binding_version` принимает 1 или 2.
- `app_source_receipts`: PK `(account_id,request_key)`, составной FK `(app_id,account_id)` на владельца приложения; `intent_hash`, `committed_epoch`, `value_json`, `created_at`. Уникальный индекс `app_source_receipt_epoch(app_id,committed_epoch)`.

Новые guards: `app_source_head_no_downgrade`, `app_source_head_no_delete`, `app_source_head_no_replace_downgrade`, `app_runtime_target_no_replace`. Вместе с прежними `app_runtime_target_no_update/no_delete` они запрещают понижение floor и замену неизменяемых targets, включая `INSERT OR REPLACE` при обычном `recursive_triggers=OFF`. Точные SQL-определения проверяются schema recognizer и отдельным trusted deploy reader.

`requiredBindingVersion(db,appId)` — общий синхронный accessor. Missing head, неизвестная версия или любая историческая target revision выше 1 при floor1 — ошибка данных. После первого promote floor всегда 2, даже после возврата к target1. Это требование протокола, а не признак готового соединения.

`local_apps.connector_key/port/entry_path` остаются исторической initial tuple. Рабочий маршрут — `app_publications.active_target_revision` плюс неизменяемая запись `app_runtime_targets`. Имена, grants, aliases и обсуждения source-команда не переносит и не пересоздаёт. `requiredBindingVersion` добавлен в publication read model и branded AccessDecision; recheck закрепляет эту величину вместе с прежними pins.

## Миграция и отказ

Распознавание формата происходит до записей, затем повторяется внутри `BEGIN IMMEDIATE`. Genuine Apps3 содержит ровно target1 и active revision1. Такая база переносится в Apps4 без изменений прежних строк; source head1 добавляется отдельно. Любая noninitial target в Apps3 отклоняется целиком как `apps_source_history_unsupported`, включая неактивную историю. Молчаливого выбора floor1 нет.

Уже существующая Apps4 проверяется, а не дозаполняется. Потерянные publication/head, изменённая initial tuple, несуществующий active target или несовместимый digest приводят к отказу открытия. `ensureInitialPublication` создаёт состояние нового приложения внутри транзакции регистрации; при найденных target/policy/head только проверяет согласованность. Остаточная история без initial target также не восстанавливается автоматически.

Исторический Apps3 fixture взят из `914a91a`, а не сгенерирован новой миграцией: `deploy/connector/apps-v3.fixture.mjs`. Его независимую сверку DDL и отказ настоящего старого migrator на Apps4 описывает [reader gate](p3-apps-v4-reader.md). Форматный reader не является проверкой целостности всех данных или доказательством поддержки runtime v2.

## Registry и интеграционный контракт

`createSourceRegistry({db,now,assertActor,publications,prepareTarget,verifyPreparedTarget,onChanged,blockedPorts,limits})` возвращает `execute({op,actor,args})` и идемпотентный `close()`.

- `apps.source.prepare` асинхронен. Аргументы: `appId`, текущие `expectedPolicyEpoch/expectedTargetRevision` и ровно один вариант — `source:{hostDeviceId,connectorId,port,entryPath}` либо исторический `targetRevision` для возврата.
- `apps.source.promote` синхронен. Аргументы: тот же app/current CAS, новый `requestId`, точный `preparationId`, явные `launchPolicy/listed`, при anyone — `exposureAck` для whole-port/revision/digest/profile кандидата.
- `apps.source.history` синхронен и доступен только текущему владельцу. `limit` от 1 до 50, по умолчанию 20; cursor привязан к app/account и верхней revision первой страницы. Новые targets не сдвигают уже начатую страницу.

Prepare нельзя подключать к обычному синхронному Connect extension: такой транспорт намеренно отклоняет Promise. C2-B требует отдельного `executeAsync` adapter с завершением proof-транзакции до сетевого await и тестом настоящего подписанного HTTP. Остальные Apps-команды остаются синхронными.

## Временная подготовка

Actor identity и нормализованная target tuple копируются до await; actor/target заморожены. Проверка source выбирает точное принадлежащее владельцу устройство. Несколько connector rows с одинаковыми host/connector IDs дают явный `apps_source_device_ambiguous`, а не выбор по случайному порядку.

`prepareTarget({preparationId,actor,appId,expectedPolicyEpoch,expectedTargetRevision,target,requiredBindingVersion:2,signal})` вызывается вне SQL-транзакции и возвращает opaque evidence. В публичный DTO попадают только безопасная target view, идентификатор подготовки, CAS, `checkedAt/expiresAt`; connector key и evidence не возвращаются.

Лимиты включают незавершённые callback: 256 глобально, 4 на аккаунт, 2 на приложение. TTL — 30 секунд от начала, deadline callback — 5 секунд. На переполнении отказ без очереди и без вытеснения другой подготовки. Неотзывчивый callback после abort продолжает занимать свой слот до завершения: таймаут не создаёт возможность накопить бесконечные Promise. Close/expiry abort signal; закрытие до microtask не вызывает провайдер. Clock rollback и наступивший expiry дают отказ.

Подготовка не пишет постоянных targets, floor или active pointer. После await вновь проверяются владелец, CAS, устройство, срок и exact-channel proof. Подготовленный порт не резервируется: конкурентная регистрация проверяется ещё раз внутри promote.

`verifyPreparedTarget({evidence,preparationId,actor,target,requiredBindingVersion:2})` должен синхронно вернуть **literal true**. Promise не принимается. C2-B callback отвечает за принадлежность evidence текущему socket/channel, точной tuple и свежему подтверждению. HEAD 200–499 означает только наблюдаемый ответ процесса, включая 401/404; это не аттестация кода, качества приложения или внешнего DNS/TLS.

## Атомарное переключение

Нормализованный wire intent хэшируется независимо от существования transient preparation. В короткой `BEGIN IMMEDIATE` выполняются:

1. Проверка текущего владельца и поиск отдельного source receipt по account/request key.
2. Для retained receipt — совпадение intent; возвращается исторический receipt и отдельное актуальное состояние без новой мутации.
3. Для новой команды — exact policy/current-target CAS, подготовка этого же actor/device/app, её срок, занятость active connector+port другим enabled app и новое публичное согласие.
4. Синхронная проверка текущего proof, floor2, вставка нового immutable target либо выбор прежнего, один новый policy epoch/active pointer и consent.
5. Receipt и удаление только receipts за пределами последних 64 committed epochs этого приложения.
6. Повторная проверка actor, proof и срока после всех SQL непосредственно перед COMMIT. Любой отказ откатывает все записи.

Ни сети, ни await в write transaction нет. Name-only C1 изменение не меняет policy epoch. Grants/revoke/active alias retirement используют общий epoch и конфликтуют с устаревшим source CAS. Регистрация и source promote должны проверять занятость по **active** tuple в той же транзакции; исторические поля не являются индексом занятых портов.

Возврат проходит ту же подготовку и promote: новую проверку устройства, новое публичное согласие либо явное restricted, новый epoch. Прежний target не перезаписывается, floor не понижается. Это возврат маршрута, а не восстановление файлов/данных приложения.

## Потерянный ответ и согласование

Receipt ищется до transient preparation, поэтому после рестарта или TTL истечения можно безопасно повторить точную принятую команду. Receipt содержит хэши request/preparation, app, previous/current target revisions, digest/profile/floor, историческую политику/epoch/consent и время. Сам preparation ID не сохраняется в receipt body. Результат `current` читается отдельно: историческое подтверждение не выдаётся за текущую публикацию.

Retention — 64 source receipts на приложение по epoch, независимо от publication receipts и настенных часов. Targets не удаляются. Для давно удалённого receipt исходный CAS не перебазируется; новый запрос не создаётся автоматически. Отсутствие receipt не является доказательством отсутствовавшего эффекта.

`onChanged({appId,policyEpoch,oldConnectorKey,newConnectorKey,replayed})` вызывается после COMMIT и на retained replay. Это синхронное уведомление для идемпотентного согласования текущих bindings, а не повтор мутации. Ошибка уведомления возвращается как потерянное подтверждение при сохранённом receipt. Runtime повторно читает текущую запись при согласовании: снимок события может устареть из-за другого процесса. После commit до fresh binding ACK приложение остаётся в честном ожидании; автоматического fallback нет.

## Авторское доказательство

Команда:

```text
node --test modules/apps/test/app-source-schema.test.mjs modules/apps/test/app-sources.test.mjs modules/apps/test/app-publication.test.mjs modules/apps/test/app-publication.acceptance.test.mjs modules/apps/test/newdomains.test.mjs
```

Результат: **78/78 PASS, 0 skips**. Новые schema/source тесты — 21 из 78. Проверены genuine v3/старые v1-v2, сохранность public consent/grants/tombstones, replace/downgrade guards, no-backfill, async actor capture, after-await CAS, expiry/clock rollback, proof отказ после SQL, rollback source, потерянный postcommit ACK с новой SQLite connection, fault rollback, caps/close, epoch retention, bounded history и два настоящих SQLite worker writers с одним winner.

Существующие fixture expectations обновлены только на текущий Apps4 marker и более ранний отказ открытия повреждённой базы. Исторические DDL не заменены latest migration. В тестах registry runtime evidence — явный stub. Эти результаты не объявляются проверкой v2 по сети, signed source API, браузерного C2 UI, Linux image, production restore или deployment.

## Оставшиеся gates

Независимый model review, runtime v2/C2-B с двумя реальными upstream и signed async prepare, C2-C UI и общий C2-D gate. До публичного выпуска также нужны проверенные Apps4/v2 candidate и fallback, остановка старого writer перед миграцией, backup/restore, exact-image Linux probe и отдельный DNS/TLS/runtime-site gate. Apps4 storage reader сам по себе не делает legacy connector совместимым со сменой источника.
