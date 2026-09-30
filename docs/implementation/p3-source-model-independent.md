# P3-C2-A — независимая приёмка модели источников

2026-09-30. Reviewer: agent_ecosystem. Рабочая копия `soty-platform`, C1 baseline `914a91a`. Сверено с [общим контрактом](p3-source-contract.md), [wire contract](p3-source-wire-contract.md) и [авторским описанием](p3-source-model.md).

**В проверенной области новых блокеров не найдено.** Это ограниченная приёмка persistent модели и legacy floor guard, а не готовность source API, connector v2 или всего P3.

## Авторство и проверенная версия

Production source/schema/publications/index reviewer не редактировал. Независимо добавлен только `modules/apps/test/app-sources.acceptance.test.mjs`. Fixture строит настоящую историческую Apps3 из отдельно замороженного `deploy/connector/apps-v3.fixture.mjs`, затем проходит настоящую миграцию. Авторские test functions/fixtures модели не импортируются.

После полученного author freeze сверены SHA256:

| Файл | SHA256 |
| --- | --- |
| `modules/apps/server/sources.mjs` | `68377e448099dfed92aba47d3a9f351c1c6cefdd1c18fd367bb9e345889cd0ea` |
| `modules/apps/server/schema.mjs` | `aa7a07bdfd0b59a38c7dd53f2c8fd4eb135cad64f85de6d80bd47474a54be5c3` |
| `modules/apps/server/publications.mjs` | `467bfedf4ecd7b37b41fb525e7e0c0d1d884a1b76339ced59d2b2abfbe79e76e` |

Последовательность review: чтение authority/CAS/receipt/guards → собственные сценарии → получение freeze → чтение финальных повторных проверок после SQL → общий повтор. Предварительный и финальный независимые прогоны совпали по результату.

## Выполненные проверки

Финальная команда:

```text
node --test modules/apps/test/app-sources.acceptance.test.mjs modules/apps/test/app-source-schema.test.mjs modules/apps/test/app-sources.test.mjs modules/apps/test/runtime-binding-floor.test.mjs
```

Результат: **36/36 PASS, 0 skips, 0 failures**. Из них 12 — новые независимые сценарии, 21 — авторские schema/source, 3 — root real-network legacy floor. Авторские заявленные 78/78 не выдаются за независимо повторённые 78: здесь повторён ровно указанный набор36.

Независимые12 сценариев проверяют следующие эффекты в файловой SQLite/WAL:

1. Actor отозван во время удерживаемого асинхронного prepare: успешный ответ synthetic provider не разрешает запись. После prepare смена поколения synthetic channel также блокирует promote. Все durable rows сохранены.
2. Участник с grant, другой владелец и соседнее приложение не заимствуют source authority. Запрет private операции происходит до probe; preparation другого app не годится для записи.
3. Обычный JSON с `ready:true` не принимается настроенным строгим verifier вместо его opaque evidence. Это проверка model-to-provider seam, не обещание, что модель сама узнаёт настоящий socket.
4. `onChanged` падает после COMMIT: target/receipt остаются. Registry **и SQLite connection** закрываются, время выходит за TTL, новая connection выполняет точный replay без нового probe. Исторический receipt и более новая текущая policy различаются; foreign/revoked actor не получает replay.
5. Вторая connection действительно retire-ит активный alias через domain API. Source с прежним CAS не воскресает, tombstone и активный набор не меняются обратно.
6. Удаление grants и revoke в другой транзакции сохраняются при попытке stale source promote; не появляется orphan target или старое публичное разрешение.
7. Одинаковый текст requestId допустим отдельно для publication/source namespaces, но не переиспользуется для другой app того же аккаунта в source namespace.
8. На65-й принятой команде trigger ломает DELETE при retention prune. Весь эффект откатывается, включая новую target row, floor/policy/receipt. После снятия fault та же подготовленная команда принимается; сохранены64 receipts. Повтор уже pruned исходной команды получает CAS conflict без auto-rebase и записи.
9. Исчерпание safe-integer policy epoch откатывает вставку и первый переход floor1→2 вместе; частичного переключения нет.
10. Rollback требует подходящее новое whole-port согласие, сохраняет исходную target1 побайтно по её полям и sticky floor2. Если освобождённый прежний порт заняла другая app, rollback не отбирает его.
11. History owner/app scoped и ограничена курсором с верхней revision. Добавление targets не сдвигает старую страницу; курсор другого app/actor не раскрывает записи, отозванный actor не продолжает чтение.
12. **Два настоящих Worker/SQLite writers переключают разные apps на один свободный connector+port.** Барьер синхронизирует готовые preparations; ровно одна транзакция выигрывает. Другой writer получает `app_port_already_registered`, его target/floor не меняются. В БД одна новая target и один source receipt. Это проверка общей write-сериализации занятости, отличная от авторского race двух команд одного app.

SQL fault triggers создаются только после открытия сервисов в отдельных временных тестовых БД, затем снимаются перед reopen. Проверка runtime production schema recognizer не ослаблялась. Перед рекурсивным удалением временного каталога fixture проверяет абсолютный parent и ожидаемый префикс; workers закрываются до завершения результата.

## Authority и границы атомарности

Read-only review подтвердил: текущий owner проверяется до historical replay; request intent вычисляется без обращения к transient preparation; retained receipt ищется до TTL/runtime proof; новый эффект выполняется в `BEGIN IMMEDIATE` с общим CAS/occupied check. Floor повышается в одной транзакции с immutable target, publication epoch/pointer, receipt и prune. После всех SQL повторяются actor, синхронный verifier и срок непосредственно перед COMMIT. Network await внутри write transaction отсутствует.

`requiredBindingVersion` — общий accessor модели/decision/transport. Ревизия1 после rollback не является основанием понизить floor. Missing head не восстанавливается автоматически; INSERT OR REPLACE не меняет immutable target и не понижает floor. Историческая initial tuple не используется как индекс занятого рабочего порта. Эти границы сверены также с author schema tests и root integration.

Отдельный root floor review и дополнительный inline HTTP/WS probe существующей cookie/открытого потока записаны в §11 [wire receipt](p3-source-wire-contract.md). Там наблюдались cookie/session403, guest503, закрытие старого stream и отсутствие позднего body после внешнего floor1→2. Это не browser/TLS proof.

## Что здесь намеренно не доказано

- `prepareTarget/verifyPreparedTarget` в новых тестах — **synthetic in-memory WeakMap provider**, поколение которого управляется тестом. Он проверяет точную frozen target/actor/preparation и имитирует потерю текущего канала. Сокета, negotiation, target HEAD и v2 ACK в этих тестах нет. Следовательно, настоящая attestation коннектора ещё не принята.
- Source operations вызываются напрямую на registry. Они не подключены к HTTP/Connect в C2-A. Синхронный Connect extension не принимает Promise; C2-B требует отдельного асинхронного adapter и настоящего signed request с after-await identity/revocation. Direct model success не равен успеху этого API.
- Здесь нет C2 UI, настоящих двух upstream apps, immutable generated connector behavior, физического устройства, public DNS/TLS, Linux candidate/fallback или backup/restore проверки. Reader gate имеет собственный receipt.
- Отзыв допуска/route switch не откатывает уже совершённые внешние эффекты и не стирает данные локального приложения. Исторический receipt не означает текущий работающий маршрут.
- Bounded receipts не хранят все прошлые request keys навсегда. Гарантия после prune относится к исходному неизменённому CAS; клиент не должен автоматически перебазировать старую команду или придумывать новый intent при неизвестном исходе.

Для C2-B отдельно передана конкретная URL-boundary: `/` +2000 кириллических символов принимается как исходный local path2001, но сериализованный HTTP pathname12001 превышает существующий runtime limit8192. Проверка сериализованного HTTP path до HEAD должна дать отказ кандидату, сохранив исходный digest; это ещё не выполненная транспортная проверка и не требование менять Apps4/history в данном gate.

## Решение

Persistent C2-A можно принимать в пределах перечисленного evidence вместе с отдельным reader gate. C2-B реализация и её сетевые/подписанные проверки остаются обязательными; R1/R2 runtime races из wire contract в этом review не исправлялись и не объявлены закрытыми. Source test/doc заморожены до конкретного нового finding или согласованного изменения контракта.
