# P3-D2 — Apps6 discussion model author receipt

Дата: 30 сентября 2026. Основания: принятый D1 `3794f01`, [согласованный D2 контракт](p3-discussion-contract.md), [общий план](p3-engagement-plan.md). Это авторская приёмка модели; итоговый checkpoint принимает root после независимых tests и общего regression. Production не изменялся.

## Реализованная граница

В Apps6 добавлены ровно шесть таблиц: `app_discussion_heads`, `app_discussion_conversations`, `app_discussion_messages`, `app_discussion_changes`, `app_discussion_usage`, `app_discussion_rates`. Шесть индексов обеспечивают lookup/compound FK и глобальную для аккаунта уникальность send request key. Тринадцать известных triggers защищают lineage, immutable audience, identity/fingerprint, однократный переход body→tombstone и обязательные head/usage rows. Полные SQL definitions и projections находятся в `schema.mjs`; прежние DDL1–5 сохранены. Marker — `soty.apps-registry.v6`, `user_version=6`.

Миграция создаёт только пустые таблицы и нулевой usage singleton. Она не выдаёт адреса/права, не открывает обсуждения существующим приложениям и не материализует историю. Независимый literal Apps5 fixture взят из `3794f01`, а не из current migration. Reopen проверяет известные SQL definitions, foreign keys, immutable audience digest, связь current head с настоящей publication policy, message/tombstone counts, live bytes и непрерывный сохранённый suffix changes. Неполная v6 не дополняется автоматически. Пониженные runtime limits не превращают уже сохранённые допустимые данные в corrupt.

`syncDiscussionAudienceInTransaction(db, appId, timestamp)` синхронно работает только в существующей Apps transaction. Пока head отсутствует, hook ничего не создаёт. Существующий head меняет случайный current conversationId и увеличивает generation ровно при изменении owner/mode/нормализованных grants. Пустые промежуточные поколения не создают archive rows. Материализованный descriptor сохраняется только при первом принятом send; права нового поколения никогда не переписывают старый descriptor. Source/name/listed/alias/epoch-only изменения не меняют аудиторию; source вместе с mode change меняет её. Parent владеет атомарными hook points в publications/index, их отдельная проверка описана в общем gate.

## Авторизация и транзакции

`createDiscussionRegistry({db,now,assertActor,withAuthorityFence,resolveEntry,canUse,readCommunityAuthority,authorLabel,limits?})` предоставляет шесть синхронных operations из контракта и `close()`. Реестр не импортирует publications, поэтому hook не создаёт циклической зависимости. Pure canonical audience helpers находятся в schema.

Общий `engagement-transaction.mjs` сохраняет D1 порядок Connect→World→Apps: действительный actor проверяется до host fence и повторно внутри `BEGIN IMMEDIATE`. Account/device и необязательная строка label копируются до callback. Await/network запрещены; thenable — ошибка. Временный Apps `busy_timeout=100` восстанавливается точно в `finally`; это ограничение ожидания lock, не wall-clock deadline всей операции. Ошибка host release после Apps COMMIT означает неизвестный клиенту ACK: эффект и receipt остаются, точный retry возвращает результат. D1 использует тот же узкий helper без изменения своего DTO/receipts/CAS.

Обычные чтение и send разрешаются через каноническую Apps policy по exact domain/path. Offline источник не отменяет доступ к самостоятельному обсуждению. Restricted archive дополнительно требует текущий grant и исходный predicate. Пути старого и нового grant могут отличаться; public admission не заменяет прежний restricted predicate. Текущий публичный descriptor остаётся публичным, даже если посетитель вошёл через личный canonical grant. Membership внутри сохранённой группы динамический; новая другая группа не открывает старую переписку.

Для bounded archive pass собирается union relevant community IDs всех descriptor rows и текущего head. Host callback вызывается один раз внутри fence и обязан вернуть строгий subset кандидатов. Дальнейшие predicates проверяются локально. Все materialized conversations сначала проходят ACL, затем страница ограничивается видимыми rows. Только скрытые разговоры не порождают continuation, counts или числовые промежутки.

Владелец имеет отдельное `administrative:true`: без domain/path, `entry:null`, только read/redact, включая закрытое приложение. Этот режим не является launch bypass. Точный автор известного messageId может удалить собственный текст после потери read-доступа; это не возвращает чужую историю. `canPost` отражает право в текущей аудитории, а не обещание свободной quota или rate allowance.

## Отправка, удаление и курсоры

Send request key уникален по account среди всех apps/conversations. Fingerprint связывает app/conversation/exact domain/path/body/reply. После настоящей аутентификации сохранённый fingerprint сравнивается до изменяемого read ACL. Exact replay возвращает неизменяемую собственную `{id,conversationId,createdAt}` и отдельное `ownCurrent.removed`; message body возвращается только при новом read admission. Retarget или другой payload конфликтует и после удаления. Новый send в прежнее поколение отказывается, без переноса в новый composer.

Сообщение хранит минимальную account label snapshot — не World profile, bio или device label. Body — текст, без интерпретации HTML. Reply — только ID того же conversation, без скопированного preview. Redaction необратимо убирает body, освобождает live-byte quota, сохраняет row/request fingerprint и создаёт только одно событие удаления; повторное remove не добавляет событие и не расходует send rate.

Initial history и change cursor читаются из одного Apps snapshot. History cursor удерживает верхнюю границу созданных сообщений; retained tombstone остаётся в старой странице. Changes читает текущую message row по event ID: create→delete до polling возвращает tombstone, а не прежний body. Если change cursor старше сохранённого suffix, ответ `resetRequired:true`; клиент должен перечитать snapshot, сохранив свой локальный draft.

Курсоры scoped к account/device/app/exact entry/conversation/admin. AES-256-GCM использует случайный IV и ключ текущего instance. Plaintext всегда занимает 512 bytes, итоговый cursor — 737 ASCII bytes: это исправляет найденную reviewer утечку разрядности скрытого generation через длину ciphertext. Курсор не authority; текущий ACL проверяется до чтения cursor state. TTL — один час, backward clock/expiry и другой instance требуют reset. Restart/different worker cursor stability не обещается; текущая deployment-модель — один управляемый writer process.

## Численные ограничения

Default admission: 8192 heads/global, 8192 materialized conversations/global, 1000/app, 1000000 message/tombstone rows/global, 10000/app, 1GiB live bodies/global, 32MiB/app. Trusted overrides могут только понизить положительные defaults. Restrict/revoke/rotation существующего head, read/remove/replay не зависят от новой chat admission quota. Нет тихого удаления сообщений, predicates или ключей ради освобождения места. Исчерпание safe integer generation — явная ошибка, не сброс счётчика.

Body ≤4000 UTF16 units и ≤16KiB, well-formed Unicode, не пустой после проверки whitespace; control characters запрещены, кроме обычных tab/newline/carriage return. History default30/max50, changes default/max100, archives default20/max50. Вся JSON page, включая context и курсоры, ограничена 256KiB; предел применяется к реально сериализованным bytes. Changes сохраняет последние2048 событий/разговор (либо меньший trusted limit); снижение limit не чистит старую историю при чтении, pruning происходит при следующем изменении этого разговора.

Token buckets: account burst10/refill1 за2s, app burst60/refill1 за250ms. Только committed first send расходует slots/credit; fault rollback возвращает оба buckets. Clock regression не допускает новый send; сохранённый receipt и monotonic remove остаются доступны. Arithmetic refill ограничивает elapsed до capacity до сложения.

Это logical budgets закрытого пилота, не физический hard cap SQLite/WAL. Heads плюс conversations могут занимать до примерно512MiB только raw bounded audience JSON при лимите32KiB/descriptor; индексы, сообщения/tombstones, author snapshots, rates, SQLite pages и WAL добавляют объём. Ни body quota1GiB, ни данный тест не являются total disk/RAM доказательством или нормой для миллиардов пользователей.

## Проверено автором

Финальная команда:

```text
node --test modules/apps/test/app-discussion-schema.test.mjs modules/apps/test/app-discussion.test.mjs modules/apps/test/app-saved.test.mjs modules/apps/test/app-engagement-schema.test.mjs
```

Результат: **36/36 PASS**, 0 failures/skips, около3.9s. Из них15 новых D2 tests (9 model +6 schema),15 прежних saved tests и6 прежних migration tests. Проверены genuine5→6 preservation, atomic failed-last-DDL rollback, известные guards, corrupt reopen без repair, lazy lineage, strict scalar/Unicode/path admission, rollback первого send и удаления/prune, оба rate buckets, history/change snapshot, host fence/closed registry, lower quota/emergency rotation и точный replay. Saved regression включает настоящие два SQLite writers, bounded busy отказ, restore прежнего timeout и unknown ACK после host release.

Полный bounded1000×64 historical predicates +64 current пробег: 64064 relevant candidates, **один authority callback**, 20 visible entries и cursor,60.5ms в последнем локальном прогоне SQLite/JS. Callback в этом author test синтетический; это отдельно от root actual World JOIN64k probe и не end-to-end latency/production load benchmark.

Независимый reviewer самостоятельно воспроизвёл cursor-length RED и добавил regression. После fixed padding этот exact тест PASS. Его настоящие service/World/двухпроцессные/signed HTTP tests, parent publication hooks и trusted reader6 имеют отдельные receipts; автор не выдаёт их за собственные fixtures. Browser UI, durable client draft/pending, DNS/TLS, Linux exact-image probe, backup/restore и rollout этот этап не доказывает.

## Source freeze

| Файл | SHA256 |
| --- | --- |
| `modules/apps/server/schema.mjs` | `0dba9ad9c9e0427f56bb34c207242680e247d9f39dc96a9852802309c6fa5786` |
| `modules/apps/server/discussions.mjs` | `3709b3473759ff48ed76b07efd41861f5b8c81db9e3d48e02b2725d31f97f878` |
| `modules/apps/server/engagement-transaction.mjs` | `c8d9ed2139d6dc1e8ca4a8c61ed0680a67c19fdc4eb690b94bafe13f1ffcaffe` |
| `modules/apps/server/saved.mjs` | `6774196f29e8ec785dfba2c1a946463a6973f6010a5b67cc7c9939fd8ad2d307` |
| `modules/apps/test/app-discussion.test.mjs` | `6f13345dcaa49fc71ad27a9220a6d5ee0dc3da11ed9a872ef10dbb026473a714` |
| `modules/apps/test/app-discussion-schema.test.mjs` | `48d476d849426aacdb4b4303c283cd3ece139f1069199ffde9c22763ca9ee325` |

Latest-version assertions изменены только в разрешённых current-migrator tests: app-source-schema, app-publication, app-publication.acceptance, newdomains, app-engagement-schema. Исторические входные fixtures не переименованы в новую версию. Parent/shared source и независимые tests не редактировались.
