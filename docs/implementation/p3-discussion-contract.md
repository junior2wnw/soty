# P3-D2 — самостоятельные обсуждения приложений

Дата:30 сентября2026. Предыдущий принятый этап — D1 `3794f01`, отправлен в origin. Основание — [D0](p3-engagement-plan.md); этот документ фиксирует контракт D2 до приёмки реализации.

## Что происходит для человека

У приложения одно текущее обсуждение и доступные ему прошлые разговоры. Публичность разговора определяется его аудиторией: личный canonical вход автора не делает сообщения публичного обсуждения приватными. При смене доступа новый composer получает новый conversationId и пустой draft; прежний текст остаётся в прежнем контексте. Сохранение, участие в сообществе и уведомления остаются отдельными действиями.

Обычные чтение/отправка требуют выбранный exact entry. Владелец может отдельно открыть административный архив и удалить сообщение после закрытия приложения или адреса. Этот режим не разрешает писать или запускать приложение, не подставляет другой origin и возвращает `entry:null`. Автор сообщения может убрать собственный известный messageId после потери read-доступа; это не открывает ему историю или чужие сообщения.

## Данные и границы

Apps6 вводит шесть таблиц: heads, immutable conversations, messages/tombstones, bounded changes, usage и account rates. Бан отправителя, отдельные notifications/read-watermarks и прочее расширение модерации остаются P3-E/последующим сценариям. D2 предоставляет redaction владельца, собственное удаление и начальные лимиты.

Head появляется только при явном первом открытии обсуждения; conversation materializes при первом принятом сообщении. Смена нормализованных owner/mode/grants выдаёт новый случайный currentId внутри той же Apps transaction, где меняются права. Пустые поколения не накапливают archive rows. Имя, список адресов, листинг, epoch или только источник не меняют аудиторию. Смена источника вместе с launch policy меняет её. Restrict/revoke/rotation уже существующего head не зависят от заполненности chat-квоты.

Материализованный descriptor immutable. Restricted archive требует свежего допуска к выбранному входу, текущего grant-допуска и исходного direct/community predicate; public basis его не заменяет. Public archive требует свежего допуска к входу. Актуальное membership/право publisher управлять группой проверяются под World authority fence. World community messages и полный WorldProfile не копируются.

Архивы сначала полностью фильтруются, затем ограничиваются страницей видимых записей. Нет continuation из-за одних скрытых строк, их количества или промежуточных generations. В пределах1000 разговоров/app и64 community grants/descriptor собирается bounded набор relevant IDs; один host World JOIN возвращает пересечение активного участника и действующего publisher owner/moderator. При1000 архивов и ещё одном head максимум64064 relevant IDs; host принимает не более65536, запрещает вызов вне fence и не создаёт profile. Дальнейшие predicates используют локальный Set внутри этой операции.

## API

Factory: `createDiscussionRegistry({db,now,assertActor,withAuthorityFence,resolveEntry,canUse,readCommunityAuthority,authorLabel,limits?})`. Только синхронные callbacks. Общий helper transaction сохраняет D1 semantics и короткую busy policy; Connect→World→Apps, без await/network. `readCommunityAuthority(actor,ownerAccountId,relevantCommunityIds)` возвращает строгий subset IDs; `authorLabel` берёт только bounded подпись аккаунта, не устройство/биографию. `syncDiscussionAudienceInTransaction(db,appId,timestamp)` вызывается до COMMIT при publication update, source promotion, grants update и revoke.

| Operation | Вход и результат |
| --- | --- |
| `apps.discussion.context` | `{appId,domainId?,path?,conversationId?,administrative?:true}` → `{context,messages,historyCursor,changeCursor}`. Начальная страница и change cursor относятся к одному снимку. |
| `apps.discussion.history` | App/conversation/exact domain/path плюс `cursor?,limit?` → `{context,messages,nextCursor,resetRequired}`. В административном режиме domain/path запрещены. |
| `apps.discussion.changes` | Тот же разрешённый контекст и cursor → `{context,changes,nextCursor,hasMore,resetRequired}`. Изменения включают tombstones, в том числе сообщения вне последнего хвоста. |
| `apps.discussion.archives` | App/exact entry либо administrative плюс cursor/limit → `{entries,nextCursor,resetRequired}` без hidden totals. |
| `apps.discussion.send` | `{appId,conversationId,domainId,path,requestId,body,replyTo?}` → `{requestId,replayed,receipt,ownCurrent,message}`. Explicit entry обязателен. |
| `apps.discussion.remove` | `{appId,conversationId,messageId}` → `{id,conversationId,removed:true}`. Монотонное повторяемое удаление; нельзя изменить адресат посредством retry. |

Context содержит app/entry/conversation, current/archive, administrative, безопасную audience label и server permissions. Message содержит ID/conversation, минимальную подпись автора, body либо tombstone, replyTo только как ID, времена и canRemove. Ни raw ACL, ни source/device, ни private counters наружу не выходят.

Request key send уникален для аккаунта по всем apps/conversations. Fingerprint связывает app, conversation, exact entry, body и reply. Неизменяемая квитанция `{id,conversationId,createdAt}` отделена от `ownCurrent.removed`. Exact replay возвращает собственную квитанцию даже после потери read, но body/history разрешаются заново. Изменённый payload того же key конфликтует, в том числе после redaction. Tombstone сохраняет fingerprint; новый send в старое поколение запрещён и не переназначается новому.

Changes сохраняет ID события, не старое тело. Если между отправкой и polling сообщение удалено, возвращается tombstone, а не прежний текст. Курсоры зашифрованы и scoped к actor/device/app/entry/conversation/admin. Ключ живёт в service instance: после restart — явный `resetRequired`, затем новый snapshot. Это контракт текущего single-writer пилота, не обещание стабильного курсора между независимыми workers. D3 обязан сохранить локальный draft при reset; серверный тест не доказывает это свойство UI.

Native Connect сохраняет HTTP400 для semantic RPC errors, включая model-level conflict/unavailable; временная занятость `apps_discussion_busy`/`world_authority_busy` получает503, rate limit `apps_discussion_rate_limited` —429. Клиент различает причины по typed code; handler не повторяет mutation автоматически.

## Ограничения пилота

Body≤4000 UTF16 units и≤16KiB, корректный Unicode/plaintext. History30/max50, changes≤100, archives20/max50, JSON response≤256KiB, changes retention2048/разговор. Ограничения реального byte budget применяются к полной странице, а не только тексту.

Trusted `limits` может только понизить admission defaults: heads8192/global, materialized conversations8192/global и1000/app; message/tombstone rows1000000/global и10000/app; live body1GiB/global и32MiB/app. Сообщения не удаляются молча для освобождения квоты. Квоты не блокируют read/remove/replay/restrict/revoke; понижение settings не делает уже допустимую базу corrupt. Token buckets: аккаунт burst10/refill1 за2s; app burst60/refill1 за250ms. Повтор принятого запроса, удаление и изменение доступа не расходуют send rate.

Это логические ограничения закрытого пилота, не физический hard cap SQLite/WAL и не доказательство готовности к миллиардам пользователей. Масштабирование требует нагрузки и эксплуатационного gate. Global head+conversation bounds отдельно ограничивают сохранённые audience snapshots; их размер не прячется за body quota.

## Последовательность сдачи

1. Автор: Apps6 schema/recognizer/migration, registry, узкий shared transaction helper и авторские тесты. Root: World JOIN, пять atomic policy hook points, service/HTTP wiring.
2. Независимый reviewer: другая acceptance suite с настоящим signed HTTP, конкурентным World writer и отрицательными privacy/replay/cursor/capacity сценариями.
3. Reader engineer: literal historical5, exact old5 code, независимый Apps6 probe/manifest/rollback gate. Исторические тесты не используют current6 как якобы5.
4. Root: общий regression, D1 replay после helper extraction, typecheck/build, аудит diff и checkpoint. Затем D3 UI и D4 настоящий browser cycle; production/публичный пилот отдельно.

Статус: контракт реализован и прошёл [локальную серверную приёмку D2](p3-discussion-integration.md). D3/D4 UI и production этим не приняты.
