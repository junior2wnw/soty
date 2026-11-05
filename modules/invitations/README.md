# Приглашения в приложения и сообщества

Одна серверная модель ссылок, анкет, решений и лимитов для Apps и настоящих сообществ World. Членство приложения хранится в Invitations; членство сообщества — только в World. Текущий World использует сообщества с настройками видимости участников; отдельной модели прав для названия «канал» этот пакет не создаёт.

Основа перенесена из soty-invitations-20261021, commit f0d05fcbf65cfed1a10da0e6318367f1646a80f4. Исходные десять серверных тестов сохранены. Старый UI не перенесён: это интегрируемый backend, его наличие само по себе не означает готовый пользовательский экран.

## Контракт

Подписанные Connect extensions: invitations.get/configure/issue/revoke/preview/submit/status/requests/request.get/decide/members/member.set/inbox/reconcile. Все требуют expectedAccountId; изменения — requestId. Для управленческих операций принимается target:{kind:'app'|'community',id} либо прежний appId. Идентификаторы не заменяются синтетическими. Прежние soty.invitations.v1 и app DTO сохраняются; новые поля добавлены совместимо.

Выдача ссылки принимает label (подпись у организатора), recipientLabel (назначение получателя), необязательный recipientAccountId, mode:'direct'|'approval', expiresAt и maxUses. **Ни одна подпись не даёт роль или полномочие.** Привязка к recipientAccountId допускается только с maxUses:1. Несвязанный reusable invite может иметь maxUses:0 (без ограничения) или положительный лимит. Режим, срок и лимит фиксируются при выдаче. Повтор заявки одного человека не расходует лимит ещё раз.

Ссылки создаёт текущий владелец приложения/его явно назначенный director, либо текущий владелец/moderator сообщества World. В World допуск выдаёт обычное member. Изменение ролей и удаление World проходят прежние world.membership.*; invitations.member.set для сообщества отклоняется, чтобы не возникала вторая власть над членством. В приложении только владелец назначает других руководителей.

Публичный host preview(token) возвращает шаблон, recipientLabel и recipientBound, но не ID привязанного получателя, заявки или участников. HTTP preview, rate limit и root UI подключает хост. Invite в миниапку не обходит отдельный invite-only режим самой платформы.

## Сохранение и сбои

1. Invitations сохраняет заявку/решение, израсходованный use и стабильный claim одной транзакцией.
2. World заново проверяет target, владельца, текущего issuer, бан получателя и отзыв/отключение ссылки. Членство и admission receipt сохраняются одним World COMMIT.
3. Invitations отмечает результат. invitations.reconcile({claimId,requestId,expectedAccountId}) безопасно повторяет незавершённое действие. Получатель или текущий менеджер target может запустить повтор.

Сбой до первого COMMIT не выдаёт членство. Сбой после первого оставляет pending-admission, лимит не освобождается. После второго истиной служит receipt World; повтор не увеличивает ревизию членства. Удаление/бан участника не отменяется повтором старого claim. Отзыв ссылки запрещает новые допуски, но не отменяет уже существующее членство. Истечение срока запрещает новый claim; ранее durably принятое решение можно завершить после срока.

Это согласование двух SQLite, а не общая распределённая транзакция. Pending не выдаётся за approved. Ошибка уведомления не отменяет сохранённые данные.

## Подключение хостом

createInvitationTargetAdapter({apps,world}) возвращает withTarget, ownedTargets, admitTarget, readTargetAdmission. Их нужно передать в createInvitationsService({databasePath,actorActive,clock,...adapter,allowMigration2,onAccessChanged,onAdmission}) и зарегистрировать сервис как Connect extension.

Apps получает invitationAccess:{hasAccess,appIds}; closure разрешается после создания сервисов. onAccessChanged({appId,target:{kind,id},accountIds}) вызывается после сохранения и не передаёт анкеты/приватные метаданные. World admission использует свой after-COMMIT publisher.

Host-only readAdmission(claimId) подтверждает текущее членство/target и durable claim; readClaim и listAdmissionClaims({after,limit}) дают reconciliation. Эти порты не опубликованы как операции клиента.

onAdmission({claimId,target,issuerAccountId,recipientAccountId}) повторяется при reconciliation. Наблюдатель должен быть идемпотентным по claimId; его исключение не откатывает членство. Деньги/реферальные начисления не принимаются от клиента или в этой модели. Доверенный модуль обязан снова проверить admission и собственную authority.

## Формат2 и откат

Новый invitations.sqlite сразу получает формат2. Существующий формат1 открывается только с явным allowMigration2:true. Миграция транзакционно добавляет target kind, назначения получателей, данные заявителя и admission ledger; старые таблицы, токены, requests, members и commands сохраняются. Реальный DDL и foreign keys проверяются; неизвестная структура отклоняется.

Старый reader1 не читает формат2. До внешнего выпуска нужны совместимый reader/release declaration и закрытый снимок до миграции. Проверенный rollback — восстановление этого снимка с понятной потерей последующих записей; молчаливого downgrade2→1 нет. Production миграция этой работой не выполняется.

## Проверки

Node24.22.1: node --test modules/invitations/test/*.test.mjs.

Проверки покрывают сохранённые app сценарии, настоящий World/SQLite, faults до и после обоих COMMIT, отзыв/бан/демоцию, replay после удаления, привязку получателя, reusable limits, callback и миграцию/rollback. HTTP tests используют настоящий Root/Connect, коннектор, несколько аккаунтов и приватное приложение. Модульные проверки не заменяют приёмку интерфейса или production release.
