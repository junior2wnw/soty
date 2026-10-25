# Входы в приложения: S29 и S30

Единственный редактор: **Профиль → Безопасность → Входы в приложения**.
Пункт **Действия ИИ и приложений** остаётся отдельным редактором agent grants.
Категория настроек не добавляется; `clientId` Human OIDC не считается Root appId
или адресом приложения и не превращается в ссылку запуска.

| Настройка | Что показывает редактор | Явный отзыв |
| --- | --- | --- |
| S29 · вход и продление до24h | Семья разрешения Human OIDC, приложение, устройство, версия, срок и статус сервера | Только выбранная семья. Другие семьи и запомненное согласие сохраняются |
| S30 · запомнить согласие | Согласие того же человека для конкретного устройства и версии клиента | Согласие и семьи этой app/device/profile-version tuple. Другие устройства и клиенты сохраняются |

По умолчанию читается **Это устройство**. **Все мои устройства** выбирают явно,
только если текущий сервер объявил такой scope. Это устройства входа человека,
не компьютеры размещения. Срок в карточке относится к подтверждению через Соты;
`expiresAt:null` не означает бессрочный Native вход.

Короткие Root Source-сессии здесь не перечислены. Пустой список не доказывает
отсутствие доступа к Source-приложению. Отзыв issuer authorization блокирует
дальнейшие refresh/userinfo-проверки; он не обещает мгновенное удаление своей
сессии или cookie Native-приложения. У него остаются собственные ACL и logout.

## Проверенный серверный контракт

Сервис: `modules/human-identity/signins.mjs`; типы:
`modules/human-identity/types.d.mts`. Public discovery выполняется через
`GET /api/apps/capabilities → humanSignins`, а не `/api/runtime/config`.
Public capability не является правом: каждый метод проходит текущий подписанный
Connect actor/device и `expectedAccountId`, а list возвращает свои capabilities.
Schema1/unconfigured честно сообщает недоступность; schema2 не включает управление
запомненными согласиями. Поддержка берётся из сервера, не выводится из роли UI.

```text
identity.human.signins.list({expectedAccountId, scope:'current-device', limit:20})
identity.human.signins.revoke({expectedAccountId, scope, entryId,
  expectedRevision:entry.revision, requestId:entry.revokeRequestId})
identity.human.signins.checkRevoke(originalExactIntent)
```

`entryId`, revision и revokeRequestId выдаёт сервер. UI не генерирует новый ID,
не подставляет чужое устройство и не объединяет страницы после snapshot conflict.
Сохранённый server intent остаётся связан с исходным caller device и scope.

## Потерянный ответ и смена профиля

Перед отправкой UI сохраняет один ограниченный pending reference в sessionStorage
этого origin, отдельно для account+device. Ключи устройства, credentials, cookies
и токены не сохраняются. Если storage недоступен, отзыв не отправляется и это
прямо сообщается. Текущие WorldApi/mount generation и server CAS проверяют
исходный профиль/устройство; старый callback после A→B→A не выполняет отзыв.

После reload или повторного открытия выполняется **только readonly checkRevoke**:

- `revoked` подтверждает точный текущий tombstone; это не immutable mutation receipt;
- `active` означает, что запись не отмечена как отозванная, а не Native session-ready;
- `unavailable` после GC/ABA/CAS не подтверждает ни отзыв, ни отсутствие эффекта.

Неизвестный запрос автоматически не повторяется. Пользователь может явно убрать
только локальную проверку; это не меняет серверные входы. Для нового отзыва он
заново выбирает текущую запись и подтверждает показанный scope.

Браузер или сеть могут повторить передачу одного HTTP запроса. Exact server intent
и монотонный отзыв защищают от дополнительного эффекта; это отличается от нового
действия UI. Fixture потерянного ACK обрывает тело после headers, чтобы отдельно
проверить unknown → readonly recovery без транспортного retry Chromium.

## Квалификация

`src/world/human-signins.test.mjs` проверяет реальный UI controller через DOM ports;
`app-account-lifecycle.test.mjs` вызывает настоящий account action и проверяет
device/account generation. Opt-in `node src/world/human-signins.browser.mjs`
поднимает disposable signed RSA/OIDC/SQLite сервис и настоящие компоненты/стили.
Browser RPC bridge принадлежит fixture, это не human acceptance pilot.
Необходимы ширины320/390/1024 в обеих темах, family-only/forget, lost ACK/reload,
свои другие устройства, другой профиль и storage error с нулём отправок.
