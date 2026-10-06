# Постоянная регистрация U1

Этот серверный модуль сохраняет предложение приложения, версии descriptor, текущий допуск,
квитанции запросов и feedback intent в `data/app-registration/registry.sqlite`.
Он переиспользует закрытый контракт из `../index.mjs`, но его журнал и CAS постоянны.
Прототипные host handles и fixture receipts не восстанавливаются из JSON и не служат
подтверждением реальной регистрации. Модуль не устанавливает capability grants и не исполняет handlers.

## Интерфейс хоста

```js
import { createUniversalRegistrationService } from './registration.mjs';

const service = createUniversalRegistrationService({
  databasePath, registryId, environmentId,
  actorActive,                 // текущая проверка verified Connect actor
  withReviewedAppAuthority,    // синхронная owner/source транзакция Apps
  reviewedProfile,             // явно выбранный approved profile, не входящий payload
  approvedReferences,          // необязательные approved advanced registries
  feedback,                   // необязательный синхронный реальный persistence adapter
});
```

`reviewedProfile` имеет точную форму `{id,version,digest,feedback}` из U1 author profile.
Все три service pins в `feedback` одобряет платформа. Из них строятся реестры известных
kinds `feedback`, `capture`, `retention`; первый случайный провайдер не выбирается.
`approvedReferences` содержит необязательные массивы `bindings`, `providers`, `profiles`,
`skills`, `docs`, `placements`, `publicSubjects` с прежними закрытыми U1 формами.
История этих refs постоянна: тот же ID/version нельзя переписать, в том числе после удаления
из текущей конфигурации и повторного добавления. Capability semantic pins хранятся отдельно.

`withReviewedAppAuthority({actor,appId}, callback)` проверяет текущего владельца и удерживает
Apps source/publication authority до завершения `callback`. Он вызывается из действующего
Connect authority fence и передаёт только trusted snapshot:

```js
{
  appId, ownerId, accountId, appRevision, policyEpoch,
  target: { revision, digest, profile },
  visibility: 'private', // либо public по текущей политике Apps
  grants: { accountIds: [], communityIds: [] },
}
```

Сервер строит scope `{registryId,tenantId:actor.accountId,appId,environmentId}`, namespace
`app.<appId>` и source `{id:'apps:<appId>/target',revision,digest}`. Source digest закрепляет
действующую runtime target tuple; это не доказательство происхождения исходников или artifact.
`auth:{mode:'public'}` описывает собственную аутентификацию приложения и не утверждает
внедрение shared Soty login. Доступ к оболочке Apps продолжает проверяться существующей системой.
Owner/source/scope/profile никогда не извлекаются из payload или HTTP Host.

## Signed операции

`service.operations` содержит четыре операции для существующего Connect extension:

| Операция | Аргументы | Результат |
| --- | --- | --- |
| `apps.universal.plan` | `expectedAccountId,appId,requestId,expectedRevision,proposal` | проверенный план; grants не создаются |
| `apps.universal.admit` | те же поля | `{registration,receipt,replayed}`; начальное состояние `pending-feedback` |
| `apps.universal.get` | `expectedAccountId,appId` | `{registration:null\|registration}` |
| `apps.universal.history` | те же поля, `limit?,cursor?` | bounded owner-only digest metadata и `nextCursor` |

Простой `proposal` — `{kind:'author-draft',draft:{title,schema?}}`. В уже проверенном контексте
платформы автору достаточно названия. Scope, source, visibility, pins и UI-only defaults задаёт
платформа; capabilities/skills/docs пусты, reviews disabled.
Advanced proposal — `{kind:'descriptor-json',json:'...bounded JSON...'}`. Строка позволяет
отклонить duplicate decoded keys до `JSON.parse`, включая escaped equivalent keys.
Advanced capabilities требуют exact approved binding и semantic pin, но всё равно не получают
grants. Feedback обязан совпадать с явно выбранным `reviewedProfile.feedback`.

`revision` — CAS текущего head. `generation` меняется только при принятии нового предложения.
Подтверждение feedback увеличивает revision и сохраняет generation. Совпадающий requestId
после потерянного ответа возвращает ту же immutable receipt; текущая регистрация может уже
быть `ready`. Изменённые предложение, appId или expectedRevision при том же requestId конфликтуют.
При смене current source/profile authority прежний replay отклоняется.

History не содержит descriptor, приватных grants или текста заявки. Курсор закрепляет scope,
а каждая страница заново проверяет владельца. При `authority-stale` текущая проекция удерживает
UI gate и не выдаёт прежний descriptor как действующий.

## Feedback commit boundary

Публичной операции `ready/confirm` нет. `service.reconcileFeedback({actor,appId,expectedRevision})`
вызывает только trusted host orchestration под внешним Connect fence. Реальный sync adapter:

```js
feedback = { ensureInstallation(request), inspectInstallation(request) };
// request:
{ scope, source, profile: reviewedProfile, provisioningKey,
  intentDigest, authorityDigest, generation }
// committed receipt, либо null если доказательства нет:
{ installationId, receiptDigest, provisioningKey, scope, source,
  profile: { id, version, digest }, authorityDigest, generation }
```

Используется `inspect → ensure при отсутствии → inspect`. Exact matching committed evidence
разрешает `gates.ui:'ready'`; `agent/local` остаются `not-admitted`. Предшествующий persisted
`ready` повторно сверяется с реальным store при выдаче: отсутствие adapter/proof даёт
`feedback-held`, `ui:'held'`, подменённая receipt — конфликт. Это локальная проверка
persistence evidence, без сетевого health probe и обещания доступности всех UI компонентов.

Порядок блокировок: **Connect → World (если применим) → Apps → Registration → Feedback**.
Callbacks выполняются один раз, синхронно, без network awaits и рекурсивного захвата предыдущих
fences. Два SQLite store не образуют атомарную транзакцию: Feedback commit может сохраниться
при Registry rollback или потерянном ACK. Стабильный provisioning key и повторный inspect
позволяют восстановиться; exactly once не обещается. Intent/outbox не изображает готовый inbox,
worker, ASR, screenshot capture, MCP или общий SSO.

## Границы эксплуатации и проверки

По умолчанию: 128 scopes владельца, 4096 immutable request receipts владельца, 256 версий
на приложение, 4096 записей reference history, страница history максимум 50. Переполнение
отклоняет новые записи, сохраняет прежние квитанции и допускает exact replay. Автоматической
очистки idempotency/history нет. `close()` закрывает connection; постоянную историю не удаляет.

Schema v1 имеет exact layout/guards и immutable registry/environment metadata. Независимый
host storage probe должен закрепить эту схему отдельно; новые reader `appRegistration:[1]`,
snapshot/backup и writer gate обязательны для выпуска. Это не полномочие самого candidate
аттестовать свою совместимость. Production composition, текущие sources и реальный feedback
store проверяются отдельной серверной интеграцией.

```powershell
node --test modules/app-contract/test/durable.test.mjs
node --test modules/app-contract/test/*.test.mjs
```

Durable suite использует временные SQLite базы и два независимых Node процесса, проверяет
restart, lost ACK, competing CAS, чужой scope/source, immutable pin history, готовность только
по real-adapter evidence, квоты и pagination. Она не вызывает настоящие provider API,
пользовательские очереди или production execution. Runtime — Node с поддержкой `node:sqlite`;
для всего серверного приложения соблюдается текущий `package.json` engine.
