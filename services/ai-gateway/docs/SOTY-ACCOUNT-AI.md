# AI аккаунта Сот

Соты используют этот gateway как часть репозитория. Проверенный неизменяемый `issuer/accountId` получает один постоянный tenant, кошелёк и внутренний ключ `purpose=chat`. Внутренний ключ не имеет выдаваемого bearer-секрета. Новое устройство, повтор регистрации, рестарт и reconcile не создают дополнительный лимит. История прежнего общего tenant сохраняется без выдуманного разделения.

Включённый сервисный лимит по умолчанию: 50 ₽ на весь срок, 5 ₽ в день, 50 ₽ в месяц, максимум 1 ₽ на запрос с учётом обоих возможных provider attempts, два одновременных запроса, RPM 30, TPM 100 000, выход до 8192 токенов, 500 000 токенов в день и 3 000 000 в месяц. Это сервисный allowance, без денежного пополнения или долга пользователя. Грант фиксируется отдельной сбалансированной записью `sponsor_grant` / `sponsor_credit`, `serviceFunded=true`, `is_demo=false`.

Глобальный `SERVICE_DAILY_BUDGET_URUB` остаётся прежним, по умолчанию 50 ₽ в день на весь сервис; probe subset тоже не увеличивается. Все ключи аккаунта используют один aggregate policy под блокировкой wallet. Отдельный developer key может иметь более узкие ограничения. Новый ключ, ротация или ошибка поставщика не обнуляют общий расход.

## Приватная конфигурация

`SOTY_BRIDGE_CONFIG_FILE` указывает на server-only смонтированный JSON. Реальных ключей в исходниках и примере нет:

```json
{
  "schema": "soty.ai-gateway-bridge.v1",
  "audience": "urn:soty:ai-gateway:v1",
  "issuers": [
    {
      "issuer": "https://4-2.xn--p1ai",
      "kid": "root-key-version",
      "publicKey": "<Root Ed25519 PUBLIC KEY PEM>"
    }
  ],
  "signing": {
    "issuer": "urn:soty:ai-gateway:production",
    "kid": "gateway-key-version",
    "privateKey": "<Gateway Ed25519 PRIVATE KEY PEM>"
  },
  "root": {
    "origin": "http://soty-online-chat:8080",
    "audience": "urn:soty:root-ai-dispatch:v1",
    "allowPrivateHttp": true
  }
}
```

Направления используют разные Ed25519 пары: gateway знает только публичный ключ Root, Root — только публичный ключ gateway. Для ротации доверенный реестр может содержать несколько явно закреплённых `issuer/kid`. Не используйте provider/API keys, shared management cookie или application bearer вместо bridge keys. Резервные копии приватной конфигурации должны быть зашифрованы.

HTTPS разрешён для фиксированного настроенного Root origin. Явно разрешённый HTTP ограничен `soty-online-chat` и loopback для тестов. URL из запроса, редиректы и произвольный callback path не принимаются. При заданном неверном config процесс завершается с безопасной ошибкой. Отсутствие config оставляет прежний gateway API; signed Soty routes тогда недоступны.

Опциональный `allowance` содержит `grantedMicrorubles` и частичные `limits` с именами `normalizeLimits`. Все итоговые лимиты конечны; денежные значения — целые строки в микрорублях. Конфигурация применяется только при первом provisioning. Изменение файла не пополняет и не сбрасывает уже созданные аккаунты. Прямые изменения account policy увеличивают DB revision; уже ожидающий dispatch со старой revision отклоняется.

## Служебные операции

Dependency-free helper: `packages/soty-bridge/index.mjs`. `createBridgeSigner(...).headers(...)` принимает точные UTF-8 bytes/string тела. Единственный заголовок `x-soty-bridge` содержит `base64url(canonical envelope).base64url(Ed25519 signature)`. Envelope связывает method, exact path/query, body SHA-256, issuer/audience, account/device, operation, requestId, authorizationId, nonce и время. TTL на входе не более 30 секунд. Неверные подпись, scope, body и replay отклоняются до account mutation. Nonce атомарно сохраняется в PostgreSQL, включая работу нескольких gateway процессов и рестарты.

| Метод | Путь | Operation |
| --- | --- | --- |
| POST | `/internal/soty/v1/accounts/ensure` с `{}` | `account.ensure` |
| GET | `/internal/soty/v1/account` | `account.status` |
| POST | `/internal/soty/v1/chat/completions` | `chat.complete` |
| GET | `/internal/soty/v1/requests/:uuid` | `request.receipt` |
| GET | `/internal/soty/v1/keys` | `keys.list` |
| POST | `/internal/soty/v1/keys/create` | `keys.create` |
| POST | `/internal/soty/v1/keys/update` | `keys.update` |
| POST | `/internal/soty/v1/keys/rotate` | `keys.rotate` |
| POST | `/internal/soty/v1/keys/revoke` | `keys.revoke` |

`service:provisioner` может только ensure/status. Служебные principals `service:*` не управляют developer keys. Для inference от приложения Root должен явно закрепить sponsor-account в приватной конфигурации и повторно проверить его исходный token/capability; gateway не придумывает human identity для статического приложения.

Root создаёт долговечную provisioning task после commit аккаунта, периодически обходит существующие аккаунты и исправляет пропущенные mapping. Сетевая операция не должна находиться внутри синхронной регистрации или `modelEligible`. Первый фактический AI call может выполнить single-flight ensure. Отказ bridge не переключает трафик на anonymous/shared/provider bypass.

## Последняя проверка и отмена

После provider queue admission, непосредственно перед **каждым** fetch, gateway повторно проверяет account/key/credential state и snapshot revision. Затем отправляет fresh signed POST на фиксированный `/internal/soty-ai/v1/authorize-dispatch`, operation `dispatch.authorize`, таймаут не больше 3 секунд. Callback включает оригинальный exact request body hash, Root logical requestId, gateway UUID, captured authorizationId, account/device и фактическую пару provider/model. Root возвращает только `{ "authorized": true }` для своего живого captured actor/capability. Отказ, timeout или отмена запрещают dispatch; fallback проверяется заново.

После callback gateway повторно проверяет локальную policy и abort. Уже допущенный provider request считается in-flight: отмена распространяется на очередь и HTTP/SSE, но уже потраченный ресурс может стоить денег. Поздний ответ после abort не становится принятым результатом.

Два provider attempts ограничены общим резервом. Known usage считается по сохранённому тарифу фактической candidate model, включая более дешёвый fallback. При неизвестном dispatched usage measured charge не выдумывается: сохраняется консервативный `quota_urub/quota_tokens/quota_pending`, который блокирует повторное расходование allowance. Истечение lease освобождает никогда не отправленный резерв, а неизвестный отправленный расход остаётся pending. Фоновый `sweep` и поздняя подтверждённая attempt usage выполняют однократную reconciliation кошелька, квот и service budget. Без подтверждённого usage pending не исчезает по таймеру. Inference автоматически не повторяется.

## Ответы и developer keys

Account DTO: `{schema:'soty.ai-account.v1',accountId,state,defaultKey,allowance,usage,models,updatedAt}`. `state` — `provisioning|ready|limited|suspended|unavailable`. Во всех состояниях возвращаются full allowance/usage; `ready` требует активного default key, доступного маршрута с ценой, service budget и allowance. `defaultKey` содержит только `id,name,status,createdAt`.

Публичная model projection содержит до 300 записей и до 384 KiB, только `id,name,status,contextWindow,inputPriceMicrorublesPerMillion,outputPriceMicrorublesPerMillion,capabilities`. Три существующих canonical model IDs идут первыми; provider diagnostics/routes не выходят в account DTO. Этот предел не ограничивает настоящий inference catalog или вычисление readiness. Root принимает status до 512 KiB.

Allowance: `kind:'included',unit:'microrubles',grantedMicrorubles,availableMicrorubles,heldMicrorubles,spentMicrorubles,pendingMicrorubles,limits`. Usage содержит `requests,tokens,inputTokens,outputTokens`; account totals учитывают known attempts, pending estimates показаны отдельно. Receipt хранит исходный `logicalRequestId`; его `input_tokens/output_tokens` относятся к финальному ответу, что сохраняет native correlation. Receipt и все key operations ограничены tenant проверенного аккаунта.

Create input: `{name,limits?,allowedModels?,expiresAt?,autoFallback?}`. Update input: `{keyId,name?,limits?,allowedModels?,expiresAt?,autoFallback?,status?,expectedPolicyRevision?}`; partial fields/limits объединяются с текущей row. Optional CAS возвращает `409 policy_revision_conflict` при устаревшей revision. Rotate/revoke принимают только `{keyId}`. Internal default key недоступен через эти endpoints.

Новый customer key Сот наследует конечный snapshot лимитов аккаунта, включая output для опубликованного клиента. Явные create limits могут только сузить snapshot; `null` наследует account cap. Anonymous defaults сохраняются. Derived defaults не входят в fingerprint исходного create body: повтор после изменения account policy возвращает прежний ключ без второго эффекта.

Новые Soty key policies используют до 100 ASCII model IDs длиной до 240, в том же формате, что provider catalog. Legacy anonymous policy inputs не меняются. Keys list возвращает до 200 metadata records, действующие ключи перед последними отозванными; максимум 100 неотозванных ключей всегда остаются доступными управлению. Root принимает эту metadata response до 8 MiB.

`requestId` mutation используется как бизнес idempotency key. Повтор с новым transport nonce и тем же body возвращает безопасный прежний результат; изменённый body — `409`. Create/rotate выдают `secret` один раз, а replay — `secret:null`. Ключи хранятся только как HMAC fingerprints. Rotation сохраняет key identity и расход, старые credential versions действуют до 24 часов. Revoke немедленно отзывает все версии. Replayed mutation проверяется до CAS/key-state, поэтому потерянный успех не создаёт второй эффект.

## Проверка и ввод

Новая forward migration `006_soty_accounts.sql` сохраняет исходные migrations и ledger. До включения writer необходимо проверить её на изолированной копии БД, применить безопасную конфигурацию, provision существующие account mappings и только затем переключить Root. Старый общий ledger не копируется в новые кошельки. Откат маршрутизации не удаляет созданные tenant/ledger records.

`npm test` при `TEST_DATABASE_URL=.../<dedicated_name>_test` запускает настоящие PostgreSQL/HTTP проверки в отдельных случайных schema с cleanup только этой schema. Без URL DB tests помечены skipped; это не полная проверка. Тесты используют только synthetic локальные providers и signing keys. Есть проверки parallel ensure, повтор/рестарт/другое устройство, A/B isolation, once-only secrets и CAS, JSON/SSE/receipt, aggregate multi-key money/concurrency limits, unknown usage/crash reconciliation, final authorization после очереди и перед fallback, active abort, demo-credit denial, service sponsor/global cap и полного server startup без платного provider.
