# Личный агент: действия в выбранном Native окне

Основа Root: `046fec493b48f5ce4f29ffb2511f234830c1b901`. Этот slice подготовлен
в отдельной ветке; его проверка не включает публикацию или включение live Source.

Установленное приложение даёт один обычный bounded catalog и свой existing
Native `read`/`execute`/`readProof`. `createSelectedNativeSourceApplication`
оборачивает его в прежний External coordinator. Ровно подписанный UI selection
выбирает `appId + handle`; модель получает Native schema и reference, без handle,
Source cookies, ключей, Root actor или private input digest. Другое ready окно
не подставляется. Нужны current Root credential/grant, original Source session,
точные target/profile/resource pins и текущий Native доступ одновременно.

Для ordinary Source3 используются только существующие `/api/embed/query`,
`/api/embed/invoke`, `/api/embed/receipt`. Root request ID остаётся прежним;
при receipt проверяется Native digest внутреннего input, затем доказательство
связывается с Root digest полного выбранного input. Operator CODE задаёт Native
schema и синхронный `projectReceipt` для typed effects/artifacts. Descriptor
сам по себе не даёт исполнителя или прав.

Для HIVE `createSelectedHiveSourceApplications(scope)` даёт два узких контракта:
прочитать IDs/заголовки не более 100 ячеек и изменить заголовок одной существующей
ячейки по прочитанной revision. Это прежние HIVE GET project, POST operations и
GET changes — без нового transport, auth, таблиц или whole-project replacement.
Изменяется только `object.update.patch.title`; связи и placements сохраняются.
Исходный durable change должен иметь exact request ID/operations/project и
revision `expectedRevision + 1`. POST replay возвращает latest revision и не
является receipt конкретного действия. После независимого COMMIT proof лишь
turn-local ожидаемая revision продвигается на один шаг при fresh Native context
с теми же title/role. Original actor/device/handle/scope/pins не меняются.

Runtime сохраняет один tool + terminal. Host-only `effectCapabilities` разрешает
ровно установленный `source_write_1`, дополнительно к обычному allowlist. Native
права и бюджет проверяет прежний ledger. `source_receipt_1` — только чтение:
original invocation ID + reference + Native input. Приватный boolean ledger
check сопоставляет полный original selected input до reconciliation; input и
его digest не возвращаются. Новый handle/contract/изменённый input отказывают.
При timeout/потерянном ACK результат остаётся unknown, пока original readProof
не подтвердит COMMIT. Reconciliation не повторяет effect.

Операционная граница и лимиты сохранены: finite grants/budget; прежняя квота
10 admissions/min на principal; bounded outstanding promises; до 64 KiB Native
response в этом port; до 8 разных selected contracts/intents за turn. Ledger
не гарантирует распределённый rollback после чужого COMMIT. После закрытия
окна/отзыва согласия late ACK не становится ответом текущего окна. Новый turn
не должен повторять unknown действие новым mutation ID: следует читать original
receipt. Утрата opaque invocation ID не создаёт автоматического resend.

## Что нужно для live подключения

В существующий hash-approved `SOTY_INSTALLED_APPLICATIONS_MODULE` operator CODE
добавляет metadata factory и Native factories для exact approved scope. API
entry экспортирует обе функции; полученные rows идут в existing
`httpOptions.externalApplications`, их captured references — в
`sourceReferences`. `sourceCredentialFor({actor,reference})` возвращает private
current credential/audience существующего explicit finite grant этого account
и device. Tokens/Source cookies не помещаются в catalog, model, author JSON или
response. Модуль не генерирует production credentials и не меняет Native ACL.

Для HIVE нужен exact existing selected profile2, выбранный Native проект и
согласие Native на write. Native build должен поддерживать negotiated bounded
current-context title/revision (проверенный source `dc5e734e4ce7fd44103ca0ae3755211284543334`).
Generic Source3 не заменяет HIVE profile2. Live selected registry/module здесь
не включался. Перед live action parent должен review/install эту operator
composition и проверить настоящий выбранный HIVE field; paid model и browser
acceptance — отдельные шаги.

## Воспроизводимые локальные проверки

`modules/source-app/test/selected-native-installed.test.mjs` — actual installed
HTTP/WS + Source Native SQL: create→receipt→list/restart; два apps и два окна;
foreign handle/ref, закрытие original окна, Root/Native revoke, lost/late ACK.
`selected-native-personal-agent.test.mjs` — signed personal agent + actual Native
SQL, включая недоступный receipt после COMMIT: original reconciliation возвращает
receipt, HTTP invoke остаётся ровно один.

HIVE запускается отдельно, без fake Source и без skip:

```powershell
$env:SOTY_HIVE_NATIVE_SOURCE_ROOT = '<owned built HIVE checkout>'
node --test --test-concurrency=1 server/integration-test/selected-hive-personal-agent.test.mjs
```

Это actual Native/RP/SQL + installed connector gate с synthetic 24h consent и
local synthetic model/billing. Проверены title patch, durable original receipt,
revision advance, Native restart, latest-replay trap, чужая concurrent revision
и Native revoke. Это не production/Chrome/paid-model acceptance.

Локальный freeze: 180/180 focused tests, 0 skips; 4/4 actual action tests,
0 skips; штатный typecheck PASS. Узкий независимый review HIVE receipt/revision
не нашёл blockers; он не является полным audit всех transport/security paths.
Широкую suite целиком green не объявляем: в данном Windows окружении отсутствует
maintained Planner renewal package, а legacy OAuth consent GET возвращает 400.
На exact неизменённом Root 046 отдельно повторены 26 тех же failures в двух
группах (28 tests, 2 pass, 26 fail). Изменения unrelated Planner/Auth не делались.

Для HIVE unknown после собственного Native COMMIT изменившаяся revision
блокирует terminal текущего turn до независимого recovery. Original operation
остаётся в existing owner invocation history/ledger; loss of outer agent ACK
не создаёт автоматического retry. Этот slice не добавляет recovery UI для
утраченного locator или автоматический перенос receipt на новый handle/grant.

Скопированный public fixture происходит из HIVE `dc5e734...`:
`tests/fixtures/selected-root-http.mjs` и `soty-clock.mjs`. Root path безопасно
декодируется через `fileURLToPath`; optional constructor bindings служат только
тесту. Child environment закрыт от ambient application credentials. Teardown
учитывает `signalCode`, чтобы не ждать повторный exit уже завершённого child, и
закрывает все свои HTTP/WS sockets. Source checkout не изменяется.
