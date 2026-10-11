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

Для HIVE `createSelectedHiveSourceApplications(scope)` сохраняет два прежних
контракта (IDs/заголовки не более 100 ячеек и title patch) и добавляет создание
карточки и bounded контекст для её вставки. Это прежние HIVE GET project, POST operations и
GET changes — без нового transport, auth, таблиц или whole-project replacement.
Title patch меняет только `object.update.patch.title`. Создание исполняет
`object.add` + `occurrence.add` в одном атомарном Native batch/COMMIT, добавляя
один объект и одно отображение в существующий вид; прежние связи/placements
сохраняются. При нескольких видах без current-view pin создание не предлагается:
этот профиль не передаёт выбранный вид, выбирать первый было бы ошибкой.
Исходный durable change должен иметь exact request ID/operations/project и
revision `expectedRevision + 1`. POST replay возвращает latest revision и не
является receipt конкретного действия. После независимого COMMIT proof лишь
turn-local ожидаемая revision продвигается на один шаг при fresh Native context
с теми же title/role. Original actor/device/handle/scope/pins не меняются.

Runtime сохраняет один tool + terminal. Host-only `effectCapabilities` разрешает
`source_write_1` и два установленных aliases `hive_card_create` /
`hive_card_update_title`, дополнительно к обычному allowlist. Native
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

## Одна человеческая просьба: исправленный пробел

В исходном `2cebc883...` первый model request не содержал actual references,
revision или IDs ячеек. Вызов `source_catalog` уже расходовал единственный tool,
а прежний HIVE fake заранее знал ref/ID. Тесты того slice доказывали executor,
receipt и isolation, но не удобство выполнения одной человеческой просьбы.

Теперь перед первым model request сервер читает bounded `hive.cells.context`
через тот же metered query ledger (одна invocation, без cache/private body).
Только exact текущие grant/contract/window и свежая Native revision позволяют
предложить `hive_card_create` с единственным аргументом `title` либо
`hive_card_update_title` с `id` из verified schema enum и `title`. ID/title список
в schema обозначен данными, не инструкциями. Reference, handle, expectedRevision,
Native view/position и стабильный новый object ID держит сервер. Aliases вызывают
прежний `source_write_1` executor; дополнительного model loop/dispatcher нет.
Без grant/context alias не становится новым разрешением. Старые list/title
контракты и generic tools сохранены; новые refs/context/create grants должны
быть явно включены operator CODE. После unknown запрещён новый mutation retry.

`selected-hive-natural-request.test.mjs` проверяет реальный signed agent →
installed HTTP/WS → Native SQL: «Создай карточку “План запуска” в открытом HIVE»
даёт одну новую revision, object и видимое occurrence, original receipt и reload.
Fake provider не замыкает fixture/refs/handle/revision: всё необходимое берёт
только из actual prompt/tools. Аналогичный single-turn title edit выбирает ID
по имени из schema, а не по заранее известному ID. Two-view и revoke-during-model
случаи не создают карточку. Пустое поле (0 objects / 1 view) принимает просьбу
«Создай карточку в открытом HIVE»: одна новая карточка/occurrence переживает
Native restart, затем отдельная просьба по имени меняет её title, сохраняя один
объект и placement. Повтор outer request требует existing receipt recovery
(`agent_reconcile_required`), не повторный эффект. Это local synthetic model gate,
не production или доказательство качества реальной модели.

Это два HIVE-specific aliases. Universal Standard3 executor/ports остаются
существующей основой; natural typed tools для всех других приложений этим
пилотом не доказаны. Новый local freeze: 182/182 focused и 9/9 actual Native
tests, 0 skips; typecheck PASS. Два прежних HIVE catalog contracts сравнены
с исходным `2cebc883...`: exact equal. Секреты/config/Native/live не менялись.

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
node --test --test-concurrency=1 server/integration-test/selected-hive-personal-agent.test.mjs server/integration-test/selected-hive-natural-request.test.mjs
```

Это actual Native/RP/SQL + installed connector gate с synthetic 24h consent и
local synthetic model/billing. Проверены title patch, durable original receipt,
revision advance, Native restart, latest-replay trap, чужая concurrent revision
и Native revoke. Это не production/Chrome/paid-model acceptance.

Первый локальный freeze `2cebc883...`: 180/180 focused tests, 0 skips;
4/4 actual action tests, 0 skips; штатный typecheck PASS. Результат дополнительного
natural-request slice записан отдельно в `personal-hive-natural-request-local-receipt-20261011.json`.
Узкий независимый review HIVE receipt/revision
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
