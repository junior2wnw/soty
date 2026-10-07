# Selected Source gateway: приёмка и границы

Дата проверки: 07.10.2026. Root base `09288395873174b3a32957905808f8051e69dffc`,
ветка `codex/soty-scoped-gateway`. Исходный Планировщик сохранён отдельно; новый
Source checkout построен от curated source commit
`2fe026e7c3c2e41657016a158306c931a620a6ce`, ветка `codex/planner-scoped-gateway`.
Original `D:\планировщик` и `D:\соты` не изменены. Source не импортируется с внешнего
диска в production package: `server/soty-source-host.mjs` — переносимый artifact.

## Что выполнено

Реальный installed public connector, а не stub channel: signed Apps claim,
registration, Source prepare/promotion, Apps7 immutable admission, private Human
subject projection, Root `apps.launch`, bound-open/control messages, MAC Source
IPC и HTTP, поддерживаемый OIDC RP, Native consent и selected workspace API.
У основного relay остаются прежние HTTP/WebSocket semantics. Selected profile
не включает общий forward и не принимает authority из browser JSON/headers.

Apps7 atomically сохраняет target + approval по `(app_id,target_revision)`.
Migration только startup opt-in. V6 tuple/receipt bytes, foreign keys и прежняя
recognition проверены на literal historical data; независимый Apps7 reader,
literal прежний Apps6 reader и storage START guard проверяются отдельно.
Reader7 нужен до START; старый image не объявляется совместимым после migration.

Browser SDK cleanup — opt-in, private RAM и zero-argument. Original signer/installation
захватываются внутри actual signed launch до await. Cleanup может только закрыть
один exact app/slot/target, со свежей server authentication A. Обычный World API
остаётся expected-account A fenced. B с A handle получает403; JSON clone не
получает capability. SDK switch не выдаётся за серверный device revoke. Реальный
device revoke отдельно запрещает A cleanup и инвалидирует cached slot.

Prepare входа — fixed same-origin POST + CSRF + original slot. Source OIDC
state/nonce/PKCE принадлежат maintained `openid-client@6.8.4`. Expected Root sub
получен из private Human service, а Source независимо проверяет actual RP proof
и current Source ACL. Перед auth response закрепляется one-use stateDigest→slot;
callback/completion работают без предположения о third-party iframe cookie.
Unknown ACK можно явно отменить captured CSRF без auth replay.

## Минимальный путь и общие состояния

Человек: открыть → «Войти через Соты» → при первом подключении «Разрешить только
это пространство» → работать. Повтор к тому же semantic resource binding не
повторяет Native consent. Current profile switch закрывает старый экран;
отзыв Source grant или Root grant запрещает следующий запрос. Данные и обычный
Source вход сохраняются. Human basic proof и Root continuation конечны, до300s;
новый вход возможен, но long RP24h здесь не внедрён.

Исполнимый SDK путь автора: в preverified Source context указать название → получить подготовленный
draft → подтвердить audience. Source/feedback IDs и pins выдаёт доверенный
adapter, а не публичный manifest. Для выбранного локального проекта требуется
операторское approval/profile/key provisioning; этот gate нельзя скрыть за
title-only promise. Продуктовый мастер подключения всех Source/UI авторов здесь
не объявляется законченным по SDK tests. Собственный Source вход остаётся постоянной возможностью.

Агент: найти зарегистрированную capability и typed selected resource → проверить
её текущее состояние → выполнить в её scope. Человеческий OIDC не выдаёт agent
key. Существующий Planner `createWorkItem` означает один title-only объект без
даты; он не обещает расписание, исполнение задачи или вызов платной модели.
Настройка MCP вручную не нужна пользователю этого host adapter; Source transport
и bounded scoped key готовит оператор. Без них результат — явно not-ready.

| Видимое состояние | Проверенный смысл | Действие |
|---|---|---|
| Подключение пока не готово | Нет admission/current connector/approved key/Human config | Владелец завершает подключение |
| Войти через Соты | Транспорт отвечает; Source auth ещё не подтверждён | Подтвердить текущий профиль |
| Разрешить один проект | RP proof есть; selected Native grant отсутствует | Явное согласие владельца Source |
| Открыто | Свежие Root + Source proof + current workspace role прошли | Работать |
| Доступ изменился | Slot/profile/target/grant/role больше не действуют | Открыть заново или обратиться к владельцу |
| Не удалось подтвердить | Сбой/unknown/provider unavailable | Повторить проверку; не auto-retry мутацию |

Registry `ready` — готовность транспорта. Закрытый Source `session-status`
`ready:true` требует fresh proof; 503/unknown/denial никогда не превращаются в ready.

## Воспроизведение

Root broad regression: 642 tests, 640 PASS/0 FAIL/2 explicit opt-in skips.
Оба пропущенных gates затем включены отдельно: installed bundle promotion/restart/
rollback и real130s default WebSocket idle; набор21/21 PASS, skip0. Это отдельный
проход с пересекающимися tests, не сумма661 независимых проверок. Gateway6/6,
Source193/193 и MCP32/32 также пересекаются с более широкими наборами. Root/Source
typecheck+build и connector release/update selftests прошли.

Runtime Node24.19.0, pnpm10.30.0. В Root checkout:

```text
node --test modules/apps/test/scoped-schema7.test.mjs
node --test modules/apps/test/scoped-gateway.test.mjs
node --test modules/apps/test/scoped-embed.test.mjs modules/apps/test/scoped-embed-source.test.mjs
node --test modules/connect/test/authority-fence.test.mjs modules/human-identity/test/storage.test.mjs
node --test deploy/connector/storage-guard.test.mjs deploy/connect/host-controller.test.mjs
pnpm run typecheck
pnpm run build
```

Full channel test package locator: `SOTY_PLANNER_GATEWAY_SOURCE_ROOT`.
Supplemental constructor fixture locator: `SOTY_PLANNER_PROOF_SOURCE_ROOT`.
Их default указывает только managed Source checkout; Linux без optional package
показывает explicit skip. Explicit configured missing package — ошибка. Skip не
считается пройденным Source gate.

В Source checkout: `npm run check`, `npm test` (193/193 текущих tests),
`npm run test:mcp`, `npm run build`. Участвуют migration/restart, baseline/facts,
точные даты/ns, ACL, прежний local/public вход и MCP; новые helpers отдельно
проверяют blocked popup, unknown prepare ACK и запрет foreign authorization URL.

Chrome: сначала `node scripts/fixtures/scoped-gateway.mjs`; fixture сообщает только
safe loopback URL/config path. В disposable browser с выданным config и pinned
Playwright CLI0.1.19 выполнить `run-code --filename=scripts/fixtures/scoped-gateway.browser.mjs`.
Данные, DB, signing secrets и connector installation полностью synthetic/temp.
Fixture-only owner helpers не являются production authorization.

Browser checks: actual Human/Native buttons, committed undated Source write и
retention после Source restart, private/global rejection, один iframe и focus
после Escape в feedback, mobile390 без overflow/с toolbar≥44px, повтор без Native
consent, actual IndexedDB SDK cross-tab switch закрывает stage, fresh revoke403,
other workspace остаётся нетронутым. Root helper controls проверяются keyboard
activation с actual action/network ACK; Source/Human journey — настоящие UI clicks.
Safe receipt и screenshot: `output/playwright/scoped-gateway-acceptance.json`,
`output/playwright/scoped-gateway-runner-mobile.png` (ignored local artifacts).
Это инженерная приёмка синтетического сценария, не тест спроса/удержания людьми.

## Bounds и обязательные оставшиеся gates

Request≤1MiB целиком с multipart overhead; buffered response≤4MiB; headers≤16KiB;
4 inflight; IO8s; 256 slots по5минут; MAC10s; private host-only HttpOnly Source
cookies≤300s. Только reviewed fixed UI/auth/selected state/task/file routes.
Сеть вне SQLite fence; in-flight revoke может оставить unknown effect, поэтому
нет обещания мгновенной distributed atomicity или автоматического write retry.

Production gate ещё требует approved operator provisioning/секреты/rotation,
реальный DNS/TLS/OS trust, pinned Linux Apps7 image и encrypted cold restore,
exact image reader7/fallback rollout. Browser certificate ephemeral и deliberately
untrusted; production HTTPS, deployment и canary этот checkout не выполнял.
Human long-session hook сохраняет Source-owned current-subject порт; renewal
привносится отдельным maintained RP модулем с собственными migration/consent gates.

Следующие Source adapters готовятся тем же порядком: verified source pin → fixed
protocol/schema/resource map → current native ACL → bounded execute/readProof →
actual Source fixture → current channel + browser. Source binding смена не
ретаргетирует прежний durable capability invocation. Transit остаётся read-only;
funds/payouts/платные model calls этим transport не разрешены.

Private-resource feedback: нужен Source-owned current resource/receiver ACL,
верифицируемый context/recipient, scoped durable queue/inbox и typed outcome,
explicit text/voice/screenshot preview+Send, idempotency/lost-ACK/revoke gates.
App-level Root feedback и его Apps7 metadata smoke не заменяют эти права.
Parent media capture без Source receiver ещё не означает отправку обращения.

Managed reviews: нужен реальный provider write/auth/moderation port, approved
namespace+subject binding, durable author/proof semantics, own guest/profile path,
per-call author/tenant/resource grants и actual publish/revoke/receipt tests.
Текущий public-read Поведай renderer и наличие subject manifest не являются
managed write. Private issue, public review и будущая reputation остаются разными
явными получателями/проекциями, без глобального объединения score.
