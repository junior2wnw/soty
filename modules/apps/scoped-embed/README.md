# Выбранное локальное пространство: закрытый transport profile

`soty.selected-human-embed.v1` — отдельный opt-in поверх существующего
device-scoped apps channel. `soty.relay-restricted.v1` сохраняется без расширения.
Проверен полный локальный путь signed Root Apps → установленный коннектор →
fixed broker → настоящий Планировщик → OIDC → Native consent → iframe.
Это локальная реализация в isolated worktrees, без production переключения.

Для человека: «Подключить выбранный проект» → профиль Сот → отдельное согласие
в исходном приложении на одно пространство → «Открыть». Повторный вход сохраняет
ту же связь. «Отключить от Сот» закрывает continuation/сессии коннектора; исходное
приложение, обычный вход и данные остаются. Менять профиль в середине согласия
нельзя: намерение сохраняет исходный профиль/проект, а новый запуск имеет новый
контекст. Native consent показывает профиль и точные разрешённые изменения.

## Приватные порты

- `scopedEmbedProfile(profile)` принимает только approved host configuration:
  конкретные appId, connector identity, target tuple pin, Source profile pin,
  typed selected resource, issuer/clientId, embed/native/parent origins.
- `createScopedEmbedAuthority({profiles,withAppAuthority,withHumanSubjectAuthority})` принимает исходный
  непреобразованный actor из signed Connect, создаёт opaque continuation и на
  `read({reference,connector})` снова вызывает настоящий participant authority
  fence для этого actor/app/entry/target/policy. Private Human port проецирует
  expected issuer/sub и current client generation под Connect fence. Он не
  создаёт Source permission или RP proof. JSON accountId не создаёт slot.
- `createLocalScopedEmbedBroker(...)` вызывает один утверждённый локальный порт,
  проверяет исходную authority до/после IO и текущий local binding. BFF cookies
  хранятся приватно по continuation; native `planner_session` не передаётся.
- `createSourceProofVerifier(...)` проверяет MAC полного закрытого контекста,
  app/profile/resource/continuation, method/path, body/cookie digest, срок и
  одноразовый nonce до Source middleware. Ключ — только приватный host mount.
- `createSourceCurrentSubjectPort(...)` независимо требует **настоящий RP proof**
  с fresh userinfo из поддерживаемой OIDC библиотеки и связывает issuer/sub с
  текущим Root launch. Сам Root accountId, MAC envelope или HTTP header не
  являются доказательством входа человека. Native consent повторно проверяет
  именно исходную continuation; picker/email/name не дают доступ.
- `sourceConsentDigest(profile)` отдельно фиксирует durable native grant к
  app/resource/semantic Source profile. Issuer/sub → Source account association
  не является этим grant. Новое согласие нужно при изменении semantic binding;
  новый launch/device или UI deployment с той же binding не добавляет запроса.

Source SDK entry — `source-host.mjs`. Он собирается в переносимый Node ESM artifact
без внешнего пути к репозиторию. Планировщик использует optional `embed.bridge`
`verifyRequest(req,body)`/`continuation(req)` и передаёт сохранённый actual proof и
continuation в `currentSotySubject(req,expected)`. Они хранятся вместе с PKCE/proof/
session/native intent зашифрованными в Source. Nonce store должен атомарно и
устойчиво к restart потреблять nonce, удалять истёкшие и ограничивать объём.
`embed.bridge.consentDigest` задаётся только approved host profile; выдача и отзыв
Source grant атомарны. Отзыв сохраняет immutable association с Source account.

## Границы прототипа

Только `/embed`, фиксированные production assets, Source auth callback/completion,
selected state/search/history/audit, objects и files. Global/legacy state/auth,
keys, workspaces, configuration, arbitrary URLs/commands и WebSockets запрещены.
Source текущие membership/roles остаются обязательными. Транспорт не повторяет
изменение после потери ответа; ошибка ответа не доказывает отсутствие эффекта.

Запрос ≤1 MiB, ответ ≤4 MiB полностью буферизуется до свежей authority проверки,
Заголовки ≤16 KiB, 4 inflight, timeout8s, 256 continuations по5минут, MAC proof10s. Браузерный Origin
нужен для записи и точно совпадает с approved embedOrigin. Только три Source BFF
HttpOnly host-only cookies со сроком≤300s; истёкшие cookies и continuations удаляются.
Source credentials и keys не входят
в descriptor, context frame или публичный API. В первом файловом пилоте весь
multipart-запрос должен поместиться в1 MiB вместе со служебными полями; больший
запрос получает413, даже если обычный Source допускает больший файл.

## Реальные точки включения Root

1. `server/schema.mjs` и readers runtime bindings/publication/inspection принимают
   profile **только** с exact immutable approved admission. Apps7 включается
   явным startup migration; v6 rows/tuple bytes/receipts сохраняются. Literal old
   Apps6 reader отказывает до START; новый independent Apps7 reader и portable
   storage probe проверяют marker/DDL/guards/tuples/admissions отдельно от current
   application code. Старый profile и whole-port exposure не расширены.
2. `server/index.mjs`: под действующим branded session/access check захватывается
   original continuation и передаётся в opt-in `bound-open`; сохранены
   повторные проверки stream/chunk/revoke. Authority-read/invalidate и Source
   auth-intent state-digest messages доступны только authenticated exact connector.
3. `scripts/agent-modules/local-apps.mjs`: opt-in target/profile reader и dispatch
   hook после текущей identity/binding/channel проверки вызывают этот broker.
   Private `SOTY_SELECTED_EMBED_HOST_FILE` задаёт approved profile и host key;
   Root `SOTY_SELECTED_EMBED_REGISTRY_FILE` закрепляет те же reviewed pins.
   Browser body/header/Host не выбирает destination, command или authority.
4. Callback popup не может рассчитывать на iframe CHIPS cookie. До редиректа в
   issuer доверенный Source регистрирует `stateDigest → original continuation`
   через тот же bound channel. Только exact callback/completion получает этот
   контекст и показывает fixed content-free completion. Сам state без server-side
   binding/Source PKCE/nonce не даёт authority; editor/data маршруты закрыты.
5. `src/world/app-stage.ts`: opt-in popup permissions только для approved profile;
   user gesture открывает blank popup до await, same-origin POST prepare требует
   current original slot + one-use CSRF. Fixed completion не показывает editor;
   bounded session-status обновляет iframe даже при сохранённом Root COOP.
   Account/generation/abort/dispose guards сохраняются; замена stage отзывает
   предыдущую continuation. По умолчанию popup sandbox не расширяется.

Actual installed-channel handoff и Chrome sandbox/CHIPS path проверены. Отдельно
нужны production DNS/TLS/OS trust, Linux Apps7 image и encrypted cold restore
перед включением. Ephemeral localhost certificate не доказывает безопасность HTTPS.
Human v1 session≤300s без refresh; её продление — другой модуль/следующий gate.

При смене профиля обычные World reads/mutations остаются original-account fenced.
Opt-in browser SDK захватывает signer до сетевого await `apps.launch` и хранит
private RAM zero-argument cleanup capability для fixed `apps.scoped.close` одного
slot. A cleanup подписывает только A; B с украденным A-handle получает403.
JSON clone не получает callback, stale launch ACK закрывается исходным signer,
dispose удаляет callback/keys; срок не дольше slot. Отозванный device не может
подписать cleanup, cached slot инвалидируется сервером независимо.

Source prepare ACK может быть неизвестным: blank popup закрывается, а explicit
cancel использует только captured CSRF. StateDigest регистрируется до ответа;
callback после cancel/close/switch/promotion не создаёт новый slot. Запись
автоматически не повторяется. Target/catalog metadata сами права не выдают.

## Доказательства

`test/scoped-embed.test.mjs`:5constructor/MAC/realHTTP tests, включая изменение
заголовков во время ожидания, истечение cookies и запрет неутверждённого redirect.
`test/scoped-embed-source.test.mjs`: supplemental constructor fixture actual
signed Connect + maintained OIDC + packaged Planner BFF. Она требует текущий
Source SDK с Human principal contract; прежний constructor artifact не подменяет
этот pin. `test/scoped-gateway.test.mjs`: полный actual installed channel/Source,
SDK cross-tab switch/stale ACK/foreign handle/device revoke, unknown prepare ACK,
exact Apps7 U1 + mandatory feedback metadata и missing-admission no-fallback.
`scripts/fixtures/scoped-gateway.browser.mjs`: настоящее Chrome UI, Source write
и restart, повтор без Native consent, mobile390, focus/iframe, cross-tab SDK switch
и fresh revoke. Только synthetic accounts/temp DB, без production данных.
Существующий `test/connector-runtime.test.mjs`:2actual installed connector
HTTP/WS tests подтверждают сохранение прежнего режима. Optional Source absent
в Linux обязан давать явный skip, а не считаться пройденной Source проверкой.

Команды, состояния подключения и оставшиеся обязательные Source/provider gates —
[acceptance.md](./acceptance.md). Manifest не объявляет private-resource feedback,
managed reviews или другой Source adapter готовыми.
