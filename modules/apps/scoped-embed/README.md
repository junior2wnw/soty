# Выбранное локальное пространство: закрытый transport profile

`soty.selected-human-embed.v1` — отдельный opt-in поверх существующего
device-scoped apps channel. `soty.relay-restricted.v1` сохраняется без расширения.
Текущие файлы дают проверенные constructor ports и Source SDK; основной Root
gateway/channel/profile readers ещё не подключены к новому режиму.

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
- `createScopedEmbedAuthority({profiles,withAppAuthority})` принимает исходный
  непреобразованный actor из signed Connect, создаёт opaque continuation и на
  `read({reference,connector})` снова вызывает настоящий participant authority
  fence для этого actor/app/entry/target/policy. JSON accountId не создаёт slot.
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

## Нужные точки включения Root

1. `server/schema.mjs` и readers runtime bindings/publication/inspection должны
   принимать новый profile **только** с approved host pin. Старый profile нельзя
   расширить, а Source нельзя объявить whole-port exposure.
2. `server/index.mjs`: под действующим branded session/access check захватить
   original continuation, передать closed context в opt-in `bound-open`, оставить
   повторные проверки stream/chunk/revoke. Authority-read/invalidate и Source
   auth-intent state-digest messages доступны только authenticated exact connector.
3. `scripts/agent-modules/local-apps.mjs`: opt-in target/profile reader и dispatch
   hook после текущей identity/binding/channel проверки вызывают этот broker.
   Не принимать context из браузерного body/header и не добавлять общий proxy.
4. Callback popup не может рассчитывать на iframe CHIPS cookie. До редиректа в
   issuer доверенный Source регистрирует `stateDigest → original continuation`
   через тот же bound channel. Только exact callback/completion получает этот
   контекст и показывает fixed content-free completion. Сам state без server-side
   binding/Source PKCE/nonce не даёт authority; editor/data маршруты закрыты.
5. `src/world/app-stage.ts`: opt-in popup permissions только для approved profile;
   fixed completion сообщает exact-origin opener и обновляет текущий stage.
   Account/generation/abort/dispose guards сохраняются; замена stage отзывает
   предыдущую continuation. По умолчанию popup sandbox не расширяется.

Отдельно нужны actual profile/channel handoff, browser sandbox/CHIPS и HTTPS
проверки перед production включением. Fixture не доказывает безопасность HTTPS.
Human v1 session≤300s без refresh; её продление — другой модуль/следующий gate.

## Доказательства

`test/scoped-embed.test.mjs`:5constructor/MAC/realHTTP tests, включая изменение
заголовков во время ожидания, истечение cookies и запрет неутверждённого redirect.
`test/scoped-embed-source.test.mjs`: actual signed Connect + maintained OIDC +
packaged Planner BFF + native selected-resource consent, source task/state,
profile switch/revoke. App-registry/channel hook здесь — явно synthetic private
constructor fixture, не полный новый gateway.
Существующий `test/connector-runtime.test.mjs`:2actual installed connector
HTTP/WS tests подтверждают сохранение прежнего режима. Optional Source absent
в Linux обязан давать явный skip, а не считаться пройденной Source проверкой.
