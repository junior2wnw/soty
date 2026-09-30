# P4-A — публичное описание возможностей и проверяемый контракт

Дата: 30.09.2026. Статус: **контракт принят root после D4; реализация A1 начинается отдельно**, не реализованный публичный API. Изменён только этот документ. [Мастер-план](../plans/soty-human-agent-platform-20260930.md) прочитан целиком в предыдущем проходе; при возобновлении сверены §6–11/P4 и текущие catalog/validation/access/composition. Основание последовательности A/B/C/D — [P4 preflight](p4-external-agent-preflight.md). R3/R4 имеют отдельный checkpoint; этот документ не разрешает production, установку пакетов или миграцию Notes/Capabilities.

## 1. Минимальный законченный результат A

Человек, браузерный ИИ и независимый HTTP-клиент могут открыть `/agents`, найти описание создания личной записки на русском или английском, получить **ту же** конкретную версию схемы, сверить digest и увидеть, что выполнение ещё выключено. Весь сценарий работает без входа, без SPA и без обращения к пользовательским Notes. Это пригодный для использования discovery, но ещё не create→receipt сценарий.

В A входят только public search/get, output validation существующего профиля, документация ограничений, OpenAPI3.1.2, HTML и прямые machine links. HTTP POST создания, Invocation execution, OAuth/MCP, private catalog, новые форматы хранения и proof двух ИИ-клиентов остаются за B/C/D. SDK/AS dependencies здесь не устанавливаются: ранняя фраза preflight о lockfile в A уточняется до фиксации выбора в плане; фактический lockfile и adapter conformance — в C при их использовании.

## 2. Что реально существует и где план требует уточнения

| Проверенный исходник | Факт и решение |
| --- | --- |
| `modules/capabilities/server/catalog.mjs` | `notes.createDraft@1` — input ровно `{title,body}`, output ровно `{noteId,revision}`, `executionEnabled:false`. Не добавлять idempotencyKey/Invocation/URL в бизнес-схему. Широкий пример §8 master уточняется транспортным envelope B, а не редактированием @1. |
| Там же | Digest содержит title/description/visibility/schemas/resources/effects/recipients/binding, исключает только operational `executionEnabled`. Добавленные `digest`/`charges` также не входят в исходный semantic contract. RU/EN инструкции и readiness — отдельная sidecar projection. |
| `index.mjs` | Public фильтр уже выполняется до поиска/count/paging. Однако search отдаёт полные схемы, ищет только подстроку в ID/RU title/description и не ограничивает ответ UTF-8 байтами. Это исправляется в одном public view, без второй БД/registry. |
| `catalog.mjs`/`validation.mjs` | Есть bounded input validator, но нет output validator. Это собственный ограниченный профиль, не полный JSON Schema engine. Строки проверяются через UTF-16 `.length`; lone surrogate проходит. Профиль @1 нельзя молча заменить. |
| `access.mjs` | Opaque actor создаётся после проверки credential/audience; актуальные principal/creator device/grant ancestry проверяются повторно. Но private descriptor не имеет ownership/discovery ACL, а host owner может выдать grant на любой известный ID. Наличие grant само по себе поэтому **не доказывает право обнаружения private descriptor**. |
| `server/http-app.js` | Есть только `/api/capabilities/v1/status`, по умолчанию false/null. Catalog/HTML/OpenAPI routes отсутствуют, неизвестные GET обычно попадают в SPA fallback. Новый API namespace обязан давать JSON404/405 вместо HTML200. |

Read-only Node probe без БД подтвердил digest `95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204`. Заголовок из80 `😀` принят (160 UTF-16 units),81 отклонён; lone high surrogate принят; NUL отклонён; LF принят. Это факт текущей реализации, не желаемый Unicode contract внешнего write.

## 3. Одна модель и точный public API

`createCatalog` остаётся единственным реестром CapabilityVersion и validator. Существующий inline public view из `index.mjs` выделяется в `modules/capabilities/server/discovery.mjs`; сервис по-прежнему экспортирует `catalog.search/get`. Сам registry не становится общедоступным. Новый renderer и OpenAPI builder потребляют только public projection.

Предлагаемые чистые exports:

```js
createPublicDiscovery({ catalog, documentation })
// search({query='',limit=10,cursor?})
// get({capabilityId,version})
// contract({capabilityId,version}) -> {canonicalJson,digest}
// schema({capabilityId,version,kind:'input'|'output'}) -> exact schema

createCatalog(...).validateOutput(entry, result) // same bounded profile as input
buildDiscoveryOpenApi()                       // local refs, public GET only
renderDiscoveryIndex({result,query,origin})
renderCapabilityPage({detail,origin})
```

`documentation` — массив server-owned sidecars с ключом `{capabilityId,version,contractDigest}`. Для каждой public версии обязательно ровно одно явное описание: отсутствие даёт `documentation_missing`, дубликат — `documentation_invalid`, без общего fallback или выдуманного перевода. Изменение документации не меняет cap_contracts и digest. Несовпадение relevant sidecar digest отвергается как `documentation_contract_mismatch` при построении view, до открытия SQLite/pins. Private/отсутствующие версии отбираются вне public view: их sidecars не участвуют в публичной валидации, лимитах, revision или выдаче. Сопоставление выполняется один раз при constructor; per-query обхода исходной конфигурации нет.

Sidecar содержит RU/EN короткое название/назначение/поисковые фразы, примеры input, случаи «не подходит», объяснение сохранности и ограничений. `examples:[]` допустим для общей capability, если автор не предоставил пример; максимум128 примеров на язык внутри общего16KiB бюджета. Пилот Notes обязательно содержит проверяемый пример в обоих языках. Input каждого предоставленного примера проходит **существующий** validator. Примеры результата помечены как иллюстрации и проходят новый output validator; это не свидетельства выполненного вызова. Никаких авторских URL для fetch/import.

`revision` выдачи и документации — SHA-256 hex64, не timestamp. `links` имеет только `html`, `detail`, `contract`, `inputSchema`, `outputSchema`: относительные server-generated пути. Sidecar detail содержит `{revision,contractDigest,locales:{ru,en},validation}`, где locale — `{title,summary,useWhen,notFor,examples}`, example — `{input,output}` с явной пометкой «пример» в HTML; `validation` описывает профиль из §4. Keywords нужны только bounded search index, не загружаются во все ответы. Public строки descriptor/sidecar проверяются на well-formed Unicode перед рендером; ошибка server-owned public configuration закрывает её публикацию, не подменяет символы.

### Маршруты A

| Метод/путь | Ответ |
| --- | --- |
| `GET /api/capabilities/v1/catalog?query=&limit=&cursor=` | `{scope:'public',revision,items,total,cursor}` |
| `GET /api/capabilities/v1/catalog/:id/versions/:version` | `{scope:'public',capability,documentation,links}`; прежний `capability` содержит schema/digest/operational flag |
| `GET /api/capabilities/v1/catalog/:id/versions/:version/contract.json` | Только canonical JSON исходного semantic contract; SHA-256 его UTF-8 bytes равен digest, без wrapper/newline |
| `GET /api/capabilities/v1/catalog/:id/versions/:version/schemas/:kind` | Exact исходный input/output schema; `kind` только `input` или `output`, JSON без добавления `$id`/`$schema` в pinned object |
| `GET /api/capabilities/v1/openapi.json` | OpenAPI3.1.2 описывает только реально реализованные read routes и существующий status |
| `GET /agents` и `GET /agents/capabilities/:id/versions/:version` | Semantic SSR index/search и version page, без обязательного JavaScript |
| `GET /agents/sitemap.xml` | Только канонические public HTML URLs, без search/cursors, частных объектов или пользовательских runtime origins |
| `GET /api/capabilities/v1/status` | Существующий DTO сохраняется; A сообщает `notesCreateEnabled:false,audience:null` |

HEAD повторяет статус/заголовки GET без body. Неизвестный route в API namespace — JSON404; известный route с неподдерживаемым методом —405 с Allow. A не добавляет фиктивные POST/create/auth/MCP paths в OpenAPI. Описываемый `notes.createDraft@1` пока можно только изучить; кнопки исполнения/подключения отсутствуют. Будущий transport envelope `{invocationId,status,result?,openUrl?}` описывается как предстоящий, не как текущая API operation.

OpenAPI использует `security:[]` для этих public GET и локальные `$ref` на response/error types; `servers:[{url:'/'}]` не зависит от Host. Встроенная business schema доступна по versioned links, а не замаскирована под уже существующий POST. Не рекламировать весь контракт wrapper как `application/schema+json`: wrapper — обычный JSON. У отдельных schema responses явно указана поддерживаемая dialect/profile в документации, исходные bytes схемы не дополняются.

### Search, paging и предельная работа

Public candidates отбираются **сначала**; private metadata не участвуют ни в haystack/ranking, ни в revision, total, cursor, ETag или sitemap. Bearer/cookie на HTTP public endpoints не меняют этот view. Аргументы чистых методов точные: search только `{query?,limit?,cursor?}`, get/contract только `{capabilityId,version}`, schema добавляет только `kind`; `actor`, `accountId`, `grantId` и прочие неизвестные ключи отвергаются как `invalid_input`, а не принимаются с молчаливым игнорированием. На HTTP также запрещены неизвестные query keys/дубликаты; scope ответа всегда public. Авторизационные данные не отражаются в ответе/логах.

Пилот остаётся ≤128 server-owned versions. Query — well-formed Unicode, ≤200 UTF-16 units и≤800 UTF-8 bytes до NFKC; нормализация NFKC/trim/lowercase служит только поиску и не меняет контракт. ≤12 whitespace tokens; все должны встречаться в bounded public ID/title/description/RU/EN sidecar haystack. Приоритет exact ID, затем prefix ID, затем остальные совпадения; tie-break ordinal ID и numeric version. Это лексический поиск, не обещание семантического понимания/качества внешней поисковой системы. Прямой get case-sensitive.

`items` — короткие `{capabilityId,version,appId,title,summary,digest,executionEnabled,match,links}`. `match` — enum `exact-id|id-prefix|text|browse`, не сгенерированное моделью объяснение. Full schemas/binding/resources загружаются только через get. Default10/max20; элемент≤4KiB canonical UTF-8, страницаJSON≤64KiB. Если bytes заканчиваются раньше requested limit, cursor продолжает **после последнего фактически отданного** item; total остаётся числом public matches, а не длиной страницы. Один item должен помещаться гарантированно, иначе constructor отклоняет projection. Ничего не обрезать внутри schema/идентификатора.

Query-specific cache не создаётся без bound. Нормализованный индекс фиксирован и строится один раз из≤128 public записей. Cursor≤512ASCII bytes, strict base64url+exact JSON shape, содержит revision/query fingerprint/позицию; меняющийся query/order/docs/readiness revision делает его недействительным. Public cursor не является полномочием и не обязан быть криптографически подписан. Подмена позиции может только перескочить по уже публичному списку. `cursor_invalid`→400, клиент начинает поиск заново; не auto-rebase.

Sidecar≤16KiB/version; full detail≤384KiB UTF-8. Existing registry input≤256KiB сохраняется. HTML list≤256KiB, detail≤512KiB после escaping; sitemap≤128 entries. Проверять реальные serialized bytes до отправки, не только число объектов. При превышении фиксированного projection budget — controlled error без частичного HTML/JSON. Предельные fixtures являются частью gate; это не доказательство обслуживания миллиарда пользователей.

## 4. Schema, output, Unicode и digest

В A `validateOutput(entry,result)` запускает `canonicalJson(result)` и тот же recursive bounded validation, что input. Entry получен из server registry, schema из запроса не принимается. Closed object/required/type/enum/array bounds/safe integer/size проверяются до будущей successful receipt. Не возвращать в ошибке native output или входной body. Контракт @1 сохраняет output **только** noteId/revision; более строгая проверка server-generated noteId по Notes identity принадлежит native adapter B. Прежняя input validation и глобальный `text()` в A не переписываются.

Публикуемые schemas синтаксически используют bounded subset JSON Schema2020-12, однако совместимый validator @1 дополнительно ограничивает UTF-16 и canonical JSON bytes. Он не является полным engine. Для конкретной Notes @1:

- title≤160 и body≤100000 UTF-16 code units; supplementary Unicode character занимает2; составные графемы/combining marks не являются одной единицей лимита;
- input canonical JSON≤262144UTF-8 bytes, глубина≤20/число nodes≤10000; Notes storage quota/serialized note overhead дополнительно проверятся native handler в B;
- C0 controls запрещены, кроме TAB/LF/CR; DEL запрещён. Никакой нормализации/trim текста записки;
- integer — finite safe integer. Дополнительные ключи, URL получателя, accountId/noteId для перезаписи отклоняются;
- generic старый `minLength` также вычисляется по UTF-16: не заявлять полное соответствие стандарта на основании @1, где minLength отсутствует. Поддержка авторских schemas в P6 потребует отдельного profiled/versioned решения.

JSON Schema `maxLength` имеет стандартный смысл числа Unicode characters; нельзя переопределить этот keyword как UTF-16. Поэтому original schema остаётся неизменной, а sidecar отдельно сообщает `runtimeProfile:'soty-capability-v1'`, legacy UTF-16/storage/control limits и их проверяемые примеры. Эти дополнительные ограничения объясняют, почему прохождение внешнего JSON Schema validator ещё не означает runtime admission. OpenAPI response schemas A проверяются согласно стандарту; legacy business profile описан отдельно.

Lone surrogate — выявленная незакрытая граница: внутренний @1 validator его принимает, RFC8259 описывает проблемы интероперабельности. A не меняет silently input и не обещает успешное сохранение такого текста. **До write-enable B** требуется отдельное решение и regression: явно согласованный внешний well-formed-Unicode transport admission либо новая версия бизнес-профиля, если меняется версия контракта. Нельзя молча заменить символ на U+FFFD и затем выдать receipt на другой текст. Public query/новые sidecar строки в A отвергают ill-formed Unicode; raw percent/UTF-8 query decoding строгий и однократный.

Digest прежний, сериализатор прежний: sorted own keys + ECMAScript JSON serialization + SHA-256. Не называть его JCS/RFC8785 conformance без проверки (в частности, нынешний serializer допускает lone surrogate). Для независимого клиента raw `contract.json` даёт точные bytes для SHA-256 без необходимости переписать JS number/escape serialization. Schema documents — точные подпроекции, их собственный body hash **не** равен digest целого capability. Семантическое изменение под id/version приводит к существующему `capability_version_conflict` при reopen.

Detail с operational `executionEnabled` и обновляемой документацией не является immutable response. Contract bytes неизменны; docs/readiness имеют отдельный revision/ETag. В A readiness выключен во всех surfaces. Перед B один реальный handler-availability источник должен питать catalog/status; флаг не становится доказательством наличия execution grant у конкретного клиента.

## 5. Account-filtered контракт до OAuth: зафиксировать, не симулировать

Root согласовал public-only A: не вводить неиспользуемый внутренний список ради формального пункта. В будущем отдельный метод `catalogForActor({actor,query,limit,cursor})` принимает **только** opaque actor из проверенного credential, а не client/account/grant поля из JSON. Независимая ownership/discovery policy решает, какие descriptors этому account вообще видимы; затем текущая principal/grant chain ограничивает действия. Проверка исполнения и выдача private metadata остаются разными решениями.

Фильтрация выполняется до ranking/count/cache; cursor связывается с account/client/principal/grant, текущим discovery-policy epoch и catalog revision. Revocation/expiry проверяются заново на каждой странице/get; private, unknown, чужая версия дают одинаковый not_found. Actor cache не переживает отзыв. Ответы `private,no-store`, без общего public ETag; wrong audience/поддельный actor не используют public fallback под видом авторизованной выдачи. Публичный endpoint при этом остаётся явно public независимо от переданного bearer.

До появления этой политики ни grant issuance, ни guessed capabilityId, ни trusted-looking description не выпускают private descriptor. В B/C первое Notes metadata остаётся public, но создание проверяет отдельный действующий create-only grant. OAuth не создаёт право обнаружения автоматически и не требует переделки существующей identity модели.

## 6. HTML, origin, безопасность и cache

SSR использует `modules/ui/palette.mjs` и `modules/ui/hex.mjs`, не повторяет геометрию/цветовые константы и не импортирует старый UI. Короткий header «Возможности для ИИ», поиск с видимой label, карточка «Создать записку», статус «Описание опубликовано. Выполнение пока недоступно», RU/EN examples и явные schema/OpenAPI links. Нативные links/form/details,44px targets, focus,320px/short-landscape, никаких canvas-only controls. Полные схемы доступны по ссылке/раскрытию, не загружаются целиком в каждую search card. Нет выдуманных ratings, authors, проверки клиента или «доступно всем ИИ».

Рендер только escaped text/атрибуты; никакого исполнения Markdown/HTML из description/examples. HTML, формы поиска, раскрытия и ссылки работают без JavaScript. Единственный optional same-origin `capability-docs-update.js` отвечает на readiness handshake настоящего ServiceWorker и не включается в SPA. CSP: `default-src 'none'; base-uri 'none'; object-src 'none'; script-src 'self'; style-src 'unsafe-inline'; img-src 'self' data:; frame-ancestors 'none'; form-action 'self'`. Inline/remote scripts отсутствуют. Отсутствующий script/ответ не даёт права пропустить сохранение черновиков; URL `/agents` тоже не доказывает readiness, потому что предыдущий worker мог отдать там cached SPA. См. [обоснование PWA-стыка](p4-discovery-http-plan.md). Примеры с `</script>`, кавычками, bidi и URL-подобным текстом остаются данными. Sidecar/capability текст не выбирает получателя, URL запроса или право.

Root регистрирует routes после existing Apps Host classifier/security headers, до static/SPA fallback. Named/unknown app hosts не получают shell documentation вследствие обхода classifier. `id` — один percent-encoded segment, декодирование строго один раз; round-trip IDs с допустимыми `/`/`@` проверяется реальным HTTP router, никаких regex из ID. Version — каноническое положительное десятичное число≤1000000. Duplicate query keys, bad UTF-8/percent, неизвестные поля/type, чрезмерный URL→controlled400/414 без эха.

Absolute canonical/sitemap URLs строятся только из явного `discoveryOrigin`, проверенного root до открытия сервисов: точный configured shell origin, HTTPS, либо HTTP с точным hostname `localhost`, `127.0.0.1` или `[::1]` для dev; без credentials/path/query/fragment. Это тот же узкий local origin набор, что у Connect; произвольный 127/8, поддомены localhost и другие формы loopback не расширяют исключение. Host/Forwarded/Origin не выбирают canonical или OpenAPI server. Без заданного origin relative links/API работают; absolute canonical/sitemap не выдумываются (sitemap unavailable, внешний SEO gate открыт). Если robots.txt отсутствует, root может добавить минимальный настоящий route с ссылкой на sitemap при configured origin; существующую политику robots не перезаписывать вслепую. Search/cursor pages получают noindex,follow; sitemap перечисляет только канонические detail/index pages: максимум129 URLs (index плюс128 versions) и128KiB serialized UTF-8. Robots — не ACL.

Чтобы страница не оставалась сиротой, root добавляет обычную ссылку «Для разработчиков и ИИ» → `/agents` в существующее вторичное меню PWA, без нового экрана/изменения основного apps-first сценария. Это отдельная маленькая интеграция `src/world/app.ts`, не владение доменного автора. В A не требуются `llms.txt`, JSON-LD с неподтверждёнными claims, внешняя регистрация или автоматическая установка клиентам.

JSON public GET может явно разрешать CORS `*` **без credentials**, только для этих read routes; cookie/Authorization не меняют representation, Set-Cookie не используется. Content-Type/nosniff обязательны. Query/данные credentials не пишутся в журналы. Ошибки — короткий `{error:{code}}`, allowlist; неизвестное внутреннее исключение→500 без stack/DB/schema values. Никаких внешних schema fetch/redirect, SSRF endpoint или automatic package imports.

Начальная cache policy проста: public successful discovery responses `public,no-cache` с детерминированным ETag representation; dynamic status `no-store`; ошибки `no-store`. Public check всегда предшествует If-None-Match/304. Это разрешает validation cache, не обещает неизменность readiness. Нет бесконечного map для каждой query. Будущий private view использует отдельную политику, не общий CDN/public cache. RFC9111 не заменяет серверную проверку доступа и не гарантирует поведение злонамеренного cache.

## 7. Последовательность и непересекающееся владение

| Шаг | Автор/файлы | Завершённый gate |
| --- | --- | --- |
| A1. Профиль и registry projection | agent_ecosystem: `modules/capabilities/server/catalog.mjs` (только output method), новые `discovery.mjs`, `documentation.mjs`, свои `catalog-discovery.test.mjs`/`catalog-profile.test.mjs` | Pinned digest unchanged; real @1 Unicode/input/output fixtures; compact public search/paging/byte bounds/private noninterference. `validation.mjs` читается, не переписывается. |
| A2. Документы и semantic HTML | agent_ecosystem: новые `openapi.mjs`, `discovery-pages.mjs`, свои `discovery-pages.test.mjs`; тот же receipt | Exact machine/schema links, local refs/OpenAPI shapes, escaped SSR, no fake operation/ready; no JS required. |
| A3. HTTP composition | root: `modules/capabilities/server/index.mjs` (подключение одного view), новый `server/capabilities-discovery.js`, `server/http-app.js`, одна ссылка в `src/world/app.ts`, свои real HTTP tests | Trusted origin/Host precedence, error namespace, encoded IDs/query, ETag/CORS/methods, content/size limits, false status. Owner access/DB layout/Notes не меняются. |
| A4. Независимый audit | whole_product_critic: pure discovery acceptance/source review; publishing_architecture: отдельный actual HTTP acceptance/receipt; root browser | Public/private/query/cache adversarial proof на настоящем HTTP; schema/output/Unicode comparison; actual320/667/desktop browser и links без JS. |

A1→авторский freeze/review→A2/A3 integration→A4. Root может выполнить A2/A3 параллельно только после точного DTO freeze, без общего владения одним файлом. Никаких package installs, новых БД/DDL, crypto/identity rewrites или deployment в этой последовательности. Генерировать OpenAPI из server-owned DTO/schema helpers, а не сканировать административные routes. README/example updates только после реально работающих routes.

## 8. Конкретная приёмка и оставшиеся gate

1. HTTP public+private fixture: private insertion/change/removal не меняет public items/total/revision/cursor/ETag/sitemap; known-private get/schema/contract такой же404, как unknown. Grant/cookie/bearer/query account spoof не расширяет public view.
2. Stable paging: повтор запроса одинаков, нет дублей/пропуска при byte-limited page; query/docs/availability change invalidates cursor; malformed/base64 huge/fractional/negative offset отвергнут. Search RU/EN показывает реальную Notes function; неподходящий query честно возвращает0, без mock данных.
3. Digest: canonical bytes реальной @1 дают зафиксированный SHA; порядок ключей не меняет digest; изменение description/schema/effects под1 конфликтует после реального reopen; operational flag не меняет semantic digest, но меняет detail/revision. Sidecar не может скрыть semantic mismatch.
4. Input/output: отсутствующее/лишнее поле, неправильный тип/null/array, revision0/float/unsafe integer, result body/URL/account rejected.80/81emoji, combining sequences, lone surrogates, TAB/LF/CR/NUL/DEL и canonical bytes limit проверены с ожидаемым **нынешним** @1 поведением. Original схемы неизменны; внешнее стандартное сравнение не выдаётся за прежний validator.
5. Escaping/HTTP: malicious description/example не исполняется; encoded ID round-trips; malformed URL/query не попадает в SPA200; неверный method405; Host/Forwarded не подменяет canonical; app-host classifier сохраняется. No remote schema fetch или зависимость от cookies.
6. Bytes/performance:128 записей, schemas у предела, RU/astral text, множество tiny query tokens; output budgets и bounded work проверены, без per-query retained cache. Измеренные время/память записываются как локальные результаты, не как capacity guarantee.
7. Browser: semantic search/get/schema/OpenAPI работают без JS;320px и667×375/desktop без horizontal overflow, фокус виден, все действия имеют имя; no body приватных Notes, false availability читается однозначно. Это отдельная настоящая проверка root, ещё не выполненная этим автором.

**B1/B2**: exact Notes/Capabilities formats+reader/fallback/restore, native create-only/cross-DB crash proof, output проверка до successful receipt, idempotency/purge/no-resurrection, quota/rate/input retention, решение ill-formed Unicode, реальные HTTP write/read receipts. **C**: SDK/AS exact install/lockfile, OAuth resource/PKCE/consent, MCP профили, service child grant. **D**: реальные два клиента/RU-EN задачи и внешний HTTPS release. Private discovery policy — отдельный prerequisite до любого private descriptor release, не скрытый пункт OAuth.

## 9. Первичные источники и граница вывода

Проверены30.09.2026:

- [OpenAPI3.1.2](https://spec.openapis.org/oas/v3.1.2.html): выбранная совместимая ветка3.1, не заявление «последняя версия» (официальная страница также перечисляет3.2.x). Схемы используют2020-12; OpenAPI описывает существующие HTTP operations, не выдаёт полномочия.
- [JSON Schema2020-12 validation §6.3](https://json-schema.org/draft/2020-12/draft-bhutton-json-schema-validation-01#section-6.3): стандартный смысл string length не равен JS UTF-16 `.length`. Поэтому профиль сервиса и дополнительные ограничения названы отдельно.
- [RFC8259 §8.2](https://www.rfc-editor.org/rfc/rfc8259.html#section-8.2): одиночные surrogate escapes создают проблемы интероперабельности; JSON parsing само по себе не доказывает round-trip корректного Unicode.
- [RFC8785](https://www.rfc-editor.org/rfc/rfc8785.html): JCS имеет дополнительные требования к данным. Нынешний serializer Сот не объявляется прошедшим JCS conformance; независимая сверка использует published canonical bytes.
- [RFC9111 §5.2](https://www.rfc-editor.org/rfc/rfc9111.html#section-5.2): различаем revalidation/no-store/private и серверную авторизацию. [RFC9309](https://www.rfc-editor.org/rfc/rfc9309.html): robots не служит механизмом допуска.
- [Google: AI features](https://developers.google.com/search/docs/appearance/ai-features): доступный текст/internal links/crawl eligibility нужны, особого AI-файла не требуется; индексация и показ не гарантированы. Из этого не делается обещание рейтинга в ChatGPT или автоматической установки инструментов в любой ИИ.

MCP/OAuth версии preflight в этом узком A заново не объявлены проверенными клиентами и не устанавливались. Их повторная актуальная проверка и совместимость требуются при C; код/version drift не скрывается названием стандарта.

Read-only evidence: catalog SHA `0a3a20f2acf3e72e4645733f605cf9e3f7f5fdb474415077cf27db87f8cd23f0`; validation `ce7a5731ac13dbc67646ac38a94f27d012c20ba9643bcc69fc5a5d5f31fd7076`; access `dabf5d1e6cb8af62dcd447ab0b2279476a937a77fa820c37175241de262ee37e`; index `90c1177e1835b73dc0a59f6dbc22063809b99ce761fd6d571caa8677b84fd5ff`. Master SHA `0a1897f2b1377bc3fefdd78109dc8f5faecde1fa952c0906007bdba8fc6644c7`; preflight `9c0d3a0008d6a4559db28e180881975d9a0b64a1c6174de1ccc219b5a64425a6`. Выполнены только чтение, первичные источники и малый in-memory Node probe digest/Unicode; A unit/HTTP/browser acceptance ещё не выполнялись.
