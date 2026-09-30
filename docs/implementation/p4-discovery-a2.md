# P4-A2 — semantic HTML и OpenAPI

30.09.2026. Авторский source/API freeze в `soty-platform`, Node24.13.1/Windows. При первом A2 freeze A1 source hashes совпадали с [предыдущим receipt](p4-discovery-a1.md); последующая согласованная browser delta приведена ниже. Этот срез не меняет HTTP adapter, frontend, output/input schemas, авторизацию, Notes или БД. Root выполняет настоящую HTTP/browser приёмку отдельно.

## API и состояние

```js
buildDiscoveryOpenApi() // frozen JSON object; one static document
renderDiscoveryIndex({result, query = '', limit = 10, origin = '', noindex = false}) // complete HTML string
renderCapabilityPage({detail, origin = ''}) // complete HTML string
```

Renderer принимает только принятый public DTO. `discovery_render_invalid` означает неверные renderer args/origin/links/scope; `projection_too_large` — превышение serialized bytes. Все ошибки содержат только code. Root adapter отображает внутреннюю ошибку коротким контролируемым ответом. Origin — явный точный HTTPS origin либо loopback HTTP (`localhost`, `127.0.0.1`, `[::1]`); без path/credentials/query/fragment/неявной нормализации. Пустой origin оставляет relative links и не выдумывает absolute canonical. Parent отдельно проверяет совпадение с configured shell origin.

Поиск — обычная GET form с видимой label; переход страницы сохраняет query/limit и настоящий public cursor. Search/cursor pages имеют `noindex,follow`, canonical ведёт на основной публичный адрес без query. Detail содержит RU назначение/границы, native English disclosure с английскими примерами и обычные machine links. Примеры подписаны как иллюстрации, не как выполненные вызовы. Notes явно выключен; нет фиктивной кнопки запуска/подключения, рейтинга, автора или обещания регистрации во внешних ИИ.

Графитовая палитра и hex-геометрия импортированы из `modules/ui/palette.mjs` и `modules/ui/hex.mjs`. Нет копии формул/цветов, старого UI, удалённых fonts/images или canvas-only управления. Предусмотрены44px controls, visible focus, reduced-motion, native details, wrap для code/длинных строк,320px и short-landscape media rules. Реальная геометрия/keyboard отдельно проверяется root; эти правила CSS сами по себе не являются browser proof или WCAG certification.

## Безопасность и пределы

Title/description/query/examples идут только через HTML text/attribute escaping. Renderer не исполняет HTML/Markdown, не делает schema fetch и не превращает URL-подобную строку в ссылку. Каждый machine link дополнительно сверяется с точным server-generated путём выбранного ID/version; чужой scheme, host, другой capability или path отвергается. Public detail `visibility:private`/не-public scope не рендерится.

После полного escaping index ограничен256KiB, detail512KiB. При превышении controlled error, без частичной выдачи или усечения схем. Большая схема остаётся отдельным exact JSON-документом по ссылке, а не копируется в HTML;240+KiB schema fixture даёт страницу меньше25KiB. Реальная Notes projection без origin: index12300 bytes, detail17806 bytes, OpenAPI17993 bytes (`JSON.stringify`). Это локальные размеры конкретного содержимого, не capacity guarantee.

Root согласовал amendment: один optional self-hosted `<script defer src="/capability-docs-update.js"></script>` отвечает только на PWA readiness handshake. Это root-owned asset. Поиск, текст, disclosures и ссылки не зависят от него. Inline scripts, PwaController/installer, remote assets отсутствуют. Молчание документа и URL `/agents` не считаются readiness; старый worker мог отдать по этому URL SPA с unsaved editor. All-window fail-closed правило и тесты самого handshake принадлежат root.

## OpenAPI

Выбранная версия —3.1.2, не заявление о самой новой версии. `servers:[{url:'/'}]`, `security:[]`; только шесть реально подключаемых A3 GET маршрутов: catalog search/detail/contract/schema, status и openapi. Нет write/OAuth/MCP operations. Schemas описывают DTO/data; pinned business schemas доступны отдельными GET и не подменяются OpenAPI wrapper. Все `$ref` локальные. Опубликованы query/paging bounds, errors, status false/null, cache/HEAD semantics. Profile отдельно объясняет стандартный Unicode character count и дополнительные legacy UTF-16/bytes/control ограничения.

Перепроверены первичные страницы [OpenAPI3.1.2](https://spec.openapis.org/oas/v3.1.2.html) и [JSON Schema2020-12 validation](https://json-schema.org/draft/2020-12/draft-bhutton-json-schema-validation-01#section-6.3). Источники объясняют формат, а не доказывают клиентскую совместимость или запуск Notes.

## Авторский gate

```text
node --test --test-concurrency=1 modules/capabilities/test/discovery-pages.test.mjs
10/10 PASS, 0 skips, 0 failures

node --test --test-concurrency=1 modules/capabilities/test/catalog-profile.test.mjs modules/capabilities/test/catalog-discovery.test.mjs modules/capabilities/test/discovery-pages.test.mjs
27/27 PASS, 0 skips, 0 failures
```

Syntax и scoped diff-check прошли. Тесты: actual public DTO/local references/frozen OpenAPI, strict canonical origins, encoded IDs, hostile title/query/example escaping, отсутствие fake write control, exact links, native paging/query preservation, honest empty state, oversized escaped output и отдельная большая schema. HTML source affordance test ограничен наличием правил; не заявляет геометрию/фокус реального браузера.

Для независимой проверки **response schemas** использован уже установленный Python `jsonschema4.26.0`, `Draft202012Validator`: actual search/detail/raw semantic contract/schema/sidecar/status/error values и отрицательные DTO. Никакая зависимость не устанавливалась. Также стандартный validator принимает81emoji в Notes title при maxLength160, а неизменный runtime отвергает их как162 UTF-16 units — различие проверено, не замаскировано. Этот oracle test явно skip при отсутствии установленного Python jsonschema в другом окружении; здесь он исполнился. Полный OpenAPI meta-schema/conformance suite, реальные SDK/HTTP/browser/индексация этим тестом не доказаны.

## SHA-256

| Файл относительно `modules/capabilities` | SHA-256 |
| --- | --- |
| `server/openapi.mjs` | `9f665c2d05579128e7d97c9d94e14445cf304bc9d8642603fda2b105680519ad` |
| `server/discovery-pages.mjs` | `5dc300d6be86a47816c86f9a94c84e6477d7c6215cdc9682d92350d2bc4bbf3e` |
| `test/discovery-pages.test.mjs` | `ad5dfe5299ce793545ae346f65295a9d125c4f4deda5a1b7e94640dc28d66396` |

A3/A4, настоящий320/667/desktop браузер и JS-disabled flow остаются внешними gates. Source после freeze не меняется без конкретного finding.

## Узкая коррекция после browser finding

Root на1440px обнаружил сжатую кнопку поиска: `width:100%` у поля вместе с flex-shrink кнопки переносили «Найти» и давали70px высоты. Добавлено только `.search-row>.button{flex:0 0 auto}`. Общие ссылки/кнопки сохраняют перенос длинного текста;320px layout по-прежнему переводит поиск в две строки. Собственный browser/измерение исправленной48px высоты не заявляется — это повторный gate root.

Из RU/EN sidecar summary удалён operational статус. Назначение записки остаётся в summary, выключенное исполнение — в отдельном badge/status. Это ожидаемо меняет documentation revision на `91336fd3868cfb51af221dbf1c60008dbb1e747d1af40ee7d550d2decc705812`, но **не** Notes semantic digest `95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204`.

Дополнительный root finding: RU H1 сразу переходил к H3 назначения. Эти два заголовка стали H2 с прежним визуальным размером; English сохраняет H2 title→H3 subsections. Проверка outline настоящего rendered HTML сначала дала RED `heading level jumps from H1 to H3`, после исправления общий27/27 PASS,0skip. В существующий semantic test добавлена проверка единственного статуса в карточке, без ещё одного CSS snapshot test. Syntax проверен. Текущие размеры без origin: index12360B/detail17782B; OpenAPI без изменений17993B.

Актуальные SHA-256 этой delta:

| Файл относительно `modules/capabilities` | SHA-256 |
| --- | --- |
| `server/documentation.mjs` | `5f9c95f091c8db64a3c903c91d16d0fef0650cca61bd89380cadb7e4e066f514` |
| `server/discovery-pages.mjs` | `80f958ece791c04ece97a3eb4db3a9ec6802d5b843c7fa6447d580e637240a38` |
| `test/discovery-pages.test.mjs` | `47549d42e0bb91ddaeb13a7b475b08e4e3283dc3321e5819bed0d132f6f02fa8` |

`catalog.mjs`, `discovery.mjs`, `validation.mjs`, `openapi.mjs` в этой коррекции не менялись. Три исправления ограничены предъявленными findings; source снова frozen для HTTP/browser повторения.
