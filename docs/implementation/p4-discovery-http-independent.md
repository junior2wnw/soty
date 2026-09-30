# P4-A4 — независимая проверка HTTP discovery

30.09.2026. Проверен A3 transport/composition по [контракту A](p4-discovery-contract.md) и [плану HTTP/PWA](p4-discovery-http-plan.md). Production-файлы не изменены. Добавлен только отдельный `server/test/capabilities-discovery.acceptance.test.mjs`; авторские test helpers не импортируются.

Результат: **7/7 PASS, 0 FAIL, 0 SKIP**, Node 24.13.1 / Windows, финальный serial run 897.5005 ms. В проверенной области consequential blocker не воспроизведён. Это локальная HTTP-приёмка; браузерная геометрия/клавиатура, PWA и внешний HTTPS принадлежат отдельным gate.

```text
node --test --test-concurrency=1 server/test/capabilities-discovery.acceptance.test.mjs
```

## Что исполнено

Использованы настоящий `createCapabilitiesService`, SQLite, публичный catalog, Express middleware и HTTP на временном loopback-порту. Один сценарий проходит полный `createHttpApp` с Apps Host classifier и Connect. Сырые Host-заголовки отправлены через TCP, поэтому Node HTTP client не мог заранее объединить/нормализовать их. Все БД, descriptors и credentials-строки синтетические; общий QA сервер, аккаунты и production storage не использовались.

| Проверка | Наблюдаемое свойство |
| --- | --- |
| Origin admission | Настоящий Connect constructor и discovery принимают четыре проверенных canonical origins: HTTP `localhost`, `127.0.0.1`, `[::1]`, и HTTPS. Одиннадцать недопустимых explicit origin/shell configurations отвергаются до изменения существующего temp storage: совпадают состав файлов/каталогов и SHA каждого файла, включая уже созданную SQLite. Проверены другие `127.*`, `*.localhost`, IPv4-mapped IPv6, короткая/числовая/hex форма IPv4, неканонические case/default-port/trailing-slash, userinfo. Это проверка указанной матрицы, не утверждение о тождестве всех URL parsers. |
| Raw Host и порядок middleware | Повторный одинаковый Host, разный регистр второго Host, отсутствующий/пустой/неправильный Host дают400 без discovery/SPA. Named/deep-named/legacy app hosts получают `app_not_found`, даже с поддельными Forwarded/X-Forwarded-Host/Origin. Эти поля не подменяют настроенный sitemap origin. |
| Raw namespace/query и HEAD errors | Encoded duplicate keys, unknown fields, overlong/malformed UTF-8 и percent sequences, encoded noncanonical version, unknown namespace routes и URL сверх8192 дают controlled400/404/414, `no-store`, без ETag/SPA/эха ввода. HEAD совпадает с GET по статусу и representation headers, но имеет0 body bytes. POST известного route даёт405 и `Allow: GET, HEAD`. |
| Настоящая UTF-8 пагинация |25 публичных synthetic descriptors, limit20, многобайтные summaries: первая страница **18 записей / 65282 байта**, вторая **7 / 25380 байт**. Лимит65536 реально сработал раньше row limit, порядок и все25 ID сохранены, дублей нет; повтор первой страницы идентичен. |
| Cursor boundaries | Нормализованный тот же query и добавление private descriptor/недопустимого private sidecar сохраняют public revision, ETag и продолжение. Изменение public documentation/readiness либо query отвергает прежний cursor. Padding, длина513, fatal UTF-8, negative/fractional/out-of-range offset, дополнительное поле и неканонические JSON bytes отвергаются. Public cursor не объявляется секретом или правом. |
| Public/private conditional cache | Для known-private detail/contract/input/output и unknown ID одинаковый404, даже с `*`, weak/public ETag, synthetic bearer/cookie. HEAD также404, без ETag/body. Public representation/ETag не меняются от этих заголовков; CORS `*`, без credentials/Set-Cookie. Неправильный хвост If-None-Match не превращает ранний match в304. |
| Exact bytes и успешный HEAD | HTTP canonical bytes `notes.createDraft@1` дают сохранённый digest `95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204`. Exact input/output schemas не дополнены. ID с `/` и `@` проходит один encoded segment; double-encoding отвергается. Успешные GET/HEAD совпадают по Content-Length/type/cache/ETag; weak HEAD conditional даёт304 без body. Status остаётся false/null, `no-store`, без304. |

## Прочитанные границы

`createHttpApp` проверяет explicit discovery origin до открытия сервисов и ставит raw Host/Apps classifier раньше discovery. `createCapabilitiesService` строит и валидирует public view до SQLite open/pinning. HTTP adapter проверяет существование public representation до conditional response, ограничивает raw target/serialized response и выдаёт короткий allowlisted error. Private descriptors/sidecars отфильтрованы до public search revision/count/cursor.

Отказ конфигурации проверялся с **заданным** `discoveryOrigin`; это не аудит всех исторических startup-failure путей при отключённом discovery origin. Host precedence проверяет действующую серверную композицию, не безопасность стороннего reverse proxy. Внешние DNS/TLS/cache не участвовали. Испытание UTF-8 даёт конкретную границу ответа и наблюдение локального запуска, не capacity guarantee.

A2 HTML/OpenAPI импортированы и реально обслуживались текущими модулями; полную semantic HTML/schema-conformance приёмку этот HTTP receipt не присваивает. Browser/PWA, P2 modal lifecycle, физическое устройство и установка PWA проверяются другими участниками отдельно. Notes execution, OAuth/MCP, private discovery policy, реальные внешние клиенты и публичный deployment здесь не реализовывались и не заявляются готовыми. Test cleanup закрывает сервисы/socket и удаляет только собственную проверенную temp directory.

## SHA256 проверенного среза

| Файл | SHA256 |
| --- | --- |
| `server/capabilities-discovery.js` | `de4a11a9d5fcd968d4ea64bf722852781b5c15bb90a2add0b7eb424721bd7e96` |
| `server/http-app.js` | `fbca271510e7ce8bfc9ff7f02b19d7f4f2c1be93856ab5b2c4ef3342c8b12be7` |
| `modules/capabilities/server/index.mjs` | `c612197e627151abb9f463b15affa2af80fde5625f4dbd3334fb6d0391099984` |
| `modules/capabilities/server/discovery.mjs` | `08a3e67ffe0beb5da5317917556edc4c37f24ca4a7883d4bc6b854b069d61be3` |
| `modules/capabilities/server/openapi.mjs` | `9f665c2d05579128e7d97c9d94e14445cf304bc9d8642603fda2b105680519ad` |
| `modules/capabilities/server/discovery-pages.mjs` | `5dc300d6be86a47816c86f9a94c84e6477d7c6215cdc9682d92350d2bc4bbf3e` |
| `modules/connect/server/index.mjs` | `2b0e8a62ad2f59bcbfdef54ced237bd07aca74d2e3ddf4104ce2fc8aab9a7b97` |
| `modules/apps/server/hosts.mjs` | `682b0dd4f8101e271fd5efa44612bb3a843d9ec5764c470fa5f291371ab17768` |
| Новый acceptance test | `8692849937e2b94e472419be9cf8d1a5035f20bfaf459ffaf08fcba97625e5a0` |

Синтаксис нового теста и whitespace-check — PASS. Новых dependencies, source changes, commit/push или heavy suite в этой независимой работе нет.
