# P4-A3 — HTTP и PWA: порядок интеграции

30.09.2026. Основание — принятый [контракт A](p4-discovery-contract.md), checkpoint `05c8ba38be1e51918ebd9386558c5d316c802978`. План интегратора выполнен в локальной области; фактические результаты и отдельные browser/external границы записаны в [интегральной квитанции](p4-discovery-integration.md). A1 public DTO должен пройти авторский freeze перед изменением composition. Production не меняется.

## Последовательность

- [x] Подключить единственный `createPublicDiscovery` до SQLite open/pinning. Каждая public версия требует явный sidecar; у custom test fixtures он передаётся отдельно. Никаких переводов или примеров, придуманных runtime. Existing input semantics, digest, access/Invocation и DB format не меняются.
- [x] Добавить read-only HTTP adapter с ограниченным raw request target, строгим однократным percent/UTF-8 decoding, duplicate/unknown query rejection, канонической версией и предсказуемыми 400/404/405/414. API ошибки — только allowlisted code, без request/body/stack.
- [x] Проверить явный `discoveryOrigin` до открытия storage. Он должен совпадать с configured shell origin, использовать HTTPS либо loopback HTTP. Canonical/sitemap никогда не выводятся из Host/Forwarded/Origin. Без настройки работают relative links; sitemap не объявляется доступным.
- [x] Зарегистрировать routes после Apps Host classifier/security middleware и до SPA fallback. Сохранить JSON status false/null. Реализовать точные contract bytes, exact input/output schemas, OpenAPI, semantic HTML и sitemap из public view. Public доступ проверяется до conditional response.
- [x] Выдавать bounded serialized responses, `public,no-cache` и deterministic ETag, HEAD без body; errors/status `no-store`. Public JSON CORS `*`, без credentials, без Set-Cookie. Неподдерживаемые методы не становятся SPA200.
- [x] Dev server проксирует `/agents` на тот же backend и передаёт собственный явно известный origin; не наследует production config. Расширить настоящий dev integration test, а не только проверять строку конфигурации.
- [x] PWA worker не подменяет offline `/agents` и capability documents корневым HTML. Все window clients по-прежнему подтверждают update; документация может ответить через маленький optional same-origin script. Контент/поиск/ссылки не требуют JS. Молчание и сам URL `/agents` никогда не доказывают готовность. Изменивший URL/новый client требует повторного подтверждения. Проверить эти границы на выполняемом worker, не снимком исходника.
- [x] После freeze параллельного P2-I lifecycle добавить единственную вторичную ссылку «Для разработчиков и ИИ». Не переносить classic UI, не менять apps-first navigation.
- [x] Реальные HTTP tests: public/private/nonexistent, encoded IDs, query/cursor, raw contract hash, GET/HEAD/ETag, CORS, hostile Host/Forwarded, immutable admission/startup, genuine full composition. Проверить существующие capability/Connect пути после осознанной адаптации legacy public args.
- [x] Независимый A4 audit; настоящий browser320/667×375/desktop, HTML ссылки/поиск/details без JS, keyboard и отсутствие horizontal overflow. Зафиксированы фактические байты/ограничения и отдельный незавершённый внешний HTTPS/indexing gate. Machine links проверены HTTP; встроенный browser отказал прямому JSON переходу, что отдельно сохранено в receipt.
- [ ] При внешней приёмке открыть JSON links в обычном целевом браузере: текущий встроенный browser завершил этот переход `ERR_BLOCKED_BY_CLIENT`, не успехом.

## Подтверждённые стыки

`scripts/dev.mjs` сейчас проксирует только `/api`, `/ws`, `/health`, `/ready`. Без добавления `/agents` Vite вернёт SPA вместо серверной документации. Исходный `public/sw.js` использовал cached shell как fallback любой navigation; это неверный ответ для versioned machine documents. Независимый reviewer воспроизвёл `/agents`, `/agents/missing`, `contract.json` → cached SPA200 в выполняемом worker. Поэтому нельзя исключать window из update handshake по одному URL: под `/agents` может оставаться прежняя SPA с несохранённой запиской. Принято небольшое уточнение A: optional script только в новом read-only документе подтверждает readiness через transferred port настоящего ServiceWorker; отсутствие script/ответа продолжает блокировать update. Скрипт не регистрирует worker, не открывает storage и не перезагружает документ. Native SSR формы и содержание работают без него.

Legacy service acceptance передаёт `actor` в public search/get. Новый strict DTO отвергает неизвестные аргументы, поэтому тест сохраняет две отдельные проверки: actor injection rejected и public private-query/get non-disclosure без actor. В HTTP Authorization/Cookie игнорируются публичным read view; это не private fallback и не разрешение выполнения.

Sidecar проверяется до открытия базы. Для real SQLite pin-conflict теста sidecar должен соответствовать предъявленному изменённому descriptor, иначе правильный более ранний отказ docs mismatch не доказывает DB pin gate. Test-only helper допустим; production generic fallback отсутствует.

## Первичные основания

- [RFC9110 §9.3.2 и §13.1.2](https://www.rfc-editor.org/rfc/rfc9110.html): HEAD описывает представление GET без содержания; If-None-Match для GET/HEAD использует weak comparison. Conditional response не обходит выбор существующего public representation.
- [WHATWG URL, form-urlencoded parsing](https://url.spec.whatwg.org/#urlencoded-parsing): стандартный URLSearchParams decoding не является строгим отказом на каждый повреждённый percent/UTF-8 sequence. Здесь strict raw parsing — явный собственный API contract; `+` в query сохраняет обычный смысл пробела, percent decode выполняется один раз.
- [WAI-ARIA APG, modal dialog](https://www.w3.org/WAI/ARIA/apg/patterns/dialog-modal/): при исчезнувшем opener выбирается логическое продолжение. Отдельный P2-I подплан исправляет порядок cleanup/render/return до добавления новой ссылки.

Источники прочитаны 30.09.2026. Они обосновывают решения, но не заменяют тест конкретной реализации и не гарантируют индексацию внешними ИИ.
