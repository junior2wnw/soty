# P4-A1 — публичная проекция и профиль результата

30.09.2026. Авторский source/API freeze в worktree `soty-platform`, база `05c8ba3`, Node24.13.1/Windows. Это чистые функции registry/discovery; HTTP, HTML/OpenAPI, browser и независимая приёмка ещё отдельные A2–A4 gates. Production не менялся. Основание — [принятый контракт](p4-discovery-contract.md).

## Изменение

В `catalog.mjs` добавлен только `validateOutput(entry,result)`: entry должен быть тем же объектом из данного registry, затем выполняются прежний `canonicalJson` и прежний recursive value validator. `validateInput`, `validation.mjs`, Notes input/output и semantic contract не изменены. `notes.createDraft@1` остаётся выключенным; semantic SHA-256:

`95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204`.

Новые `documentation.mjs` и `discovery.mjs` не открывают БД, не вызывают executor и не загружают внешние schemas. Sidecar фиксирует RU/EN назначение, границы, проверяемые примеры и дополнительный legacy-профиль. Standard string length и UTF-16 runtime length названы отдельно. Lone surrogate всё ещё принимается прежним value validator; A1 отвергает его только в новых публичных metadata/query. Write admission остаётся решением B, а не скрытой заменой @1.

## Замороженный API для A2/A3

```js
createPublicDiscovery({catalog, documentation = BUILTIN_DOCUMENTATION})
// -> Object.freeze({search, get, contract, schema})

search({query = '', limit = 10, cursor} = {})
// {scope:'public', revision, items, total, cursor:string|null}
// item: {capabilityId,version,appId,title,summary,digest,executionEnabled,match,links}

get({capabilityId,version})
// {scope:'public',capability,documentation,links}
contract({capabilityId,version})
// {canonicalJson:string,digest:string} -- HTTP emits only canonicalJson bytes
schema({capabilityId,version,kind:'input'|'output'})
// exact original schema, without injected $id/$schema
```

Все аргументы exact; `actor`/`accountId` не принимаются. Public view одинаков для всех. Ошибки запросов: `invalid_input`, `query_invalid`, `cursor_invalid`, `not_found`. Ошибки конфигурации: `discovery_invalid`, `documentation_invalid`, `documentation_missing`, `documentation_contract_mismatch`, `projection_too_large`. Error.message содержит только code. Output validator сохраняет старые `invalid_input`/`payload_too_large`. HTTP status mapping принадлежит A3.

`links` имеет пять относительных server-generated полей: html/detail/contract/inputSchema/outputSchema. ID encoded как один URL segment; get case-sensitive. Результаты, вложенные metadata и schemas frozen. Sidecar cloning отделяет snapshot от дальнейших изменений исходной конфигурации.

Для каждой public версии нужен ровно один явный sidecar `{capabilityId,version,contractDigest,locales:{ru,en},keywords:{ru,en}}`; locale — `{title,summary,useWhen,notFor,examples}`. `examples:[]` допустим, максимум128/язык в общем16KiB. Notes имеет непустой проверенный пример в обоих языках. Нет fallback/фиктивных переводов. Root согласовал эти уточнения перед freeze; relevant digest mismatch отвергается до будущего открытия SQLite/pins в A3. Custom test registries должны передавать явные test-only sidecars с собственным digest.

Private/unknown sidecars не участвуют в проверке содержимого и публичных лимитах. Constructor делает один проход server-owned массива и сохраняет только совпадения с public versions. Обход исходной config на запросе отсутствует. Это не обещание constant-time обработки произвольно огромной доверенной конфигурации.

## Пределы и поиск

Public фильтр предшествует index, revision, rank, total, cursor и выдаче. Индекс≤128 versions; query до NFKC≤200 UTF-16 units/800 UTF-8 bytes, после NFKC/lower/trim≤12 whitespace tokens. Все tokens должны встречаться в public ID/title/description и положительных RU/EN sidecar текстах; `notFor`/example bodies не становятся положительными ключевыми словами. Ranking exact ID→prefix ID→text, затем ordinal ID/numeric version. Это лексический поиск.

Default10/max20; item≤4KiB, page≤64KiB canonical UTF-8, sidecar с generated profile/revision≤16KiB, detail≤384KiB. Byte cropping продолжает после реально выданных items. Schema никогда не усекается. Нет per-query cache. Cursor≤512ASCII bytes, strict canonical base64url/UTF-8/JSON с revision/queryHash/offset; неподписанная public позиция не является полномочием. Изменённые docs/readiness инвалидируют cursor, сохраняя отдельный semantic digest.

## Авторские проверки

Последовательно, без heavy parallel suites:

```text
node --test --test-concurrency=1 modules/capabilities/test/catalog-profile.test.mjs modules/capabilities/test/catalog-discovery.test.mjs
17/17 PASS, 0 skips, 0 failures

node --test --test-concurrency=1 modules/capabilities/test/access.test.mjs
11/11 PASS, 0 skips, 0 failures
```

Второй прогон — baseline regression существующего service до A3 composition, не HTTP proof нового discovery. `node --check` обоих новых modules и `git diff --check` прошли. Проверки включают реальный SQLite reopen/durable pin, exact schemas/digest, input/output Unicode/bytes/control/type boundaries, output object provenance, invalid examples/config, snapshot immutability, strict args/cursors, public-only byte noninterference при130 лишних malformed sidecars,128 public versions и40 больших карточек с byte-cropped paging без потерь/дубликатов. Pin fixture явно содержит matching candidate sidecars, чтобы будущая A3 проверка документации не маскировала проверяемый DB conflict.

Нет заявления о настоящем HTTP/browser/поисковом индексе внешних ИИ, OAuth/MCP, private ACL, выполнении Notes, production capacity или произвольном JSON Schema conformance. Root/critic выполняют независимые gates отдельно.

## SHA-256 freeze

| Файл | SHA-256 |
| --- | --- |
| `server/catalog.mjs` | `498a06025107598b450406114bc331aef71cd0824c384dfdf7927d10dc6fc072` |
| `server/discovery.mjs` | `08a3e67ffe0beb5da5317917556edc4c37f24ca4a7883d4bc6b854b069d61be3` |
| `server/documentation.mjs` | `0fb296890e5073e51c15231c8c07b44da9eb4ef9b8d8ceeadd10b44a01c10e4b` |
| `server/validation.mjs` (не изменён) | `ce7a5731ac13dbc67646ac38a94f27d012c20ba9643bcc69fc5a5d5f31fd7076` |
| `test/catalog-discovery.test.mjs` | `076a6cfed46ac664f87b4175dc3df023c5f0a5a03e7954879d7b58fb1913ccfb` |
| `test/catalog-profile.test.mjs` | `d745f5e8856c254c8d9c5b090a86bc1a1447c1388d43f3506f81da54046ed938` |

Все пути таблицы относительно `modules/capabilities`. Дальнейшие изменения A1 — только после конкретного review finding; A2 использует этот DTO без изменения primary schema.

После browser finding root одобрил узкую правку RU/EN summary в `documentation.mjs`: operational статус вынесен из summary в отдельный уже существующий badge. Актуальный SHA sidecar и повтор27/27 указаны в [A2 browser delta](p4-discovery-a2.md#узкая-коррекция-после-browser-finding). Primary Notes contract/digest, validator и discovery logic не изменились.
