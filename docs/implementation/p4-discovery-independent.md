# P4-A — независимая приёмка discovery

30.09.2026. Проверена замороженная **чистая модель A1** в `C:\Users\Junio\.codex\worktrees\soty-platform\соты`, Windows, Node `v24.13.1`. Основание — [принятый контракт](p4-discovery-contract.md). HTTP composition, SSR/OpenAPI, PWA и browser gate этим результатом не закрыты.

## Результат A1

Независимый файл [discovery.acceptance.test.mjs](../../modules/capabilities/test/discovery.acceptance.test.mjs): **10/10 PASS, 0 FAIL, 0 SKIP**, 190.9 ms по Node runner. Авторские fixtures, test helpers и serializer не импортируются. Тесты вызывают настоящие `createCatalog` и `createPublicDiscovery`; собственные декларации/sidecars — синтетические server-owned данные. БД, HTTP, браузер, Notes write и Invocation не запускались.

```powershell
node --check modules/capabilities/test/discovery.acceptance.test.mjs
node --test --test-concurrency=1 modules/capabilities/test/discovery.acceptance.test.mjs
```

В проверенных сценариях новых блокирующих дефектов A1 не обнаружено. Это ограниченный результат для перечисленных исходников и контрактов, не утверждение универсальной безопасности или полного соответствия JSON Schema/JCS.

## Независимые доказательства

| Сценарий | Наблюдаемый результат |
| --- | --- |
| Один ID содержит public версии 1/3 и private версию 2; приватные title/readiness/order меняются, добавлены 259 отсутствующих sidecars с некорректным содержимым | Полный transcript search/get/contract/input/output и продолжение исходного cursor совпадают побайтно. Private версия и unknown дают одинаковый `not_found`; приватные слова не ищутся. |
| Private версия становится public в новом экземпляре | Без описания именно этой версии — `documentation_missing`; с явным описанием total/revision меняются. Старый snapshot остаётся прежним и не раскрывает новую версию. |
| 73 длинных encoded ID и многобайтные summaries; клиент меняет limit между страницами | Первая страница реально превышает 60 KiB и обрезается раньше 20 items. Каждая страница ≤64 KiB, item ≤4 KiB; все 73 ID получены ровно один раз. Ссылки сохраняют весь ID в одном encoded segment. Это чистая проверка DTO, не HTTP routing. |
| Cursor после Unicode NFKC/lowercase и после изменения публичной проекции | Эквивалентное написание query продолжает тот же поиск. Другой query, docs/readiness revision, malformed UTF-8/base64url/JSON, лишнее поле, неверная позиция отклоняются. Допустимая подмена позиции лишь перескакивает по public результатам. |
| Identity/credential fields в каждом public методе | `actor`, account/grant/authorization/cookie и includePrivate отвергаются как `invalid_input`; тестовые значения не отражаются в ошибке и не расширяют выдачу. |
| Изменение исходных declarations/sidecars и возвращённых nested schemas/examples/links | Опубликованный transcript/revision не меняются; новые keywords из изменённого исходного массива не попадают в индекс. |
| Семантика против документации/readiness | Docs/readiness меняют discovery revision, но сохраняют raw semantic bytes/digest. Изменение description меняет digest; старый sidecar не может скрыть несовпадение. SQLite reopen pin-conflict здесь не проверялся. |
| Настоящая Notes @1 | SHA-256 raw contract остаётся `95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204`; опубликованы ровно прежние input/output schemas. 80 emoji/combining pairs проходят, 81 не проходит; текст не нормализуется. Input ровно 262144 UTF-8 bytes принят, добавление одного CJK символа даёт `payload_too_large`. |
| Старый input/output профиль и новые public strings | Legacy lone surrogate принят validator, что явно названо в sidecar; тот же некорректный Unicode в public example отклоняется. Output с лишним body/URL, неверной revision или подставленным registry entry отвергается без отражения native values. Пустой noteId остаётся допустимым по старой схеме; это не доказательство действительного Notes ID. |
| Индекс 128 public версий и ошибки query | Все 128 записей доступны через bounded paging. Сверхлимитные/ill-formed/слишком многотокенные query отвергаются, следующий исходный поиск не изменён. |

Измерение JSON bytes выполнено независимым `JSON.stringify` + `Buffer.byteLength`, digest — стандартным `node:crypto`. Исторический digest и обе Notes schemas зафиксированы литералами. Тест не объявляет generated sidecar examples результатами совершённой операции.

## Read-only аудит исходников

- Catalog diff добавляет только `validateOutput`: сначала проверяет принадлежность exact entry этому registry, затем использует тот же canonical payload gate и рекурсивный validator. Input/`validation.mjs` не изменены.
- Public selection предшествует документации, индексу, revision и lookup. Private/unknown sidecars отбрасываются при единственном constructor scan; query не обходит исходную конфигурацию.
- Снимок заморожен, per-query persistent cache отсутствует; fixed public index ограничен 128. Byte-cropped page продолжает позицию после реально возвращённых элементов.
- Непубличная часть registry должна оставаться допустимой для существующего `createCatalog` и общего лимита 128. Приёмка не требует успешного запуска с произвольно повреждённым private schema. Игнорирование unused sidecars не означает constant-time обработку произвольно огромной trusted configuration.
- Output shape в A1 — совместимость профиля, не право исполнения. Строгий реальный Notes ID, well-formed external write admission, idempotency и successful receipt остаются отдельным B gate.

## HTTP/PWA preflight — отдельно от PASS

Read-only исполнение прежнего `public/sw.js` в VM подтвердило: offline navigation `/agents`, `/agents/missing` и versioned `contract.json` могла вернуть cached SPA с 200. Поэтому исключение client из update handshake по одному URL небезопасно: под адресом справки уже может жить старый редактор.

Root выбрал общий fail-closed handshake и optional fixed same-origin readiness script **только в SSR документах**. Контент, форма и ссылки работают без JS; недоставленный/запрещённый script не превращает timeout в готовность. Helper не должен входить в общий SPA bundle, иначе его ранний `ready:true` мог бы обогнать async draft guard. Script только отвечает через transferred port на `PREPARE` от доверенного ServiceWorker; без регистрации worker, storage, navigation или reload.

Для последующей A3/A4 приёмки остаются: точный reserved namespace и controlled unknown responses; network-only offline navigation для `/agents` и capability API; immutable client id+URL snapshot и latest check; сохранение второго `saveDrafts` перед reload. Dev proxy имеет отдельный worker (`vite.config.ts`), поэтому dev HTTP proof не заменяет исполнение production worker. На этом этапе эти изменения не отмечены как протестированные.

## Проверенные версии

| Файл | SHA-256 |
| --- | --- |
| `modules/capabilities/server/catalog.mjs` | `498a06025107598b450406114bc331aef71cd0824c384dfdf7927d10dc6fc072` |
| `modules/capabilities/server/discovery.mjs` | `08a3e67ffe0beb5da5317917556edc4c37f24ca4a7883d4bc6b854b069d61be3` |
| `modules/capabilities/server/documentation.mjs` | `0fb296890e5073e51c15231c8c07b44da9eb4ef9b8d8ceeadd10b44a01c10e4b` |
| `modules/capabilities/server/validation.mjs` | `ce7a5731ac13dbc67646ac38a94f27d012c20ba9643bcc69fc5a5d5f31fd7076` |
| `modules/capabilities/test/discovery.acceptance.test.mjs` | `1db7ec0ca41bd2bdc9ee453a69c4eefee1a73c499462dfb96e1196b29c352b6b` |

Авторские 17 focused +11 legacy PASS не присвоены этому независимому прогону. Root HTTP/startup pin gate, rendered escaping/OpenAPI, real browser320/667/desktop, public HTTPS/crawling и внешний write сценарий остаются отдельными доказательствами. Production, чужие tests и исторические временные директории этим этапом не изменялись; commit/deploy не выполнялись.
