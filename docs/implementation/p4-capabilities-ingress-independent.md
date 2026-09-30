# P4 native ingress — независимая приёмка

30.09.2026. Scope: новый `server/capabilities-ingress.js` и авторский `server/test/capabilities-ingress.test.mjs`. Reviewer добавил только `server/test/capabilities-ingress.independent.test.mjs` и этот отчёт; production/root tests не менял. Auth, trusted Host, routing, domain admission и HTTP projection ещё не входят в этот gate.

После root memory correction **новых блокеров в ingress slice не найдено**. Проверены фиксированные пределы: raw body2 MiB, общий body deadline15 s от начала чтения,8 readers/2 на socket peer,60 attempts за process-local peer window60 s,2048 peer records. Lower-limit overrides позволяют короткие tests и не могут увеличивать defaults. Это не distributed rate limit, предел общей памяти процесса или общий срок HTTP connection.

## Memory RED → correction → GREEN

Исходный reader удерживал `chunks.push(chunk)` до конца body. Byte cap ограничивал полезные bytes, но позволял по Buffer view и array entry на каждый однобайтный HTTP chunk. Независимый дочерний процесс с actual HTTP parser получил **65536 bytes** корректного JSON, разбитого на65536 chunks. Завершающий chunk ещё не послан: body остаётся pending. После трёх GC с event-loop turns heap вырос на **13953288 bytes**, около213× payload. Это измерение процесса, без чтения внутреннего `chunks`, подмены Buffer или mock pixel/state assertions. Test child имел `--expose-gc --max-old-space-size=64`; client allocations находятся в другом процессе.

Persisted test требует меньше2 MiB retained heap для этого64 KiB upload — запас для runtime noise, а не точное требование к heap каждого приложения. Он падал на исходном source SHA `5037c1ff9ba73c3f9f0f1b47ff24c2ba7afa02ac21bea75471df6ccc414ab803`. Reviewer run: **5 tests /4 PASS /1 FAIL /0 SKIP,433.735 ms**. Остальные реальные HTTP cases уже проходили.

Root заменил список chunks одним `Buffer.allocUnsafe(bodyBytes)`, последовательным `chunk.copy` с offset и проверкой overflow **до** копирования. Decode читает только заполненные `0..bytes`, не неинициализированный хвост; buffer освобождается до parsing/settlement либо при ошибке. Byte/deadline limits не увеличены. Reviewer перечитал узкую correction. Это ограничивает raw allocation максимумом16 MiB при восьми readers; декодированные строки, JSON result, sockets и прочая память процесса считаются отдельно.

Root после correction повторил author+independent: **12/12 PASS,747.63 ms**. Тот же child case наблюдал **252424 bytes** retained heap вместо13953288. Это root GREEN, не второй собственный запуск reviewer. Test и его threshold не изменялись ради результата.

## Остальные независимые доказательства

- Два настоящих незавершённых HTTP body занимают слоты. Третий получает отказ; abort первого освобождает ровно один слот. Replacement проходит, следующий опять отказывает, пока второй исходный reader жив. Double release из auto-settlement и host finally не делает counter отрицательным и не допускает лишний body.
- Actual UTF-8 payload ровно на raw cap проходит; плюс один whitespace byte отказывает. Равнозначная decoded строка с более длинным JSON escape тоже отказывает по raw bytes. Затем нормальный запрос снова проходит. Реальные дубли `Content-Type` с разным регистром обнаруживаются через raw headers; error не возвращает исходный body.
- Invalid body и успешный POST расходуют попытки одного настоящего socket peer. Новые `X-Forwarded-For`, `Forwarded` и `X-Real-IP` не создают новую peer identity и не обходят rate отказ.
- JSON keys сравниваются после decode: escaped duplicate body/idempotencyKey, неправильный тип, trailing non-JSON whitespace и второй object отвергаются. Допустимые escaping, surrogate pair emoji и NFD сохраняют исходные значения. Actual HTTP invalid UTF-8 — encoded surrogate, overlong sequence, truncated multibyte и code point вышеU+10FFFF — не доходит до accepted callback.

Авторские7 tests прочитаны: в том числе непродлеваемый chunk traffic deadline, BOM, body abort, early refusal, GET bypass body budget и bounded peer table с expiry только неактивных records. Эти cases не выдаются за собственный independent implementation. В independent HTTP fixture host response намеренно минимален и при ошибке закрывает соединение; это не настоящий private native router.

## Воспроизведение и identity

```powershell
$ingressNode = Join-Path (Get-Location).Path 'var/toolchains/node-v24.21.0-win-x64'
$env:PATH = $ingressNode + [IO.Path]::PathSeparator + $env:PATH
& (Join-Path $ingressNode 'node.exe') --test --test-concurrency=1 server/test/capabilities-ingress.independent.test.mjs
```

Среда — Windows, изолированный Node24.21.0. Tests не создают persistent files и не читают credentials. Для child переданы только runtime path/Windows temp системные поля; process останавливается по собственному handle, не по поиску чужого PID. Тяжёлые suites не запускались параллельно.

| Frozen файл | SHA256 |
| --- | --- |
| `server/capabilities-ingress.js`, после root correction | `7619c9eeb217641f9bf6ae6d386f1658de647e6dce161fae4c8a234ffbb72c19` |
| `server/test/capabilities-ingress.test.mjs` | `ca5670a3a44e868ffa24dced223231bf9cf7387ee8e9952184bcae121d71c417` |
| `server/test/capabilities-ingress.independent.test.mjs` | `2a77eba6d5cdd2d41006d721d4033e86de1f24919558d845a5aa50e29ad30fea` |

## Обязательные границы последующей composition

`finish(error)` приостанавливает незавершённый request и освобождает reader; он не дочитывает недоверенное тело. Настоящий handler должен выставлять `shouldKeepAlive=false`/`Connection: close` для unread/incomplete body при timeout, overflow и раннем отказе. Иначе paused upload останется жив вне reader budget. Root принял это для router; в данном gate production router ещё не проверяется.

Fatal UTF-8 не равен well-formed Unicode проверке decoded JSON: escaped lone surrogate формально может быть JSON string. Его отклонение до admission остаётся обязанностью fixed native domain boundary из effect plan; @1 validator не изменён. Прежняя native authority, current read ACL, content-free receipt, no-store/private errors и отсутствие SQL/token/input в реальном HTTP ответе требуют отдельной composed network приёмки. Process-local ingress сам не предоставляет ни одно из этих полномочий.
