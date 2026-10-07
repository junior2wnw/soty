# Доставка bounded413 и closing socket fence

Кандидат находится только в own branch `codex/capabilities-body-causal-f34`. Baseline producer `13682c5b7c34affeb525e02c2f19d92d22bc26a1` сохранён отдельно: `output/body-causal/producer-13682c5b`, manifest SHA `977d3153d8076885b9e16e7719ba6176b38cff71198d46b28b8630d915479646`. Его исходные f34 handler/fixture и 5/12 ECONNRESET trace не перезаписаны.

При отказе unread oversized body handler ждёт завершение закрытого discard перед отправкой 413 и закрытием TCP. `capabilities-rejection-drain.js` не парсит, не копирует/накапливает body и не выполняет domain/auth/ledger operations. Profile фиксирован: 2162688 unread bytes, 2000ms, 8 active slots, 2 per peer; peer map содержит только active entries. Несоответствующий declared size, overflow, capacity, wall deadline и abort дают non-success закрытие connection. EOF после deadline не считается успехом. Поддержанные exact2MiB+1 uploads подтверждаются actual request end, затем получают JSON413.

Действующий валидный JSON limit остаётся 2MiB. Discard budget относится только к оставшимся unread bytes после отказа; для chunked input существующий Native reader мог уже прочитать до своего2MiB budget плюс текущий native chunk. Это конечная суммарная граница, а не увеличение допустимого body. Нет нового body/source-kind selector. Fully-read semantic failures сохраняют прежний keepalive; incomplete authority/header rejection сохраняет immediate401/400.

`http-closing-socket.mjs` ставит процессный private fence синхронно на header/data oversized failure, до async Promise rejection. Первый user middleware HttpApp проверяет его до hosting/traffic/REST/MCP; index upgrade entry проверяет до upgrade dispatch. Одна following parser-owned request quarantined, не передаётся route/auth/admit и не уничтожает преждевременно первую413; дальнейшая pipeline приводит к abort. Fence существует до emitted socket close; metadata и terminal error listeners очищаются. Нет claims delivery по server finish/closed flag.

Проверки на explicit bundled Node24.19.0, `--test-concurrency=2`, actual native HTTP fixtures: **16/16 PASS, 0fail, 0cancel, 0skip**. Native keys/auth не выводились; все stores создавались в owned synthetic fixture directories, loopback-only. Исходные HTTP tests/fixture не изменены.

- 12 повторов исходного f.http exact2097153 bytes: все получили413/payload_too_large.
- 6 fragmented uploads: три chunk patterns через Content-Length и chunked; все413.
- 6 actual rawTCP pipelines: Content-Length/chunked + valid REST/MCP/upgrade. Один413, ноль following route/auth/effect; Notes ledger0.
- Huge declared size, genuine client abort, slow partial/lying length, peer slot exhaustion и endless chunked byte overflow: bounded non-success, ledger0, slots/peer entries/data/error listeners освобождены; valid request после отказов создаёт ровно одну Note.
- Исходный6 HTTP suite,4 lifecycle и2 independent auth/unknown-wire suites сохраняют поведение, включая unfinished early401 и revoke до admission.

Команда проверки из own WT:

```powershell
& 'C:/Users/Junio/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node.exe' --import './scripts/probes/capabilities-causal-readonly-deps.mjs' --test --test-concurrency=2 server/test/capabilities-oversized-rejection.test.mjs server/test/capabilities-actions.test.mjs server/test/capabilities-actions-lifecycle.test.mjs server/test/capabilities-actions.independent.test.mjs
```

Полный безопасный output — `output/body-causal/fix-final-tests.log`. Dependency resolver читает существующие Main dependencies; Main/config/node_modules не изменены. Fix требует независимого Root review перед merge. FullWorld/production/Engine/SourceCold не запускались этим кандидатом.
