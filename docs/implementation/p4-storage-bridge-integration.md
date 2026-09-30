# P4-B1: strict v3 storage bridge — локальная интеграция

30.09.2026. Принят отдельный bridge для исторических **Notes1 / Capabilities1** вместе с Rooms1–2 и Apps1–6. Native schema2, её migration и исполнение `notes.createDraft` сюда не входят. Production не обновлялся.

Основание: [контракт](p4-native-storage-contract.md), [preflight](p4-storage-guard-preflight.md), [авторская реализация](p4-storage-guard-bridge.md) и [независимая приёмка](p4-storage-bridge-independent.md). Последние два отчёта содержат точные source hashes и границы SQLite-проверок.

## Итог

Trusted read-only probe распознаёт Notes и Capabilities независимо. Строгие image manifest, format DTO и сохранённый START receipt теперь имеют version3 и все четыре stores. Старые v1/v2 declarations не расширяются подразумеваемыми пустыми базами; copied container label не заменяет actual image identity. Неизвестный формат, неправильная структура, orphan sidecars и недоступная база закрывают запуск без инициализации или repair.

Rollout и host-controller проверяют actual old image до CREATE/изменений serving container и повторяют свежую проверку работающего прежнего reader непосредственно перед STOP. Это закрывает автоматический первый переход с unlabelled/v2 image: возврат в тот же несовместимый container нельзя объявить готовым откатом. Для production остаётся отдельный reviewed bootstrap/recovery.

Historical schema fixtures закреплены raw SHA и `.gitattributes` с LF, чтобы `core.autocrlf` не менял проверяемые bytes на Windows. Runtime pin Docker этим checkpoint не меняется.

## Проверки

Из primary worktree, isolated Node24.21.0 / SQLite3.53.4, process-local PATH с тем же runtime для дочерних процессов:

```text
node --test --test-concurrency=1 deploy/connector/*.test.mjs deploy/connect/*.test.mjs
```

Итог root: **223 tests / 220 PASS / 0 FAIL / 3 skip**, 75.918 s. Лог: `output/implementation-20260930/p4-b1-storage-bridge-deploy-final-root.log`. Независимые 7/7 входят в этот общий прогон; отдельный reviewer gate не суммируется с ним как дополнительные уникальные tests.

Три открытые проверки: opt-in Docker archive foreign-owner0600; file symlinks Apps; file/sidecar symlinks Notes/Capabilities при недоступном Windows privilege. Они не объявляются пройденными. Directory junction checks выполнены. Exact Linux runtime/RO WAL и Docker mount semantics имеют следующий отдельный [canary plan](p4-storage-linux-canary-plan.md).

Первый общий прогон нашёл два слишком широких root assertions: после отказа свежего probe controller законно выключал restart policy никогда не запускавшегося candidate. Assertion исправлен на точную границу — отсутствие START/STOP, policy mutation serving container и maintenance enter; working original сохраняет `Running=true`/`always`, candidate остаётся `created`/`no`, durable transaction сохраняется. Production-код ради PASS не ослаблялся. Финальный полный повтор прошёл.

Проверены также прежние deploy/rollback/recovery, delayed START receipt, Apps migrations и сохранность main/WAL. `git diff --check` проходит. World/type/build из [runtime baseline](p4-runtime-baseline.md) относятся к предыдущему checkpoint; здесь они не выдаются за повторный запуск. Bridge не меняет frontend, HTTP/domain modules или пользовательский UX.

## Открытые условия

Format recognizer не равен полному row/FK/FTS integrity scan, cross-store atomicity или backup restore. SHM read marks не считаются побайтово постоянными. Connect/World не получают новые reader declarations в этом bridge.

Далее: default-off native reader2, настоящий create-only effect с durable proof и независимыми crash/race проверками. До production writes требуются actual compatible fallback, Linux proof, согласованная зашифрованная копия всех stores и выполненное isolated восстановление. Свежий GET-only inventory подтвердил прежние serving ID/image/StartedAt; пользовательские данные и конфигурация не читались и не изменялись.
