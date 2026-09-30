# P4-B1b — локальная приёмка deployment reader2

30.09.2026. Root интегрировал [reader2](p4-reader2-implementation.md), [независимую приёмку](p4-storage-bridge-independent.md) и Dockerfile manifest. Manifest v3 теперь заявляет точные Notes `[1,2]`, Capabilities `[1,2]`; Rooms/Apps не меняются. Это исходный контракт будущего image, не утверждение о нынешнем production/fallback image.

## Проверенные исходы

- Полный serial `deploy/connector/*.test.mjs`: 161 tests,157 PASS,2 FAIL,2 platform SKIP,60045.942ms. Оба FAIL относились к старым негативным fixtures, считавшим format2 неизвестным. Они заменены format3; отдельные новые cases проверяют, что настоящий original reader1 несовместим с format2, хотя candidate reader2 совместим. Отказ происходит до STOP, policy/helper и candidate START.
- Весь исправленный `deploy/connector/rollout.test.mjs`: 57/57 PASS,0 SKIP,301.1856ms. Включает два добавленных случая old image/new format. Production controller этим исправлением не менялся.
- Весь `deploy/connect/*.test.mjs`: 82 tests,81 PASS,0 FAIL,1 platform SKIP,32254.7175ms. В host-controller такие же проверки различают неизвестный format3 и реально несовместимый reader1/format2, сохраняя running/policy/created-state проверки до STOP.
- Обновлённый inventory двух deploy suites составляет245 tests:242 PASS и3 platform SKIP по этим последовательным прогонам. Это не один общий зелёный запуск245 тестов. Пропуски Windows platform cases сохранены явно; Linux filesystem поведение проверяется следующим отдельным canary.

Логи: `output/implementation-20260930/p4-reader2-root-deploy.log`, `p4-reader2-root-rollout-correction.log`, `p4-reader2-root-controller.log`. Node24.21.0/SQLite3.53.4, concurrency1. Независимые7/7 reader tests входят в полный connector suite; отдельно выполненные авторские WAL cases reviewer не выдаёт за собственные.

Старые v1 fixture/provenance не переписаны. Настоящий main1/WAL2, mixed пары и сохранённые native witnesses проверены локально; production Notes/Caps не мигрировались. Root review Dockerfile подтверждает только точность source manifest. Реальный Linux reader2, полный application image, совместимый fallback, bootstrap прежнего unlabelled serving и согласованный encrypted restore остаются открыты по [Linux подплану](p4-reader2-linux-plan.md).

Предыдущие Linux canary sources/bundles/receipts неизменяемы; новый reader2 получает отдельный namespace, source, review и однократный запуск. Никакого START с подменённым label или обходом compatibility guard не добавлено.
