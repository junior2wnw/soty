# P4-D2-R1c — root Windows sender library evidence

01.10.2026. **WINDOWS LIBRARY CHECKPOINT ПРИНЯТ: ROOT52 PASS и два независимых source/evidence GO.** Основание — [sender plan](p4-authenticated-sender-plan.md), [source receipt](p4-authenticated-sender-receipt.md), принятый R1b `c030addbc2cfa378bc1de2e41e3cdcf8971bdb48`. Runtime исполнял только root; author и два reviewers читали source/evidence. Operator/SSH/Docker handoff — отдельный следующий slice по [preflight](p4-restore-operator-handoff-preflight.md).

Root перед первым запуском сверил exact worktree SHA новых source/test, unchanged R0/R1a/R1b tests/sink, Node и system tar; после запуска pins совпали снова. Один serial process завершился exit0. Log `output/implementation-20260930/p4-authenticated-sender-windows-first.log`:12,415B, SHA256 `2c0f609c303c9bf88d9e99f0da8a06edf60b03e113b23f86c27fab86c79db74d`. **52/52 PASS, 0 fail/cancelled/skipped/todo, 6638.726ms**;42 top-level cases/0 suites. Это25 sender cases (18 top-level+7 nested) и27 прежних R0/R1a/Windows receiver cases.

Команда: pinned Node24.21.0 `--test --test-concurrency=1 --test-reporter=tap` с `verify-backup.test.mjs`, `restore-backup.test.mjs`, `restore-sink.test.mjs`, `restore-sender.test.mjs`. Startup `NODE_OPTIONS=--v8-pool-size=1`, `UV_THREADPOOL_SIZE=1`; PATH ограничен pinned Node directory/Windows System32/Windows, поэтому fixture использует сверенный system tar. Node SHA `ba4e6d110e8c1592a1ecd390f6b05f3da124b13871a5be62b341a07a853c6c32`; tar SHA `4e598a8cec84af779e3377442fce2b94b976f614ed4c6e5a665e19308fd1e379`. Исходный log сохранён; повтор после PASS без нового изменения/concern не требуется.

## Что проверено

- Unchanged API/CLI/dry parity, включая прежние parser/envelope/GCM и fixed errors. R0 сохраняет key-before-open и прежние receipts; private sender не добавляет output options в R0/dry.
- Два positional encrypted passes по одному настоящему FD, stat/EOF и trusted archive/manifest/witness pins. Независимо собранные framing bytes совпали с output; все consumed transient chunks wiped после callback. Изменение pathname не переключает FD.
- Повреждённый key/tag/archive, joint DB omission и limits+1 дают0write/0end в первом pass. Real second-pass tag mutation даёт уже переданные bytes, но GCM final отказывает и end не вызывается; newly valid ciphertext replacement не заменяет approved archive pin.
- Native HWM1 drain-before-callback, held write и actual close-before-callback: sender/буфер остаются pending до supplied callback, следующий read не начинается. При failure suppressed drain не ожидается.
- Held final и destroy не дают ранний success. Abort после end с незавершённым final и manual finish при native writableFinished=false дают cleanup_pending с настоящим native close; actual final callback освобождён fixture owner после assertions. Pre-end0end и post-end1end/no success различаются.
- Real descriptor operations, actual awaited close, bounded virtual monotonic deadline controls, captured options, ignored abort reason, fixed diagnostics и удаление owned listeners проверены. Произвольному hostile JS stream/hung syscall не обещается hard termination.

## Source binding

| File | Bytes | SHA256 exact worktree |
|---|---:|---|
| `deploy/connect/backup-format.mjs` | 28250 | `f4e57a4791bc184e4b06618cc15ee8b6f539dd23de28ef8adee87cca15aa41bb` |
| `deploy/connect/restore-backup.mjs` | 23569 | `171b2b9669dab3c89c6672e3f259f913f3a91a0f9d61c920f5e184ae9234f5a9` |
| `deploy/connect/restore-sender.test.mjs` | 30289 | `f6945200d0da1e98c6cec8f3c39e8849b1c9335723da99cb98dd9abddf43d252` |

До runtime root и два независимых reviewers приняли исходный shared pass/lifecycle, затем только narrow native-finish guard +один causal case. Обратная текстовая реконструкция восстановила предыдущие source/test SHA; остальные24 cases не менялись. Это **source finding**, не выполненный RED старой версии; обе истории SOURCE FREEZE/NOT RUN сохранены в авторском receipt. Whole-product critic и publisher отдельно прочитали first-run log, сверили source binding и все52 cases: **evidence GO local library checkpoint**, повторов/правок с их стороны не было. Происхождение actual execution/exit0 принадлежит root; TAP сам по себе не доказывает OS/transport closure или Windows ACL.

## Граница результата

Всё исполнено на synthetic Windows fixtures. Sink на Windows проверяет только unsupported-before-I/O; **Linux66 не выполнялся этим gate**. Предыдущий actual Linux R1b41 связан со своими историческими source pins, а не автоматически с новой версией shared reader. Windows DACL/FileShare keeper/DPAPI handoff, реальный SSH→Docker receiver и coupled receipts не проверены. Не читались настоящий keystore/backup/config; serving не менялся, модели не вызывались.

Library PASS не подтверждает immutable Windows copy или completed underlying transport, полноту настоящего B/cold source R1d, exact application images, first serving transition, production restore либо весь master-план. Следующий slice сначала докажет Windows guard на synthetic данных, затем binary handoff/actual receiver completion/отдельный RO readback; existing journal остаётся единственным владельцем принятия результата.
