# P4-B2 Connect authority fence — независимая приёмка

30.09.2026. Проверены root diff `modules/connect/server/index.mjs`, его отдельный `authority-fence.test.mjs`/`support/authority-writer.mjs`, [host plan](p4-native-host-plan.md) и [effect plan](p4-native-effect-plan.md). Production source и авторские tests reviewer не менял. Единственный новый исполняемый файл reviewer: `modules/connect/test/authority-fence.independent.test.mjs`.

После узкой root correction **блокеров в этом Connect slice не осталось**. Это приёмка сериализации и lifecycle одного host-only fence. Native Notes execution, opaque context lifetime, три-store recovery, внешний HTTP и production rollout этим не принимаются.

## Найденный RED и исправление

Первоначальный `withAuthorityFence` хранил пойманное значение в `primaryFailure`, а восстановление PRAGMA проверяло `if (!primaryFailure)`. JavaScript разрешает `throw undefined/null/false/0/''`. Если после такого callback ещё и падает восстановление timeout, первоначальное исключение заменялось ошибкой cleanup.

Независимый сохранённый case воспроизвёл **RED**: callback `throw undefined`, injected `DatabaseSync.exec` failure только на восстановлении `PRAGMA busy_timeout=713`; наблюдаемый результат — `Error: synthetic restore failure` вместо `undefined`. Это нарушение заявленного сохранения первичной ошибки при двух сбоях, не подтверждённый ACL bypass и не утверждение, что текущий Notes handler бросает такие значения. Fault injection не заменяет SQL: обычные BEGIN/ROLLBACK и остальные PRAGMA выполняются настоящим DatabaseSync.

Root заменил хранение значения ошибки отдельным `hasPrimaryFailure` boolean. Reviewer перечитал полный текущий diff: флаг ставится в catch независимо от значения throw; restoration error всё ещё возвращается, когда более раннего failure не было. Acquisition busy по-прежнему переводится в `connect_authority_busy` только до входа в callback; исключение downstream с тем же SQLite errcode не переименовывается. Assertions не ослаблялись.

## Независимые сценарии

Все шесть cases имеют собственную fixture, без импорта авторского support. Подписанные requests используют настоящее Connect challenge, P-256 signature, bootstrap и enrollment. В keys/receipts не выводятся private keys или credentials.

1. **Реальный конкурентный revoke.** Заранее открыт второй Connect service в отдельном Node Worker с отдельной SQLite connection. Сигнал снимается непосредственно перед его фактическим `BEGIN IMMEDIATE`, затем main fence подтверждает отсутствие завершения writer и ещё действующий actor. После release signed revoke коммитится; следующий fence видит actor неактивным. Второй target проверяет обратный порядок revoke-before-fence. Это real SQLite concurrency между connections/threads, не fake callback и не отдельный OS process.
2. **Busy и точное восстановление политики.** Предыдущий connection-local timeout установлен в713 ms, внутри fence виден100 ms. Другой настоящий writer удерживает BEGIN дольше короткой acquisition policy. Отказ не вызывает callback; после него обычная signed mutation дожидается release и проходит. Проверены восстановление ровно713, callback failure с SQLite errcode5 без ложного acquisition mapping и повторное использование service. Значения timeout — busy policy, не обещание общей latency/fsync deadline.
3. **Nested entry до чтения запроса и proof.** Во время fence `handle` отказывает до getter `origin` и до consumption готового signed proof; тот же proof затем работает. `close` и nested fence закрыты. Отдельная signed extension ловит nested refusals, затем сама падает: внешний proof остаётся consumed, action error сохраняет прежний безопасный mapping, последующий status/обычный request работают.
4. **Синхронная граница.** Обычное возвращённое значение сохраняет identity. AsyncFunction/AsyncGeneratorFunction не вызываются; Promise/thenable результат не засчитывается успехом. Rejected Promise обработан без unhandled rejection, transaction закрыта, следующий fence работает. Это проверка доверенного callback contract, не sandbox для произвольного JS или запрет любой когда-либо захваченной closure.
5. **Primary failure.** Persisted RED покрывает все falsy throw values и обычный Error при injected restore failure. После каждого пути transaction закрыта и service пригоден к дальнейшей работе.
6. **Ошибка после другого commit.** В callback коммитится запись в отдельной synthetic SQLite DB. Fault injection до Connect COMMIT, сразу после действительного Connect COMMIT и при восстановлении PRAGMA оставляет этот downstream effect существующим. Rollback Connect вызывается лишь когда его transaction действительно ещё открыта. Это реальная демонстрация границы разных DB commits, не native Notes proof/reconciliation test.

Авторские семь cases дополнительно проверяют rollback+restore failure с оставшейся transaction и fail-closed до recovery, closed service и outer transaction после nested refusal. Они прочитаны; независимый файл не выдаёт их за собственную реализацию.

## Исполнение и атрибуция

Окружение: изолированный **Node v24.21.0 / SQLite3.53.4**, Windows. Независимый запуск после передачи test slot:

```powershell
$fenceNode = Join-Path (Get-Location).Path 'var/toolchains/node-v24.21.0-win-x64'
$env:PATH = $fenceNode + [IO.Path]::PathSeparator + $env:PATH
& (Join-Path $fenceNode 'node.exe') --test --test-concurrency=1 modules/connect/test/authority-fence.independent.test.mjs
```

- **Reviewer run до repair:**6 tests,5 PASS,1 FAIL,0 SKIP,1200.7796 ms. Падение — описанный primary-error case; остальные пять прошли.
- **Root run после repair:**13/13 PASS author+independent,2140 ms, по сообщению root. Reviewer не запускал второй параллельный suite ради дублирования; repair проверен чтением.
- **Root прежний полный Connect regression до repair:**77/77 PASS,0 SKIP,5234.0093 ms; сводка прочитана в `output/implementation-20260930/p4-connect-fence-root-regression.log`. Этот прежний run не объявляется полным regression окончательной correction.

Fixtures создаются только в новых случайных `soty-connect-independent-*` директориях. Cleanup прекращает собственных workers, закрывает собственные connections и проверяет canonical parent, prefix и случайный ownership marker перед удалением. Ранее оставленные/запрещённые directories не затрагиваются. Глобальная prototype instrumentation ограничена тестом и восстанавливается в finally; suite исполняется последовательно.

## Source identity и оставшиеся границы

| Прочитанный файл | SHA256 |
| --- | --- |
| `modules/connect/server/index.mjs`, после root correction | `41eb5e3f14989ec0ae3139495f396a0f2d266ad52c63825df8482d05fdd9d2a8` |
| `modules/connect/test/authority-fence.test.mjs` | `ecf5309b4c9d5c82d55d2b457cf19092c5466a82b8ec93643d0de0fbbd696ac9` |
| `modules/connect/test/support/authority-writer.mjs` | `92a77743e2e62fd74a3ac768f18731aa3d1d04ddab84ac0178a283de2cd58af7` |
| `modules/connect/test/authority-fence.independent.test.mjs` | `bba47c4c28e3b6d854c5f108a89b51bae1afdc0fc1652e706076f79af130b043` |
| `docs/implementation/p4-native-host-plan.md` | `9d0c36d984676a0814fb75dfbebcfbd36ff4edc73b4f34c9cdcfb2a95027fa26` |
| `docs/implementation/p4-native-effect-plan.md` | `5467e0dcd8b2a2efa02bd695d3b28a626c2da4d3a1a3387346847aa04589393b` |

Connect fence выдаёт только writer serialization, не identity и не право создавать Notes. Current/original credential, creator devices и ancestry обязан проверять coordinator под Connect→Caps; Notes получает отдельный opaque token с активным frame и повторной проверкой до commit. Actorless proof-first settle после revoke не даёт права отправить HTTP receipt: внешняя projection снова проходит current read ACL. Из-за проверенного separate-commit поведения любая ошибка после Notes/Caps COMMIT не означает `not_applied` и не разрешает refund/new-ID retry. Эти доменные и network seams остаются отдельными B2 gates.
