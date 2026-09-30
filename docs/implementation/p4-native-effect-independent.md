# P4-B2 — независимая проверка native Notes effect

30.09.2026. Независимый domain review после B1a, [effect plan](p4-native-effect-plan.md), [B2a API checkpoint](p4-native-effect-b2a.md) и [host plan](p4-native-host-plan.md). Проверяются реальные пользовательские последствия: неизвестный результат не превращается в отрицательный, временное отключение не отменяет намерение, новое удостоверение не продлевает старое разрешение, историческая квитанция не восстанавливает удалённую записку.

Окончательный собственный bounded gate: **3/3 PASS,0 FAIL,0 SKIP,776.3752ms**, Node24.21.0/SQLite3.53.4. Исправления прочитаны после авторского refreeze; в проверенном domain slice открытых блокеров не осталось. Это не общий B2 verdict: HTTP, реальные kill/reopen и two-writer suites имеют отдельные доказательства, deployment/migration здесь не разрешались.

## Подтверждённый defect: operational disable превращался в отмену

В новом `modules/capabilities/test/native-effect.independent.test.mjs` построена собственная composition без авторских fixtures: подписанный owner через настоящий Connect, opaque credential от действующего Caps API, реальный `Connect.withAuthorityFence`, отдельные SQLite Connect/Caps/Notes.

Последовательность:

1. Native admission и отдельный durable marker успешно завершаются.
2. Caps повторно открывается с `executionEnabled=false`; исходное разрешение пользователя по-прежнему действует.
3. Старый общий `invocations.reconcileAuthorization` получает `capability_disabled` и записывает `cancel_requested=1`.
4. Новый `nativeNotes.reconcile` читает null proof, принимает этот флаг за отмену пользователя и коммитит terminal cancellation, refund и input purge.

Собственный RED на Node24.21.0/SQLite3.53.4: **1 test,0 PASS,1 FAIL,0 SKIP**,309.0327ms. Фактическая безопасная диагностическая строка: `outcome=not_applied, status=cancelled, cancel=1, inputPurged=true`; ожидание — `held`, сохранённые input/reservation и возможность одного эффекта после включения. Это не только неверная надпись: временная остановка необратимо закрывала исходное намерение.

Автор внёс узкую поправку в общий reconciliation: `capability_disabled` для существующего native intent возвращает `authorized:false` без cancel/budget mutation. Я прочитал diff: явный `requestCancel`, настоящий revoke и proof-first settlement не отменены. Сначала автор повторил независимый case: **1/1 PASS,320.95ms**. Затем мой окончательный трёхсценарный прогон независимо подтвердил GREEN: `outcome=held, status=accepted, cancel=0, inputPurged=false`; после включения происходит ровно один эффект.

## Собственные реальные сценарии

Все три используют собственную fixture без авторских seed/helpers, реальный подписанный Connect и его synchronous authority fence, actual Caps/Notes services и три SQLite files.

| Сценарий | Фактические assertions |
| --- | --- |
| Operational disable после marker | Нет ложного cancel, input/reservation сохранены; same-key reuse и один effect после включения |
| Notes COMMIT → потерянный возврат → человеческая правка/trash/purge → cancel + revoke | Permanent proof существует при отсутствии Caps receipt. Сохранённый context уже недействителен. Reconcile завершает success/spent1 с исторической revision1 receipt; старое удостоверение не читает результат. Note остаётся deleted, FTS пуст, proof неизменён; input purged. Другая действующая credential того же grant читает ту же квитанцию после reopen с handler disabled и недоступным Notes port. Изменённый input с тем же key конфликтует, новой Notes нет |
| Новая credential после original expiry | Текущий read/replay разрешён, но исходный абсолютный срок исполнения не продлён: failed/not_applied, ноль Notes/proofs, reservation освобождена без spending. Откат clock не открывает terminal intent снова |

Второй сценарий теряет return через явный fault port **после настоящего Notes COMMIT**, до Caps receipt. Это не аварийное завершение процесса. В projection отдельно проверено отсутствие исходного title/body, bearer, store identity и input digest. Подписанная человеческая правка и purge выполнены настоящим Notes RPC; тест не удаляет Notes SQL-обходом.

## Прочитанные существенные границы

- Admission проверяет текущий read actor/key/fingerprint до новых operational/quota checks. Authorization snapshot исходного исполнения при replay не заменяется свежей credential. `get` отдельно проверяет current own account/client/principal/grant; actorless completion не является внешним допуском.
- `beginAttempt` коммитит marker до Notes effect. Effect удерживает Connect→Caps и открывает Notes transaction в том же порядке. Отсутствие proof проверяется в сохранённом matching Notes store; чужой/missing store не даёт отрицательное доказательство.
- Notes документ, FTS, counters, обычная receipt и permanent create proof коммитятся вместе. Caps receipt, spent/released budget, status и input purge коммитятся другой транзакцией. Между ними остаётся окно неизвестного результата; компенсация Notes или новый key его не закрывают.
- Exact permanent proof имеет приоритет перед cancel/revoke/operational disable. Терминальный результат читается как исторический revision1 факт без текущего body/existence lookup Notes. Scope внешнего текущего чтения всё равно обязателен.
- Opaque token связан с mode и текущим синхронным frame; public/generic native dispatch/result обход не добавлен. Sibling policy epoch не используется как слепой запрет всей сохранившейся цепочки, исходный абсолютный expiry остаётся обязательным.

## Отдельные исправления composition и recovery

Reviewer передал автору две source hypotheses. Автор воспроизвёл их отдельными RED: normal sync port, вернувший rejected Promise, завершал actual child с exit1; malformed host fence сохранял callback и принимал позднюю работу уже после возврата из `fenced`. Это ошибки доверенной server composition, не внешняя атака и не отказ настоящего синхронного Connect fence.

Авторский `native-faults.test.mjs` сообщил **7/7 PASS** после поправок: rejected Promise из шести вариантов ports/verifier/fence наблюдается без `await`; synchronous refusal сохраняется, late callback не пишет DB. Я прочитал поправки отдельно: `synchronous` присоединяет rejection handler до отказа; callback допускается только при `accepting && running && !invoked`; `finally` снимает accepting и frame. Notes verifier использует такое же containment. Это авторское actual-child доказательство плюс независимый source review, не мой дополнительный исполняемый gate.

Ещё один defect нашёл root: один pending intent с чужим Notes incarnation останавливал recovery page и не давал обработать следующую здоровую proof-positive строку. Согласованная поправка возвращает для такого элемента только `{invocationId,errorCode:'native_reconciliation_failed'}` и продолжает bounded страницу; inventory failure до списка остаётся ошибкой. Прочитаны исходники и авторский реальный old-store/healthy-proof case. Этот per-item error не является `not_applied`; private exception/body не публикуются. Recovery вызывает только `reconcile`, никогда `execute`. Новая union-форма должна читаться вместе с последующим B2b refreeze; первоначальная B2a квитанция описывает старый вариант, останавливавший страницу.

## Проверенные source bytes

| Файл | SHA256 |
| --- | --- |
| `modules/notes/server/native.mjs` | `16371da8a23d9017b0380f01341d57fa26b08d5b44d0fb33cda86a56ae9520f9` |
| `modules/notes/server/index.mjs` | `32befd49b596714293f01ae08a7b5a0015e15fc7b9f8eb9311e95d76b8a9dfc7` |
| `modules/capabilities/server/native-notes.mjs` | `1b3f285692f3cfa4054c3f4ce88b15704db14834bb4ede6fb1ced043a161b02a` |
| `modules/capabilities/server/index.mjs` | `d9984d1aa2b8e6fdbac18d0cdb989ee3e9604ff8b3e3980ad091fab135cf9618` |
| `modules/capabilities/server/access.mjs` | `e837bd8c01813f8a7efb2a7f2872e21bcdb9615c2eadb59f42fcac6b067d15e5` |
| `modules/capabilities/server/invocations.mjs` | `b4e1fb3b48016d820593ef6e0ba04e5192843ae1a566343778ee0009f4ddb836` |
| `modules/capabilities/test/native-effect.independent.test.mjs` | `e3a72c0893fe6ddae1ad65a6f45382e9451e7a89a3d2baddc9257621deacb616` |

Reviewer не менял эти production files или авторские tests. Исходные B2a SHA не переносятся на исправленную реализацию.

## Команда и предел доказательства

```powershell
$independentNode = Join-Path (Get-Location).Path 'var/toolchains/node-v24.21.0-win-x64'
$env:PATH = $independentNode + [IO.Path]::PathSeparator + $env:PATH
& (Join-Path $independentNode 'node.exe') --test --test-concurrency=1 modules/capabilities/test/native-effect.independent.test.mjs
```

Все БД создаются в новом temp directory с собственным nonce marker; cleanup проверяет canonical parent/prefix/marker. Ранее запрещённые temp paths не затрагиваются. Подписи и bearer используются только в памяти; тексты/токены не печатаются. Здесь нет HTTP/browser/remote запуска, DDL migration rollout, Linux reader2 или backup/restore проверки. Настоящие crash workers/two-writer/rate/recovery tests автора и HTTP tests root атрибутируются отдельно и не выводятся из source review.
