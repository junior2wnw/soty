# P4-C1c — independent encrypted auxiliary-store review

Дата: 2026-09-30. Проверяется первый изолированный Session/Interaction increment поверх принятой baseline3 `dc1ae217424b33cca0e9a5b60a6e4e719ea0d991`.

## Область и текущий статус

Прочитаны `oauth-profile.mjs`, `oauth-crypto.mjs`, `oauth-artifacts.mjs`, узкая redirect-правка `oauth-baseline.mjs`, фактические OAuth DDL/guards и авторские проверки. Новые независимые файлы — только этот отчёт и `modules/capabilities/test/oauth-artifacts.independent.test.mjs`. Production и авторские fixtures/tests не менялись.

Узкий increment принят независимой проверкой: **3/3 PASS, 0 skips, 3703.557 ms**, с первого запуска на Node24.21.0/SQLite3.53.4. Новых material blockers в проверенных domain files не найдено. Готовность полного OAuth/CLI этим результатом не объявляется.

Команда:

```powershell
node --test --test-concurrency=1 modules/capabilities/test/oauth-artifacts.independent.test.mjs
```

Фактически использован изолированный `var/toolchains/node-v24.21.0-win-x64/node.exe`. Log: `output/implementation-20260930/p4-oauth-artifacts-independent.log`. `node --check` нового файла прошёл. Child-процессы завершились с code0, без stderr; собственные temporary resources очищены. Test slot возвращён root сразу после выполнения.

## Независимая матрица

Собственный fixture создаёт временную настоящую Caps3 SQLite через действующий migrator, использует только синтетический key и собственные Session payloads. Авторские support fixtures не импортируются. На удалении проверяются resolved temp parent, собственный prefix и случайный ownership marker.

1. Два разных OS child-процесса открывают одну базу, сообщают готовность и затем конкурируют за один Session UID с разными jti. Должен остаться ровно один ID; повтор проигравшего после завершения обоих процессов также не создаёт row. Проверяются исходный busy timeout, отсутствие незакрытой transaction и нулевое количество authority rows.
2. При уже занятых1023 auxiliary slots два OS процесса пробуют разные новые Sessions. Должен появиться ровно один дополнительный artifact. Все прежние1023 ciphertext/digest/metadata rows должны остаться точными; проигравший не проходит после завершения гонки. Если первоначальный отказ был временным busy, отдельный тестовый повтор обязан подтвердить уже занятую квоту. Это не автоматический production retry.
3. Настоящие ciphertext и plaintext digest допустимой Session B переносятся SQL в существующую Session A при сохранении реальных DDL/guards. После закрытия и открытия keyless `createCapabilitiesService` распознаёт actual3, но не объявляет OAuth порт. Decrypting store обязан отказать для A при find/findByUid/upsert/destroy, читать B и не удалять/исправлять строки через live cleanup. Подмена — явная synthetic persisted corruption, а не внешняя OAuth атака.

Эти сценарии дополняют авторские17 cases: они не повторяют unit-матрицу каждого AAD field, границ JSON и удержанного второго handle. Два child-процесса здесь доказывают конкуренцию SQLite storage API; настоящего Connect authority fence, AS token race или consent в них нет.

## Source review

Canonical snapshot проверяет собственные data descriptors, well-formed strings, depth≤12, nodes≤1024 и canonical UTF-8≤16384 bytes; неизвестное поле с undefined не исчезает до exact shape validation. AEAD использует AES-256-GCM, nonce12/tag16; AAD включает registry, issuer, model, idHash, profile и keyId. При чтении проверяются tag, digest, canonical form и согласованность plain row projections. Codec владеет копией key и обнуляет именно её при close; это не обещание обнуления всех host-owned копий ключа.

Session window привязан к payload.iat и не пересоздаётся из времени приёма нового jti. Same-ID upsert сохраняет createdAt/UID/retainUntil и не оживляет истёкшую существующую row. Сохранение исходного iat при библиотечном resetIdentifier отдельно проверено предыдущим actual Provider HTTP gate в [session review](p4-oauth-session-review.md); новый процессный тест не подменяет его.

Для atomic admission используется синхронная injected Caps transaction с busy100ms, live schema/project/registry/lineage проверяются внутри неё. Квота проверяется до INSERT; очистка вспомогательных строк имеет один batch≤64. Удаление истёкших ephemeral artifacts по явной retention policy не равнозначно crypto validation и не используется как repair живого ciphertext.

Изменение baseline redirect ограничено сохранением явно указанного decimal `:80` до URL normalization. Прежние запреты userinfo/query/fragment/не-loopback HTTP и неканонического pathname сохранены; root static-client callback остаётся дополнительной более узкой проверкой. Обсуждавшаяся Grant seconds/milliseconds поправка в этот increment не входит.

## Найденная граница composition

Независимое чтение выявило несовместимость первого host wrapper со store: wrapper передавал `requestBinding()` каждому model upsert, а вспомогательные Session/Interaction намеренно требуют отсутствия request/stagedGrant. В настоящем authorize Provider уже имеет client context, поэтому этот путь мог отказать `oauth_context_invalid`, хотя ручное создание библиотечных моделей вне HTTP проходило.

Root принял находку, подтвердил actual HTTP RED `invalid_grant` и исправил передачу request context только требуемым bound models. Финальный root support HTTP gate — **2/2 PASS, 698 ms**, `output/implementation-20260930/p4-oauth-support-http-final.log`. Это атрибуция root, не ещё два независимых cases. Его fixture вызывает настоящий production Provider wrapper и encrypted auxiliary store, но предоставляет synthetic readiness facade; owner approval/Grant/token не выдаются. В промежуточном test assertion ошибочно ожидалась сохранённая Session до login; pinned shared/session сохраняет лишь `!new || touched`, поэтому exact первый HTTP результат — Interaction. Root сохранил промежуточный log и исправил именно эту fixture expectation.

Это finding host composition, не повод расширять authority вспомогательных rows. Независимый файл не дублирует root Provider-wrapper fixture. Ранее выявленные host ingress mapping/trusted-proxy вопросы также остаются отдельной host областью; этот crypto verdict не означает принятия всего незавершённого router/UI.

## Проверенный source pin

Все значения — SHA-256 локальных bytes, совпавшие с авторским freeze до независимого запуска:

| Файл в `modules/capabilities/server/` | SHA-256 |
| --- | --- |
| oauth-profile.mjs | e44bf5acd13176b103dd32246af9f9f354258eeaacb202005bd37b4eb97f10b5 |
| oauth-crypto.mjs | cb9712d5308cff9309fd0e0446a421002f57e9423ca1c302267869bd4c2417d1 |
| oauth-artifacts.mjs | a36a1814bb528561322fc6acc0fe9f3ed979f99765a875252cafc56a5d31faf0 |
| oauth-baseline.mjs | 0cbb127273ab43b437fa7eb540124b5de5d7f5a1d3ed8e6580c35398179263f2 |

## Предел вывода

Store остаётся isolated factory. `hasKey()` не означает готовность AS; `createCapabilitiesService` ещё не подключает его. Bound Grant/code/RT/AT, consume/revoke, owner consent, bearer authentication, private host routing, browser/CLI и Linux gates не подтверждаются этой матрицей. Наличие правильного формата ciphertext без ключа также не доказывает его расшифровку. Полные module/server/world suites здесь не запускались.
