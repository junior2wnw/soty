# P4-B1b — reader2 до разрешения миграции

30.09.2026. Только подплан и статическая сверка. В этой работе не меняются production source, Dockerfile, данные, bridge или замороженные canary bundles; тесты, Docker и SSH не запускаются. Основания: [storage contract](p4-native-storage-contract.md), [B1a baseline](p4-native-storage-baseline.md), [принятый bridge](p4-storage-bridge-integration.md), [первый Linux canary](p4-storage-linux-canary-plan.md).

Последовательность обязательна: **точный host reader2 → проверенный полный default-off application image → этот image реально становится serving baseline → отдельное разрешение migration2**. Локальные readers и label не заменяют последние два шага. Native admission/execution остаются B2; этот план их не открывает.

## 1. Нынешняя граница и три отдельных checkpoint

| Checkpoint | Что должно быть доказано | Чего он ещё не разрешает |
| --- | --- | --- |
| B1b-local | Independent probe распознаёт точные Notes1/2 и Capabilities1/2, guards/receipts сохраняют strict v3; Windows WAL/negative/recovery suite и independent review | START нового production image, миграцию реальных файлов |
| B1b-image/bootstrap | Exact Linux helper и полный default-off application/fallback обслуживают старые/новые и смешанные форматы; согласованный restore выполнен; baseline реально serving | Native effect, OAuth/новые credentials, автоматическую migration при обычном startup |
| Последующее migration admission | Actual **SAME old container** уже имеет подходящий reader2; свежие guard, backup/restore и отдельный trusted migration run согласованы | Объявлять общий COMMIT двух файлов или завершённый B2 reconcile по одному номеру схемы |

Bridge commit `ae914f55e6d8d64628a7279189d2d55dfd05de45` поддерживает только Notes1/Capabilities1. Его probe SHA256 `d51b36c12e316e660fd2b3ab6d8fe2350833aff242f2ae95d296fb63409afd59`, guard `cd10d07f1f2b75e2f8c05ca469ae97f3bb7fb35ba76ebf085345eb99530f3a1c`. После появления хотя бы одного v2 этот bridge — неподходящий fallback независимо от выключенного endpoint.

B1a принят root после закрытия обоих validator findings и независимого source verdict. Итоговый root gate: **1064 tests / 1059 PASS / 0 FAIL / 5 explicit opt-in skip**, type/build PASS; это атрибуция root, не повторный запуск автора этого плана. DDL ниже не менялся. Принятый contract SHA256 `2ae6181faef1d38d3d4ddd7825c96398bd1a3d967e827abd21f4b21a1df0c273`; final source hashes ниже. При подготовке B1b fixtures provenance дополнительно закрепит итоговый B1a commit, когда root его создаст; промежуточные hashes/review findings не становятся release baseline.

## 2. Manifest, DTO и независимый probe

Image label `io.soty.storage.readers` полного reader2 baseline:

```json
{"version":3,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1,2],"capabilities":[1,2]}}
```

Version envelope остаётся **3**, ключи не расширяются. `soty.storage-format.v3` и `soty.storage-start.v3` теперь допускают `notes` и `capabilities` независимо `empty|1|2`. Все прежние строгие scalar/key/duplicate/prototype проверки сохраняются. Старые manifest/receipt v1/v2 по-прежнему отвергаются. Manifest v3 с честным `[1]` остаётся распознаваемым, но при actual2 получает `storage_reader_incompatible`; `[1,2]` не подразумевается из нового host code. Unknown Notes3/Capabilities3 запрещены, включая committed WAL. Rooms1–2 и Apps1–6 не расширяются.

Пути неизменны: `/data/notes/notes.sqlite`, `/data/capabilities/capabilities.sqlite`; trusted `project_id=soty` берётся из host composition. Сохраняются topology, exact actual image identity, один local Docker volume `/data`, NoCopy/RO helper, writer inventory, finite timeout, обычный SQLite read-only transaction. Ни current schemas, ни candidate imports, ни мигратор внутри probe не исполняются. До каждого START, RW recovery helper и включения restart policy требуется актуальный guard; прежний success receipt сам не разрешает повторный запуск.

Новые проверки дополняют существующие literal v1 projections, FTS5 definition/shadow tables/hidden columns и explicit indexes:

- В v2 разрешены только перечисленные ниже новые objects. Для двух новых tables проверяется полное известное SQL с PK/UNIQUE/FK/CHECK и `STRICT`, а также fixed projection; для четырёх indexes и 17 triggers — exact name, target table и нормализованный SQL. Наличие произвольного trigger не допускается.
- Notes v1 tables остаются non-STRICT; **только** новый `note_native_creates` — STRICT. Нельзя применить один общий strict-флаг ко всем Notes2 tables. Capabilities tables остаются STRICT. Notes FTS5 module/options и пять shadow tables прежние.
- Проверка metadata имеет точный набор ключей, а не только успешный поиск lineage: Notes1 `{lineage,project_id}`, Notes2 плюс `registry_id`; Caps1 `{lineage}`, Caps2 плюс `project_id,registry_id`. Registry ID — строка из 32 lowercase hex. Нет coercion, missing ID backfill или автоматической привязки Cap1 к проекту во время RO probe.
- Никаких registry IDs, proof/input/body/credential данных в обычном format/start receipt. Probe проверяет формат, не происхождение store, row/FTS integrity или бизнес-согласованность двух файлов.

Сохраняются отказ для unknown files/orphan sidecars/symlink/junction/nonregular/truncated main и отсутствие repair/checkpoint/FTS rebuild. Нормальный SQLite view учитывает committed WAL; raw main header не является версией актуального состояния. `immutable=1` fallback запрещён. WAL входит в persistent состояние, его нельзя отбрасывать при копировании; ошибки доступа к необходимым sidecars не обходятся чтением одного main. [SQLite WAL](https://www.sqlite.org/wal.html#the_wal_file)

## 3. Exact DDL2 inventory и fingerprints

Сверены literal `V2_DDL` из `modules/notes/server/schema-v2.mjs` и `modules/capabilities/server/schema-v2.mjs`, без исполнения DDL. Принятые final source SHA: Notes `2a9f8b51f408179a6bb5333da63b2fd9b2452313b15597f01165aeba66be6fda`; Caps `5ca8b4025535566c90a1b1d610e37837b4b41e6074117a96fe720504050af25f`. Перед release фиксируются принятый B1a commit и полный dependency graph отдельно; никакой hash из candidate не становится доверенным определением схемы.

| Store | user_version / lineage | Metadata / итоговый inventory без `sqlite_*` |
| --- | --- | --- |
| Notes2 | `2` / `soty.notes.sqlite.v2` | 3 metadata keys; 11 tables, включая FTS virtual/shadows, 2 explicit indexes, 6 triggers — **19 objects** |
| Capabilities2 | `2` / `soty.capabilities.sqlite.v2` | 3 metadata keys; 13 tables, 12 explicit indexes, 11 triggers — **36 objects** |

```text
note_native_creates(
  source_store_id,invocation_id,account_id,note_id,mutation_id,
  input_digest,capability_digest,revision,created_at)
PK(source_store_id,invocation_id)
UNIQUE(account_id,note_id), UNIQUE(account_id,mutation_id)
FK(account_id,note_id) -> notes(account_id,id)

cap_native_note_intents(
  invocation_id,account_id,notes_store_id,note_id,mutation_id,
  input_digest,input_bytes,started_at,input_purged_at)
PK(invocation_id)
UNIQUE(account_id,note_id), UNIQUE(account_id,mutation_id)
FK(invocation_id,account_id) -> cap_invocations(id,account_id)
```

Новые identity/digest CHECK не сводятся к column names: store IDs32hex, note/mutation IDs `n_`/`m_` +64hex, input digest64hex, native capability digest ровно `95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204`; Notes revision1, safe timestamps, Caps input_bytes1..262144. Остальной SQL берётся из согласованного [DDL contract](p4-native-storage-contract.md), не выводится заново из этих кратких описаний.

Literal fingerprints: извлечь содержимое `const V2_DDL` без backticks, CRLF заменить LF, сохранить все остальные bytes. Notes: **2458 bytes**, SHA256 `e15a1dc6d57dfe6c3de56e6d5052f42080955612f56b39a70f21fc8c93ff0add`; Caps: **5122 bytes**, `64c394ad739eba70bea95e2ffb18c29d2f0753b55c1b4373060620fde650a8c8`.

Таблица ниже — SHA256 UTF-8 SQL отдельного объекта после того же normalization, что frozen probe: single-quoted literals сохраняются; вне них удаляется whitespace/завершающий `;`, регистр приводится к lower case. Это review fingerprint. Implementation хранит собственный literal SQL map и проверяет его, не импортирует domain source и не доверяет заявленному hash.

| Notes object | Type / target | Нормализованный SHA256 |
| --- | --- | --- |
| `note_native_creates` | table | `5337bf4c76bcf60ddd8a3c49762eb6751f3481f8fbd8add362e0d79b57227549` |
| `note_native_create_no_update` | trigger / note_native_creates | `ddedd21f945b8cc6f54bcaed6088b630118e6553b181d9b5b99cfa7cd0da3d3b` |
| `note_native_create_no_delete` | trigger / note_native_creates | `e95990e35d9e51f559c31c10df3616d47be739512c997149f6b29bc7d3f42d65` |
| `note_native_create_no_replace` | trigger / note_native_creates | `a0a34b61d8671dbd0019790a447e1d7e25eb45e3f72d80f0894c204c26179502` |
| `notes_identity_no_update` | trigger / notes_meta | `fed6868bfbc0089c92de7d1b3e6addddbfeef4c9505e980bd093b987dc35fdd2` |
| `notes_identity_no_delete` | trigger / notes_meta | `7b349ee5606a2d72aa00f0377a9aa2c2a5dc77b90a35a8b55f5a95c6e823e505` |
| `notes_identity_no_replace` | trigger / notes_meta | `3323f02105e2a4b1d0e42a249d9a9f851e9a268f5754a5d7fd3ba0d7bc89fa23` |

| Capabilities object | Type / target | Нормализованный SHA256 |
| --- | --- | --- |
| `cap_invocations_native_identity` | UNIQUE index / cap_invocations(id,account_id) | `42e75af0059d8c346aa5fc3fc95fe24c301c06a7b6379562739de12b661d7583` |
| `cap_invocations_account_admission` | index / cap_invocations(account_id,created_at,id) | `a7e5c731afbe213091ab584c360ebf1b27f82a30e15168af9033efe1b50c5416` |
| `cap_invocations_principal_admission` | index / cap_invocations(account_id,principal_id,created_at,id) | `25c7b4b2737cd99ad5be4922370e40dee4bc793011de36253fbfa7be2b852bd8` |
| `cap_invocations_nonterminal` | partial index / те же columns; status NOT IN succeeded/failed/cancelled | `605527305ca252385229f2c2b251a7d6d9ef5845f34419ee45ef8cf9c8903e30` |
| `cap_native_note_intents` | table | `7d74b3ee40162a56c5b4662fefe77c8b29f5ea972f7a971179ec9c1764c4b9ad` |
| `cap_native_note_admission` | trigger / cap_native_note_intents | `2d5e631085e679e870d67759a9cbc91446e3d69f71c5d1d2c55eff2c5b827016` |
| `cap_native_note_no_replace` | trigger / cap_native_note_intents | `537a968071f56a57b0695032dfcd976a996b9ee11f408a5013a9608c92d3f788` |
| `cap_native_note_no_delete` | trigger / cap_native_note_intents | `9c55468d9dfd8d5fdb567364f637ef62e1c0714e07272394ae3282449eb466a7` |
| `cap_native_note_update_guard` | trigger / cap_native_note_intents | `00eec0daf2abcfc46cc4070417eea5934faa81667504eae819e678c25929154d` |
| `cap_native_note_input_guard` | trigger / cap_invocations | `e652594686e93e9699a7e88208b9318c5631a4c64b907434c702add0fd257803` |
| `cap_native_receipt_no_update` | trigger / cap_receipts | `6f12032081910c31f96cf22fcd201d03f5d4c1b21591be75274acc4b8983ab93` |
| `cap_native_receipt_no_delete` | trigger / cap_receipts | `320b57e4ddb7b3732c3019703797518b803e6ae9506580d878eb94b1c4ebf0e8` |
| `cap_native_receipt_no_replace` | trigger / cap_receipts | `0af80ac806fda16a6860276568e44d8a51cb6d29edd01f70de9cb7ee12c7730b` |
| `cap_identity_no_update` | trigger / cap_metadata | `6b9f89ddd2046dffcd801485c50d9065ee7ca1d2dcbed477c54d87962bb9f5ab` |
| `cap_identity_no_delete` | trigger / cap_metadata | `e4251aba76f16ee991edffef829c073bc106d0d968809adf1dc25daf18bba92a` |
| `cap_identity_no_replace` | trigger / cap_metadata | `4cd4c11ca71d1711159db59f7870c0c218ae7e13af3a9a5765190e499871236f` |

## 4. Полный application baseline и mixed bootstrap

Фактический конструкторный switch обоих stores — `allowNativeMigration:false` по умолчанию, строгий boolean. Сейчас `server/http-app.js` передаёт Notes/Caps `projectId:'soty'`, но **не передаёт** этот migration option. План не вводит имя environment flag/RPC, которого нет в code.

| Notes / Caps actual | Default-off baseline | Разрешение нового native effect |
| --- | --- | --- |
| empty / empty | Создаёт v1 / v1 | Нет |
| 1 / 1 | Читает/обслуживает прежние доменные данные без перехода на2 | Нет |
| 2 / 1 или 1 / 2 | Читает каждый точный формат независимо; существующий ID2 неизменён | Нет до обоих2 и отдельного B2 admission |
| 2 / 2 | Читает exact2 и сохранённые proof/receipts; не downgrade | Нет только по этому признаку |
| unknown/partial/wrong-project/missing-ID2 | Отказ; не создавать заново и не продолжать по main-only | Нет |

Отдельные missing stores также допустимы: их обычное создание даёт1 при default-off. Полная matrix probe — девять сочетаний `empty|1|2` каждого store. Пустой SQLite-файл с неподходящим layout не приравнивается к отсутствующему store.

Успешная explicit migration атомарна **внутри одного** store. Registry ID создаётся в его transaction, v1 rows не переписываются, новые native tables пусты; Cap1 project binding появляется впервые из trusted configuration. После crash между двумя COMMIT допустим mixed state; завершить второй переход можно только отдельным разрешённым migration run, а не автоматическим repair при startup.

Baseline должен не только открыть schema2, но и сохранить native invariants при обычной работе: generic dispatcher не берёт native intent; generic cancellation/settlement не высвобождает reservation и не объявляет отсутствие эффекта; input/proof сохраняются, terminal receipt читается по ACL. Если B2 reconciler отсутствует, unknown остаётся удержанным. Это допустимый безопасный fallback с временно недоступным native completion, не восстановленный native сервис.

Gate проверяет **полный application image**, а не сборку одного migrator: Notes ordinary create/read/edit/search, исторические Cap receipts/budgets, default-off restart/reopen, safe-hold seeded native pending/terminal, отказ unknown. Версии и ID сравниваются до/после, без выгрузки содержимого. World/Connect и возможные прежние Rooms/Apps migrations проверяются отдельно: default-off Notes/Caps не обещает, что первый startup всего image не изменит другой store. Непроверенный migration другого store блокирует bootstrap.

## 5. Первый unlabelled serving image: допустимый bootstrap

Fresh root read-only inventory от30.09.2026 подтвердил прежний running container/image/StartedAt и отсутствие actual reader label; это не serving reader2. Точный safe inventory хранится в `output/implementation-20260930/p4-linux-readonly-preflight.json`; перед действиями проверяется заново. Имеющийся full image/build или label в будущей ветке не меняет бинарник старого container.

Штатные `rollout.mjs`/`host-controller.mjs` уже проверяют actual old image до CREATE/STOP и повторно guard перед STOP. Их **нельзя ослаблять** для первого перехода. При unlabelled old обычный rollout завершается до serving mutations. Оба пути возвращают SAME old container с последними файлами; отдельный fallback image пока не является его автоматической заменой.

Минимальный предлагаемый bootstrap — отдельная ещё не реализованная, явно рассматриваемая maintenance/recovery transaction. Её нельзя спрятать во флаг `skipGuard` обычного rollout:

1. До STOP закрепить exact original identity/config/mount, quiescent controller journals и отдельный single-writer inventory. Подготовить полный default-off reader2 candidate **C** и проверенную recovery конфигурацию **R**, включая actual image labels, ordinary service health и все format/read tests. C и R могут использовать один и тот же проверенный binary, но тогда это recovery к известному baseline, не независимость от общего дефекта этого binary. Путь R обязан работать при неуспешном C и иметь собственную проверенную конфигурацию/допуск, а не только имя image.
2. До STOP выполнить rehearsal этого bootstrap и encrypted restore на synthetic/согласованной изолированной копии. Согласовать maintenance window, предел действий, safe configuration transfer и исход при невозможности запустить C/R. Backup mechanism/доступ к ключу уже проверены; свежий согласованный cold backup реальных файлов снимается после остановки всех writers и **до первого START C**.
3. Старый unlabelled container — только outgoing **stop-only** объект. Его идентичность разрешает выключение автоматического restart и один journaled STOP, но никогда последующий START, возврат restart policy или RW recovery helper. Не добавлять/копировать ему новый label; не объявлять его совместимым из-за v1 на диске. Точное выполнение STOP и отсутствие второго writer проверяются перед backup/новым START.
4. Перед любым START C/R и enable-restart применяется обычный strict v3 guard к actual image и актуальному volume. Ошибка C ведёт к отдельно проверенному R на последнем состоянии либо к maintenance/fail-stop. Возврат старого unlabelled container не входит в plan. Ambiguous mutation сначала reconcile по exact ID; повторный START/CREATE/STOP по одному timeout запрещён.
5. После ordinary health, read/write и version comparison C становится записанным serving baseline. Notes/Caps должны остаться1 либо прежним mixed/2, если такие входные данные были отдельно допущены; registry IDs не меняются. Никакого native admission. Только теперь последующий стандартный release может иметь настоящий SAME old reader2 C.

Это **предлагаемый release contract**, не уже существующая функция controller и не разрешение production действий. Root должен отдельно согласовать/реализовать/review exact bootstrap script и его аварийные ветви. Пока этого или другого эквивалентного доказанного recovery route нет, первый production переход заблокирован. Локальные B1b source/tests можно завершить независимо.

## 6. Backup, restore и более поздняя migration2

Перед первой реальной migration2 нужен свежий согласованный cold snapshot: остановлены ingress/dispatch и все writers; отсутствуют unresolved host mutations; архивируется **одна generation** всего необходимого состояния. Inventory включает Connect `connect/accounts.sqlite`, World `world/world.sqlite`, Notes, Capabilities, Apps, Rooms и нужное connector/controller/config/key состояние. Конфигурация с секретами — только в зашифрованном backup; в receipt лишь безопасные paths, hashes, versions и идентификатор generation.

SQLite online backup создаёт snapshot одной database. Вывод для нашей архитектуры: независимые живые backups Notes и Caps не доказывают единую межфайловую точку; здесь требуется общее quiescence/согласованный filesystem snapshot. Это наше release требование, а не обещание cross-file atomicity SQLite. [SQLite Backup API](https://www.sqlite.org/backup.html)

Хранить полный корректный набор main/WAL и нужных sidecars; не удалять WAL для уменьшения архива. SHM не считается неизменяемым источником доменных данных. Архив вместе с release/config provenance восстанавливается в **новый isolated volume**, никогда поверх работающего. После расшифровки действительно запустить exact C/R default-off с закрытым внешним dispatch/network; проверить domain integrity/FK/FTS, retained receipts/budgets и native hold, затем повторный cold restart. Проверка hash зашифрованного файла без restore/start не закрывает этот gate.

Store IDs2 подтверждают identity отдельных файлов, но две разновременные копии того же store сохраняют один ID. Нельзя признать пару согласованной только по ID/schema2 или отдельно вернуть Notes из старого backup при новых Caps receipts. Такой restore может потерять ACKed данные/эффект и изменить authority. Восстановление старой общей generation после новых writes требует отдельного решения об RPO/потере данных; это не автоматический rollback кода.

Перед разрешённой migration2 ещё раз проверить actual SAME old reader2 image/container, свежий guard и актуальную backup generation. После миграции проверить 2/2, прежние IDs, ordinary domains, native hold/reconciliation по отдельному B2 gate. Failure после первого COMMIT оставляет supported mixed state; не снижать user_version, не удалять таблицы и не стартовать reader1. Откат кода к C сохраняет latest files; завершение/отмена native операций требует compatible proof-first recovery, не очистки reservation.

## 7. Проверки B1b: сначала Windows, затем отдельный Linux/image gate

Последовательные bounded suites под `var/toolchains/node-v24.21.0-win-x64/node.exe`, PATH тот же у workers, `--test-concurrency=1`; test slot согласовать с root. Ниже — требуемые проверки, **не выполненные результаты этого документа**.

| Проверка | Существенное доказательство |
| --- | --- |
| Historical1 | Literal fixtures и exact old migrators из `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e` сохраняются; не заменять historical1 нынешним migrator с marker1 |
| Literal2 | Новые independent literal additions + v1 base; сравнить все19/36 SQL objects с окончательно принятым B1a. Закрепить commit/source/DDL SHA, populated synthetic proof/ledger states явно назвать fixture, не native execution |
| Смешанные пары | Все9 empty/1/2, ordinary default-off service reopen для1/2,2/1; один unknown3 отвергает весь START |
| WAL и отказ старого reader | Настоящий v1 main + committed v2 WAL отдельно Notes/Caps, mixed2/1 и1/2; frozen bridge probe из ae914f5 отказывает, новый распознаёт2; old image `[1]` нельзя START/RW-helper/restart. Main/WAL bytes до/после normal RO; SHM не байтовая гарантия |
| Exact objects | Отсутствующий/изменённый каждый guard, неверный target table, extra view/trigger/index, потерянный UNIQUE/FK/CHECK/STRICT новой table, altered partial predicate, metadata set/ID/project/type; отказ без repair |
| Filesystem/corruption | Унаследованные orphan/symlink/junction/foreign files, truncated main, unreadable sidecars, corrupt/future committed WAL. Не обещать обнаружение каждого незакоммиченного повреждённого хвоста, который SQLite законно игнорирует |
| Fresh recovery | После старого success receipt изменить actual format в SQLite, затем START/restart/recovery обязаны заново проверить; actual image label, copied container label, unknown3 и first-unlabelled отказ **до STOP** |
| Application | Полный default-off baseline fresh1/1, historical1/1, mixed и2/2; retained data/FTS/receipts, IDs, safe-hold; malformed/unknown refusal. Domain-owned B1a tests дают часть evidence, не заменяют полный image |
| Регрессия | Rooms/Apps/topology/manifest/start/controller прежние tests без ослабления. Independent acceptance отдельного автора; общий deploy suite один раз после freeze и согласования слота |

Устаревший migrator выполняет persistent pragmas до refusal: B1a уже различает held-WAL byte-preserving отказ и DELETE-mode изменение journal mode. Error code unsupported сам по себе не означает unchanged disk. Главный release барьер — RO host guard **до** запуска этого старого приложения.

Замороженный `p4-storage-linux-canary.review2.bundle.mjs` SHA `e2853c4882de7520949b3de9392a6c5037022e8aa18c35f0ce458d7f312c3927` проверяет **bridge Notes1/Caps1**, Apps6 и отдельные unknown2. Он не изменяется и не переименовывается в reader2 proof. Его root review/execution ведётся в [отдельном журнале](p4-storage-linux-canary-result.md); даже успешный результат не закрывает B1b Linux2.

После B1b freeze потребуется **новый отдельно reviewed immutable canary artifact**: exact reader2 helper + literal2 synthetic data, genuine main1/WAL2, mixed pairs, unknown3, malformed guards, foreign-owner0600/RO mount/symlink; main/WAL audit и OOMKilled=false. Сохранить no-copy/effective-env/identity/one-mutation journal/byte budgets и reconciliation без повторного запуска старых bundles. Его программа/объёмы заново измеряются; прежние counts/limits не копируются как якобы доказанные после роста DDL.

Exact Linux runtime/FTS и actual full application/fallback image проверяются исполнением. Docker base metadata Node24.15.0 и локальный Windows Node24.21.0/SQLite3.53.4 не доказывают Linux reader2/WAL поведение. Изолированный canary не касается serving volume; холодный restore, bootstrap serving и последующее migration admission — следующие самостоятельные gates с отдельным разрешением.

## 8. Предлагаемый узкий ownership после отдельного разрешения

Storage author:

```text
EXISTING source:
  deploy/connector/storage-probe.mjs
  deploy/connector/storage-guard.mjs
EXISTING author tests:
  deploy/connector/storage-guard.test.mjs
  deploy/connector/storage-notes-capabilities.test.mjs
NEW author tests/fixtures:
  deploy/connector/storage-native-v2.test.mjs
  deploy/connector/notes-v2.fixture.mjs
  deploy/connector/capabilities-v2.fixture.mjs
  deploy/connector/fixtures/notes-v2/provenance.json
  deploy/connector/fixtures/capabilities-v2/provenance.json
DOCS:
  deploy/connector/README.md
  docs/implementation/p4-reader2-rollout-plan.md
  docs/implementation/p4-reader2-implementation.md
```

Literal2 fixtures могут read-only импортировать frozen literal1 base; не импортируют current domain schema. Provenance фиксирует accepted B1a pin и SQL hashes. Существующие v1 fixtures/provenance и independent acceptance не принадлежат новому author scope. В старом author suite отделить historical bridge2-refusal от current reader2; запрещено массовое переименование «future2» в3 без сохранения исходного barrier assertion на frozen bridge.

Root: `Dockerfile` exact label, full image/runtime/HTTP composition, обе orchestration зоны и их tests, при необходимости envelope fixtures в existing Apps tests; отдельные bootstrap/migration admission/backup/restore/Linux canary gates. Domain author: B1a DDL/validators/safe-hold и final refreeze; позднее B2 только по отдельному плану. Independent reviewer: новый отдельный reader2 acceptance/source review. Новые подагенты не требуются. Конкретный Linux2 script scope выдаётся отдельно; прежний canary/source/bundle не входят в этот ownership.

## 9. Критерий завершения подплана

Локальный B1b можно принять при exact DDL freeze, новых independent readers/tests, сохранённом старом barrier и review. В итоговом receipt отдельно перечислить ещё открытые внешние пункты: Linux2 execution, actual application/fallback, first-unlabelled bootstrap, encrypted whole-generation restore и explicit migration admission. До их выполнения нельзя писать «production готов» или «автоматический откат обеспечен».

В рамках этой подготовки проверены source/DDL fingerprints, текущие constructors, manifest/probe/rollout/restore branches и первичные SQLite references. Создан только этот документ. Source bridge и review2 bundle повторно сверены по SHA и не изменены; никаких данных, runtime, remote или системных настроек эта работа не меняла.
