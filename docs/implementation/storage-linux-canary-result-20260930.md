# Linux room-format canary: PASS 30.09.2026

**Linux Docker read-only WAL/SHM gate для закреплённого rooms probe пройден на третьей отдельно разрешённой попытке.** Полное подтверждение приведено ниже; первые два отказа сохранены как история. Все созданные во всех попытках container/volume удалены, отсутствие подтверждено, safe журналы сохранены. Рабочий контейнер не перезапускался. Это не Apps reader proof, не проверка восстановления backup и не production rollout.

Исходные canary scripts, tests, bundles и полная история evidence сохранены в `C:\Users\Junio\.codex\worktrees\soty-experience-release\соты\output\implementation-20260930`. В новый `soty-platform` перенесены этот отчёт, план, финальный review3 receipt и независимый post-cleanup readback с теми же hashes. Старый rooms probe не переименован в доказательство нового Apps probe.

## Идентичность попытки

- Bundle: `output/implementation-20260930/storage-linux-canary.review.bundle.mjs`, 52 074 bytes.
- SHA-256 перед передачей в stdin повторно проверен: `1b709a16ef66a8d55ce06ff02b36466d7a13ae85d035a6233900b898183fb715`.
- Документированный `ssh dev`, существующий host Node; без установки/pull/build/tag.
- Host run: `2026-09-30T02:27:42.605Z`–`02:27:43.046Z`, 441 ms, SSH exit1, полный JSON receipt получен, stderr0 bytes.
- Run ID: `1170cf4d2502dec56feaf7ad5d41386f`.
- Созданный container: `1bbd97d04ee9fc9902942b94f10f9892a48867e5299855b93b10183b5685f370`.
- Созданный volume: `soty-room-canary-1170cf4d2502dec56feaf7ad5d41386f`.
- Docker mutations самой попытки: volume CREATE1, container CREATE1, START0, STOP0, DELETE0. Контейнер преднамеренно удержан для проверки несоответствия; никакая фаза writer/probe/audit не запускалась.

## Подтверждённая причина отказа

Read-only reconcile в `02:28:57.061Z` подтвердил exact task label, container name/image, статус `created`, `Running=false`, `StartedAt=0001-01-01T00:00:00Z`, `RestartPolicy=no`, один mount только созданного тома, `NetworkMode=none`, отсутствие port bindings. Volume был `local`/`local`, без driver options; hash mountpoint совпал с исходным journal.

Docker вернул в `HostConfig.Mounts[0]` только ключи `Source, Target, Type, VolumeOptions`. `ReadOnly:false` отсутствовал; `VolumeOptions.NoCopy=true`, Subpath отсутствовал. При этом фактический `Mounts[0].RW=true` был задан и соответствовал запросу writer. Исходная проверка требовала буквальное false в декларативном поле и безопасно отказала до START.

Локальное исправление нормализует **только отсутствующее `HostConfig.Mounts[].ReadOnly` в false**. Реальный `Mounts[].RW`, exact source/name/target, subpath, NoCopy и остальные ограничения остаются строгими. `null`, число и строка не принимаются вместо boolean.

## Точная очистка и сохранение evidence

Очистка отдельно разрешена интегратором только для указанных двух synthetic ресурсов. Перед удалением повторно сверены journal, labels/image/ID, статус created/never-started и исходный serving baseline.

1. В `02:31:31.859Z`–`02:31:32.863Z` отправлен ровно один `DELETE` exact container с `force=false&v=false`; последующий inspect подтвердил404. Проверка списка всех containers превысила заданный response cap256KiB, поэтому до volume DELETE сценарий остановился с `cleanup_response_limit`. Повторного container DELETE не было.
2. В `02:33:27.549Z`–`02:33:27.623Z` отдельное продолжение прочитало оба журнала, вновь подтвердило container404 и принадлежность exact volume. Ограниченный Docker запрос с filter `volume=<exactName>` вернул0 attached containers. Отправлен один volume DELETE с `force=false`; inspect подтвердил404.
3. Финальный receipt: `containerAbsent=true`, `volumeAbsent=true`, `servingUnchanged=true`, `ok=true`. START/STOP при очистке не выполнялись. Нет prune, cascade volume deletion или удаления чужих ресурсов.

Serving baseline до/после: container `d86bc0b9b8f88a7693a227cca0b15864ac5388a8c6e478f25e0f1c3c7d9c4ceb`, image `sha256:d07345cb66b2c1ab903861f36902d7b301cc25289778d0d178c81143281ca97e`, `running=true`, неизменный StartedAt `2026-09-28T01:15:45.442670947Z`. Производственные volume/config/controller/timer/network не менялись; пользовательские данные и секреты не выводились.

Safe local evidence в `output/implementation-20260930/`:

- `storage-linux-canary-20260930-attempt.json` и `storage-linux-canary-20260930-receipt.json` — исходная попытка.
- `storage-linux-canary-20260930-reconcile-read.json` — выбранные безопасные поля Docker и journal.
- `storage-linux-canary-20260930-cleanup-receipt.json` — подтверждённый container404 и остановка до volume DELETE.
- `storage-linux-canary-20260930-volume-cleanup-receipt.json` — финальные оба404, attachments0, serving unchanged.
- `storage-linux-canary-cleanup-1170cf4d.mjs` и `storage-linux-canary-cleanup-volume-1170cf4d.mjs` — точные использованные cleanup scripts. Повторно не запускать; single-shot intents сохранены.

Remote private directory `/tmp/soty-room-canary-1170cf4d2502dec56feaf7ad5d41386f/` сохранён с `receipt.json`, `cleanup-receipt.json`, `cleanup-volume-receipt.json`. В нём только служебные идентификаторы/коды/числа/hashes, без application config и пользовательских данных.

## Вторая отдельно разрешённая попытка

`storage-linux-canary.review2.bundle.mjs`, 52 221 bytes, SHA-256 `c21958ee370fbca5e6f765b599579e4a2e6bc73151c58f4e2c5a34121e511e88` повторно проверен перед единственной передачей через документированный SSH. Host run `02:37:59.666Z`–`02:38:00.835Z`, 1168ms. Run ID `6104170204afe8007e7246c539a701f5`, synthetic container `975ad13cf16519b5eabeb6b36fc7f4052952ed23c37c6917c73dd00bcd167987`.

Mount/config gate пройден; writer начал работу, затем завершился с **ExitCode13**. Outer bundle вернул exit1/`container_exit_unexpected`; это разные коды. Другие phases не запускались. Автоматическая cleanup: container CREATE1/START1/DELETE1, volume CREATE1/DELETE1, STOP0. Полный receipt получен; `cleanupComplete=true`, `needsReconciliation=false`, `servingUnchanged=true`. Stderr writer эта версия не сохраняла до удаления, поэтому точное сообщение той ошибки утрачено.

Независимый read-only readback в `02:48:34.527Z` подтвердил container404, volume404,0 containers по exact task label и неизменный serving baseline. Evidence: `storage-linux-canary-review2-20260930-receipt.json`, `storage-linux-canary-review2-post-cleanup.json` в прежнем output каталоге. Remote journal `/tmp/soty-room-canary-6104170204afe8007e7246c539a701f5/receipt.json` сохранён.

По [официальной документации Node](https://nodejs.org/api/process.html#exit-codes), код13 соответствует незавершённому top-level await. [Linux man-pages](https://www.man7.org/linux/man-pages/man7/pid_namespaces.7.html) описывает специальные ограничения сигналов namespace PID1 и принудительную доставку SIGKILL из предкового namespace. Исходный writer вызывал self-SIGKILL, затем `await new Promise(()=>{})`; это **согласованная с фактами гипотеза**, не восстановленный stderr той попытки. [Docker](https://docs.docker.com/reference/cli/docker/container/run/) по умолчанию использует PID namespace; canary явно не запускал init helper.

Локальная проверка того же generated writer и точного DDL с единственной заменой filename на отдельный synthetic temp показала: mainHeaderVersion0, SQLiteuserVersion2, WAL111272 bytes, SHM32768 bytes, main4096 bytes, rowCount1, markerMatches=true. Windows self-kill дал code1, не13. Это **Windows Node24.13.1**, а удалённый runtime — Linux Node24.15.0; проверка подтверждает пригодность DDL/fixture, но не воспроизводит Linux PID1 semantics. Safe evidence: `storage-linux-canary-review2-local-writer-receipt.json`; raw stderr/source/marker не сохранены в нём.

## Исполненный review3 artifact

`output/implementation-20260930/storage-linux-canary.review3.bundle.mjs`, 58 322 bytes, SHA-256 `38beaaa9995a1df5d49fa033e5dc0d6795c2d1f8e316f297bf272b35f1994247`. Probe/guard/DDL payloads не менялись, core guards frozen. Изменения только synthetic canary: внешний единственный SIGKILL после строгого ready вместо self-kill; durable intent и read-only reconcile при потерянном ответе; классификация bounded stderr передcleanup без raw paths/source/SQL/marker.

`node --test output/implementation-20260930/storage-linux-canary.test.mjs`: **14/14 PASS**, независимо повторены интегратором до допуска. Без Docker/SSH: 13 mock/contract scenarios и реальный локальный generated writer, удерживаемый после ready до внешнего прекращения, затем generated read-only audit. Audit подтвердил mainHeader≠2, SQLiteversion2 и markerHash. Синтаксис bundle проверен. Эти локальные tests сами по себе не являются Linux/Docker proof; отдельный remote результат ниже.

## Подтверждённый Linux результат review3

Bundle hash повторно сверён непосредственно перед stdin передачей. Одна remote execution, host interval `2026-09-30T02:53:16.009Z`–`02:53:24.406Z`, **8397ms**, SSH exit0, полное JSON receipt, stderr0 bytes. Run ID `71c225abd642f22013eb6ca7cdf3b62b`. Exact прежний image/Node24.15.0;8 task containers,7 starts,2 host SIGKILL,0 STOP,8 container DELETE и1 volume DELETE. Все API mutations относятся только к новым task resources. Ни install/pull/build, ни application start не выполнялись.

| Фаза | Наблюдаемый результат |
| --- | --- |
| writer-valid | Строгий ready после COMMIT/FULL; один внешний KILL; exit137 |
| probe-valid | RO mount; exit0, `soty.storage-format.v1`, rooms2 |
| audit-wal | RO mount; Node24.15.0, mainHeaderVersion0, SQLiteuserVersion2, markerMatches=true, rowCount1 |
| fixture size | main4096, WAL111272, SHM32768, всего148136 bytes |
| writer-unknown | user_version3, ready и один внешний KILL; exit137 |
| probe-unknown | exit1, `storage_format_unknown` |
| writer-corrupt | Только synthetic SQLite checkpointed/заменён; exit0 |
| probe-corrupt | exit1, `storage_format_unreadable` |
| unknown-reader | Real image inspect: copied container label не авторизовал old unlabelled image; `storage_reader_unknown`, START не отправлен |

Final receipt: `ok=true`, `cleanupComplete=true`, `needsReconciliation=false`, `servingUnchanged=true`. Независимый read-only readback в `02:54:25.986Z`: все8 exact container IDs вернули404, volume404,0 containers по exact task label; serving ID/image/running/StartedAt остались равны исходному baseline. Его StartedAt по-прежнему `2026-09-28T01:15:45.442670947Z`.

Safe evidence в `output/implementation-20260930/`:

- `storage-linux-canary-review3-20260930-attempt.json` — single execution intent.
- `storage-linux-canary-review3-20260930-receipt.json` — полный safe результат; SHA-256 `94bf2afd4b2a9d692aaf2f787c78fddea6cef3fe9344523f85a65e200d37288d`.
- `storage-linux-canary-review3-post-cleanup.json` — независимые404/taskcount0/baseline; SHA-256 `02150595f068aa81a4903a0cdb2accac7ae78fcf7497ab4622b164e6aef46e8d`.
- Сохранённый private remote journal: `/tmp/soty-room-canary-71c225abd642f22013eb6ca7cdf3b62b/receipt.json`.

Проверены только exact pinned rooms probe/guard sources, synthetic WAL и заявленные Docker ограничения на этом engine. Не доказаны: полнота/целостность production данных, future compatible application/rollback image, fresh encrypted backup/isolated restore, Apps reader или registry migration. Форматный probe не заменяет эти отдельные release gates. Core guard files не менялись этой проверкой.
