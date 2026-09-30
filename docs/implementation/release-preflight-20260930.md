# Проверка реального выпуска и P3 — 30.09.2026

Статус: **чтение production выполнено; новый выпуск не запускался**. Наблюдения сделаны 30 сентября 2026, приблизительно 01:07–01:16 UTC. Они не заменяют повторную проверку непосредственно перед переключением. Файлы данных, настройки, DNS, сертификаты, контейнеры, timer и журналы production не изменялись; новые backup или открытые копии данных не создавались. Закрытые значения разбирались внутри процесса, наружу выходили только выбранные поля, числа и хеши.

## 1. Что действительно доступно

Документированный транспорт `ssh dev` работает с `BatchMode=yes`, `StrictHostKeyChecking=yes`, без добавления нового host key. Настроены пользователь `ai2`, адрес `95.105.28.201`, порт 22. Docker доступен через эту сессию. Прямой `statvfs` каталога Docker volume от пользователя ai2 отказал в доступе; необходимые агрегаты затем прочитаны внутри **точно установленного** работающего контейнера. Повышение прав, новый helper-контейнер или обход сетевого доступа для этого не потребовались.

| Факт | Текущее подтверждение |
| --- | --- |
| Репозиторий | `https://github.com/junior2wnw/soty.git` |
| Рабочая ветка интеграции | `codex/human-agent-platform-20260930`; HEAD на начало проверки `36116cb`, локальный P2 checkpoint затем `a307f98`; deploy guard ещё отдельный непубликованный набор изменений |
| Рабочий host | `dev`, Linux x86_64, kernel `6.8.0-139-generic` |
| Serving container | `d86bc0b9b8f88a7693a227cca0b15864ac5388a8c6e478f25e0f1c3c7d9c4ceb` / `soty-online-chat` |
| Serving image | `sha256:d07345cb66b2c1ab903861f36902d7b301cc25289778d0d178c81143281ca97e` |
| Serving revision | `24c2da22d48b89d295da52b100ef067c7b61ef63` |
| Node в приложении и controller | `v24.15.0` |
| Docker | `29.3.0`, API `1.54`, minimum API `1.40` |
| Caddy | `v2.11.2`, admin API выключен |
| Данные | Единственный RW mount работающих контейнеров для `soty-online-chat-data` принадлежит указанному serving container |
| Автообновление | Timer enabled / active / waiting; service inactive / dead после успешного завершения, `ExecMainStatus=0` |
| Runtime для устройств, опубликованный manifest | `1.3.0`; это фактическая публичная версия, не локальный будущий `1.3.1` |

У Docker-контейнера нет собственного поля healthcheck status. Это не было заменено догадкой о здоровье: отдельно проверены настоящие HTTP readiness endpoints.

Дополнительное безопасное чтение 30.09.2026 до 01:56 UTC подтвердило точный serving container и профиль его `/data`: один RW named volume, `Driver=local`, `Scope=local`, пустые `Options`, `Source` равен `Mountpoint` volume, явный `DATA_DIR=/data`, непустого `Subpath` нет. У закреплённого serving image пусты runtime hooks `NODE_OPTIONS`, `NODE_PATH`, `LD_PRELOAD`, `LD_LIBRARY_PATH`, `ENV`, `BASH_ENV`; значения остальных image defaults не выводились. Это проверка выбранного тома и образа, а не доказательство отсутствия любого внешнего host/remote writer.

На операторском Windows `docker` отсутствует. WSL `Ubuntu-24.04` доступен, но внутри нет `docker`, Node и `/var/run/docker.sock`. Установка движка/пакетов не выполнялась. Дополнение02:54UTC: отдельный разрешённый synthetic canary через существующий Docker на `dev` прошёл Linux RO WAL/SHM gate для rooms; [точные границы и receipt](storage-linux-canary-result-20260930.md). Serving resources/config не менялись; все созданные тестовые ресурсы удалены.

## 2. Реальный путь выпуска

Источник — [Connect deployment README](../../deploy/connect/README.md), [host controller](../../deploy/connect/CONTROLLER.md), [квитанция 28.09](../../deploy/connect/DEPLOYMENT-20260928.md). Шаблон `deploy/timeweb/compose.yml` **не является** текущим способом выкладки: прежний Timeweb узел был превращён в forwarding path, его старую копию данных нельзя запускать писателем.

Подтверждены выбранные поля активной конфигурации:

```text
configFile   /home/ai2/.local/share/soty-connect/config-24c2da2.json
sourceRoot   /home/ai2/.local/share/soty-connect/source-85f36a1
stateDir     /home/ai2/.local/share/soty-connect/state-24c2da2
revision     24c2da22d48b89d295da52b100ef067c7b61ef63
healthOrigin http://127.0.0.1:18182
releaseDir   /home/ai2/.local/share/soty-connect/releases
source       https://xn--n1afe0b.online/releases/connect/stable.json
channel      stable
```

Серверный Git HEAD совпадает с закреплённым revision, checkout detached. 56 изменённых путей находятся только внутри `modules/connect`, как и предполагает подписанный модульный updater; изменений host source вне Connect нет. Host journal указывает на фактический image/container. Connect `0.1.2`, sequence `2`; `transaction=null`, module `pending=null`, host/module locks отсутствуют, pause marker отсутствует.

Штатная новая whole-app generation требует независимого clean checkout проверенного commit, сохранения установленного Connect baseline и anti-rollback sequence, `rebase-host.mjs`, нового подписанного module tree с большим sequence, проверки Linux image/canary и выбора нового controller/config. Прямой `git pull` в действующий pinned source или произвольный `docker compose up` обходят этот контракт.

Подписанный artifact необходимо формировать из LF Git bytes. Квитанция 28.09 уже содержит реальный отказ CRLF artifact; повторять его нельзя. Приватный ключ публикации остаётся в DPAPI, не в аргументах, env или файле publish config.

## 3. DNS, TLS и граница приложения

Настроенные публичные origins оболочки:

- `https://xn--n1afe0b.online` — `соты.online`;
- `https://soty.pochinit.online`.

Текущий runtime template: `https://{appId}.soty.pochinit.online`. В выбранных Soty-полях container env, controller config и связанных host blocks Caddy **отдельной runtime registrable zone не найдено**. Проверка не перечисляла чужие сайты, admin zones или аккаунты регистратора; не найденное здесь не означает отсутствия у владельца других доменов.

Windows DNS и оба authoritative server `ns1.reg.ru`, `ns2.reg.ru` подтвердили `95.105.28.201` для основного домена и тестового имени внутри wildcard `*.soty.pochinit.online`. Обычный resolver также подтвердил этот адрес для второго shell domain и `pochinit.online`; AAAA для проверенных имён не получен. DNS-запрос тестового имени не сопровождался TLS handshake или выпуском сертификата.

Для обоих действующих shell origins выполнена штатная проверка доверия TLS 1.3. Сертификаты содержат соответствующий точный SAN, сроки окончания — 24.11.2026 и 09.12.2026 соответственно. `/`, `/health`, `/ready`, `/api/connectors/storage-ready`, `/api/apps/capabilities` возвращают 200; `storageReady=true`, `maintenance=false`, Apps `configured=true`. `/ready` дополнительно проверен с операторского Windows-компьютера, а не только с сервера. TLS allow неизвестного app host возвращает 403 на обоих origins. Новые приложения/сессии/сертификаты для проверки не создавались.

**P3 blocker:** runtime и второй shell origin различаются по origin, но относятся к одному site `https://pochinit.online`. Прочитанный текущий PSL содержит правило `online`, не содержит `pochinit.online` или `soty.pochinit.online` как public suffix. Следствие по [WHATWG HTML](https://html.spec.whatwg.org/multipage/browsers.html#sites): поддомены приложения и второго shell имеют общий registrable domain. Отдельный origin, sandbox и точные Origin checks полезны, но не превращают их в разные sites.

Варианты, требующие отдельного решения до public admission:

1. Предпочтительно выделить один **подтверждённо контролируемый registrable domain только для недоверенных runtimes**. Имя здесь не выдумывается и не резервируется; владение, DNS/TLS и absence of trusted applications на нём ещё нужно установить.
2. Сделать `соты.online` канонической оболочкой, а shell-страницы `soty.pochinit.online` направлять туда. Это может убрать конкретное соседство с оболочкой Сот, но приложения остаются same-site с другими доверенными проектами `pochinit.online`; один redirect не доказывает пригодности всей зоны. Переход профилей/browser storage, разрешённых origins, tickets, callbacks и сохранённых ссылок требует отдельной миграции.

Регистраторские полномочия, доступ к DNS API, wildcard DNS-01 credentials и неизвестные изолированные домены не проверялись. Current edge использует on-demand TLS с registry ask, а не подтверждённый wildcard certificate. В live Apps DB `soty.apps-registry.v1`: один app в состоянии revoked, один device binding, grants 0. Это соответствует прежнему smoke и **не доказывает** работу 3–5 настоящих опубликованных проектов.

## 4. Реальные старые данные

Наблюдение `/data` выполнено последовательным ограниченным чтением; содержимое, имена комнат, account IDs, токены и ключи не выводились. Короткие чтения SQLite были read-only/query-only. Это живой sweep, не согласованный холодный snapshot.

| Метрика | Значение |
| --- | ---: |
| Файлов во всём data directory | 1 074 |
| Сумма размеров файлов | 92 040 120 bytes |
| SQLite files, сумма | 33 599 488 bytes |
| WAL files, сумма | 8 590 360 bytes |
| Symlinks | 0 |
| JSON в корне | 912 |
| Сумма корневых JSON | 41 425 995 bytes |
| Наибольший JSON | 12 908 626 bytes |
| JSON больше текущего безопасного importer bound 64 MiB | 0 |
| Полная привычная room shape | 894 |
| Дополнительные parse-valid старые объекты с room fields | 11 |
| Известные служебные JSON | 3 |
| Прочий объект без room fields | 1 |
| Malformed legacy room candidates | 3 |
| Изменение size/mtime во время чтения JSON | 0 |
| Encrypted file records в 894 полных room objects | 50: 2 complete, 48 chunk |
| Наибольший такой file record | 341 830 bytes |

905 parse-valid room objects — это классификация формы, **не результат запуска всего v2 importer**. Старые частичные передачи, идентичности сообщений, declared totals, snapshots и tombstones должны быть проверены на изолированном восстановлении. Один неизвестный служебный объект не был автоматически объявлен комнатой только из-за подходящей длины имени.

Три malformed candidates имеют имена, подходящие действующему `/ws/[A-Za-z0-9_-]{16,96}`; в них имеются поля `snapshot`, `updates`, `closed`. Это не `connector-store.json`, `traffic-control.json` или `traffic-exit-pool.json`. Размеры 17 159, 17 871, 18 331 bytes, mtime 28.04.2026; корректный UTF-8, BOM отсутствует, его удаление не исправляет parsing. Имена/тела не публикуются. Fingerprints содержимого для сверки внутри recovery-процедуры:

```text
bcc89409aeae3beb49df6dec3e3ca71eb58cd53acc6d80d181dddd3a962ba7e5
10ea14dcf969585b4a03cf3abae1f2d884e0cec64854522a2ab8d40c9c1d2a04
b48233e87faca94ee408e10ca4d99806cbe1ea31866f0e30e308bb4b0fa5b386
```

Старый reader скрывает parse failures возвратом пустого состояния; новый v2 reader должен fail closed, сохраняя исходный файл. Автоматическая замена на `{}`, удаление или присвоение нового auth недопустимы. Нужен явный recovery disposition этих трёх старых кандидатов: доказанная реконструкция из имеющейся истории/backup либо сохранённый оригинал с понятной недоступностью. Свежий rollout не должен объявлять все 912 JSON здоровыми комнатами.

`rooms-v2.sqlite` сейчас отсутствует. Connector: 333 jobs, 59 durable requests, nonterminal jobs 0, quick_check `ok`; maintenance/rollback markers отсутствуют. Connect user_version 3 / 20 accounts; World user_version 3; Notes user_version 1. Проверка не выполняла платный inference и не читала результаты задач.

## 5. Диск и запас

Файловая система `/data`: total `1 006 450 962 432`, available `172 888 854 528` bytes (около 161 GiB), free с учётом reserved blocks `224 089 194 496`. Для оценки пользователя процесса используется available, а не большее free.

Текущий набор около 92 MB, старые JSON около 41 MB: сохранить оригиналы, добавить v2 database, зашифрованную копию и изолированный restore по объёму реально. Сборка Docker и чужая нагрузка на тот же диск учитываются отдельно; snapshot свободного места не является резервированием.

Квота 10 GiB P2 — **логические байты/полные reservation partial files**, не предел физического диска. Base64 увеличивает данные примерно до 13.33 GiB без учёта страниц/метаданных. Для эксплуатации разумный начальный бюджет: до 15 GiB main DB, до 15 GiB дополнительного WAL как плановый запас, 15 GiB encrypted backup, 15 GiB isolated restore и минимум 5 GiB образов/журналов, всего около 65 GiB, плюс независимый резерв свободного места. 161 GiB сейчас выше этого бюджета; это не измеренная верхняя граница WAL или будущего диска.

При долгом reader checkpoint может задерживаться, а WAL расти; [SQLite WAL](https://sqlite.org/wal.html) требует отдельного контроля checkpoint и читателей. Нельзя назвать 15 GiB жёстким ограничением без такого механизма. Один файл 512 000 000 bytes занимает уже около 0.64 GiB ciphertext/base64 на стороне сервера; две его durable копии — около 1.28 GiB до WAL и backup. OPFS клиента имеет собственную квоту и при сборке может одновременно держать parts и final — это не расход серверного volume.

Перед реальным выпуском повторить available/disk/inode checks, измерить candidate images и фактический cold archive, зафиксировать не менее выбранного запаса и правила отказа новых uploads при нехватке места. Не очищать чужие Docker images, snapshots, partials или историю, чтобы проверка прошла.

## 6. Резервирование: что доказано сейчас

Текущий controller использует Node `/home/ai2/.local/share/soty-connect/node/bin/node` и закреплённый adapter:

```text
/home/ai2/.local/share/soty-connect/host-tools/1aa5c9273fa3eec0cbdc20de2823c3e51f55fff9/deploy/connect/backup.mjs
```

Adapter требует остановленный точный container, отсутствие других writers, read-only/no-network tar helper; шифрует поток до записи на диск. В metadata входят исходная конфигурация и необходимые secrets. Backups не являются обычными незашифрованными tar.

На сервере найдены 2 `.enc` суммарно 178 757 875 bytes. Копия от 28.09, `soty-2026-09-28T01-15-32-937Z-13b113b6.enc`, размер 90 480 008 bytes, присутствует также в документированном операторском каталоге. Сегодня повторно выполнен штатный `operator-keys.ps1 -Action verify-backup`: **PASS**, GCM authenticated, offline metadata, полный tar 1 078 entries, 912 корневых room-file candidates, 2 SQLite +1 корректный пустой SQLite. SHA-256 локальной и серверной копий совпал:

```text
3dfcce8a7068aaddab46c3a82b252b6faa67d11bb07310ab83a89016b53100f0
```

DPAPI recovery key доступен текущему Windows-профилю; plaintext разбирался только в памяти. Эта проверка доказывает чтение существующего архива и его неизменность. Она **не** является свежим backup текущих 20 accounts, проверкой логики всех room JSON или настоящим развёртыванием восстановленной базы.

`verify-backup.mjs` не является restore CLI: он проверяет архив и не извлекает данные. Готового проверенного decrypt/extract→isolated-volume пути в прочитанных deploy scripts нет. Нельзя подменить реальную restore-проверку успешным `authenticated:true`.

## 7. Блокирующая граница отката P2

Установленный production host controller проверяет `/api/connectors/storage-ready`, Connect и model/policy readiness, затем при откате запускает прежний mapped image на актуальном volume. Старый image `24c2da2` читает JSON и не знает о `rooms-v2.sqlite`. После подтверждённых v2 writes такой откат способен показать старые комнаты и вновь писать в старые JSON, хотя общий `/health` останется успешным.

**Локально реализован исполняемый guard** в `deploy/connector/storage-guard.mjs` и независимый host-owned `storage-probe.mjs`. Оба deploy paths проверяют фактический room format и capabilities точного immutable image перед application start, включением restart policy и восстановлением. Connect дополнительно проверяет старый reader до offline status/enter helpers и повторяет проверку при settlement старого `restored` journal. Старый JSON-only/unknown reader после v2 не допускается; возврат старого backup для обхода ошибки запрещён.

Проверка отличает пустой volume (`rooms:"empty"`) от legacy JSON и v2 SQLite. Existing v2 + retained JSON требует reader v2. Требуется ровно один явный `DATA_DIR=/data`; иначе приложение использует другой default path. Corrupt/zero-length/unknown SQLite, orphan journal, volume subpath, nested `/data` mounts и иной running writer дают отказ. [Docker поддерживает монтирование подпапки тома](https://docs.docker.com/engine/storage/volumes/#mount-a-volume-subdirectory), поэтому проверка его корня не доказывает формат данных приложения; эта конфигурация пока явно отклоняется. Метка `io.soty.storage.readers` читается с точного image ID; скопированная container label не считается доказательством. Host source probe запускается отдельно в закреплённом trusted Node image, с единственным read-only `/data`, без application Env/config/secrets/network/ports; journal фиксирует intent и exact helper identity. Неопределённые create/start не повторяются. Helper не удаляется и не перезапускается автоматически.

Новый config требует `storageProbeImage` для application activation. Rebase сохраняет ранее выбранный pin либо берёт подтверждённый active image только как Node runtime для host-owned probe. Это не делает его совместимым application reader. Старый controller/config не приобретают защиту через обновление `modules/connect`.

Поддержанный профиль ограничен управляемым single writer и стандартным Docker local volume без driver options. Exact volume Name/Mountpoint/Driver/Scope/Options проверяются до и после probe; bind `/data` отклоняется. Проверка других running containers обнаруживает совпадающие имена тома и лексически пересекающиеся Source. Она не доказывает отсутствие произвольных symlink/driver aliases, host/remote writers; это отдельное операторское условие topology, а не универсальная гарантия guard. Контракт labels/probe покрывает только комнаты. Совместимость будущего Apps v2 и остальных durable stores требует отдельного подпункта.

Проверка pinned host source перенесена до baseline/load/recovery и повторяется перед activation/settlement, включая catch settlement. Pending journal не разрешает выполнять helpers из изменённого checkout. Probe не копирует application container Env; Docker может наследовать defaults самого image, поэтому доверенный pinned Node image и его hooks проверяются отдельно.

Локальная приёмка: `node --test deploy/connector/storage-guard.test.mjs deploy/connector/storage-guard.acceptance.test.mjs deploy/connect/host-controller.test.mjs deploy/connect/rebase-host.test.mjs deploy/connector/rollout.test.mjs` — **108/108 PASS без skips**, независимо повторено reviewer: 101 авторский +7 независимых acceptance-сценариев. Проверки используют реальные SQLite файлы/WAL и injected Docker faults. Дополнение02:54UTC: отдельный [real Linux Docker synthetic canary](storage-linux-canary-result-20260930.md) подтвердил RO WAL/SHM чтение exact rooms probe, mainHeader0/SQLiteversion2/markerHash, отказ unknown/corrupt format и old unlabelled reader; task cleanup и serving baseline проверены. Остаются: positive exact-image reader test будущего приложения, предварительно построенный compatible rollback image и проверенный переход первого v2 выпуска, backup/restore и отдельный Apps reader gate. Если SQLite не может открыть read-only mount без создания `-shm`, guard откажет; нельзя обходить это `immutable=1` или RW helper.

Новый guard в checkout не защищает старый постоянно установленный controller. Перевод выбранного timer/service на проверенное поколение host source требует отдельной явной операции с сохранёнными журналами. Удаление/игнорирование pending recovery, lock или pause marker не решает несовместимость.

## 8. Runbook следующего разрешённого выпуска

Ниже — порядок **будущих** действий, а не отчёт об их выполнении. Значения `VERIFIED_*` заполняются после независимой приёмки; команды с неподставленными placeholders не запускаются. На этом этапе production mutations запрещены.

1. Зафиксировать final Git commit, чистое дерево, Linux image identity/labels и тесты. Не публиковать stable feed, пока timer старого поколения может автоматически принять artifact. Подготовить проверенный guard и reader-compatible rollback image; испытать crash/rollback на disposable синтетическом volume. Сначала поддержка читателя/guard, затем разрешение нового формата.
2. Остановить только updater timer и подтвердить отсутствие controller process/locks/pending transaction. Не удалять чужой lock. Повторить live state, единственного writer и очередь через штатный read-only maintenance `status`. Если есть work — дождаться завершения; не отменять/стирать задания ради выпуска.
3. Закрыть admission **всех писателей** на время cold snapshot: connector maintenance сам по себе не блокирует комнаты/Notes/World. Дождаться accepted writes, остановить ровно проверенный serving container штатно и убедиться в отсутствии другого RW writer этого volume. При неподтверждённой остановке не начинать backup или новый writer.
4. Выполнить существующий encrypted adapter на точном stopped container. Шаблон проверенной команды на host:

   ```sh
   /home/ai2/.local/share/soty-connect/node/bin/node \
     /home/ai2/.local/share/soty-connect/host-tools/1aa5c9273fa3eec0cbdc20de2823c3e51f55fff9/deploy/connect/backup.mjs \
     VERIFIED_STOPPED_CONTAINER_ID \
     VERIFIED_PUBLIC_BACKUP_KEY_PATH \
     /home/ai2/.local/share/soty-connect/backups
   ```

   Adapter path/public key обязаны совпасть с выбранным закреплённым config; не подставлять key из новой недоверенной поставки. Получить только safe receipt, перенести готовый `.enc` по SSH, сверить SHA-256, затем проверить его в операторском процессе:

   ```powershell
   & '.\deploy\connect\operator-keys.ps1' -Action verify-backup -BackupFile 'ABSOLUTE_VERIFIED_ENCRYPTED_ARCHIVE'
   ```

5. **Реальный isolated restore gate пока требует реализации и приёмки extractor.** Сначала полностью проверить GCM и tar policy; только затем второй authenticated pass расшифровывает в ограниченный поток без plaintext промежуточного архива. Согласованный extractor должен сохранять DB/WAL/права, отклонять traversal/symlink escapes, не запускать архивный код и не печатать metadata/secrets. Запись разрешена только в новый exact empty isolated volume, никак не в live mount.
6. Восстановленную копию проверять container с `--network none`, без `-p`, Docker socket, production credentials/env и запуска `server/index.js`. Только offline migration/audit entrypoint из проверенного candidate image: SQLite integrity/FK, counts, durable identities/revocations, импорт всех определённых legacy rooms, original-file hashes, 3 malformed dispositions и отсутствие появления очереди на исполнение. Это предотвращает polling/replay задач и обращение к моделям. Настоящий PWA/connector runtime на восстановленной пользовательской копии не запускается.
7. Проверить два разных образа на этой копии: candidate читает и пишет synthetic canary state; compatibility rollback reader видит последние writes; JSON-only/unknown reader отвергается **до start**. Проверить shutdown/checkpoint. Только после этого выбирается production generation/подписанный artifact через существующий controller. Исходные volume/container/журналы и encrypted backup сохраняются.
8. После запуска: exact image/revision/reader capability, `/ready`, storage readiness всех изменённых хранилищ, counts/identities, один writer, public HTTPS/PWA/version, реальная file replay/delete/partial recovery. Не заявлять received/downloaded по relay ACK. Новый public app admission остаётся закрыт до отдельного P3 runtime-zone/browser/abuse gate.
9. Timer возобновляется только после подтверждения выбранного controller/config/state. После admission rollback сохраняет актуальный volume и применяет только совместимый reader. При uncertainty — `recovery_required`, не старт старого JSON reader и не восстановление более старого snapshot.

Изолированный restore — проверка восстановления, а не основание заменить live данными от 28.09. Более новые accounts, revocations, Notes, jobs и комнаты сохраняются.

## 9. Итог готовности

- Транспорт, текущий runtime, единственный writer, DNS/TLS, zero nonterminal jobs, чтение encrypted recovery material и запас диска подтверждены.
- До P2 deployment: Linux Docker приёмка локально реализованного format guard, совместимый rollback и выбранный новый controller, свежая холодная копия, настоящий isolated restore/migration и disposition трёх malformed legacy candidates.
- До P3 public admission: выбранная отдельная runtime site boundary, подтверждённое владение/операционное управление зоной, DNS/TLS и поддерживаемые браузеры, настоящие проекты и abuse gates.
- Никакая настройка, ключ, сертификат, DNS-запись или production container в этом preflight не изменены.
