# Реальный изолированный Linux canary: пройден

Root выполнил reviewed harness v6 на Dev. Controller завершился с exit0 и complete.ok:true; 21 дочерний контейнер завершился с exit0, два собственных тома и зашифрованные свидетельства сохранены. Production mutations: **0**. Это проверка изолированных образов и данных, не публикация новой версии Сот.

Application revision: `8e7a10468a144ed1842f9448adebc7099831cea3`.

| Образ | Режим | Actual image ID |
| --- | --- | --- |
| Baseline | compiled legacy1, новые admissions выключены | sha256:c8754ba67f1883cb33417a970ef9947b0c1a499934d35f32f58f58a59c23c2e4 |
| Features | compiled legacy0, Universal/Human включены | sha256:21018c46a27f97ad419d37feb2ec1221ebcf10805ed8292fe67a6534f6f63558 |
| Прежний serving image | только metadata/отказ reader до START | sha256:1c79c2a71aa3438472da8b197f03494908ee242e67fd76bb6a72d3da6340c7da |

Старый image не создавался и не запускался на новых данных. Совместимый fallback — новый baseline, который умеет читать Human2. Serving container не переключался.

## Что доказано

1. Baseline реально запустил installed-image createHttpApp и private Unix operator; измерены compiled legacy1 и отсутствие Human HTTP. Synthetic signed Connect создал профиль, связал local connector, зарегистрировал приложение и записал Notes через настоящие grants/coordinator/effect/receipt. Fixture использует существующий плоский note.noteId с проверкой revision.
2. После чистой остановки writer независимый host probe подтвердил v3 и отсутствие новых Universal stores. Notes2/Caps3 подготовлены явными trusted migration constructors; HTTP не выводит миграцию из входных данных.
3. Features запустил actual image factory с explicit humanIdentityRenewalMigration:true. Прежние actor/app прошли registration/provider inbox, feedback submit/replay, support reply, ready_to_check и подтверждение resolved автором обращения.
4. Два RP fixture BFF прошли OIDC code+PKCE+JOSE+userinfo: один issuer/sub, разные локальные RP IDs, подписанный stayInAppSeconds:86400. Fixture clock сдвинут на310секунд; реальные refresh requests сохранили две consumed tombstones. Это clock-injected gate; настоящий HIVE browser wall-time проверен отдельно.
5. Cold probe подтвердил v5: Rooms2/Apps6/Notes2/Caps3/AppRegistration1/Feedback1/Human2 плюс retained Connect. Прежний image отвергнут до создания контейнера и START.
6. После stopped features новый совместимый baseline сохранил Connect/app/note и приватные IDs/source pins/rows/ciphertext/consumed evidence. Human admissions выключены.
7. После остановки всех writers AES-GCM backup включил coherent /data, приватное состояние RP и синтетическую issuer/client/artifact/clock конфигурацию. Внешний custody key исключён. Исходная конфигурация заменена unavailable markers. В новый пустой собственный том восстановлены данные и **байты конфигурации из ciphertext**, совпадение проверено внутри приватного процесса.
8. Restored feature обслужил настоящий userinfo обоих RP; прежние IDs, identity/source pins, feedback и consumed evidence сохранились. Семь versioned product stores плюс Connect — не обещание ровно семи физических DB files.
9. Installed-image universal-operator.test.mjs в отдельном контейнере: 9 tests / 8 PASS / 0 FAIL / 1 Windows-only skip. Все семь обязательных native Linux cases пройдены.

Cold probe/evidence используют unchanged host storage-snapshot.mjs перед read-only SQLite probe, как production storage guard. Source volume остаётся read-only; SHM создаётся лишь в ограниченном tmpfs snapshot. Нет immutable-URI shortcut на живом WAL. Backup ограничен64MiB/256files/64dirs/depth8; ссылки и неподтверждённый restore payload отвергаются.

## Изоляция и pins

Application archive: /home/ai2/codex-soty-universal-20261007-9f8dcd71/source-8e7a104, bytes неизменены. Harness — отдельный flat RO packet из трёх mjs и закрытого manifest.json, не код приложения этой revision. Runtime imports — из actual image /app; независимые DockerApi/storage guard/snapshot/probe/local connector — из host archive. [Точные pins и исполненная команда](./universal-canary-launch-20261007.md).

Успешный nonce20f5e9cac0e34dde8bced550ff460d2e, packet digest4f5c8dd92d3d970253f3cdd082235fcce98fe7c5bd8972d1ee8a3d17a5d7eaf8. Сibling harness/canary каталоги owner0:0/mode0700, packet/config/key/receipt files0600. Fixed metadata preparation меняла только проверенные собственные leaf paths; source bytes/UID1000 и host task parent0700 сохранены. Windows Git archive содержит CRLF: pins относятся к точным экспортированным bytes, отдельно бинарно сверенным с archive и Git blobs; runtime ничего не нормализует.

Только controller получил Docker socket. Children: network none либо namespace собственного entry, без published ports/socket, read-only rootfs, cap-drop ALL,768MiB/2CPU/96PIDs, bounded tmpfs. Нет production mounts/config/queues/accounts. Перед CREATE/START/STOP/receipt повторно проверены exact owned IDs, source и RO packet. Неоднозначные операции не повторяются; marker исключает повторный nonce. Recursive delete отсутствует.

Прежние попытки не объявлены успешными: v3 ancestor modes; следующий v3 raw LF/CRLF pins; v4 ошибочная nested Notes receipt; v5 отсутствие snapshot перед cold read-only SQLite. Захваченные собственные writers остановлены, namespaces сохранены отдельно. v6 исправляет harness, сохраняя exact application/image bytes.

Safe log: D:/соты/output/soty-universal-platform-implementation-20261006/canary-20f5e9cac0e34dde8bced550ff460d2e-linux.log. Только step names/counts/bools/public image/source pins; секреты, private evidence digests, request bodies и raw stderr не выводились. Local harness tests11/11PASS.

## Пределы проверки

Controlled entry использует actual image factory/compiled mode/SDK/SQLite/operator, но не доказывает production loader/closed rollout CLI, настоящие HIVE/Planner account rows, общий RP SDK, browser UI или external author onboarding. Backup сделан после явной остановки writers; crash/concurrent backup и отзыв после момента snapshot — отдельные сценарии. Синтетические RP/ключи не являются пользовательскими аккаунтами. Новые Apps7/query/guidance изменения требуют отдельной сборки и reader/fallback gates. Production выпуск не выполнен.
