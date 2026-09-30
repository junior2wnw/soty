# P2-G — передача файлов и хранилище v2

30 сентября 2026. Рабочая копия: soty-experience-release/соты. Production не изменён. Локальная реализация и доказательства ниже; общий P2 принимает интегратор после независимого review.

## Что реализовано

- Клиент резервирует слот до File.slice/шифрования: блок 256000 байт, максимум 8 ожидающих блоков / 4000000 wire bytes. Неконтролируемый производитель получает явный отказ, а не неограниченную очередь Promise. Повтор соединения отправляет тот же ID/ciphertext. Прогресс увеличивается после relay ACK. Это подтверждение сохранения сервером, не скачивания другим человеком.
- Отправленный файл скачивается из исходного File/Blob. ReceivedFile.bytes остаётся совместимым пустым legacy cache; фактическое содержимое — blob/url.
- Получение новых chunks использует OPFS: части на локальном диске браузера, затем disk-backed File; сборка передаёт File в native writer без полного ArrayBuffer. Входной crypto admission — максимум 4 операции / 2048000 заявленных plaintext chunk bytes; длина ciphertext связана с bytes+AEAD tag до decrypt.
- OPFS — временная локальная plaintext-копия, доступная этому origin, а не серверное раскрытие E2EE. Очистка при закрытии TunnelSync; остатки после crash очищаются при следующем открытии cache под Web Locks, не затрагивая живую вкладку. При отсутствии OPFS/Locks fallback допускает только файлы до 16 MB и 32 MB консервативного общего memory reservation; большой файл получает явный отказ. Ошибка локального хранения не удаляет серверные данные.
- SQLite rooms-v2.sqlite хранит отдельные записи блоков, событий, файлов, receipts, tombstones и import manifest. WAL + synchronous=FULL; короткие BEGIN IMMEDIATE транзакции. Admission/whole-file reservation/chunk/counters/receipt атомарны; ACK отправляется после commit.
- Payload сохраняется по allowlist, extra properties не попадают на диск/replay. Повтор ID с другой авторизационной device identity, типом или ciphertext отклоняется. Удаление не позволяет позднему chunk восстановить файл.
- Hello содержит только ограниченные метаданные и список незавершённых передач. Одна сохранённая запись в полёте на peer; cursor не держит read transaction через ожидание сети. Новый клиент отправляет replay.ack после decrypt и принятия блока локальным cache. replay.skip явно означает локальный отказ, не подтверждение скачивания и не удаление серверных данных. Старый клиент получает отдельные прежние file/update frames с pacing.
- От rate-limit освобождён только маленький (до 512 bytes), allowlisted, authenticated ACK/skip точного ненулевого in-flight sequence. Остальные сообщения, включая padded ACK до hello, используют обычный лимит.
- После reload собственное устройство восстанавливает и собственные файлы. Исключаются только fileId с действующим локальным Blob или активным manual send, а не все файлы своего deviceId.
- Shared metadata cache ограничен 128 rooms с retain/release соединений. Неизвестный room lookup не создаёт постоянную запись до claimAuth. Истёкшие disconnected join requests очищаются при cache admission, а не только при сообщении в старую комнату.
- Незавершённые передачи видимы через onFilePending, имя расшифровывает клиент. Карточка показывает сохранённый объём, не предлагает скачивание и содержит явное удаление. discardFileTransfer останавливает локального производителя этого fileId и ждёт ACK удаления. Полный content/history quota не блокирует удаление: у каждого существующего файла заранее ограничен один tombstone slot.
- Никакого автоматического TTL удаления серверных partial/complete файлов. Новая загрузка после обрыва не считается возобновлением старой; старые принятые bytes можно явно удалить. Automatic source resume после browser reload не заявляется.

## Лимиты и память

Параметры createRoomStore(dataDir, { limits }) задаются на уровне композиции. Defaults:

| Ресурс | Лимит |
|---|---:|
| Один файл | 512000000 полезных байт |
| Room: файлы вместе с полными резервами partial | 2 GiB |
| Все rooms: полезные байты и полные резервы | 10 GiB |
| Получаемые файлы на room | 4 |
| File identities, включая tombstones | 4096 на room / 100000 всего |
| Content receipts | 100000 на room / 1000000 всего |
| Независимые encrypted update payloads | 64 MiB на room |
| Общий консервативный резерв метаданных | 256 MiB |
| Новые durable rooms / cached rooms | 10000 / 128 |
| Безопасное чтение одного legacy JSON | 64 MiB |

2/10 GiB — полезные байты, **не размер SQLite на диске**. Base64, AEAD tags, receipts, SQLite pages/freelist и WAL добавляют расход. Нужны свободное место, мониторинг и резерв для backup. Внешний ENOSPC не превращается в ACK.

Нельзя называть весь pipeline «256 KB памяти». Помимо ограниченных живых очередей существуют JSON/crypto/native socket buffers, SQLite cache, UI state, браузерный GC и disk-backed Blob accounting. На OPFS при сборке parts и final file временно сосуществуют, проверяется приблизительная доступная квота для 2×size; параллельное потребление другими вкладками всё ещё может вызвать явный quota error. Legacy complete — отдельный неделимый ciphertext с большей памятью для decrypt; server-side рекодирование без E2EE ключа невозможно.

Snapshots Yjs больше не молча удаляют независимые encrypted updates: сервер не способен доказать покрытие concurrent операций зашифрованным snapshot. История имеет явный лимит; безопасная согласованная compaction — отдельная будущая работа.

## Проверено локально

Receipt: evidence/p2-file-storage-20260930.json.

1. 36/36 focused tests PASS:
   - server/test/realtime-file-durability.test.mjs — 12;
   - server/test/room-storage-acceptance.test.mjs — 7 независимых тестов рецензента;
   - src/transport/file-transfer.test.mjs — 12;
   - src/transport/file-receive-store.test.mjs — 3;
   - src/platform/command-file-transfer.test.mjs — 2 (интегратор).
   Проверены реальные loopback WS/SQLite, commit barrier, injected actual SQLite failure с rollback reservation/chunk/receipt, duplicate fingerprint, two-connection reservations, удаление при полной квоте, corruption, import, backup/restore, auth/replay backpressure, неизвестные комнаты, expiry cache, stale hello, ложный bytes до crypto, Unicode/empty/binary SHA256, offline/reconnect/destroy, bounded unconstrained producer.
2. Специальный regression: four interrupted transfers remain manageable after reload; explicit ACKed discard frees a slot. Четыре partial → reload с durable inventory → пятая отклонена → явное подтверждённое удаление → пятая принята.
3. Клиентский regression: pending inventory decrypts names after reload and explicit discard waits for the delete ACK. До ACK карточка не исчезает; поздние chunks того же отменённого fileId отклоняются.
4. Реальный большой server proof, запуск SOTY_LARGE_FILE_TEST=1 node --test server/test/room-storage-large.test.mjs:
   source.bin 512000000 байт → 2000 AES-GCM blocks → настоящий WS / ACK → SQLite close/reopen → authenticated cursor replay → decrypt → restored.bin.
   SHA256 обеих сторон: d002f5af1c28f9dc2477bc1d4929bcf373d51f792aa7532eea1928709e9a2b96.
   Повтор после server audit fixes: PASS, 248 s передачи/восстановления; peak sampled Node RSS 133197824 bytes, heap 33720720 bytes, максимум одновременных receive operations 1. DB size до последнего checkpoint 681652224 bytes. Это Node pipeline, не измерение браузера/телефона.
5. Настоящий desktop Chrome: src/transport/file-storage.test.html — 512000000 bytes, 2000 блоков через реальный OPFS helper, сборка в File, File.stream проверяет **каждый байт**, temporary cache удалён. PASS за 18 s, reservedMemory=0, peakOperations=1.
   Coarse performance.memory: baseline 184608311; write 469528747; assembly 161804823; verification 528356209 bytes. Значение зависит от общего browser process/GC; verification тоже создаёт временные read buffers. Это **не** hard bound JS heap/RSS и **не** доказательство на слабом телефоне.
6. Typecheck PASS после partial lifecycle. Общие build/world/визуальную приёмку выполняет интегратор.

Первый исторический подэтап заменял неправильный seen-before-save и JSON reference race (6 tests PASS). Текущий v2 заменяет его JSON writer; прежние доказательства rename не выдаются за доказательства SQLite. Все релевантные invariants повторены в новых actual-store tests.

## Миграция, deployment gate и откат

Legacy JSON не перезаписывается и не удаляется. Импорт одной комнаты транзакционный: исходный hash/size/counts в room_imports, auth/closed/IDs/ciphertext сохраняются, actual chunk counts/declared sums согласованы. Отсутствующие части сохраняются как incomplete; импортированные данные выше новой квоты доступны, новые файлы ограничены. Unknown/extra payload поля не нужны протоколу, исходный JSON остаётся recovery-архивом.

До production обязательны внешние проверки состояния, которых этот локальный этап **не выполнял**:

1. Остановить старые writers; получить согласованный зашифрованный backup каталога данных. Не копировать только основной SQLite файл при работающем WAL writer.
2. Инвентаризировать именно legacy room JSON: counts/sizes/валидность схемы без печати auth/content. Если хотя бы один источник >64 MiB, **остановить rollout**: текущий bounded importer вернёт room_legacy_streaming_import_required. Streaming importer для такого набора ещё не реализован.
3. Проверить фактические disk capacity, число rooms/files/partial и grandfathered превышения. Подтвердить restore на отдельном каталоге и сравнить counts/hash/receipt, не только HTTP health.
4. Подтвердить release-format compatibility: SQLite user_version=2; неподдерживаемая версия fail-closed. Первый v2 write исключает возврат к v1 binary, читающему старый JSON.
5. Rollback только на сборку с совместимым v2 reader либо отключение новых uploads при сохранении чтения. JSON, оставшийся после миграции, **не** является актуальным rollback данных.
6. Проверить PWA update/reload/phone/browser matrix и explicit partial discard через конечный UI. Локальная OPFS quota/совместимость и RSS слабых устройств остаются отдельной измеримой границей.

Закрытие room store ожидается вместе с app services после закрытия WebSocket server. SQLite COMMIT/FULL и quiesced backup проверены локально; реальная power-loss durability зависит также от OS/накопителя. Не заявляется production-scale нагрузка, глобальная кластерная доставка или доступность миллиардов пользователей.

Основания: [File API](https://www.w3.org/TR/FileAPI/), [File System Standard](https://fs.spec.whatwg.org/), [Storage Standard](https://storage.spec.whatwg.org/), [WebSocket API](https://websockets.spec.whatwg.org/), [WebRTC buffering](https://www.w3.org/TR/webrtc/#dom-rtcdatachannel-bufferedamount), [SQLite atomic commit](https://www.sqlite.org/atomiccommit.html), [SQLite WAL](https://www.sqlite.org/wal.html).
