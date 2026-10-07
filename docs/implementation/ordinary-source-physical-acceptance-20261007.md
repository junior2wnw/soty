# Отдельное приложение: установка и восстановление

Проверено 07.10.2026 на собственном Linux-стенде. Это фактическая приёмка установки, сохранения данных и зашифрованного восстановления обычного Source-приложения. Совместный вход через текущие Соты, реальные ASR/OCR и production принимаются отдельно.

Пакет `95331b4ffca87d473a25a26955b5a182`, Source helper revision `f138b368e72ba2ec0e90116a1934dbd30c8e28af`, manifest `50bfcff180aaafb2fec3106a6792d20775be74f5245966c6b447e9ec858b9c09`.

Runtime Source — `sha256:ffce4d753b8025e5b7d8a150b0c2a46d48a18f0c027685ca31c1d33a82506f23`, собранный из установщика Main `b589ee5b2df49ef7660cf03e9de6a39d229d1cf9`. Проверены полный config/RootFS после переноса, отдельные Source labels и реальные runtime-файлы. Helpers работают с Node24.15.0, UID1000, readonly rootfs и без сети. Только отдельный ограниченный supervisor получает собственный Docker socket.

| Проверка | Результат |
| --- | --- |
| Независимый читатель Native3 | Приняты точные 29 SQL objects; прежний Reader2 отказал |
| Параметры запуска | Чужой realm, отсутствующий ключ и неверный ключ нужной длины отклонены до listener |
| Существующие данные | Права, ciphertext и исходные квитанции сохранены; ключ проверяется на существующих зашифрованных моделях |
| Реальный установщик | Listener запускается с проверенной конфигурацией; подмена identity анонимным запросом не даёт доступа |
| Копия и восстановление | RSA3072/AES-GCM; восстановление конфигурации, ключей и Native данных в новый физический volume |
| Независимое сравнение | Два разных physical mountpoints; закрытые snapshot/cipher/config/secret сравнения совпадают |
| Содержимое | 2 principals, 2 memberships, 1 ticket, 1 исходная receipt; по 6 физических файлов до и после восстановления |
| Завершение | Все собственные Source/helper/supervisor containers удалены; cleanup подтверждён |

Отдельный Linux stream probe подтвердил настоящий native FS sender и receiver через ограниченный RAM-файл: двукратная проверка аутентичности, fresh target, Reader3, байты, закрытие FD и удаление RAM-файла. Sender `Duplex` запрещён; receiver проверяется по своему реальному `Readable`-контракту, включая отказ `emitClose:false` до чтения. Этот probe сам по себе не является physical cold restore.

Для Source добавлен отдельный constructor-only Native3 restore port с фиксированным `native/native.sqlite`, store/format/realm и checkpoint. Он использует тот же проверенный шифратор, parser, limits и sink. Основной Root/R0 продолжает требовать `connector-store.sqlite`; архив Source через основной Root port отклоняется. Metadata не подтверждает Native права и не заменяет независимый SQL reader.

После переноса этих проверенных изменений в Main `c9f2c833a58510a98b9242e46c79c2ff17777dea` regression summary: 245 tests, 243 passed, 2 explicit skips, failures/cancelled0. Исходный collector ожидал TAP, а Node выдал spec reporter; summary независимо прочитан из неизменного журнала. Отдельный exit code Node не был сохранён, поэтому эта запись не объявляет новый release image принятым. Runtime B02 остаётся прежним принятым образом ядра.

Прежние отказы сохранены: JSON object order, отсутствующий nested mount target, auto-start CLI после bundling, Root-only path/checkpoint и ошибочное предположение о receiver `Duplex`. Они исправлялись в новых пакетах; прежние данные и доказательства не переписывались.

Артефакты:

- [Actual physical cold](D:/соты/output/soty-universal-platform-implementation-20261006/source-cold953-public.json).
- [Независимая проверка](D:/соты/output/soty-universal-platform-implementation-20261006/source-cold953-independent-public.json).
- [Linux native stream probe](D:/соты/output/soty-universal-platform-implementation-20261006/source953-stream-probe-public.json).
- [Main regression summary](D:/соты/output/soty-universal-platform-implementation-20261006/main-c9-source-connect-regression-public.json).

Source Long4 остаётся отдельным предложением. Принятый Native3 restore не читает будущий format4. Windows isolation/ACL, текущий Root login и качество обработки реального голоса не подтверждены этой приёмкой.
