# Private Attach: текущий Root sender, 08.10.2026

В отдельной ветке от общего `f34dc35` добавлен host-only `createStagedAuthenticatedBackupSender({beforeBody})`. Существующий `sendAuthenticatedBackup` по-прежнему отвергает неизвестный `beforeBody` в своих options. Старые Root/Source3 readers, restore format, paths, key custody и лимиты не заменяются donor-копиями.

Новый порт вызывает callback ровно после первого полного GCM/tar/manifest/source-witness прохода, используя тот же удерживаемый descriptor; до callback и после его завершения перечитывает неизменность исходного файла. Callback получает только immutable authentication pins и `check()` текущего срока/отмены, без private key или plaintext. Второй проход и первый body byte начинаются после успешного callback. Readiness ACK не считается byte progress и не сбрасывает wall/idle budget. Неопределённый или отвергнутый callback, abort и смена файла запрещают body; позднее завершение не возрождает отправку.

Factory отвергает accessor/proxy options без выполнения их traps. Это private JavaScript composition port, не HTTP, MCP или author JSON permission. Он не выдаёт engine access, Native role, эксклюзивную writer lease и не доказывает фактический serving STOP. Это остаётся обязанностью внешнего проверенного host operator и будущей phase boundary.

На Windows Node24.19 проверены51 sender/parser/sink/SourceNative3 cases, PASS51/0FAIL/CANCEL/SKIP. Пять новых causal tests покрывают удержание bytes до hook, corrupt GCM/no-hook, изменение held archive, rejection/abort/late settlement, незавершающийся hook и неизменный idle deadline; шестой подтверждает closed legacy options и отсутствие accessor/proxy execution. Проверки используют только synthetic keys/archive/data.

Новые phase modules ещё не подключены. Требуются отдельный reviewed dependency closure без старых vendor readers, actual Linux/immutable image, fresh SOTYBAK1 transfer/restore и реальные старый→candidate→старый boots/config/data proofs. Native HIVE929 и соседняя PrivateAttach fixture не подменяют эти gates. Main и production этой веткой не изменены.
