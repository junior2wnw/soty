# Канал восстановления поверх текущего Root Core

Ветка `codex/universal-private-attach-core`, исходный Root `f34dc3549e5b1b7c270186bc800adf0a82ff72ee`, reviewed sender Core `90335314a663441d34148e8c312e3e4d42350eb7`. Модули добавлены отдельно в `deploy/connect/private-attach`; работающие маршруты, default Root reader, ordinary Native3 specialization и операционный controller не переключены.

Пять phase adapters и шесть новых warm/readiness/child модулей перенесены из проверенных donor packets. Старые parser/sink/sender/inventory aliases не копировались. Adapter вызывает текущую named factory `createStagedAuthenticatedBackupSender({beforeBody})`; generic `sendAuthenticatedBackup` по-прежнему отвергает hook в public options. Ошибка закрывает и client, и readiness lease.

Новый strict receiver использует текущий `extractOwnedBackup`, фиксированные пути `/owned/target/{data,config}`, разные RAM/config и physical/data mounts, пустые директории владельца и pin mount namespace. Metadata не выбирает пути, executable, reader или limits. Preflight нормализует current Core paths, требует `/operator/` и read-only Source closure; retained pins обновлены под текущие файлы. Receipt сохраняет восемь полей и подтверждение readback.

Операционный host lease, актуальный доступ к данным, реальный serving STOP и watchdog должны поступать от отдельного владельца. `noteServingStopped` сам по себе их не доказывает. Эти модули не включены в production controller; SOURCE tests не заменяют physical receiver/cold restore и old→new→old проверку на новой версии.

## Проверки

Node24.19 Windows, канонические Git bytes: 149/149 PASS, 0 FAIL/SKIP/CANCEL, 45.74s. Проверены настоящие native child pipes/close, warm read0, bind EOF, ACK, пределы, отмена, deadline, late/reentrant callbacks и sender GCM first pass. Все 13 runtime modules прошли syntax check. Current Core SHA для пяти файлов зафиксированы отдельно; логических изменений Core этим переносом нет. Пять файлов приведены к точным LF-байтам существующего Git revision только в отдельном checkout; Git diff пуст. Предыдущий исправленный запуск 149/149 с Windows line endings сохранён отдельно.

Первый запуск сохранён: 143 tests, 130 PASS, 13 FAIL. Причины: не перенесённые historical test paths, два старых ожидания вызова accessor и изменение нашего нового receiver после pin capture. Перед успешным запуском closure и pins заморожены; изменений во время второго запуска не было.

Тестовые изменения раскрыты: imports переведены на текущий Root; четыре historical byte-comparison tests заменены current Core pin checks, так как historical sources сохранены в donor packet, а в runtime не копируются. Два старых getter counterexamples усилены: accessor отклоняется с **0 выполненных getters**, 0 body и закрытым sender. Остальные assertions не изменены. Historical donor files и первый failed run остаются доступными.

Core ранее отдельно проверен: Windows51, Linux sender/parser46, независимые peer11 и peer58. Эти результаты имеют свой scope и не складываются в число новых тестов. Actual Linux port qualification и independent source review этой новой closure ещё предстоят.

Ни Engine/network/model operations, ни установка, публикация, миграция или serving activation переносом не выполнялись.
