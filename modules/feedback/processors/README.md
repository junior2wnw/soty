# Локальная обработка вложений обратной связи

`createLocalFeedbackProcessor` запускается установленным Source/локальным агентом
с доверенной конфигурацией хоста. Пути executable/model/scratch не принимаются из
обращения, app manifest или результата модели. Speech использует локальный
`faster-whisper`, CPU/int8/4threads и уже установленную модель; загрузка из сети
отключена. Windows OCR использует установленные языки ru/en и WinRT. Отсутствующий
движок возвращает безопасный отказ, без подмены облачной услугой.

Source передаёт `currentAuthority(signal)` только из своего проверенного native
auth/project scope. Проверка обязательна до чтения media, перед child process и
после его завершения/чтения результата. Поле `{allowed:true}` не является проверкой.
Эта callback проверка не заменяет текущий native ACL в атомарной Source transaction
при последующем сохранении оценки. В модуле нет собственной очереди, Root DB,
сохранения assessment, автоматического исправления кода или публикации.

Одна незавершённая работа, default deadline120s/max180s на всю операцию. Timeout,
caller AbortSignal и close запрещают выдачу результата и останавливают собственный
child. Игнорирующая abort Source callback продолжает занимать slot до фактического
завершения. Никаких shell command strings или вызовов произвольного executable из
пользовательского текста. Output/error logs child ограничены8KiB и отбрасываются.

Вложение сначала проходит общий настоящий parser: ≤1MiB, PNG/JPEG/WebP или
Opus WebM/Ogg≤120s. OCR дополнительно учитывает `MaxImageDimension` установленного
движка; больший допустимый upload может быть недопустим для OCR и не получает
фиктивный текст. Результат ограничен32KiB UTF8, закрытым DTO и помечен
`trust:'untrusted-content'`. Он остаётся пользовательским содержимым, а не
инструкцией, grant или подтверждением исправления. Поддерживаются ru/en; качество
на реальных голосах/акцентах/шуме требует отдельной измеренной приёмки.

Scratch directory заранее создаёт оператор под своим аккаунтом; Linux требует
uid ownership/mode700/no symlink/canonical path. Windows оператор должен обеспечить
private ACL родительской папки. Создаётся только собственный `job-*`, фиксированные
input/output имена и create-exclusive private output. Очистка проверяет точный
родительский путь, удаляет только три собственных известных файла и пустую папку.
Рекурсивного удаления, удаления путей автора или persistent result cache нет.
После аварийного завершения процесса остаток требует явного операторского recovery,
а не автоматического сканирования чужих папок.

Проверки:

```text
node --test modules/feedback/test/local-processor.test.mjs
node scripts/fixtures/local-feedback-processing.mjs <synthetic-directory> <python> <cached-model-directory> <WindowsPowerShell>
```

8/8 tests проверяют отсутствие engines, current permission, сокрытие private errors,
ignored-abort deadline/slot, cancellation/close и настоящий failed child spawn с
очисткой файлов. Отдельный installed-engine gate прошёл на synthetic Russian
voice+PNG: настоящий local ASR12.182s/OCR0.547s, expected phrases matched, fresh
authority≥5checks, after-engine revoke скрыл текст, actual child cancellation и
private scratch cleanup прошли. Text/result files не входят в public receipt.
Первый synthetic FFmpeg WebM был отвергнут narrow media profile **до** engine;
финальная приёмка использует допустимый Ogg Opus. Это не обещание поддержки любого
файла с расширением webm.

Source feedback protocol v1 по-прежнему сообщает `asr:false`: ещё требуется отдельный
approved processor grant, Source queue/job/idempotency/assessment boundary и UI
с явным получателем. Наличие работающего engine само по себе не включает обработку
всех обращений и не даёт агенту доступ к чужим проектам.

API и ограничения движков сверены с первоисточниками:
[faster-whisper](https://github.com/SYSTRAN/faster-whisper/blob/master/README.md),
[Windows OCR](https://learn.microsoft.com/en-us/uwp/api/windows.media.ocr.ocrengine.recognizeasync?view=winrt-26100).
