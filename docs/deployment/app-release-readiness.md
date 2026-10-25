# Готовность размещения: проверяемые границы

Пайплайн должен давать воспроизводимый результат без потери данных, новых прав
по умолчанию и вымышленной готовности. Таблица относится к новому приложению,
обновлению, смене origin и выпуску самой платформы. Неприменимый сценарий имеет
причину; непроверенный — открытый gate, а не успешную отметку.

## Приёмка приложения

| Граница | Минимальное доказательство |
|---|---|
| Исходники | Exact Git revision/image digest, clean release copy; dirty user source сохранён |
| Первый запуск | Migrations до optional workers; health отражает нужные зависимости, реальные payload/freshness |
| Ресурсы | Свободные disk/inodes/ports/Docker networks; build не уничтожает данные других проектов |
| Коннектор | Current owner/device, та же граница workspace/доверия, живой источник; не копия коннектора на каждую карточку |
| Именной адрес | Exact app/domain/source/publication pins, fresh snapshot; unknown/nested host denied, TLS валиден |
| Native | Fixed loopback, per-request access/pin; query и app cookies/CSP сохранены, Soty cookie не уходит в app |
| Source/Native пара | Разные approved origins/targets; карточка ведёт в Source, отдельный вход — Native; exact Root policy GET не заменяет Native Human/ACL/короткую lease |
| WebSocket/изменения | Настоящие 101 + message exchange; wrong/missing Origin, invalid session и changed pin отвергнуты |
| Вход человека | Human OIDC/BFF, issuer/sub/current userinfo/native ACL; private owner mode явно ограничен одним владельцем |
| Разные люди | Два разных пользователя; чужие записи/роли и неверные identity claims отвергнуты |
| Отзыв/срок | Deny, expired/revoked device/grant/client/session не становятся анонимным успехом; поведение после TTL реально проверено |
| Браузер | Card → named iframe → настоящее UI → permitted write → полный Root reload → прежняя запись |
| Разные поверхности | Standalone, fullscreen → return с тем же iframe, нужные nested frames, callback и файлы |
| Readiness | `load` = документ; legacy unsupported = neutral loaded; scoped denied/unknown не подделаны ready |
| Agent/MCP | Exact audience/scopes/source grants, стабильный intent ID, реальный effect/read receipt; guidance не считается live QA |
| Данные | Прежние IDs/ACL/строки или проверенный экспорт; browser drafts/outbox сохранены отдельно от серверного volume |
| Config/rollback | Encrypted backup с decrypt/restore scope, CAS → validate → known-process reload → active/disk equality; совместимый откат |
| Повтор | Тот же проект обновляет прежний app/domain; conflict перечитывается; uncertain mutation не создаёт новый intent |

Для бизнес-приложения дополнительно применяется
[матрица стандартных модулей](../../skills/soty-app-deploy/references/standard-modules.md)
в разрешённом объёме. Reviews/docs/feedback/guide не имитируются статическими
заглушками и не получают чужих данных/прав.

## Проверки исходного пайплайна

Node должен соответствовать `package.json` (>=24.15.0), зависимости — lockfile.
`node scripts/check-app-release.mjs` запускает закрытый набор затронутых tests,
typecheck, штатный prebuild и production build. **Нужен установленный Caddy**:
абсолютный путь в `SOTY_CADDY_BIN`; отсутствие бинарника останавливает gate,
а не превращает skipped native/zone acceptance в PASS. На Windows переменная
задаётся PowerShell `$env:SOTY_CADDY_BIN='C:\tools\caddy.exe'`, на Linux —
обычным env prefix. Gate не загружает Caddy из сети и не использует live config.

Штатный Dockerfile также передаёт реальный Caddy в `app:release:test`: бинарник
берётся из immutable official `caddy:2.11.2` image по закреплённому digest.
Он нужен только в build stage и не добавляется в production image. Поэтому
стандартная сборка тоже больше не пропускает native/zone acceptance молча.

Тесты запускают только собственные loopback listeners и synthetic stores.
Они не включают issuer, не регистрируют клиентов, не публикуют приложение,
не управляют живым Chrome и не поднимают второй production writer.
MCP tests владеют своим consent document и не зависят от случайного старого dist.

Это **проверка пайплайна размещения**, не всего продукта. Для платформенного
релиза дополнительно нужны все применимые upstream suites, storage reader/
migration compatibility, current DATA qualification, drain/prepare/promote,
single writer, immutable image и реальная production/browser приёмка.
Нельзя выпускать чужие незавершённые изменения только ради новой карточки
или обходить незакрытый gate механизмом recovery.

## Skill и discoverability

Maintained skill — `skills/soty-app-deploy`, установка —
`node scripts/install-soty-deploy-skill.mjs`. `agents/install-receipt.json`
содержит путь к репозиторию и hashes, но путь может устареть: проверить наличие,
Git и текущую рабочую копию. Исторический receipt не является live state.

Обновление по умолчанию сохраняет локальные правки и отказывает при конфликте.
Если другой выпуск уже добавил полезные references, сначала включить их в
maintained source и проверить итог. Для намеренно проверенного merge можно
передать `--reviewed-baseline FILE`: строгий
`{schema:"soty.skill-update-review.v1",files:{"SKILL.md":"<current SHA256>"}}`.
Только перечисленные точные текущие bytes разрешают замену; изменившийся
baseline отказывает. Это не force и не разрешение перезаписать неизвестные правки.
Unknown target files сохраняются. Следующая установка повторяется без baseline.

Новый Root MCP после развёртывания объявляет `apps_diagnose` и `apps_auth_guide`
в tools/list вместе с прежними операциями. Текущий live server может быть старше:
до публикации использовать проверенную CLI, не заявлять, что новые инструменты
уже доступны всем удалённым клиентам.

## Receipt выпуска

Хранить безопасные source/image/app/domain pins, tool versions, exact checks
и результаты (PASS/FAIL/SKIP), отдельные RED/GREEN артефакты, config hashes,
data/browser/access evidence и проверенный rollback. Секреты, full private rows,
claim codes, tokens, auth headers, полный boot URL и дампы env запрещены.
«Создано», «проверено локально», «внедрено» и «принято человеком» — разные факты.
