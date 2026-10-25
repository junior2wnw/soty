# Подготовка, восстановление и обновление

Знания из реального восстановления/размещения 20.10.2026; конкретные серверы,
секреты, app IDs и версии одного проекта не являются defaults другого.

## Данные и удаление

До разрушительных действий установи точную принадлежность контейнеров,
volumes/networks/paths/images. Авторизация на очистку одного проекта не даёт
разрешения prune shared cache или удалять другие проекты. Generic debug image
удаляй только после доказательства принадлежности, например Git blob provenance.

Config/данные с секретами архивируй зашифрованно. Проверяй decrypt, содержимое,
целостность и restore нужной СУБД. Архив PG files без проверенного старта PG
не называй проверенным восстановлением. Прерванный архив не считается backup.
Согласованная SQLite backup учитывает WAL; VACUUM после backup проверяется по
integrity и прежним строкам/IDs, а не размеру. Secure erase не следует из rm.

Для полного Root release используй maintained `deploy/connect/gpg-release-backup.mjs`,
а не прежний `deploy/connect/backup.mjs`. Штатный latest-data promote требует
operator `fullEncryptedBackup` callback после подтверждённого STOP exact writer
и restart policy `no`, до candidate START. Нужны **оба** артефакта: полный DATA
volume и effective configuration (inspect/Env и все binds), с authenticated
decrypt/readback и отдельным isolated restore. Частичный архив не заменяет их.
Проверь clean exit/PID0/неизменный lifecycle и отсутствие других writers до и
после. Дополнительные внешние зависимости учитываются отдельно.

Используй существующие CurrentUser-DPAPI `windows-backup-key.py init/check/send`
и SSH stdin → GPG fd3; Node24/hash/helper проверяются до unwrap. Ключ не попадает
в argv, Env, plaintext file или Docker logging. DPAPI blob привязан к Windows
профилю и сам по себе не переносимый recovery key. При доступном checkout полный
путь: `deploy/connect/GPG-RELEASE-BACKUP.md`, wiring — `deploy/connector/README.md`.
Unknown backup/helper outcome сохраняет `recovery_required`, а не новый retry.

## Canary на текущей копии

Копия реальных DATA требует той же конфиденциальности, private configuration и
изолированного owned volume без внешнего обслуживания/Gateway вызовов. Bounded
online file copy с проверенным стабильным окном — preflight, не глобально
согласованный snapshot и не backup. Квалификация synthetic data не заменяет
current-data canary, а запуск baseline не квалифицирует будущий candidate.

Сравнивай full Config/HostConfig/Networks и **полные** Mount records, отсортированные
детерминированно с сохранением всех полей; raw-array hash зависит от порядка
Docker inspect. Не удаляй RW/Source/Name/другие поля ради совпадения.
До каждого application START вызывай существующий `guardStorageStart` с
независимо закреплённым probe image/source. Probe запускается с
`SOTY_STORAGE_PROBE=1`: exit0 без JSON не является успехом. Нужны явный текущий
World result и `soty.storage-start.v7`, не унаследованный image/container label.
После clean STOP сравни схемы/строки/IDs/ACL и служебные stores, а не только counts;
проверь serving CAS и отсутствие copy writer. Убирай plaintext copy только по
точным owned IDs после завершённого custody/restore proof, без global prune.

## Ресурсы и сборка

Проверь disk/inodes, свободные порты и Docker address pools до build/up.
Если адреса исчерпаны, сначала доказать принадлежность устаревших networks и
освобождать только их. Мало диска — собирать проверенные families последовательно,
не запускать пачку тяжёлых build вслепую. Не удалять действующие данные ради места.

Публичный synthetic acceptance container может работать read-only/cap-drop ALL.
В этом случае root без DAC override не прочитает чужой TemporaryDirectory0700:
запускай fixture от UID/GID владельца, не ослабляй права private дерева сервера.
Сообщение Node «Could not find» может скрывать EACCES, а не отсутствующий source.

## Исходники и зависимости

Версию выбирать по реальному поведению, Git history, сборке и сохранности данных,
не по одному mtime/названию «final». При Windows→Linux переносе проверять Git
object bytes/line endings: CRLF tar рабочего дерева может сделать весь сервер
dirty относительно LF commit. Нормализация допустима лишь в собственной чистой
release-копии после проверки, не reset пользовательского dirty checkout.

GitHub push отказал из-за workflow scope — проверь, нужен ли legacy workflow
вообще. Не расширяй credential scope и не активируй старый production CD ради
переноса исходников. Не исключай обязательный действующий CI без основания.

Если сервер не может забрать приватный GitHub repo, не переноси на него личный
GitHub token/SSH key ради deploy. Допустим Git bundle уже опубликованного
проверенного commit через имеющийся SSH transport: закрепи base/target SHA,
проверь hash файла, `git bundle verify`, точный FETCH_HEAD и ff-only integration
при чистых tracked files. Bundle сохраняет Git blobs без Windows CRLF archive
подмены. Сохрани transport receipt; приложение, volumes и прочие сервисы не
пересоздавай из-за отказа доставки кода.

Optional integrations проверять отдельно: доступность DNS, HTTP/WS, действующие
proxy, реальный payload и freshness. Один health зелёный не доказывает все источники.
Функции, отключённые текущим scope, должны оставаться отключёнными.

## Квитанции

При восстановлении проверь связанное служебное состояние отдельно от основной
базы. Например, read marker из более новой копии не должен объявить прочитанным
новое сообщение с повторно использованным sequence: нужна совпавшая identity
записи или проверенная инвалидация. Неизвестное состояние показывается как unknown,
а не ложный ноль; последующее подтверждённое действие должно уметь его восстановить.
Восстановление UI/route не означает откат пользовательских данных.

Оставь source/image pins, config CAS/active equality, encrypted backup + точный
restore scope, реальные browser/data/access проверки и совместимый rollback.
Сохрани RED и GREEN отдельно. Missing fixture, skipped acceptance и stale legacy
suite — отдельные статусы, не «всё прошло». Полный release gate определяется
`docs/deployment/app-release-readiness.md`, а не наличием красивого отчёта.
