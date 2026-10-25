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
