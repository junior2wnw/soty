# Приватная поддержка ядра Сот

По умолчанию выключена. Получатель не выбирается из браузера, текущего
аккаунта, администратора или истории. Настоящий получатель и включение
требуют отдельной явной настройки владельцем сервера.

Host option `createHttpApp(..., { coreFeedback: { enabled, supportOwnerAccountId } })`
или переменные `SOTY_CORE_FEEDBACK_ENABLED=true` и
`SOTY_CORE_FEEDBACK_OWNER_ACCOUNT_ID` задают существующий Connect account.
Формат конфигурации проверяется до открытия хранилищ; существование получателя
проверяется по локальной Connect SQLite без выдачи его идентификатора клиенту.
Эта реализация не меняет настоящую конфигурацию и не назначает получателя.

Signed Connect RPC: `core.feedback.context/submit/list/get/reply/status/accept`.
Аргументы соответствуют существующим feedback operations, кроме отсутствующего
`appId`. `owner`, `recipient`, `domainId`, `path` и другие полномочия из запроса
не принимаются. Installation/ticket IDs — указатели, не права доступа.
Отключённый context возвращает `readiness: unavailable`, без открытия feedback DB;
отправки запрещены. В Help нет неработающей кнопки для выключенного или старого
backend. При подтверждённой готовности Help открывает существующую панель
с историей, ответами, выбранными вложениями и явным подтверждением результата.

## Хранение и совместимость

Отдельный файл сейчас выпал бы из разрешённого snapshot/independent probe.
Поэтому core facade использует канонический `feedback/feedback.sqlite` и тот же
ticket engine. DDL, user_version=1, lineage, metadata, guards и reader declarations
не изменены. Внутренний `app_id=soty.core.feedback` отделяет очередь; наружу он
не выдаётся и не является зарегистрированным приложением. Apps validator
`modules/apps/server/protocol.mjs` принимает только `app-` +32hex, поэтому этот
scope не может использоваться обычными Apps feedback endpoints.

Первое обращение к ready context закрепляет получателя существующей immutable
строкой `feedback_installations`. Проверка владельца выполняется при запуске и
внутри каждой транзакции после `BEGIN IMMEDIATE`; уже запущенный другой instance
не сможет открыть вторую core очередь на другого получателя. Смена требует
отдельной проверенной миграции, которую этот модуль не выполняет.

Исходный reporter читает только свои tickets, configured support account — очередь
Сот. Текущий active device проверяется до и после SQL-эффекта. Replies и статусы
имеют expectedRevision, записи — requestId и точный replay; решение подтверждает
reporter. Core не вызывает app registrations и не ослабляет Apps authority.

Общий файл сохраняет общий предел базы 64MiB и существующие лимиты очереди:
core и Apps делят бюджет/SQL writer. Переполнение отклоняет новые эффекты,
не удаляя историю. Разделение физических бюджетов потребует отдельного изменения
storage readers/backup, а не скрытого нового файла. Откат на прежний backend
сохраняет данные feedback1; core UI/operations там недоступны до возврата поддержки.

Локальный core draft имеет отдельный ключ `soty.core-feedback.v1:<account>`.
App draft keys и байты прежнего формата не меняются. Panel включён в
flush/dirty/update/navigation/account-dispose guards; same-account refresh
сохраняет открытое окно. Вложения выбирает человек; ASR, агенты, почта,
автоматический capture, публичные отзывы и внешние уведомления не включаются.

## Проверки

`server/test/core-feedback-http.test.mjs`: настоящий signed HTTP/SDK + disposable
SQLite, lost ACK/restart/replay/history, reporter isolation, support reply/status,
readback/accept, unsigned denial, revoked enrolled device, owner-change/competing
instance, Apps namespace denial, actual independent probe + offline snapshot и
signed readback из восстановленной копии.

`src/world/core-feedback.test.mjs`: actual reused panel + World methods + signed
HTTP/SDK/SQLite с контролируемыми DOM/storage/Web Locks ports: draft restore,
блокировка навигации/обновления/закрытия при отказе storage, сохранение окна после
refresh, обе роли в UI, late ACK/dispose/account ABA и повтор того же intent.
`dialog-focus.test.mjs`: Help OFF/old server/foreign scope/late account response.
Эти controller-проверки не являются визуальной или microphone-приёмкой браузера.
