# Детали обращения в standard3

Подтверждённый дефект: разрешённое изображение размером 1048576 байт вместе с
допустимой перепиской размером 196608 байт после JSON/base64 даёт HTTP-ответ
1595055 байт. Лимит standard1/2 — 1500000; корректное обращение невозможно
прочитать целиком. Новая версия исправляет только этот бюджет.

`STANDARD_SELECTED_SOURCE_V3` сохраняет orchestration/auth-контракт standard2.
Только GET `/api/embed/feedback/ticket` получает 2097152 байт ответа. Root
использует отдельный точный compiled pin; BFF выбирает тот же закрытый контракт
для своего router и Native runtime. Native input, остальные outputs, upload,
list, mutation ACK, media, conversation, proof и TTL сохраняют прежние границы.
Для Native runtime без BFF требуется constructor option
`{sourceProfile: STANDARD_SELECTED_SOURCE_V3}`; числовой пользовательский лимит
не принимается.

Browser caller для этого контракта создаёт
`createSourceFeedbackClient({profileVersion: 3})`. Одинаковый бюджет применяется
к чтению потока и strict JSON parser; его общие defaults не изменены.
Прежние browser callers по умолчанию используют версию1. Версии1/2 и их digest
остаются прежними. Обновление reviewed Source profile выполняется явно,
с текущими согласиями и правами; старое согласие не расширяет доступ.

Проверка `feedback-read-budget.test.mjs` использует настоящий валидатор PNG,
закрытые runtime/browser boundaries, Root route adapter и maintained Root
OIDC/BFF с Native SQLite. Допустимые данные читаются после перезапуска; отзыв
Native доступа блокирует чтение. Синтетическая переписка готовится только
в новом test fixture и сравнивается в фактическом SQLite порядке.

Это узкое исправление общего SDK. Оно не включает приложения, не мигрирует
Native хранилища и не подтверждает готовность production или внешнего агента.
