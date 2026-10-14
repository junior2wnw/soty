# Source-вызовы агента без переключения открытого приложения

Глобальный агент выбирает установленный Source по точному reference и действующему grant человека. Для Native Root-SSO workspace он получает приватный серверный handle через существующие Apps gateway, коннектор и `/api/embed/session-continue`. Обычная подпись Source ACK и свежий Native current-context обязательны. Никакое окно, карточка приложения, браузерный cookie или новый публичный RPC не создаются.

Backend использует отдельный непрозрачный instance существующего формата. Поэтому выбор Native по умолчанию не изменяет `main` или уже открытое окно. Handle и contextRef добавляются сервером; модель получает обычную внутреннюю schema операции. Root/account/device, текущие app target/profile/policy, Source grant и оригинальные Native ACL/COMMIT fences проверяются существующим транспортом.

Видимое окно и выбранный backend Source сохраняют отдельные снимки. `current_context` и HIVE aliases описывают видимое окно. Перед каждым последующим инструментом или ответом проверяются все Native контексты, использованные этим ходом. Поддельный/чужой handle, смена scope или отзыв уже захваченного grant остаются ошибкой. Только отсутствие или истечение authenticated current-context credential у правильно принадлежащего видимого окна позволяет убрать его UI-инструменты, если другое приложение имеет отдельный действующий context grant и admitted Native Root-SSO profile.

Повторное обращение к уже допущенной записи проверяет её исходную квитанцию. При смене backend сохраняются invocation ID и оригинальный завернутый input, включая contextRef; unknown не превращается в повторный invoke. Приватный backend handle имеет исходный конечный срок (до 300 секунд). После закрытия, истечения или перезапуска Root этот handle не восстанавливается авторитетным DTO; неизвестную запись нельзя автоматически перенести в новую сессию или повторить. Закрытие удаляет только соответствующие backend sessions; существующая UI renewal family сохраняется.

Headless acquisition поддерживает только реально установленный `STANDARD_ROOT_WORKSPACE_SOURCE` (`soty.standard-workspace@2`). Legacy RP/Basic profiles сохраняют существующий selected-window путь и требуют своего подтверждённого входа. Этот код не выдаёт новые grants, не создаёт Native owner link и не мигрирует профиль приложения.

## Проверка

Обычные Root/Apps/Source проверки выполняются без внешнего Native checkout. Дополнительный интеграционный gate получает существующий OTsov явно:

```powershell
$env:SOTY_OTSOV_NATIVE_FIXTURE_ROOT='D:\otsov'
node --test --test-concurrency=1 server/integration-test/global-otsov-personal-agent.test.mjs
```

Gate поднимает настоящий Root, подписанного synthetic человека, установленный HTTP/WS коннектор, этот SDK и maintained OTsov `ProjectStore`/Native consumer. Все базы, ключи, grants и provider replies временные; Live конфигурация и платная модель не читаются и не вызываются. Проверены: создание с общего поля; сохранение другого видимого Native контекста; отдельный действующий Source при expired visible grant; original receipt после lost ACK и смены backend; foreign/revoke denial; wrong reference и expired target grant. Сохранение проекта подтверждает переносимые исходники и Native COMMIT, а не публикацию или запуск созданного приложения.
