# Проверка приложений Сот — 28 сентября 2026

Область: модуль приложений и связанный manifest-результат. Итог ниже подтверждает локальный модуль и изолированный браузерный стенд; интегрированный UI/рабочий сервер проверяются отдельно корневым агентом.

## Выполненные проверки

`node --test modules/apps/test/*.test.mjs`: одиннадцать проверок, все проходят.

- Действительно отдельный HTTP-проект на loopback; runtime подключается исходящим WebSocket, gateway получает HTML/JS и POST. Два разных account/device principals меняют и читают одни данные; третий не получает launch.
- Настоящий WebSocket проходит через тот же app grant и канал. Исключение участника закрывает существующее соединение, новый HTTP-запрос со старой cookie запрещён. Владелец сохраняет доступ.
- Отзыв приложения закрывает текущий поток HTTP. Отзыв runtime token закрывает каналы после bounded re-auth. Понижение владельца приложения в группе снимает групповой доступ.
- Бинарный ответ 4 MiB проходит без искажения. Регулятор WebSocket проверяет маски, RSV, control frames и суммарный размер фрагментированного сообщения.
- Повторное использование ticket, чужой Origin, чужой владелец устройства, порт самого коннектора, host-switch path и redirect к другому адресу запрещены.
- Остановленное приложение отличается от отключённого коннектора. Registry/grants/owner binding сохраняются после переоткрытия SQLite.
- Повтор регистрации после потерянного ответа сохраняет app ID. Не совпадающие параметры того же порта отвергаются.
- Claim secret не создаётся при подключении. Явное локальное действие ждёт ACK digest перед возвратом кода; использованный код не даёт привязать устройство другому аккаунту.
- Manifest проверяется по реальному пути и размеру; выход через junction, недопустимый порт, лишний executable field и устаревший файл не становятся предложением приложения.

## Настоящий браузер

Проверено через Codex In-app Browser в локальном стенде `http://localhost:5306`, без доступа к боевым аккаунтам, ключам или установленному коннектору. Приложение загружается на отдельном `http://app-<id>.localhost:5306` в sandbox iframe.

1. Владелец открыл «Покупки»; загрузились отдельные CSS/JS и статус «Общий список · подключено» от WebSocket.
2. Через форму добавлено «Яблоки для семьи».
3. Другой account principal открыл приложение, увидел эту запись и отметил её выполненной.
4. После отзыва членства браузер сразу показал «Соединение закрыто», поля и кнопки sample-приложения стали неактивны.
5. Открытие посторонним показало `apps_access_denied`.

Браузерная проверка обнаружила реальный дефект первоначального `SameSite=Lax`: localhost и app.localhost являются разными site, cookie не отправлялась в iframe. Решение использует [стандарт CHIPS](https://developer.mozilla.org/en-US/docs/Web/Privacy/Guides/Third-party_cookies/Partitioned_cookies): `__Host` + `HttpOnly; Secure; SameSite=None; Partitioned`. Повторный полный сценарий прошёл. Это именно браузерная проверка хранения/отправки cookie, а не подстановка Cookie заголовка Node-тестом. После фиксации результата стенд и временная вкладка остановлены.

## Полный runtime и реальный Connect

`modules/apps/test/connector-runtime.test.mjs` запускает **сам `scripts/soty-connector.mjs`** отдельным дочерним процессом, а не подменённую реализацию runtime. У него временные data/workspace, отдельный loopback-порт, `managed=false`, `autoUpdate=0`, минимальная среда без production credentials. Установленная служба пользователя не изменяется.

Проверка использует настоящий `createHttpApp` со всеми подключёнными Connect/World/Apps extensions. Три независимых signing/encryption key pairs создают три аккаунта через HTTP Connect proof. Маршрут проходит полностью: регистрация installation → аутентифицированный app channel → локальный POST `/apps/claim` → подписанные `apps.claim`/`apps.register` → одноразовый launch → gateway → отдельный web-проект → HTTP, POST и WebSocket. Отзыв account grant через подписанный `apps.update` немедленно закрывает существующий WebSocket и запрещает следующий HTTP. Отсутствующий Origin и даже другой loopback Origin локального claim получают 403. Shell `X-Frame-Options: DENY` не попадает в ответ приложения.

Вторая проверка проводит типизированный `appProposal` через действующий connector job store: создание agent job с `output='local-app'`, lease, завершение, read и повторное открытие durable store. Inference при этом не вызывается; тест удостоверяет транспорт и сохранение результата, а не выдает себя за работу модели.

## Границы подтверждения

- Browser principal switch в стенде последовательный; одновременные независимые HTTP/WS-сессии проверены транспортным тестом, настоящий Connect proof — интеграционным. Два реальных browser profiles в итоговом UI относятся к общей приёмке.
- Проверен установленный Chromium/IAB. Firefox/Safari, мобильные реальные устройства, wildcard DNS/TLS и внешний сетевой relay в production не объявляются проверенными.
- Manifest extractor проверяет настоящий workspace и отвечающий проект. Фактический запуск OpenCode через production inference не симулируется этим тестом и не объявляется выполненным.
# Дополнение: запуск разработки и PWA

Проверено 2026-09-28 после интеграции World и local apps:

- `node --test scripts/dev.test.mjs`: 2/2 PASS, последний запуск 7.95 с. Настоящие Vite + `server/index.js`, signed Connect bootstrap, World RPC, HTTP, WebSocket с исходным Origin; чужой Origin отклонён. Отдельный app host дошёл до gateway и не получил shell `X-Frame-Options: DENY`. Каталог `var` не отдаётся Vite. Оба порта освобождены после остановки; занятый API-порт завершает запуск ошибкой до открытия UI.
- Живой Chrome на отдельном `http://127.0.0.1:5390`: новый профиль, общий мир и «Мои соты» загрузились; в консоли нет JS warning/error. Временные вкладки и dev-процессы 5390/5391 закрыты. Данные проверки изолированы в `var/dev-qa/data`.
- `npm run typecheck`: PASS. Проверочная Vite 7.3.2 build: PASS. Production bind по умолчанию сохранён; dev supervisor использует HOST=127.0.0.1 и IPC для готовности/остановки API.
- Подтверждён и исправлен дефект CSS proxy chunk: старый `classicAssets` включал удалённый `/assets/style-IihVFDn-.js`. Генерация теперь выполняется после CSS cleanup. `writeBundle` проверяет все precache URL на диске при каждой сборке.
- Проверен итоговый output `output/world-validation-20260928/dist`: SW revision `soty-online-221be907ba272bf25383`, SHA-256 `f2833da6da9f6d9253b5e1d7780d2c88d7536cc962390ab2d599da179ad5ea13`; World 5 файлов, classic 6, shell 3, всего 13 уникальных URL, отсутствующих 0. Последующие UI-сборки меняют revision и автоматически повторяют проверку.
- Документ запуска: `docs/development-world.md`. Описаны отдельный dev-коннектор, точный Origin, sample app, production DNS/TLS/reverse proxy и актуальная граница совместимости CHIPS. Реальный production DNS/TLS не изменялся и не заявляется проверенным развёртыванием.
