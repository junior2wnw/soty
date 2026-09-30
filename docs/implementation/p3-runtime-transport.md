# P3-B2 — запуск и непрерывный допуск HTTP/WS

2026-09-30. Основание: [B2/B3 подплан](p3-runtime-transport-plan.md), B1 checkpoint `3ddb7b6`. Статус: локальный transport gate принят; браузерная приёмка B3 и публичный выпуск не выполнены.

## Реализованный контракт

- `apps.launch {appId,domainId?,path?}` выдаёт одноразовый30s ticket для точного app/domain/origin и проверенного локального пути. Без domainId сохраняется canonical-вход; named-only конфигурация требует явного адреса. Новый claim остаётся выключенным до publication update.
- Только fresh branded recheck после полного чтения тела ticket exchange создаёт session. Повтор, другой alias, новые grants/epoch/target и потеря actor не наследуют старое разрешение. Cookie host-only Secure/HttpOnly/Partitioned, абсолютный срок аккаунта1h.
- `GET /_soty/session` проверяет предъявленную cookie без продления и без выдачи новой. Cookie отсутствует → отказ этому служебному endpoint. Новое публичное обращение без cookie может создать anonymous decision только для активного anyone alias. Ошибочная или просроченная предъявленная cookie не подменяется guest.
- Каждый HTTP/WS stream хранит отдельный branded holder. Проверка перед open/chunk/end, до входящего head/data/end/ack, после ожидания ACK и callback записи ответа. Anonymous lease30s можно обновить только пока он действителен; deadline аккаунта не сдвигается. Истечение/отзыв закрывают свой stream, не соседей.
- Membership event перепроверяет допуски сразу. Audit по умолчанию10s перечитывает права, включая изменения другого SQLite writer без event. Пауза event loop может увеличить фактическую задержку; пропущенный expiry никогда не продлевается задним числом. Тестовый параметр accessAuditMs ограничен25..10000ms.
- На connector максимум32 streams, из них public-basis максимум24. Signed public visitor занимает public-квоту; последующее получение membership не повышает основание уже открытого stream. Восемь мест доступны grant-basis.
- Unsafe HTTP и все WS требуют единственный точный Origin; GET/HEAD могут не иметь его. Передаются только разрешённые заголовки; Cookie/Authorization посетителя, upstream Set-Cookie и внешние HTTP redirects не переходят границу. Это браузерная защита, не AI-identity.
- Connector sync строится из текущего RuntimeTarget; old connector v1 не подтверждает revision и не гарантирует неизменность кода. В B2 изменять источник ещё нельзя. C должен согласовать смену target, синхронизацию и наблюдение готовности до открытия нового источника.
- Shell frame-src включает проверенные сохранённые named zones, в том числе при отключённых новых claims. App policy использует точный origin текущего запроса, поэтому named-only вход не зависит от legacy template.

## Проверки интегратора

`app-runtime.test.mjs`: три настоящих сетевых сценария с существующим production connector runtime и отдельным sample app — HTTP/assets/public POST/WS, ошибки Origin/headers/redirect, restrict и private вход; absolute session deadline; CSP настоящего shell без legacy и после отключения новых names. **3/3 PASS**.

Прежние реальные Apps transport tests **9/9 PASS**. Связанный срез Apps/model/ingress **68/68 PASS**. `agent_ecosystem` независимо просмотрел модельные/async границы и повторил publication19/19; это source review, не транспортная приёмка.

Новый отдельный [acceptance harness](p3-runtime-independent.md) прошёл **30/30 PASS**: delayed request ACK, actual response write с удержанным callback, реальные30s head/ACK timeouts, освобождение quota, expiry и второй SQLite writer. Последний root общий прогон на исправленном/frozen коде **335tests / 332pass / 0fail / 3opt-in skip**, typecheck и production build PASS. Логи `output/implementation-20260930/p3-b2-{world,types,build}-root.log`. Политические часы некоторых сценариев управляемы; удержанный callback доказывает конкретную after-await границу, не физическую backpressure или измерение RSS.

Независимая проверка обнаружила настоящий lifecycle defect: после отзыва во время upload сервер рекламировал keep-alive, хотя выход из body iterator разрушал недочитанный IncomingMessage. Следующий GET на reused socket зависал; новый TCP socket работал. Теперь ранний отказ при `!req.readableEnded` заранее снимает keep-alive и посылает `Connection: close`. Regression не переключён на `agent:false`: короткий16-byte и многокусковый upload проверяют следующий запрос обычного Agent. Различие parsed `message.complete` и consumed `readableEnded` сверено с документацией [Node HTTP](https://nodejs.org/api/http.html#messagecomplete) и [Node Streams](https://nodejs.org/download/release/v24.20.0/docs/api/stream.html#readablereadableended); преждевременный выход из async iterator разрушает stream по его стандартному контракту. На локальном Node24.13.1 исправление подтверждено сетью.

## Что этим не доказано

В B3 ещё нужны браузерный bootstrap с подтверждением именно новой cookie-сессии, private direct-link recovery, account-switch и iframe/отдельная вкладка. Успешный POST session не равен принятой браузером cookie. Проекции готовности и настройки публикации расширяются в C; `runtimeReady:false` из B1 ещё не заменён фактической наблюдаемой готовностью.

Нет публичного DNS/TLS-пилота, production rollout, нового Linux candidate/fallback proof, свежего backup/restore, physical mobile/installed PWA приёмки. Полученные ранее байты и выполненные upstream действия не отменяются закрытием доступа.
