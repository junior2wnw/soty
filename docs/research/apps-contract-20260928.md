# Приложение как сота: исследование и контракт

Дата: 28 сентября 2026. Область: этапы 3 и 4 общего плана. Это исследование и архитектурный контракт, не утверждение о выпуске на рабочий сайт. Точные реализованные сигнатуры находятся в [README модуля](../../modules/apps/README.md), результаты — в [локальной приёмке](apps-acceptance-20260928.md).

## Вывод

Работающий web-проект на доверенном устройстве становится именованным приложением с постоянным ID. Посетитель открывает его через шлюз Сот. Коннектор устанавливает исходящий канал и обращается только к конкретному loopback-порту, который выбрал владелец. Аккаунт, права группы и доступ к компьютеру остаются разными сущностями.

## Что подтверждено текущим кодом

- `scripts/soty-connector.mjs` получает durable jobs через long-poll; результат OpenCode содержит текст, код завершения и session ID. Готового транспорта HTTP-приложений нет.
- `server/connector-store.js` хранит digest installation token и проверяет связку link/device/connector. Работающий коннектор может быть удостоверен без передачи ключа браузеру.
- Connect подтверждает запрос подписью установки, но её `dev_<public-key-hash>` не совпадает с `device-<uuid>` коннектора. Знание Link ID не доказывает владение машиной.
- `scripts/build-agent-release.mjs` встраивает локальные модули без вложенных import. Runtime-модуль должен получать операции ввода-вывода зависимостями.

## Первичные источники и решения

1. [MDN: same-origin policy](https://developer.mozilla.org/en-US/docs/Web/Security/Defenses/Same-origin_policy) определяет origin через протокол, имя хоста и порт; разные пути не изолируют приложения. Решение: каждому приложению собственный origin; API аккаунта и приложение не размещаются под разными путями одного origin.
2. [MDN: iframe](https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/iframe) объясняет sandbox и риск сочетания scripts/same-origin для содержимого на origin родителя. Решение: отдельный origin плюс `sandbox="allow-scripts allow-forms allow-same-origin"`, без top-navigation/popups/доступа к устройствам; загружать только открытое приложение, а не скрытые iframe всех сот.
3. [MDN: frame-ancestors](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Security-Policy/frame-ancestors) задаёт разрешённых родителей документа. Решение: шлюз разрешает только origin интерфейса Сот; приложение не наследует ключи и browser storage родителя.
4. [OWASP: SSRF prevention](https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html) рекомендует ограничивать назначения и не принимать произвольные полные URL; автоматические перенаправления могут обойти проверку. Решение: регистрация принимает целочисленный порт, коннектор сам строит адрес `127.0.0.1`; управляющий порт коннектора и служебные порты запрещены; redirects не исполняются коннектором.
5. [OWASP: WebSocket security](https://cheatsheetseries.owasp.org/cheatsheets/WebSocket_Security_Cheat_Sheet.html) требует проверки Origin, аутентификации и актуальных прав, включая закрытие существующих соединений. Решение: origin-проверка каждого browser handshake, привязанная к приложению сессия, отдельный реестр активных потоков и немедленная отмена при отзыве.
6. [Node.js: streams](https://nodejs.org/api/stream.html) описывает backpressure и `pipeline`/AbortSignal. Решение: ограниченные чанки, лимит незавершённых потоков, кредитное управление между шлюзом и коннектором, прекращение чтения при занятом получателе, закрытие обоих концов при отмене.
7. [OpenCode: CLI](https://opencode.ai/docs/cli/) документирует JSON events для `run`. Решение: сохранить имеющийся CLI-транспорт; результат приложения определяется валидированным manifest-файлом в разрешённой рабочей папке, а не регулярным выражением по ответу LLM.
8. [MDN: partitioned cookies](https://developer.mozilla.org/en-US/docs/Web/Privacy/Guides/Third-party_cookies/Partitioned_cookies) описывает разделение cookie по приложению и top-level site. Браузерная проверка подтвердила необходимость этого контракта: app-сессия использует `__Host`, `HttpOnly`, `Secure`, `SameSite=None`, `Partitioned`, поэтому iframe не зависит от включённых обычных third-party cookies.

## Модель данных

```ts
type VerifiedActor = { accountId: string; deviceId: string };
type ConnectorIdentity = { linkId: string; deviceId: string; connectorId: string };
type LocalApp = {
  id: string; // app- + 32 lowercase hex digits, immutable and hostname-safe
  ownerAccountId: string;
  connector: ConnectorIdentity;
  name: string; // 1..64 characters
  port: number; // explicit loopback port, never arbitrary URL
  entryPath: string; // same-origin absolute path, default '/'
  grants: { accountIds: string[]; groupIds: string[] };
  state: 'enabled' | 'revoked';
  revision: number;
  createdAt: number;
  updatedAt: number;
  sourceJobId?: string;
};
type AppObservation = {
  appId: string;
  state: 'ready' | 'stopped';
  observedAt: number;
};
```

`offline` вычисляется по отсутствию аутентифицированного канала устройства. `stopped` означает подтверждённый отказ/недоступность выбранного сервиса. Неизвестное состояние не называется готовым. `revoked` имеет приоритет над всеми наблюдениями. Порт/connector/link не возвращаются гостю; ему доступны имя, хозяин и состояние.

## Контрольный API

App service не принимает `accountId` из тела как доказательство личности. Root вызывает методы только после проверки Connect proof:

```ts
createAppsService({
  dataDir,
  appOriginTemplate, // https://{appId}.apps.example.org; explicit config
  shellOrigins,
  authorizeActor(actor), // is account installation still active?
  ownsDevice(actor, connector), // explicit verified ownership mapping
  isGroupMember(accountId, groupId),
  authenticateConnector({linkId, deviceId, connectorId, token}),
});

service.list(actor, { deviceId?, groupId? });
service.register(actor, { connector, name, port, entryPath?, grants? });
service.update(actor, { appId, name?, grants?, enabled? });
service.launch(actor, { appId }); // returns a one-use launch URL and expiry
service.revoke(actor, { appId }); // disable app, invalidate sessions and streams
service.invalidateAccess({ accountId?, groupId?, deviceId? });
service.handleRequest(req, res); // app-host content and launch bootstrap only
service.handleUpgrade(req, socket, head); // connector channel or app WebSocket
service.close();
```

Владелец может зарегистрировать и изменить приложение только после `ownsDevice`. Член группы может открывать приложение только пока `isGroupMember` возвращает true. Публикация в каталоге и разрешение пользоваться приложением — отдельные операции; начальное приложение приватное.

Новая привязка коннектора, если готового проверенного owner mapping ещё нет: runtime выдаёт одноразовый короткоживущий случайный секрет через локальный origin-ограниченный endpoint; Connect-подписанный запрос обменяет его на account ownership. Секрет не сохраняется в общих логах, не входит в URL, не выдаётся группе. Привязка не может быть подтверждена одним Link ID.

## Изоляция и сессия приложения

1. Подписанный запрос `launch` проверяет текущую установку, аккаунт, устройство-владельца и grants.
2. Одноразовый ticket (30 секунд) связан с app/account/Connect device/revision и попадает только в fragment bootstrap URL.
3. Bootstrap на app origin отправляет ticket POST-запросом, стирает fragment и получает host-only HttpOnly partitioned cookie, имеющую короткий срок действия и доступ только к одному приложению.
4. Каждый запрос и WebSocket handshake повторно проверяет grants и actor validity. Проверяется точный app Host и browser Origin; путь не может переключить назначение на другой host/port.
5. Запросы к local app не содержат connector token, Connect proof, app session cookie или LLM credential. Приложение получает только явно разрешённые HTTP headers.
6. CSP и Permissions Policy шлюза ограничивают родителя/возможности; никакого CORS доступа к control plane с app origin. `Set-Cookie` локального приложения требует отдельного bounded cookie contract или запрещается в первой версии.
7. Revoke закрывает активные HTTP/WS streams и делает старые tickets/cookies бесполезными. Уже прочитанное пользователем содержание невозможно отозвать; новых данных после revoke быть не должно.

В production требуются wildcard DNS/TLS и настроенный app-origin template. Отсутствие этого адреса возвращает явное `apps-origin-not-configured`, а не размещает недоверенный код на origin Сот. Для разработки допускается HTTP только на loopback/`.localhost`.

## Транспорт v1

Отдельный исходящий WebSocket runtime → server, удостоверенный installation token (first-frame auth, не query parameter). Каждый app request имеет random stream ID, только один зарегистрированный app ID и проверенный path. Сервер передаёт runtime запись назначения, разрешённую owner mapping. Runtime никогда не принимает полный URL от браузера.

HTTP: request head → ограниченные body chunks → end; response head → ограниченные body chunks → end. Для WS после успешного local handshake передаются text/binary messages с ограничением размера. Во всех направлениях канал поддерживает cancel/error и подтверждение принятого чанка; у отправителя не более небольшого фиксированного количества неподтверждённых чанков. Нет неограниченного накопления в памяти и записи HTTP-трафика в durable job store.

Пределы первой версии: до 32 потоков на коннектор, 48 KiB на чанк, 8 MiB request body, 64 MiB response body, 1 MiB на WebSocket message, 30 секунд ожидания response head, ограниченное время неактивности. Размер WebSocket message учитывает фрагментацию; compression extensions не согласуются. Эти пределы выдаются как диагностируемые ошибки, а не молчаливое обрезание ответа.

Совместимость первой версии: обычные HTML/CSS/JS, same-origin fetch/form, HTTP assets и WebSocket на origin приложения. URL должны быть относительными или строиться от location origin. Не обещаем поддержку захардкоженного localhost, service worker, внешнего OAuth, произвольных redirect hosts и доступа к устройствам браузера. Порт conнектора и loopback admin endpoints не публикуются.

## Связанный результат OpenCode

Задача создания приложения явно включает контракт `.soty/app.json` в выбранном workspace:

```json
{
  "schema": "soty.local-app.v1",
  "name": "Покупки",
  "port": 3000,
  "entryPath": "/"
}
```

После успешного задания runtime проверяет реальный путь workspace и manifest, отсутствие выхода через symlink/junction, размер, точную schema/поля, разрешённый порт и отвечает ли локальный сервис. Он добавляет нормализованное предложение в результат задания. Это ещё не публикация и не выдача доступа. UI показывает готовую соту и действие «Добавить»; добавление использует тот же register API. Доступ исходно только владельцу, расширение — отдельным действием.

Manifest не содержит command, URL, credentials, произвольные заголовки или grants. Запуск проекта остаётся частью явной OpenCode-задачи; сервер не исполняет текст manifest как shell-код. Переиспользуется тот же app ID после обновления существующего проекта.

## Проверка сдачи

- Отдельный loopback sample app со статикой, JSON POST и WebSocket; проходит именно реальный runtime→gateway канал.
- Владелец и второй Connect principal читают/меняют общую запись. Третий principal не получает launch ticket.
- После revoke уже открытый WebSocket второго участника закрывается, его следующий HTTP request запрещён, старый ticket не применяется повторно.
- Удаление из группы закрывает доступ без отдельного ручного отзыва каждого приложения.
- Offline/stopped/reconnect проверяются остановкой runtime и отдельно сервиса; после перезапуска запись приложения сохраняется.
- Slow reader, oversized body/message, unexpected frame, malformed path, encoded host switch, redirect, origin mismatch и forged owner не открывают дополнительный маршрут.
- Manifest вне roots, junction/symlink наружу, слишком большой/неизвестный формат, недоступный порт и неуспешная задача не превращаются в готовое приложение.
- Browser QA отдельно проверяет загрузку iframe, assets, формы и cookie policy в двух сессиях; Node transport test этого не заменяет.
