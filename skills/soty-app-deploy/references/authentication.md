# Аутентификация приложения

Сначала установи действующий app/client/Source profile, issuer, capabilities и
роли. Не меняй рабочий вход ради ещё не квалифицированного способа. При наличии
checkout дополнительные operator/protocol gates находятся в
`docs/deployment/app-authentication.md`; публичная инструкция не выдаёт доступ к
его приватной конфигурации. Наличие кода не означает включённый production issuer.

## Выбрать существующий контракт

| Контур | Подтверждает | Не заменяет |
|---|---|---|
| Gateway / Native ingress | Можно открыть конкретный app/source | Human identity и права на данные внутри приложения |
| Human OIDC `/human-identity` + серверный Source RP/BFF | Проверенную identity и собственную сессию приложения | Native tenant/role ACL и согласие на продление |
| SourceSSO по approved Source profile | Поддерживаемый адаптер получает проверенный Root-контекст точного приложения, Source, ресурса и экземпляра | Доступ к данным, отзыв и независимый standalone вход |
| Agent OAuth / MCP | Права actor на ресурс, операции, срок и бюджет | Human identity, другие MCP и Native/project grants |

SourceSSO применим только при действующем approved descriptor/profile, адаптере и
квалифицированном закрытом/server-to-server транспорте. Не выводи его из названия
SSO, пути `/embed`, ingress cookie или заголовка `user_id`. Если адаптера нет,
используй доступный квалифицированный вход либо сохрани точный статус зависимости.
Новый Native profile из спайка не объявляй действующим контрактом.

## Human OIDC / BFF

1. Проверенный issuer, approved confidential client, точный HTTPS callback.
   Секреты и ключи — только на сервере, резервные копии зашифрованы.
2. Maintained `modules/source-rp`: Code + S256 PKCE, state/nonce, подпись,
   issuer/audience/expiry и свежий userinfo. Connect approval + human consent.
3. Собственная Secure/HttpOnly app session и native ACL. Стабильная связь
   `(iss,sub)` с local account; старый аккаунт привязывать только двусторонним
   proof, не по email/имени и не присваиванием owner ID.
4. Basic300 по умолчанию. Finite renewal требует reviewed host/client profile,
   выбора человека и durable encrypted Source/CAS. Истечение не обходить
   бесконечным refresh. Отозванные устройства/client/grant дают отказ.
   Запомненное устройство и конечное продление — разные capabilities; UI показывает
   только фактически включённый срок. Постоянный профиль Сот не означает постоянную
   сессию каждого приложения.
5. Два пользователя → отдельные данные/роли. Один человек → два приложения с
   одним issuer/sub и отдельными sessions. Write → полный reload → прежние IDs.

`modules/human-identity/examples/bff.mjs` — **test fixture**, не production SDK:
in-memory sessions, loopback cookies. Production API — Source RP плюс собственные
storage/migrations/native guards приложения. Обязательная схема создаётся до
optional Telegram/agent worker, иначе свежая установка не сможет работать.

Ingress `/_soty/session`/cookie разрешает открыть app/source, но не передаёт
human identity. Private single-owner adapter допустим только за restricted
gate, fixed loopback, current ACL/source pin и own CSRF/Origin. Его local user 1
нельзя переносить на многопользовательский вход.

Agent OAuth `/oauth` и MCP требуют собственных audience/scopes/app grants.
Доступность инструмента не означает разрешение читать или менять данные.
Human OIDC не выдаёт агенту права автоматически; Native ACL проверяется отдельно.

Для краткого guide: `node scripts/soty-app-release.mjs auth-guide --mode human-oidc --output NEW_FILE`
или Root MCP `apps_auth_guide({mode:"human-oidc"})`. Другие режимы — private-owner
и agent. Эти read-only помощники ничего не активируют и не требуют секретов.

Для существующего Python/FastAPI сохраняй Native data model; поддерживаемый
серверный OIDC SDK проверяй теми же настоящими Root protocol gates.
Собственный JWT verifier не нужен. Прежнюю private-owner строку можно сохранить
только для точного исходного Root владельца по закреплённой provenance + новому
human proof, до первого входа. Уже многопользовательская база требует отдельной
двусторонней привязки. Не импортируй токены старого канала в браузер.

Раздели также browser-local layout/preferences и обработай смену/выход во всех
вкладках. Проверь Native API/WS с чужим user_id в payload: identity берётся из
server session. Фильтры/избранное → очистка кеша → открытие из Native DB под
прежним ID. Callback/private API не кешируются, auth code не пишется в access log.

## Завершить вход без лишней страницы

Сохрани исходное намерение и допустимый локальный return target в проверяемом
state до авторизации. При действующем разрешении продолжай сразу к работе.
Первое согласие, отзыв, новый профиль/client version/scopes и нужная проверка
остаются явными решениями человека.

- Script-opened popup после подтверждённого серверного обмена уведомляет
  исходное окно без токенов. Оно заново проверяет собственную сессию; событие
  BroadcastChannel/postMessage само не является proof. Popup можно закрыть,
  если оно действительно открыто скриптом.
- Same-window callback сразу ведёт к разрешённому исходному маршруту. Не
  принимай внешний `return_to` и не оставляй обязательное «Вход подтверждён →
  открыть приложение ещё раз» после уже успешного входа.
- Blocked popup, отсутствие opener/канала или отмена получают понятный fallback,
  без бесконечной петли окон. Полные callback URLs не попадают в логи.

## Несколько профилей и вкладок

Ожидаемый subject/frame связывается доверенным контрактом с исходным ticket/session
или Source context. Его не берут из UI-селектора или произвольного заголовка.
Совпадение текущих Root и Source cookies недостаточно: после полного входа B обе
cookie могут стать B, пока старый iframe ещё показывает A.

Проверь A открыт → B полностью входит → A делает HTTP, долгий WS, reconnect,
refresh и callback. Результат A либо ясный отказ; никогда данные B в окне A.
Смена/выход исключает устаревшие ответы и чужой кэш. Короткий proof на WS handshake
не является бессрочным разрешением всех сообщений. Неподдерживаемый/недействительный
frame binding не превращается в анонимный вход или fallback на другую cookie.
Если действующий контракт этого не обеспечивает, не обещай независимые профили
одного браузера и оставь этот сценарий явно неквалифицированным.

## Пределы сессий

Перед широким выпуском проверь actual issuer budgets, active families,
rotation/retention и storage: Source users, Basic300 и finite24 capacity не равны.
Два тестовых пользователя или повышенный config не доказывают массовую нагрузку.
При registration клиента сохрани keys и профили прежних clients. Если требуется
reload Root — свежая quiescence, encrypted restore proof и same-image/config CAS;
старый rollout из истории не воспроизводится.
