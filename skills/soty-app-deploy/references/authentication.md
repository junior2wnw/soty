# Аутентификация приложения

Для входа разных людей — существующий Human OIDC `/human-identity` и серверный
Source RP/BFF. Сначала открой `docs/deployment/app-authentication.md` в найденном
актуальном репозитории Сот. Там точный private configuration entrypoint, шаги,
API и реальные protocol gates. Наличие кода не означает включённый production issuer.

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
