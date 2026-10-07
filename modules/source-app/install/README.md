# Подключить обычное приложение

Из папки проекта создайте заявку:

```text
soty-source author --title "Мой проект"
```

Команда определяет framework по package.json, создаёт **только** публичный `.soty/author.json` существующего U1 title-only schema и не запускает scripts. Старые `.soty/app.json`, данные и исходники сохраняются. Заявка ничего не исполняет и не получает Native права.

Дальше в проверенном контексте выбирают Source/доступ, подтверждают предлагаемые вход/обратную связь/действия, выполняют проверку подключения и получают reviewable draft. Этот пакет поставляет исполнимый Source/CLI, а не весь self-service wizard/публикацию. Root registration/admission/client/private connector binding проверяются отдельно; metadata не означает Ready. Обычное приложение может сохранять собственный вход.

## Установка оператором

1. Оператор берёт проверенный packet/пины и одобренный existing installed connector с Std2. В `operator.template.json` заменяет placeholders **проверенными значениями допуска**, не пожеланиями автора. До этого template намеренно не принимается loader. Приватный `/data/install/operator.json` содержит exact reviewed Source profile/client/resource, локальный connectorPort, listener127.0.0.1:port и `/data/native` SQLite directory. Секреты — отдельные files `/data/install/secrets/{client,transport,cipher}.key`, не manifest/editor/.env/arguments. Transport/cipher —32 случайных байта в canonical base64url43; client secret предоставляет существующий reviewed RP client. На Linux config/key files600, secret/data dirs700,UID1000.
2. Для **нового** Source: `soty-source init --config /data/install/operator.json`. Создаётся Native format3 и один выбранный пустой ресурс, **без principal/grant**. Own Native guest/linked policies defaultfalse. Опциональный новый пустой guest создаёт первого Native principal только после fresh actual Root/OIDC и явного Source consent по отдельно одобренной Source policy. Это не доказательство владения старым приложением/tenant, не приглашение произвольных участников и не Root-owner-all-data. Existing Native account подключается с двумя независимыми proof.
3. `soty-source check --config /data/install/operator.json`, затем `reader` и `serve` с тем же `--config`. `reader` доSTART независимо проверяет literal format1/2/3 и FK. Root показывает app transport, затем реальный Native login/selected ACL. Проверяют реальное открытие, Source current query/write/receipt/feedback, revoke/restart и TLS callback. Одобрение/допуск и release отделены от локальной установки. При defaultfalse guest policy собственный Native login/права должен предоставить Source; пустая база сама не открывает owner/support доступ.

Source.cfg не принимает caller identity,grants,owner,key/URL/commands из author manifest/body. Config load выдаёт opaque host handle, JSON.stringify(handle) не содержит конфигурации/секретов. Diagnostic CLI возвращает safe IDs/status, `connected:false`; `jobsReady:false` и `longReady:false` остаются честными. `allowLinkedLogin` только Source-native current login policy, Basic300 не возрождается/не становится Long. Source owner/support независим от Root app owner.

## Какой адрес видит человек

`profile.nativeOrigin` — браузерный адрес Native consent: reviewed публичный HTTPS-домен или explicit same-machine loopback. `listener` — **локальный HTTP port процесса**, к которому обращается trusted installed broker. `connectorPort` — собственный local authority IPC; это не браузерная ссылка. На SSH Dev `localhost` сервера не является `localhost` удалённого человека.

Для remote author нужен отдельный достижимый Native HTTPS entry, отличный от Root embed hostname. `soty-source native-portal --config ...` выдаёт fixed Caddy entry: только GET `/soty/connect` и POST `/soty/authorize`, остальные пути404. Proxy сохраняет exact Host выбранного Native origin; Origin/CSRF/RootMAC/actual OIDC проверяются Source. Forwarded headers не дают identity. HTTPS callback остаётся на approved Root embed `/api/embed/callback`; Root sandbox/COOP не ослабляются. DNS/TLS/достижимость со стороннего браузера — отдельный gate; generated portal не является подтверждением.

Derived Source image использует pinned whole-green Root8fd исключительно как Node/dependency base. Собственный ENTRYPOINT запускает `/app/source-app/install.mjs`; Native SQLite и private config находятся в отдельном named `/data` volume. На Linux Source **trusted BFF only** может использовать approved host-network, чтобы existing loopback connector IPC оставался доступным; процесс слушает только127.0.0.1. Это не разрешение worker использовать host-network: processor остаётся no-network/no-socket и defaultoff.

`compose.template.yaml` — операторская спецификация одного Source, с measured immutable image digest и explicit named volume вместо anonymous `/data`. Она не запускается CLI автора и не создаёт connector/client/grants. На другом host/Root server without installed connector тот же template не доказывает transport. Build выполняют по exact canonical Git packet `Dockerfile`; после build отдельно измеряют image/Reader3 и проверяют compatible baseline BEFORE first format3 write. Публичный packet не включает operator.json, ключи, cookies или Native данные.

## Данные и откат

ДоimageSTART нужен независимый Reader3. Старый Reader2 отвергает Native3 BEFORESTART; готовая compatible Reader3 baseline нужна до3write. При неподдерживаемом schema/key/Native grant Source отказывает, не открывает legacy bypass. Current/compatible Source image/cold должны сохранить все Native IDs/receipts/FKs/encrypted session bytes и private config/key equality.

Backup полного stopped Source volume/config/secrets только encrypted. Для Root-managed Docker volume уже существует guarded `deploy/connect/backup.mjs` +independent restore/sink: actual STOP/no other RW mounts/offline metadata/RSA3072-AES-GCM/tar bounds. Source packet physical-cold fixture ещё отдельный review/RUN; local temp/restart не считается power-loss/physical-volume acceptance. Restore старого Basic/Root ref не создаёт разрешений: нужен новый явный вход с current Native authority.

Нет внедрения finite24h consumer, Source model processing,managed public reviews,remote broker/self-service production publish в этом пакете. Клиент feedback,source-native receipts и generic fixed query/invoke уже реальные; отсутствующие Source permissions/features остаются not-ready.
