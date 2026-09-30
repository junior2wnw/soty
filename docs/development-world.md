# Разработка и запуск Сот

## Локальная разработка

Нужен Node.js **24.13.1 или новее** и установленные зависимости проекта. После `pnpm install` выполните одну команду:

```powershell
npm run dev
```

Откройте **http://127.0.0.1:5173**. Команда запускает Vite с обновлением интерфейса без перезагрузки и настоящий `server/index.js`. API слушает `127.0.0.1:5174`; `/api/*`, `/ws/*`, `/health` и `/ready` доступны через адрес интерфейса. Заголовки Host и Origin сохраняются: проверка подписей Connect и происхождения WebSocket работает и в разработке.

Данные разработки сохраняются в `var/dev/data`, отдельно от `data` рабочего сервера. Ctrl+C завершает оба процесса; занятый порт вызывает понятную ошибку, а не незаметный переход на другой адрес. Доступ по сети по умолчанию отсутствует. Оба поддерживаемых имени интерфейса, `127.0.0.1` и `localhost`, являются локальными; выбранный адрес нужно использовать последовательно.

```powershell
npm run dev -- --host localhost --port 5173 --api-port 5174 --connector-port 49425
```

Параметры можно задать через `SOTY_DEV_HOST`, `SOTY_DEV_PORT`, `SOTY_DEV_API_PORT`, `SOTY_DEV_CONNECTOR_PORT`, `SOTY_DEV_DATA_DIR`. Значения `PORT`, `DATA_DIR`, `SOTY_APP_ORIGIN_TEMPLATE`, ключи модели и прочие настройки рабочего сервера не наследуются процессом разработки. Vite не читает `.env` файлы при этом запуске; только явно названные `SOTY_DEV_PUBLIC_*` доступны браузеру. Секреты в них хранить нельзя.

Для проверки агента можно отдельно передать серверу `SOTY_DEV_GONKA_API_KEY` и `SOTY_DEV_GONKA_BASE_URL`; при необходимости доступа приложения к модели — `SOTY_DEV_GONKA_APPLICATION_TOKENS`. Они остаются серверными. Без ключа чат, сообщества, Connect и локальные приложения работают; `/health` отвечает 200, а `/ready` сообщает 503, поскольку его действующий контракт проверяет готовность inference. Запуск разработки ориентируется на `/health` и не делает платных вызовов модели.

Service Worker в режиме разработки удаляет только старые кэши `soty-online-*` этого origin и отменяет свою регистрацию. Профиль, ключи устройства и IndexedDB сохраняются. Это исключает загрузку старой сборки поверх Vite. Офлайн-режим и обновление установленного PWA проверяются на собранной версии. Vite не отдаёт каталоги данных, конфигурацию коннектора, ключи и базы; изменения в `data`, `var`, `output` и `backups` не вызывают перезагрузку интерфейса.

## Отдельный локальный коннектор

`npm run dev` не запускает и не перенастраивает установленный коннектор. Для разработки запустите исходный runtime в отдельном терминале с собственным каталогом и портом. Следующий пример использует адрес интерфейса по умолчанию:

```powershell
$devConnectorDir = Join-Path (Get-Location) 'var\dev\connector'
$devWorkspaceDir = Join-Path (Get-Location) 'var\dev\workspace'
New-Item -ItemType Directory -Force -Path $devConnectorDir, $devWorkspaceDir | Out-Null
$devConfigPath = Join-Path $devConnectorDir 'connector-config.json'
if (-not (Test-Path -LiteralPath $devConfigPath)) {
  @{ workspaceRoot = $devWorkspaceDir; allowedRoots = @($devWorkspaceDir) } |
    ConvertTo-Json | Set-Content -LiteralPath $devConfigPath -Encoding utf8
}
$env:SOTY_CONNECTOR_DATA_DIR = $devConnectorDir
$env:SOTY_CONNECTOR_SERVER_URL = 'http://127.0.0.1:5173'
$env:SOTY_CONNECTOR_LINK_ID = 'soty_dev_local_12345678901234567890'
$env:SOTY_CONNECTOR_DEVICE_ID = 'soty_dev_host'
$env:SOTY_CONNECTOR_DEVICE_NICK = 'Мой dev-компьютер'
$env:SOTY_CONNECTOR_AUTO_UPDATE = '0'
$env:SOTY_CONNECTOR_MANAGED = '0'
$env:SOTY_CONNECTOR_UPDATE_URL = 'http://127.0.0.1:5173/agent/manifest.json'
node scripts/soty-connector.mjs --scope Dev --port 49425
```

Каталог коннектора содержит его локальные учётные данные, поэтому не добавляйте его в Git. В примере он находится в уже исключённом `var/`. При повторном запуске существующая конфигурация не перезаписывается.

Откройте интерфейс на **точно таком же origin**, как `SOTY_CONNECTOR_SERVER_URL`, выберите подключение компьютера и подтвердите привязку. `localhost` и `127.0.0.1` считаются разными origin. Локальный POST `/apps/claim` требует точного совпадения; он создаёт одноразовый код только по явному действию и ждёт подтверждения сервера.

Для проверки реального приложения в третьем терминале:

```powershell
node modules/apps/examples/sample-app.mjs
```

Пример печатает локальный порт. В Сотах добавьте приложение, выберите привязанный компьютер, укажите этот порт и имя. Его адрес будет `http://app-<id>.localhost:5174`; современные Chromium-браузеры разрешают `.localhost` без изменения файла hosts. Порт 5174 обслуживает отдельный gateway origin, не интерфейс Vite. Публикация в группе или конкретному пользователю выполняется отдельным явным действием.

Ограничения совместимости, обмен манифестом агента и модель доступа описаны в [modules/apps/README.md](../modules/apps/README.md). В частности, v1 поддерживает относительные HTTP/WS адреса и partitioned session cookies gateway; произвольные URL, собственная cookie-авторизация приложения и внешние редиректы в этот контракт не входят.

Для проверки именованных адресов используйте отдельный **новый** каталог данных:

```powershell
npm run dev -- --named-apps --data-dir var/named-apps/data
```

Этот режим разделяет канонические адреса `app-<id>.legacy.localhost:<api-port>` и имена `<name>.named.localhost:<api-port>`. Обычный запуск сохраняет прежний шаблон. Не переключайте существующую базу между режимами: сохранённые зоны и адреса должны оставаться неизменными. Локальный HTTP не подтверждает готовность публичных DNS/TLS и не заменяет проверку HTTPS на отдельном домене.

## Собранная версия

```powershell
npm run typecheck
npm run build
npm start
```

По умолчанию сервер слушает порт 8080 на всех интерфейсах, использует `dist` и `data`. Для локальной проверки собранного приложения задайте отдельные каталог данных и origin:

```powershell
$env:HOST = '127.0.0.1'
$env:PORT = '8080'
$env:DATA_DIR = Join-Path (Get-Location) 'var\preview\data'
$env:SOTY_CONNECT_ORIGINS = 'http://127.0.0.1:8080'
$env:SOTY_APP_ORIGIN_TEMPLATE = 'http://{appId}.localhost:8080'
$env:SOTY_LOCAL_CONNECTOR_PORT = '49425'
npm start
```

Если используется dev-коннектор, его `SOTY_CONNECTOR_SERVER_URL` должен совпадать с origin этой проверки. Для другого каталога сборки есть `SOTY_DIST_DIR`.

## Рабочая среда: DNS, TLS и origin

Основной интерфейс и каждое локальное приложение должны иметь разные origin. Пример конфигурации сервера:

```text
HOST=127.0.0.1
PORT=8080
DATA_DIR=/srv/soty/data
SOTY_CONNECT_ORIGINS=https://soty.example
SOTY_APP_ORIGIN_TEMPLATE=https://{appId}.soty-apps.example
SOTY_LOCAL_CONNECTOR_PORT=49424
```

`SOTY_CONNECT_ORIGINS` — точный список адресов интерфейса через запятую. Шаблон приложения содержит ровно один `{appId}` в имени хоста. В рабочей среде требуется HTTPS; HTTP разрешён только для отдельного `.localhost` origin. Без шаблона создание/открытие локальных приложений недоступно, остальные функции сервера продолжают работать.

Нужны DNS-записи для `soty.example` и `*.soty-apps.example`, ведущие на reverse proxy, и действующие TLS-сертификаты для обоих имён. Wildcard-сертификат покрывает один уровень поддоменов. Один экземпляр Node-сервера обслуживает интерфейс, API, connector channel и все адреса приложений. Прокси должен сохранять Host, Origin и Set-Cookie, передавать WebSocket Upgrade и не буферизовать потоки приложений. Например, соответствующий фрагмент Nginx:

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    '' close;
}

server {
    listen 443 ssl;
    server_name soty.example *.soty-apps.example;
    ssl_certificate /etc/ssl/soty/fullchain.pem;
    ssl_certificate_key /etc/ssl/soty/privkey.pem;
    client_max_body_size 8m;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header Origin $http_origin;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_buffering off;
        proxy_read_timeout 150s;
    }
}
```

Сертификат в примере должен включать оба имени; также допустимы отдельные TLS server blocks. `SOTY_TRUST_PROXY` задаётся только для фактического доверенного reverse proxy, например `loopback` для локального Nginx. Не добавляйте общий `X-Frame-Options: DENY` поверх всех ответов: gateway выдаёт свою политику iframe, а основной интерфейс запрещает встраивание отдельно.

Сессия приложения — `__Host-soty_app_session; Secure; HttpOnly; SameSite=None; Partitioned`. Проверенный браузерный контракт — современный Chromium с CHIPS. Учётные данные Connect, коннектора и inference не передаются iframe. Права, публикации и привязки сохраняются в SQLite; активные каналы и короткоживущие билеты находятся в памяти. После перезапуска приложение открывается заново. Горизонтальное масштабирование живых каналов в этой версии не реализовано: несколько независимых серверов за случайной балансировкой использовать нельзя.

## Проверки

```powershell
node --test scripts/dev.test.mjs
node --test modules/apps/test/*.test.mjs
npm run world:test
npm run typecheck
```

`scripts/dev.test.mjs` поднимает отдельный настоящий API и Vite, проходит подписанный Connect bootstrap, проверяет World, HTTP, WebSocket, отказ чужому Origin, отдельный app host, освобождение портов и занятой порт. Тест не использует данные установленного коннектора и не вызывает модель.

Service Worker строится после удаления Vite технических CSS chunks. Каждая сборка проверяет наличие на диске **всех** URL в `shell`, `buildAssets` и `classicAssets`; отсутствующий файл завершает сборку ошибкой. World загружается отдельно от классической части, а классические сценарии получают дополнительный кэш. Предыдущий кэш сохраняется на один выпуск для уже открытых вкладок; новый worker не вытесняет текущую версию во время редактирования.

Решения сверены с [порядком Vite plugins](https://vite.dev/guide/api-plugin#plugin-ordering), [настройками Vite proxy/Origin](https://vite.dev/config/server-options#server-proxy) и [контрактом CHIPS](https://developer.mozilla.org/en-US/docs/Web/Privacy/Guides/Third-party_cookies/Partitioned_cookies). Поведение проверялось на установленном Vite 7.3.2; зависимость проекта остаётся в разрешённом диапазоне 7.x.
