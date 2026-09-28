# Соты: выпуск интерфейса и модулей, 28.09.2026

Обновление опубликовано на https://xn--n1afe0b.online и https://soty.pochinit.online. Runtime commit `24c2da22d48b89d295da52b100ef067c7b61ef63`, Connect 0.1.2 / sequence 2. Ниже — фактические доказательства и границы проверки.

## Исходная рабочая версия

- Host: `dev`, serving container `soty-online-chat`, loopback `127.0.0.1:18182`.
- Production revision: `7f76096bf291d0ed3c741573d76af908f70cfbf7`.
- Production image: `sha256:b0c58d917925faba77ac34d23dc042036741634378680f7e994baf480ad6220a`.
- Connect: 0.1.1, sequence 1, tree `1c96138eb59eea8b0733b06e3200f2c68f31e027c22e7a487935209575c0a50d`.
- Live data volume: `soty-online-chat-data`. Исходные source/state/journals сохраняются.

## Подготовлено

- Ветка `codex/experience-audit-20260928`, основной commit `85f36a15d744f0e41b7370548a638a2d57b531a1`, исправление shutdown `24c2da22d48b89d295da52b100ef067c7b61ef63`; оба push подтверждены. Runtime собирается из `24c2da2`.
- Signed Connect 0.1.2 / sequence 2 сформирован командой `git -c core.autocrlf=false archive`; file hashes сверены с Linux Git blobs. Первая неподанная версия с CRLF отклонена при сверке и никогда не продвигалась в feed.
- Module tree: `63ada2ef2609083596ebf025bfc64526175060ecfb06e069a1c7e6d4fbc6be69`.
- Release bytes SHA-256: `7c37de5014646c57232e478f05a4a158d697cc23735c0c3a374694ce425e3349`.
- Manifest SHA-256: `8b53207478171301a510d4936745e62d848e3375f200b2f536ca7187af895549`; 28 файлов, 430741 bytes; expiration 2026-12-27T00:55:52.233Z.
- Private signing/backup keys остаются в Windows DPAPI. На host передан только подписанный публичный artifact.
- Новый TLS candidate сохраняет прежние site blocks и разрешает отдельные app-hostnames через registry gate. Исходный Caddyfile SHA-256 `8521ff947ff10ebf99c9d67c14d4132bc272df1b43febde5a4c5b1aea0bc4239`, candidate `ac4943654fb09f5846372fa1b9404ede39692ab8712c90dc7ad278b701316666`.
- Зашифрованная копия Caddyfile проверена расшифрованием только в RAM: 9420 bytes, исходный hash совпал. Encrypted SHA-256 `61f15aeff93c77ab9f95f7292441060b804d4abf41fba7f1950b5b7e6586317d`.
- Caddy 2.11.2 запущен через `caddy run --config /etc/caddy/Caddyfile`, без `--resume`; admin API отключён. Для применения после проверки app registry gate используется поддержанный [SIGUSR1 reload](https://caddyserver.com/docs/command-line#signals), с прежним PID и проверкой файла по hash.

## Серверная приёмка

Linux Docker gate выявил открытый IPC после SIGTERM у supervised дочернего сервера. Исправление закрывает IPC при штатном shutdown; регрессия требует завершения без SIGKILL. Проверка не отключалась.

- Все Dockerfile gates выполнены на Linux. Дополнительные controller tests: 60 PASS, один Windows DPAPI skip; реальный Docker reader зашифрованного backup проверен с чужими владельцами файлов 0600.
- Canary image: `sha256:6278757b95d060a8ac2293b1911b348fd302ebd565e4d3c141b0558abbd6d887`.
- Синтетическая цепочка: новая версия ready, Apps configured, shell 200; запись v4; старый reader отвечает HTTP 200, но storage-ready 503; повторное открытие новым reader успешно. SQLite integrity и hash истории неизменны. Оба завершения нового runtime имеют exit 0. Запросов к модели: 0.
- Повторная cached сборка меняет OCI attestation/index, сохраняя runtime manifest `684d316158cf1239b822ea5a033e01179f9a179d323df8713cfcdf34ff40b3b1`, config `bcd87e8793f70827ed1f052681852c8115066b6626a41d66e19ef1db7b54890b` и 14 layers. Канонический hash `{Config,RootFS,Os,Architecture,Variant}` через Docker API 1.45: `60b76d5cefe77942664ca7461503c9ab18bdfaee92408ddb7232a89d4c9b3652`. Текущий API опускает 11 пустых полей Config; его равный для обоих образов hash — `08f20b7c87b39ac1ebcdfcabc7e829547fa6449fa6dcff69eb5184d7c845ee14`. Сравнивать нужно ответы одной версии API.
- Перед переключением: 912 корневых JSON-файлов; Connector: 333 jobs/inputs/results, 3589 events, 59 requests, 24 records; Connect: 16 accounts, 1 enrollment. Обе SQLite integrity: ok. History SHA-256 `6eaf8fc863f010ee8a9d9559f586a3c3fee01fc3867f5f7c5491eb6882eb412f`.
- Подготовлено отдельное поколение controller/config/state с прежним sequence 1 и точным runtime baseline; старые журналы не переписывались. Подписанный LF artifact sequence 2 опубликован. Обновление выполнено через systemd с UMask 0077; timer возобновлён после успешной приёмки.

## Рабочая выкладка

- Serving image: `sha256:d07345cb66b2c1ab903861f36902d7b301cc25289778d0d178c81143281ca97e`. Его Config, 14 RootFS layers, OS и architecture равны принятому canary image. Container: `d86bc0b9b8f88a7693a227cca0b15864ac5388a8c6e478f25e0f1c3c7d9c4ceb`.
- Controller outcome `admitted`; module sequence 2; transaction и pending — null. Проверен ровно один работающий writer volume. Service завершился с exit 0; timer active/waiting и указывает на новое поколение config/state.
- Model-readiness hash сохранён: `9a73df78168b25f67dc8404ff60cc2086d6bb704b5624bb39c490d8089f102ed`. Контроллер сверил конфигурацию, доступность моделей и policy до/после переключения; реальные запросы к модели не выполнялись.
- Offline backup: `soty-2026-09-28T01-15-32-937Z-13b113b6.enc`, SHA-256 `3dfcce8a7068aaddab46c3a82b252b6faa67d11bb07310ab83a89016b53100f0`. Копия загружена оператору; GCM-аутентификация, offline metadata и весь tar проверены расшифрованием в RAM: 1078 entries, 912 room files, 2 SQLite и 1 корректный пустой SQLite. Открытые данные на диск не записывались.
- До публичного QA повторены SQLite integrity/counts/history hash: значения Connector и Connect полностью совпали с baseline, все 333 задания сохранены. Набор 912 корневых JSON-файлов совпал. Дополнительное сравнение backup с живыми файлами в RAM: 906 побайтно прежние, у 6 обновился encrypted snapshot после переподключения клиентов; auth/closed, пустые updates и files прежние. В 5 меняются только ciphertext/nonce/id/createdAt, в 1 также deviceId/deviceNick. Это соответствует `hello → scheduleSnapshot → storeUpdate` с повторным шифрованием; совпадение plaintext без ключей комнат не заявляется. Добавлений, удалений и изменений файлов во время контрольного sweep — 0.
- Caddy candidate применён через SIGUSR1 к прежнему PID 3768; admin API остаётся отключённым. Активный autosave канонически равен адаптированному файлу, обе основные страницы/health отвечают 200. Для однократного operator helper требовалось разрешить сигнал host-процессу в AppArmor; serving containers и их защиты не менялись. При неподтверждённом сеансе guard проверенно восстановил исходный Caddyfile, затем подтверждённый сеанс завершился `committed`.

## Публичная приёмка

Оба адреса возвращают 200 для `/`, `/health`, `/api/connectors/storage-ready` и `/api/apps/capabilities`; `storageReady:true`, `maintenance:false`, `configured:true`.

Сквозной Apps smoke на реальном HTTPS/WSS: **11/11 PASS**, 8280 ms. Проверены подписанная привязка коннектора, регистрация loopback-проекта, отдельный TLS origin, одноразовый ticket, cookie attributes, запрет anonymous/foreign-origin, HTML/JS/CSS/POST и фильтрация заголовков, WebSocket echo, отзыв с закрытием сокета и запретом нового запуска. Cleanup подтверждён. Остались только скрытый QA-аккаунт, offline binding и revoked app; реальные пользователи и платные модели не использовались.

В браузере принята опубликованная `index-CaCvolqx.js`: 320/390/1440 px без горизонтального переполнения; открываются записки и библиотека возможностей; светлая тема, ползунок яркости и возврат к системной теме работают. PWA сообщает сохранённую offline-оболочку. Скриншоты реального сайта — `output/release-audit-20260928/screenshots/public-*` в исходной рабочей папке оператора. Общие локальные проверки и ограничения — [итоговая приёмка](../../docs/research/audit-final-acceptance-20260928.md).

## Границы отката

После появления account-owned задач `soty.connector-job.v4` требуется совместимый reader. Старый код с `/health:200` и `storage-ready:503` не является рабочим откатом. Обычный откат сохраняет актуальный live volume; более старый backup автоматически не восстанавливается.
