# Площадка Сот: точка обнаружения

Проверена 05.10.2026. Это карта для поиска актуального состояния, а не
разрешение перезаписать production по старым IDs.

- Соты: https://4-2.xn--p1ai (4-2.рф).
- Новые именные приложения: https://NAME.4-2.xn--p1ai.
- Прежняя зона https://xn--n1afe0b.online сохраняется; NFC остаётся
  https://nfc.xn--n1afe0b.online. Его app/domain IDs не менять ради новой зоны.
- Discovery/старый совместимый вход: https://soty.pochinit.online.
- HIVE native: https://hive.4-2.xn--p1ai, process hive-soty-app, loopback 18085.
- Старый HIVE entry и локальные черновики: https://4-2.xn--p1ai/__hive.
- Pocket ID: https://id.4-2.xn--p1ai; прежние issuer/callbacks сохраняются.

Сервер этого пользователя доступен через документированный SSH alias dev.
Не использовать его как универсальное значение для чужой установки.
Caddy /etc/caddy/Caddyfile, admin off; Соты soty-online-chat на loopback 18182,
том soty-online-chat-data. Hosting-файл внутри контейнера:
/run/connect-releases/soty-app-hosting.json. Проверять live Config/HostConfig,
active Caddy и SQLite в памяти, выводя только выбранные безопасные поля.

Текущая конфигурация:

    {
      "schema": "soty.app-hosting.v1",
      "domainProfile": "shell-subdomains-v1",
      "namedAppZone": "https://4-2.xn--p1ai",
      "discoveryOrigin": "https://soty.pochinit.online",
      "retainedNamedAppZones": ["https://xn--n1afe0b.online"]
    }

За основу возврата брать совместимый релиз с retainedNamedAppZones
(4939923 или новее), а не выпуск до изменения зоны. Совместимость current data
и storage readers проверить живым guard перед запуском.

Рабочие копии текущего внедрения находятся в D:/etap/output/domain-swap-20261004;
исходные D:/соты и D:/etap могут иметь принятые незакоммиченные изменения.
Папка не определяет правильную revision сама: сверять Git и serving image.

Основная инструкция — [app-release.md](app-release.md). Отчёты оператора
с revision/images, backup/config hashes и actual checks — в output текущей
задачи. Старые one-off domain swap/rollback команды не повторять без CAS-проверки
и реконструкции, сохраняющей все последующие маршруты.
