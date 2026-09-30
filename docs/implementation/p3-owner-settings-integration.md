# P3-C1 — интеграция owner settings

2026-09-30. C1 локально принят после реализации, независимых повторных проверок и фактического browser gate. Внешние release gates остаются отдельными.

## Результат

`apps.inspect` проходит общий подписанный API и существующую проверку `expectedAccountId`. Сам read-model синхронно читает один SQLite snapshot и доступен только действующему владельцу. Канонический и выбранные именные адреса используют тот же `runtimePath`, что настоящий запуск; небезопасный прежний путь не отнимает возможность закрыть доступ.

`apps.update` принимает необязательный `expectedRevision` для совместимости с прежним клиентом; новое окно всегда отправляет версию. Внутри write transaction проверяется точное совпадение до записи. Отсутствующие grants сохраняются без нового предоставления прав. Явно изменённые grants требуют текущих полномочий. Обычное переименование не меняет policy epoch и сохраняет действующие ticket/session/HTTP/WS; проверка актуальных прав при этом не отключается.

Карточки и inspect используют активный immutable RuntimeTarget и связанное с ним устройство. Connector-v1 observation хранится только в памяти действующего канала и привязан к текущим revision/digest. Это привязка серверной конфигурации, **не ACK target от v1 connector**. Такой ACK и смена источника относятся к C2.

Наблюдение истекает строго через45000ms от серверного приёма. После disconnect времена очищаются; новый канал не наследует прежний ready. HEAD404 доказывает ответ процесса, поэтому статус называется «источник отвечает», а не «приложение проверено». `checkedAt` позволяет клиенту вычислить остаток без предположения, что его часы совпадают с сервером; latency вычитается консервативно.

## Выполненные проверки

- Root: `app-inspection.test.mjs`14 + `source-observation.test.mjs`3 + независимый `app-settings.acceptance.test.mjs`14 = **31/31 PASS**, exit0, лог `output/implementation-20260930/p3-c1-backend-root.log`.
- Независимые transport сценарии используют production connector, настоящий loopback HTTP/WS, Worker threads с двумя SQLite writers. Синтетические аккаунты ограничены временным fixture; это не production и не browser OAuth.
- Ранее после index integration: существующие Apps/service/runtime + source freshness **15/15 PASS**.
- Итоговый root world gate после всех исправлений: **447 tests / 444 pass / 0 fail / 3 explicit skip**, отдельный Connect **70/70 PASS**, typecheck и production build PASS. Логи `output/implementation-20260930/p3-c1-{world,connect,build}-final.log`.
- Независимые повторные пробы смешения аккаунтов A → B → A и задержанного каталога прошли на настоящем Connect client/service; focused gate **17/17 PASS**. Окно привязано к исходному аккаунту до подписи запроса, запуск ставится в очередь до необязательных метаданных. Позднее имя не заменяет iframe.
- `git diff --check` на текущем backend срезе PASS.

Проверка интерфейса, фактического Clipboard и двух браузерных вкладок зафиксирована в [browser receipt](p3-owner-settings-browser.md); [независимый аудит](p3-owner-settings-ui-review.md) отдельно указывает границы VM/storage. На финальном freeze повторное открытие приложения подтверждено реальным HTTP/WS, настройки показывают свежий источник; снимок `p3-settings-desktop.png`. Публичная зона, реальные DNS/TLS, совместимый Linux candidate/fallback и isolated restore всё ещё требуют своих gates. Смена устройства/порта относится к следующему C2.
