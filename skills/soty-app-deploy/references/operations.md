# Операторский путь и проверка

## Обычный проект

Переиспользуй текущую hosting-зону и wildcard route. Не повторяй обмен корневых
доменов, настройку identity, выпуск платформы и создание коннектора на каждый
проект. Root, id, www, discovery и прежние app-зоны сохраняются.

Wildcard DNS ведёт на TLS edge; Caddy требует on_demand_tls permission endpoint
/api/apps/tls-allow. Использовать один https:// catch-all с host expression;
не literal https://*.zone site: он требует wildcard-сертификата/DNS challenge
и может перекрыть точный native сертификат. Сохранить все обслуживаемые зоны
в expression, объединить существующий catch-all, убрать только проверенные
старые wildcard блоки. После adapt проверить отсутствие wildcard subjects
в TLS automation. Проверить known enabled host → 200, unknown/nested host →
отказ. Не включать свободную выдачу сертификатов неизвестным именам.
Для массового потока измерить реальные лимиты сертификатов/ресурсов прежде,
чем обещать неограниченное размещение.

## Native

Генерировать только из свежего экспорта и конкретного domain ID.
Фиксированный upstream/порт не берётся из заголовков пользователя.
Digest закрепляет текущий source, проверка доступа выполняется на каждый запрос.

Критический Caddy rewrite — uri /_soty/ingress-check? с завершающим вопросительным
знаком. Проверить настоящий Caddy на запросах с query, а не regex шаблона.
Cookie Сот до backend удаляется, собственный cookie приложения сохраняется.
Дополнительный frame-ancestors CSP не должен затереть default-src и остальные
политики приложения. Не снимать X-Frame-Options запрет неразобранного backend.

Для воспроизводимой проверки генератора:

    SOTY_CADDY_BIN=/usr/bin/caddy node --test deploy/apps/native-ingress.acceptance.test.mjs deploy/apps/named-zone.acceptance.test.mjs

Тест поднимает отдельный loopback Caddy и synthetic upstream/gateway;
проверяет query, cookie, CSP и отказ. Это не публикация и не проверка production.
На Windows переменную задавать нативным способом; не переиспользовать HOME.

## Любое применение

1. Fresh serving IDs/images/ports/mounts, current registry/source/policy,
   disk/active config и PID/mode Caddy. Удалённые stdout/stderr собрать в памяти,
   выдать выбранные поля; не печатать inspect/Env/credentials.
2. Проверенный encrypted config backup и подготовленный exact rollback.
   Для изменения DB/schema — отдельно подтверждённый backup/restore.
3. Полный кандидат → adapt/validate → локальный listener. Проверить entry
   с параметрами, queried health/API, assets, boot, неправильную сессию/pin,
   старые маршруты. Успешный build не проверяет эти связи.
   В isolated без bootstrap-сессии gateway может вернуть HTML-вход вместо
   API/asset. Проверять содержимое и настоящий ответ приложения после входа
   в браузере; один HTTP 200 не является health приложения.
4. CAS по исходному SHA → атомарный config write → reload только известного
   serving процесса → active/disk equality. Если SHA изменился — перечитать,
   не затирать параллельную работу.
5. Для самой оболочки — полный pinned Docker build и deploy/connector/cli.mjs
   prepare/promote, текущие mounts/config fingerprint, drain и один writer.
   Старый serving image сохраняется stopped. Recovery не штатный обход.
6. Публичные DNS/TLS/HTTP проверки → реальные браузер/доступ/чтение/запись →
   data equality и отчёт. Если UI остался старым из-за PWA, применить предложенное
   обновление при сохранённых черновиках; не удалять профиль/browser storage.

Откат возвращает совместимый image/route, сохраняя текущие данные и IDs.
Проверить поддержку всех актуальных storage formats и retainedNamedAppZones:
релиз до введения зоны может быть непригоден для возврата.

## Минимальные receipts

source revision/image; app/domain/source digest; serving IDs и mounts;
old/new config SHA и active match; config backup verified; DNS/TLS/HTTP;
browser read/write/reload/fullscreen/restore; old-data equality;
команда отката с ожидаемой текущей версией. Не включать private document,
cookies, tokens, claim codes, auth headers или secret config.
