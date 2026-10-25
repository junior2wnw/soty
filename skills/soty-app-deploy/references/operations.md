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

Генерировать только из свежего экспорта и конкретного утверждённого domain ID.
Фиксированный upstream/порт не берётся из заголовков пользователя.
Digest закрепляет текущий source, проверка доступа выполняется на каждый запрос.

У пары Source/Native экспорт v2 может описывать закрытую конфигурацию, но native
план требует anyone, оба active bound alias и текущий Source launchEntry.
Native alias и port/path берутся из nativeEntry.target; манифест соответствует
этому target. Ссылка Сот остаётся Source entry. Иной явный domain ID не заменяет
ни одну поверхность; старые saved/deep paths не переписываются.

Критический Caddy rewrite — uri /_soty/ingress-check? с завершающим вопросительным
знаком. Проверить настоящий Caddy на запросах с query, а не regex шаблона.
Cookie Сот до backend удаляется, собственный cookie приложения сохраняется.
WebSocket аутентифицируется обычным HTTP subrequest: в forward_auth удалить
Upgrade, Connection и Sec-WebSocket-Key/Version/Protocol/Extensions; в самом
app proxy сохранить upgrade. Проверить настоящий 101 и обмен сообщениями,
не только HTTP entry. Для POST/PUT/PATCH/DELETE и WebSocket проверять exact
Origin приложения; чужой и отсутствующий Origin отвергаются.
Exact GET /_soty/ingress-check идёт первым прямо в Root loopback, без Native
upstream и без forward_auth: fixed surface=native, app, primary и Native digests;
query/upgrade удаляются. Это публичная policy, не Human session или доступ к данным.
У ordinary Native primary==Native; DATA forward_auth сохраняет прежний app/target
контракт и удаляет caller surface/primary. У пары DATA проверяет оба текущих pin,
а остальные /_soty/* на Native alias закрыты. Все четыре служебных заголовка и
Root cookie удаляются перед Native upstream. Не пускать caller headers выбирать
public policy вместо DATA authentication.
Дополнительный frame-ancestors CSP не должен затереть default-src и остальные
политики приложения. Не снимать X-Frame-Options запрет неразобранного backend.

Native SDK получает nativeGuard из плана: HTTPS rootCheckUrl на Native alias,
app/origin и два digest. Guard должен проверять собственную Human session/ACL
и текущую короткую Root lease для HTTP/WS/длительных streams; Caddy проверяет
только новый handshake. При откате к primary Native v1 новый snippet сохраняет
exact policy GET (digests равны). Старый Root без typed policy означает отказ,
а не fallback на ответ HTML или отключение SDK guard.

CLI verify пары проверяет exact policy JSON, pin/revision/epoch/admission и lease
до 1 s с лимитом 4096 байт. Вместе с entry/query 200 это только HTTP gate:
nativePolicyChecked не подтверждает загрузку Native SDK, вход человека или
сохранность данных. В v1 такого подтверждения тоже нет.

Для воспроизводимой проверки генератора:

    SOTY_CADDY_BIN=/usr/bin/caddy node --test deploy/apps/native-ingress.acceptance.test.mjs deploy/apps/paired-native-ingress.acceptance.test.mjs deploy/apps/named-zone.acceptance.test.mjs

Тест поднимает отдельный loopback Caddy и synthetic upstream/gateway;
проверяет query, cookie, CSP, реальный WS echo, missing/foreign Origin,
неверную сессию/source pin, primary и paired policy GET без рекурсии и отказ
Native private данных без собственной Human session. Это не публикация и не
проверка production.
Пропущенный из-за отсутствия SOTY_CADDY_BIN тест не считается пройденным gate.
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
   У Caddy adapt поле file_server.hide может зависеть от имени файла кандидата.
   Финальное active/disk сравнение делать с реальным serving path после записи;
   не игнорировать произвольные различия и не выдавать filename-only delta за
   доказательство неверного применённого routing.
5. Только UI/публичные документы оболочки — существующий versioned publisher
   `deploy/ui` с конечным inventory и полным byte readback. UI и backend pins
   различаются; обложка или инструкция не требуют нового backend/data writer.
   При доступном checkout см. `docs/deployment/ui-release.md`.
   Изменение backend/identity/storage — полный pinned Docker build и
   deploy/connector/cli.mjs prepare/promote, текущие mounts/config fingerprint,
   drain и один writer. Старый serving image сохраняется stopped.
   Recovery не штатный обход.
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
