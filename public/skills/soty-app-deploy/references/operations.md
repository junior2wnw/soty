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

Новый Source alias сначала закрепляется bound/inactive на том же app ID. При
`sourceEntryActivation:true` private prepare принимает exact
`sourceEntryAction:{action:'activate',domainId}` и возвращает проверенный origin;
адрес включается только в той же транзакции, что Source promotion. До неё не
сохранять Source имя в обычной публикации прежнего порта. Отсутствующая capability
не разрешает такой обход; подробнее — `authentication.md`.

После promotion у пары Source/Native экспорт v2 может описывать закрытую
конфигурацию, но native план требует anyone, оба active bound alias и текущий Source launchEntry.
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
   Caddy adapt выполнять с одинаковыми физическими filename и cwd. Stdin
   `--config -` меняет auto-hide имя в `file_server.hide` и не доказывает равенство
   с реальным файлом. Сравнивать полный JSON; это поле не исключать. Финальное
   active/disk сравнение делать с реальным serving path после записи.
5. Только UI/публичные документы оболочки — существующий versioned publisher
   `deploy/ui` с конечным inventory и полным byte readback. UI и backend pins
   различаются; обложка или инструкция не требуют нового backend/data writer.
   При доступном checkout см. `deploy/ui/README.md` и
   `docs/deployment/ui-release.md`. Используй полный reviewed source closure
   publisher или его проверенный bundle; копии одного helper недостаточно.
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

## Если меняются сами Соты

Выбери qualified Node24 по абсолютному пути и проверь его версию/hash **до
импорта** operator factory: default Node18 может упасть ещё при загрузке модулей.
Backend собирается целиком из одной immutable Git revision с frozen dependencies;
historical backend overlay не является способом доставить новые API/identity.
Перед prepare/promote квалифицируй exact image и обратный переход на текущей
копии DATA; [копия и backup](recovery.md) не взаимозаменяемы.

UI publisher получает finite release из candidate dist с retained immutable URLs.
Independent UI source/producer и backend image/source закрепляются отдельно:
новый static dist не является dist иного frozen image. Сверяй все retained URL
и immutable bytes; обновлённые docs не меняют прежний sealed release.
Оба overlay строятся из одного одобренного **unwrapped canonical ingress**;
восстановленный current overlay должен побайтно совпасть с disk. Не оборачивай
уже установленный overlay повторно. Изменившийся ingress требует новой сверки
canonical. До применения нужны свежие active=disk, config SHA и точный
PID/UID/startTicks/argv SHA процесса Caddy. При adminOFF штатный publisher посылает
один SIGUSR1 тому же процессу; backend/DATA writer он не заменяет.

Используй существующие publisher `execute`/`inspect` с exact plan и encrypted
baseline/candidate. Потерянный результат не разрешает новый transaction,
повторный execute, другой receipt directory или слепой rollback. Только readonly
inspect и независимая сверка process/disk/active разрешают неизвестность; apply
сам возвращает `activeReloadVerified:false`. После него проверь публичные новые
и retained bytes, headers, PWA и восстановление WS: прежний Node PID этого не
доказывает. Подробный bounded driver и команды находятся в `deploy/ui/README.md`.

## Ограниченный оператор: причина, проверка и восстановление

Обычное приложение использует короткий maintained recipe. Preflight, конечный
inventory, сравнение pins и readback выполняются автоматически, а человек
проверяет понятное намерение и результат. Новый агент продолжает по receipt,
а не по сотне ручных сверок из старого чата. Эта ветка платформенного QA нужна
только при изменении самих Сот, не для каждого нового приложения.

Окончательный app/operator plan до stage прогоняй локально его фактическим
schema/contract validator со всеми требуемыми ID/hash зависимых receipts.
Проверка шаблона или несвязанные pure tests не заменяют проверку финального DTO.

Для wrapper проверяй настоящий CLI module graph в безопасном Linux subprocess,
не только импорт factory: top-level-await цикл может завершиться до операций.
Сохраняй ограниченные exit/stdout/stderr; утраченный stderr не превращается в
наблюдённую причину после отдельного воспроизведения.

Публичный reader-код через bind требует явных mode 0644 и каталога 0755 даже при
umask 077; private ops/bin остаются 0700, планы/reviews/executors и секреты 0600.
Это не решает DAC закрытого файла DATA. Отдельный reviewed observer может
получить только DAC_OVERRIDE при CapDrop:ALL для finite observer allowlist
(baseline/new-first/old/new-second), own-copy DATA/code
RO, rootfs RO, network:none и прежних ресурсных guards. Negative write должен дать
EROFS. Это не право canary/production и не повод менять ownership, добавлять
DAC_READ_SEARCH или общие capabilities; отдельное существующее исключение
config-extract не расширяется на другие роли.

Уточнение 26.10.2026, источник — платформенный LOAD/readback и negative-контроли
bind aliases; область — оператор Root. Обычный app/Source SDK/CLI recipe не требует Root build.
Raw CAS сравнивается при одной версии Docker API и одной полной projection:
versioned и unversioned inspect могут возвращать разные поля при прежних CID/PID.
Сначала проверь endpoint/version, затем реальное изменение; поля не выбрасывай.
Собственные объекты и обычные peers тома ищи exact label/volume filters с all=true;
при очистке учитывай stopped peers. Но volume-only не доказывает Root sole-writer:
bind родительского каталога или подкаталога с другим destination может не попасть
в такой ответ. Для этого ограниченно и постранично прочитай **все ACTIVE** контейнеры,
inspect каждого и сохрани исходный RW/Running/path-overlap predicate; paused и
переходные active states тоже учитываются. Сохрани 4 MiB на ответ, предел страниц,
курсор и отказ при дубликате/переполнении. Контроли путей используют POSIX-семантику
сервера. LOAD не требует истории stopped-контейнеров и не разрешает их очистку.

Закрепляй хеш по объекту и точным байтам producer, не по похожему имени:

| Объект | Разные идентичности |
|---|---|
| Образ | Фактический Engine imageId/manifest descriptor и Buildx config IID хранятся отдельно |
| SDK | SHA архива `.tgz`, SHA `SDK-CAPSULE.json` и его sourceCommit — отдельные pins |
| Файл SDK | LF-файл поставки и CRLF-checkout могут иметь разные SHA; runtime pin берётся из точного поставляемого файла |

CREATE проверяется по точному prospective body и полной существующей projection.
Если network:none не использует MAC, не отправляй только пустой верхнеуровневый
`/MacAddress` в QA body. Остальные поля сохраняются; глобальная normalization
не следует из одного доказанного roundtrip diff.

Собственный holder с Node PID1 должен штатно завершаться по SIGTERM. Если STOP
потерял ответ или transport истёк, читай тот же intent и actual state/terminal
helper; второй STOP не следует из timeout. Поля transaction/operation берутся
из своего контракта. Специальный purpose outcome с ok:false принимается только
по закрытому predicate и независимому readback. Reclaim старой cipher-копии
опирается на фактический off-Dev rehash и exact план, сохраняя backup/ключ/журнал.
Названия restore/candidate не заменяют проверку текущих mounts и назначения.

## Минимальные receipts

source revision/image; app/domain/source digest; serving IDs и mounts;
old/new config SHA и active match; config backup verified; DNS/TLS/HTTP;
browser read/write/reload/fullscreen/restore; old-data equality;
команда отката с ожидаемой текущей версией. Не включать private document,
cookies, tokens, claim codes, auth headers или secret config.
