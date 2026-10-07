# Обработка материалов: Source3, запуск по умолчанию выключен

Это новый opt-in модуль ordinary Source, не миграция чужих legacy ACL. Source3 добавляет пять таблиц и четыре immutable trigger; всего 29 проверяемых объектов. Root Apps8/DDL, standard source profile2 `f68eee…`, frozen profile1, Source RP49 и finite Basic300 остаются прежними.

## Права и понятный путь

Автор отправляет обращение с выбранным изображением/аудио. Отдельным действием он разрешает конкретную цель для точной ревизии обращения и digest сохранённых материалов. Владелец Native проекта отдельно выбирает одобренный engine/policy и сохраняет ограниченный grant. Root App owner, display name, manifest и `allowEmptyGuest` не делают человека Native владельцем. В существующем проекте нужен его действующий Native session/owner role + независимый реальный OIDC proof. Новый пустой проект — отдельная явная Native policy; только после подтверждения/OIDC она создаёт реальные новые Native строки, а не права к прежним данным.

Runnable example показывает оба действия. Кнопка «Обработать обращение» выключена, пока реального допустимого исполнителя нет. «Сохранить разрешение владельца» создаёт queued grant, но явно не обещает запуск. Отказ не изменяет исходное обращение. При потере ответа — «Результат пока не подтверждён» / «Проверить результат»: только чтение той же квитанции, не автоматический новый intent/запуск. Transcript остаётся буквальными данными; triage — только предложения priority/labels с provenance и проверкой человеком. Статус обращения не меняется.

## Opt-in и миграция

Trusted Source constructor `feedbackProcessing:{policy,engines,enforcer?}` включает модуль и запрашивает format3. Новый пустой DB требует `initialize:true`; existing2→3 требует отдельного `allowFeedbackJobsMigration:true`. Existing1 сначала явно проходит принятую 1→2 миграцию. HTTP/body/manifest не выбирают schema/key/engine/role/migration. При отсутствии constructor config Source остаётся прежним.

Independent `reader3.mjs` с literal hashes запускается до любой новой записи. Миграция заменяет только `native_meta` marker и добавляет namespace; old Native IDs/FKs/строки/receipts/cipher сохраняются. Frozen reader2 отказывает Source3 до START. Новый compatible baseline с reader3 читает [1,2,3] даже с выключенной обработкой; fallback — этот подготовленный baseline, не старый frozen57ad. Cold-copy/restart и `foreign_key_check=0` проверены; Source3 Linux image/cold deployment ещё отдельный gate.

## Source SQL и квитанции

Reporter consent ограничен его точными Native/Source sessions, Native generation, membership revision, Source key ID и сроком; его отзыв прекращает допуск. Owner grant дополнительно хранит exact resource incarnation, owner session/generation/role revision, исходную Source session, private Native binding digest, ticket revision, actual retained media digest, engine/policy pins и бюджет. Grants и results encrypted AES-GCM/AAD. Ни один bearer/token/rootContext не возвращается в UI.

Перед claim и после процесса host proof port BFF получает свежие actual installed Root-channel context, maintained RP userinfo и Native proof. `processingContext` в encrypted Source session — только locator к исходному ref; он не восстанавливает Root authority. Missing/expired/closed ref, Root restart, profile/resource change, revoke или key mismatch запрещают новый START/final. Basic не превращается в service credential/24h/resume.

В Native `BEGIN IMMEDIATE` claim/final снова проверяют consent/grant/ticket/media/engine/policy/current owner/session/key/deadlines. Network/process работают вне SQL. Durable `queued→started` CAS допускает одного исполнителя; begun/unknown не захватывается новым процессом и не перезапускается. Между процессом и final любое изменение прав оставляет unknown, без result. После COMMIT потерянный ACK не отменяет запись: immutable encrypted receipt читается без повторного процесса. Current Native ticket ACL нужен и для чтения результата. Отмена останавливает owned executor и сохраняет unknown; общий таймер не доказывает rollback.

Все действия используют существующие fixed `/api/embed/query`, `/invoke`, `/receipt`: `feedback.processing.context/ticket/consent`, `feedback.job.grant/revoke/status/result`. Host-only `processing.process(jobId)` не является RPC или полномочием из JSON. Нет per-author Root кода, нового Root URL/command forwarding или новой Root DDL.

## Лимиты и честная готовность

Нормативный потолок маленького pilot: wall60s, CPU10s, cleanup2s, scratch2MiB, output32KiB, media1MiB/3, parallel1, grant≤900s. При START current authority должна покрывать полный wall+cleanup; Native Basic300 может оказаться слишком коротким — тогда запуск запрещён. Это не обещание качества/ёмкости 120s ASR. Unknown удерживает слот, оператор проверяет реальный outcome; автоматической lease takeover нет.

Engine/enforcer constructor brand подтверждает custody, не sandbox. Production executor дополнительно обязан иметь current host admission `assertHostBounds`, реально обеспечивать hard CPU/network/process/scratch bounds и уважать abort. Windows/default missing enforcer — not-ready. Тестовые `syntheticTestOnly` порты не делают production готовым. Реальный job→Linux executor bridge, approved binary/model pins и measured budgets ещё не внедрены. Existing Root faster-whisper/Windows OCR не эквивалентны этому enforcer. Нет cloud/model calls, пользовательского media, публикации или расходов.

## Проверки текущего slice

* Source full suite 59/59, 0 skip (Windows Node24.19), включая прежний Basic/Native/standard2/SDK94 путь, package вне Root, reader3 и новые gates. Source declaration strict/TSC/build прошли.
* Реальные два OS Source процесса: один processor claim/call/receipt. После убийства started остаётся started/unknown; cold retry не перезапускает его.
* Actual installed HTTP/WS + signed Root/Human/OIDC: два новых gates. Native participant denied grant; independently existing Native owner прошёл dual proof. Grant COMMIT/wire loss/restart восстанавливает original receipt. Закрытие настоящего original Root slot во время held synthetic processor блокирует final result. Synthetic executor проверяет lifecycle, не OS/модель.
* Actual Chrome HTTPS shared Root UI → Native new-empty consent → same Source iframe: retained PNG, отдельное reporter согласие, Native SQL owner grant, disabled START, Source3 counts 1ticket/1consent/1grant/1job/0result. 390px: overflow false, buttons≥44. Ни Human, ни Source cookies не инжектировались. Natural cycle userinfo200=13/token200=1, 429=0. Это local same-machine transport; publisher localhost для remote user не готов по этому результату.
* Отдельный Root guarded Linux fixed probe8/8 на image `c03a61d12e03870747e9013860fc36e23e53920da9b07ea1e795b3fef9628ae6` (revision56fe2cb, Node24.15). Hard RLIMIT_CPU1s SIGKILL1008ms/cgroup1002577µs; wall507ms/cancel257ms; scratch ENOSPC2MiB; FS/net/subprocess denied. PreSTART spec/mounts/UID1000/no-net/read-only/128MiB/pids32/tmpfs/data/no-anonymous-volume independently checked, stopped0/noOOM. Receipt `/home/ai2/codex-soty-universal-20261007-9f8dcd71/feedback-enforcer-f4da3ee4d349ede5c1a63a619a539b7f-root-public.json`; `models:false,sourceJobsReady:false`. Этот probe не соединён с Source jobs и не измеряет модель.

Следующий обязательный шаг — reviewed fixed host executor + real job→sandbox gate с exact approved engine/pin, current Source proof и result/cancel/CPU/wall/lost-ACK evidence; затем Source3 Linux reader/image/compatible cold gate. Production и remote self-service readiness этими тестами не заявляются.

## Отдельная delta отмены

Review исходного `0cecf09` выявил два job service: Native revoke и host process держали разные RAM maps. Final Native deny запрещал результат, но не доставлял отмену текущему executor. Новая delta создаёт один constructor-branded service на точных store/resource/incarnation; Native и server.processing разделяют тот же service. BFF current-proof port связывается приватной lazy closure, не полем JSON.

Actual installed Native invoke/revoke с held executor теперь доставляет Abort до admitted wall, вне Native SQL; result/receipt отсутствует и повторная обработка запрещена. Контроль с исходным Index `0cecf09` действительно падает на требовании ранней отмены, а не только сравнивает implementation. Межпроцессная отмена остаётся отдельной bounded-poll/OS-kill задачей Linux bridge; same-process Abort не считается мгновенной распределённой отменой. Callback, игнорирующий Abort, не является hard wall guard.
