# Source sessions: явный повторный вход и finite Long

Working base `c533dd0` сохранена в отдельном managed worktree `soty-source-app-sessions`; исходный SDK/пакеты не меняются. Linux joint/Reader3 physical cold/production image пока не выполнены: Dev capacity guard запрещает RUN. Метаданные и готовность исполнения разделены.

## A. Повторный Basic-вход без новой Native-роли

Обычный Source сегодня выдаёт Basic до300s, привязанный к исходному Root slot. New-empty policy создаёт Native principal, Native24h session и selected-resource consent, но Native cookie не выдаёт. После Basic expiry повторный Native capture без собственного cookie отказывает: пустой проект уже не пуст. Это конкретный незакрытый returning-user путь.

Минимальная delta — Source-native constructor opt-in `allowLinkedLogin`, defaultfalse. При новом явном входе Source может подготовить **только login candidate**, если его текущая SQL authority содержит immutable `(issuer,sub)→principal`, тот же realm/resource/incarnation/semantic grant/device, current active membership и **непросроченную активную Native session** из выбранного Source consent. Отсутствующий/просроченный/отозванный Native grant запрещает этот путь. Кандидат не даёт чтения данных и не создаёт principal/resource/session/role/grant.

До callback кандидат — Source-owned разрешение проверить новое OIDC-доказательство. Нужны новый реальный PKCE/state/nonce/code exchange, exact issuer/sub, свежий Root/currentuserinfo, затем atomic final Source SQL recheck captured generations/membership/grant/resource. Старый Basic cookie/AT/Root reference не используется для разрешения. Новый Basic Source session создаётся обычным новым login intent; Native principal, roles, Native session и selected consent сохраняются. Initial legacy attach требует независимый Native login + новый OIDC proof; простое наличие Root account/profile не создаёт link.

Этот opt-in не меняет public Std1/2 маршруты, request/response grammar, Basic TTL или Root broker. Правила Source auth должны явно разрешать linked-login; стандартный Native hook остаётся deny-default. Source только выбирает по проверенному Root expected issuer/sub; callback всё равно независимо доказывает фактическую identity.

Проверки: две независимые Native realms; реальный new-empty→write/feedback→Basic expired→NEW OIDC→same principal/grants/data; Source restart; changed/revoked/expired Native session, membership, Source key, Root actor/device/target и чужой Root subject отказывают. Captured candidate не может читать/писать/обрабатывать до OIDC. Unknown exchange требует нового явного входа; unknown COMMIT — readonly exact completion того же original slot, без code replay. Concurrent callbacks/два OS writers не создают второй principal/Native grant.

## B. Finite24h consumer — отдельная schema review

Std2 уже объявляет `finiteSeconds:86400`, fixed session-continue route и closed continuation ACK с `renewable:boolean`. Source BFF имеет optional rpSessions port, но ни ordinary Source, ни session creation пока не создают durable RP marker: Long не реализован. Это позволяет рассмотреть совместимый **trusted constructor opt-in**, не менять frozen Std1/2 pins. Решение окончательное только после проверки реального Root resume body/cookie port и Native consent duration semantics; новый wire/redirect/permission смысл потребует отдельного immutable profile.

Native Source format4 предлагается отдельно: encrypted RP heads/CAS states/one-use resume anchors+receipts в собственном namespace, literal reader4 и compatible reader3/4 baseline **до4writes**. Source3 rows/cipher/Native IDs/FKs неизменны. Storage port использует exact RP49; network вне SQL, current/final Native authority inside transactions. Hard deadline = original login start+24h, без sliding; expired/revoked Native authority не возрождается RP head. Unknown RT delivery consumes oldRT permanently, no takeover/rerun.

Для нового Root5min slot нужен fresh originalActor/device/issuer/sub/current approved source/resource/pins плюс current Native grant и актуальный RP userinfo. Old cookie/anchor/locator только находит record; Source final SQL решает право. One-use receipt подтверждает exact requestId/intent. Source long marker не делает Basic renewable; смена target/identity/profile/resource отказывает или требует нового явного consent. Semantic consent duration/purpose должен явно учитывать trusted Long opt-in; старое согласие нельзя молча сделать длиннее.

## C. Production packet после отдельного review

Планируется standalone ordinary Source image Node24 +pinned openid-client6.8.4 +portable SDK, private closed trust-config loader, defaultoff/Basic300. Конфигурация содержит exact issuer/client/callback/embed/parent/resource/transport, secret-file references и source key; никаких app-request/body/header конфигураций. Выводятся только safe IDs/status/pin SHA. Native origin должен быть browser-reachable: public reviewed HTTPS либо разрешённый local-loopback native profile. SSH Dev localhost не является браузерным localhost удалённого человека. Native origin = Root embed не допускается без отдельного fixed Native entry mapping.

Image/reader/cold packet должен проверить current+compatible baseline, explicit initialization/migration, actual disk-backed encrypted cold backup/restore/key equality, current Native grants/receipts, refusal old reader BEFORE START. RAM fixtures не заменяют этот gate. Публикация/RUN/модели/пользовательские данные не включены в этот план; ProductionReady=false до фактических проверок.
