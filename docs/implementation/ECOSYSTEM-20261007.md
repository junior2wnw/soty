# Реализация экосистемы Сот

Поручение: одно понятное поле для людей, групп, приложений, устройств и игр; «Моё» содержит личный выбор, «Поиск» показывает доступные публичные проекты. Пользователь сам выбирает пространство для приложения. Старые домены, данные, ключи устройств и входы сохраняются.

## Порядок работы и приёмки

Каждый пункт проходит: факты и пользовательский сценарий → решение и ограничения → реализация → независимый review → проверки ошибок и повторного входа → визуальная проверка → исправления → повторная проверка. Пункт считается внедрённым только после проверки текущего публичного выпуска. Unit tests, макет и живой пользовательский сценарий учитываются отдельно.

Состояния: `pending`, `implementing`, `reviewing`, `verified-component`, `verified-integration`, `deployed`. Неисполненный сценарий не получает PASS. Критерий «100 из 100» означает выполнение согласованных проверяемых требований, а не доказанное мировое первенство.

## Выпуск 1: добавление приложения на личное поле

Внедрён 07.10.2026: `25b3d482e7da7b3b7c7f8b2ca51cb6af21bf9c63`, immutable image `sha256:e0486fef91f2eb09220a2afae964629151f87d4b4dd007141a627df29699555a`. Живой HIVE проверен на desktop и 320 px; сохранение выбранного пространства, возврат и отсутствие дубля подтверждены. Первый выпуск был отклонён из-за сброса пространства; автоматический откат с сохранением принятого ярлыка проверен, дефект исправлен. Backend и схема не менялись. Свидетельства: `D:/соты/output/soty-implementation-20261007/release-r2/deploy-proof/` и `research/UI-RELEASE-ACCEPTANCE.md`.

1. Подтвердить базовый выпуск и сохранность рабочего дерева. База: 698a75f82ba9780494e10e1bf060781e94095b5e. Отдельная ветка codex/soty-ecosystem-20261007; исходные и соседние незавершённые изменения не переносить.
2. Разделить две операции: личный ярлык сущности и закладка конкретного входа. Основной «+» выбирает пространство; закладка доступна в меню приложения. Отдельный ярлык не даёт доступа и не публикует приложение.
3. После регистрации использовать canonical app ID подписанного ответа. Сообщение «Проект подключён»; затем выбор личного пространства. Не выдавать регистрацию за добавление ярлыка.
4. Устранить дубли в одном пространстве; разрешить ярлыки той же сущности в разных пространствах. Сохранять прежнюю расстановку и собственные старые закрепления при первом входе.
5. Проверить двойной клик, потерянный ACK, offline, quota, другой аккаунт, другую вкладку, исчезнувшее пространство, конфликт и уже принятый, позднее удалённый ярлык.
6. Не терять работающий iframe, черновик и поисковый контекст. Несохранённый выбор участвует в общих Back/reload/update guards. После закрытия offline намерение остаётся в журнале своего аккаунта; восстановление связи повторяет исходное намерение.
7. Проверить desktop, 320/390 px, landscape, клавиатуру, фокус и fullscreen. Обычный пользователь отдельно оценивает понятность «поле» и «закладка».
8. Typecheck, связанные проверки, весь world suite и production build. Совместимость старого хранилища, зашифрованный backup, холодный откат и неизменность старых таблиц проверяются до выкладки.
9. Commit/push → неизменяемый образ → SSH Dev → проверка публичной страницы и реального HIVE. Не смешивать выпуск с миграцией незавершённого U1.

## Выпуск 2: проекты SSH Dev в «Поиске»

1. Снять свежий inventory реальных процессов, источников, health и публичных доменов. Уже зарегистрированные HIVE/NFC переиспользовать; разные владельцы остаются разными владельцами.
2. Для каждого проекта: старый вход → карточка «Поиска» → предварительный просмотр → открыть → добавить в своё пространство → вернуться → повторно войти. Приватные кабинеты/проекты не становятся публичными из-за публикации карточки.
3. Первыми квалифицировать Тавыш и Переметрику, затем Поведай и остальные действующие пользовательские сайты. Не считать 502 готовым приложением. Отсутствующий Planner сначала развернуть с правильным входом; Scope оставить ограниченным до квалификации его публичной границы.
4. Подтвердить source через уже принадлежащий пользователю connector. Owner operations выполнять существующим подписывающим Connect-клиентом; connector bearer не заменяет владельца. Не править реестр/owner через SQL.
5. Начальная зона — фактически работающая 4-2.рф. Предложенные имена: tavish.4-2.рф, perimetrica.4-2.рф, poveday.4-2.рф. Приблизительное «42.рф» не доказывает владения новой зоной.
6. Изоляция по умолчанию. Native alias допускается после cookie/OAuth/CSRF/CSP/WebSocket/storage проверки. Сохранить старый ingress и данные; не заменять старый домен принудительным redirect. Новая origin не получает старый IndexedDB или non-extractable private key автоматически.
7. Отдельно сверить domain claim, source digest, revision, epoch, whole-port exposure acknowledgement и listing. Exact repeat безопасен; потерянный ответ сверяется с receipt, а не повторной регистрацией нового проекта.
8. Каждый проект получает отдельное свидетельство old/new route, auth/data continuity, embed/browser и отката. Только после него следующий проект публикуется.

## P0: доверие, память, деньги и исполнение

| Подплан | Пункты и обязательная проверка | Текущее состояние |
|---|---|---|
| P0.1 Identity → inference tenant | Connect principal; account budget; request identity; revoke/ABA/replay | verified-component в отдельной commerce ветке; host/RP/CSRF integration pending |
| P0.2 Device ceremony/vault | отдельные signing/wrapping ключи; proof of possession; generation CAS; device revoke | pending integration |
| P0.3 Gateway | bounded leases; общий reserve/settle; unknown held; supplier reconciliation; quotas/revoke | удержания и reconciliation core verified-component; trusted supplier worker, historical pending и leases pending |
| P0.4 Personal memory | неизменяемый scope; trusted admission; erase/tombstones; durable restore floor; cross-account и replay | verified-component; production admission не подключён |
| P0.5 Executor | key-free guest; approved roots/egress; resources; cancellation stop receipt; no host fallback | pending |

## P1: личный агент и единый UX

| Подплан | Пункты и обязательная проверка | Текущее состояние |
|---|---|---|
| P1.1 GLM | exact zai-org/GLM-5.3-Flash; bounded stream assembly; validated tools; abort/unknown usage; no implicit retry | verified-component; live transport/controller/paid dispatch pending |
| P1.2 AIST/U1 | existing envelope1.0; trusted profiles; existing grants/effect ledger; version pins; data_only | proposal examples verified; integration pending |
| P1.3 Controller | task identity; resource/model budgets; U1 dispatch; domain proof; idempotent resume | pending |
| P1.4 Memory UX | «Запомнить»/«только здесь»; audience; export/delete; stale sources | pending |
| P1.5 Поле/чат | placements и entity отдельно; search visibility; Telegram-понятные controls; drafts; mobile/focus | placements release 1 deployed; последующие agent/voice сценарии pending |

## P2: разговор

1. P2.1: выделить transcript/turn/epoch/scheduler из реального voice donor; не переносить чужой business context. Текстовый GLM brain и audio provider — разные компоненты.
2. P2.2: live call admission на account/device/membership; проверять удаление и повторный вход со старым JWT; TURN budget отдельно.
3. P2.3: компактная кнопка звонка; микрофон выключен до явного действия; прослушивание, mute, завершение, приглашение агента и запись — отдельные понятные состояния. Чат и участие не дают агенту tool authority.
4. P2.4: STT/GLM/TTS как отдельный qualified stack; аудио/music/call focus и interrupt; reconnect и лимит расходов.
5. Gate: два реальных устройства, индивидуальный/групповой/agent call, revoked membership, потеря сети, account switch и cancellation. Сейчас pending; dictation не выдаётся за звонок.

## P3: предметные adapters

1. Общий descriptor/skills/resources поверх AIST и U1. Descriptor — описание, не разрешение. Pins и results имеют точные версии/источники.
2. Шахматы: GLM предлагает только легальный ход; chess.js проверяет позицию, очередь и expected ply; отмена и stale result не меняют игру.
3. HIVE/Notes: scoped чтение/изменение; текущая ревизия; знания не расширяют аудиторию; shared ledger для результатов.
4. Переметрика: документы/версии/строгие grants; приватный API не открывать всем посетителям карточки.
5. Planner/Scope: процессы планирования и просмотр исходников — разные поверхности; approved roots и owner boundary.
6. Тавыш: голос/music focus, playback/manual choice и content rules; участие в разговоре не включает фоновое распознавание музыки.
7. Поведай: публичный отзыв, приватный кабинет и модерация; автора нельзя подменять агентом.
8. Каждый adapter проходит подготовку, authorization, apply, доказанный результат, повтор и rollback. Все подключаемые effects пока pending.

## P4: подключение любого сайта и Transit

1. Onboarding: существующий проект/устройство → source proof → limited private preview → понятный адрес → public/listed отдельным действием → добавить себе. Старый домен сохраняется.
2. Transit adapter: квалифицировать текущую реальную версию, transport integrity/limits/reconnect/revoke; исторический тест не доказывает live readiness.
3. Четыре отдельные роли: публикация приложения, relay, Internet exit и TURN. Каждая имеет consent, caps и kill switch; Internet exit выключен по умолчанию.
4. Gate: новый разработчик и внешний агент подключают bounded пример без скрытых привилегий; unavailable source/expired claim/lost ACK не создают ложную публикацию. Pending, кроме существующих app hosting механизмов.

## P5: commerce

1. Supplier settled costs и unknown accounting; estimate не выдаётся за счёт поставщика.
2. Реальный wallet: canonical account, reserve/settle, пополнение/возврат/лимиты, отсутствие demo-credit в public acceptance.
3. External resale: собственные ограниченные gateway keys; raw upstream bearer не становится пользовательским ключом через шифрование; aggregate caps и abuse isolation.
4. Платные acceptance calls и финансовые операции требуют отдельно определённого пилота/лимита. Код можно подготовить без оплаты. Pending.

## P6: обнаружение, картинки и итоговый аудит

1. Смена основной origin — отдельная миграция с доступом со старого домена, recovery/pairing и сохранением данных; не подразумевается текущими alias.
2. Public directory, app cards, manifest/schema docs, bounded agent onboarding и понятные недоступные состояния; приватные поля не индексируются.
3. Existing app-art pipeline: profile → generative draft → import → multi-width QA → versioned asset → manifest. Рабочий pipeline переиспользовать.
4. Images/video providers подключаются через те же budget/admission/data policies; расходы/retention/progress/cancel не обходят P0.
5. Итоговый аудит всей системы: реальные люди + synthetic personas, телефоны/desktop, агенты/разработчики/владельцы/модераторы; accessibility, скорость, безопасность, нагрузка, disaster recovery. Повторить все применимые acceptance сценарии; не заменять реальные проверки отзывом модели.

## Неизменяемые границы

Нет upstream provider secret в браузере/guest; нет новых SQL owners/сочинённых grants; нет автоматического pin всех проектов каждому человеку; нет публичного private backend порта; нет paid smoke по умолчанию; нет fake activity; нет удаления старых origin/state. Незавершённый U1 имеет отдельный data reader/rollback gate.

Подробная исследовательская спецификация, источники, 71 будущий интеграционный сценарий и prior decisions: D:/соты/output/soty-agent-ecosystem-20261007/. Фактический журнал текущей реализации и свидетельства: D:/соты/output/soty-implementation-20261007/. Эти артефакты не являются доказательством внедрения всех P0–P6.
