# Общий голос Сот: операторский запуск и диагностика · 20.10.2026

Область: общий операторский голосовой профиль `soty.voice-orb-installed.v2`. Описанный ниже общий сервис Source V2 (исходный MANIFEST `5860a795d0508f527673ed7129e6305da5265357019702d819787708736c5637`, итоговая квота SHA256 `267243d39f6386682f3467e1b323a046ed45b853687e3a80929f89a14b1da65d`) применён локально в Root и M02. В обеих копиях прошли 11 проверок общего профиля, 5 проверок инициализации SQLite и 3 проверки владения исходом запуска. Это не публикация и не доступность пользователям; сервис остаётся выключенным. Исходный Realtime orb v2 ранее прошёл синтетические component17/17, installation6/6, controller12/12, SDK8/8 и независимый SQLite crash replay1/1 (evidence `soty-voice-v2-first-root-20261020-c9cda35da978`). Эти результаты не являются проверкой нового shared profile, настоящего микрофона, слышимой речи, прав модели, расходов или deployment.

Сервис по умолчанию выключен. Путь запуска: server/index.js → createRootStartup → loadVoiceOrbConfiguration → createHttpApp → signed voice.orb.*. Для включения нужны строго `SOTY_VOICE_ORB_ENABLED=1` и явно указанный абсолютный `SOTY_VOICE_ORB_CONFIG_FILE`. Значения `true` и ` 1` отвергаются. При `0` или пустом flag файл даже не читается. Этот документ не включает сервис, не устанавливает частный профиль и не даёт нового разрешения на расходы.

Пользователь Сот не должен заводить личный OpenAI ключ или проект. В новой схеме V2 оператор явно устанавливает один отдельный профиль голосового сервиса. Сам файл не подтверждает владение проектом и не является разрешением на расходы.

| Схема | Обязательные поля и допуск |
| --- | --- |
| `soty.voice-orb-installed.v2` | `service`: tenantId, serviceId, apiKey, projectId, authorizationId, validUntil, maxStarts, maxAccountStarts. `admission`: явная policy `soty-authenticated-accounts`, connectProjectId `soty`. maxStarts — общий конечный лимит 1–1 000 000; maxAccountStarts — 1–maxStarts для одного аккаунта на всех его устройствах. validUntil должен быть будущим и покрывать всю запрашиваемую сессию. |
| `soty.voice-orb-installed.v1` | Совместимость прежних own profiles: profiles 1–128, в каждой записи tenantId/accountId/deviceId, собственные apiKey/projectId, authorizationId/validUntil/maxStarts 1–1000. Запись привязана к account/device; общий ключ/проект между разными владельцами отвергается. Это временный технический путь, не требование к массовому пользователю. |

В обеих схемах maxDurationMs опционален: default 180000, допустимо 15000–900000. OpenAI projectId — явно установленное оператором значение `proj_*`; имя не доказывает владение. admission.connectProjectId `soty` означает только существующий Root Connect namespace, не OpenAI проект. service.tenantId — фиксированный billing tenant оператора; это не tenant данных приложений пользователя. Tenant, ключ, модель, admission и права нельзя назначить аргументами voice RPC.

Ключ не поступает из browser и не возвращается в status. Filename абсолютный, regular, bounded64KiB, UTF-8, без symlink там, где OS обеспечивает O_NOFOLLOW; POSIX owner+private mode проверяются. На Windows документирование private файла не доказывает ACL: доступ файла обеспечивает владелец развёртывания. Не выводить файл, ENV, ключ, Authorization headers или полные дампы коннектора; проверять локально только выбранные safe поля/булевы итоги. KUF/NativeVoice, личные ключи других людей, чужие проекты, старые бюджеты и разрешения не используются как fallback.

GLM worker отдельно должен быть подключён через существующую personal-agent configuration, с фиксированным zai-org/GLM-5.3-Flash, autoFallback:false и подтверждёнными gateway receipts. Один только voice operator profile не подключает GLM, не даёт доступ к приложениям и не выдаёт per-turn Notes permission. Voice model фиксирована gpt-realtime-2.1, сложный worker — GLM. Responses text backend отсутствует; speech interpretation всё равно имеет собственные вычисления и стоимость. Выгодность этого разделения ещё не измерена.

В shared V2 до чтения квоты и старта проверяется текущий оригинальный signed Connect actor и внутренний read-only GLM eligibility port. Port повторно использует тот же установленный transport guard, не получает credential, не вызывает модель и не создаёт task или RPC. Отсутствующий/ложный/неподтверждённый port закрывает старт. Известно запрещённому GLM аккаунту не создаются reservation или OpenAI session. Прежняя V1 voice конфигурация нового port не требует.

Это не всеобщий onboarding: существующая personal-agent installed V1 отдельно ограничивает допуск своим accountIds ≤128. Shared voice не расширяет этот список, не выдаёт новые Source/apps права и не подключает человеку worker автоматически. Допуск всех пользователей через Portal/self-service остаётся открытой задачей. Также сохраняются 128 одновременных in-memory сессий, линейный indexed COUNT и один SQLite writer; отдельные копии ledger на разных Root не образуют общий лимит.

## Как читать safe status

Читать через существующий signed Connect voice.orb.status в правильном аккаунте; не копировать signing keys и не давать general RPC сторонним страницам. Проверять право → фактический status → допустимый вызов → реальное сохранение/readback. Возможность вызова не является разрешением на выполнение.

| Наблюдение | Что означает | Что делать |
| --- | --- | --- |
| configured:false, missingBinding:not_enabled | Operator не включил этот голосовой сервис. | Оставить текстовый помощник. Не просить у человека ключ и не запускать автонастройку. |
| missingBinding:voice_transport_not_bound или voice_quota_not_bound | Trusted runtime не получил transport/admission. | Исправляет оператор; никакого browser fallback. |
| configured:false при null missingBinding | Например, отсутствует настроенный personal-agent worker. Поле missingBinding пока не различает эту причину. | Проверить safe status существующего personal-agent отдельно; не утверждать, что голосовая модель сломана. |
| configured:true, canStart:false, quota отсутствует | В shared V2 текущий actor не прошёл GLM eligibility либо отсутствует trusted port. | Проверить signed actor, actual Source port и существующий GLM account allowlist. Не запрашивать пользовательский OpenAI ключ и не расширять права автоматически. |
| configured:true, canStart:false, quota.allowed:false | Нет действующего authorization или исчерпан лимит: в V1 для account/device, в V2 для аккаунта на всех устройствах либо для всего сервиса. | Не делать POST. Проверить actor/profile/expiry/reservedStarts; не очищать ledger и не придумывать деньги. |
| configured:true, canStart:true | Runtime binding и конечная квота позволяют попробовать отдельный явный старт. | Это не доказательство ключа, прав модели, оплаченного баланса, физического mic или полного voice UX. |
| Signed status не удалось подтвердить | Сеть, профиль, срок права или сервис недоступны; причина ещё не установлена. | Показать «Голос сейчас недоступен. Доступен текстовый помощник». Не переименовывать в «не настроен». |
| missingBinding:closed | Этот process-owner уже закрывается/закрыт. | Не создавать новый provider call в старом owner. |

wholeVoiceReady:false и paidActivationVerified:false специально остаются false: unit evidence не считается live activation. Они не являются самовключающимся feature flag. UI v2 пока использует общий текст «Голос пока не подключён» для всех configured:false; это известная copy limitation, хотя controller хранит voice_quota_unavailable отдельно. Не описывать её как уже исправленную в UI.

## Квота и неизвестный исход

SQLite dataDir/voice-orb-quota.sqlite содержит persistent reservation и attempted claim до provider POST. Ключ записи — hash actor/project/requestId, payload/intent digest; raw audio/task/key в ledger не сохраняются. maxStarts ограничивает число запусков, maxDurationMs — время собственного session-owner. Это не денежный бюджет, hard-dollar ceiling или settlement. Большие/поздние provider расходы не доказаны этим счётчиком. Unknown запрос остаётся reserved; удаление базы, смена authorizationId ради обхода или автоматический retry новым requestId не являются recovery.

Для V2 общий service и account счётчики проверяются одной SQLite transaction до единственной reservation. Новое устройство, смена OpenAI ключа/проекта, authorizationId или дат не сбрасывают накопленные старты. При обычной ротации сохранять tenantId/serviceId и ledger; их смена означает другую явно выбранную область, а не recovery. remainingStarts текущего аккаунта может быть меньше authorizedStarts−reservedStarts, если исчерпан общий сервис. Изменение допуска/лимитов — действие оператора, не новое платёжное разрешение.

Одинаковый signed start request нельзя использовать для второго платного POST даже после RAM eviction/restart. Crash после attempted1 без outcome оставляет unknown. Losing replay не имеет права записать no-attempt outcome оригинального владельца. Исправление проверено real file SQLite close/reopen/readback независимым тестом Root. Наблюдаемые response token counts — не финальная сумма за session.

End останавливает свои browser tracks/peer, отменяет owned GLM/sideband и пытается POST hangup для точного provider call. HTTP200 hangup лишь принимает teardown. remoteTerminationVerified/accountingFinalized/playbackVerified остаются false. «Неизвестно» нельзя заменять «всё закрыто/ничего не списано»; automatic retry отсутствует. Если browser был принудительно убит или потерял сеть, сохранены server deadline и попытка owner cleanup, но удалённое завершение/расчёт этим не доказаны.

## Совместимость и откат

V2 использует существующую SQLite таблицу и индекс без миграции, сохраняет V1 записи/счётчики. Старый V1 reader строго отвергает V2 config до вызова provider; новый reader сохраняет прежнюю V1 ветку. Source V2 применён целиком с eligibility port и локально проверен. Разрешённый deployment и физический голос проверяются отдельно; до этого оставлять сервис OFF.

Для отката выключить сервис; вернуть совместимый Source и только явно сохранённый разрешённый V1 config, если нужен прежний режим. Сохранить ledger и приватный профиль; не переупаковывать shared credential в пользовательские V1 profiles, не удалять базу и не запускать автоматическую выдачу ключей. Этот документ не выполняет ни миграцию, ни настройку, ни включение.

## Пользовательское действие и authority

Mic включается только явным click «Говорить»/orb. Выбор operator profile не означает consent на запись или создание Notes. Доступ приложений проверяет существующий runtime на каждом действии. Одноразовый narrow Notes flag выдаёт signed UI, а не JSON speech tool. Существующий checkbox — промежуточный разрешённый slice; полная hands-free цепочка со снятием неоднозначности без ручной галочки ещё не закончена. OTsov preview/create и остальные tools не считаются voice-ready автоматически.

При account/device switch, revoke, unmount, pagehide и explicit End нужно освобождать только свои media и original session. Opaque voiceSessionCleanup callback нельзя сериализовать, переназначить на другой target или превратить в новый task. Server authority/revocation fence сохраняется. На новом аккаунте старый результат не отображается. Если физическая сеть недоступна, UI сообщает неопределённый исход, не обещает удалённый stop.

## Как проверять включение позже

До будущего live запуска необходим явно разрешённый владелец проекта/расхода, действующий exact actor и worker account. Подготовить private config вне Git, безопасно проверить поля локально и включить только разрешённый deployment. Само включение ещё не модельный запрос. Затем один согласованный физический mic → завершённый voice function → GLM receipt → actual Notes write/readback → слышимый ответ → End, с проверкой прерывания/смены аккаунта и доступным operator расходом. Не заменять эту проверку unit mocks. Этот документ сам такого разрешения не выдаёт.
