# Общий голос Сот: операторский запуск и диагностика · 20.10.2026

Область: Source-кандидат Realtime orb v2, MANIFEST b3ac24e3d25f5c7244e862c8c4ce1c3153641dbec8f37e8eed417b325ae740f4. Root измерил component17/17, installation6/6, controller12/12, SDK8/8, независимый real-SQLite crash replay1/1; прежний SDK counterfactual0/8. Evidence: C:/Users/Junio/.codex/tmp/soty-voice-v2-first-root-20261020-c9cda35da978. Это синтетические provider/media tests. Применение в репозиторий, реальные права модели, микрофон, слышимая речь, расходы и deployment этим не подтверждаются.

Сервис по умолчанию выключен. Единственный реальный installed путь: server/index.js → createRootStartup → loadVoiceOrbConfiguration → createHttpApp → signed voice.orb.*. Для operator enablement нужен SOTY_VOICE_ORB_ENABLED со строгим значением1 и явно указанный абсолютный SOTY_VOICE_ORB_CONFIG_FILE. Значенияtrue/пробел1 отвергаются. При0/пустом flag файл конфигурации даже не читается. Этот документ не включает сервис, не устанавливает частный профиль и не даёт нового разрешения потратить деньги.

В config schema soty.voice-orb-installed.v1 обязательны profiles; maxDurationMs опционален, default180000, допустимо15000–900000. В каждой записи требуются tenantId/accountId/deviceId, apiKey/projectId собственного явно разрешённого OpenAI проекта, authorizationId/validUntil/maxStarts. maxStarts —1–1000; profiles —1–128. Ключ не поступает из browser и не возвращается в status. Filename абсолютный, regular, bounded64KiB, UTF-8, без symlink там, где OS обеспечивает O_NOFOLLOW; POSIX owner+private mode проверяются. На Windows документирование private файла не доказывает ACL: доступ файла обеспечивает владелец развёртывания. Не выводить файл, ENV, ключ, Authorization headers или полные дампы коннектора; пример разрешения можно проверять в памяти, выводя только выбранные safe поля/булевы итоги.

Текущий own-profile slice связывает запись с конкретным account/device и запрещает shared key/project между разными owner-профилями. Это временное ограничение реализации. Пользователям массовых Сот не предполагается выдавать обязанность заводить собственный OpenAI ключ. Следующий service onboarding должен иметь явно принадлежащий Сотам project, verified account admission, per-account/global лимиты и финансовый учёт. Такое shared service владение/финансирование сейчас не установлено. KUF/NativeVoice, чужие проекты, старые бюджеты и разрешения не используются как fallback.

GLM worker отдельно должен быть подключён через существующую personal-agent configuration, с фиксированным zai-org/GLM-5.3-Flash, autoFallback:false и подтверждёнными gateway receipts. Один только voice operator profile не подключает GLM, не даёт доступ к приложениям и не выдаёт per-turn Notes permission. Voice model фиксирована gpt-realtime-2.1, сложный worker — GLM. Responses text backend отсутствует; speech interpretation всё равно имеет собственные вычисления и стоимость. Выгодность этого разделения ещё не измерена.

## Как читать safe status

Читать через существующий signed Connect voice.orb.status в правильном аккаунте; не копировать signing keys и не давать general RPC сторонним страницам. Проверять право → фактический status → допустимый вызов → реальное сохранение/readback. Возможность вызова не является разрешением на выполнение.

| Наблюдение | Что означает | Что делать |
| --- | --- | --- |
| configured:false, missingBinding:not_enabled | Operator не включил этот голосовой сервис. | Оставить текстовый помощник. Не просить у человека ключ и не запускать автонастройку. |
| missingBinding:voice_transport_not_bound или voice_quota_not_bound | Trusted runtime не получил transport/admission. | Исправляет оператор; никакого browser fallback. |
| configured:false при null missingBinding | Например, отсутствует настроенный personal-agent worker. Поле missingBinding пока не различает эту причину. | Проверить safe status существующего personal-agent отдельно; не утверждать, что голосовая модель сломана. |
| configured:true, canStart:false, quota.allowed:false | Backend bound, но для этого exact account/device нет действующего authorization или закончилась квота. | Не делать POST. Проверить actor/profile/expiry/reservedStarts; не очищать ledger и не придумывать деньги. |
| configured:true, canStart:true | Runtime binding и конечная квота позволяют попробовать отдельный явный старт. | Это не доказательство ключа, прав модели, оплаченного баланса, физического mic или полного voice UX. |
| Signed status не удалось подтвердить | Сеть, профиль, срок права или сервис недоступны; причина ещё не установлена. | Показать «Голос сейчас недоступен. Доступен текстовый помощник». Не переименовывать в «не настроен». |
| missingBinding:closed | Этот process-owner уже закрывается/закрыт. | Не создавать новый provider call в старом owner. |

wholeVoiceReady:false и paidActivationVerified:false специально остаются false: unit evidence не считается live activation. Они не являются самовключающимся feature flag. UI v2 пока использует общий текст «Голос пока не подключён» для всех configured:false; это известная copy limitation, хотя controller хранит voice_quota_unavailable отдельно. Не описывать её как уже исправленную в UI.

## Квота и неизвестный исход

SQLite dataDir/voice-orb-quota.sqlite содержит persistent reservation и attempted claim до provider POST. Ключ записи — hash actor/project/requestId, payload/intent digest; raw audio/task/key в ledger не сохраняются. maxStarts ограничивает число запусков, maxDurationMs — время собственного session-owner. Это не денежный бюджет, hard-dollar ceiling или settlement. Большие/поздние provider расходы не доказаны этим счётчиком. Unknown запрос остаётся reserved; удаление базы, смена authorizationId ради обхода или автоматический retry новым requestId не являются recovery.

Одинаковый signed start request нельзя использовать для второго платного POST даже после RAM eviction/restart. Crash после attempted1 без outcome оставляет unknown. Losing replay не имеет права записать no-attempt outcome оригинального владельца. Исправление проверено real file SQLite close/reopen/readback независимым тестом Root. Наблюдаемые response token counts — не финальная сумма за session.

End останавливает свои browser tracks/peer, отменяет owned GLM/sideband и пытается POST hangup для точного provider call. HTTP200 hangup лишь принимает teardown. remoteTerminationVerified/accountingFinalized/playbackVerified остаются false. «Неизвестно» нельзя заменять «всё закрыто/ничего не списано»; automatic retry отсутствует. Если browser был принудительно убит или потерял сеть, сохранены server deadline и попытка owner cleanup, но удалённое завершение/расчёт этим не доказаны.

## Пользовательское действие и authority

Mic включается только явным click «Говорить»/orb. Выбор operator profile не означает consent на запись или создание Notes. Доступ приложений проверяет существующий runtime на каждом действии. Одноразовый narrow Notes flag выдаёт signed UI, а не JSON speech tool. Существующий checkbox — промежуточный разрешённый slice; полная hands-free цепочка со снятием неоднозначности без ручной галочки ещё не закончена. OTsov preview/create и остальные tools не считаются voice-ready автоматически.

При account/device switch, revoke, unmount, pagehide и explicit End нужно освобождать только свои media и original session. Opaque voiceSessionCleanup callback нельзя сериализовать, переназначить на другой target или превратить в новый task. Server authority/revocation fence сохраняется. На новом аккаунте старый результат не отображается. Если физическая сеть недоступна, UI сообщает неопределённый исход, не обещает удалённый stop.

## Как проверять включение позже

До будущего live запуска необходим явно разрешённый владелец проекта/расхода, действующий exact actor и worker account. Подготовить private config вне Git, безопасно проверить поля локально и включить только разрешённый deployment. Само включение ещё не модельный запрос. Затем один согласованный физический mic → завершённый voice function → GLM receipt → actual Notes write/readback → слышимый ответ → End, с проверкой прерывания/смены аккаунта и доступным operator расходом. Не заменять эту проверку unit mocks. Этот документ сам такого разрешения не выдаёт.
