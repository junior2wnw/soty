# Приёмка единой системы Сот

Обновлено 07.10.2026, 10:18 UTC. Общая программа ещё не завершена. Код, проверенный сценарий и внедрение учитываются отдельно. Production и исходные рабочие каталоги не изменялись.

Текущий Root runtime: `2bb9bfca77eb2323eec0e5d68d43caba05d0f563`. Последняя полная Root image + encrypted cold restore приёмка относится к прежней `56fe2cb`; она не подтверждает готовность текущего выпуска. Новая Linux сборка пока останавливается на 15-секундной проверке запуска Source. Срок, SQLite durability и полномочия не ослаблялись.

## Пользовательский результат

| Требование | Создано и проверено | Что требуется до приёмки |
| --- | --- | --- |
| Один способ подключения своих и сторонних приложений | U1 contract, standard-resource adapter, неизменные Standard1/2 pins; Source formats1/2/3 и host-only async Native authority SPI. Независимый installed Source suite: 60/60 PASS, без пропусков. Combined connector1.4.7 | Выдача доверенного auth профиля, проверка владения, понятная установка автора, настоящий browser-reachable Native HTTPS origin или broker. Loopback fixture не подтверждает чужой сервер |
| Вход без регистрационной формы | Maintained OAuth Code/PKCE, отдельные issuer/sub, исходный профиль Сот, точные redirect, связывание прежней идентичности только с согласия, отзыв устройства. Secure resume-cookie исправлена; actual HTTPS тест проходит | Общий пользовательский путь для каждого Source, все QR/recovery варианты, потеря всех устройств, отвязка Source. Общий вход сам по себе не выдаёт Native права |
| HIVE внутри Сот | Source `689794b`, actual Editor и Native проект. Fresh immutable d7/689 Chrome после реальных 300 секунд сохранял описание и перемещение; Native IDs/ACL/head/grant сохранялись, Source продолжение подтверждалось. Обычный цикл дал 2415 userinfo200 и 0 rate429. Independent current composition: 12/12 PASS | Fresh browser на Root2bb9: голос, отправка, перезапуск, отзыв, смена профиля, мобильный экран. Отдельный production Node/D1/vinext image +0011 cold restore. Редкие прежние Human400, projectGET401 и ticketGET403 пока не объяснены |
| Планировщик внутри Сот | Source `16d3c2f`; Root d22 actual installed + real wall175sec positive/revoke/unknown: 13/13 PASS, без пропусков. Basic не восстанавливается после истечения; lost ACK и restart учитываются | Source production image/config loader/cold restore и совместная браузерная проверка на окончательном Root |
| Частная обратная связь каждого проекта | Общий private-project contract, HIVE D1/voice/PNG/support/reporter verification/restart; Source3 widget и явные consent + Native owner processor grant. Source-only Reader3/2→3 migration и durable job/receipt состояния | Включение каждым автором по настоящим Native правам; Source3 Linux/cold; реальный ограниченный процессор и агентская очередь. Root владелец и автор сообщения не становятся Native owner автоматически |
| Запись голоса и снимок | User gesture → preview → Use → отдельная Send; bytes в RAM и точная привязка peer/account/project. Fresh d7 Chrome автоматически остановил синтетическую запись120с; мобильный dialog не выходил за390px. Root2bb9 исправляет CSP для media preview; независимые28/28 PASS и TSC | Полный fresh natural120с Preview/Play/Use/Send после CSP fix, Source persistence/restart/privacy. Прежний preview readyState0/error4 был реальным отказом и не считается PASS. Visual revoke действует по текущему30с polling |
| Расшифровка, OCR и обработка агентом | Closed local-only job contract, encrypted context, immutable receipt, no auto takeover/rerun; shared running service реально отменяет установленный Native executor. Отдельный Linux probe8/8 подтвердил CPU/wall/cancel/scratch/FS/noNet/nochild bounds | Native job → реальный OS bridge, отзыв другого процесса, fresh final permission и Source3 cold. Выбор моделей, качество, реальные бюджеты120с аудио. Probe не подтверждает готовность ASR/OCR; Windows host пока not-ready |
| Отзывы о приложении, проекте и человеке | Native0036/typed scopes и прежний RP49 сохранены; joint PG26 actual runtime + two OS + unknown/revoke + encrypted cold PASS. Отдельный Source Reader3-off image b677 построен и offline проверен | Новый0037 NativeSQL/final transaction fence, Source3 runtime, два OS writer, revoke/expiry rollback и unknown receipt; текущий Root Apps/HTTPS, UI всех трёх subjects, отдельная publication и Source image/cold |
| Агент понимает проекты, MCP и skills | Точный разрешённый catalog, readonly ledger, Native grant/revoke, actual Planner HTTP/MCP, content-addressed guidance. Peremetrika fixed-GET selected port: Main wire5/5, Native41/41 PASS | Peremetrika shared login/UI/durable primary/remote broker, остальные adapters; межприложенческая задача с явными write grants, результатом, отменой и локальным исполнителем |
| Человек, агент, разговор и задачи | Прежние чаты/Notes/Apps сохранены; Native Notes effect ledger работает | Законченный совместный сценарий разговора → задача → согласованное действие → результат; ответственность, отмена, передача локальному/серверному исполнителю |
| Простота и масштабирование | Один Source contract; Source владеет данными и ACL; новые авторы не получают собственный Root DDL. Apps8 reader/fallback/encrypted restore прошли прежнюю56fe приёмку. Current userinfo quotas600/min client+account,9600/min host,2048 slots; нет permission cache | Текущий полный совместимый release, измеренные capacity/SLO/стоимость, понятное заполнение квот, export/offboarding, load и независимые failure scenarios. Пилотные64 profiles/100 apps/256 slots не доказывают неограниченный рост |

Первый законченный межприложенческий сценарий соединяет HIVE, Планировщик и Переметрику: один человек, отдельные текущие Source права, чтение разрешённых данных, два согласованных изменения, видимый результат и обращение к нужному владельцу. TRANSIT получает разрешённый локальный контекст и обзор статуса; финансовые действия требуют собственных свежих полномочий. Тавыш сохраняет local-first данные и воспроизведение.

## Что должно быть простым

- Человек видит приложения, проекты, разговоры, задачи и обратную связь. Ключи, transport, issuer и digest остаются в технических настройках.
- Автор подключает приложение один раз, подтверждает владение, выбирает доступные возможности и проходит реальную проверку входа/проекта/обратной связи. Manifest сам по себе не означает готовность.
- Source управляет разрешённой библиотекой и выдаёт текущую квитанцию выбранного проекта. Root хранит указатель и версию adapter, не копию Native ACL.
- Неизвестный ответ сохраняет исходный запрос. Система проверяет результат отдельно, не повторяет эффект с новым ключом и объясняет пользователю состояние.
- Агент получает только разрешённые tools и pinned guidance выбранного приложения. Изменения требуют текущих Native grants; текст обратной связи остаётся недоверенными данными.

## Ближайшие обязательные проверки

1. Воспроизвести Linux Source startup задержку с fine-phase и IO измерением, исправить установленную причину. Отдельная correctness правка уже сохраняет init reader до COMMIT и закрывает/zeroizes отказавшее хранилище; она не заявлена как ускорение.
2. Закончить fresh HIVE natural browser/capture на Root2bb9 + Source689 и затем production Source image/cold.
3. Принять новый Reviews NativePG/Source3 runtime и Source3 jobs → Linux OS bridge по отдельным реальным gates.
4. Получить текущие Root baseline/feature images и новый Apps8 encrypted cold restore; собрать совместимый Root + Source комплект.
5. Закончить author onboarding, recovery и совместную задачу человека/агента. Измерить нагрузку и стоимость.
6. Подготовить конкретный сохранённый rollout и обратимый compatible fallback. Разрешение на production требуется перед переключением после готового reviewable комплекта.

## Сохранённые доказательства

- [Прежняя полная Root56fe/Apps8 image+cold приёмка](universal-apps8-image-canary-20261007.md).
- [Native PostgreSQL0036](povedai-postgres-native-acceptance-20261007.md), [joint RP/PG26 runtime+cold](povedai-postgres-rp-acceptance-20261007.md).
- [Самостоятельный Source Reader3-off image и offline inspector](povedai-source3-off-image-acceptance-20261007.md).
- [Planner renewal](planner-source-rp-renewal.md), [operator rollout и custody](universal-cli-rollout-20261007.md).

Полная хронология, прежние отказы, точные packets и публичные квитанции сохраняются в `D:/соты/output/soty-universal-platform-implementation-20261006/STATE.md` и соседних артефактах. Повторный успешный focused test не заменяет отказ полного image gate. Production не обновлялся.
