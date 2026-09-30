# P3-D1: независимая приёмка сохранённых приложений

Дата: 30 сентября 2026. Основание: согласованный `p3-engagement-plan.md` (`4fa5f72`), Apps5 только для сохранений. Проверены текущие исходники и реальные локальные исполнения. Apps6/обсуждения, пользовательский интерфейс, публичный выпуск и внешние эксплуатационные gates этим отчётом не закрываются.

## Вердикт

**В ограниченном D1 gate блокеров не осталось. 20/20 независимых тестов PASS, 0 skip.** Найденный HTTP-дефект временной занятости воспроизведён до исправления и повторно проверен после него. Production, код авторов и старые тесты этим исполнителем не менялись; добавлены три независимых файла тестов и этот отчёт.

| Набор | Результат | Что исполняется |
| --- | --- | --- |
| `modules/apps/test/app-saved.acceptance.test.mjs` | 11/11 PASS | Настоящие Apps/World SQLite, WebSocket claim, service registration, затем подтверждённый offline source. Два отдельных OS process для account CAS. |
| `modules/world/test/authority-fence.acceptance.test.mjs` | 4/4 PASS | Настоящий World API и второй Node-процесс с `world.membership.remove`; отдельный writer для busy/recovery. |
| `server/test/app-saved-http.test.mjs` | 5/5 PASS | Настоящий `createHttpApp`, подписывающий Connect client, реальное enrollment второго устройства, HTTP, World/Apps/Connect SQLite и отдельный World process. |

Финальная команда:

```text
node --test modules/apps/test/app-saved.acceptance.test.mjs modules/world/test/authority-fence.acceptance.test.mjs server/test/app-saved-http.test.mjs
```

Финальный общий прогон: 20 tests, 20 pass, 0 fail/cancelled/skipped/todo; около8.4s на текущем Windows/Node. `git diff --check` для независимых файлов — PASS. Авторские21/model, root reader/deploy и общий world suite здесь не присваиваются независимому исполнению.

## Подтверждённые границы

1. Сохраняются exact `appId/domainId/origin/path`, включая query/hash-SPA. Публичный alias не превращается в private canonical; другое приложение, inactive имя и retired имя не становятся запасным маршрутом. Другой реально enrolled Connect device получает ту же личную запись.
2. Saved DTO содержит только выбранный вход, личный label snapshot и разрешённые `name/status/canManage`. После потери доступа новое закрытое название не возвращается, старое личное название сохраняется. Retired address не подменяется другим активным alias. Удалить свою запись можно после app revoke.
3. Реальная World membership и действующие полномочия автора в исходной группе проверяются заново. Выход читателя или потеря автором роли admin делает текущую saved projection недоступной. Первый signed Connect аккаунт сохраняет приложение без создания World profile.
4. Потерянный ACK не меняет intent. Replay прежнего save после последующего remove возвращает историческую квитанцию отдельно от current `entry:null`. Изменённые body/path под тем же ключом конфликтуют. Прежний expected revision после очистки receipts и reopen не воскрешает удалённое.
5. Проверены200 активных записей на фоне201 доступного приложения у трёх авторов. Full quota не блокирует remove, retained replay или accepted no-op. Принятый no-op продвигает account head. Два процесса с разными приложениями и одной account revision получают ровно один commit и один CAS conflict.
6. Проверены страницы длинных валидных путей с фактическим ограничением JSON256KiB, стабильным порядком, foreign cursor denial и `apps_saved_cursor_expired` после изменения библиотеки. Новая записываемая строка, включая закодированный boot path, проходит тот же launch limit. Некорректный путь/аккаунт не оставляет head или запись.
7. Без host fence новые saved operations закрыты, прежние claim/register/list работают. AsyncFunction и отложенный callback не дают незаметного эффекта. Nested fence, World execute/close внутри него и async callback result запрещены; после ошибки connection остаётся пригодным для работы.
8. Настоящий второй World process пытается удалить membership во время fence. До освобождения writer не проходит; разрешение сохраняется на протяжении admission/Apps COMMIT. После освобождения следующий запрос уже denied. Дополнительный signed HTTP сценарий проверяет эту же границу через производственную композицию.
9. Signed seam наблюдает порядок Connect→World→Apps: actor transaction и World writer lock уже удерживаются перед захватом Apps. Ошибка до Apps callback оставляет revision0. Искусственная ошибка после Apps COMMIT приводит к неизвестному ответу, а точный повтор восстанавливает уже сохранённый receipt. Это не межбазовый откат.
10. Занятый Apps writer даёт `apps_saved_busy`, занятый World writer — `world_authority_busy`; оба signed HTTP503. В Apps contention test файл БД и WAL byte-identical до unlock, World lock освобождён, повтор того же requestId после unlock принимается один раз. Штатная writer wait policy World восстанавливается после короткого fence timeout.

## Найденный дефект и исправление

**RED:** внутренние typed busy ошибки корректно обозначали временную недоступность, но общий Connect HTTP adapter преобразовывал любой неуспешный результат в400. Два независимых настоящих signed HTTP теста завершились ровно `400 !== 503`; error codes уже были правильными.

Root изменил только mapping `apps_saved_busy`/`world_authority_busy`→503 в `modules/connect/server/http.mjs`. Adapter не повторяет POST сам. **GREEN:** оба случая пройдены в итоговом20/20, а прежние `authentication_required` и `app_unavailable` по-прежнему возвращают400 согласно существующему Connect dialect.

Авторская дополнительная поправка Apps `BEGIN IMMEDIATE` с временным busy policy100ms проверена независимо. Без неё уже захваченный World fence мог ждать обычный Apps timeout5000ms. Число100 — policy SQLite, не доказанная жёсткая wall-clock гарантия для ОС или произвольной нагрузки.

## Точность свидетельств и ограничения

- В HTTP client только локальное IDB заменено memory storage; proof, enrollment, actor, HTTP handler и предметные базы настоящие. Direct service tests используют явный test actor predicate; они не подменяются формулировкой «проверена вся авторизация».
- Синхронный pause180ms и файловые маркеры координируют настоящее выполнение второго OS process. Это доказательство lock ordering и конкретного race, не benchmark пропускной способности или физической сети.
- Throw после Apps COMMIT — контролируемая инъекция потери результата. Она проверяет квитанцию после уже исполненного эффекта; не заявляется испытание всех разновидностей разрыва питания/сети.
- Fixture сначала ждала только клиентский WebSocket close и однажды получила переходное `starting` до server close. Исправлена синхронизация: ждать наблюдаемого server `source.observation=offline` до теста. Семантические assertions сохранены. Connect envelope `ok:true` также учтён явно, без удаления неизвестных полей из ответа.
- В одном промежуточном незавершённом busy-прогоне teardown попытался удалить собственную synthetic директорию до закрытия тестового SQLite blocker и получил EBUSY для WAL. Lifecycle теста исправлен локальным `finally` до общего teardown. Осталась только fixture `C:\Users\Junio\AppData\Local\Temp\soty-saved-http-independent-qPJs4V`; повторная ручная очистка не выполнялась. Это не отказ approval review и не пользовательская база. Прежняя запрещённая cleanup directory `soty-settings-independent-ta3z3p` не трогалась.
- Браузер, mobile320/667, screen reader, local-pin migration и сохранность нового UI draft ещё не проверены в D1. Нет проверки scale миллионов аккаунтов/hostile OS writer, Linux deployment, DNS/TLS и restore. Historical Apps4→5 reader proof принадлежит отдельному root gate и намеренно не дублировался.
- D2 ещё не реализован этим этапом: нет вывода о готовности app discussion, audience lineage, moderation или доставке tombstones сообщений.

## Привязка к исходникам финального прогона

SHA256:

| Файл | Hash |
| --- | --- |
| `modules/apps/server/schema.mjs` | `894f20556d1d29596bc6b56a8450ab76dcc1a36c5232e1cdc182bebb93b5a272` |
| `modules/apps/server/saved.mjs` | `60e5a5a2d5088776880d12f60fba71ed04d6a9b2642005e22a96ef04caee11c1` |
| `modules/apps/server/engagement-access.mjs` | `2b7a34bbfc63147729b1f12e1e7c3a21bb739ed330837d1a0e93ae516174acf1` |
| `modules/apps/server/launch-path.mjs` | `84c184b2943d4c5ff1e92fe2cdaac2c95fceec131b3dad3e2efb72db29e6ae36` |
| `modules/apps/server/index.mjs` | `9b7e08a0b785c746827bb43cb44e01f0907b3a9306b9ae523c5cc381777464fb` |
| `modules/world/server/index.mjs` | `e22a4aa68f57261127a91ef1b83b163f1a68cd952f14e953c2d7458b9bf2f01a` |
| `modules/connect/server/http.mjs` | `00c08f4d1539583ae21952cd95dba4a16b33da7eb46be66c2c5d2adb2645436f` |
| `server/http-app.js` | `3389e675c53080993d357f85cb761d9998d6115951e90282c333b9b69f82052f` |
| `modules/apps/test/app-saved.acceptance.test.mjs` | `935a644fc123554719b352ea9e0d8899964a0903c61e7d8b70677af03dd697ea` |
| `modules/world/test/authority-fence.acceptance.test.mjs` | `39929634e854210147697a1a4336d17a12b173658e5f2759c1525c3776923903` |
| `server/test/app-saved-http.test.mjs` | `304aa69b01c95d9615dd854b44a4ffff13852cee2ffd7ea522fee25ad2c0cec5` |

Изменение этих файлов после отмеченного прогона требует соразмерной повторной проверки. Этот отчёт не заменяет общий release checklist.
