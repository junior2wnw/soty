# Соты: реализация утверждённого единого поля

Дата: 06.10.2026. Пользователь утвердил рендеры `D:/соты/output/soty-field-v2-20261006/{01-mine,02-search,03-mobile}.png` и поручил реализовать интерфейс, исследования, план по подпунктам, обсуждение решений, перекрёстный аудит и проверку пользовательских/агентских сценариев.

База: `5166b3be5918c4731a4688a2edbd7a9d55179c3b`. Рабочая ветка: `codex/soty-unified-field-20261006`, чистая актуальная release-копия `C:/Users/Junio/.codex/worktrees/soty-ui-release-20261005/соты`. Исходный dirty checkout `D:/соты` сохраняется. Приёмка и журнал обсуждений: `D:/соты/output/soty-unified-field-20261006/`.

## Результат и границы

Главный экран — одно настоящее поле со сторонами «Моё» и «Поиск». Приложения имеют выразительные обложки, люди — реальные доступные аватары или честный fallback, группы — понятный контекст, устройства — компактное состояние. Личная расстановка сохраняется; обзор/фокус/телефон меняют камеру. Пользователь управляет отдельным ярлыком и целым пространством с предпросмотром места и отменой.

Это работающие компоненты и реальные API; рендер целого интерфейса не используется вместо кнопок/данных. Сценарные данные создаются в изолированном стенде. На публичном сайте не создаются вымышленные знакомства, группы, сообщения или присутствие ради похожего снимка. Новые UI не ломают чаты, приложения, помощника, Notes, вход/восстановление, сохранённое и прежние глубокие ссылки.

Публикация на существующий SSH dev уже разрешена в этой сессии. Выпуск производится после проверок из одной revision/image, с сохранением конфигурации/данных/права доступа и проверенным откатом. Дополнительное согласование обычных обратимых решений не требуется.

## Команда и независимые точки зрения

| Исполнитель | Собственная зона | Персона для итоговой приёмки | Кто проверяет его работу |
| --- | --- | --- | --- |
| Root | Продуктовый контракт, общий план, shell/routes/app.ts, integration, performance, release | Новый пользователь, владелец приложения, оператор dev | Все три агента |
| world_field | Чистая модель/geometry/camera/gesture/state/rendering engine | Power-user с100+ярлыками; пользователь клавиатуры | messenger: хранение/права; app_art_pipeline: визуальное исполнение |
| messenger | Авторизованный каталог/search/persistence adapter/backend acceptance | Новичок, участник private группы, агент через SDK/API | world_field: state/операции; app_art_pipeline: ясность состояний |
| app_art_pipeline | CSS/tokens/обложки/visual fixtures/измерения | Пользователь телефона, дизайнер, low vision/reduced motion | world_field: geometry/gesture; messenger: настоящие данные и действия |

Разделение файлов соблюдается; общие `app.ts`, план и решение конфликтов принадлежат root. Подагенты не коммитят, не отправляют ветки и не выполняют SSH/Docker/production mutations. Каждый подтверждает мнение отдельно; root записывает принятое решение и основание. Несогласие не скрывается.

## Процесс для каждого подпункта

1. Предложение владельца: механизм, риск, самый простой альтернативный путь.
2. Обсуждение с соседними владельцами: контракты, понятность, privacy, performance.
3. Реализация с проверяемым критерием; meaningful regression для найденного сбоя.
4. Проверка другим агентом и root по фактическому UI/API/source.
5. Исправление найденного; целевой повтор исходного сценария, сохранение предыдущего FAIL.
6. Закрытие подпункта только со ссылкой на evidence; новый сбой возвращает его в работу.

## P0. Каноническая основа и изоляция

- [x] P0.1 Проверить Git/status и использовать действующий managed worktree, не сбрасывая принятый checkout.
- [x] P0.2 Создать отдельную ветку от последнего проверенного UI-релиза.
- [x] P0.3 Зафиксировать утверждённые три рендера, их SHA, sample-data ограничения и field-contract.
- [x] P0.4 Перепроверить live revision/контейнер/config preservation выбранными безопасными полями перед подготовкой выпуска.
- [x] P0.5 Поднять выделенный QA UI/API/connector с отдельными данными; сохранить исходные пользовательские серверы.

Готовность: точные source/image identities, изолированный стенд и безопасная карта запуска. Владелец root; review messenger.

## P1. Исследования и продуктовый контракт

- [x] P1.1 Собрать первичные HCI работы о spatial memory, semantic zoom, группировании и ориентировании; записать ограничения применимости.
- [x] P1.2 Проверить актуальные W3C требования к drag alternatives, target size, focus/contrast/reduced motion; обратить их в конкретные UI проверки.
- [x] P1.3 Прочитать первичные материалы о responsiveness и измерениях; определить budgets до оптимизации.
- [x] P1.4 Разделить подтверждённые источники, инженерные выводы и гипотезы; не придумывать исследования/результаты людей.
- [x] P1.5 Согласовать private personal context ≠ community, entity ≠ placement, layout ≠ permission, search ≠ personal mutation.
- [x] P1.6 Согласовать default/empty/add/remove/space rename/duplicate shortcut semantics и ограниченную плотность обзора.
- [x] P1.7 Провести первое обсуждение архитектуры; все три независимых мнения и решение root записать в журнал.

Готовность: research register с URL→вывод→ограничение→проверка и публичные интерфейсы модулей. Существование исследований не доказывает «лучший UI в мире»; локальные измерения и сценарии доказывают конкретную реализацию. Все агенты участвуют.

## P2. Визуальная система и assets

- [x] P2.1 Измерить shell/sidebar/header/title/switch/scene/preview у1586×992 reference; зафиксировать сопоставимые anchor bboxes, а не субъективное «похоже».
- [x] P2.2 Согласовать graphite/light palette, champagne controls, Manrope weights, contour/hex shapes, расстояние между пространствами.
- [x] P2.3 Переиспользовать approved cover pipeline и immutable WebP; недостающие assets добавить через контролируемую provenance binding, не по случайному похожему имени.
- [x] P2.4 Обложка/знак/название/статус остаются разными слоями; round people и compact devices визуально отличаются от apps.
- [x] P2.5 Зафиксировать доступные размеры текста/targets независимо camera scale. Обрезка длинного имени даёт полный доступный label и detail.
- [x] P2.6 Реализовать right preview и mobile sheet с доступным закрытием, focus restore и видимыми действиями.
- [x] P2.7 Screenshot/overlay review desktop/phone; исправить конкретные elements и переснять затронутые состояния.

Готовность: работающие geometry/art layers, dark/light, измеренный скриншот, аккуратные empty/unknown состояния. Владелец app_art_pipeline; review world_field/root.

## P3. Модель и надёжное сохранение расстановки

- [x] P3.1 Версионированные account-owned contexts, независимые shortcut IDs/entity refs, мировые context positions и локальные hex slots.
- [x] P3.2 Strict validation: owner, IDs, coordinate bounds, уникальность slots/placements, количество/размер документа, unknown version.
- [x] P3.3 Durable account-scoped load/save с revision/request identity и явными saved/offline/conflict/storage-error состояниями.
- [x] P3.4 Cross-tab/account switch/restart recovery: stale response не применяется к новому owner, unsaved state не теряется молча.
- [x] P3.5 Offline/quota/failed acknowledgement: repeat сохраняет immutable intent; volatile данные честно отмечены, unsafe leave/PWA activation блокируются или дают рабочий recovery.
- [x] P3.6 Legacy UI preferences переносить только в допустимой account границе; unscoped pinned IDs не копировать между аккаунтами.
- [x] P3.7 Отмена/undo — конкретные inverse operations с проверкой текущего revision, без перезаписи чужих изменений.

Готовность: реальные reload/restart и независимые account/conflict tests. Владелец messenger + world_field; cross review обоими.

## P4. Поле, камера и масштабирование

- [x] P4.1 World coordinates не меняются от viewport, resize, search/pagination или metadata updates.
- [x] P4.2 Общая сцена с раздельными контекстами и стабильными app/person/device/community anchors; одинаковая identity может иметь несколько личных shortcuts.
- [x] P4.3 Overview показывает узнаваемые контексты; focus раскрывает readable children; phone выбирает context без изменения layout.
- [x] P4.4 Pan/zoom/cursor anchor/fit overview/return focus; экранная камера ограничена и восстанавливается отдельно для двух сторон.
- [x] P4.5 Видимые узлы ограничены; выбранный/dragged/focused объект не удаляется culling. Search page append не переставляет существующие объекты.
- [x] P4.6 Чистый model/layout покрыть unit/property-oriented tests настоящих инвариантов; DOM lifecycle очищает observers/frames/listeners.

Готовность: spatial continuity и bounded render на100+/предельном количестве объектов. Владелец world_field; review app_art_pipeline/root.

## P5. Перемещение, keyboard и touch

- [x] P5.1 Gesture state idle→pressed→pan или move-preview→commit/cancel. Threshold исключает случайное открытие после drag.
- [x] P5.2 Empty-slot preview, occupied swap preview, context move сохраняет детей; pointercancel/lost capture/Escape уничтожает preview без записи.
- [x] P5.3 Longpress/Расставить на touch различают pan и перенос; второй pointer и boundary condition не дают случайного commit.
- [x] P5.4 Явное «Переместить»→контекст→позиция доступно tap/click без drag; keyboard arrows/Enter/Escape и shortcuts работают с visible focus.
- [x] P5.5 Undo и cancel при storage failure/account switch, no accidental join/grant/publish.
- [x] P5.6 Перенос между личными пространствами проверяется отдельно от операций сообщества; labels объясняют назначение.

Готовность: реальные pointer/keyboard/touch сценарии, storage отказ и restore; geometry не меняется после cancel. Владелец world_field; review messenger/app_art_pipeline.

## P6. Общий поиск и разрешённая информация

- [x] P6.1 Federation существующих World people/groups и Apps catalog с явным public/member/owner provenance; private filtering применяется сервером.
- [x] P6.2 Type filters apps/people/communities, games входят в приложения; devices только own, exact query/topic matches и cursor contracts.
- [x] P6.3 Debounce/sequence и bounded concurrency, максимум2 active search и один latest waiter; поздний результат другого query/account не заменяет актуальный.
- [x] P6.4 «На поле» выбирает personal context; «Вступить» отдельно сохраняет signed membership semantics.
- [x] P6.5 Public preview показывает только разрешённую витрину; membership/current permissions проверяются при изменениях, приватная история не маскируется public excerpt.
- [x] P6.6 Activity truth: только реально доступные свежие события со временем/TTL; unknown/offline/zero выводятся честно.
- [x] P6.7 Search additive/empty/error/retry/stale/deep link; доступ к приложению подтверждается существующим entry/launch contract.

Готовность: owner/member/outsider/signed agent API acceptance и реальные UI действия. Владелец messenger; review world_field/root.

## P7. Интеграция и совместимость

- [x] P7.1 Общий field shell #mine/#world, «Моё / Поиск», nav «Поле · Чаты · Помощник» и draft-safe переходы.
- [x] P7.2 Обратные ссылки сохраняют camera/query/selection; старые #community/#app/#notes/#assistant/#messages routes работают.
- [x] P7.3 Запуск app/device/contact/community из field сохраняет настоящие capabilities; settings/add/source/saved/deployment actions доступны.
- [x] P7.4 Notes/assistant/editor/PWA guards учитывают незавершённую расстановку и не меняют глобальный storage protocol без причины.
- [x] P7.5 Theme/brightness/reduced-motion, sidebar/topbar/mobile safe areas/short keyboard viewport; preview не закрывает focused controls.
- [x] P7.6 Actual app iframe/chat/back/refresh/offline/access-loss regression против последнего release.

Готовность: настоящий интегрированный стенд с текущими API, typecheck/build и regression фактов прежнего релиза. Владелец root; review все.

## P8. Скорость и измерения

- [x] P8.1 До/после измерить bounded100+/максимальную сцену, render/update/drag/pan/switch, visible node count, callbacks и памяти жизненного цикла.
- [x] P8.2 Pointer move не выполняет sync storage/network/full scene rebuild; visual preview идёт через requestAnimationFrame/transform.
- [x] P8.3 Layout/geometry reuses precomputed paths; image responsive renditions/decode/lazy load, culling и immutable cache по approved pipeline.
- [x] P8.4 Browser synthetic interaction latency сравнить с200ms good INP ориентиром; не выдавать synthetic за CrUX/real-user p75. Локальный drag/frame goal16.7ms, отсутствие long task>50ms в обычной сцене, результаты сохраняются.
- [x] P8.5 Fourfold CPU throttle проверяет graceful bounded field; если budget нарушен — исправить причину и целевой повтор.
- [x] P8.6 Lifecycle50mount/dispose и смена аккаунта: subscriptions/frames не накапливаются, disconnected surface не poll.

Готовность: actual measurements с размером сцены, браузером/CPU режимом, сравнимым baseline и пределами. Root + world_field, independent app_art_pipeline.

## P9. Полная приёмка по персонам

- [x] P9.1 Новичок: пустое поле→добавить→выбрать context→открыть→вернуться; «сохранить/вступить» различимы без знания модели.
- [x] P9.2 Телефон одной рукой: switch/search/focus/preview/longpress/pan/cancel и short/landscape viewport.
- [x] P9.3 Power-user:100+/предельная сцена, многократные moves/swap/undo/context move/zoom/resize/reload и cross-tab.
- [x] P9.4 Keyboard/low vision/reduced motion: весь маршрут безdrag/hover, focus visible/not-obscured, contrast/200% text/layout.
- [x] P9.5 Private participant/outsider: search/grants/loss of access/invite-only/leave/archive, отсутствие private name/avatar/history leakage.
- [x] P9.6 Owner/operator/agent: создание/обновление/source/entry/retry/revision conflict через реальные SDK/contracts; no pseudo-actions.
- [x] P9.7 Offline/slow/storage quota/malformed saved state/lost ACK/account switch/PWA update в каждом значимом маршруте.
- [x] P9.8 Visual matrix320/390/768/1024/1440 и reference1586×992 dark/light; additional390×430/844×390/200% text. Измерения header/title/switch/contours/covers/preview/targets и actual screenshots.
- [x] P9.9 Разделить fixture screenshots, actual signed API, controlled failures и read-only public acceptance; собрать source hashes/coverage matrix.
- [x] P9.10 Все агенты независимо проходят финальный план/соседние подпункты; повторное обсуждение и закрытие каждого P1/P2 с evidence.

Это конечная явная матрица, а не утверждение о проверке всех мыслимых людей/аппаратных устройств. Реальные человеческие исследования или physical iOS/Safari без проведения не заявляются. Владелец каждого сценария назначен в coverage; root интегрирует.

## P10. Выпуск

- [ ] P10.1 Fresh commit/source hashes, appropriate full tests/typecheck/build; сохранённый самостоятельный UI+API review.
- [ ] P10.2 Commit/push ветки, exact Git archive/full immutable Docker build и declared storage readers.
- [ ] P10.3 Live config/mount/writer/state audit, свежий authenticated encrypted cold backup и физическая restore/old+new clone проверка совместимости.
- [ ] P10.4 Guarded rollout с maintenance/fresh receipt/config preservation/одним writer; существующие named routes, domains и прочие apps сохраняются.
- [ ] P10.5 Actual public files hashes/cache/ready и real browser desktop/mobile route/action acceptance; production не seed ради картинки.
- [ ] P10.6 Итоговый report: что реализовано/проверено/внедрено, точный revision/image/rollback, непроверенные аппаратные/платные/человеческие аспекты; очистка только принадлежащих аудиту процессов/томов.

Готовность: committed receipt + public acceptance. Root only release; independent agents проверяют доказательства.

## Журнал решений

### Обсуждение1: модель поля, до редактирования engine

world_field: нынешняя field geometry зависит от viewport и entity-level keys; для persistent личных пространств нужна отдельная чистая модель с shortcut identity, world camera и preview-before-commit gesture. messenger: entity snapshots не следует писать в layout; нужны account scope/revision/recovery, search provenance и отдельные signed социальные действия. app_art_pipeline: нынешние overlays уменьшают видимость covers, scale делает подписи слишком мелкими; требуются screen-space-readable captions/targets и измеримые scene anchors.

Предварительное решение root: использовать отдельный единый field module поверх существующих authority contracts; хранить личный layout отдельно от live DTO; coordinates/slots независимы от viewport, render camera независима от сохранения. Public contracts и backend пределы будут согласованы после capacity/SDK inspection перед параллельными edits.

## Статус

P0–P9 реализованы и проверены в пределах явно заданной матрицы. Финальные native recovery8/8, genuine UI16/16, desktop/mobile/context focus, short viewport и200% layout/DPR equivalent PASS; отдельные source/coverage receipts фиксируются владельцами. P10 выполняется следующим этапом: свежий commit/image, encrypted physical restore/cold old-new-old и публичная приёмка. Статус публикации подтверждается только итоговым rollout receipt в отдельном каталоге evidence.
