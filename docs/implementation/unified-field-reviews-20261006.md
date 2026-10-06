# Журнал решений и независимых проверок

## Обсуждение 1: модель и права

**world_field:** личное пространство отдельно от community, entity отдельно от shortcut. Мировые координаты сохраняются, viewport меняет камеру. Drag выполняется как preview→commit; отмена не пишет layout. Исследования spatial UI поддерживают проверку устойчивых позиций, но не необходимость 3D. Предложены keyboard и single-pointer move, collision/swap, bounded undo.

**messenger:** публичный каталог не должен раскрывать hostDeviceId, connectorId, grants и закрытые группы. Старый catalog ограничивал кандидатов до фильтра, поэтому одного frontend search недостаточно. Предложены отдельные permission-filtered directory operations, account-derived owner, CAS receipts и atomic IndexedDB outbox. World extension tables добавляются отдельно без повышения старой schema 3; старый reader должен сохранить их.

**app_art_pipeline:** плоская управляющая оболочка и эмоциональные материалы внутри hex. Основной принятый cover manifest и его URLs сохраняются; field — отдельный профиль и точные ID bindings. На обзоре нужно менять детализацию вместо уменьшения текста до 6–8 px. Проверка в нескольких размерах и фактические crop обязательны.

**Root:** приняты отдельные personal document/search scene, права через существующий Connect SDK, новый тонкий directory, независимый field art profile. Стартовое пространство содержит настоящие встроенные возможности; доступны explicit add и одноразовая миграция pinned refs только этого аккаунта. Каталог не превращается автоматически в личную кучу объектов. Production не наполняется вымышленными людьми ради скриншота.

## Обсуждение 2: первый рабочий экран

**Root, фактический browser pass:** общий обзор в 1280×720 показал слишком мелкие app tiles и обрезанные подписи, а inverse-scaled заголовки пространств стали слишком большими относительно сцены. Desktop overview controls отсутствовали. Эти состояния не приняты как финальные. Сообщены владельцу визуального слоя; добавлены настоящие zoom/overview controls и fit после разрешения metadata.

**world_field:** добавлены public rename/remove context, explicit atomic remove-with-shortcuts с undo, отдельные inspect/context menu/Shift+F10. Открытие приложения осталось быстрым. Сохранённый receipt с более новым current document не считается повреждённым ответом: требуется видимый конфликт и явный выбор версии.

**messenger:** последовательное исчерпание world source могло спрятать apps за длинным каталогом людей. Изменён frontend federation на независимые cursors и круговой бюджет источников. Неверный legacy ref больше не блокирует остальные refs в batch. Уточнена разница durable offline outbox и несохранённого RAM intent.

**app_art_pipeline:** отдельно проверены crop всех шести обложек в native hex и длинные подписи; black-pill заменяется спокойным scrim. Созданы только dev portraits, так что реальные production профили сохраняют собственные разрешённые аватары/fallback. Согласованы отдельная shell область field и сохранение принятых chat/app экранов.

**Root:** исправлены исходные причины, новые состояния продолжают проверяться. Фикстура использует тот же controller/engine/CSS, но её transport контролируемый и данные вымышленные. Проверка прав производится другими тестами с настоящими signed Connect HTTP и WebSocket, а не объявляется свойством фикстуры.

## Обсуждение 3: точность, сеть и отмена

**app_art_pipeline:** исходная крупная lattice не позволяла совместить сцены рендера и реальные размеры аватаров/заголовков без перекрытий. Согласована мелкая сетка размещения при неизменном размере визуального hex; desktop сравнивается в явном «Обзоре», телефон использует фокус пространства. Native hit-test выявил прозрачные прямоугольные углы кнопок. Clip-path применяется к настоящей кнопке; отдельное действие карточки сохраняет target44px. Промежуточные неудачные снимки сохранены отдельно от финальной приёмки.

**world_field:** pointer packets объединяются в один RAF; pointerup применяет финальную позицию перед единственным commit. Escape и другой changed command сбрасывают preview, queued frame не может восстановить отменённый перенос. Общий immutable summary document кэшируется до изменения раскладки; pan не клонирует256 refs на каждом кадре. ready ждёт настоящий ResizeObserver с ненулевой областью. Native mouse/keyboard/touch, delayed ACK/destroy, TTL и50 mount/dispose повторены.

**messenger:** сетевой PUT внутри локальной serialize-очереди задерживал следующие durable действия. Local-first сохраняет каждое намерение в IndexedDB сразу, GET/PUT работают отдельно; logical projected revision не скачет от ACK своей очереди. Наблюдатель другой вкладки не становится автором чужого outbox. Собственный unresolved conflictDocument остаётся до явного выбора; если другая вкладка убрала его durable bytes, UI честно показывает volatile и даёт скачать/отменить. Все signed owner/device/account fences сохранены.

**Root:** приняты эти исправления и независимые регрессии. Личный фильтр видим после очистки строки, совпадение названия пространства не накрывается empty overlay, undo обновляет пустое состояние. Query scope, выбранный фильтр и generation проверяются до применения ответа. Максимум два поиска выполняются одновременно, очередь хранит только последний запрос; native controller с закрытым RPC gate и100 сменами строки запустил Alpha, Beta и Queued-99. Добавление заново читает доступный каталог, app settings обновляют metadata.

## Обсуждение 4: ошибка записи и выпуск

**messenger:** настоящая abort-транзакция IndexedDB показала перекрытие error feedback и нижней панели; сценарий не объявлялся пройденным по unit-тестам. QA отдельно проверяет достижимость экспорта, подтверждение discard и navigation guard. Контролируемый abort не называется исчерпанием физического диска.

**app_art_pipeline:** persistent error/conflict получает отдельную flow-полосу над controls; transient saving не меняет размер камеры на каждый ACK. Прямой повтор проводится на320/390/1440 и коротких viewport.

**world_field, независимый выпускной review:** первоначальный cold snapshot hash не включал PRAGMA user_version. Root включил epoch в logical hash всех старых/новых/rollback запусков. Дополнительно schema objects сравниваются по type+name+table и точному количеству, поэтому одноимённый посторонний trigger не скрывается за старой таблицей. Whitelist допускает только8 объектов и одну meta строку; любые другие изменения старых таблиц отклоняются.

**Root:** Linux проверка private cold helpers27/27; реальные старый/новый/старый image cold boots на физически восстановленной production копии остаются обязательным отдельным gate. Сохранение конфигурации и двух model routing hashes проверяется в памяти, секреты не выводятся и хранятся в backup только зашифрованно.

## Независимая итоговая приёмка

**world_field — power-user и клавиатура:**24 чистых model/layout/camera/contracts проверки, native gesture/keyboard/culling/cancel/negative ACK и50 lifecycle cycles. На256 refs:15 дискретных действий, EventTiming max72ms при1×CPU и96ms при4×CPU в24 пространствах; самый плотный один контекст дал104/152ms соответственно. Порог наблюдения16ms и квантование8ms учитываются. Stress frame p95≈66.8ms при4×CPU означает, что стабильные60fps на физическом телефоне не доказаны. Проверка cold helpers после исправлений принята без оставшегосяP1.

**messenger — новичок, private participant, API агент:**43 собственные проверки и16 genuine integrated UI сценариев: context/shortcut/keyboard move/undo/cancel/reload; private contact не попадает в global search; добавление приглашённой группы не вступает в неё, затем настоящий Accept открывает чат; реальный второй device/account switch сохраняет владельца. Targeted recovery8/8 PASS: настоящий download event и скачанный JSON без ключей, guard undurable ухода, подтверждённый discard и переход в чаты без скрытого server revision. На320/390/1440 controls44px доступны, не перекрывают toolbar. Контраст actual light/dark error toast после отдельного palette исправления14.33:1/14.06:1. Lazy-картинка за clipping viewport не называется сломанной видимой картинкой; произвольный первоначальный bound140px ширины заменён согласованными метриками156px высоты/16px caption/44px control, исходный flagged receipt сохранён.

**app_art_pipeline — телефон, внешний вид и reduced motion:**48 operator/public pipeline проверок, сохранение всех старых manifest/WebP bytes, native fallback/preview/focus/motion. Desktop1586×992 сравнивается с первым утверждённым рендером: основные anchors отличаются приблизительно на16px или меньше. Вымышленные QA portraits существуют только в development fixture. Native keyboard scroll на390×430/844×390 сохраняет доступность44px controls; preview-close возвращает title/node на390/1024/1586 в обеих темах. Увеличение200% проверено честным layout793×496/DPR2 equivalent с native CDP PNG1586×992: physical font32px/targets88px, stage240, без горизонтального overflow. Native Ctrl+= в headless не менял zoom и не выдаётся за успешную проверку. Подтверждённый короткий viewport stage=0 и повторное mobile append focused search устранены с исходными FAIL и отдельным PASS.

Последняя проверка caption использует solid glyph pixels из native PNG и тот же scene с убранным текстовым слоем. Начальные HIVE3.91/4.388:1 и Шахматы4.494:1 не достигли принятого строгого4.5:1 бюджета; усилена одна строка нижнего CSS gradient, без изменения raster/anchor/version. Повтор9/9 PASS: HIVE9.088:1 desktop/9.744:1 phone, Шахматы9.554:1, остальные≥13.69:1. Первый FAIL receipt остаётся рядом с accepted receipt.

Общий прогон:1182 PASS/0 FAIL/5 штатно SKIP; дополнительные connect/release/art/dev269 PASS/0 FAIL/3 SKIP. Последние persistence/search/state33/33 PASS, typecheck и Vite build PASS. Не суммируем перекрывающиеся suites как разные независимые проверки. Точные counts/границы и свежие source/image identities собираются в `D:/соты/output/soty-unified-field-20261006/`.

Diff review сохраняет одно намеренное исключение: завершающая пустая строка в frozen `modules/world/test/support/legacy-world-v3.mjs`. Её не форматируем после проверки: support SHA25653a5f608ebe32fbba151c7577bc00e5af53bb9daeb9f2a5f713c35d657257a2d и normalized original95f3a6bdffd0efae4730339fc091929a6e6a5e81f9d8c56a962a505dee4b5934 привязывают настоящий reader5166b3b. Остальные добавленные/изменённые файлы не имеют ошибок whitespace.
