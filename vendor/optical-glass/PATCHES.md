# Local revision soty-sdf-boundary-2

Основание — собственный `volumetric-glass-ui` 1.4.0, commit `a8aa5d1389f5c86e5d1129912fa92daa8d0229de`. Исходный SHA256 core: `57760bc88cd0ad6c3cf5abfcbfeb2bc72e847cd8154d0b1c22951423da545b4e`. Проверено повторным чтением `D:\регионЖилье\src\liquid-glass.js` 04.11.2026; исходная папка не менялась. Текущий локальный SHA256: `98631ec01ae4bc8f8cbc7c4037b34ae8472b800b50ba647854e967f13957bdda`, 144787 bytes. Эти значения и промежуточная revision1 отдельно записаны в `PROVENANCE.json`.

На настоящем polygon note до исправления был виден прямоугольный серый след. В той же browser session, с неизменным tint, отключение HTML shadow его сохраняло, а скрытие только GL canvas убирало. Это отличает оптическую причину от CSS shadow. Снимки: `output/playwright/native-ghost-control-a-no-dom-shadow.png` и `native-ghost-control-b-no-gl.png` в рабочем проекте Сот.

Изменён только custom-shape SDF путь core:

- Custom `lensDistance` семплирует SDF в ближайшей точке прямоугольника, затем продолжает положительное расстояние за его границей. Раньше `sampleAtlas` возвращал ноль вне target и превращал дистанцию в константу `range`: тень получала прямоугольный разрыв и плоский хвост внутри scissor. Общий `sampleAtlas`, content maps и analytic round-rect не менялись.
- `distanceToOutside` в chamfer учитывает прозрачность за границей mask canvas. Заполненный край раньше не видел внешних прозрачных пикселей и мог иметь неверную глубокую внутреннюю дистанцию, особенно у прямоугольника без пустой рамки. Теперь расстояние до exterior ограничено ближайшей стороной canvas.
- Отдельный `sampleShapeAtlas` ограничивает UV центрами крайних texel. Общий `sampleAtlas` преднамеренно оставлен прежним для content maps. Без half-texel inset LINEAR filtering смешивал SDF на границе packed tile с прозрачным gutter: encoded red становился около .25 вместо .5, и дистанция прыгала примерно на42px. Только первые два изменения (revision1) проходили CPU edge tests, но НЕ убрали ghost в настоящем браузере. Это наблюдение сохранено как отрицательный контроль, а не выдано за успех.

Продолжение за bounding box является непрерывной консервативной оценкой для произвольного SDF: это расстояние до его края плюс неотрицательная SDF на краю, а не аналитически точное расстояние до всех сегментов за пределами bbox. Внутри shape atlas точность и shader-оптика сохраняются; текст и содержимое никогда не копируются в GPU background.

`src/world/field-glass-sdf.test.mjs` выполняет настоящий `_chamferDistance` и scalar GLSL helper на синтетических supersampled rect/ellipse/hex/concave masks, alpha edge и нескольких масштабах. Два намеренно возвращённых дефекта обнаруживаются тем же CPU edge-oracle. Эти CPU/source проверки не доказывают итоговые пиксели GPU.

В отдельном настоящем browser optical-only fixture, без DOM shadow, воспроизведён тот же дефект. Custom uniform был `[-1,84,.1,0]`: версия о случайном analytic rounded-rect отвергнута. `ux-glass-probe-no-shadow.png` убирает только shader shadow, `ux-glass-probe-distance.png` показывает его bbox-разрыв, `ux-glass-probe-half-texel.png` исправляет только atlas sample. После переноса правки в canonical runtime и обычной перезагрузки просмотрен `output/playwright/ux-glass-probe-canonical-fixed.png`: жёсткий прямоугольник исчез, плавная тень формы и настоящая GL-оптика сохранены.

Один неизменный pixel oracle (`output/playwright/ux-glass-edge-oracle.mjs`) проверяет10 соседних пар в пустых bbox-границах этой фиксированной390×345 polygon сцены. При26px sigma тени предел2 уровня8-bit допускает округление, но ловит жёсткий скачок: `ux-glass-probe-baseline.png` max8→FAIL; `ux-glass-probe-canonical-fixed.png` max1→PASS. Это проверка конкретного optical fixture, не полная приёмка всего приложения/всех GPU. Root отдельно перепроверяет настоящую заметку и остальные browser сценарии.

## Native visibility, 04.11.2026

`soty-core-3` сохраняет обе SDF-правки и добавляет общий `glass-native-visibility-1`. Ядро проверяет фактическую видимость DOM перед каждым optical pass. Закрытый `details` и пропущенное `content-visibility` содержимое не должны рисовать стекло только из-за ненулевого layout rect. Собственная рамка закрытого details/его summary продолжает рисоваться; opacity-политика не менялась.

Контроль на старом SDF-vendor: скрытое поле имело `checkVisibility()=false`, но один GPU pass и тысячи отличающихся пикселей против удаления только optical registration. После узкого исправления hidden/control кадры совпадают, открытие возвращает GPU pass без замены DOM/значения. Исходный общий patch применён отдельными hunks, без копирования upstream core поверх SDF-версии. Точная цепочка трёх patches и текущий hash — в PROVENANCE.json.

Публикации, npm-релиза и нового лицензионного разрешения этим изменением не было.

## Declarative role ownership, 04.11.2026

`uiLayerRevision=soty-universal-working-ui-4` меняет только cleanup `src/ui-kit.js`: role, заданная компонентом декларативно, сохраняет поздние изменения/удаление при destroy, detach и ignore. Старый снимок восстанавливается только после императивных setRole/removeRole через существующий overrides. Новых реестров нет; производные оптические атрибуты очищаются как раньше. Одни и те же hunks применены в isolated library и этом vendor; текущий UI SHA/bytes записан в PROVENANCE.json. Исходный отрицательный контроль восстановлен по прежнему UI SHA: 4 FAIL/2 PASS → 6 PASS. UI source приведён к LF. Core `8304ee1472a1a056f3c8df319c3a5334bf66da8e91ab4128399da6c99e09ab7c`, SDF/visibility, CSS, публичная версия1.4.0 и лицензия не менялись. Публикации не было.

## CSS-only compact widgets, 04.11.2026

UI local revision5 добавляет только scoped `.glass-ui__widget` / content / identity / glance / actions к существующему ui-kit.css. Исходные34465bytes CSS SHA7c5047777833ecd6f9f39b59ce71dc96cfef4311f2aeb2e710561442b02fea46 сохранены полностью; новые2214bytes дают scroll content, glance и native actions44×44 без height-hiding. Явный static marker позволяет использовать материал без UI JS/GPU; приложение сохраняет свой DOM и собственный код/данные. В существующем Source Native ui.style объединяются shared+consumer CSS, новыхassets/routes/SDK нет. Добавлен CSP self-friendly переносимый пример. Browser evidence: isolated library output/playwright/widget-css-after.json; исходный icon34×44/нетscroll/glance и fixed1024px control обнаруживаются тем же oracle. Четыреviewport, short-height action, same input/caret/timeOrigin, native states/forced-colors/ignore проверены. Это не Root app acceptance и не физический Android. Core8304/UIJS521200/SDF/optical defaults не менялись; публичная версия/лицензия/публикация не менялись.

## Explicit widget UI context, 04.11.2026

Local UI revision6 добавляет src/widget-context.js и adjacent types / package export ./widget-context. Это только UI glance/interactive, strict source+origin+plain exact envelope, integer epoch echo на каждом iframe load; onReset сбрасывает opt-in, current ACK допускает onReady. Нет Source grants, actors/tokens/URL/commands, новой Source SDK/bus/TTL/polling. Child standalone/oldhost остаётся interactive; same-mode setMode не отправляет сообщения; dispose/current fences обязательны. CSS prefix36679bytes SHA2f840080... полностью сохранён; append скрывает editor/actions только в explicit glance, input/value/caret сохраняются. В glance normal-colors убраны border/shadow/radius, но сохранён читаемый Source material fill. Полная прозрачность дала contrast1.02/1.09 на чужом tone backdrop и отвергнута; Source plate даёт identity≥5.47 и metric≥13.38 в own4tone matrix. Core8304/UIJS521200/SourceSDK/app-work не менялись. Public version1.4.0 и публикация прежние. Own proof: isolated output/playwright/widget-context-browser.json, widget-context-contrast.json;44NodePASS. Root/physicalAndroid/двух Source apps adoption — отдельная приёмка.


## Shared native action list, 05.11.2026

`.glass-ui__action-list` adds flat44px action rows inside one native Glass sheet. No iframe, event or permission changes; static native overlay remains the readable96% canonical material. Same additive CSS hunk is in isolated library; its pre-existing CSS prefix differs and was preserved. Vendored provenance records exact before/after; this is not an upstream/npm release. Browser/native focus/viewport controls are recorded separately.

## Native pointer boundary registration, 06.11.2026

`styleRevision=soty-native-pointer-boundary-1` records the already approved `glass-native-pointer-boundary-1`; Core remains `soty-core-3`, all three JS/SDF/visibility patches and both previous CSS patches retain their hashes. Public version1.4.0, upstream commit and license:null are preserved. No new style behavior is introduced here.

Exact owned CSS: `f8fa7d920a1d06759fdf67befd17da4c51e81152ef64346cf084d06e3bd8671f` /39355 bytes → `6a58c8f58e2d9cea8f71b60e78b3d8c24355732cfafcec7eef5d046d06144d23` /39623 bytes. The earlier candidate has exactly one268-byte insertion; every original byte is retained. The zero-specificity `:where(.glass-ui[data-glass-ignore], .glass-ui [data-glass-ignore])` boundary resets inherited pointer suppression to auto; explicit native class/inline/descendant pointer rules keep precedence. This literal CSS has an exact `-text` attribute, like the pinned uPlot payload. Other upstream sources are not normalized.

The accompanying existing `docs/UI_KIT.md` explanation is also registered: `537e0c9ba4cc7495870b8aaa15669eec0d0aabe77d6df340980702b49b60728a` /39598 bytes → `f902d6140d7bbcdb2da22283970c4354e55739248b39217ee25087ec715c8b0c` /40327 bytes. Its bytes are unchanged by this registration.

Evidence: portable [browser oracle](../../src/world/glass-action-sizing.browser.mjs), SHA256 `5ba3337094146f606d19d8dab68fbb1f7a2e0d600d694d87cd879972f802ae82`. External audit reference `d14-glass-native-pointer-freeze-final.json`, SHA256 `19577a9329474f783355a3600782786740356ba5d7c23c57c097ea5d0a907451`. Run `old-native-pointer-final.tap.log`: OLD2FAIL/0SKIP, SHA256 `28c7022d3b7455e3a46b16728c3154df54d820aa2b1dbb6bbacddc65e1bd29d0`; run `actual-repo-glass-action-sizing.tap.log`: actual applied8PASS/0FAIL/0SKIP, SHA256 `c2eee522598c16451469e3a62f9be9db0964ef62ba033b783a37bffac1540c8c`. This reuses verified installed-Chrome pointer/uPlot evidence; it does not claim GPU/Android/IME acceptance or an upstream publication.
