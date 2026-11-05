# Состояние интеграции — 2026-11-04

Цель этой части: универсальный content host на canonical canvas; настоящий NotesService, личные media bytes и field v2 free placement вместо отдельной демонстрационной модели. Root интегрирует screen/directory/HTTP и проводит browser acceptance. Источники истины: NotesService, MaterialsService, CanvasObjectService metadata/ACL и FieldDocument placement; содержимое никогда не копируется в layout.

Готовы изолированные providers `src/world/native-field-objects.ts` и reusable module `modules/materials`. API и ограничения — [README](README.md). Не объявлено полное выполнение R04 по модульным тестам. Сохраняется запрет production/личных загрузок в этой работе.

Engine `createUnifiedField` читает `AnyFieldDocument/AnyFieldEntityRef`, включая v2 object IDs. Вызовы без новой placement сохраняют literal v1. Только явные free/resize/new object commands конвертируют документ в v2. История хранит before/after целиком; undo resize/add может вернуть точный исходный v1. `fieldShortcutPlacement` — единственный источник интерпретации placement. `slot` у FieldNode для free — только производная совместимость, не durable geometry.

Новый host option: `createObject(node): FieldObjectView|null`. FieldObjectView `{element,update(node),setVisible(visible),activate?(),flush(),hasUnsavedChanges(),dispose()}`. Renderer может быть notes/materials/app/внешний provider. Host делает article вместо button, отдельную mask wrapper, удерживает focused/не сохранённый локально editor, обновляет provider без замены DOM, вызывает lazy visibility и общий flush/dispose. Provider binding у CanvasObject immutable; при смене entity identity host remounts. Новые metadata не передают body в `thinEntities`.

Security/lifecycle addition: optional `objectKey(node):string|null` — текущая разрешённая стабильная source identity (без content revision). Root выдаёт key только по свежей ACL-aware projection. Fallback→key promotes native content; same key keeps caret; revoke/key change immediately disposes+scrubs native textarea/DOM/media URL, даже если branch ещё dirty. Existing authorized Notes outbox остаётся в своём account store. Generic unavailable fallback не показывает прежний private title/aria. В отсутствие objectKey остаётся совместимый provider behavior. `createNativeFieldObjects` также предоставляет objectKey для составного host resolver.

API для screen:

```ts
engine.addFreeShortcut(entity,contextId,{kind:'free',x,y,width,height});
engine.setShortcutPlacement(shortcutId,placement,contextId?);
engine.resizeShortcut(shortcutId,width,height);
```

Free center относительно context, width/height 16..8192 из shared field contract. Free objects допускают намеренное overlap; автоматическое hex placement избегает занятых free bounds. Маска в `entity.presentation` плоская `{shape:'hex'|'rect'|'ellipse'}` / `{shape:'polygon',points:[{x,y}]}`; validator делегирует shared `validateCanvasMask`, без arbitrary CSS/HTML. Размер не дублируется в entity. Polygon ray casting и CSS clip сохраняют настоящие пустые углы; inspect/44px resize handle лежат вне mask.

Управление: mouse direct drag; touch swipe pan и long-press inspect; Space/middle pan; один pinch owner. Free drag сохраняет точные world pixels без hex snap. Нижний правый resize держит исходный верхний левый угол, preview не пишет. Pointercancel/rotation/blur не коммитят preview. `M` → arrows двигают free на 10 world px, Shift — 1px; Enter commit, Escape cancel. Resize handle arrows — 8px, Shift — 1px; все native textarea/audio/video/iframe controls сохраняют свои keys. Existing hex direct manipulation и компактный поиск сохранены.

Доказательства этой части: `.ux-v2-engine-final.log`, explicit subset 185/185 (engine/art/camera/compact/context/focus/input/layout/live/pointer/readability/resize/state/uniform/v2/glass/native/materials), TypeScript 5.9.3 `--noEmit` PASS на Node24.22.1. Это не весь repository test suite. Screen-facing AST fixtures до правки root имели отсутствующие runtime ports; они не ослаблялись этим агентом. Значимые отрицательные controls: exact450204c не знает place/resize command; ignored free geometry теряет640px width; owner/pending leaks; icon-only native substitute; focused editor overwrite; GL handle без rendered optical frame; premature async GL destroy.

Открытая приёмка: browser pixels/focus/hit areas, реальный audio/video decode+play, Android touch+soft keyboard, reload в целевой среде, field v2 persistence/server path, public/private/shared object content route. Trusted Materials native port уже реализован и проверен вместе с настоящим ObjectService; production HTTP composition root ещё обязан проверить целиком. Owner-scoped materials.* не ослаблен. Public note editing также не получает notes.put автоматически; Root отвечает за notes object provider.

Следующий шаг: Root монтирует универсальный host и native provider; наблюдает настоящую страницу desktop/mobile и закрывает перечисленные сценарии доказательствами. Не менять engine/layout/state параллельно до root handoff/freeze.

Финальный scoped recheck после source-revoke/promotion и trusted Materials ports: `.ux-v2-engine-final.log` — 194/194, 0 failures. Предыдущий промежуточный прогон191/194 имел отсутствующий production inspectShortcut port в uniform fixture; порт восстановлен без изменения assertions, повторный полный scoped набор зелёный. TypeScript последнего source состояния PASS. Engine SHA256 A6D25AB0045339CDCF45A5C9E5EFCBB0AA5B96A5E4B364B3B113BBF5BAF1C6D5; native renderer25B1CA388075D26C2EC077C7494D5CC10C90D2FC4DAFDCFC4E7389744F1C912B; MaterialsService73B35289489C98A6929B163A25021B7F03391CB1E45642A085DD418F78A7115D. Files frozen for Root integration/browser pass.
