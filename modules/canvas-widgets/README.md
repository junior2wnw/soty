# Виджеты настоящих приложений Сот

Один provider soty.apps размещает главную страницу любого доступного приложения через существующий Apps runtime. Приложение может принадлежать другому человеку. Отдельных app-specific веток, второго реестра приложений или iframes по произвольному URL здесь нет. Optional зарегистрированные widget descriptors и их typed settings — следующий совместимый контракт, не фиктивно готовая функция.

## DTO

```js
{schema:'soty.canvas-widget.v1',widgetId:'widget-'+uuid,ownerId,revision,
 appId,entry:{domainId,path},source:{revision,digest,profile},
 contractVersion:1,state:'active'}
```

Owner — человек, закрепивший окно; владелец приложения остаётся в Apps. AppId и точный entry immutable. Source pin меняется только явным refresh с CAS. WidgetId одновременно служит instanceId существующего gateway формата widget-UUID. В SQLite только привязка/пин/receipt: нет копии названия, каталога, обложки, геометрии, шрифта, маски, URL tickets или credentials. Маска — в Canvas Objects, размеры/позиция — только в fieldv2.

Подписанные операции: canvas.widgets.create/get/list/resolve/refresh/remove/restore. ExpectedAccountId обязателен; изменения также требуют requestId. Create принимает appId и необязательный exact entry:{domainId,path}. Без entry авторитетный Apps service выбирает текущую главную точку входа. С явным entry fallback запрещён. Participant может закрепить чужое приложение, если его текущие права допускают этот вход.

Get/list/resolve — только личные instances текущего владельца. Public viewers читают их исключительно через проверенный объект полотна. Remove/restore меняют состояние instance и не удаляют приложение/данные Source. Предел1024 instances и50000 command receipts на человека. Receipt/CAS/row COMMIT атомарны; replay сохраняет widgetId и исходный receipt с актуальным metadata DTO.

## Host wiring

```js
widgets=createCanvasWidgetsService({
 databasePath,projectId,actorActive,
 withActorAuthorityFence:(originalActor,cb)=>connect.withActorAuthorityFence(originalActor,cb),
 withAppSourceAuthority:(request,cb)=>apps.withWidgetSourceAuthority(request,cb),
 onChange,
});
const provider=createCanvasWidgetProvider({widgets,
 withObjectAuthority:(context,cb)=>objects.withObjectAuthority(context,cb)});
// register provider as trusted Root code; object binding {providerId:'soty.apps',ref:widgetId}
```

withSourceAuthority проверяет objectOwnerId=widget owner, затем права **текущего зрителя** на exact App entry. Он не подменяет actor владельцем instance. Создание Canvas Object требует его собственного widget instance. В read допускаются другие viewers с их реальными Apps grants. Branded object context и действующий Source frame нужны одновременно; поздняя/JSON-копия context не выдаёт Source authority.

App port withWidgetSourceAuthority использует существующие World/Apps authority fences и registry. Возвращается текущее WorldAppRecord-подобное metadata, exact entry, source pin и runtimeAllowed. Offline/starting/stopped — состояние соединения, не отзыв прав. Временные ошибки хранения/authority не превращаются в available:false; известный запрет/отсутствие приложения даёт закрытую проекцию.

Анонимный viewer может увидеть metadata только реально публичного активного named alias; runtimeAllowed всегда false. Известный unlisted alias разрешён для явно показанного widget, но не появляется из-за этого в public discovery. Object visibility не публикует приватное приложение.

## Источник изменился

Provider view имеет schema soty.canvas-widget-view.v1/kind widget, widgetId/instanceId, текущие app/entry/source и expectedSource. Несовпадение pin даёт state=needs-refresh и runtimeAllowed=false. Название/обложка не требуют замены instance; metadata читается заново из Apps. Менять source pin, appId или entry молча нельзя. Canvas.widgets.refresh фиксирует новый Source только для того же точного входа и после свежей проверки participant ACL.

Pin использует существующий runtime target revision/digest/profile. Это привязка маршрута/допущенного Source, а не доказательство хэша исполняемого кода или версии пакета.

Одной проверки до render недостаточно: Source может измениться до mint ticket. Поэтому AppsService имеет optional trusted constructor port checkWidgetLaunch. При widget instanceId он вызывается после актуального Apps ACL decision и до mint/launch. Wire widgets.checkLaunch(request): он проверяет текущий active instance, appId/exact entry/source pin. Missing/removed/wrong entry или needs-refresh отказываются. Сами Apps ACL, Source admission, runtime readiness и gateway session проверки сохраняются.

Тот же guard вызывается в обоих scoped renewal путях: перед новым slot и перед ticket из cached retry. Phase renew проверяет appId/domainId/source pin; путь продолжает проверять прежний gateway, включая нормализацию Root '/'→'/embed'. Для selected non-workspace Source context намеренно не содержит instanceId. Private rootInstanceId хранится только в Root RAM record/attempt witness, чтобы защита действовала и там; Source context, wire и user DTO не меняются.

**Release gate:** undefined hook сохраняет прежний gateway. Root пока имеет legacy MicroWidgets/localStorage UUIDs. До включения строгого hook нужно явно преобразовать поддерживаемый legacy путь в service-owned instances/объяснить upgrade; глобальный отказ старых ID без этого не является совместимым выпуском. Для новых service-owned instances hook обязателен. Нельзя вместо него тихо запускать current target известного stale widget.

Временная разрешённая миграционная композиция: widgets.hasInstance(id) ? widgets.checkLaunch(request) : true. HasInstance включает removed: известный stale/removed instance никогда не становится legacy fallback. Unknown IDs сохраняют только прежний current Apps ACL и не являются доказательством Canvas admission. Новый canonical UI принимает IDs исключительно из Widget service/Object projection. Миграция прежнего localStorage и закрытие режима неизвестных legacy IDs остаются открыты; потерянную Widget DB нельзя выдавать за выполненную миграцию.

Host notifyAppChanged({appId}) вызывается после Apps COMMIT и перечисляет фактические active instance bindings в onChange, не меняя ревизии. Root вызывает Objects.notifySourceChanged({providerId:'soty.apps',ref:widgetId,ownerId}); публичные зрители проходят свежие объектные/Apps gates. Hint не содержит содержимого и не объявляет public transition.

## Проверка

Node24.22.1: node --test modules/canvas-widgets/test.mjs modules/canvas-widgets/http.test.mjs modules/canvas-widgets/renewal-guard.test.mjs.

Unit/SQLite тесты используют реальные Apps service/registration/ACL с двумя разными владельцами, Canvas Objects provider, право participant, отзыв viewer, текущий rename, unlisted preview, retirement exact alias, fault записи receipt и reopen. Source metadata не копируется в instance DB.

HTTP acceptance поднимает настоящий Connect, World/Apps/Objects/Widgets, два независимых HTTP приложения и два реальных коннектора. Signed third user открывает чужой app через обычный Apps launch→session→HTTP200, затем теряет доступ. После projection выполняется настоящий source.prepare/promote: строгий hook обнаруживает гонку и блокирует ticket. Negative control с отключённым hook на том же сценарии запускает новый Source молча; после восстановления hook снова отказывает. Явный refresh/replay и перезапуск сервисов/новый client сохраняют instance и открывают подтверждённый Source. Removed instance не запускается.

Отдельный actual Apps/ScopedGateway/Widget SQLite тест выполняет SourceRegistry cutover от другого writer, реальный channel ACK и expired Source-only renewal legacy selected Source без instanceId в context. Guard off воспроизводит выдачу нового Source ticket; guard on запрещает и новое создание slot, и cached ticket mint. Fixture Human/Source-proof constructor ports синтетические; этот тест не объявляется Native RP/OIDC интеграцией.

Общий Root HTTP/UI wiring, визуальная работа двух одновременно открытых AppStage, optional widget descriptors, нагрузка и production release/readers остаются отдельной интеграцией. Эти backend tests не объявляют их выполненными.
