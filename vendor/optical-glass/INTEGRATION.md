# Собственная оптика Сот

Проверенный источник: `D:\регионЖилье`, clean commit `a8aa5d1389f5c86e5d1129912fa92daa8d0229de`, пакет `volumetric-glass-ui` версии `1.4.0`. Core получен из проверенного snapshot и имеет явно учтённую локальную правку `soty-sdf-boundary-2`; исходные документы сохранены побайтно. Базовый и текущий SHA256 — в `PROVENANCE.json`, причина и проверка правки — в `PATCHES.md`. `src/liquid-glass.d.ts`, descriptor и этот документ — новые локальные дополнения, не части upstream snapshot.

`module-descriptor.json` — машиночитаемый указатель UI-модуля. Исходники также включены в существующий каталог модулей Сот через фиксированные публичные пути; их реальная HTTP-доставка проверена интеграционным тестом. Это не доказательство публикации новой сборки работающего сервера или npm-пакета. В vendor присутствуют все исходные файлы, заявленные в package.json: UI kit, site, design.css и presence с документацией. Актуальные hashes записаны в PROVENANCE.json: `localRevision: soty-core-3`, `uiLayerRevision: soty-universal-working-ui-6`. Сервер presence не запускался. Название пакета в документации upstream не является доказательством текущего npm-релиза. Upstream не содержит декларации лицензии; не добавлять от своего имени MIT/SPDX-разрешение.

Оболочка и native dialogs используют общий material runtime. Connect создаёт `data-glass-role` и `data-glass-safe-area` вместе с DOM, отдаёт собственный `panel.element` и подключается через `mountMaterialGlass` без классификации CSS-классов. Остальные прежние roots используют compatibility `mountShellGlass`/`mountDialogGlass`; полная миграция всех компонентов на явные роли ещё не закончена. UI-kit сохраняет декларативные роли компонента при очистке и исходный rollback императивного API. Native dialog имеет собственный canvas в top layer; контент сторонних iframe остаётся у приложения.

## Компактное окно приложения

Общий CSS-компонент `.glass-ui__widget` сохраняет нативные поля, действия и DOM.
Source владеет содержимым и данными, полотно — размещением и читаемым фокусом.
`src/widget-context.js` экспортирует `bindGlassWidgetContext` для Source и
`createGlassWidgetHost` для владельца iframe; типы лежат рядом. Контекст содержит
только UI-режим `glance|interactive` и корреляцию текущей загрузки. Точные origin,
Window и закрытая форма сообщений обязательны; этот API не выдаёт права Source.
На каждой загрузке `onReset` снимает прежнее подтверждение, `onReady` возвращает
его после нового ответа. Старое подтверждение не принадлежит новому документу.

В `glance` скрыты только явно размеченные editor/actions; полезное состояние
остаётся видимым. Подложка сохраняет контраст, декоративная рамка убирается.
В `interactive` редактор доступен и при маленькой высоте с экранной клавиатурой.
Источник без нового host использует `interactive`. Не определяйте этот режим
по размерам iframe или браузерному focus. Примеры двух разных Sources находятся
в `modules/canvas-widgets/examples/counter` и `reading-list`; второй использует
неизменённый подписанный Source SDK. Полная live Native-приёмка учитывается отдельно.

## Vanilla field adapter

```ts
import { mountFieldGlass } from './src/world/field-glass';

const glass = mountFieldGlass(engine.element, {
  onStatus: state => { /* webgl/fallback, reason, surfaces */ },
});
await glass.ready;
// Optional; child/size/theme observers already update the decoration.
glass.refresh();
// Explicit retry after an optical failure; never re-registers app data.
glass.refresh({ retry: true });
// On field/account disposal:
glass.dispose();
```

Монтировать на `engine.element` (`.uf-viewport`), а не на всё приложение. Повторное mount того же root возвращает тот же owner. `options.background` при необходимости принимает URL картинки; без него создаётся локальный SVG из текущего фонового цвета темы. Внешний URL должен иметь подходящие CORS/CSP. Фон не получает изображения, тексты, личность или содержимое приложений.

Engine маркирует видимые DOM targets `data-field-glass-node="hex|rect|ellipse|polygon"`; polygon получает только проверенные normalized points через `data-field-glass-points`. Адаптер создаёт внутри target пустую декоративную `.uf-glass-face`; текст, обложка, HTML button, focus, hit clip, координаты и жесты сохраняют своего прежнего владельца. Используется один low-level `OpticalGlass.create` и один WebGL2 canvas на поле; UI-kit и его CSS подключаются отдельно для оболочки. Материал виден по кромке под обложкой и под прозрачной поверхностью native content; настоящие надписи не растрируются в shader.

Для hex SDF строится из того же `roundedHexPath(1,.12)` и нормализованной матрицы, что и CSS hit clip. Остальные shapes используют настоящий draw mask: world-radius roundRect, ellipse или validated polygon. `rounding:0` исключает дополнительное скругление в экранных пикселях; нормализованный векторный контур сохраняет world/screen пропорции при zoom и при общей перестройке атласа. Layout resize/target membership инвалидируют SDF atlas; pan/zoom/selection сами по себе этого не делают. Защитный raster budget оставляет слишком большие элементы на рабочем DOM fallback и не меняет их геометрию/содержимое.

`content:null`, `interaction:0`, `motion.enabled:false` исключают изменение живого DOM и auto safe-area. Это сохраняет исправления prototype: shader не добавляет padding при zoom, DOM/GPU форма не расходится из-за clamp абсолютного radius. GPU initialization начинается после двух requestAnimationFrame (между ними есть возможность первого DOM paint). `webgl` выставляется только после running+initialized, отсутствия context loss/GL error и ненулевого счётчика opticalPasses после первой ожидаемой GL frame. Один context handle не выдаётся за отрисованный кадр. Проверка этого состояния — техническая, не доказательство визуального качества кадра. У пустого поля GPU не создаётся; добавление видимого target автоматически запускает ту же композицию. Muted/disabled/hidden targets не оставляют яркую оптическую поверхность поверх своей скрытой DOM-презентации.

В forced colors, prefers-reduced-motion и `data-motion="off"` действует статический существующий material fallback. В upstream reduced-motion не замораживает все временные shader-составляющие, поэтому адаптер не обещает полностью статичную оптику при работающем GPU. При context loss/ошибке shader остаются настоящие DOM controls и обложки; режим честно становится `fallback`. `dispose()` отключает observers/listeners/frames, удаляет собственные face/canvas и восстанавливает принадлежавшие root атрибуты. Асинхронный upstream start завершается до окончательного destroy, чтобы позднее выделение GL-ресурсов не пережило cleanup.

Наблюдаемые поля: `data-field-glass="webgl|fallback"`, `data-field-glass-stage="waiting-for-paint|initializing|ready|fallback"`; callback status содержит mode, число поверхностей и безопасный reason-code. GPU-картинка, совпадение кромок, читаемость, стоимость кадра и работа пальцами проверяются настоящим браузером/устройством. Unit/source tests их не заменяют.

Существующая библиотека может использоваться другим vanilla/framework приложением напрямую: см. исходный `docs/LIQUID_GLASS.md`. Сам адаптер не читает сеть Сот, ACL, app registry, localStorage/IndexedDB и не выдаёт агенту пользовательских полномочий.
