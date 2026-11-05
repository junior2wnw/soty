# Volumetric Glass API

`src/liquid-glass.js` — независимый WebGL2-движок цельного объёмного стекла. DOM задаёт расположение, а материал, форма, содержимое и оптика описываются небольшими объектами.

## Быстрый старт

```html
<canvas id="glass-canvas"></canvas>
<button class="offer"><span class="label">Квартира</span></button>

<script type="module" src="/src/my-app.js"></script>
```

```js
import OpticalGlass from "./liquid-glass.js";

const scene = OpticalGlass.create({
  canvas: "#glass-canvas",
  background: "/background.jpg",
});

const offer = scene.add({
  target: ".offer",
  shape: { radius: 0.38, bevel: 0.22, irregularity: 1.4 },
  optics: { ior: 1.47, refraction: 1.25, depth: 54, roughness: 0.02 },
  style: { tint: [0.8, 0.94, 1], matte: 0.05 },
  surface: { texture: 0.8, scale: 1.1, grain: 0.7, rear: 0.9 },
  content: OpticalGlass.text(null, {
    source: ".label",
    color: "#101820",
    depthEm: 0.15,
    roundingEm: 0.16,
  }),
});

scene.start();
```

Модуль не зависит от лаборатории, её стилей или панели настроек. В новом коде предпочтителен обычный ES-импорт `OpticalGlass`.

## Встраивание в карточку или компонент

Canvas не обязан занимать весь экран. Передайте контейнер как `root`, и все строковые селекторы будут искаться только внутри него:

```js
const root = document.querySelector(".property-card");
const scene = OpticalGlass.create({
  root,
  canvas: ".property-card__canvas",
  background: "/images/property.jpg",
  dprCap: 1.5,
});

scene.add({
  target: ".property-card__action",
});
```

`root` может быть `Element`, `Document` или `ShadowRoot`. Вместо селектора всегда можно передать сам canvas/target. Геометрия рассчитывается в локальных координатах canvas, поэтому на странице разрешено несколько независимых сцен.

Для внешнего CDN с корректными CORS-заголовками используйте `crossOrigin: "anonymous"` в настройках сцены или отдельного `OpticalGlass.image()`.

## Любой живой DOM: списки, формы и контролы

Для функционального интерфейса не нужно превращать HTML в картинку. Оставьте настоящие кнопки, ссылки, поля, списки, таблицы, ползунки и пользовательские компоненты поверх оптического canvas. Браузер продолжает отвечать за фокус, клавиатуру, клики, выделение, формы и доступность, а оптический движок рассчитывает единый материал под ними.

Минимальный декларативный вариант:

```html
<section
  class="filters"
  data-optical-glass="panel"
  data-glass-depth="72"
  data-glass-matte="0.24"
  data-glass-safe-area="auto"
>
  <label>
    Комнат
    <input type="range" min="1" max="6" value="3" />
  </label>

  <button
    data-optical-glass="control"
    data-glass-shape="pill"
    data-glass-depth="46"
  >Показать квартиры</button>
</section>
```

```js
const composition = scene.mount(document.body);
await scene.start();
```

`scene.mount()` сканирует `[data-optical-glass]`, подключает каждый элемент и наблюдает за DOM через `MutationObserver`. Пустой атрибут использует спокойный пресет `quiet`; выразительные режимы выбираются явно. Если список добавит новую отмеченную строку, она станет стеклянной без повторного запуска сцены. Если строку удалить, модуль снимет обработчики и освободит её из композиции.

```js
const row = document.createElement("button");
row.dataset.opticalGlass = "control";
row.dataset.glassShape = "pill";
list.append(row); // подключится автоматически

composition.disconnect(); // прекратить наблюдение, оставить элементы
composition.destroy();    // убрать созданные этим mount() элементы
```

Встроенные пресеты:

| Пресет | Для чего |
|---|---|
| `quiet` | безопасный производственный режим для форм, списков и рабочих панелей; декларативный default |
| `clear` | прозрачный универсальный объект |
| `frosted` | матовая панель или подложка |
| `panel` | крупный составной контейнер |
| `control` | кнопка, чип, бегунок, переключатель |
| `dense` | особенно толстый акцентный объект |
| `line` | линия, трек, разделитель, индикатор |

Свой пресет может наследовать встроенный:

```js
scene.definePreset("property-row", {
  preset: "control",
  optics: { depth: 52, refraction: 1.18 },
  style: { tint: [0.5, 0.82, 1], matte: 0.12 },
});
```

Поддерживаемые декларативные параметры: `data-glass-depth`, `data-glass-ior`, `data-glass-refraction`, `data-glass-roughness`, `data-glass-matte`, `data-glass-tint`, `data-glass-tint-strength`, `data-glass-texture`, `data-glass-texture-scale`, `data-glass-grain`, `data-glass-rear`, `data-glass-radius`, `data-glass-rounding`, `data-glass-bevel`, `data-glass-irregularity`, `data-glass-interaction`, `data-glass-layer`, `data-glass-layers`, `data-glass-motion`, `data-glass-motion-mode`, `data-glass-motion-coupling`, `data-glass-parallax`, `data-glass-tilt`, `data-glass-lift`, `data-glass-motion-scale`, `data-glass-motion-inertia`, `data-glass-layer-separation`, `data-glass-content-depth`, `data-glass-safe-area` и формы `pill`, `pebble`, `soft`, `square`, `line`. Полный объект настроек можно передать JSON-строкой в `data-glass-options`.

То же самое программно:

```js
const cards = scene.addAll(".property-card", ({ index }) => ({
  preset: index === 0 ? "dense" : "clear",
  content: OpticalGlass.live(),
}));

scene.update(cards[0], {
  optics: { depth: 140 },
  style: { matte: 0.28 },
});

scene.remove(cards.at(-1));
```

`OpticalGlass.live()` означает, что содержимое остаётся живым DOM. Его плоскость слегка утоплена в объём и наследует движение всей иерархии. Для настоящей оптической гравировки надписи, изображения или Canvas-рисунка используйте соответственно `OpticalGlass.text()`, `OpticalGlass.image()` или `OpticalGlass.custom()`.

### Безопасная зона для скруглённого содержимого

`data-glass-safe-area="auto"` не позволяет тексту и контролам забраться в круглые углы. Движок вычисляет минимальный внутренний отступ из геометрии окружности `r × (1 − 1/√2)` и добавляет вклад фаски. Поэтому усиление скругления автоматически сдвигает содержимое внутрь, а исходные CSS-отступы сохраняются как нижняя граница.

То же самое программно:

```js
scene.add({
  target: ".rounded-panel",
  shape: { radius: 0.42, bevel: 0.2 },
  content: OpticalGlass.live(".rounded-panel", { safeArea: "auto" }),
});
```

Для `OpticalGlass.live()` значение `"auto"` включено по умолчанию. Вместо него можно передать число в пикселях, пару `[inline, block]`, объект `{ top, right, bottom, left }` или явно отключить защиту через `safeArea: false`.

### Связанное движение и вложенные стёкла

Живой DOM двигается вместе со своей стеклянной подложкой. Движок использует отдельные CSS transform-составляющие `translate`, `rotate` и `scale`, поэтому не затирает обычный `transform` приложения. Если стеклянный контрол вложен в стеклянную панель, движение естественно складывается: при переходе курсора на контрол панель сохраняет свою инерцию, а контрол получает дополнительный локальный наклон и параллакс.

```js
scene.add({
  target: ".property-card",
  preset: "panel",
  content: OpticalGlass.live(".property-card"),
  motion: {
    enabled: true,
    parallax: 6.5,       // перемещение поверхности, px
    tilt: 4.5,           // максимальный пространственный наклон, deg
    lift: 10,            // подъём по оси Z, px
    scale: 0.006,        // приближение при наведении
    press: 0.014,        // физическое продавливание
    inertia: 0.13,       // скорость следования; меньше — тяжелее
    layerSeparation: 4,  // параллакс между физическими слоями
    contentDepth: 1.4,   // глубина живого DOM внутри поверхности, px
  },
});
```

`motion.mode: "group"` задаёт общее движение контейнера, `"surface"` — локальную реакцию вложенного объекта, а `motion.coupling` управляет их связностью. У каждого физического слоя можно дополнительно задать `motion.response` и `motion.separation`. Это полезно для неодинаковых листов стекла: задний слой движется тяжелее, передний — быстрее. `prefers-reduced-motion` автоматически отключает пространственное движение DOM, сенсорный наклон и реакцию оптики за указателем.

Наклон телефона подключается одним вызовом. Для продуктового интерфейса лучше показывать явную кнопку: на iOS разрешение требует пользовательского жеста, а пользователь понимает, зачем сайту нужен датчик. Движок усредняет нейтральное положение по нескольким сэмплам, фильтрует дрожание, пересчитывает оси при повороте экрана и использует акселерометр как запасной источник, если события ориентации не приходят:

```js
const tilt = scene.enableDeviceTilt({
  requestOnFirstInteraction: false,
  autoStart: false,
  maxAngle: 22,
  deadZone: 0.028,
  smoothing: 0.22,
  strength: 0.92,
  calibrationSamples: 10,
  motionFallback: true,
});

enableTiltButton.addEventListener("click", async () => {
  if (tilt.active) tilt.calibrate();
  else await tilt.request();
});
```

`autoStart: false` гарантирует, что даже Android без системного запроса разрешения дождётся вашей кнопки. `tilt.source` возвращает `"calibrating"`, `"orientation"` или `"motion"`, а `tilt.permission` — текущее состояние разрешения. После касания сенсор снова плавно получает управление; все отдельные стёкла сохраняют общее физическое направление без случайного перекоса осей. `tilt.disable()` полностью снимает оба слушателя и сбрасывает реакцию.

### Универсальные меню

Три готовых способа используют один жизненный цикл, ARIA-связи, закрытие снаружи, `Escape`, возврат фокуса и reduced-motion:

```js
const popover = scene.createPopover({ trigger: ".filter", panel: ".filter-menu" });
const drawer = scene.createDrawer({ trigger: ".settings", panel: ".settings-panel" });
const orbit = scene.createOrbitMenu({ trigger: ".more", panel: ".quick-actions" });

drawer.setMode("popover");
drawer.show();
drawer.hide({ restoreFocus: true });
```

Панель может содержать любой живой DOM. Если она ещё не подключена как стекло, метод добавит стеклянную поверхность автоматически; `glass: false` оставляет материал под контролем существующей декларативной композиции.

## Многослойное стекло

Слои одного элемента задаются от заднего к переднему. Движок использует ping-pong framebuffer: каждый проход читает готовое изображение предыдущего. Поэтому второй слой преломляет не исходный фон, а свет, уже прошедший через первый слой.

Промежуточная сцена хранится в линейном свете и, при поддержке видеокартой, в `RGBA16F`. Это предотвращает неверное сложение яркости и накопление 8-битных полос в длинных стеках.

```js
const card = scene.add({
  target: ".glass-card",
  optics: { ior: 1.47, refraction: 1.1, depth: 46 },
  layers: [
    {
      offset: [-8, 9],
      scale: [0.98, 0.94],
      optics: { ior: 1.435, refraction: 0.9, depth: 28 },
      style: { matte: 0.18 },
      content: false,
    },
    {
      offset: [4, -4],
      scale: 0.99,
      content: true,
    },
  ],
});
```

Настройки слоя переопределяют только указанные свойства и наследуют остальные от элемента.

```js
scene.addLayer(card, {
  offset: [7, -6],
  optics: { depth: 34, ior: 1.5 },
});

scene.updateLayer(card, 1, {
  style: { matte: 0.3 },
  surface: { texture: 1.4 },
});

scene.removeLayer(card, 0);

scene.setLayers(card, [rearLayer, middleLayer, frontLayer]);
```

Последний слой удалить нельзя: элемент всегда остаётся физически определённым.

## Произвольная форма

Любой Canvas-контур превращается в поле расстояний и получает настоящий округлый профиль:

```js
const drawShape = ({ context, width, height }) => {
  context.beginPath();
  context.arc(width / 2, height / 2, Math.min(width, height) * 0.42, 0, Math.PI * 2);
  context.fillStyle = "white";
  context.fill();
};

scene.add({
  target: ".orb",
  shape: OpticalGlass.shape(drawShape, {
    bevel: 0.18,
    irregularity: 0.8,
  }),
});
```

SVG path тоже поддерживается:

```js
scene.add({
  target: ".logo",
  shape: OpticalGlass.path("M50 4 L96 96 L4 96 Z", {
    viewBox: [0, 0, 100, 100],
    padding: 0.08,
    bevel: 0.18,
    rounding: 14,
  }),
});
```

Для узкой линии изгиба используйте `OpticalGlass.bend(target, options)`.

## Содержимое

### Внутренняя объёмная гравировка

Поверхностная надпись и лазерная фигура внутри цельного стекла — разные материалы. Для второго случая оберните текст, изображение или Canvas-рисунок в `OpticalGlass.volume()`. У текста есть короткая запись `volumeText()`:

```js
const crystal = scene.add({
  target: ".crystal",
  preset: "quiet",
  optics: { ior: 1.49, depth: 156, refraction: 1.34 },
  content: OpticalGlass.volumeText("ДОМ", {
    fontFamily: '"Manrope", sans-serif',
    fontWeight: 800,
    color: "#d8f6ff",
    fillOpacity: 1.3,
    volume: {
      thickness: 0.62,
      position: 0,
      scatter: 1.35,
      facets: 1.1,
      parallax: 1.15,
      grain: 0.7,
      glow: 0.95,
      dispersion: 0.48,
    },
  }),
});
```

Одна и та же обёртка принимает все растровые источники движка:

```js
const logo = OpticalGlass.volume(
  OpticalGlass.image("/logo.png", {
    width: 240,
    depthFromLuma: true,
  }),
  { thickness: 0.7, facets: 1.25 },
);

const figure = OpticalGlass.volume(
  OpticalGlass.custom(({
    maskContext, colorContext, depthContext, positionContext, width, height,
  }) => {
    // Альфа maskContext задаёт силуэт, colorContext — цвет рассеяния.
    // Необязательная серая карта глубины: белый = полная толщина,
    // серый = более тонкий участок, чёрный = нулевая толщина.
    maskContext.fillStyle = "white";
    maskContext.fillRect(0, 0, width, height);
    colorContext.fillStyle = "#d9f7ff";
    colorContext.fillRect(0, 0, width, height);
    depthContext.fillStyle = "#b8b8b8";
    depthContext.fillRect(0, 0, width, height);
    // Вторая карта распределяет детали между передней и задней частью объёма.
    positionContext.fillStyle = "#808080"; // серый = центр
    positionContext.fillRect(0, 0, width, height);
  }, {
    width: 280,
    height: 220,
    depthMap: true,
    positionMap: true,
  }),
  { thickness: 0.76, position: 0.08, grain: 0.82 },
);
```

У `image(..., { depthFromLuma: true })` яркость изображения становится локальной картой глубины. У `custom()` `depthContext` создаётся только при `depthMap: true`, а `positionContext` — при `positionMap: true`. На карте положения чёрный уводит деталь к передней части, серый оставляет в центре, белый — к задней. Без собственной карты глубины движок автоматически строит мягкий округлый профиль по краям маски. Поэтому обычный текст сразу выглядит объёмным, а сложной фигуре можно задать и толщину, и пространственный рельеф без нового шейдера или 3D-модели. Атлас хранит маску в локальных координатах элемента: при scroll, resize и движении слоя фигура остаётся жёстко связана со своим стеклом.

| Параметр `volume` | Назначение |
|---|---|
| `thickness` | доля доступной толщины стекла, которую занимает гравировка, `0.025–0.96` |
| `position` | положение между гранями: `-1` ближе к передней, `1` ближе к задней |
| `scatter` | плотность светорассеивающих микроповреждений |
| `density` | необязательная универсальная оптическая плотность гравировки; заменяет `fillOpacity` для объёмного режима |
| `facets` | выраженность нормалей, боковых граней и фасеточного блика |
| `parallax` | различие видимого положения ближних и дальних срезов |
| `grain` | неоднородность лазерных микроточек: `0` гладко, `1` зернисто |
| `glow` | сила направленного свечения микроточек |
| `dispersion` | слабое спектральное разделение на внутренних гранях |

Параметры обновляются частично, без пересоздания материала и без потери остальных настроек:

```js
scene.updateContent(crystal, {
  text: "СВЕТ",
  volume: { thickness: 0.88, position: 0.2 },
});
```

Готовая интерактивная проверка находится в [`examples/engraving/`](../examples/engraving/): в ней отдельно меняются текст, параметры внутреннего объёма, IOR, преломление, шероховатость, матовость, оттенок, микрорельеф, задняя грань, форма и сила отклика. Выбранный материал можно сбросить или скопировать как чистый JSON для другого проекта.

### Текст и любой шрифт

Шрифт, размер, вес и интервалы можно взять из CSS исходного DOM-элемента или переопределить в конфигурации:

```js
content: OpticalGlass.text("Выбрать", {
  fontFamily: '"My Font"',
  fontWeight: 700,
  color: "#10233b",
  depthEm: 0.17,
  roundingEm: 0.2,
  fillOpacity: 0.96,
})
```

Без `volume` это поверхностная гравировка: `depthEm` задаёт глубину канавки, `roundingEm` — плавность стенок, а `fillOpacity` работает как оптическая плотность пигмента и может быть больше `1`. Надпись входит в ту же карту высот, поэтому на закруглениях вокруг букв тоже меняются нормаль и свет. Пигмент рассчитывается как внутренний слой после объёмного рассеяния стекла — матовость больше не делает тёмные буквы серыми.

Параметры содержимого можно менять без пересоздания сцены:

```js
scene.updateContent(glass, {
  depthEm: 0.22,
  fillOpacity: 1.35,
  color: "#05080b",
});
```

### Изображение

```js
content: OpticalGlass.image("/plan.png", {
  width: ({ rect }) => rect.width * 0.62,
  depth: 8,
  rounding: 10,
  fillOpacity: 0.94,
})
```

### Пользовательский рисунок

```js
content: OpticalGlass.custom(({ maskContext, colorContext, width, height }) => {
  maskContext.fillStyle = "white";
  maskContext.fillRect(0, 0, width, height);
  colorContext.fillStyle = "#ff5d45";
  colorContext.fillRect(0, 0, width, height);
}, { width: 120, height: 120, depth: 7, rounding: 9 })
```

## Параметры материала

| Параметр | Назначение |
|---|---|
| `shape.radius` | радиус скругления обычной линзы |
| `shape.radiusMax` | верхний предел радиуса в px |
| `shape.rounding` | сглаживание острых граней произвольного SDF-контура |
| `shape.bevel` | ширина округлого перехода к центральной массе |
| `shape.irregularity` | естественная неровность контура |
| `optics.ior` | показатель преломления материала |
| `optics.refraction` | художественный множитель видимого смещения |
| `optics.depth` | физическая толщина слоя; диапазон лаборатории 8–220 px |
| `optics.roughness` | оптическая шероховатость поверхности |
| `style.tint` | RGB-оттенок стеклянной массы в диапазоне 0–1 |
| `style.tintStrength` | концентрация красителя: 0 — бесцветное, до 4 — плотное окрашенное стекло |
| `style.matte` | рассеяние и матовость |
| `surface.texture` | сила микрорельефа |
| `surface.scale` | масштаб неоднородностей |
| `surface.grain` | мелкое зерно |
| `surface.rear` | неровность задней поверхности |
| `interaction` | сила поворота и блика за указателем |
| `seed` | уникальный характер конкретного куска стекла |

Общий множитель фактуры не уничтожает различия элементов:

```js
scene.setTextureStrength(0);   // идеально гладко
scene.setTextureStrength(1);   // исходная фактура каждого слоя
scene.setTextureStrength(2.5); // выраженный рельеф
```

### Смена фона без пересоздания

```js
await scene.setBackground("/images/terrace-evening.webp");
```

Метод сначала декодирует новое изображение и только после успешной загрузки заменяет GPU-текстуру. Если исходный фон недоступен при первом запуске, сцена использует `fallbackBackground`, а затем встроенный оптический градиент — чёрный canvas не появляется.

```js
const scene = OpticalGlass.create({
  canvas: "#glass-canvas",
  background: "/images/property.webp",
  fallbackBackground: "/images/property-fallback.jpg",
});
```

После изменения текста или размеров вызовите `scene.refresh()`.

```js
scene.stop();       // пауза без потери ресурсов
await scene.start(); // продолжение той же сцены
scene.destroy();    // удалить текстуры, framebuffer, программу и обработчики
```

После `destroy()` сцена намеренно не запускается повторно: создайте новый экземпляр. Это защищает приложение от скрытых обработчиков и утечек GPU-памяти.

## Как проходит кадр

1. Фон один раз переводится из sRGB в линейный свет и кэшируется до изменения размера.
2. Для каждой точки считается локальная толщина профиля и реальный путь луча с учётом угла преломления.
3. Поглощение и объёмная матовость рассчитываются экспоненциально по Beer–Lambert, поэтому тонкая кромка прозрачнее толстого центра.
4. Каждый слой читает предыдущую сцену и рассчитывает нормаль, дисперсию, Fresnel, микрорельеф, рассеяние, гравировку и каустику.
5. Результат слоя записывается во второй FP16 framebuffer, после чего буферы меняются местами.
6. Финальный линейный цвет преобразуется обратно в sRGB только при выводе на экран.

Такое устройство универсально: число слоёв и форма элементов не зашиты в отдельные эффекты.

## Производительность

- дорогой шейдер запускается только в ограничивающем прямоугольнике линзы и её тени через GPU scissor;
- все элементы используют один универсальный набор GPU-параметров, поэтому динамическое добавление не перекомпилирует шейдер;
- после двух начальных копий фона между ping-pong-буферами переносится только область, изменённая предыдущим стеклом, а не весь экран;
- неизменный линейный фон не пересчитывается каждый кадр;
- прямоугольник DOM вычисляется один раз на элемент, а не для каждого его слоя;
- матовое размытие использует пять симметричных выборок и полностью пропускается у прозрачного материала;
- перестроение Canvas/SDF-атласов откладывается до завершения изменения размеров;
- `scene.getMetrics()` возвращает FPS, время кадра, число проходов, оценку обработанных пикселей и точность буфера.

## Регрессионные тесты

```bash
npm run test
```

Тесты проверяют зависимость рассеяния и диапазона преломления от локальной толщины, толщины 8/53/220 px, накопление света в стеках 1/2/4/8 и наличие линейного FP16-конвейера с ограничением области рендера.
