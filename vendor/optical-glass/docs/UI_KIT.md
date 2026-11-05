# Универсальный UI-слой

`createGlassUI()` добавляет семантические роли поверх физического движка. Он не заменяет HTML компонентами и не забирает управление сеткой: кнопка остаётся кнопкой, SVG остаётся SVG, форма остаётся формой. Роль выбирает материал, порядок прохождения света, безопасную область и характер движения.

## Локальное расширение: режим рендера и общий fallback

Этот раздел описывает исходное расширение поверх1.4.0. Версия package и публичная публикация им не изменены; уже установленный npm-пакет не получает эти изменения автоматически.

```js
const ui = await createGlassUI({
  root: document.querySelector("#application"),
  background: "/images/background.webp",
  renderPolicy: "auto", // "auto" | "static" | "optical"
  onRenderStatus(status) {
    // {policy, mode:"static"|"optical", reason, gpuReady:boolean}
    console.log(status.mode, status.reason);
  },
});

await ui.setRenderPolicy("static");
console.log(ui.renderMode, ui.renderStatus); // static, gpuReady:false
ui.destroy();
```

`auto` — стандартная попытка оптического рендера при разрешающих условиях. `static` не создаёт WebGL сцену и использует общий CSS glass material. `optical` явно запрашивает попытку оптики, но не обходит forced colors/reduced motion и также получает читаемый fallback при отказе GPU. `auto` и `optical` требуют прежний обязательный `background`; в `static` он необязателен. Перед поздним переходом из static в оптику задайте фон через `setBackground()`.

Новый контроллер имеет getters `renderPolicy`, `renderMode`, `renderStatus` и async `setRenderPolicy(policy)`. Снимок status заморожен. Возможные reason: `policy-static`, `forced-colors`, `reduced-motion`, `ignored`, `initializing`, `awaiting-frame`, `unavailable`, `context-lost`, `ready`, `destroyed`. Наличие controller/canvas handle не является доказательством кадра. `optical`/`gpuReady:true` выдаются только после следующего animation frame, когда scene имеет GL, running/initialized, контекст не потерян, `getError()` возвращает `NO_ERROR`, а opticalPasses≥1. Это технический witness, не автоматическая оценка визуального качества.

В static, pending initialization и fallback getters **`glass`, `scene`, `composition`, `canvas` равны null**. Не используйте их без проверки режима. `setRole`, `removeRole`, `registerRole`, `setTheme`, `refresh` и `destroy` продолжают работать без GPU, сохраняя прежние возвращаемые значения. `setBackground()` в static запоминает источник без загрузки изображения и возвращает Promise с null. `setTheme()` сохраняет detached JSON theme. `refresh()` возвращает тот же контроллер; `destroy()` идемпотентен и восстанавливает принадлежавшие библиотеке атрибуты/class, не уничтожая исходный DOM.

При context loss материал сразу становится static, optical handles очищаются; бесконечного автоматического восстановления нет. Для явной повторной попытки используйте `await ui.setRenderPolicy(ui.renderPolicy)`. Отказ first-frame proof также оставляет static; позднее появление содержимого/снятие скрытия можно подтвердить такой явной попыткой. Изменения media preferences сериализуются; запоздавшая инициализация устаревшего поколения освобождается и не публикует успех. Ожидание next frame ограничено одной секундой, после чего остаётся usable static. Это не отменяет сетевую загрузку фона в нижнем `createGlassSite`: используйте доверенный доступный фон/fallbackBackground.

`onRenderStatus` — observer: его исключение не роняет успешную инициализацию. Неверные policy/theme/role/root/selector и ошибки конфигурации не выдаются за hardware fallback: они по-прежнему отклоняют операцию. Валидация/управление доступом приложения не относятся к рендеру.

### Переносимые material tokens

Общий fallback рисует материал и кромку, а не оставляет role background прозрачным. Он использует те же публичные CSS tokens для всех consumers:

| Token | Тип/назначение |
|---|---|
| `--glass-ui-surface` | Цвет обычной поверхности |
| `--glass-ui-surface-strong` | Более плотный читаемый цвет поверхности |
| `--glass-ui-edge` | Цвет кромки |
| `--glass-ui-shadow` | Полное значение `box-shadow`, например `0 8px 24px rgba(0,0,0,.2)` |
| `--glass-ui-accent` | Цвет выделения/основного действия |
| `--glass-ui-ink` | Цвет текста |

`data-glass-tone="light"` задаёт светлые defaults; без него используются тёмные. Тема продукта может переопределить tokens в своём root. Менять optical theme через `setTheme` и CSS palette — отдельные действия; library не угадывает бренд по названию приложения.

### Состояния native controls и ввод текста

Styles читают `:disabled`, `:checked`, `aria-disabled`, `aria-busy`, `aria-invalid`, `aria-selected`, `aria-pressed` и `:user-invalid`. Для label/control с прямым input состояния `:has(> input:checked)` и `:has(> input:disabled)` оформляют сам контейнер выбора. Это только визуальное отражение native состояния: библиотека не меняет checked/disabled/value/ARIA и не превращает `aria-disabled` в блокировку действий. Приложение продолжает владеть поведением и формой.

`field` по умолчанию имеет `motion.enabled:false`, interaction0; его оптика не наклоняет введённый текст и caret. Для native input/select/textarea/contenteditable и их field wrappers по умолчанию задаётся `data-glass-safe-area="off"`: renderer не увеличивает их DOM padding автоматически. Явно заданный автором safe-area атрибут сохраняется. Textarea/contenteditable получают bounded corner shape с radiusMax20 вместо огромной pill формы. Эти свойства возвращаются при удалении роли/destroy; сами поля, значения, selection и event handlers не пересоздаются. Анимации окружающих контейнеров по-прежнему выбираются их собственной ролью/темой.

### Граница чужого компонента

```html
<section data-glass-role="panel">
  <button data-glass-role="control">Действие</button>
  <div data-glass-ignore>
    <!-- Внешний редактор/полотно владеет своими controls и материалом. -->
    <textarea>Текст редактора</textarea>
  </div>
</section>
```

`data-glass-ignore` исключает **сам узел и descendants** из role/range enhancement, optical declarative selector и прямых правил оформления UI, включая44px native sizing и motion rules. При динамическом добавлении границы возвращаются собственные ранее изменённые атрибуты; при снятии role scan снова доступен. Императивный `setRole()` возвращает пустой список для ignored target. Для `observe:false` после изменения границы вызовите `refresh()`.

Единственное нейтральное исключение — `pointer-events:auto` с нулевой specificity на самом `data-glass-ignore` boundary: оно прекращает унаследованное `pointer-events:none` от Glass container. Это не сброс descendants; собственные explicit правила компонента, включая `pointer-events:none`, сохраняют обычный приоритет. Если ignored boundary должен быть целиком неинтерактивным, задайте `pointer-events:none` непосредственно ему собственным селектором или inline style.

Граница не является Shadow DOM/iframe isolation: наследуемые CSS theme/font остаются обычным наследованием, а стили самого компонента сохраняются. Здесь нет `all:unset`, пересборки чужого DOM, перехвата его API или выдачи полномочий. Styles внутри cross-origin iframe приложение подключает самостоятельно.

## Подключение к другому проекту

Установите пакет командой `npm install volumetric-glass-ui`. Для локальной разработки его также можно подключить из соседней папки или из созданного командой `npm pack` архива. Приложению нужны только модуль и один файл стилей; сборщик заберёт общий WebGL-движок один раз.

## Быстрый запуск

```js
import { createGlassUI } from "volumetric-glass-ui/ui";
import "volumetric-glass-ui/ui.css";

const ui = await createGlassUI({
  root: "#application",
  background: "/images/background.webp",
  canvasMode: "viewport",
});
```

Для длинной страницы с фиксированным фоном используйте `canvasMode: "viewport"`: canvas остаётся размером с экран, поэтому изображение не растягивается на высоту всего документа и сохраняет исходную резкость. Для изолированного блока оставьте стандартный `canvasMode: "root"`.

```html
<main id="application" class="glass-ui">
  <section data-glass-role="shell">
    <nav data-glass-role="navigation">...</nav>
    <article data-glass-role="panel">
      <svg><!-- живой график --></svg>
      <button data-glass-role="accent">Продолжить</button>
    </article>
  </section>
</main>
```

Canvas создаётся автоматически. Весь функциональный DOM остаётся поверх него, а стеклянные поверхности последовательно рендерятся от общего листа к локальным контролам.

## Это материалы, а не закрытый каталог компонентов

Роль описывает физический материал и место в оптической иерархии, а не конкретный виджет. Поэтому библиотеке не требуется заранее знать про ваш компонент. Роль можно назначить стандартному тегу, Custom Element, узлу React/Vue/Svelte, контейнеру карты, редактора или компонента сторонней библиотеки:

```html
<property-filter data-glass-role="card">
  <!-- Любая ваша разметка и логика -->
</property-filter>
```

```js
ui.setRole(myUnknownElement, "card");
```

Если нужен не новый вид компонента, а действительно другой материал или порядок света, зарегистрируйте собственную роль через `roles` или `ui.registerRole()`. Перечислять все возможные элементы сайта в ядре не нужно.

## Девять ролей

| Роль | Оптический слой | Назначение |
|---|---:|---|
| `shell` | 0 | общий цельный лист или корпус приложения |
| `navigation` | 10 | верхняя панель, dock, крупная навигация |
| `panel` | 20 | графики, формы, таблицы, составные секции |
| `card` | 30 | карточка, показатель, тариф, строка списка |
| `field` | 35 | поиск, поле ввода, select, фильтр |
| `control` | 40 | кнопка, вкладка, переключатель |
| `accent` | 45 | главное действие или выбранное состояние |
| `line` | 30 | разделитель, трек, индикатор |
| `overlay` | 80 | popover, drawer, dialog |

Слой определяет не CSS `z-index`, а физическую последовательность: `panel` получает уже преломлённый результат `shell`, а `control` — результат всех поверхностей под ним.

## Нативные поля формы

`ui.css` приводит к общей визуальной системе чекбоксы, радиокнопки, переключатели, range, progress, meter, select, textarea, date и file input. Семантика, клавиатура, значения формы и доступность остаются нативными. Для range `createGlassUI()` автоматически синхронизирует заполненную часть трека; это можно отключить через `enhanceControls: false`.

Само поле получает физическую поверхность обычной ролью:

```html
<input data-glass-role="field" type="date" />
<button data-glass-role="control">Отправить</button>
```

У `fieldset` заголовок автоматически отводится от толстой оптической кромки. Для любого живого содержимого геометрическая safe-area учитывает и окружность углов, и широкую фаску; длинные слова переносятся внутри поверхности, а не выталкивают её по горизонтали.

## Своя роль и материал

Пользовательская тема объединяется с базовой до проверки наследования, поэтому новый материал может наследовать любую UI-роль:

```js
const ui = await createGlassUI({
  root: "#application",
  background,
  theme: {
    name: "Brand material",
    presets: {
      "brand-chart": {
        preset: "ui-panel",
        optics: { depth: 64, refraction: 0.92 },
        style: { tint: [0.35, 0.72, 1], tintStrength: 0.28 }
      }
    }
  },
  roles: {
    chart: { preset: "brand-chart", hitArea: "container" }
  }
});
```

`hitArea: "container"` пропускает указатель к вложенным ссылкам, полям и кнопкам, не разрывая их совместное движение со стеклом. `hitArea: "control"` используется для самостоятельного кликабельного стекла. Для новых ролей безопасным значением по умолчанию является `container`; интерактивный HTML-элемент (`button`, `a`, `input` и другие) всё равно остаётся доступен сам по себе.

После запуска роль можно добавить обычному элементу без пересоздания сцены:

```js
ui.setRole("#sales-chart", "chart");
ui.setRole(document.querySelector("#checkout"), "accent", {
  optics: { depth: 42 }
});
ui.removeRole(".temporary-card");
```

Новые узлы с `data-glass-role` подключаются автоматически. При удалении узла наблюдатели и оптический элемент освобождаются. Декларативный `data-glass-role` принадлежит компоненту: его изменение или удаление сохраняется при detach, ignore и `destroy()`. Императивные `setRole()`/`removeRole()` временно владеют этим атрибутом и возвращают его исходное значение при cleanup. Производные оптические атрибуты возвращаются к исходным значениям в обоих случаях.

## Любое содержимое

- Текст, таблица, форма, видео, SVG и Canvas остаются живым DOM и двигаются вместе с поверхностью.
- Вложенные панели не перехватывают клики: hit-area отделена от оптической последовательности, поэтому сложные формы и меню остаются функциональными на каждом уровне.
- Графики можно обновлять любой библиотекой: роль применяется к контейнеру и не вмешивается в данные.
- Для физической гравировки короткой надписи или знака используйте низкоуровневые `OpticalGlass.text()`, `image()` или `custom()`.
- Не гравируйте длинный основной текст: живой DOM чётче, доступнее и дешевле обновляется.

## Сложная страница

Рекомендуемая иерархия для dashboard, каталога или конструктора:

```text
stage (фон и canvas)
└─ shell
   ├─ navigation
   │  ├─ field
   │  └─ control
   ├─ panel
   │  ├─ card
   │  └─ control
   └─ overlay
```

Не создавайте несколько физических слоёв внутри каждой карточки без визуальной причины. Последовательные поверхности `shell → panel → card → control` уже дают настоящую накопленную глубину. Внутренние `layers` нужны только для отдельного многослойного предмета.

## Качество и производительность

UI-слой не снижает разрешение, число образцов или оптическую модель по показателю FPS. Качество управляется явно через параметры сцены. Оптимизация происходит без изменения картинки: движок ограничивает каждый проход областью элемента, переиспользует атласы и пропускает обрезанные/невидимые поверхности.

Для тяжёлой полноэкранной композиции сначала уменьшайте число декоративных поверхностей, а не физическую точность материала. `dprCap` является осознанным верхним пределом конкретного продукта, а не автоматическим режимом деградации.

## Отказы и диагностика

- Неизвестная роль при первоначальном запуске даёт понятную ошибку до создания сцены.
- Динамическая неизвестная роль помечается `data-glass-ui-state="error"` и попадает в `ui.diagnostics`.
- `onError(error, { target })` позволяет отправить проблему в мониторинг приложения.
- При отсутствии WebGL остаётся функциональный CSS-fallback без имитации физической точности.
- `prefers-reduced-motion` переводит UI в static glass fallback без WebGL; материал и native функциональность сохраняются.

Полный эталон: `examples/dashboard/`.


## Спокойный рабочий материал

Это один opt-in материал поверх тех же девяти ролей. Прежний `GLASS_UI_THEME` и CSS без modifier сохраняются. Экспорт входит в существующий `volumetric-glass-ui/ui`; новый JS option, package version и публикация не добавлены.

```js
import { createGlassUI, GLASS_UI_WORKING_THEME } from "volumetric-glass-ui/ui";
import "volumetric-glass-ui/ui.css";

const root = document.querySelector("#application");
root.dataset.glassMaterial = "working";
root.dataset.glassTone = "light"; // убрать/light→dark при смене существующего tone

const ui = await createGlassUI({
  root,
  background: "/images/workspace-background.webp",
  theme: GLASS_UI_WORKING_THEME,
  renderPolicy: "auto",
});
```

Нужны оба действия: `theme` выбирает оптику; `data-glass-material="working"` выбирает CSS геометрию/палитру/иерархию. Только modifier не меняет материал GPU, только theme не включает working palette. При смене CSS tone consumer по-прежнему выбирает подходящий background через существующий `setBackground`. Переопределённые автором tokens остаются обычным CSS; после изменения ink/accent/backdrop нужно снова проверить контраст.

Working optics сохраняет положительные depth/refraction и настоящий WebGL: depth8–26, refraction0.10–0.22, bevel0.025–0.055, roughness0.008. Поверхности спокойны, с тонкой кромкой, малой неоднородностью и без наклона native controls. Роли и порядок слоёв прежние. Контейнеры сохраняют объём, а введённый текст/caret не становятся частью оптического изображения.

CSS задаёт малым контролам16px radius, textarea/contenteditable20px; круглый SVG-only icon control с aria-label сохраняет круг. Прежняя минимальная область44px не уменьшена. Primary имеет iris fill: light #6050cc с белым текстом, dark #b9adff с тёмным текстом. Selected — tinted surface+edge, keyboard focus — отдельный внешний ring. Составной field владеет одним ring; вложенный native input не получает второй. Поддержаны native checked/disabled/busy/invalid/selected/pressed и непустой aria-current кроме false, включая page/step. Атрибуты, значения, события и permission semantics библиотека не меняет.

Тёмные рабочие поля/контролы имеют читаемую полупрозрачную подложку: это защита текста от светлого изображения под стеклом, а не GPU-off или размытие glyphs. Основной iris accent используется также в native checkbox/radio/range/switch/progress состояниях. `data-glass-ignore`, static policy, context loss, reduced motion и forced colors сохраняют прежние границы; forced colors возвращает системные Canvas/CanvasText/Highlight.

### Проверка локального варианта04.11.2026

- Новый `tests/ui-working-material.test.mjs`: на исходнике2RED, после изменения4PASS. Тот же bounded-optics oracle отвергает прежнюю глубокую lens-theme. Hash исходного GLASS_UI_THEME и первые25255 bytes исходного CSS проверяются неизменными.
- Совместимость: неизменённые ui-kit/render-policy/site-integration28PASS; вместе с новым файлом32PASS.
- `tests/ui-working-material.fixture.mjs` поднимает только локальный HTTP49329; `tests/ui-working-material.browser.mjs` выполняется через playwright-cli в собственной изолированной сессии. Проверяет реальные optical frames, CSS/геометрию/состояния, два tone, static и forced colors, native DOM/value/caret и ignore,390px viewport.
- В browser fixture обе working roots дали gpuReady=true/12optical passes. Primary contrast5.97:1 light/8.15:1 dark. Измеренный по PNG-подложке контраст native textarea11.13:1 light/10.40:1 dark. До исправления тот же pixel oracle находил dark3.35:1; отдельный неизменённый focus oracle находил2rings. Исходные RED сохраняются в `output/playwright/working-material-focus-reading-before.log`.
- Старый material в том же fixture не проходит working oracle: primary/secondary не различены, control geometry и selected fill отличаются от рабочего контракта. Для его прозрачного primary CSS контраст помечен unknown: прозрачный computed fill нельзя считать чёрным фоном. Ранний диагностический вывод с таким числом не является доказательством реального контраста старого рендера.
- Кадры просмотрены: `output/playwright/working-material-optical.png`, `working-material-mobile.png`; полный финальный результат — `working-material-browser-final.log`. Это материал библиотеки на synthetic fixture, не принятие интерфейса Сот, не физический Android и не доказательство читабельности любого произвольного backdrop. Реальные продуктовые screens ещё должны пройти визуальную критику.

Дополнительно browser CSSOM выявил dropped vendor rules: смешанный WebKit/Mozilla selector-list целиком игнорировался браузером. Неизменённый oracle дал RED, затем vendor-specific правила разделены; final CSSOM WebKit thumb/progress=true. Контроль: output/playwright/working-material-native-css-before.log; final structuredproof: output/playwright/working-material-browser-final.json.

## Компактный Source-виджет без JS материала

CSS-only static material — полноценный разрешённый режим. Приложение подключает `ui.css`, явно ставит `data-glass-render-mode="static"` и сохраняет живой авторский DOM. `createGlassUI`, GPU, scene, новый mount/SDK или сообщения родителю для этого не нужны. Этот маркер **не является доказательством optical frame** и не выдаёт полномочий приложению. Native disabled/checked/busy/invalid/focus и forced-colors работают через CSS.

```html
<main class="glass-ui glass-ui__widget"
  data-glass-material="working" data-glass-tone="light"
  data-glass-role="card" data-glass-safe-area="off"
  data-glass-render-mode="static">
  <section class="glass-ui__widget-content">
    <p class="glass-ui__widget-identity">Короткое имя</p>
    <div class="glass-ui__widget-glance"><output>3</output><span>сохранено</span></div>
    <!-- Свои form/list/editor остаются теми же DOM nodes. -->
    <form id="add"><input data-glass-role="field"
      data-glass-safe-area="off" aria-label="Новая запись"></form>
  </section>
  <footer class="glass-ui__widget-actions">
    <button form="add" data-glass-role="accent">Добавить</button>
  </footer>
</main>
```

Общий API композиции — эти CSS-классы. `identity` и `glance` необязательны: визуал принадлежит приложению, не закрытому каталогу шаблонов. Прямой `output` внутри glance получает крупную метрику; карта, фото, список или рисунок остаются обычным Source DOM. Не добавляйте вторую app-level toolbar к уже существующей шапке окна. Поля/кнопки получают явные семантические роли; библиотека не угадывает их по классам приложения.

Корень заполняет выделенную область. Content прокручивается, actions не сжимаются; native action имеет минимум44×44 CSS px. При высоте меньше122px сам документ может прокручиваться: CSS не скрывает editor по media-height/клавиатуре и не обрезает действия. Для полноэкранного iframe-документа автор задаёт `html,body{height:100%;min-width:0;margin:0}body{height:100dvh;overflow-y:auto}`. Не задавайте body `overflow:hidden` и не заменяйте адаптацию `transform:scale` фиксированной1024px страницы. Для карточки внутри другого layout родитель должен выделить ей определённую высоту; глобальный body stylesheet библиотека не меняет.

Маска и внутренний safe-area — дело хоста. Source использует реальный выделенный прямоугольник, не копирует Root mask/padding и не вычисляет world-scale по ResizeObserver. При масштабе полотна44 CSS px могут быть меньше44 экранных: preview остаётся обзором, рабочее попадание проверяется в существующем фокусе1:1. Изменение размера/фокус не требует remount или замены inputs; соседний `data-glass-ignore` остаётся отдельной границей. Если нужен optical material, те же роли можно подключить через прежний `createGlassUI({theme:GLASS_UI_WORKING_THEME,...})`; композиция от этого не меняется.

Переносимый пример: `examples/compact-widget/index.html`, `ui.css`, `ui.js`. Его JS — только демонстрационная модель списка, не material runtime; сохранение/авторизация остаются у приложения. Пример рассчитан на стандартные self assets без inline CSP исключений.

Для host с фиксированными `/assets/ui.js`, `/assets/ui.css`, `/assets/source-sdk.js` объедините bytes общей библиотеки и собственного consumer CSS **при подготовке пакета**, передав результат существующему `ui.style`. `ui.document/script/sdk` и число assets не меняются; дополнительных HTTP routes/import graph/GPU context нет. Наша локальная revision не означает, что опубликованный npm1.4.0 уже содержит этот CSS.

Проверки: `node --test tests/ui-widget.test.mjs`; `node tests/ui-widget.fixture.mjs` и own browser49333 с `tests/ui-widget.browser.mjs`. Один geometry oracle обнаруживает исходный icon34×44, отсутствие content scroll/glance и намеренную desktop1024px страницу; после композиции проверяются четыре viewport, short-height form submission, тот же iframe/input/caret, static/native states и forced-colors. Это библиотечный browser gate; не Android IME/Root app/двух независимых Source приложений приёмка.

## Явный glance/interactive: маленькая UI-only граница

CSS/ResizeObserver внутри iframe видят его logical viewport, а не world-scale родителя. Один размер может быть и обзором, и рабочим фокусом. Если Source показывает «+» в обзоре, а первый tap только открывает фокус, это ложное обещание команды. Для явно подключённого Source используйте отдельный `widget-context`; CSS-only static материал по-прежнему допустим, но сам не доказывает поддержку glance.

```js
// Child / Source. parentOrigin — trusted exact config, например profile.parentOrigin.
import {bindGlassWidgetContext} from "volumetric-glass-ui/widget-context";
const context = bindGlassWidgetContext(main, {parentOrigin});
// При teardown: context.dispose().
```

Source оставляет identity/glance и оборачивает form/list/editor в `.glass-ui__widget-editor`. Существующая `.glass-ui__widget-actions` остаётся отдельным slot. В явном `data-glass-widget-mode="glance"` скрыты только editor/actions, glance виден; в interactive библиотека не скрывает их по height/keyboard и не заменяет DOM, input/value/selection. Авторский `hidden`/ignore остаётся авторским. Source сохраняет собственный читаемый material fill: в normal-colors glance снимаются только декоративные border/shadow/radius, чтобы родитель мог рисовать outer mask. Полная прозрачность оказалась нечитаемой при несовпадении Source tone и parent backdrop; forced-colors сохраняет системный материал. Parent/Source body transparency не переписывается библиотекой.

```js
// Parent. origin — exact actual iframe entry origin, не "*" и не произвольный URL.
import {createGlassWidgetHost} from "volumetric-glass-ui/widget-context";
const host = createGlassWidgetHost(frame, {
  origin, initialMode: "glance", isCurrent,
  onReset() { frame.removeAttribute("data-widget-context"); },
  onReady() { frame.dataset.widgetContext = "ready"; },
});
host.setMode("interactive"); // существующее действие host focus
host.setMode("glance");      // существующий возврат на полотно
host.dispose();             // перед reset/закрытием iframe binding
```

Оба API принимают optional `view` для своего window. Child без нового host/в standalone остаётся interactive. Parent default также interactive; preview host явно выбирает glance. Повторный `setMode` того же значения — no-op. Source может объединить helper ESM bytes и собственный module script в существующий `ui.script`: нет imports в helper и нет четвёртого Native HTTP asset. Полный SDK/app-work не меняется. Runtime module и adjacent types: `src/widget-context.js`, `widget-context.d.ts`; package export `./widget-context`, тип `GlassWidgetMode`.

Протокол — только schema `glass-ui.widget-context.v1`, strict plain exact envelopes: announcement `{schema,type:"ready"}`; host mode `{schema,type:"mode",mode,epoch}`; acknowledgement `{schema,type:"ready",epoch}`. Epoch — монотонный целочисленный UI correlation, **не credential/grant/доказательство Source authority**. Нет actors/tokens/URL/commands, polling, TTL, cache или общей шины. Child проверяет exact configured origin + event.source===parent; host — origin + frame.contentWindow + isCurrent. Лишние/неизвестные/accessor поля отвергаются.

WindowProxy переживает Source reload/navigation. Поэтому каждый bind/load вызывает onReset, меняет UI epoch и отправляет текущий mode; только ACK текущего epoch вызывает onReady один раз до следующего load. Начальный ready лишь просит current mode. Старая queued ready/ACK не скрывает legacy header нового документа; поздние callbacks после dispose не работают. На Source permissions/session/SDK/container mask эта готовность не влияет. Переход к legacy странице означает fallback header/actions, а не выдуманную adoption.

Проверки: `tests/widget-context.test.mjs` (strict boundaries, current/dispose, stale epoch, same-mode no-op); own two-origin fixture `node tests/widget-context.fixture.mjs` (49333/49334), `tests/widget-context.browser.mjs` и `widget-context-contrast.browser.mjs` через отдельный CLI. Реальный same-frame reload→legacy→new helper, одинаковый viewport с разными modes, живой draft/caret/short-height submit и contrast light/dark проверяются отдельно от прав Source. Старый no-context control оставляет ложное действие и не проходит glance oracle. Эти проверки не заменяют Root/Android/native-provider acceptance.
