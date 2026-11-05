# Volumetric Glass Engine

Интерактивная полноэкранная лаборатория реалистичного многослойного стекла. Каждый слой рендерится отдельным оптическим проходом и получает уже преломлённый свет предыдущего слоя.

## Возможности

- цельные объёмные линзы, а не плоский `backdrop-filter`;
- последовательное преломление через любое число слоёв;
- расчёт света в линейном пространстве и FP16-буферы без накопления полос;
- Beer–Lambert-поглощение и матовость по локальной толщине стекла;
- обычные скруглённые элементы и произвольные Canvas/SVG-силуэты;
- текст, изображение или пользовательский рисунок внутри стекла;
- отдельная внутренняя лазерная гравировка: объёмный текст, растровые маски и Canvas-фигуры с картой глубины;
- живой DOM-контент: списки, формы, поля, ползунки, переключатели и любые виджеты;
- безопасная зона содержимого, которая автоматически растёт вместе со скруглением;
- декларативное подключение через `data-optical-glass` и автоматическое наблюдение за динамическими элементами;
- готовые пресеты `clear`, `frosted`, `panel`, `control`, `dense` и `line`;
- спокойный производственный пресет `quiet`, который используется декларативно по умолчанию;
- гравировка текста в общей карте высот;
- управляемые толщина, положение, фасеты, зерно, свечение, параллакс и дисперсия внутренней гравировки;
- матовость, IOR, толщина, шероховатость и микрорельеф;
- живой поворот и блик за указателем;
- связанное движение живого DOM, подложки и вложенных стёкол с 3D-параллаксом и инерцией;
- наклон всей композиции от датчика ориентации телефона с автоматической калибровкой;
- фильтрация дрожания, компенсация поворота экрана и fallback на акселерометр;
- универсальные стеклянные меню: popover, drawer и orbit;
- встроенная панель настройки каждого элемента и слоя;
- быстрые стресс-пресеты стеков 1×, 2×, 4× и 8×;
- независимый ES-модуль для Vite, Webpack и обычного `<script type="module">`;
- несколько сцен и локальные селекторы внутри отдельных контейнеров или ShadowRoot;
- горячая смена фонового изображения без пересоздания сцены и встроенный фон при ошибке загрузки;
- адаптивный интерфейс и режим reduced motion.
- необязательный безымянный огонёк для безопасного взаимодействия посетителей между сайтами.

## Подключение в другой проект

Установите публичный пакет:

```bash
npm install volumetric-glass-ui
```

### Целый интерфейс из семантических ролей

Для dashboard, каталога, кабинета или сложной посадочной страницы используйте универсальный UI-слой. Он сохраняет ваш HTML и добавляет только материал и физический порядок поверхностей:

```js
import { createGlassUI } from "volumetric-glass-ui/ui";
import "volumetric-glass-ui/ui.css";

const ui = await createGlassUI({
  root: "#application",
  background: "/images/background.webp",
});
```

```html
<section data-glass-role="shell">
  <nav data-glass-role="navigation">...</nav>
  <article data-glass-role="panel">
    <div data-glass-role="card">...</div>
    <button data-glass-role="accent">Продолжить</button>
  </article>
</section>
```

Встроены роли `shell`, `navigation`, `panel`, `card`, `field`, `control`, `accent`, `line` и `overlay`. Это материалы, а не закрытый каталог виджетов: роль можно назначить любому стандартному тегу, Custom Element или контейнеру компонента вашего фреймворка. Нативные checkbox, radio, switch, range, progress, meter, select, textarea, date и file input получают согласованный UI из `ui.css`. Собственные роли связываются с переносимыми материалами через опцию `roles`. Сложный рабочий экран находится в `examples/dashboard/`, полное описание — в [документации UI-слоя](docs/UI_KIT.md).

Самый короткий путь — высокоуровневый адаптер. Он создаёт canvas внутри выбранной секции, подключает переносимую тему, находит декларативные элементы и запускает сцену:

```html
<section id="hero" style="background:url('/images/apartment.webp') center/cover">
  <div data-optical-glass="site-panel" data-glass-safe-area="auto">
    <h1>Квартира у воды</h1>
    <button data-optical-glass="site-control" data-glass-shape="pill">Смотреть</button>
  </div>
</section>
```

```js
import { createGlassSite } from "volumetric-glass-ui/site";
import "volumetric-glass-ui/design.css";

const glass = await createGlassSite({
  root: "#hero",
  background: "/images/apartment.webp",
});

// Горячая смена переносимой темы или фона:
glass.setTheme(themeJsonOrObject);
await glass.setBackground("/images/apartment-night.webp");
```

Лаборатория умеет копировать текущий материал в JSON-тему с пресетом `site-custom`. Такой JSON передаётся в `theme` при создании сайта или в `glass.setTheme(...)`. Минимальный проверяемый пример находится в `examples/quick-start/`.

Низкоуровневый API остаётся доступен для нестандартных фигур, нескольких canvas и ручного управления жизненным циклом.

Импортируйте низкоуровневый модуль напрямую, если нужен полный контроль над сценой:

```js
import OpticalGlass from "volumetric-glass-ui";

const root = document.querySelector(".property-card");
const scene = OpticalGlass.create({
  root,
  canvas: "canvas",
  background: "/images/apartment.jpg",
});

scene.add({
  target: ".property-card__button",
  optics: { ior: 1.47, depth: 48, refraction: 1.2 },
});

await scene.start();
```

Для составного интерфейса достаточно оставить обычный доступный HTML и отметить стеклянные части:

```html
<section
  data-optical-glass="panel"
  data-glass-depth="72"
  data-glass-layers="3"
  data-glass-safe-area="auto"
>
  <input aria-label="Поиск" />
  <button
    data-optical-glass="control"
    data-glass-shape="pill"
    data-glass-parallax="8"
  >Найти</button>
</section>
```

```js
const composition = scene.mount(document.body);

// Фон можно менять на лету: накопленные элементы и обработчики сохраняются.
await scene.setBackground("/images/apartment-evening.webp");
```

Новые отмеченные элементы подключаются автоматически, а удалённые освобождают свои обработчики. Полный функциональный стенд: `http://127.0.0.1:4173/examples/universal/`.

Для продуктовых экранов подключите необязательные безопасные UI-токены и начинайте с `quiet`. Выразительные пресеты оставлены для акцентов и демонстрационных сцен:

```js
import OpticalGlass from "volumetric-glass-ui";
import "volumetric-glass-ui/design.css";

scene.add({
  target: ".property-filter",
  preset: "quiet",
  content: OpticalGlass.live(".property-filter"),
});
```

Практические ограничения типографики, движения, панелей и fallback описаны в [производственном профиле](docs/PRODUCTION_GUIDE.md).

### Живое присутствие между проектами

Вместо общих настроек или смены фона библиотека предлагает добровольный световой импульс. Он ничего не меняет на чужой странице, не содержит текста, имени или постоянного идентификатора и исчезает через 12 секунд:

```js
import { createCommunityPresence } from "volumetric-glass-ui/presence";
import "volumetric-glass-ui/presence.css";

const presence = createCommunityPresence({
  target: "#community-field",
  endpoint: "https://presence.example/v1/pulses",
  space: "our-product-family",
}).start();
```

Одинаковый `space` объединяет разные сайты. Без endpoint остаётся честный локальный режим между вкладками. Контракт сервера и reference relay: [docs/COMMUNITY_PRESENCE.md](docs/COMMUNITY_PRESENCE.md).

Независимый проверочный пример: `http://127.0.0.1:4173/examples/standalone/`.

## Запуск

```bash
npm install
npm run dev
```

Откройте `http://127.0.0.1:4173/`.

Проверка production-сборки:

```bash
npm run test
npm run build
npm run preview
```

Полная проверка тестов и сборки одной командой: `npm run check`.

## Структура

```text
.
├── index.html              # разметка демо и панели настройки
├── src/
│   ├── liquid-glass.js     # независимый WebGL2-движок
│   ├── site.js             # запуск сцены и переносимые темы
│   ├── ui-kit.js           # семантические роли сложного интерфейса
│   ├── app.js              # композиция и логика инспектора
│   └── styles.css          # интерфейс и адаптивность
├── public/                 # фон и локальный шрифт
├── examples/standalone/    # две независимые сцены и проверка жизненного цикла
├── examples/dashboard/     # плотная вложенная композиция из UI-ролей
├── examples/universal/     # живые списки, формы, ползунки и произвольные фигуры
├── tests/                  # регрессии толщины, рассеяния и стеков
└── docs/
    ├── LIQUID_GLASS.md     # API и примеры переиспользования
    └── DESIGN.md           # исходное дизайн-направление
```

Подробное API: [docs/LIQUID_GLASS.md](docs/LIQUID_GLASS.md).
