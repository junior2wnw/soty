# Подключение к другому сайту

```bash
npm install volumetric-glass-ui
```

## Рекомендуемый уровень

Для обычного продуктового сайта используйте `createGlassSite()` из `volumetric-glass-ui/site`. Низкоуровневый `OpticalGlass.create()` нужен только для специальных Canvas/SVG-форм, нескольких независимых проходов или собственного управления рендером.

```js
import { createGlassSite } from "volumetric-glass-ui/site";
import "volumetric-glass-ui/design.css";

const glass = await createGlassSite({
  root: "#hero",
  background: "/images/hero.webp",
  fallbackBackground: "/images/hero-fallback.jpg",
  canvasMode: "viewport",
  dprCap: 1.5,
});
```

Если `canvas` не передан, адаптер создаёт его первым дочерним узлом `root`, изолирует композицию, запускает движок и удаляет свои ресурсы при обычном уходе со страницы. Переданный пользователем canvas библиотека не удаляет.

`canvasMode: "viewport"` нужен, когда один фиксированный фон продолжается через длинную страницу: размер canvas остаётся равен экрану, поэтому фотография не растягивается на всю высоту документа и сохраняет резкость. Для отдельной секции со своим фоном оставьте стандартный режим `"root"`.

Для `ShadowRoot` передайте canvas явно: это сохраняет управление стилями внутри границы shadow DOM и не создаёт скрытых элементов снаружи компонента.

## Разметка

```html
<section id="hero">
  <article data-optical-glass="site-panel" data-glass-safe-area="auto">
    <h1>Квартира у воды</h1>
    <button data-optical-glass="site-control" data-glass-shape="pill">Смотреть</button>
    <div data-optical-glass="site-line" data-glass-shape="line"></div>
  </article>
</section>
```

Встроенная спокойная тема содержит `site-panel`, `site-control` и `site-line`. Динамически добавленные отмеченные элементы подключаются автоматически.

## Переносимая тема

Тема является обычным JSON без функций, DOM-ссылок и пользовательского содержимого:

```json
{
  "format": "optical-glass-theme",
  "version": 1,
  "name": "My site",
  "presets": {
    "site-custom": {
      "preset": "quiet",
      "optics": { "depth": 72, "refraction": 0.9 },
      "style": { "matte": 0.04, "tint": [0.72, 0.9, 1] }
    }
  }
}
```

```js
import theme from "./glass-theme.json" with { type: "json" };

const glass = await createGlassSite({ root: "#hero", background, theme });
glass.setTheme(nextThemeJson);       // повторно применит data-атрибуты
const saved = glass.exportTheme();  // нормализованный JSON
```

Лаборатория копирует текущий материал в совместимую тему с пресетом `site-custom`. В целевом HTML достаточно написать `data-optical-glass="site-custom"`.

## Фон и несколько сцен

Это физическая оптика, а не CSS-фильтр: движок должен получить тот же источник, который виден за стеклом. Оставляйте ту же картинку CSS-фоном секции — это одновременно корректный fallback без WebGL.

- Один hero с одним фоном — одна сцена на hero.
- Один фиксированный фон через длинную страницу — одна сцена с `canvasMode: "viewport"`.
- Длинная страница с разными фонами — отдельная сцена на крупную секцию.
- Карточки с разными фотографиями — отдельная сцена внутри каждой карточки или общий фон карточек.
- Не используйте один фиксированный canvas для участков, за которыми показываются разные несвязанные изображения.

## SPA и жизненный цикл

`autoDestroy: true` закрывает ресурсы при обычном `pagehide`, но сохраняет страницу в back/forward cache. При размонтировании SPA вызовите метод явно:

```js
const glass = await createGlassSite(options);

return () => glass.destroy();
```

`destroy()` повторяем: второй вызов безопасен. `setTheme()` и `setBackground()` после уничтожения возвращают понятную ошибку.

## Границы ответственности

- Настоящие кнопки, ссылки, поля и списки остаются DOM и сохраняют доступность.
- `data-glass-safe-area="auto"` защищает содержимое от круглых краёв.
- Для рабочих интерфейсов начинайте с `quiet`; выраженные значения оставляйте акцентам.
- JSON-тема описывает материал, но не переносит пользовательский текст, HTML и обработчики.
- Пресеты темы могут наследовать друг друга в любом порядке; неизвестные имена и циклы отклоняются до запуска сцены.
- Пакет публикуется под стабильным именем `volumetric-glass-ui`; содержимое каждого релиза проверяется через `npm pack` и полный тестовый набор.

Рабочий минимальный проект: `examples/quick-start/`.
