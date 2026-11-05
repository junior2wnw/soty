# Автономные браузерные проверки World

Три локальных layout/DOM сценария имеют расширение `.browser.mjs`, потому что им нужны Playwright и установленный Google Chrome. Они запускаются отдельно от Node-only `world:test`; успешный `world:test` без этих трёх сценариев не закрывает общий release gate. Отсутствие браузера или модуля означает незавершённую проверку, а не SKIP/PASS.

Из корня проекта, с установленными зависимостями и Chrome:

```powershell
$env:SOTY_PLAYWRIGHT_MODULE = 'playwright'
$env:SOTY_BROWSER_ARTIFACTS_DIR = Join-Path $env:TEMP 'soty-browser-fixtures'
node --test --test-concurrency=1 src/world/avatar-context.browser.mjs src/world/canvas-vault-locked-preview.browser.mjs src/world/glass-action-sizing.browser.mjs
```

В `SOTY_PLAYWRIGHT_MODULE` также можно передать `file:///.../playwright-core/index.mjs` уже установленного инструмента. Проект не добавляет Playwright в production dependencies. `SOTY_BROWSER_ARTIFACTS_DIR` задаёт каталог PNG; по умолчанию используется `soty-browser-fixtures` в системном временном каталоге.

Каждый файл запускает собственный headless Chrome и создаёт новые анонимные контексты через `browser.newPage`. Используются только `setContent`, встроенный локальный bundle и синтетические ответы; существующий Root, аккаунты и профили браузера не открываются. Hooks закрывают только принадлежащие тестам страницы и браузер.

`avatar-context` проверяет границы authority и задержанные ответы фактического AvatarHydrator. `glass-action-sizing` измеряет цели 44×44 и проверяет независимость Source; его отрицательный контроль удаляет ровно одно правило floor. Названия `optical`/`static` здесь обозначают DOM attributes, а не подтверждённый WebGL rendering.

`canvas-vault-locked-preview` проверяет preview/focus фактического renderer и сохраняет OLD-контроль под тем же oracle. `src/world/test-fixtures/canvas-vault-renderer-before-locked-preview.ts.txt` — дословный snapshot D11 из `research/d11-locked-preview-before/canvas-vault-renderer.ts`, созданного и перенесённого 05.11.2026: SHA256 `eef1129808367e3a584e78e7d44e30e99b8a765fc4edffceace54a06d8c4a786`, 18 870 байт. Суффикс `.txt` исключает исторический TypeScript из сборки продукта; harness проверяет его hash перед запуском. Это проверка геометрии синтетического locked projection, отдельная от реальных password/ACL проверок.

`glass-action-sizing.browser.mjs`: 8 top-level browser tests (прежние6 +2 viewport проверки native pointer boundary). Весь исходный файл, включая19 lexical assert call sites и тела тестов, сохранён дословным byte prefix. Новый сценарий использует настоящий vendored uPlot и реальный Chrome pointer: native click, drag selection → x-scale zoom → double-click reset. Собственные class/inline/descendant `pointer-events:none`, uPlot selection/cursors и прежняя click-through семантика Glass проверяются отдельно. DOM modes optical/static не доказывают GPU/Android/IME.
