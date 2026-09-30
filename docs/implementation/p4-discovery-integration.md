# P4-A — локальная интегральная приёмка

30.09.2026. Основание — [контракт](p4-discovery-contract.md), [порядок интеграции](p4-discovery-http-plan.md), [A1](p4-discovery-a1.md), [A2](p4-discovery-a2.md), [независимая проекция](p4-discovery-independent.md) и [независимый HTTP](p4-discovery-http-independent.md). Production не изменялся; внешняя индексация, OAuth, MCP и исполнение Notes этим этапом не включаются.

## Что работает

Один public projection обслуживает bounded search/get, versioned semantic contract, exact input/output schemas, OpenAPI3.1.2, страницы `/agents`, version page и sitemap. Каждая public версия имеет явную RU/EN документацию с проверяемыми примерами. Notes @1 сохраняет digest `95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204`; документация может меняться независимо. `notesCreateEnabled:false,audience:null` остаются честным текущим состоянием.

Private metadata не влияет на items, total, revision, cursors, ETag или sitemap. HTTP всегда выбирает доступное public представление до conditional response. Query и path декодируются строго; неподдерживаемый метод/повреждённый URL/неизвестный route не превращаются в SPA200. JSON public CORS без credentials. Canonical origin — только явная проверенная конфигурация shell, не Host или Forwarded. Недопустимая origin-конфигурация отвергается до открытия storage. Existing capability documentation проверяется до её SQLite open/pinning. Новых форматов БД нет.

В меню профиля есть одна обычная ссылка «Для разработчиков и ИИ», открывающая документацию в отдельной вкладке. Основная навигация и редакторы остаются в исходной вкладке. Задействованы общие graphite palette/hex, без старых UI styles и без внешних шрифтов/изображений.

## Фактические проверки root

Все перечисленные наборы исполнены после финальной A2 browser-delta и secondary link. Тяжёлые наборы шли последовательно.

| Проверка | Результат | Лог в `output/implementation-20260930` |
| --- | --- | --- |
| Capabilities domain + все capability HTTP + executor policy | **106/106 PASS**, 0 skips, 4.14s | `p4-discovery-capabilities-final-root.log` |
| Полный world/apps/notes/server/UI/geometry/theme/PWA/transport | **856 tests / 851 PASS / 0 FAIL / 5 прежних opt-in skips**, 94.11s | `p4-discovery-world-final-root.log` |
| Connect и настоящий dev-server integration | **73/73 PASS**, 5.06s | `p4-discovery-connect-dev-final-root.log` |
| TypeScript | PASS | `p4-discovery-types-final-root.log` |
| Production frontend + connector release build | PASS | `p4-discovery-build-final-root.log` |

Пять opt-in сценариев: immutable installed connector source flow, 130-second silent WS, настоящий OpenCode job, connector registration-recovery и настоящий512MB file. Их прежние отдельные квитанции сохраняют свои границы; текущий обычный набор не объявляется повтором этих опытов. Независимый HTTP fixture прошёл7/7 без skips; author27/27 и independent pure10/10 относятся к своим отдельно описанным срезам. Установленный Python jsonschema4.26.0 действительно проверил response DTO; полный OpenAPI conformance certification не заявляется.

## PWA

`/agents` и `/api/capabilities` network-only: offline ответ не подменяется cached shell. Все window clients по-прежнему должны подтвердить готовность к обновлению. Малый optional script есть только в read-only SSR документе; он отвечает transferred port настоящего ServiceWorker, не регистрирует worker и не открывает storage. Старый cached SPA мог оказаться под `/agents`, поэтому исключения из draft handshake по URL нет. Изменившиеся client IDs/URLs требуют нового подтверждения. Молчание по истечении10s блокирует update.

11 выполняемых worker/helper проверок прошли отдельно и входят в общий root набор: dirty editor veto, старый SPA под `/agents`, silent client, URL change, native source/port, offline API navigation и прежние shell routes. Actual installed multi-window PWA update в ОС отдельно не воспроизводился; dev worker не выдаётся за production worker.

## Настоящий браузер

Корневой стенд — `http://127.0.0.1:5420`, перезапущен на финальном renderer; данные изолированы от production. Через обычное меню профиля клавиатурой открыта новая вкладка `/agents`, исходная main/PWA осталась открытой.

Index и detail измерены при **320×760,667×375,1280×800**. Горизонтального overflow нет; видимые элементы действия высотой≥44px. На узком экране поиск закономерно расположен в две строки. Первоначальная desktop ошибка сжатия кнопки исправлена: на667/1280 кнопка80.27×48.80px, на320 —277×46.80px; «Найти» целиком. Keyboard Tab показывает outline3px. RU/EN summary больше не повторяет readiness; RU outline исправлен H1→H2, English H2→H3 сохранён. Основные изображения — `p4-discovery-index-{320,667,1280}.png`, `p4-discovery-detail-{320,667,1280}.png`; измерения — `p4-discovery-{index,detail}-geometries.json`. Снимок667 detail повторён после завершения native paint: первая быстрая серия resize захватила незавершённый кадр и не используется как доказательство вида.

Нативная GET форма ищет «сохранить заметку» и “create private note”, возвращает Notes; «удалить записку» даёт честный0. Search получает noindex,follow, canonical остаётся чистым index. Native details раскрывает English examples клавиатурой.

Отдельный QA-only proxy5430 передаёт **неизменные реальные SSR bytes** и заменяет только `script-src 'self'` на `script-src 'none'`. В этом настоящем браузерном документе повторены search, empty state, detail и раскрытие examples. Proxy не пересылает credentials, writes, shell или произвольные destinations. HTTP-проверка подтвердила строгий CSP и одинаковый SHA-256 HTML на5420/5430: `a140ea37a184313e62efe95a5920f560fe0ca721b1fc491187da9b60e7a69e89`,12418B. Квитанция `p4-discovery-no-script-browser.json`. Это проверка выполнения под запретом scripts, не изменение пользовательских browser settings.

**Граница browser-инструмента:** открытие JSON link встроенный браузер остановил с `net::ERR_BLOCKED_BY_CLIENT`; ни навигация, ни download не завершились. Этот результат не назван успешным browser JSON переходом, другой механизм обхода не применялся. Те же настоящие адреса схем/OpenAPI проверены отдельным HTTP transport и независимым набором; output schema200/application-json,178B одинаковых bytes на обоих origin. Обычный внешний браузерный переход к JSON остаётся отдельной проверкой выпуска. Физический телефон, browser zoom200%, screen readers, внешние HTTPS/DNS и индексация не подменяются viewport-тестами.

## Следующий отдельный этап

Локальная реализация discovery и узкая P2 ошибка focus приняты в описанной области. **P4 целиком не завершён.** Следующий P4-B1 фиксирует настоящие historical Notes/Capabilities fixtures, DDL/native API и trusted storage readers до любой миграции. B2 добавляет create-only durable effect с постоянным proof/reconcile, общий бюджет, отзыв, потерю ACK и отсутствие resurrection. OAuth/MCP, два настоящих клиента и production release идут позже по [preflight](p4-external-agent-preflight.md).
