# P4-C2a — интеграция MCP в действующий host

Статус: **C2a source/wire checkpoint принят локально**. C1 `8367febbb38c465f8c81b6a4248a971add26ac03` pushed перед началом C2a. Работа находится только в отдельной release worktree; original checkout и production не изменены.

## Общие операции и composition

Root извлёк `createCapabilityOperations({service,origin})` из `capabilities-actions.js`: строгая текущая Bearer authentication, фиксированное admit → proof-first reconcile → begin/execute → authorized get и прежняя явная безопасная проекция. HTTP использует этот же порт; статус201/202/200, Location, errors и historical Note link сохранены. `origin=H` определяет ссылку, а `audience=H` либо `H/mcp` — проверяемое назначение credential. Actorless reconcile никогда не выдаёт внешний read.

`http-app.js` монтирует MCP после actual Connect admission и перед AS-off OAuth guard. Shutdown сначала отменяет и дожидается MCP exchanges, затем закрывает domain stores. MCP не включает native execution/AS, не мигрирует storage и не создаёт отдельную identity, очередь или бюджет. Standalone OAuth mount сохраняет свой конечный fallback.

Composed OpenAPI описывает только реально настроенные OAuth/MCP surfaces отдельными host flags; readiness не меняет кешируемый документ. Сам HTTP contract и точные native schemas остаются источником transport output schema. Устаревший documentation sidecar теперь описывает действующее well-formed Unicode/full-document admission; semantic capability digest и schemas не менялись.

## Dependencies

Финальный выбранный runtime: exact `@modelcontextprotocol/server` и `core`2.2.0, devclient2.2.0 с pnpm lock. Исследованный Node adapter2.1.0 сначала установлен, затем удалён как неиспользуемая прямая зависимость. Наш существующий bounded Express gate создаёт стандартный Web Request из trusted H, проверенных headers и собственного AbortSignal; автоматический Node writer начал бы отправлять SDK body до последнего authority check. SDK codec/dispatch и error codec остаются штатными; SSE не переписывается.

## Исполненные root gates

| Проверка | Результат | Лог |
|---|---|---|
| Публичный sidecar/immutable schemas и digest | 15/15 PASS,0skip,225.1152ms | `p4-mcp-sidecar-root.log` |
| Первый affected HTTP import | 4PASS/3FAIL; runtime import type-only `InvalidParamsError` не существует | `p4-mcp-http-extraction-root.log` |
| Исправленный affected HTTP/AS-off/OpenAPI/resource | 18/18 PASS,0skip,7231.4727ms | `p4-mcp-http-extraction-green.log` |
| Typecheck | PASS, exit0 | `p4-mcp-types-root.log` |
| Штатный prebuild и production build | PASS, exit0; Vite3.34s | `p4-mcp-build-root.log` |

Логи находятся в `output/implementation-20260930/`; первый failure не выдаётся за бизнес-regression после выполненного effect: fixture до domain startup не дошёл. Коррекция импортов использует actual public runtime `ProtocolError`/`INVALID_PARAMS`.

Авторские [wire/lifecycle проверки](p4-mcp-transport.md): финальный собственный набор **17/17 PASS,0skip,5599.6769ms**. Независимые [SDK/authority/byte проверки](p4-mcp-independent.md): **6/6 PASS,0skip,2976.126ms** до последнего narrow early-body repair; дополнительный независимый raw `/mcp?` case сначала RED, затем неизменённый author-run **1/1 PASS,0skip,1090.9005ms** на final source. Это два отдельных результата, не заявление о новом общем7/7.

Подтверждены штатные Client2.2 modern/legacy, actual signed authority/native Note, public-only noninterference и bounded output. Последние authority/abort cases держат EOF настоящего SDK SSE, отзывают действующий signed доступ и подтверждают ноль private response bytes; возможный уже совершённый Note effect сохраняется. Найденные no-echo/early-body defects имеют причинные RED → source fix → неизменённый GREEN. Final source review критика не оставил открытых C2a blockers.

Final hashes: MCP `dd49896da1f84054fb963828ae97052a7990305713327e1b1f947aebb37e8af1`, ingress `2647b8818b8151b3a40f84a5fe9a163d4131fd9e0846135330f8ef01b635a08d`, tools `58ac86a429d2f9f6fbca82beadb42ebc7856c464f568cb3871f901d742ffba2c`. Авторский receipt SHA256 `251b05aeae46b3fbdbfe5bafa506c8f0e09a9e69e00400bb703d23b7228fed2c`, независимый receipt `eebc7be7a49e2f9a5b0d929bb9c42b997aa30c78d8d314f5600d3ec6476431bb`.

## Открытая приёмка

Следующий отдельный этап C2b — service child + owner parity; затем C2c — genuine selected CLI. SDK клиент не заменяет Codex/OpenCode login/tool/model acceptance. D1, HTTPS/image/backup/isolated restore и остальные P5–P8 остаются в master-плане. C2a не включает execution/AS, не мигрирует stores и не является production deployment.
