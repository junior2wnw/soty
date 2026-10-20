# Диагностика запуска

Открой `docs/deployment/app-troubleshooting.md` в актуальном repo. Порядок:
документ/Network timing → boot/admission/cookie → app transport/CSP → identity →
данные/reload. HTTP 200, iframe.src и событие load сами не подтверждают приложение.

Root phase trace — partial, до 24 событий/120s, без child DOM и URL. root_load
не равен boot_document_ack; отсутствие ACK не доказывает сбой. Сохраняй только
закрытые состояния и безопасные коды. Не выгружай cookies, токены, auth headers,
полные boot URLs, device keys, watch IDs или приватные документы.

Если standalone работает, а embedded document stalled **до DNS/connect** и
debugger attached, проверь влияние инструмента. Воспроизводимый случай был
20.10.2026: штатная отмена task debugger и новый launch восстановили iframe.
Не ставь этот диагноз по одному пустому окну. Пользуйся разрешённым каналом,
не отключай browser security и не переподключай блокирующий инструмент ради QA.
Этот reference сам не разрешает альтернативную технологию управления браузером.

Если network/CSP подтверждает запрет parent — проверь exact frame-ancestors и
X-Frame-Options. Если isolated запрещает нужный nested chart — review native
конкретного приложения, сохранив общий sandbox. WebSocket 400 в auth — проверь
Upgrade headers только в auth subrequest; 101 + message echo и отрицательные
Origin/ACL/pin checks обязательны. См. operations.md.

Expired boot → один свежий launch из текущей карточки. Unsupported readiness
после load → нейтральное loaded; не fake authenticated ready. Denied/unknown
scoped readiness не превращаются в успех. Lost persistence → active volume,
single writer, schema до optional workers и стабильный account mapping.

Общий помощник принимает только enum observations:

```json
{"embedded":"blank","standalone":"works","document":"stalled_before_connection","debugger":"attached"}
```

CLI: `node scripts/soty-app-release.mjs doctor --observations FILE --output NEW_FILE`.
Root MCP: `apps_diagnose({observations:{...}})`. Полная схема доступна в tools/list.
Ответ различает observed/hypothesis/insufficient_evidence, отмечен liveVerified=false
и releaseReadiness=not_assessed. Он не запускает проверок, не авторизует и не
меняет политики. Если живой Root ещё старый, не выдумывай наличие нового MCP;
используй проверенную CLI из актуального repo.
