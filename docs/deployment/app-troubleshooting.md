# Приложение не видно: проверяем причину

Сначала разделите **внешний документ, boot, приложение, вход и его данные**.
Успешный HTTP boot или событие iframe `load` не доказывают работу приложения.
Нельзя исправлять пустое окно расширением аудитории или отключением защиты.

| Наблюдение | Следующая проверка |
|---|---|
| Отдельно работает, в Сотах пусто | Network timing, текущий debugger, parent/child CSP, затем boot/session |
| Document pending/stalled до DNS/connect при attached debugger | Возможное вмешательство инструмента. Сравнить живое окно с разрешённым отключением только task debugger и новым запуском |
| Подтверждён `frame-ancestors`/X-Frame-Options отказ | Разрешить только точный проверенный parent у приложения; сохранить его остальные политики |
| Вложенный график заблокирован `frame-src` isolated | Проверить native fixed ingress для этого приложения; не менять общий sandbox |
| WebSocket 400/upgrade rejected внутри auth | Удалить upgrade headers только из `forward_auth`; проверить настоящий 101 + обмен сообщениями |
| WebSocket/POST с чужим или отсутствующим Origin | Правильный отказ. Исправлять собственный exact Origin/callback, а не снимать guard |
| Boot ticket expired | Новый launch/refresh из текущей карточки; не replay старого URL |
| Boot cookie rejected | Exact HTTPS origin, cookie policy и штатный boot flow в авторизованном браузере |
| Access denied / source pin changed | Свежий inspect и account/audience/source; supported prepare/promote. Не удалять app ID |
| Документ загружен, readiness unsupported | Нейтральное loaded; app UI/auth/data проверять отдельно. Не объявлять fake authenticated ready |
| Потеря записи после reload | Активная БД/volume, stable issuer/sub association, migrations до optional workers, единственный writer |

Debugger interference — гипотеза до сравнения. В расследовании 20.10.2026
Chrome оставлял cross-origin iframe stalled до соединения при подключённом
инструменте; штатная отмена task debugger и новый boot восстановили его.
Повторное подключение того же инструмента ради скриншота может воспроизвести
сбой. Это не общий диагноз для каждого пустого iframe. Используйте доступный
разрешённый канал и соблюдайте browser policy; разрешение на другой способ
управления нельзя выводить из этого документа. Не выключайте Chrome security,
не выгружайте cookies/tokens и не очищайте browser storage.

Root «Диагностика запуска» даёт ограниченный phase trace: только закрытые phases,
округлённое время и Root frame visibility/hit flags; до 24 событий/120 секунд.
Это частичные наблюдения, не authority/readiness. `root_load` и
`boot_document_ack` различаются; отсутствие ACK не доказывает сбой. Не добавляйте
к trace child DOM, приватный текст, boot/watch IDs или полные URL.

## Один doctor для CLI и MCP

Сохраните только реально наблюдавшиеся состояния; неизвестное можно опустить:

```json
{
  "standalone": "works",
  "embedded": "blank",
  "document": "stalled_before_connection",
  "debugger": "attached"
}
```

```text
node scripts/soty-app-release.mjs doctor --observations observations.json --output NEW_FILE
```

В Root MCP: `apps_diagnose({observations:{...}})`. Оба используют
`modules/apps/deployment-guidance.mjs` и одни строгие schemas. Модуль входит в
production image и не зависит от test-only `src/deploy/scripts`. Schema содержит
полный перечень состояний: standalone, embedded, document, debugger, framing,
websocket, boot, readiness, access, sourcePin, identity, persistence.
Произвольные URL/logs/credentials и дополнительные поля отвергаются без echo.

Doctor не делает сетевые запросы и не управляет браузером. Ответ отмечен
`basis=reported_observations`, `liveVerified=false`, `releaseReadiness=not_assessed`.
Он объясняет наблюдения и предлагает проверки; не меняет конфигурацию/права и
не превращает неизвестное в успешный релиз. Нельзя выдавать его ответ за live QA.

После двух одинаковых ошибок прекратите слепой retry. После неизвестного
результата mutation сохраняйте тот же intent/request ID и восстанавливайте
результат штатной квитанцией. HTTP transport retry не должен повторять новое
действие над данными.
