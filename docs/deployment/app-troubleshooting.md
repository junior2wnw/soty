# Приложение не видно: проверяем причину

Сначала разделите **внешний документ, boot, приложение, вход и его данные**.
Успешный HTTP boot или событие iframe `load` не доказывают работу приложения.
Нельзя исправлять пустое окно расширением аудитории или отключением защиты.
Boot документ проверяется GET; неподдерживаемый HEAD/ожидание HEAD body не
устанавливает проблему GET. Parent frame-src и child frame-ancestors/XFO
проверяются отдельно; public GET 200 не подтверждает browser admission/cookie.
Cross-origin AX about:blank не является actual document URL. Независимая
видимость у владельца в другом браузере — отдельный факт, не доказательство
причины управляемого браузера или общей исправности/поломки Root auth.

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
| Health200, но служебный POST возвращает405 или HTML | Проверить фактический entrypoint и порядок маршрутов с тем же методом и авторизацией; catch-all раздачи файлов может перехватить API |
| Новый файл есть в image, а маршрут всё ещё старый | Проверить image WORKDIR/CMD и реальные пути загруженных модулей; COPY в неиспользуемый каталог не обновляет работающий код |
| Управляющий CLI сообщает offline, а коннектор подключён | Сверить настроенный IPC/health адрес конкретного экземпляра; старый wrapper может проверять иной порт |
| Потеря записи после reload | Активная БД/volume, stable issuer/sub association, migrations до optional workers, единственный writer |

Debugger interference — гипотеза до сравнения. В расследовании 20.10.2026
Chrome оставлял cross-origin iframe stalled до соединения при подключённом
инструменте; штатная отмена task debugger и новый boot восстановили его.
Повторное подключение того же инструмента ради скриншота может воспроизвести
сбой. Это не общий диагноз для каждого пустого iframe. Используйте доступный
разрешённый канал и соблюдайте browser policy; разрешение на другой способ
управления нельзя выводить из этого документа. Не выключайте Chrome security,
не выгружайте cookies/tokens и не очищайте browser storage.

Если автоматическая проверка остановила управление из-за неопределённого
Windows browser URL, прекратите управление браузером и не обходите остановку
через PowerShell/CDP или другой инструмент. Зафиксируйте причину. Независимая
работа с кодом и разрешённые серверные проверки могут продолжаться; их результаты
не являются наблюдением Network/Console человека. Для следующего прохода нужна
достоверно выбранная вкладка нужного сайта. Само повторное разрешение пользователя
не делает неопределённый адрес проверенным.

Проверяйте интеграционный запрос на реальном собранном приложении до миграции
или переключения источника. Успешный тест отдельно подключённого handler и200
на health не проверяют порядок маршрутов основного entrypoint. Регистрируйте
API до StaticFiles/catch-all и до запуска сервера; затем проверьте разрешённый
вызов, отказ неверной авторизации и обычную страницу. Служебный маршрут при этом
сохраняет авторизацию и запрет внешнего доступа.

Проверка выполняется штатными ENTRYPOINT/WORKDIR и порядком import search path,
без подмены PYTHONPATH ради теста. В одном реальном образе новый пакет лежал в
`/app/web_controller`, а `uvicorn web_controller.standalone:app` из рабочего
каталога `/web_controller` загружал прежний `/web_controller/web_controller`.
Исходный порядок маршрутов был правильным; исправления требовала упаковка.
Закрепляйте безопасные пути и хеши реально импортированных критичных модулей,
а также доступность маршрута в этом запуске. Наличие файла и revision label
сами по себе этого не доказывают.

Для коннектора сохраняйте фактические scope, managed flag, identity, config
directory и IPC port. Например, проверенный CurrentUser-экземпляр слушал49428,
а унаследованный `ctl health` обращался к49424. Проверка явно настроенного
loopback `/health` отделяет ошибку wrapper от остановленного сервиса. Не делайте
из этого нового глобального default и не сканируйте соседние порты; не меняйте
scope/managed flag, чтобы получить успешный ответ. Перед обновлением registry
проверьте сохранение прежних entries/ключей и отсутствие активной работы.

Root «Диагностика запуска» даёт ограниченный phase trace: только закрытые phases,
округлённое время и Root frame visibility/hit flags; до 24 событий/120 секунд.
Это частичные наблюдения, не authority/readiness. `root_load` и
`boot_document_ack` различаются; отсутствие ACK не доказывает сбой. Не добавляйте
к trace child DOM, приватный текст, boot/watch IDs или полные URL.

## Ошибка ограниченного оператора

Проверь настоящий Linux CLI/module graph, а не только factory import. Для EACCES
раздели public code modes и DAC own-copy DATA; scoped observer exception требует
exact role/RO mounts и negative write→EROFS. Большой Docker GET заменяется exact
label/volume filters с all=true, включая stopped peers; bound 4 MiB сохраняется.
Prospective CREATE сверяется полноценно: у QA network:none исключается только
доказанный пустой верхнеуровневый /MacAddress; остальные поля не нормализуются. Собственный holder/PID1 получает
SIGTERM handler. После transport timeout STOP сначала читаются intent, actual
state и terminal helper; повтор STOP не является проверкой первого результата.
Детали платформенной ветки находятся в operations.md установленного skill;
обычному app recipe эти ручные операции не добавляются.

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
