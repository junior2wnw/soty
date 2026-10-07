# Обычный Source: явный повторный Basic-вход

`allowLinkedLogin` — приватная настройка конструктора Source, по умолчанию `false`. Она разрешает **новый вход**, когда Source SQL всё ещё содержит тот же immutable issuer/sub link, selected resource/incarnation, semantic grant/device и действующие Native session/membership. Она не продлевает Native session, не создаёт роль и не даёт кандидату читать данные, писать или управлять обращениями.

Человек открывает приложение, выбирает «Войти через Соты» и проходит новый реальный OIDC-вход. Source окончательно проверяет свои текущие права внутри SQLite-транзакции. Получается новая Basic-сессия до300s, привязанная к новому исходному Root slot. Principal, Native session, membership и selected consent сохраняются. При изменении или отзыве Native session/membership захваченный кандидат отказывает на final fence; новый вход может использовать только актуальную уменьшенную роль. Просроченная/отозванная Native session требует собственного Native-входа.

Initial legacy link продолжает требовать независимый Native session cookie и новый OIDC proof. New-empty policy создаёт Native owner только в новом пустом Source-owned resource при явно включённой Source-политике. Root App owner, app grant, descriptor, email или имя не становятся существующим Native owner. Переход старого Basic grant в Long здесь отсутствует.

Проверено локально на synthetic state, Node24.19:

- Четыре focused Native проверки: default-off, login-only proof до/после commitIdentity, exact identity/device/semantic/incarnation, current/final revoke/expiry/revision и два настоящих OS writers без дублирования Native principal/session/grant.
- Два installed HTTP/WS + actual signed Root/Human + maintained OIDC сценария: две независимые new-empty realms после controlled Source expiry/restart; COMMIT/lost callback ACK → consumed Root map → readonly exact completion после Source restart. Повторный обмен code/новая link/grant отсутствуют; foreign subject и Native revoke запрещены.
- Отдельный real-wall opt-in завершился после **311266ms ожидания и нового входа**, обе realms сохранили Native counts и данные, создали новую Source Basic session, `renewable:false`. Истёкший original Root slot/session отказывает; silent Basic resume отсутствует. Первый запуск этого gate остановился на слишком узком expected403 при фактическом authdenial401; он не засчитан.
- Полный локальный Source suite:88 total,87 PASS,0 FAIL/0 cancel,1 explicit wall opt-in skip; этот wall gate отдельно выполнен1/1 PASS. Strict declarations и portable build PASS. Existing installed5 отдельно5/5 PASS.

Installed tests использовали временно собранный собственный connector1.4.4 SHA `bf7773dcdde639ff86d2d18dfcad5a9bc408343f561cc06eb7235afb916552f8`, не release Main. Его generated public files не входят в этот пакет. На composed Main нужен его собственный accepted connector и независимая проверка.

Basic ACK сообщает usable access bound=min(actual AT expiry,actual Source session expiry,current Root slot expiry), не меняя ни один срок. Narrow fix и его actual installed positive/new-slot/expiry/revoke tests отдельно `fbc632453131ac562388a5d3e0fe0185c02a7fa1`.

Не проверены этим результатом: production deployment, remote browser-reachable Native consent, finite24h consumer/Root rebind, physical encrypted cold restore/Reader3 image gate, модели/пользовательские материалы и actual Linux job executor. Эти состояния не становятся Ready от наличия manifest.
