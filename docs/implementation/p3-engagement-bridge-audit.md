# P3-D3 — независимый аудит связки входа и оболочки

Дата: 2026-09-30. Reviewer: publishing architecture. Область — root-реализация `apps.entry.get`, `apps.launch.entry`, `app-launch`, `app-stage`, подключение в `app.ts` и действующий account-bound adapter. Production и тесты авторов не изменялись; создан только этот отчёт.

**Результат: новых блокирующих дефектов в проверенном срезе не найдено. 45/45 focused tests PASS, 0 FAIL, 0 SKIP.** Это ограниченный gate связки, не завершение D3/browser-приёмки.

## Прочитанные границы

| Граница | Проверенное поведение |
| --- | --- |
| Один допуск — один точный вход | `index.mjs:354` получает branded decision, проверяет текущую runtime binding, один раз вычисляет entryPath/bootPath и сохраняет тот же entryPath вместе с ticket. Возвращённый DTO содержит только appId/domainId/origin/path. Успешный ответ клиента без такого DTO отвергается; поздний lookup не используется для его восстановления. |
| Offline entry | `index.mjs:61` выполняет чтение через общий Connect → World → Apps fence, с 100 ms Apps busy policy. Resolver проверяет canonical либо явно выбранный domain и общий publication policy. Отсутствие источника допустимо; недоступный/retired/чужой domain даёт generic refusal. Не создаются saved/discussion heads, World profile, ticket или session. |
| Default-path race | Первый launcher admission сериализуется с одновременным external open. После первого разрешённого entry все повторные запросы содержат его явные domain/path; каждому открытию нужен новый билет. Query, percent encoding и SPA fragment не пересобираются из metadata. Source default, изменившийся между запросами, не подменяет уже выбранный путь. |
| Администратор ≠ запуск | Начальный `admin=1` запускает только discussion panel. Discussion feed получает `entry:null` и administrative mode; серверная ветка отдельно проверяет owner. Обычная навигация/явное runtime-действие снова проходит launcher. Административный флаг не отправляется как параметр `apps.launch`/`apps.entry.get`. |
| Account ABA и поздние ответы | `openApplication` захватывает accountId, accountGeneration и screenSequence; `transitionAccount` очищает stage и caches. Launcher дополнительно проверяет current/dispose после await и закрывает принадлежащую ему пустую вкладку. Production adapter связывает каждый RPC с исходным expectedAccountId до подписи; root-options callback не является источником нового аккаунта. |
| Metadata не authority | Stage starts admission до optional catalogue/community reads. Метаданные меняют подпись/доступность owner controls, но не selectedEntry. Community context поступает только из отдельного подтверждённого membership read и не выдаётся из разрешения запустить приложение. |
| Exact alias и route lifecycle | `sameAppLaunchLocation` не превращает именованный вход в canonical. Панель/архив/Back в том же входе сохраняют runtime node; иной явный path/domain/account создаёт другой screen. `captureEntry` передаёт сохранению и обсуждению проверенный вход; retained lookup по route восстанавливает лишь собственный локальный текст, не историю или право запуска. |

`app-stage`/`app.ts` сохраняют более ранний runtime при временном refresh failure; это не продление серверного допуска. HTTP/session/stream recheck остаётся ответственностью ранее принятого B2/C2 backend. DTO entry — место входа, не повторно используемое разрешение и не гарантия того, что пользователь не перешёл на другую страницу внутри iframe.

## Выполненный запуск

```text
node --test --test-concurrency=1 src/world/app-launch.test.mjs src/world/app-entry-context.test.mjs src/world/app-account-lifecycle.test.mjs server/test/app-entry-http.test.mjs
```

45 PASS, без пропусков; 3.79 s на этом Windows/Node запуске. В составе:

- 5 настоящих signed Connect HTTP сценариев с Apps/World SQLite, v2 connector и loopback source: exact Unicode/query/hash tuple; offline без provisioning; source default change; retired/foreign address и unsafe paths; Apps/World busy → 503 без engagement rows.
- 7 entry/route сценариев: exact DTO validation, сериализация initial/external, offline lookup, successful response без entry, отказ подмены при retry, поздний resolver после account ABA.
- 14 actual controller/stage lifecycle сценариев в TypeScript-transpiled VM с DOM adapters, включая сохранение одного iframe, отсутствие автоматического runtime в admin view, A→B→A и same-account offline preservation. Один сценарий использует настоящий сериализованный Connect client и удерживает optional metadata response: launch остаётся первым.
- 19 действующих launcher/path/popup сценариев, включая отдельный ticket на действие, раннее создание пустой вкладки, stale result и dispose.

## Проверенная версия

| Файл | SHA-256 |
| --- | --- |
| modules/apps/server/index.mjs | `3174f667b7a651fe80f892bd73ad27c2214b65ec0f99df2f917ea1bac5ba974a` |
| modules/apps/server/engagement-access.mjs | `2b7a34bbfc63147729b1f12e1e7c3a21bb739ed330837d1a0e93ae516174acf1` |
| src/world/app-launch.mjs | `99680a0c881b25898ad9a464df0be50f0f3d1bfa3e266d8654b96b69d02fbf0e` |
| src/world/app-stage.ts | `4b256454ba67805f91e0077823a3082ed33ca9198f168aadfc3a64d0e4896589` |
| src/world/app.ts | `3316b43262e0712d3dcf178434d7aee6b32e2185bde7e579924abb5c8574b0b6` |
| src/platform/world-adapter.ts | `0880d156ae76fd9ce2700ff5591a55a42aab13494656a4def156da5efd7cdd05` |

## Ограничения

VM lifecycle не доказывает browser popup permissions, cookie policy, фактические iframe HTTP/WS соединения, responsive geometry, Back/scroll/focus или компонентные draft transitions. Их проверяет root в реальном браузере. Read-only аудит source не заменяет независимую приёмку новых Saved/Discussion helpers и UI. Полный world/deploy suite здесь намеренно не повторялся; формат хранилища не менялся. Production rollout, DNS/TLS, физический телефон и disk crash durability не проверялись и не заявляются.
