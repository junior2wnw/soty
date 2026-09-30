# P4-C2b: ручной доступ с помощниками

Авторский UI срез в существующей панели «Доступы и действия». Source frozen после **8/8 новых и 13/13 существующих affected UI tests PASS**, без пропусков. Browser geometry ещё не заявлена. HTTP derive, domain authority и действительное создание дочернего доступа принадлежат другим владельцам.

## Область и неизменный API

- `src/world/access-panel.ts` — явное согласие, подтверждённая проекция grant, связь и отзыв, различимый audit, однократная выдача ключа.
- `src/world/access-panel.css` — native checkbox с кликабельной подписью, disclosure/revoke targets не ниже 44px. Существующие palette tokens и геометрия панели сохраняются.
- `src/world/access-panel-delegation.test.mjs` и собственный `test-support/access-panel-delegation.mjs` — настоящий transpiled controller с синтетическими DOM/dialog/API.
- `src/world/access-panel-delegation.test.html` — настоящий DOM/dialog/CSS и вымышленные API-ответы для отдельной проверки 320px и 667px.

Публичные options/handle компонента не меняются. Никаких новых API-запросов, DDL, ledger, local persistence или подключения помощника от лица владельца. OAuth consent и connection cards не меняются.

Manual `access.grants.issue` сохраняет Notes scope: `notes.createDraft@1`, `notes:new`, `create`, `soty:notes`, единственный root budget `invocations`. Native checkbox по умолчанию выключен: `allowDelegation:false,maxDepth:0`. Явное включение отправляет `true,1`. Значение фиксируется синхронно при submit до первого await; позднее изменение поля не заменяет согласие. Перед `access.credentials.issue` проверяется соответствие полученного grant этому выбору. Child derive этот UI не вызывает.

## Понятные состояния

Разрешение помощника показывает свой срок, server-projected общий root budget и связь с исходным разрешением. В disclosure доступны точные `grant.id`, `clientId`, `parentGrantId`, `rootGrantId`; в карточке клиента — `principal.id`. Данные не сопоставляются по label. Поле `chainState` в DTO отсутствует: `revokedAt:null` не становится «доступ действует». «Выдан» означает запись выдачи; для child явно указана зависимость от ключа и всей цепочки. Собственный revoke, отключённый principal и истёкший собственный срок имеют разные подписи.

Ручной grant/principal revoke закрывает переданные на его основе разрешения, но не отменяет уже выполненные действия. В форме, подробностях и подтверждении есть точная граница: «Отзыв одного ключа не отключает уже подключённых помощников». OAuth family-wide revoke этим текстом не переопределяется.

В audit `actorType:'service'` отображается как «Передал клиент» с точным `actorId` (parent principal), `connect` — «Изменено с устройства» с device ID. Неизвестный тип получает нейтральный «Источник изменения», а не вымышленную owner/device роль.

## Секрет и неопределённый результат

Ключ показывается однократно; закрытие/pagehide/dispose очищают поле и ссылку на секрет. При потере ответа выдача могла состояться. Секрет не восстанавливается и не создаётся повторно автоматически. Одна форма допускает одну попытку выдачи: повторный submit после unknown результата не запускает ещё одну последовательность principal/grant/credential.

Показаны известные точные principal/grant ID и действия проверки списка/истории либо явного отзыва созданного principal. Cleanup ACK обязан совпасть с captured principal и account; ответ об ином субъекте не подтверждает отзыв. Новое действие возможно только через новую форму, после проверки предыдущего доступа. Это UI discipline, не новая идемпотентность backend выдачи. Один и тот же ключ Notes invocation имеет прежнюю отдельную domain retry семантику.

## Подготовленные проверки

В новом файле восемь сценариев: default-off/narrow scope/одноразовый secret; captured checked при delayed availability; несовпавший grant ACK; lost key response без повторного создания и exact cleanup; account A→B→A с поздним ответом; child IDs/shared budget/невыдуманная chain authority; service/device audit и существующая tab navigation; pagehide и новый default-off диалог.

Синтетический DOM проверяет controller state, поля и вызовы, но не доказывает native Tab/Space/modal focus/layout или signed Connect. HTML fixture не обращается к реальным API/БД и не выдаёт рабочие credentials. Сценарии: parent/child, lost key ACK, late key 2s; есть account switch и обе темы. Browser gate следует проводить отдельно на 320×760 и 667×375: Tab/Space на checkbox, читаемость exact IDs, раскрытие и отзыв с клавиатуры, скролл длинной формы без horizontal overflow.

## Выполненные проверки и freeze

Оба запуска последовательные на isolated Node24.21.0 с command-local PATH. Новый набор прошёл с первого запуска: **8/8 PASS, 0 FAIL, 0 SKIP, 774.3241ms**. Один affected regression существующих OAuth и identity случаев: **13/13 PASS, 0 FAIL, 0 SKIP, 926.3325ms**. Эти тесты не изменялись. Исправлений между прогонами не было. `git diff --check` изменённых tracked TS/CSS — PASS. Автор не запускал typecheck/build/широкие suites; эти проверки и actual PWA walkthrough выполняет root отдельно.

```powershell
$env:PATH = (Resolve-Path 'var/toolchains/node-v24.21.0-win-x64').Path + ';' + $env:PATH
& .\var\toolchains\node-v24.21.0-win-x64\node.exe --test --test-concurrency=1 --test-reporter=tap --test-timeout=30000 src/world/access-panel-delegation.test.mjs
& .\var\toolchains\node-v24.21.0-win-x64\node.exe --test --test-concurrency=1 --test-reporter=tap --test-timeout=30000 src/world/access-panel-oauth.test.mjs src/world/access-panel-identity.acceptance.test.mjs
```

SHA256 фактических worktree bytes:

| Файл | SHA256 |
|---|---|
| `src/world/access-panel.ts` | `db5cde140ac238e652cec8afed5d8ef07c911feabd50ceac1fb70771475aef0a` |
| `src/world/access-panel.css` | `f225f0b50772fab43095ec679359898157ed3c253a2598ef107ecbba813b3b07` |
| `src/world/access-panel-delegation.test.mjs` | `9f9fa5dc33e7eaaa662d913d7fcefa7f3746da3b6b09e2c29e71b1f2bd04f1fc` |
| `src/world/test-support/access-panel-delegation.mjs` | `865229d9c314b77b58d37407154df3651f4a323294f9beec48f24330b19977ab` |
| `src/world/access-panel-delegation.test.html` | `bffd660134e3bd4fb0eb76f0a177f0800fca307b02b9538c3bd4ad5d06f54014` |
| `output/implementation-20260930/p4-service-delegation-ui-first.log` | `4c6c91806cf25efb3ef4d6d2a524df55285e5c941f2708742f2b62fc3a4e9c15` |
| `output/implementation-20260930/p4-service-delegation-ui-affected.log` | `f012c471a9adcebd672ee84e695d060d3821cbf544bdd674a8b0be4a5d9833b1` |

Новые browser/CLI processes, действительные credentials, remote deployment и реальные записи БД этим автором не создавались. Synthetic secret fixture — заведомо невыданный тестовый marker, password input по умолчанию; его показ/копирование не является production acceptance.
