# P3-C1 — независимый разбор UI и жизненного цикла

2026-09-30. Read-only review `src/world/app-settings.ts`, `.css`, `app-settings-state.mjs` и интеграции в `app.ts`. Финальный source freeze автора перечитан. В этой ограниченной области блокирующих замечаний не осталось. Reviewer не менял implementation, не занимал браузер root и не объявляет геометрию/скриншоты проверенными.

## Подтверждённые замечания

### UI-1 — выбранный закрытый адрес запирает черновик публикации

Статус: исправлено автором; повторная проверка состояния PASS, итоговый source freeze перечитан.

Владелец отмечает ещё не активный alias в несохранённом draft, затем отдельной командой закрывает этот alias. У закрытия неактивного адреса нет изменения policy epoch. После inspect `createAppSettingsDraftState.observe` сохраняет selected ID, но `publicationConflict` остаётся false. View не показывает checkbox у tombstone. `appPublicationArgs` закономерно отказывает `app_publication_domain_unavailable`; обычный refresh не очищает dirty draft, а reset-current отображается только при конфликте. У владельца нет видимого способа снять недействительный выбор, сохранив остальные правки.

Воспроизведено прямым запуском текущего state module в Node: `draftIds=[tombstone], visibleSelectable=[], publicationDirty=true, publicationConflict=false, sendError=app_publication_domain_unavailable`. Это проверка состояния, не браузерный опыт. Нужен явный путь восстановления выбора: unavailable selection с удалением либо отдельный конфликт/reset, без молчаливого изменения уже согласованного намерения.

Итоговая модель возвращает `unavailableDomainIds`, включает их в конфликт публикации, а UI показывает конкретные адреса и действие «Снять недоступные адреса из выбора». Независимый прямой запуск подтвердил сохранение оставшегося alias, названия, выбранных групп и исходного CAS после явного снятия недействительного выбора. Отдельный случай исчезнувшего из snapshot адреса также не удаляется молча.

### UI-2 — подтверждение без изменения политики создаёт лишний epoch

Статус: исправлено автором; повторная проверка состояния PASS, итоговый source freeze перечитан.

Уже опубликованное приложение: `launchPolicy=anyone`, активные адреса неизменны. Единственная постановка `exposureConfirmed=true` делает `publicationDirty=true`, поэтому открывает кнопку Save. Отправляется семантически прежняя политика. Сервер по принятому контракту bump epoch даже при accepted no-op, что прекращает текущие sessions без изменения доступа.

Прямой запуск state module подтвердил `publicationDirty=true, policyChanges=false, expectedEpoch=7`. Исправление должно оставаться в UI: согласие — условие отправки настоящего изменения anyone, а не самостоятельное изменение политики. Server CAS/no-op контракт не требует ослабления.

Итоговый `publicationDirty` сравнивает только политику и набор активных адресов; acknowledgment больше не делает неизменённую публикацию dirty. Три focused author regression tests (ack-only, retired selection, missing selection) независимо запущены reviewer: **3/3 PASS**, 0 skips, exit0. Это проверки текущего модуля состояния, не реальных DOM-событий.

### UI-3 — Back/popstate обходит подтверждение потери черновика

Статус: исправлено автором; финальный source path перечитан. Настоящий browser Back остаётся проверкой root.

Сценарий по исходникам: переход с главной к приложению → открыть настройки → изменить название или набор адресов → browser Back. Обработчик `popstate` вызывает `openRoute`; `screenHasUnsavedChanges` учитывает Notes/Assistant/Access, но не окно настроек. Далее обычная навигация вызывает `cleanScreen`, который напрямую закрывает native dialog. `requestClose` и локальное подтверждение потери правок обходятся. Обычный неотправленный ввод живёт только в модели, а `beforeunload` не срабатывает при переходе внутри того же документа.

Нужна защита перехода маршрута с явным решением владельца; принудительное закрытие при смене аккаунта или уничтожении приложения должно остаться отдельным безусловным очищением старого контекста. Same-account refresh уже сохраняет открытые настройки и текущий iframe отдельной веткой. Фактический browser Back reviewer не выполнял; доказательство здесь — достижимый source path.

Исправление перехватывает history-переход в начале `openRoute`, до общей навигации: текущий маршрут возвращается, а продолжение запрошенного перехода передаётся в `requestClose`. Отмена оставляет форму; подтверждение сначала закрывает native dialog и затем возобновляет Back. Продолжение повторно проверяет account/screen/destroy. Принудительный identity-change/destroy не проходит через подтверждение и очищает старый UI. Root передан точный browser сценарий: Back → продолжить редактирование → Back → выйти без сохранения.

## Проверенные границы исходников

- Pending intent сохраняется в account+app scoped storage до dispatch; потерянный ответ повторяет прежние args/requestId.
- Cross-tab переходы serialize через Web Locks, не удерживая lock на сетевом запросе. Exact late ACK может закрыть только совпавшую scoped запись и не удаляет чужое новое намерение.
- `expectedAccountId` входит в каждую мутацию и inspect. View guards также покрывают поздние rejection, invalid receipt, ACK-storage errors и Clipboard failures, а не только успешные ответы. Integration `onChanged`/`onPreview` проверяет account, screen sequence и `dialog.open`; закрытое окно не обновляет новый экран.
- Отдельный отзыв приложения остаётся возможен при неопределённой публикации; это важно для полного закрытия доступа. Сохранённое pending намерение не считается отменённым сервером.
- Display использует DOM text nodes. У inactive/tombstone нет копируемой рабочей ссылки; preview выбирает canonical либо active bound alias и использует B3.
- Refresh сохраняет dirty CAS bases; новый серверный snapshot не подменяет автоматически исходное намерение.
- CSS содержит перенос адресов и минимум 44px у основных действий, но исходники не доказывают фактическое отсутствие overflow или удобство на 320px.
- Native Escape и кнопка закрытия проходят локальное подтверждение; preview также не выбрасывает обычный draft молча. `beforeunload` защищает полный уход со страницы. Durable pending сохраняется и после явного выхода, но не описывается как отменённое действие.
- Metadata update меняет заголовок/карточку без пересоздания работающего iframe и чата. Same-account refresh оставляет настройки; ошибка сети сохраняет окно только после подтверждения того же локального аккаунта.

## Зафиксированные исходники первого UI среза

SHA-256 первого проверенного freeze, до дополнительной проверки аккаунта ниже:

- `src/world/app-settings.ts`: `a5aa50691ae1b4fb81cccad57471b5fb78b563489d1e4874c0bd60ffdc78a512`
- `src/world/app-settings.css`: `d47211a7948487328ce80319571e0271082323b9c4d7af92606aeb4c0d695c61`
- `src/world/app-settings-state.mjs`: `9ebb0e2db8191c3f84e83e8a14c4f489fd95c415ceda34c5189805ce843299d7`
- `src/world/app.ts`: `318eecef8bbaf9aad4fb472438945727a9c461e25810940388279e1d052a4318`

## Границы проверки

View переведён на постоянные controls/sections; input/details не пересоздаются при inspect, storage event или истечении observation. Замечания root о historical claim receipt против current state, потере details/focus при render и clock skew не дублируются здесь. Финальные lifecycle handlers и исправление UI-3 перечитаны после freeze. В этот отчёт не входят новые browser checks, screen-reader проверки, physical phone, Clipboard permissions, фактическая геометрия и реальное поведение истории браузера. Независимые deferred/receipt/real-service сценарии другого reviewer и actual-browser работа root имеют отдельные receipts.

## Дополнительный срез C1: аккаунт и необязательные данные приложения

Назначен root после первого UI freeze. Проверен `app.ts` с SHA-256 `f4b31771fe6219888f1bc3ce6581c5fa6c0b052e29ce1cd3e3bf141a7668b499`, фактический `world-adapter.ts`, общий Connect browser client и его подписанный service. Первоначальная ограниченная приёмка выше не распространяется автоматически на этот дополнительный срез.

Авторские controller tests независимо повторены: **9/9 PASS**, 0 skips. Они исполняют настоящий TS controller через VM с инертными UI ports. Подтверждены единое очищение online/offline/invalid account, сохранение Notes/Settings только при доказанном прежнем аккаунте, закрытие старого окна и подавление поздних resource successes/errors. Это не реальный DOM/IndexedDB/browser опыт.

### UI-4 — ответы разных аккаунтов могут пройти проверку A → B → A

Статус: исправлено автором; первоначальный независимый probe повторён с account context, PASS.

В `refresh` profile и community list идут двумя RPC; после них проверяется только соответствие profile ID текущему локальному аккаунту. Общая очередь Connect сериализует запросы в одной вкладке, но не блокирует переключение аккаунта другой вкладкой. Её уведомления также попадают в очередь и могут прочитать уже окончательное состояние A.

Независимый inline probe использовал настоящие `createClientWithStorage`, подписи, `createConnectService`, две валидные synthetic installations, shared memory storage со snapshot чтением и асинхронную очередь уведомлений. Между завершением первого snapshot-current-read и вторым RPC другая сторона переключается на B, затем после второго snapshot-current-read обратно на A. Результат: `beforeIsA=true, profileIsA=true, communitiesIsB=true, afterIsA=true, pageObservedB=false`. Никакой `actor` или RPC response не подменён. Memory snapshot/notification scheduler здесь моделирует границу между вкладками; это не тест настоящего IndexedDB.

Необходима явная привязка admission обоих RPC к исходному account либо проверяемая identity у каждого ответа. Повторный account-ID read до и после пары недостаточен. Предложен backward-compatible client-only `expectedAccountId` option, проверяемый внутри сериализованной admission до challenge/sign; wire args World остаются прежними.

Исправление реализует третий параметр `extension(op,args,{expectedAccountId})`: значение копируется до queue, проверяется с фактической installation внутри queue, затем сохраняются прежние current-identity проверки до подписи и после ответа. World adapter замыкает account ID каждого mount, включая отложенный contact action; старое окно не читает mutable ID нового окна. Повтор исходного независимого probe подтвердил `ACTIVE_PROFILE_CHANGED` для второго запроса; traffic содержит только challenge/profile A, community B и challenge для B отсутствуют. Двухаргументный API остаётся совместимым; старые клиенты без context не получают новую межзапросную гарантию автоматически.

### UI-5 — необязательный каталог оказывается перед запуском в общей очереди

Статус: исправлено автором; первоначальный actual-controller/Connect probe повторён, PASS.

`openApplication` начинает `loadApps` и optional community metadata раньше `launcher.launch`. Отсутствие `await` на metadata не даёт независимости: реальный Connect client сериализует весь RPC и ставит launch за медленным каталогом.

Probe с настоящим transpiled `app.ts`, `createAppLauncher`, Connect browser client и подписанным service удерживал успешный ответ `apps.list`. Пока он удерживается: `actualOps=[apps.list], hasRuntimeFrame=false`; после release: `actualOps=[apps.list,apps.launch], hasRuntimeFrame=true, title=Actual name`. UI заменён простыми ports; копии поведения методов нет. Передача JSON между VM realm и реальным клиентом нормализована только в fixture.

Достаточна узкая перестановка: начать существующий `launcher.launch()` до optional catalog/chat RPC, сохранить Promise и дождаться его внутри прежнего try/catch. Новая параллельная очередь и изменение транспорта не требуются. После фикса нужно повторить hold/release и убедиться, что поздний metadata меняет название/settings без пересоздания iframe.

Финальная реализация запрашивает launch первым. При повторном hold `apps.list` независимый probe получил `actualOps=[apps.launch,apps.list]`, frame уже вставлен с временным заголовком. После release сохраняется тот же объект frame, заголовок обновляется и появляется ровно одна кнопка настроек владельца. Это доказательство порядка API и мутаций UI ports, не фактического запуска страницы внутри браузерного iframe. Уже выполняющийся посторонний запрос в общей очереди этим не прерывается.

### Итог дополнительного среза

Новых блокирующих замечаний в согласованной области не осталось. Reviewer повторил авторские focused suites: Connect identity **6/6**, actual controller **10/10**, actual adapter mount closure **1/1** — всего **17/17 PASS**, 0 skips; сверх этого повторил два исходных независимых inline probes UI-4/UI-5. Исторический двухаргументный ABA control закономерно воспроизводит старую границу; новый World adapter всегда передаёт закреплённый context.

SHA-256 повторно проверенного final freeze:

- `modules/connect/browser/client.mjs`: `560bcd768a32c27a8f1e85fc36c2321fbd00aa28a5a84f8c1c8ad297022c6b54`
- `src/platform/world-adapter.ts`: `0880d156ae76fd9ce2700ff5591a55a42aab13494656a4def156da5efd7cdd05`
- `src/world/app.ts`: `67035f82506413fac8aa2469853840c9fe103e1b657983ecb80f247f2a65fc20`

Сохраняются границы: synthetic in-memory storage и notification scheduler; реальный подписанный Connect service/client; TS controller/adapter исполняются через VM с UI ports. Reviewer не занимал браузер, не проверял фактический IndexedDB scheduler, Clipboard, телефон, DNS/TLS или production. Browser alias hard reload, реальная геометрия и окончательный C1 release gate остаются отдельной работой root. Production implementation reviewer не изменял.
