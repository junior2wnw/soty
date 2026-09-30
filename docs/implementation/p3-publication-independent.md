# P3-B1 — независимая приёмка модели публикаций

2026-09-30. Проверяет `publishing_architecture`; реализацию модели выполняет `agent_ecosystem`, интеграцию принимает root. Проверка относится к модели, миграции и внутреннему решению доступа. Она не означает завершение публичного HTTP/WS runtime, интерфейса публикации или production rollout.

Статус: **независимая локальная приёмка B1 — PASS**. После исправления воспроизведённого дефекта при получении community grant финальный набор прошёл **36/36, без failures и skips**. Новых блокеров в указанной модельной границе не найдено. Ниже сохранены проверяемые границы, первоначальное воспроизведение и проверка окончательной версии.

## Независимость образцов и изменений

Новый `modules/apps/test/app-publication.acceptance.test.mjs` не импортирует авторский test fixture. Для Apps2 используется замороженный DDL `deploy/connector/apps-v2.fixture.mjs` из принятого checkpoint `b4b5200`; historical Apps1 DDL взят из `635022f`. В обоих случаях создаётся старая схема, а не новая схема с заменённым marker. Данные включают нескольких владельцев, два приложения одного владельца, активное и отозванное приложение, реальный grant, canonical origins, bound aliases, tombstone и исторический domain receipt.

Единственная правка прежнего `domain-acceptance.test.mjs`: его fixture создаёт текущую схему и напрямую добавляет apps; теперь внутри той же seed transaction он вызывает обязательный `ensureInitialPublication`. Название schema-теста уточнено с «v2» до «known schema». Это исправление тестового образца, не автоматическое восстановление частичной v3 в product. Отдельный независимый сценарий проверяет отсутствие такого скрытого восстановления.

Миграция, публикации, domains, service index, deploy и UI reviewer не изменял. Все проверки выполнялись на собственных временных SQLite-файлах. В concurrency cases работают разные Worker connections; операции синхронизированы барьером готовности. Workers имеют ограниченное время жизни и завершаются до удаления своего временного каталога.

## Проверенные инварианты

| Область | Проверка фактического поведения |
| --- | --- |
| Старые данные | Apps1 user_version0/1 и Apps2 →3 с reopen: app/device/правильные grant rows сохраняются; Apps2 canonical origins, aliases, tombstone и domain receipts не изменяются. Начальные публикации restricted, unlisted, epoch1, target1; ни один alias не включается. |
| Старый grant index | Посторонняя stale optimization row v1 не превращается в право; `grants_json` остаётся прежним источником прав. После миграции настоящий приглашённый входит, посторонний не видит и не открывает приложение. |
| Ошибка миграции | Запрещённый исторический source path приводит к отказу целой миграции: старый marker/user_version, строки и DDL остаются прежними, частичные v3 objects не сохраняются. |
| Регистрация | Новый app получает private publication и неизменяемый initial target. Повтор точной регистрации уже опубликованного app возвращает тот же ID без сброса policy, target, адресов или epoch. |
| Адрес не равен публикации | Новый claim после включения anyone не включается автоматически и не меняет publication epoch; status-only/runtimeReady=false остаётся честным. |
| Набор адресов | Активация чужого app того же владельца, чужого владельца, canonical origin, tombstone, неизвестного ID и дубликатов отказывает без частичной записи. Listed требует anyone и хотя бы один активный адрес. |
| Согласие | Public update требует whole-port acknowledgement с точными target revision/digest/profile. Чужой digest, неверная revision, page scope и другой профиль отказывают. |
| Target | Digest независимо вычислен из зафиксированных полей; UPDATE/DELETE запрещены; чужая target revision не проходит составной FK. Повреждённый digest отказывает при read/admission. Старый apps.update не принимает port/entryPath. |
| Повтор и текущая личность | Lost-response replay переживает restart. Текущие actor/owner/expectedAccount проверяются до возврата истории. Receipt сохраняет прежний результат, `current` честно показывает последующую restriction/revoke/retire. Смена intent под тем же действующим ключом отказывает. |
| Пространство ключей | Publication и domain receipts независимы. Внутри publication request key принадлежит аккаунту, а не каждому app отдельно; другой аккаунт может использовать свой такой же ключ. |
| Retention | При 70 обновлениях и убывающих timestamps остаются ровно64 последних committed epochs данного app. Другой app не затрагивается. Удалённый старый retry не подставляет свежий epoch и отказывает CAS; retained retry возвращает прежний receipt. Emergency restriction не блокируется history limit. |
| Атомарность publication | Ошибки INSERT нового receipt и DELETE во время prune откатывают одновременно policy, active set, epoch, новый receipt и удаление старого. |
| Grants | Ошибка на policy epoch откатывает и изменение legacy grants. После успешного удаления старые решения недействительны, но новый публичный вход по anyone остаётся возможным. Повтор того же набора grants не увеличивает epoch. |
| Retire | Ошибка финального domain receipt откатывает tombstone/head, active set и epoch. Успешный retire снимает listing последнего адреса, replay старой публикации не возвращает адрес и прежнее имя не захватывается повторно. |
| Revoke | Ошибка удаления active domains после изменения policy откатывает также состояние app. После успешного revoke все адреса закрыты; исторический publication receipt не воскрешает app. Повтор revoke не увеличивает epoch. |
| Доверенное решение | Решение заморожено, включая вложенные actor/route. Копия, JSON/structured clone и решение другого экземпляра не проходят branding. Требуется точный origin; публичность alias не открывает canonical гостю. |
| Срок | Default recheck сохраняет expiresAt. В точке expiry и после неё решение нельзя оживить новым TTL; превышение предела TTL отказывает. |
| Внешние права | Membership и право владельца администрировать сообщество проверяются при recheck даже без записи нового локального epoch. Отозванный actor отказывает. |
| Частичная v3 | Отсутствующая publication row возвращает registry_corrupt; не создаётся автоматически ни при legacy update, ни при reopen. Domain registry требует оба coupling hooks. |

## Параллельность: правильные исходы

При двух одновременных publication updates с одним expected epoch принимается ровно один intent. При двух точных same-key повторах принимается одно изменение; оба запроса возвращают один receipt, один ответ помечен replayed.

Для **разных команд** publication update и legacy grants правилом не является «ровно один успешный запрос». Проверены оба допустимых порядка отдельно и реальная гонка двух SQLite connections:

- publication первой: оба запроса могут завершиться успешно, итоговый epoch `E+2`, новые grants действуют;
- grants первыми: grants успешны, старая publication получает CAS409, итоговый epoch `E+1`.

Также проверены гонки publication с active retire и revoke: победившая публикация не мешает последующему закрытию; если закрытие произошло первым, старое намерение публикации не проходит CAS. Итоговые tombstone/revoked state и active set проверяются в базе, а не по одному сообщению успеха.

## Finding при смене основания доступа

Root выявил необходимость разделить `subject` и `accessBasis`: signed visitor публичного адреса может не иметь grant. Такому посетителю нельзя выдавать резерв private capacity только за наличие аккаунта. Независимый тест подтвердил уже добавленную границу: при потере community grant без изменения policy epoch прежний grant decision отказывает; новый signed или anonymous public вход остаётся допустимым по anyone.

Critic выявил обратный переход — получение grant в действующей public session. Дополненный независимый test воспроизвёл его на предыдущей реализации: при неизменном epoch `recheckAccess(originallyPublic)` выдавал `app_access_changed`. Автор исправил поведение: исходный basis сохраняется в течение этого допуска; public остаётся public, пока alias+anyone действуют, grant требует сохранённого grant. Новый decide/launch может выбрать grant. Reviewer перечитал private `loadAccess`/branding/recheck: forced basis берётся только из проверенного branded decision. Caller не может передать `accessBasis` в recheck options и повысить себе режим. Финальный независимый прогон подтвердил и gain, и loss сценарии.

Это влияет на выбор лимита и непрерывность stream в будущем B2. Модельный тест не доказывает, что реальный gateway уже использует соответствующий capacity bucket.

## Локальная воспроизводимость

Среда: Windows x64, Node `v24.13.1`.

```text
node --test modules/apps/test/app-publication.acceptance.test.mjs modules/apps/test/domain-acceptance.test.mjs
36 tests; 36 pass; 0 fail; 0 skip; exit 0
```

Финальный прогон после исправления выполнен на замороженных источниках: 26 новых publication acceptance cases и 10 прежних domain cases, включая вложенные проверки. Duration `12459.9058ms`, exit0. Ранее тот же суммарный набор без дополнения reverse membership-gain прошёл36/36; отдельное последующее воспроизведение до исправления:

```text
node --test --test-name-pattern="signed public traffic" modules/apps/test/app-publication.acceptance.test.mjs
1 test; 0 pass; 1 fail; exit 1; app_access_changed
```

`git diff --check` изменённого прежнего fixture прошёл. Полный Apps suite и интеграционный checkpoint выполняет root; reviewer не приписывает себе авторские или root результаты.

SHA256 локальных bytes окончательного прогона (Git CRLF/LF normalization может изменить файловый hash):

| Файл | SHA256 |
| --- | --- |
| `modules/apps/server/schema.mjs` | `85be313a1deea3a377abd3247227f2bca84207a460ab77f9e719426216c3b1ba` |
| `modules/apps/server/publications.mjs` | `3c3f1eafdb2dea841edf3338fb6f3efbfa5ecebac456f4c9e85fe29d5f232d3e` |
| `modules/apps/server/domains.mjs` | `c6b366b42d603c73bb9632f824e71bcdfe62a9d32c3c1ba647554da136e1a92e` |
| `modules/apps/server/index.mjs` | `b4d2db4f68a6bacd0a5b58c74b53b9c6c599b1adfdfe5f8afff451e076f75d78` |
| `modules/apps/test/app-publication.acceptance.test.mjs` | `9a775a825bdf019b610486b799535874646ccafc246fba854a28752a3f0a15c5` |
| `modules/apps/test/domain-acceptance.test.mjs` | `6547338a7188b3d4aca69f622baf0307c1349489ae1f26911d1b2b315af232ed` |

## Что ещё не доказано этим gate

1. Реальный HTTP/WS admission, session/ticket exchange, anonymous stream limits, recheck после await/ACK и прекращение передачи при revoke — следующий B2. Один успешный вызов policy API этого не доказывает.
2. Browser iframe/cookie partitions, private direct link, UX согласия whole-port и именования — отдельная B3/C приёмка. В модели `runtimeReady=false` сохранён.
3. Power-loss, настоящий Linux Docker read-only WAL, совместимые candidate/fallback images и восстановление production backup остаются release gates. SQL fault rollback и конкурирующие connections не заменяют эти испытания.
4. Receipt retention ограничена64 последними обновлениями app. Вечная уникальность удалённого request key и автоматическое выяснение исторического исхода не обещаются; клиент не должен сам перебазировать неопределённую команду.
5. Target revision закрепляет маршрут, owner, профиль и согласие на порт, но не неизменность кода веб-приложения на чужом устройстве. Авторский app отвечает за свои административные endpoints.
