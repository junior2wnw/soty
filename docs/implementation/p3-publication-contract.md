# P3-B — публикация и право запуска

2026-09-30. Рабочий контракт следующего этапа, основанный на A1/A2 `60bd426`. До завершения независимой приёмки A3 реализация B не начинается. Публичный runtime остаётся выключенным. Условия production из preflight не заменяются этими локальными задачами.

## Продуктовый результат

Автор отдельно выбирает имя, право открытия и появление в каталоге. Имя резервирует адрес; само по себе оно ничего не публикует. Человек с публичной ссылкой открывает приложение непосредственно. Закрытая ссылка требует действующего допуска и возвращает человека к выбранному приложению. Прежние адреса, аккаунты и grants сохраняются. Запуск, сохранение, обсуждение и вступление в сообщество — разные действия.

## Последовательность и владельцы

1. **B1 — модель и транзакции.** `agent_ecosystem`: explicit Apps2→3 migration, publication policy, immutable initial RuntimeTarget, точный набор включённых aliases, CAS/receipts, атомарная инвалидизация при grants/revoke/retire. Свои tests и freeze. Aliases всё ещё status-only. Root не меняет одновременно его module files.
2. **B1-review.** `whole_product_critic`: независимые негативные тесты и проверка миграции. `publishing_architecture`: после freeze схемы отдельный trusted Apps3 reader/manifest переход; до этого probe обязан отвергать3. Root повторяет связанный regression и фиксирует checkpoint.
3. **B2 — допуск и транспорт.** После B1-freeze root интегрирует AccessDecision, exact-origin ticket/session, guest runtime, Origin guard и постоянную проверку HTTP/WS. Автор модели отвечает на findings и меняет только согласованные files. Critic пишет отдельные acceptance tests.
4. **B3 — пользовательский вход и независимая приёмка.** Проверить private/granted/unlisted/public, обычное окно/iframe/отдельное открытие, отозванный доступ и потерянную связь. UI имени и аудитории расширяется в C после рабочего доменного контракта; наличие серверного метода не считается завершением UI.

Каждый checkpoint требует результата, аудита, исправлений и записанной границы доказательств. Автор и reviewer не редактируют одну область одновременно.

## Данные и инварианты B1

- `launchPolicy = restricted | anyone`; `listed` независимо от запуска, но `listed=true` требует `anyone`. Начальные значения — `restricted,false`.
- Legacy canonical origin всегда использует прежние личные/community grants. Публичность named alias не расширяет canonical автоматически.
- Удаление личного grant у приложения с `anyone` прекращает старый account session, но не запрещает этому человеку открыть публичный адрес как анонимному посетителю. Для полного закрытия нужны restricted, отключение адреса либо revoke приложения. UI не обещает персональный запрет, если публичный вход сохранён.
- Publication хранит точный `activeDomainIds`. Только bound aliases данного app/owner могут войти в набор. Canonical исключён; новый claim и прежний alias после миграции выключены. Tombstone никогда не активируется. Retire атомарно выключает адрес и инвалидирует относящиеся к нему допуски.
- `app_publications`: appId, policy, listed, monotonic policyEpoch, activeTargetRevision, updatedAt. `app_runtime_targets`: неизменяемые записи с ключом `(app_id,revision)`, connector key, port, entry path, runtime profile и digest. Начальная target revision1 закрепляет фактическую конфигурацию существующего приложения. Составной FK публикации указывает на target того же приложения.
- В B один авторитетный указатель runtime target. Прежние поля источника служат исторической совместимости, не второй редактируемой конфигурацией. Изменение источника и повторное согласие — отдельный C; B не добавляет частичную смену порта.
- Publication update требует владельца, текущего аккаунта, CAS, request ID и receipt с явно отдельным пространством ключей от domain claims. Не обещается дедупликация одним ключом между различными API.
- Для `anyone` необходимо явное подтверждение публикации **всего порта и конкретного target/profile**. Это не разрешение только просматривать главную страницу. Веб-приложение автора отвечает за свои административные API и бизнес-авторизацию.
- Grants/revoke/retire проверяются и записываются вместе с соответствующим epoch/invalidation в одной транзакции. Нет окна, где сохранены новые grants, но остаётся прежний действующий допуск.
- Receipt history ограничена последними64 принятыми publication updates каждого app, отсортированными по committed policyEpoch. Проверка действующего owner предшествует replay. Каждый accepted update, включая no-op, увеличивает epoch; insert/prune входят в ту же транзакцию. Grants/revoke/retire не уменьшают epoch. Повтор исходного envelope после prune имеет старый expectedEpoch и отказывает409, не повторяя изменение. Он не называется «не выполнено» или «receipt expired»: отсутствие записи не доказывает исторический исход. Клиент может сверить current view, но не должен автоматически подставлять свежий epoch в неопределённый pending request. Новое решение получает новый request ID и подтверждение. Уникальность изменённого intent под удалённым ключом навсегда не обещается. Нет lifetime quota переключений, которая блокирует emergency restrict/revoke.
- UI не отправляет новое «Сохранить» без изменения намерения: accepted no-op безопасен для CAS, но новая команда зря инвалидирует открытые сессии.

## Допуск B2

Внутренний `AccessDecision` включает `subject: account | public`, actor только для account, appId, domainId, точный origin, policyEpoch, targetRevision и expiresAt. Это серверное решение, не доверяемые браузеру поля. Public visitor не получает подставленного владельца, его cookies или credentials.

Identity и основание права различаются: `accessBasis: grant | public` закрепляет действующий личный/community grant либо публичную политику. Зарегистрированный посетитель без grant остаётся `subject:account,accessBasis:public`. Канонический адрес допускает только grant. Повторная проверка сравнивает basis: потеря membership или прав организатора закрывает прежний granted session даже на `anyone` при неизменном app epoch; свежий публичный вход остаётся возможным. Public stream sublimit считает basis, поэтому регистрация аккаунта сама по себе не открывает резерв ёмкости личных приложений.

Основание уже выданного решения не повышается автоматически: public-basis остаётся публичным до конца этого допуска, даже если человек получил membership; проверка продолжает требовать `anyone` и активный alias. Новый launch может выбрать grant. Grant-basis требует действующего grant и не понижается скрыто до public. Изменение epoch по-прежнему инвалидирует все прежние pins независимо от basis.

`apps.launch` принимает необязательный domainId. Его отсутствие сохраняет legacy контракт. Named launch требует bound+enabled exact alias и свежего права текущего actor. Обмен короткого одноразового ticket на сессию повторно читает адрес, аккаунт, grants, epoch и target **после** асинхронного чтения body. Ticket одного origin не работает на другом alias того же приложения. Реальный статус устройства проверяется отдельно от прав.

Публичный direct request получает ограниченное внутреннее решение для текущего адреса при каждом запросе. Private direct entry использует только фиксированный доверенный маршрут оболочки с app/domain ID и проверенным локальным path. Произвольного `returnUrl`, перехода на чужой origin или раскрытия private metadata нет.

Проверка допуска стоит до открытия stream, перед отправкой и после await ACK, на end и входящих head/data, а также в audit/revoke. Смена membership, отзы́в устройства или отключение адреса должны прекращать последующий поток; уже доставленные байты, cookies/storage приложения и выполненные действия не исчезают задним числом. Измеренный верхний срок проверки изменения внешнего membership фиксируется в acceptance.

Публичные streams имеют отдельный подлимит внутри общего лимита32 на connector. Они не могут занять все места личных приложений. Точное значение и освобождение ёмкости при error/disconnect/timeout проходят нагрузочную проверку B2; это резерв доступности, не обещание защиты от любого DDoS.

Cookies — host-only `__Host-`, Secure, HttpOnly, Path=/, Partitioned. Встроенное и отдельно открытое приложение могут иметь разные partitions, поэтому «Открыть отдельно» получает новый ticket, а не предполагает перенос сессии iframe. Используемые свойства и различие Path/границы безопасности описаны в [MDN Set-Cookie](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Set-Cookie) и [CHIPS](https://developer.mozilla.org/en-US/docs/Web/Privacy/Guides/Third-party_cookies/Partitioned_cookies).

HTTP unsafe methods без Origin, с `null` или чужим Origin отказывают; WS требует точный Origin. GET/HEAD могут не иметь Origin. Это защита браузерного запроса, не удостоверение личности ИИ-клиента. Разделение safe methods основано на [RFC9110](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.1); сайт автора не должен менять данные через GET. [MDN CSRF](https://developer.mozilla.org/en-US/docs/Web/Security/Attacks/CSRF) объясняет, почему автоматическая cookie-аутентификация сама по себе не доказывает намерение человека.

Действующие ограничения headers/Cookie/Authorization/Set-Cookie, редиректов, Service Worker, сети, media и размеров продолжают действовать. Runtime profile явно называет ограничения. Target revision закрепляет конфигурацию маршрута, а не неизменность кода на компьютере. Старый connector protocol не подтверждает revision: до отдельной проверки версии/ACK нельзя обещать, что он исполняет неизвестный ему target revision.

## Решающие проверки

1. Empty/v1/v2→3 и reopen сохраняют app IDs, canonical origins, grants, tombstones и отключённую named-публичность. Неизвестная/повреждённая схема не переписывается.
2. Два writers с одним expected epoch: ровно одно изменение; потерянный ответ и одинаковый request ID не удваивают результат. Другой intent под действующим ключом отказывает.
3. Public app + новый claim остаётся закрытым по новому адресу до отдельного explicit update набора aliases.
4. Чужой, canonical, retired alias и target другого app не активируются; owner/account проверяется до выдачи исторического receipt.
5. Legacy canonical не открывается гостю после включения named anyone; каталог/listed не становится правом private launch.
6. Ticket для alias A не обменивается на B; смена policy/grants/target/retire во время чтения request body отменяет обмен.
7. Revocation на HTTP/WS/await ACK запрещает следующий frame и не возвращает ложный успешный ответ; старый cached decision не становится бессрочным.
8. Wrong/missing/null Origin на unsafe methods и WS отказывает; forwarded Host не подменяет точный host.
9. Private direct link возвращается к выбранному app/path после обычного допуска; нет open redirect и пересылки чужих credentials.
10. На уровне release будущая3 схема отказывает v1/v2-only fallback; фактически совместимые candidate/fallback образы и Linux proof проверяются отдельно до выкладки.
11. Публичная нагрузка достигает guest sublimit; private launch того же connector остаётся возможным. При освобождении соединений счётчик не течёт, превышение лимита не выдаётся за потерю прав.
