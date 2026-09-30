# P4-C1a — проверка выбранного OAuth provider

30.09.2026. Root установил **oidc-provider9.12.2** exact через pinned pnpm10.30.0 с `--ignore-scripts`. Package/lock diff добавляет эту dependency и20 транзитивных packages, существующие pins не обновляет. Изолированный Node24.21.0; системные установки и пользовательские конфигурации не менялись. Это первый protocol/storage-seam checkpoint по [C1 плану](p4-oauth-connection-plan.md), не готовое подключение внешнего ИИ.

## Что действительно выполнено

`server/test/capabilities-oauth-provider.test.mjs`: **4/4 PASS,0 SKIP,975.1677ms**. Log `output/implementation-20260930/p4-oauth-provider-spike.log`.

- Настоящий token endpoint pinned Provider проверяет S256. Неверный verifier оставляет code непотреблённым.
- Документированный `Provider.ctx` доступен в public adapter `consume` с уже разобранными params и authenticated client. Ранняя exact resource проверка отвергает неверный code/refresh resource до CAS; последующий правильный запрос успешно выдаёт/обновляет токен.
- Настоящий `/revoke` для AT вызывает family revocation, включая RefreshToken и Grant. Другое подключение остаётся действующим. Этот наблюдённый результат исправил первоначальное неверное предположение о single-AT revocation; C1 принимает family-wide модель явно.
- Два Provider/SQLite handles с одним issuer и отдельными HTTP listeners синхронизированы после чтения одного RT, перед consume. SQLite CAS допускает максимум одного победителя; проигравший отзывает family, поздний token upsert отказывает. Это **один OS process**, не требуемая будущая проверка двух AS процессов и не доказательство atomicity всего async token exchange.
- Полный сетевой `authorize → interaction → synthetic owner consent → resume → code → token` подтверждает `state`, `iss` и registered redirect. Получены ровно шесть ожидаемых моделей: Session,Interaction,Grant,AuthorizationCode,RefreshToken,AccessToken. Во всех наблюдённых writes есть конечные exp/ttl. Лог содержит только имена полей, не их значения.

Тестовый SQLite adapter/auto-consent принадлежат только disposable fixture. Он не выдаётся за production encrypted adapter, Connect identity, новый UI, два внешних CLI или реальный Notes effect. Основная C1 реализация должна повторить эти seams со своим bounded persistent storage, действующим Connect actor и неизменным B2 результатом. Никакой import внутренних OAuth grant handlers не потребовался.

## Подготовленные локальные инструменты

Глобальный pnpm11.19.0 пытался автоматически выполнять install при script invocation. Для следующей работы root подготовил отдельный `var/toolchains/pnpm-10.30.0/bin/pnpm.cjs`; официальный npm tarball4457020B сверён с registry integrity `sha512-K1dT3gFdSA7riPW1th4AUfBbQwGAioLsi4QMnSrfd0jrNSyD9cFZPKcD/xAXKVvD/dMRmruWhu/Ja5/LGCAJNw==`. Он запускается явным isolated Node и соответствует packageManager/существующим modules. Системный PATH постоянно не изменялся.

OpenCode1.18.15 подготовлен отдельно в `var/toolchains/opencode-1.18.15-win-x64/opencode.exe`. Official baseline ZIP60500953B имеет SHA256 `98df4ed9993406e190b9a4c937aea98d733bb047c47e93c9f0c2f90ab90c2982`, совпадающий с прежним release descriptor. Внутри проверен единственный `opencode.exe`178673032B. Реальные `--version` и `mcp --help` прошли. Это доступный бинарник, не OAuth/LLM compatibility proof; следующие auth испытания используют отдельные client homes.

## Решения до DDL freeze

Каждый AT сохраняет собственную неизменяемую Cap credential; обновление не продлевает исходное полномочие Invocation. Любой OAuth token revoke отключает его connection family. Чтобы повтор после re-consent не создал второй Note, принята дополнительная OAuth-only область idempotency `account + issuer + static profile + resource`: другой connection с тем же opaque key получает generic conflict без старого результата и нового эффекта. Это явная новая transport semantics с одним битом о занятом ключе, без обещания полной изоляции коллизий между такими connections; стабильные случайные keys нужны клиенту. Same-connection replay и legacy service credentials сохраняют B2 поведение. Нового ledger и переноса старого pending effect под новый grant не создаётся.

## Повторное согласие и срок AS session

Независимый reviewer обнаружил несовместимость default `expiresWithSession` с выбранным create-only scope: без `offline_access` library связывает token с короткой browser Session и её последним grant данного client. Это не соответствует независимым24h connections. Принято явное public `expiresWithSession:()=>false`; действительность по-прежнему ограничивают durable connection/root/creator и первоначальная expiry. `Session.authorizations[clientId].persistsLogout` включён в точный storage profile; браузерная сессия не становится identity или remembered consent Сот.

Новый actual authorize test без client `prompt=consent` проходит два fresh consent одного static profile в одном cookie jar, обновляет обе семьи, удаляет настоящую Session через public Provider model, снова обновляет обе семьи и отзывает одну без отключения другой. При profile=true — причинный RED `400!=200` при двух **неотозванных** families; profile=false — GREEN. Исходный toy adapter тоже был уточнён: borrowed `Interaction.grantId` — metadata, его destroy не отзывает family. Первый неуспешный опыт смешивал эти две причины; самостоятельный causal RED после исправления fixture сохранён отдельно.

Последний принятый root run до независимого ingress audit: **11/11 PASS,1942.8627ms** (6 Provider cases +5 ingress cases), `p4-oauth-session-profile-green.log`; causal RED `p4-oauth-session-profile-red.log`. Удаление Session проверено; ожидание реальных600s, host TTL preservation при resave/resetIdentifier, два AS OS processes и durable domain3 ещё не проверены этим test.

## Host ingress и offline

Добавлен отдельный boundary `server/capabilities-oauth-ingress.js`: form≤16KiB в одном buffer, strict UTF8/percent/duplicate decoding, deadline10s, URL≤8KiB, ≤16 concurrent handlers и bounded socket-peer rate registry. Семантику grant/PKCE/tokens сохраняет библиотека. Installed Provider parser сам допускает56KiB и использует chunks; выбран его штатный pre-parsed `req.body` fallback после внешнего bounded reader. Actual token/refresh/revoke проходят; library выдаёт фиксированное предупреждение, что upstream parsing не рекомендуется. Оно не подавлялось, internal handlers не импортируются и не меняются. Совместимость fallback должна оставаться regression при обновлении pinned dependency.

Независимый actual-Provider test выявил, что ранний вариант release по `res.close` освобождал слот до окончания async adapter. Исправление: lease освобождает владелец **после awaited handler в finally**; для Provider применяется public `provider.use` с `await next()`, а не Express `next()`. Abort во время body чтения освобождает buffer сразу. Socket close не объявляется завершением retained downstream work. Независимый итог этой поправки фиксируется отдельным ingress receipt.

`public/sw.js` явно исключает `/oauth`, `/mcp`, OAuth/PRM `.well-known` namespaces из cached SPA navigation fallback. Существующий Notes fallback сохранён. Расширенный focused offline route test **1/1 PASS94.0762ms**, `p4-oauth-sw-network.log`; это route contract, не установленная PWA с настоящим consent.

[Storage/API contract](p4-oauth-storage-contract.md) принят для отдельной реализации baseline3. Reader3, реальный signed consent, доменный/HTTP/CLI аудит и release остаются следующими последовательными этапами. Host ingress/provider пока не смонтированы в production.

## Принятый итог C1a

[Независимый ingress receipt](p4-oauth-ingress-review.md): **15/15 PASS,0 skips,3019.061ms** одним последовательным запуском4independent+5ingress+6Provider. В частности actual Provider продолжает удерживать admission после socket close до завершения adapter; после завершения слот снова доступен. Real16384tinychunks memory probe retained heap235120B<2MiB; это heap delta одного fixture, не production RSS.

Root после итогового исправления прочитал все независимые tests/receipt. Typecheck PASS, полный `src/platform/pwa.test.mjs` **11/11 PASS,10152.782ms**, Vite production build PASS3.52s; логи `p4-c1a-{types,pwa,build}.log`. Connector prebuild не повторялся: его исходники этим срезом не изменены, предыдущий B2 release artifact сохранён. Historical2 четыре source files дополнительно byte-compared root с exact Git blobs cc7f65b; `.gitattributes` фиксирует LF для их воспроизводимости. Мигратор fixture здесь не исполнялся.

Завершён узкий C1a: выбранный provider реально проверен, HTTP lifetime/buffer границы исправлены, storage/API contract принят и прежний v2 source сохранён. **Реализация domain3, реальный Connect consent и доступ внешних клиентов ещё впереди.**
