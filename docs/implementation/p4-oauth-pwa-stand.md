# P4-C1: локальный persistent OAuth → PWA stand

01.10.2026. Новый stand использует **actual `createHttpApp` + snapshot текущего production dist**, настоящий Connect/Provider/Notes2/Capabilities3 и новый собственный каталог данных. Авторский fresh-run HTTP smoke после root metadata fix — **GREEN**. На момент этого smoke browser не открывался; identity, consent, OAuth codes/tokens и Notes rows не seed-ились. Это локальная подготовка root browser gate, не production deployment и не доказательство совместимости настоящего Codex CLI.

## Финальный стенд5493/5494: текущее состояние

После root review копия `var/p4-oauth-pwa-final/` запущена **один раз**, с новым owned data и memory keys:

- PWA `http://127.0.0.1:5493/`, test-only client `http://127.0.0.1:5494/`.
- Run `f951cfb90e480b7fa119c79d652b25b0`, PID48260, tool session9041; started `2026-09-30T21:48:00.567Z`.
- Issuer `http://127.0.0.1:5493/oauth`, resource `http://127.0.0.1:5493`, callback `http://127.0.0.1:5494/callback`.
- Data `var/p4-oauth-pwa-final/runs/f951cfb90e480b7fa119c79d652b25b0/data`.
- Read-only smoke **PASS** at `21:49:01.253Z`: PWA/metadata/PRM200; native ready; все9 выбранных SQL counts, client actions и7 route finishes0 до root browser.

Root добавил production consumed-consent GET/copy correction перед этим start и принял её отдельными targeted checks; автор её не менял. По сравнению с reviewed confirmed stand в новой копии изменены только namespace/порты и добавлен startup source pin `server/capabilities-oauth-document.js`. Counter bytes неизменны; новые pure tests/build не запускались. Root единолично выполняет browser действия. Последующие metadata/SQL/квитанции этого автора относятся к подготовке, не к ещё не завершённому edit/replay/revoke gate.

**Прерывание предшественника:** перед этим новым запуском root проверил отсутствие прежних PID40896/PID51960 и освобождение5491/5492 через GetProcess/GetNetTCP. Их `status.ready=true` теперь исторические записи, а не проверка живости. Причина/способ завершения и graceful close этим автором не установлены; прежние данные и evidence сохранены. Root успел наблюдать в confirmed run реальный normal-account Notes HTTP201 и client1start/1callback/1exchange/1native, `callbackRefererPresent:false`. Это атрибутированный root результат, не повторная SQL-проверка автора. **Edit/replay/revoke были прерваны и не считаются пройденными.** Старые DB не открывались новым выдающим AS с другими ключами.

Guide ниже применять к текущим5493/5494. Остальные адреса и PID сохранены как история отдельных запусков.

## Предыдущий стенд для проверки Origin5491/5492

После подтверждённого root browser finding создана отдельная копия в `var/p4-oauth-pwa-confirmed/`. Она запущена **один раз** после root source review и исправления production header policy:

- PWA: `http://127.0.0.1:5491/`; test-only внешний клиент: `http://127.0.0.1:5492/`.
- Issuer `http://127.0.0.1:5491/oauth`, audience `http://127.0.0.1:5491`, callback `http://127.0.0.1:5492/callback`.
- Run `170c1ac95ebf7bbeb9856ff63658bdda`, PID40896, tool session64176; started `2026-09-30T21:05:47.782Z`.
- Fresh owned data: `var/p4-oauth-pwa-confirmed/runs/170c1ac95ebf7bbeb9856ff63658bdda/data`.
- Safe smoke **PASS** at `21:07:49.219Z`; до root browser accounts/Notes/proofs/connections/interactions/Grant artifacts/token artifacts/credentials/invocations0, все client action counters0.

Подробные source pins, ограниченные счётчики и границы этого доказательства приведены ниже. Root получил сигнал готовности к actual browser; автор не выполнял bootstrap, `/start`, согласие, token exchange или Notes create. Теперь этот процесс завершён, как отмечено выше; адреса5487/5488 ниже относятся к ещё более раннему сохранённому предшественнику.

## Первый стенд: сохранённый предшественник5487/5488

- PWA: `http://127.0.0.1:5487/`.
- Локальный тестовый внешний клиент: `http://127.0.0.1:5488/`.
- Issuer: `http://127.0.0.1:5487/oauth`; HTTP resource/audience: `http://127.0.0.1:5487`.
- Статический client profile: `soty-codex-cli`; exact callback: `http://127.0.0.1:5488/callback`.
- Исторический run: `7a1616184a86c391566af9ed48e757fb`, PID51960, tool session17454.
- Owned data: `var/p4-oauth-pwa/runs/7a1616184a86c391566af9ed48e757fb/data`.
- Безопасное локальное состояние: `var/p4-oauth-pwa/status.json`; детали source/dist pins находятся в `owner.json` и `dist-inventory.json` этого run.

**Сохранённый actual HTTP RED:** в первом run `56320f85afae2c63d2d460e419de09ce` GET `/.well-known/oauth-authorization-server/oauth` возвращал200 и правильный issuer, но endpoints были без `/oauth`: `/authorize`, `/token`, `/revoke`, `/jwks`. Его `smoke-final.json.pass=false` сохранён без изменения. Root исправил trusted provider mount prefix и truthful metadata/query admission, сообщил собственный causal integration26/26 PASS. Этот автор production source не правил. Fresh run с неизменными stand/client повторил реальные запросы: все четыре endpoints exact `/oauth/*`, scope только `notes.createDraft`, response mode только `query`, token auth только `none`. `smoke-green.json.pass=true`. Это завершает только stand smoke, не полный browser OAuth→Notes gate.

Использованы только два HTTP servers с `createHttpApp`: отдельные Rooms/Apps WebSocket mounts **не добавлялись**. Browser scope этого stand — Connect signed HTTP, consent/callback, Notes и AccessPanel. Live Rooms/Apps runtime не входит в его доказательства.

## Собственные файлы и данные

Новые исполняемые файлы только в ignored `var/p4-oauth-pwa/`:

- `stand.mjs`: loopback host5487/client5488, свежий run, explicit migrations, memory keys, bounded public-dist snapshot, безопасный status и lifecycle.
- `client.mjs`: отдельный test-only native client, memory PKCE/state, exact callback checks, реальные code exchange и Notes HTTP, ручные replay/refresh.
- `runs/<runId>/owner.json`, `dist-inventory.json`, `dist/`, `data/`: сохраняются после остановки. Никакого automatic удаления.
- `status.json`, `smoke.json`, `smoke-final.json`, `smoke-green.json`: только выбранные безопасные поля, counts/HTTP statuses/hash. Нет token/code/verifier/cookie/private keys. Старые smoke не переписаны.

Перед каждым запуском резервируются **ровно**5487/5488 на127.0.0.1. Занятый порт не переключает stand на другой порт и не останавливает чужой процесс. Старые5470/5481/другие data directories не использованы. Создаётся новый random owned run, существующая DB с новым AES key не переоткрывается.

Только свежие Notes/Caps databases проходят явные trusted constructor migrations: Notes fresh→2; Capabilities fresh→2, close, затем2→3. Production host открывает уже подготовленные stores со штатными default-off flags. После host startup read-only count настоящей Connect `accounts` равен0. Crypto RSA2048/JWKS, cookie keys и AES32 bytes существуют только в памяти процесса; raw OAuth secrets не пишутся клиентом/stand в файлы или stdout. Стандартные encrypted Provider artifacts после будущего согласия будут находиться в обычной Caps3 базе. Это не encrypted backup config и не restartable key archive.

Stand очищает ambient `SOTY_*` **только в своём процессе** перед dynamic production imports; системная среда и config files не меняются. Все authority origins и native/OAuth flags заданы явно. Gonka и application tokens отключены explicit пустыми options; client HTTP может обращаться только на фиксированный127.0.0.1:5487, не следует network redirects и не выполняет third-party fetch.

Dist snapshot:50 files /26 451 449 B; index SHA `3841df96ae9fc8008f3c5238f82b7164d972adf6cfcdbe255a0218803765840b`; inventory SHA `6a0a6cba31fb476b4c709b5aea164922a152a20221c2c1959f78578821b22fe2`. Snapshot целиком публичный, включая существующий Xray ZIP20 934 014 B. Проверяется каждый file hash; symlinks запрещены, лимиты512 files/32MiB. Последующие сборки root не меняют уже запущенный snapshot. Первый пробный start отказал до открытия DB на первоначальном8MiB per-file bound этого ZIP; пустой run `993795b2aa128594c9effffb30424868` оставлен без удаления. Затем per-file bound согласован с общим32MiB, без урезания dist.

## Browser guide для root

1. Открыть PWA5487. Создать **новый тестовый Connect аккаунт** штатным интерфейсом. Stand не знает private Connect identity и не подписывает вместо человека.
2. Открыть клиент5488. Нажать «Запросить подключение». Это создаёт лишь memory PKCE S256/state и перенаправляет на настоящий authorize.
3. В настоящем consent PWA проверить выбранный аккаунт и разрешить или отклонить. Никакой отдельной фиктивной consent page у stand нет.
4. При согласии exact callback/state/iss запускает один code exchange и один `POST /api/capabilities/v1/notes/drafts`. После ответа —303 на чистую локальную result URL; code не отражается в HTML/логах. Страница показывает только safe receipt/status и ссылку в PWA.
5. «Открыть эту записку в Сотах» использует **проверенный production route** `/#notes/<noteId>`. Он подтверждён `src/world/app.ts` route/openNotes и фактическим `capabilities-actions.js result.url`; client дополнительно требует точное совпадение возвращённого URL. В PWA должен быть выбран тот аккаунт, который дал согласие. Отредактировать записку обычным UI.
6. На result page «Повторить тот же запрос»: новый ручной POST использует прежний token и **тот же immutable input/idempotencyKey**. Ожидается historical receipt без перезаписи правки владельца. Страница не получает право читать актуальный текст Notes через этот create-only API.
7. «Обновить токен доступа» выполняет только реальный refresh. При принятом ответе заменяет AT/RT в памяти; не создаёт записку автоматически. После неизвестного/неуспешного refresh повтор старого RT блокируется, чтобы потерянный ответ не приводил к невидимому повторному использованию token family.
8. В настоящей PWA `/#access` отозвать конкретное подключение. Ручной exact replay должен отказать. Старая receipt на клиенте остаётся явно названной «Последняя подтверждённая квитанция», а текущий отказ показан отдельно.
9. Второе **новое согласие** того же аккаунта создаёт другую connection. После уже выполненного intent повтор того же ключа ожидаемо даёт `invocation_request_conflict`, а не вторую записку. Для независимого другого сценария требуется новый **явный** intent в тестовом source; никакой автогенерации после ошибки нет. Другой Connect account имеет собственный scope.

Постоянный intent этого stand: `p4-oauth-pwa-one-note-v1`; title «Проверка OAuth → Соты» и фиксированный двухстрочный synthetic body. Это исходный текст клиента, не readback Notes. Неподтверждённый ответ создания имеет отдельный unknown state; exact replay допустим, автоматический повтор отсутствует. Denial не делает token exchange/Notes POST.

## Границы client и проверка

На5488 strict Host/loopback address, callback browser-session cookie + exact memory state/issuer, duplicate-query rejection, fixed registered redirect/resource. `/replay` и `/refresh` принимают только POST с exact Origin, bounded form и session-bound CSRF, не дублируют busy work.4 live flows,16 results,8 local browser sessions, максимум4 downstream operations; истекающие PKCE flows очищаются. Form≤2KiB/3s; outbound body≤32KiB, ответ≤64KiB/5s, один preallocated buffer; HTML≤32KiB. Client pages no-store/no-referrer, CSP без scripts/third-party resources. Полные URL/query, cookies, authorization headers и raw HTTP error bodies не печатаются.

Синтаксис обоих файлов проверен. Только короткий real HTTP smoke, без broad suites и без browser:

- `/` на5487 =200, bytes совпали с owned actual dist index.
- PRM200, native status `notesCreateEnabled:true` и exact audience.
- Client200/no-store; чужой Host обоих listeners400; invalid callback400; foreign-Origin replay400.
- Accounts/Notes/native proofs/OAuth connections/artifacts/invocations все0; authorize/code exchange/native/refresh counters все0.
- Scoped metadata200 с правильными issuer/authorize/token/revoke/JWKS URLs и exact query/scope/none declarations после root fix. Старый endpoint mismatch сохранён в отдельном **RED** receipt. Unscoped `/.well-known/oauth-authorization-server` корректно отказал400; это не нужный discovery URL для issuer path `/oauth`.

Текущие SHA256 source: `stand.mjs` — `6b577e037d3cd40a0d03db9970a75c86647c61695f126f7555d456a745dd937f`; `client.mjs` — `32a3e9ec8f53423a9e5d56c0eb0937e6d9540c9dc05ce79111d69902c331c6f2`. RED safe smoke `smoke-final.json` — `f69e74c0f831a3be8ec920789341adbef8bffc7b9128dd6db2e927a7d04d74be`.

Fresh GREEN safe smoke `smoke-green.json`:1 396 B / SHA `8f9a5b8bba66ccb89b34205d22f1cb9d7ef45e893841e6286e45d3d62dd114df`. Fresh `owner.json`:2 365 B / SHA `16a59c8809ff4d5eed8d641e1555b834307ef18c83a03f1c1c7d5ced55303b7d`; в нём перечислены полные source pins перед фактическим import и проверены повторно после composition. Changed root files в этом запуске:

| Source | Bytes | SHA256 |
|---|---:|---|
| `server/capabilities-oauth.js` | 12 430 | `d20cd27ff668cbe02dd8943ea960dc4d7d8149d15d3c49e1a66b59d7e4a67584` |
| `server/capabilities-oauth-provider.js` | 11 745 | `f4cd38b6174c71085627650a3eb2b8d4a5277d64ec2949b279f5dfdf193e0bc1` |
| `modules/capabilities/server/oauth-profile.mjs` | 14 230 | `e16b589a2cf8c492e7eb567989e465288009aea4bbc3b3c0db98319bd5799e07` |

Source stand/client и public dist между RED/GREEN не менялись. Author не запускал bootstrap, `/start`, authorize, callback с действительным state, token exchange или Notes create. Counts0 в GREEN — снимок **до** root browser действий, не обещание, что stand навсегда останется пустым.

## Остановка и пределы доказательств

Собственный процесс слушает SIGINT/SIGTERM: перестаёт принимать запросы, закрывает только свои listeners/services и ожидает уже принятые bounded client jobs. Файлы не удаляются. Если процесс завершится аварийно, ordinary SQLite/WAL сохраняется; ephemeral keys/PKCE/AT/RT утрачиваются. Старый run не используется для нового выдающего AS с другим ключом. Сам статус-файл не является proof graceful shutdown или отсутствия уже принятого эффекта.

Фактическая остановка прежней session59507 через tool Ctrl+C дала exit1; PID53072 исчез, оба owned порта освободились, old data сохранились. `stoppedAt` не был записан, поэтому **graceful close не доказан**. Выбранные факты и прежний safe status сохранены `runs/56320f85afae2c63d2d460e419de09ce/stop-observation.json`. Fresh process не переиспользовал его DB/ключи. Никакого удаления старого run не было.

Это **настоящий локальный HTTP/PWA стенд**, но пока не фактический human browser OAuth→Notes proof, не два внешних CLI, не внешний HTTPS/domain, не full image/restore/reader3 production admission. Без root browser actions здесь нет утверждения об approve, callback, созданной записке, клавиатуре или geometry. Root проверяет source перед использованием; дальнейшие host findings исправляет их владелец.

## Последующие browser действия root и обновление dist без перезапуска

После исходного GREEN smoke root создал аккаунт и одобрил запрос через actual PWA. Автор выполнил только read-only выбранные counts: на `2026-09-30T20:39:35.010Z` accounts1, approved interactions1, pending0, denied0, connections1; Notes0, native proofs0, Grant artifacts0. Client starts1/callbacks0/exchange0/native0/refresh0, activeFlows1, retainedResults1. Файлы читались в отдельных readonly transactions, а не одном cross-store snapshot. Receipt `counts-approved-before-ui-refresh.json` в текущем run, SHA `5a4238aae23b32b4bdf402147d96a8dc7d24288bf7694973d517ff48b4c91c53`. Это доказывает состояние на момент чтения и отсутствие native результата тогда; не причину отсутствия browser navigation.

Root просмотрел подготовленный автором `var/p4-oauth-pwa/refresh-dist.mjs` (SHA `29aa8d2de95232dcae08b3843bd4c341d307cbd46bee589d2250e4bbfd1adcda`), собрал production dist и сам применил первый controlled refresh `b1af0ac0cc10cf55881eaa63`. Manifest SHA `8699b3f2ba61d5e2a73a6b0639d6a852626c3ededd2cee118eab3430d2e47232`; application journal SHA `f8512726736a70967f2546443dc85ebb7bf4e3c951dc612d644b4f32ff71bf63`, terminal `complete` at `20:44:13.999Z`. Candidate50files,16 добавленных assets, union66files/30 876 770 B. Index SHA `8c63949623da663b1e5ad3e0d2f5f860619d2187565b7ce10eb815e977476a57`; SW SHA `14c93256da244837afd255c7efe7d9bd4c775d2da8884be6694ad0ec80f4a073`. Оригинальные файлы/manifest сохранены; stand/client/process/SQLite не изменялись. Стартовый `status.dist` по-прежнему обозначает **первоначальный** snapshot, последующие изменения подтверждает отдельный manifest/journal.

Первоначальная гипотеза о `form.submit()` с последующим удалением формы **не доказана**: root отдельно наблюдал actual POST на изолированном sink и для старой, и для retained-form версии. Улучшения Promise/busy lifecycle и диагностики browser CSP/timeout проверены отдельно; им не приписывается причинное устранение исходного сбоя. Первая interaction затем естественно истекла, её срок не продлевался.

Для второй версии подготовлен только новый `refresh-dist-next.mjs`, который принимает ровно указанные predecessor manifest/journal и их completed union. Read-only измерение нового candidate50files/26 455 246 B, added16/4 426 759 B, будущий union82/35 303 529 B выявило превышение32MiB на1 749 097 B. Root явно согласовал **отдельный QA served-union cap48MiB**, сохранив candidate/per-file32MiB,512files и все production/runtime/container/canary bounds. Это удержание публичных старых assets для live страниц, без удаления или тихого увеличения общего runtime лимита. История и rationale находятся в ignored `refresh-dist-next-plan.md`.

Second helper SHA `c7d32dac03b0dfba4386683e2ae5011d5321d022cb686a6755278243dc8bc18f`,11 020 B; план SHA `c71b0297fb097646a9b1e7cdb0a686a740a080bb6a1b2e034faa0ffa7bb9a697`. Автор проверил синтаксис и source, но **не запускал второй prepare/apply**, не перезапускал сервер и не открывал browser. После protocol finding root отменил применение второго refresh: потребовался отдельный новый runtime. Старый процесс и данные не останавливались/не удалялись этим автором.

## Confirmed-Origin runtime5491/5492: ограниченная дельта и проверка

Root зафиксировал отдельный native DOM probe: `Referrer-Policy:no-referrer` даёт literal `Origin:null` на form POST, а `same-origin` сохраняет same-origin Origin. Это согласуется с отказом strict Origin guard; вместо ослабления guard root исправил headers consent HTML и provider resume HTML. Автор не правил production source. Header-политика исключительных late errors не объявляется универсально проверенной: root/critic отдельно ограничили это утверждение покрытыми paths.

Новые stand/client являются точной копией сверенных исходников с перечисленными дельтами: ports/owned namespace; bounded route-only counters; coalesced safe status publishing; булев Referer на **валидном** callback. По отдельному root согласованию HTML test-client тоже имеет `same-origin`, чтобы его ручные `/replay`/`/refresh` прошли неизменённые strict Origin/CSRF проверки. JSON/303 test-client сохраняют `no-referrer`. Ни auth adapter, ни seed identity/consent, ни новый permission API не добавлены.

Счётчики используют семь фиксированных labels: authorizeGET/interactionGET/contextGET/completePOST/resumeGET/confirmPOST/tokenPOST. Перед Provider URL rewrite захватываются только label и класс Origin `exact/null/absent/other`; после response `finish` записываются safe numeric status/aggregate. Не сохраняются UID/query/state/code, raw URL/header/body, Origin/Referer values, cookie/token/verifier/key. Число возможных aggregate keys ограничено644; publisher удерживает одну активную запись и один dirty flag. `close` без `finish` не считается; ноль finish не доказывает отсутствие незавершённого запроса. `callbackRefererPresent:null|boolean` обновляется только после exact state/iss/session проверки callback, поэтому invalid probe не заменяет наблюдение настоящего возврата.

Авторские pure-counter tests: **3/3 PASS**,0skip,72.1542ms на isolated Node24.21.0. Проверены route capture после rewrite, отсутствие secret-shaped синтетических значений в counters, Origin null/absent/duplicate, finish≠close, detached snapshots и bounded status keys. Это не simulated OAuth acceptance. Синтаксис и exact hashes четырёх новых файлов перепроверены перед единственным startup. Старые stand/client hashes совпали с frozen pins.

| Новый source | Bytes | SHA256 |
|---|---:|---|
| `stand.mjs` | 10 274 | `cf03f12ea5d18129c5e23735052eb9328c7e5d732ee4ea4afc757b2b5dbba83c` |
| `client.mjs` | 23 542 | `e2ff09ea155f1e1fde2a5572aba42ea3bc4e29ee3ecd6997f7c4ddc4a4ad5d21` |
| `route-counters.mjs` | 3 103 | `188107726813df71e86a61aaacb85031587d29cf1b53de0029c71be0c2a416a9` |
| `route-counters.test.mjs` | 3 324 | `95da7caa70abcb9d1f2f2ef187515f29ecfa15532923eb371e22d51f1d26bd23` |

New public dist50files/26 455 246 B; index SHA `449a8fd9ab6a63c804288bb71101063b9f81ddc3ac047f13156786d2b87ad03a`, inventory SHA `7e5db8729e5276762d9f7427b0d2dd16b2772147bb536efcd1252bca40bb0c0a`. New owner.json2 562 B/SHA `a5c2345e1898c2f9181049c85a9b541071ccd3ba113f2934906b83cf4242314a` фиксирует source вокруг actual import. В том числе production `capabilities-oauth.js`12 696 B/SHA `24c1a33f0438a5f3204b7fd2e7749f246f821e002d1acede45cf5bb705b6e271`, provider11 877 B/SHA `0d57d161631aac39d93cc0370a54bc4c7f3266b959ddf007812a74cfbda483de`; domain profile не изменён относительно предыдущего run.

Новый `var/p4-oauth-pwa-confirmed/smoke.json`1 484 B/SHA `6f68cc4f6d8d8e94b2a2fa09ced620c193f65f5590ce8944c29ca1745cad66e7` подтверждает только GET smoke и selected readonly SQL: PWA200 exact bytes, scoped metadata200/PRM200, exact `/oauth` endpoints, query-only/native-scope/none, native Notes enabled/exact audience, client HTML same-origin/JSON no-referrer/no-store. SQL и все action/route counters были0 **до root browser**. Consent/resume HTML policy не проверялась этим smoke через выдуманную interaction; её genuine browser path принадлежит root. Cross-store atomic snapshot не заявлен.

Эти stand используют отдельные web origins, но HTTP cookies не изолированы номером порта. Новый сервер использует свежие ключи и не принимает старую authority; этот helper не обещает независимые browser cookie jars одного127.0.0.1. Connect storage остаётся origin-scoped, новый аккаунт создаёт человек в новой PWA. На момент подготовки5491/5492 автор оставил прежние данные/артефакты и процесс без изменений; последующее завершение процессов описано выше. Ключи не записаны на диск. Полный OAuth→Notes browser результат, внешний HTTPS, настоящие CLI и image/restore gates не выводятся из подготовки.

## Final runtime: exact pins и zero smoke

Final source hashes: `stand.mjs`10 299 B — `2f182ebda47f9fbb8eb0e5b36c091868317d5550982c43acc804ddbfa952afa4`; `client.mjs`23 542 B — `b2285400365ad5d0b8f2d18e27c62b814abbea324243ad09b398822452de74e5`; `route-counters.mjs`3 103 B — прежний `188107726813df71e86a61aaacb85031587d29cf1b53de0029c71be0c2a416a9`. Exact predecessor hashes и разрешённая diff записаны в `var/p4-oauth-pwa-final/source-review.json`. Перед source acceptance проверен синтаксис stand/client; counter tests не повторялись для побайтно прежнего алгоритма.

Owner.json финального run2 704 B/SHA `03237adf9ba26b405e452cc3a38573669d115fa11b7dc58d61de780e03ec509d` фиксирует source перед actual import и повторную проверку после composition:

| Production source | Bytes | SHA256 |
|---|---:|---|
| `server/capabilities-oauth.js` | 13 257 | `a9ad1b1a70a5762c8038d2c765e7d831bb47886e9bb2a74f8f46356a08850bd7` |
| `server/capabilities-oauth-provider.js` | 11 926 | `7d273aa714babbc1603a15a56a0d7cd1dd14fe57d96427397f3a9abe67f0c031` |
| `server/capabilities-oauth-document.js` | 3 056 | `70f222f85d49816de4f953ab013d0ec1279b41564b20806ac05b0b2bdd3fe430` |

UI dist50files/26 455 246 B не пересобирался этим автором: index SHA `449a8fd9ab6a63c804288bb71101063b9f81ddc3ac047f13156786d2b87ad03a`, inventory SHA `7e5db8729e5276762d9f7427b0d2dd16b2772147bb536efcd1252bca40bb0c0a`. Startup скопировал root latest dist в новый owned snapshot.

`var/p4-oauth-pwa-final/smoke.json`1 599 B/SHA `f44c2c0fb9bae6aeaef606b8d029b2105dc921254108b2e7a07560a2714f4f4e`: GET-only smoke с exact PWA bytes, metadata/PRM200, exact issuer/endpoints, query-only/scope/none, native ready, public PWA/metadata/client JSON `no-referrer`, client HTML `same-origin`, client no-store. Selected readonly SQL0 и client/route counters0 относятся к моменту `21:49:01.253Z`, до root browser; чтения разных stores не образуют атомарный cross-file snapshot. Никакого `/start`, bootstrap, authorize, token или Notes POST у автора не было. Valid consent/consumed document headers и дальнейшее browser поведение этот smoke не симулирует и не объявляет проверенными.
