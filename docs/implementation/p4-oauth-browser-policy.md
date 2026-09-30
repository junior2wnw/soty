# C1 — native form Origin / OAuth document policy

Дата: 2026-10-01. Авторский scope — только новый `server/test/capabilities-oauth-browser-policy.test.mjs` и эта квитанция. Production headers исправляет root; DOM/browser, UI, fixtures, domain ports и DDL автор теста не меняет.

## Причина и точная проверка

Root отдельно наблюдал на настоящем HTML form: `no-referrer` даёт literal `Origin: null`, а `same-origin` сохраняет origin. Его evidence — `output/implementation-20260930/p4-oauth-form-origin.json`; это браузерное наблюдение root, не результат нового Node test. Существующий wrapper правильно отказывает opaque Origin. Исправляется policy только двух документов, отправляющих native form, а не admission `Origin: null`.

Два новых случая используют прежний **настоящий** `oauthNativeFixture`: production `createHttpApp`, signed Connect proofs, Notes2, Caps3, encrypted OAuth ports и pinned Provider9.12.2. Нет synthetic consent, replacement auth/storage/readiness или вручную вставленных authority rows. HTTP requests следуют вручную; native callback никогда не запрашивается. Форма завершения решения и account-switch autoform останавливаются перед POST вместо автоматического `f.complete`.

1. Consent HTML должен иметь `Referrer-Policy: same-origin`, прежний no-store и точный CSP form-action. После действительного подписанного approve тот же cookie/expectedAccountId с **литеральным** заголовком `Origin: null` получает403. До/после сравнивается digest точных persisted rows connections/interactions/artifacts/credentials/grants/invocations и счётчики Notes/proof. Никакого нового Provider Grant/token/Note. Те же данные с точным origin дают303, настоящий callback code и HTTP token exchange.
2. Настоящий A→B flow с одним browser cookie jar достигает Provider resume200 autoform. Проверяются точный action, настоящий XSRF и script hash/CSP. Literal null с действительными fields получает403 и не меняет Session или обе семьи. Точные origin/fields затем завершают account switch/code/token, а RT аккаунта A по-прежнему обновляется; обе семьи active, Notes/proofs по-прежнему пусты.

Context/token API, completion/confirmation/callback redirects и403 оставляют `no-referrer`; обязательная HTML policy не распространяется на эти ответы. Значения code, cookie, XSRF, bearer и raw error не логируются. Сравнение сохранённых строк выводит только digest при несовпадении, не ciphertext/private payload.

## RED и последующий gate

До root patch, HEAD `a7ef7b3253ad6df6d28aab1c15e1861c39c07ae2`: **0PASS / 2FAIL / 0SKIP, 1622.7904ms**. Оба отказа ровно в последних assertions: actual `no-referrer`, expected `same-origin`. Все предшествующие null-origin negative и exact-origin positive пути исполнены успешно. Лог: `output/implementation-20260930/p4-oauth-browser-policy-red.log`, SHA256 `e79908d5a7a73e9f8d7ddf0fa1e2c1e39c7b4ab0f686c67df7f431462b5f65ef`.

После двух узких production header corrections, выполненных root, неизменённый test дал **2PASS / 0FAIL / 0SKIP, 1564.4232ms**. Лог: `output/implementation-20260930/p4-oauth-browser-policy-green.log`, SHA256 `ba3a054f5eff92f4fa793a8fb761c4996689bc52c0906a57ac513ef4844b74ef`. Это единственный final narrow повтор; serial slot освобождён сразу после него. RED сохранён отдельно, assertions не ослаблялись.

Команда запуска: isolated Node24.21.0 (его directory первым в PATH), `node --test --test-concurrency=1 server/test/capabilities-oauth-browser-policy.test.mjs`. Никакие широкие suites/builds не запускались. Библиотека печатает прежнее фиксированное предупреждение о заранее разобранном body; оно не содержит request data и не объявляется новым product defect.

Test SHA256 первого двухслучайного checkpoint: `73388388fd70c0925c7d2016b5a102e3d63d19c8efddc3b35b1e2810fd674afc` — одинаковый в RED/GREEN. Проверенные root-owned working-file pins этого checkpoint: `server/capabilities-oauth.js` → `24c1a33f0438a5f3204b7fd2e7749f246f821e002d1acede45cf5bb705b6e271`; `server/capabilities-oauth-provider.js` → `0d57d161631aac39d93cc0370a54bc4c7f3266b959ddf007812a74cfbda483de`. Diff сохраняет строгий Origin admission и default no-referrer; только consent HTML и Provider resume200 interaction HTML получают same-origin.

Граница evidence: HTTP requests здесь явно задают заголовок, а не моделируют вычисление Origin браузером. Реальный browser policy causality принадлежит отдельному опыту root. Набор не заявляет CLI/PWA navigation, browser callback listener, production TLS либо полный повтор OAuth lifecycle.

## Дополнение: уже использованная consent-ссылка

По следующему точному поручению root добавлен **ровно один** case, только в конец прежнего файла. Первые9429 bytes сохраняют SHA256 `73388388fd70c0925c7d2016b5a102e3d63d19c8efddc3b35b1e2810fd674afc`: обе прежние проверки не изменены. Новый случай получает настоящий signed approve → complete → code → HTTP exchange, а затем дважды открывает тот же consumed document и его `/context`. Никакого придуманного invalid UID/подменённого clock/нового fixture.

Документ должен остаться отказом, но дать фиксированный standalone HTML с безопасной ссылкой домой, no-store/no-referrer и строгим CSP только для hash собственного CSS; без script/form/input. Context остаётся JSON API. Повторные reads не меняют exact persisted Caps digest/Notes counts и не отражают настоящий UID, code, state, browser nonce, context digest, AT/RT. Проверяются только булевы результаты, ни одно такое значение не входит в лог.

Первый focused RED: `--test-name-pattern='consumed signed consent'`, **0PASS / 1FAIL / 0SKIP, 4286.6845ms** (сам case516.4894ms). Отказ ровно на line177: consumed document имеет не-HTML content type. Предшествующие context/no-change/no-reflection assertions прошли. Лог `output/implementation-20260930/p4-oauth-consumed-document-red.log`, SHA256 `e0b540632e3613e4f5f1c034b27e80e2a5e393effe3cf84b9fe5dbf3bb5cdcac`.

После root HTML helper/copy patch, без правок нового test, выполнены только два последовательно запрошенных запуска на isolated Node24.21.0:

- Полный собственный browser-policy файл: **3PASS / 0FAIL / 0SKIP, 2448.0821ms**. Лог `output/implementation-20260930/p4-oauth-consumed-document-green.log`, SHA256 `c0bc08088b187705de69cabe004195f9a6154042ede36b1e6a313359069975a4`.
- Root-owned `capabilities-oauth-native-flow.test.mjs`, только `--test-name-pattern='expired actual Provider resume'`: **1PASS / 0FAIL / 0SKIP, 1016.0563ms**. Лог `output/implementation-20260930/p4-oauth-expired-resume-targeted.log`, SHA256 `170b7489da4aad9d7ac96643575c1c4bb3afe0c9a62afb45e20d704b7f8d440b`. Изменение текста его assertion выполнил root, автор нового test только запустил этот один case.

Frozen трёхслучайный test SHA256 `e5d5779f847377bbb949c388646d56d14f3d8b239af2c4247c7645ed8347fc39` одинаковый в RED/GREEN дополнения. Слот освобождён. Production автор теста не правил. Проверенные root working-file pins: wrapper `a9ad1b1a70a5762c8038d2c765e7d831bb47886e9bb2a74f8f46356a08850bd7`; Provider adapter `7d273aa714babbc1603a15a56a0d7cd1dd14fe57d96427397f3a9abe67f0c031`; fixed document `70f222f85d49816de4f953ab013d0ec1279b41564b20806ac05b0b2bdd3fe430`; native-flow test `055b169f0f8eac0a4451aedd3fb7e90d15df4f938814f907be4abfadfa8f89c4`. Это узкая причинная проверка consumed/expired HTML surface, не полный повтор всех OAuth tests.
