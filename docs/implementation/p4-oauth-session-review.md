# P4-C1 — независимый Session lifecycle seam

30.09.2026. Pinned **oidc-provider 9.12.2**, Node **24.21.0**, actual loopback OAuth HTTP + Provider + SQLite adapter fixture. Итог **3/3 PASS, 0 skips, 673.411 ms**. Production/domain files не менялись. [Тест](../../server/test/capabilities-oauth-session.test.mjs) не вызывает `Session.save`, `persist` или `resetIdentifier` вручную: их вызывает библиотека из настоящего authorize/resume flow.

**Найдено существенное ограничение конфигурации:** `ttl.Session: 600` означает скользящий срок. Повторный authorize через10s сохраняет прежний `iat`, но ставит `exp=original+610`. Поэтому одна эта настройка не выполняет принятый контракт «не более600s с первоначального создания». Root и domain author уведомлены до реализации support port; source baseline3/DDL не требуют изменения.

## Наблюдения и причина

1. `lib/shared/session.js:29–48` в finally сохраняет уже существующую Session на каждом authorization request, даже до завершения нового interaction. Он каждый раз получает `ttl.Session` и вызывает `session.save(ttl)`.
2. `lib/models/base_model.js:84` устанавливает `exp=epochTime()+ttl`; `lib/models/formats/opaque.js:31` сохраняет первоначальный `iat` найденной Session. Поэтому fixed600 продлевает окно.
3. `lib/actions/authorization/resume.js:111` меняет identifier существующей Session. `lib/models/session.js:85–95` сначала вызывает `adapter.destroy(oldId)`, затем новый `upsert`; `uid` и `iat` при этом остаются прежними. Реальный HTTP trace подтвердил этот порядок и смену `jti`, без прямого вызова lifecycle из теста.
4. `lib/helpers/interaction_policy/prompts/consent.js:11` содержит default `native_client_prompt`: native code client без результата текущего consent требует interaction даже при известной Session/старом grant. В actual HTTP второй authorize без `prompt` создал другой interaction; `prompt=none` вернул `interaction_required` без code и без вызова consent fixture.
5. `lib/helpers/process_response_types.js:115–118` при explicit `expiresWithSession:false` не связывает code с жизнью Session и пишет `Session.authorizations[clientId].persistsLogout=true`. `lib/models/token_helpers.js:9–16` при отсутствии/false этого флага не требует Session для token validity. Обе ранее выданные RT семейства успешно обновились после controlled expiry Session.

Это поведение закреплённой версии, не обещание для будущего library upgrade. Ранее accepted provider seam проверял immediate Session.destroy; он не доказывал неподвижность первоначального окна при repeated save/reset. Новый тест закрывает именно этот пробел.

## Минимальное решение для host/domain

Host задаёт function `ttl.Session(ctx, session)`: для новой Session600; для найденной — остаток `session.iat + 600 - currentEpochSeconds`. При неположительном остатке — controlled expiry, **не** `Math.max(1, remainder)` и не fallback600. Support adapter `find` сам отказывает по абсолютному expiry; это не зависит от стандартного library clockTolerance15s.

Domain Session/Interaction profile проверяет finite safe `iat/exp`, `exp>iat` и `exp<=iat+600`. Для новой Session row после reset `created_at` берётся из проверенного первоначального `payload.iat*1000`, `retain_until` не позже того же original bound. Новый `jti` сам по себе не выдаёт ещё600s. Повторные updates не расширяют immutable original retention. Это особенно важно, потому что штатный reset удаляет старую row до вставки новой — искать прежнюю row после destroy уже поздно.

Проверка payload остаётся окончательной границей. Между вычислением TTL и фактическим save может пройти время; неожиданный overflow не должен молча увеличивать expiry. На уже истёкшей Session обычный следующий authorize получает новый session/interaction и новое согласие. Если expiry пересечена внутри текущего request, допустим управляемый отказ и повторный новый authorize.

Actual expiry fault был введён только сдвигом Date на публичном `authorization.accepted` event уже resumed request: старт на original+599, после события original+601. Библиотека вернула **303** на зарегистрированный callback с `invalid_request`, прежним state и **без code**. Это нормальный OAuth error redirect из `lib/shared/authorization_error_handler.js`, а не HTTP400 page. Следующий HTTP authorize создал свежую Session; ранее принятая RT осталась рабочей. Тест подтверждает отсутствие code **в ответе**, а не отсутствие промежуточных AS artifacts: library может сохранить code/grant до failure в Session finally. Не следует выдавать такой отказ за rollback всех ранее совершённых adapter writes.

`Session` и `Interaction` остаются вспомогательными artifacts. Их upsert/destroy не дают Connect authority, не разрешают auto-consent и не отзывают отдельные connections. Borrowed `Interaction.grantId` не должен становиться family owner. На каждом новом authorize требуется новое signed решение владельца в будущей host composition; synthetic route этой проверки не заменяет.

## Исполненные случаи

| Case | Результат |
|---|---|
| Fixed600 counterexample | На second authorize через10s библиотека resave того же Session ID делает original-window610s. HTTP resume затем меняет ID, сохраняя original uid/iat и610s; old-ID destroy предшествует new-ID upsert. Это **отрицательный контроль конфигурации**, тест зелёный потому, что явно ожидает и фиксирует дефект. |
| Remainder TTL | Первый и второй завершённые authorize через100s используют разные interactions и Session IDs, но один original uid/iat/expiry/row.createdAt. Native prompt срабатывает без клиентского prompt. `prompt=none` не обходит consent. На original+601 обе RT refresh200; следующий authorize получает новый uid/iat, reason `no_session` и собственное600s окно. |
| Expiry внутри resumed HTTP | Clock crossing599→601 даёт controlled registered error redirect/no code; сохранённые Session не продлены. RT предыдущей принятой семьи продолжает работать. Новый authorize после ошибки даёт новую Session и consent. |

Внутренние shapes, полученные через actual adapter.upsert: ровно шесть моделей `AccessToken, AuthorizationCode, Grant, Interaction, RefreshToken, Session`. В этом flow у Interaction наблюдались keys `cid,exp,grantId,iat,jti,kind,params,prompt,result,returnTo,session`. Session assertions проверяют stable uid/iat, changing ID, exp и `authorizations[clientId].persistsLogout:true`; у code/RT/AT `expiresWithSession` не true. `provider.interactionResult` вызывает save с оставшимся `interaction.exp-epochTime()` (`lib/provider.js:233`), а не с новым полным600.

Optional `trusted,lastSubmission,acr,amr,state` здесь не провоцировались. Их допустимость определяется просмотренным pinned constructor/IN_PAYLOAD, не искусственными adapter payloads; этот опыт не доказывает все отключённые feature shapes. Для root composition достаточно уже согласованного bounded Session/Interaction profile с этими documented optional fields; дополнительные модели/универсальный serializer не нужны.

## Запуск и честные границы

```text
node --test --test-concurrency=1 server/test/capabilities-oauth-session.test.mjs
```

Toolchain directory `var/toolchains/node-v24.21.0-win-x64` добавлен в PATH. Log `output/implementation-20260930/p4-oauth-session-independent-final.log`. Первый log `p4-oauth-session-independent-first.log`:2 PASS/1 FAIL из-за моего неверного ожидания HTTP400 вместо OAuth303. После чтения pinned error handler assertion стал точнее: exact303/callback/error/state/no code. Lifetime assertions не ослаблялись.

Использованы только loopback ephemeral listener, in-memory SQLite и собственный cookie jar с path/expiry. Redirect на callback проверяется как значение и **не запрашивается**. Consent route синтетический и только тестовый, adapter адаптирован из прежнего Provider seam; это не второй production adapter. Issued token/code/state/cookie values остаются в памяти и не выводятся. HTTP и библиотечный lifecycle настоящие; только Date управляется `node:test` и public event clock fault. **600s реального wall-clock ожидания не было**, native browser/CLI/Connect/crypto/domain3 port не участвовали. После каждого case listener и SQLite закрываются.

## Frozen evidence

Test SHA-256: `45ff40b61b6765722c13ba336a6ca7ca74560dc2ef4bbd09b53ef60e154b1fa0`.

Прочитанный exact installed9.12.2 source; пути ниже относительно `node_modules/oidc-provider/`:

```text
d7299d8358bbf3a8c45d1ad1a6561cc63c2e308705975af925715805b8618313  lib/shared/session.js
305aff9ef7c9c3c0f0061c5cd0562a9c1b0d940403f131cb875582018a965c7d  lib/models/session.js
77623c1d7d76d840f93f58b1aef57713c7d48dde1f689686208827320ef06d2c  lib/models/base_model.js
116a242cbe98d59b50d2c25e6c9b1810b97bc9a3d505a6ac3f03c54d43e9cb15  lib/models/formats/opaque.js
a0eaff78325ca4916677a22bacca63313a2ed6b76859bbd0e936ebf894a1ef42  lib/actions/authorization/resume.js
26bb342117ff2b05885bebd19ad3373357b8e89bd2f4cd59de0a30f688032d73  lib/helpers/interaction_policy/prompts/consent.js
d01c0eae8065fd2804910152951fec01c15ecca5934861f758dfc8d48104e20a  lib/helpers/process_response_types.js
48537da10b858215825c53da41d614fcb5698acb4d092e5bd2594564a037a64f  lib/models/token_helpers.js
5c1bb286f1872c9f1023f7fb66593fe376d77477ccf82b7e08d82b844847547c  lib/models/interaction.js
94a64ce834032c4d04ece497664e201590a8bb871698d178af7e275f16f1796d  lib/provider.js
8c02c7d60b9a84114a052098cd046c6444fe043909296d81deea7c1818791649  lib/shared/authorization_error_handler.js
```

Следующий integration gate должен использовать будущие закрытые domain ports и root host configuration, не этот fixture. Reader labels, encryption, actual signed consent, rotation/revoke concurrency, full CLI и rollout этим отчётом не принимаются.
