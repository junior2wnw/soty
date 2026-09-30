# P4 OAuth consent — независимая проверка интерфейса

30.09.2026. Финальная независимая проверка controller: **14/14 PASS, 0 FAIL, 0 skips**, 352.1267 ms. Исходные account/focus/clock findings и дополнительный disclosure lifecycle закрыты воспроизводимыми RED→GREEN. Материальных незакрытых blockers в этой ограниченной controller-проверке не обнаружено. Native browser, реальный подписанный consent и OAuth end-to-end остаются отдельными gates; этот результат не объявляет весь consent flow готовым.

## Граница

Read-only review: `src/world/oauth-consent.ts`, `oauth-consent.css`, `src/platform/oauth-consent.ts`, `src/entry.ts`, `server/capabilities-oauth.js`. Независимый автор владеет только новым `src/world/oauth-consent.acceptance.test.mjs` и этой квитанцией. Production source, HTTP composition, доменная модель и браузер принадлежат другим авторам.

Тест исполняет настоящий TypeScript controller после `typescript.transpileModule` в VM. DOM, выбранный аккаунт, ответы портов, wall clock и monotonic clock — управляемые synthetic ports. Это причинная проверка controller, **не настоящий DOM, native keyboard/focus, Connect signing, HTTP/Provider/OAuth flow, geometry или screen reader**. В частности, DOM port не имитирует native blur при удалении узла: assertion требует живой connected focus target и обнаруживает оставшуюся ссылку на удалённый control.

## Исходные воспроизведения

| Finding | Причинный сценарий | Наблюдение |
|---|---|---|
| U1: потеря места клавиатуры и раскрытого пояснения | Открыть details, сфокусировать «Разрешить», удержать ответ `decide`, затем вернуть ошибку | `render()` удаляет активный control через `replaceChildren`; до ответа active node уже disconnected. Новый details закрыт. Native browser отдельно проверяет фактический уход в BODY. |
| U2: чужой текущий аккаунт при завершении принятого решения | Начать approve под A; переключиться на B; получить сохранённое approved A через явный reread; нажать возврат | `complete()` вызывается один раз под B. Второй самостоятельный case открывает уже approved A сразу под B с тем же результатом. Это account-consistency finding; тест не утверждает, что B получает grant A. |
| U3: expiry зависит от часов устройства | Серверные `checkedAt` и `expiresAt` дают ещё 60 секунд; часы браузера на час впереди | Fresh pending proposal не показывает «Разрешить», поскольку controller сравнивает server timestamp с `Date.now()`. |

Два исходных PASS сохраняют важные соседние границы: поздняя ошибка не возвращает фокус с выбранного внешнего control; dispose/observed account change не принимают поздний context response и не вызывают новое решение.

Исходный production controller: SHA256 `d7d941c57017500c22f227a86835661a8315dbc86d9d492dd0b422b62571a147`, 8 954 bytes; platform adapter: `8be3b319935eea53a25e79d114851c182ab6953b472ca2b383fff74c547de48c`, 4 801 bytes. Это worktree bytes до исправления, не committed Git identity.

Команда на isolated Node 24.21.0:

```powershell
& .\var\toolchains\node-v24.21.0-win-x64\node.exe --test --test-concurrency=1 src/world/oauth-consent.acceptance.test.mjs
```

Первый запуск: **6 tests / 2 PASS / 4 FAIL / 0 skips, 638.6361 ms**. Сохранённый лог: `output/implementation-20260930/p4-oauth-consent-independent-red.log`, SHA256 `75632838a3ea281c1920401b59c37fc3239fbec9b5d4830ce46b95d636a5eee5`. Файл теста на этом запуске: `c8a367e41b733ffab39f63f84ca9d9cc96c181d6669ab7865a8c80d337b7c0ee`, 10 176 bytes.

## Принятое направление исправления

Автор сохраняет logical focus key только для активного узла внутри текущей карточки, восстанавливает такой control синхронно после render и использует heading как fallback, если прежний action исчез. Busy controls остаются фокусируемыми с `aria-disabled` и синхронным guard; внешний фокус не захватывается после await. Раскрытое пояснение сохраняется.

Presentation получает server `checkedAt` и immutable `decidedAccountId`; локальный остаток срока вычисляется по monotonic elapsed с консервативным учётом сетевого времени. Completion повторно сверяет текущий аккаунт и передаёт его в native POST для server comparison. Те же RED assertions и узкие соседние сценарии подтвердили controller-часть исправления. Чтение server source подтверждает сравнение заявленного completion account с сохранённым решением; synthetic controller test не создаёт серверную аутентификацию.

После исходного исправления добавлены восемь причинных продолжений: account change во время асинхронного completion read; наблюдённый A→B→A с запоздалым snapshot A; busy controls и отсутствие повторных решений/native completion; lost ACK→явное чтение→один completion; отдельный deny; отрицательный wall-clock skew; сетевой elapsed больше server TTL; открытое пояснение через account panel и промежуточный loading.

Follow-up: **14 tests / 13 PASS / 1 FAIL / 0 skips, 365.9392 ms**. Все исходные четыре RED стали GREEN, как и остальные новые authority/TTL/retry cases. Единственный RED обнаружил неполное U1 исправление: account panel close вызывает `load()`, промежуточный render без context удаляет details, а следующий render принимал отсутствие узла за `open=false`. Автор заменил это на retained boolean, сохраняя прежние authority guards. Лог `p4-oauth-consent-independent-followup.log`: SHA256 `aa3a1577f69fd8148bddda36484932588b61ca7ea7f6e8f5336b4d34ad1082d4`.

Последний повтор того же файла: **14/14 PASS, 0 skips, 352.1267 ms**. Лог `output/implementation-20260930/p4-oauth-consent-independent-final.log`, SHA256 `862b11e7b8505703b562e21cd8c8f6b264c160390be26d65a6238bd01cfc6b25`. Assertions после RED не ослаблялись. `node --check` нового test file и whitespace inspection пройдены. Это один final14, а не сумма повторных прогонов.

## Прочитанный финальный срез

SHA256 ниже — actual worktree bytes, не Git-normalized identity. После final controller run CSS отдельно исправлялся root; его прочитанный hash не означает geometry test в VM.

| File | Bytes | SHA256 |
|---|---:|---|
| `src/world/oauth-consent.acceptance.test.mjs` — собственный test freeze | 15 780 | `eb6c20e34bd9a74ef204eadeb3755dcae441ceb7bd29e022017643bd837811f4` |
| `src/world/oauth-consent.ts` — controller в final14 | 11 567 | `79a5d7716d3e083ff048900bba3b5e3f5258905f9694b540f0f62d26b6cd3d55` |
| `src/platform/oauth-consent.ts` — read-only | 5 469 | `05ceee4db0abdcf2ea01bbdd645ba96d2ddc7216df787d3d98e4937f5e219829` |
| `src/world/oauth-consent.css` — read-only | 3 822 | `96e2f1889f5c61886b1b6ac253d7d50e1492c6a0c3408a7a0fc11470601ff5a9` |
| `src/entry.ts` — read-only | 2 309 | `66a1dbe409db8f8438bec1a4764d232b1a3976a5a27c1990698208aa19d14e1f` |
| `server/capabilities-oauth.js` — read-only | 11 545 | `298e63d9ad7008ffd39b247c418dbc169381c3074df02ef547e2e3095e1c5113` |

## Не подменять доказательства

- Ветка unknown decision ACK дополнительно передана автору: прежний catch снова предлагал approve/deny, хотя сервер уже мог сохранить immutable решение. Исправлено на stale/readback; synthetic committed-before-rejection case подтверждает явное чтение approved A и один completion без повторного decision. Доменный COMMIT и реальная потеря HTTP ответа этим тестом не создаются.
- Геометрия 320 px/короткого экрана, длинный account label, native Tab/Enter/Space, account panel return и visual focus принадлежат browser gate root. Переданный мной source risk `.sw-button { white-space: nowrap }` **root подтвердил в actual IAB**: ширина account control 658.34 px при viewport 320 px, document overflow. Root исправил `white-space:normal/min-width:0` и отдельно прежний отсутствовавший hex clip. На момент этой квитанции actual geometry повтор после исправления ещё не передан; VM этих размеров не измеряет.
- Root отдельно сообщил actual IAB CSP RED→GREEN: native POST→303→302 с `form-action 'self'` не достигает loopback, exact registered callback разрешает один переход. Это атрибуция его отдельного опыта, не мой browser run и не полный Connect/OAuth flow. Read-only server review видит точное callback-разрешение только для consent document после registry check; остальная shell сохраняет `self`.
- Endpoint в рабочем production host пока не включён; synthetic presentation не является доказательством настоящего подписанного решения, grant/token или завершения внешнего клиента.

## Первичные ориентиры

WAI-ARIA APG рекомендует сохранять фокус на кнопке, если её действие не меняет текущий контекст; при смене контекста фокус переносится в логически соответствующее место. Это обоснование проверки U1, а не заявление WCAG conformance: [Button Pattern](https://www.w3.org/WAI/ARIA/apg/patterns/button/).

High Resolution Time определяет monotonic clock, не зависящие от ручной корректировки системных часов. U3 проверяет именно разделение server TTL и локального elapsed: [High Resolution Time, редакция 2026-09-01](https://www.w3.org/TR/2026/WD-hr-time-3-20260901/). Server остаётся authority для истечения срока; clock в UI не выдаёт право завершения.
