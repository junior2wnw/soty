# P4 — независимая проверка OAuth discovery и mount

Проверены текущий diff относительно `7fb7047f5d7c4d4c0daa8ea3615b2590e744e3c6`, новый discovery-follow test, сохранённые root RED/GREEN logs и фактический исходник установленного `oidc-provider@9.12.2`. Проверка source-only: reviewer не менял production/tests и не повторял suites.

**Вердикт: блокеров в этом узком изменении не найдено.** Исправление позволяет внешнему клиенту действительно использовать опубликованные endpoints, scope и response mode. Оно не объявляет весь C1/C2, MCP или реальные CLI готовыми.

## Проверенный контракт

- `req.baseUrl = '/oauth'` — константа после host/route admission, а не значение из запроса. В pinned `helpers/oidc_context.js:urlFor` mount определяется через `originalUrl`, затем `mountPath`/`req.baseUrl`. Canonical discovery сохраняет выводимый `/oauth`; у RFC 8414 alias путь переписывается и этот fallback нужен. `originalUrl` остаётся прежним для ingress. Router завершает обработку в Provider callback, поэтому переписанный путь не попадает в последующий SPA handler.
- `clientAuthMethods: ['none']` использует штатную конфигурацию Provider. Она также убирает неприменимые signing-alg metadata через собственную нормализацию библиотеки. Это не вручную написанный token protocol.
- После `await next()` меняются только `scopes_supported` и `response_modes_supported` успешного route `discovery`. Обе discovery URL используют этот route. Остальные library metadata и генерация endpoint URL сохраняются. `configuration.discovery` здесь не годится: `actions/discovery.js` применяет `defaults`, который не заменяет уже заполненные поля.
- Scope остаётся ресурсным `notes.createDraft`; он не добавляется в top-level OIDC scope configuration ради косметического исправления JSON. В pinned `requestParamOIDCScopes` эта конфигурация участвует в реальной обработке OIDC scope, поэтому projection — более узкое решение.
- Опубликованный `query` теперь можно явно передать. Router принимает только отсутствующий `response_mode` или строку `query`; такой же optional field разрешён валидатором сохраняемого Interaction. `form_post`, `fragment` и неверные формы остаются запрещены. Режим не вводит новую authority: у `code` он совпадает с прежним implicit default.

## Причинные доказательства и их авторство

Root выполнил actual HTTP test через существующий `oauthNativeFixture`: PRM → RFC 8414/canonical discovery → опубликованные authorization/token/revocation/JWKS endpoints. Запрос выбирает scope и mode из metadata, проходит signed approval и encrypted domain, получает настоящий token, создаёт Note, затем отзыв через опубликованный endpoint снимает право. Два неразрешённых mode дают HTTP400. Проверка JWKS разбирает `application/jwk-set+json` и не ожидает обычный JSON content type.

Сохранённые RED подтверждают три разные проблемы: alias терял mount; discovery рекламировал `openid` вместо фактического scope; явный рекламируемый `query` не доходил до consent. Source review подтверждает исправление и входного allowlist, и persisted Interaction validator. Отдельный второй причинный прогон старого persisted validator этим reviewer не воспроизводился. Промежуточная ошибка разбора JWKS относилась к fixture; финальный test действительно читает wire body.

Финальный **root** gate: `26/26 PASS`, `0 FAIL`, `0 SKIP`, `6098.4378 ms`, [p4-oauth-discovery-profile-final.log](../../output/implementation-20260930/p4-oauth-discovery-profile-final.log). Он включает profile/artifacts, discovery-follow, account-switch и native HTTP. Reviewer прочитал log и assertions; это атрибуция root run, не собственный повтор. Upstream warning об already parsed body остаётся видимым; console-clean результат не заявляется.

## Совместимость и пределы

DDL и marker не меняются, но новый optional field является изменением допустимого plaintext Interaction. Старое приложение с прежним строгим allowlist может отказать при чтении такого артефакта. Его нельзя объявлять совместимым rollback reader только по числу `schemaVersion=3` или по успеху read-only SQL probe. До release/recovery нужен фактически совместимый reader; проверка этого deployment gate не входит в данный срез.

Сохранённые OIDC descriptive fields библиотеки не означают поддержку OIDC login: facade по-прежнему не принимает `openid`. Проверены локальные HTTP/Provider/Connect/Notes/Caps и опубликованные URL; public deployment, TLS edge, браузерные cookies/CSP, произвольные клиенты и реальные CLI login не проверялись этим тестом. В частности, этот результат не подменяет будущий MCP gate.

## Точные байты review freeze

SHA256 относятся к рабочим файлам, а не обещают совпадение Git LF bytes после изменения окончания строк.

| Файл | Bytes | SHA256 |
|---|---:|---|
| `server/capabilities-oauth.js` | 12430 | `d20cd27ff668cbe02dd8943ea960dc4d7d8149d15d3c49e1a66b59d7e4a67584` |
| `server/capabilities-oauth-provider.js` | 11745 | `f4cd38b6174c71085627650a3eb2b8d4a5277d64ec2949b279f5dfdf193e0bc1` |
| `modules/capabilities/server/oauth-profile.mjs` | 14230 | `e16b589a2cf8c492e7eb567989e465288009aea4bbc3b3c0db98319bd5799e07` |
| `modules/capabilities/test/oauth-profile.test.mjs` | 8999 | `130eba4965b10c51b57dc97a52a41d71fcfc654065d67e17fea71afd2ab8d2b9` |
| `server/test/capabilities-oauth-discovery.test.mjs` | 4475 | `706563cee7d4594ee426f147b87cf6c6cb5b74ec6c0456f2fce83b1cd718956d` |
| `p4-oauth-discovery-red.log` | 4447 | `7252032cc67a33d3f6c1b71ce65088a89167a17529b787604122be52f3f006e0` |
| `p4-oauth-discovery-profile-red.log` | 1224 | `26382838b1d06758b24409d827749ce98eb065dae65dbb9764894cafd491825f` |
| `p4-oauth-discovery-explicit-query-red.log` | 1094 | `8ca522ed23edfef44f7dc5281de12808f68de2fb93f44aa79b4fa4a7dbcc7e6b` |
| `p4-oauth-discovery-profile-final.log` | 3840 | `bf4573477496191f96d69d5f9b92fd073311511778c22a06f52dea04d4872dec` |

Все logs в таблице находятся в `output/implementation-20260930/`. Значения codes/tokens/state и секретная конфигурация в квитанцию не включены.
