# P4-C1 — независимая проверка OAuth ingress

30.09.2026. Проверены host ingress, авторские HTTP tests и bounded-body integration с настоящим `oidc-provider@9.12.2`. Найден один причинный дефект admission, исправленный root; повторная проверка **15/15 PASS, 0 skips**. Это локальный protocol/lifecycle checkpoint, не production OAuth, signed Connect consent или приёмка внешних клиентов.

Автор этого review изменил только новый `server/test/capabilities-oauth-ingress-independent.test.mjs` и этот документ. Production ingress и авторские tests менял root. Linux2 bundles/receipts, deployment, remote containers и credentials в этой работе не затрагивались.

## Найденный дефект и исправление

Исходный ingress освобождал lease по `res.finish`/`res.close`. Независимый тест выполнил реальный HTTP `/revoke` через pinned Provider и удержал его публичный `Adapter.find('AccessToken')` на управляемом Promise. Неизвестный synthetic token не создаёт права или artifacts.

При `requests=1` клиент первого запроса отключился, пока adapter продолжал работу. Второй запрос вошёл в тот же adapter до завершения первого: peak стал 2. Assertion ожидаемого capacity refusal получил **RED** за 58.6498 ms. Это нарушало принятую границу одновременных AS обработчиков; ограничение только открытых sockets такого свойства не даёт. Начальный timeout разработки fixture был отдельно вызван отсутствующим в нём custom `/revoke` route и не считается продуктовым RED.

Root удалил автоматический release по HTTP events. Владелец lease теперь освобождает его в `finally` **после `await` всей обработки**. Integration использует публичный `provider.use(async (ctx, next) => { … await next(); … })`: вызов Express `next()` не даёт такого completion boundary. Ошибка/обрыв во время чтения тела по-прежнему немедленно очищает buffer/listeners и отклоняет `readForm`; внешний `finally` освобождает admission.

Независимая fixture адаптирована только к этому публичному lifetime contract. Критерии сохранены и усилены: после client disconnect второй запрос получает capacity refusal, peak adapter work остаётся 1; после освобождения первого gate обычный третий запрос успешно завершается. Timer-based release, отмена уже выполненного эффекта и патч внутренних OAuth handlers не используются.

## Фактические проверки

Один последовательный запуск на Windows, **Node 24.21.0**, `--test-concurrency=1`: **15 tests / 15 PASS / 0 FAIL / 0 SKIP, 3019.061 ms**.

- Четыре независимых случая: actual Provider disconnect/late adapter/recovery; real chunked HTTP memory; raw duplicate Content-Type/Content-Encoding; ранний body abort, замена ровно одного reader и сохранение peer attempt rate.
- Пять авторских ingress tests: границы байтов/UTF-8, формы, finite body timeout, capacity и rate/URL constraints.
- Шесть текущих авторских Provider tests: PKCE/resource-before-consume, family revocation, два SQLite/Provider handles, реальная authorize/consent serialization, bounded upstream form, независимость повторного consent от короткой AS session. Последний случай уже присутствовал в проверенном source; это не новый независимый тест автора данного документа.

Memory probe — отдельный Node child с `--expose-gc --max-old-space-size=64`, actual HTTP parser и **16 384 однобайтовых chunk bodies**, удержанных до terminating chunk. После GC измерен прирост retained heap **235 120 B**, проверенный порог `< 2 MiB`. Проверка наблюдает process heap, не приватные поля ingress и не подменённый allocator. Она не измеряет максимальный RSS, kernel buffers или суммарную память production Provider.

Raw header тесты отправляют оба duplicate headers по TCP без нормализации `fetch`. Downstream не вызывается, ответ `invalid_request`; следующий корректный запрос допускается. Abort/rate case использует новые соединения и разные X-Forwarded-For: они не возвращают потраченную attempt и не создают новую peer identity. Cleanup ждёт завершения child/server, не оставляет тестовые listeners.

Команда:

```powershell
& '.\var\toolchains\node-v24.21.0-win-x64\node.exe' --test --test-concurrency=1 server/test/capabilities-oauth-ingress-independent.test.mjs server/test/capabilities-oauth-ingress.test.mjs server/test/capabilities-oauth-provider.test.mjs
```

Log: `output/implementation-20260930/p4-oauth-ingress-independent.log`. Syntax check и `git diff --check` прошли; warnings о будущем LF→CRLF в root package/lock не являются изменением проверенного ingress.

## Прочитанные границы и ограничения

- Ingress выделяет один bounded 16 KiB buffer, ограничивает form полями/байтами, отвергает duplicate/ambiguous fields и заголовки, compression и неверный UTF-8. `rawHeaders` используются до library fallback; semantic OAuth validation остаётся у Provider.
- Прочитан exact installed `oidc-provider@9.12.2/lib/shared/selective_body.js`: его собственный reader имеет limit 56 KiB и массив chunks. После полного upstream чтения используется предусмотренная библиотекой ветка `req.body`; фиксированное предупреждение ожидаемо и не содержит данных запроса. Мы не отключали предупреждение и не изменяли библиотечный код.
- Admission — per-process host work bound. Socket peer берётся из `remoteAddress`, а не произвольного forwarded header. Перегруженный/rejected запрос тоже расходует attempt. Внешний proxy, multi-process/global rate и pre-header TCP admission этими tests не доказаны.
- `bodyTimeoutMs=10000` ограничивает получение тела, **не** весь adapter lifetime. Не завершающийся downstream теперь удерживает свой слот; освобождение по выдуманному timeout снова нарушило бы work bound. Cancellation/finite domain deadlines должны быть согласованы с реальным host и не означают отсутствия принятого OAuth/Notes эффекта.
- Дальнейшая production composition обязана применять awaited lifetime к каждому своему AS handler, включая собственные interaction routes. Проверенный Provider middleware не доказывает ещё не собранные Host/origin admission, private no-store/error mapping, durable encrypted OAuth adapter, браузерное согласие или два внешних CLI.

Новых consequential blockers в проверенном slice после исправления не найдено. Это утверждение ограничено перечисленным source и actual tests.

## Проверенный SHA-256 срез

| Файл | SHA-256 |
|---|---|
| `server/capabilities-oauth-ingress.js` | `24aa0008755b3669ff0d9c73c8ac7a3df06643699bb6bbd010a212e05ffb9d59` |
| `server/test/capabilities-oauth-ingress.test.mjs` | `a1b23a51b8dd888e942eb4aa8d203caea7c5d066a80de46d8d6f7cc32eb0af4d` |
| `server/test/capabilities-oauth-provider.test.mjs` | `855089317e75fc069cfda0b93323797fc2dccf4cbe3597125411cf2cc3761abd` |
| `server/test/capabilities-oauth-ingress-independent.test.mjs` | `532bebd165b861ca09c9c152233daeeeeeede2ee4dd61b8c9be29039159fb45c` |
| `output/implementation-20260930/p4-oauth-ingress-independent.log` | `b6e952a9505981cad509e0cdafccefcffc5268bfffcbed446d6ede0385e0a8a2` |
