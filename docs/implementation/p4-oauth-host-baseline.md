# P4-C1 — подключение reader3 к HTTP host

30.09.2026, root. Дополняет [domain baseline](p4-oauth-storage-baseline.md); не включает AS endpoints, issuance, новый OAuth bearer или согласие.

Найдена и воспроизведена ошибка composition: `server/http-app.js` создавал recovery только для Notes2/Caps2. Поэтому совместимый default-off reader3 открывался, но его сервер не планировал reconciliation. Минимальная правка допускает Notes строго2 и Caps строго2/3. Никакого `>=2`, автоматической миграции, зависимости от AS key или включения новых Notes-вызовов нет.

## Причинная проверка и результат

До правки новый настоящий Caps3 host дал два ожидаемых отказа: recovery равен null после reopen с выключенным исполнением. Existing Caps2 recovery и два scheduler tests прошли. Log `output/implementation-20260930/p4-oauth-host-baseline-red.log`:3 PASS/2 FAIL,2636.7585ms.

После исправления one-line gate и уточнения нового test assertion по реальному owner DTO `invocations` итог: **11/11 PASS,0 skips,4713.9792ms**, Node24.21.0. Log `output/implementation-20260930/p4-oauth-host-baseline-green.log`:

```text
node --test --test-concurrency=1 server/test/capabilities-recovery.test.mjs server/test/capabilities-schema3.test.mjs server/test/capabilities-actions.test.mjs
```

Промежуточный `p4-oauth-host-baseline-fixture-error.log` имеет10 PASS/1 FAIL: новый тест ошибочно читал `items`, тогда как действующий signed API возвращает `invocations`. Исправлен только новый assertion; API не переименовывался, этот промежуточный результат не считается успешным.

Проверены реальные signed Connect/HTTP/stores:

- Test fixture выполняет отдельные явные native1→2 и OAuth2→3 перед стартом. Сам HTTP host не получает migration flags или OAuth key/config и открывает существующий actual3 с прежним registry ID.
- Native HTTP создаёт одну Notes-записку на3. После reopen с execution off свежий вызов получает503; прежний current-authorized HTTP receipt и signed owner history читаются, exact private title/body доступны владельцу через Notes. В публичном receipt нет title/body/token, стоит no-store. Сохраняются один Invocation и один proof.
- Existing cancellation/pending recovery сценарий выполнен отдельно на Caps2 и Caps3: созданный host scheduler присутствует даже при execution off. Его таймер закрывается, затем проверяется один детерминированный scheduler turn на настоящем reopened host coordinator. Отменённая заявка завершена, другая остаётся pending; никакая новая Note не исполняется. Это не ожидание настоящего таймера1s и не proof crash этого HTTP процесса.
- Прежние6 HTTP cases сохраняют create/get/replay, edit/purge/disabled history, две личности и scope/revoke boundaries, strict Host/auth/body/routes, default-off/mixed stores и trusted audience.

Proof-positive recovery после настоящего Notes COMMIT→lost return→Caps3/reopen→Connect revoke/Notes purge отдельно исполнена domain author в указанном baseline receipt. Root не выдаёт свою обычную HTTP receipt-проверку за повтор этого fault scenario.

Схема4 отвергается domain reader до host activation и native coordinator проверяет точный admitted format; эти negative cases входят в domain gate. Нового image label или production reader/start/restore этим результатом нет. Сам OAuth и два полноценных клиентских сценария остаются следующими этапами.

## Финальная совместная проверка baseline3

После независимого domain review root выполнил все `modules/capabilities/test/*.test.mjs` и все14 `server/test/*capabilit*.test.mjs` последовательно на Node24.21.0: **243/243 PASS,0FAIL,0skip**,40311.3621ms. Log `output/implementation-20260930/p4-oauth-baseline-integrated.log`. Включены новый independent7, schema3/recovery host, существующие discovery/Connect/HTTP/access/native tests и pinned-provider/ingress suites. Это один фактический полный зелёный запуск после исправлений, а не сумма прежних результатов.

Предшествующий module-only log `p4-oauth-baseline-full-red.log` содержит168PASS/2FAIL: прежний `native-storage.acceptance.test.mjs` ожидал supported `[1,2]` и unsupported для одного marker3 без DDL. Теперь advertised support `[1,2,3]`; неполная3 всё ещё отвергается как `schema_layout_invalid`, отдельная4 — `schema_version_unsupported`. Byte/no-repair assertions сохранены. После исправления этот файл9/9PASS,1146.3318ms (`p4-oauth-historical-acceptance-green.log`), затем выполнен общий243. Исторические frozen v2 fixtures не изменены.
