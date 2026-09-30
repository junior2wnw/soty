# P3-C1 — согласованный снимок настроек владельца

2026-09-30. Авторская область: `modules/apps/server/inspection.mjs`, `modules/apps/test/app-inspection.test.mjs`, этот документ. Общая интеграция `apps.inspect`, проверка `expectedAccountId`, CAS `apps.update`, TTL/доставка наблюдений и перенос `runtimePath` в `protocol.mjs` принадлежат root. UI и независимая приёмка — отдельные области. Источник приложения в C1 не меняется.

## Интерфейс

```js
const inspection = createAppInspection({
  db, assertActor, domains, publications, inspectSource,
  shellOrigin,
  nameClaimsEnabled: false,
  namedAppZone: '',
  now: Date.now,
});
inspection.read(actor, { appId }); // synchronous soty.app-inspection.v1
```

`db` — открытый `DatabaseSync` схемы Apps v3. `domains` и `publications` должны использовать **то же соединение**, а не отдельную БД. `assertActor` синхронно проверяет действующего доверенного actor и бросает ошибку при отказе. `read` не принимает `expectedAccountId`, return URL, выбранный target или дополнительные аргументы: общий transport guard проверяет/удаляет expectedAccountId до вызова. Constructor проверяет точный HTTP(S) shell origin без path/query/hash/userinfo; доверенность этого origin и изоляцию named zone проверяет общий startup policy. `namedAppZone` дополнительно проходит существующий `normalizeNamedAppZone`.

`inspectSource` вызывается синхронно, без I/O, Promise и изменения БД, только после проверки владельца. Получает frozen descriptor и frozen вложенные объекты:

```js
{
  app: { id, ownerAccountId, state, revision },
  target: { appId, revision, digest, profile, connectorKey, port, entryPath },
  device: {
    connectorKey, ownerAccountId, name,
    identity: { linkId, hostDeviceId, connectorId },
  },
}
```

Target выбирается через текущий `app_publications.active_target_revision` из `app_runtime_targets`. Device берётся по **target.connector_key**. Проверены принадлежность владельцу и совпадение connector key с сохранённой identity; старые `local_apps.port/entry_path/connector_key` не заменяют source. Publication registry проверяет immutable target digest/profile, inspection сверяет свою target row с его проекцией.

Callback возвращает только `{state,observedAt,freshUntil,evidence}`. Нельзя вернуть свою identity, порт, `ready`, `runtimeReady`, дополнительные provider fields или Promise. `null`/`undefined` — unknown/not-observed. Разрешены сочетания:

| state | timestamps | evidence |
| --- | --- | --- |
| `unknown` | оба null | `not-observed` |
| `offline` | оба null | `connector-offline` |
| `unknown`, `responding`, `unreachable` | safe integer ≥0; freshUntil строго позже observedAt | `connector-v1-observation` |

Истёкшее наблюдение сохраняет прежние timestamps и evidence со state unknown. Расчёт 45-секундной свежести и проверка действующего channel/target — обязанность root observer; этот модуль не выдумывает новое время. Для revoked приложение вообще не наблюдается, возвращается unknown/not-observed. Невалидный callback output даёт `apps_observation_invalid`; исключение observer превращается в фиксированный `apps_observation_unavailable`, без исходного сообщения.

Top-level `checkedAt` — server clock сразу после получения наблюдения. Caller передаёт тот же `now`, что используется observer; функция обязательна по типу, результат должен быть safe integer ≥0. Неверный результат или исключение дают контролируемый `apps_inspection_clock_invalid`. Клиент может консервативно считать оставшуюся свежесть из `freshUntil - checkedAt` с вычетом request roundtrip, не сравнивая часы браузера и сервера и не продлевая старое наблюдение новым чтением. Четыре поля самого observation остаются прежними.

## Снимок, права и ссылки

`read` начинает deferred `BEGIN`; первый SELECT приложения закрепляет SQLite/WAL snapshot. До subordinate read и наблюдения проверяется текущий owner. Domain metadata/quotas, publication/active set, source target и device читаются внутри того же snapshot. Никаких await между чтениями нет. Перед возвратом повторно проверяется actor; ошибка откатывает read transaction. Вложенная транзакция не принимается. Результат отражает один согласованный снимок, а не гарантию отсутствия конкурентных изменений после него. Следующая команда всё равно использует соответствующий CAS.

Поле `addresses.claimOrigin` показывает точный настроенный named base origin только при включённых claims. Сохранённые зоны не включают новый claim самостоятельно. `canReserveName` дополнительно учитывает enabled и обе текущие квоты; tombstone учитывается в usedByApp/usedByAccount.

Активный bound alias с anyone получает постоянный `origin + полный entryPath`. Canonical и активный restricted alias получают trusted shell URL `#launch/<app>/<domain>?path=<encoded entryPath>`. Query и hash-SPA проходят без потери/двойного декодирования. Boot ticket, linkId, connector key, owner identity, exposure ACK и старые runtimeReady/runtimeMode заглушки в DTO не попадают.

Неактивный и retired alias не имеют shareUrl. Revoked сохраняет факты об адресах, но **все shareUrl null и все actions false**. `canPreview` означает разрешённую попытку через canonical либо active bound alias в named-only конфигурации, независимо от online/наблюдения. Это не проверка здоровья программы, DNS или TLS.

Ссылки используют общий `runtimePath` из `protocol.mjs`. Старый unsafe entryPath остаётся видимым в source; shareUrl становится null и canPreview false. `canEdit/canPublish` для enabled остаются true, чтобы не мешать ограничить или закрыть такую публикацию. canPublish — право на изменение политики, а не заявленная готовность приложения или каталога.

## Авторская проверка

```text
node --test modules/apps/test/app-inspection.test.mjs
14 passed, 0 failed, 0 skipped
```

Проверены current-owner privacy до callback, отказ допущенному не-владельцу даже у anyone, закрытая проекция identity, расхождение legacy/immutable target, реальная запись **вторым SQLite connection между SELECTs** первого WAL snapshot, согласованные quotas/publication/domain revisions, private/public share URLs с query/hash-SPA, inactive/tombstone, named-only preview, disabled claims при retained zone, account quota с учётом tombstone, revoked и unsafe legacy paths. Наблюдения проверяются отдельно от разрешения preview; неверные identity/ready/Promise/evidence не принимаются. `checkedAt` снимается после observation с управляемыми часами; неверные часы не оставляют открытую транзакцию и не раскрывают исключение. При callback failure/actor invalidation read закрывается без записи.

Эти тесты проверяют read-model на настоящем SQLite, но fixture identity guard не заменяет интеграционную проверку подписанного Connect. Реальный connector, 45-секундная свежесть, signed HTTP, UI/Clipboard/keyboard, DNS/TLS и браузерный preview здесь не объявляются проверенными. Они остаются gate соответствующих авторов и независимого reviewer.
