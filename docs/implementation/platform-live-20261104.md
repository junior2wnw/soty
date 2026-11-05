# Подтверждённые изменения живого полотна

Локальный кандидат на базе `450204c317e56d1c092046ca9864a05fc985149b`, 4 ноября 2026. Backend создан, проверяется отдельно от интерфейса и production. Схемы SQLite, persisted registry IDs и старые reader versions не меняются.

`platform.live.wait` — подписанная асинхронная Connect operation. Доказательство и rate limit фиксируются до ожидания; SQLite transaction через await не удерживается. Browser client должен включать operation в свой существующий parallel metadata allowlist, сохраняя account/localRevision fence.

Вход: `{expectedAccountId,cursor?,waitMs?,watch?}`. `waitMs` — целое 0…15000, по умолчанию 15000. `watch` — до 256 ссылок `{kind:'app'|'person'|'community',id}`. В ответе только `{schema:'soty.platform-live.v1',cursor,changed,reset}` плюс обычный Connect envelope `ok:true`. Там нет metadata, ID объектов, имён, картинок, портов, tickets и grants.

Initial read, потерянная серверная session и restart дают `reset:true`. Cursor подписан, привязан к account/device и boot generation. Чужой cursor отвергается. После `changed`/`reset` интерфейс заново читает directory/cover/profile с текущими правами; hint никогда не является разрешением на данные или запуск.

Private mutation получает только её account, текущий разрешённый зритель либо прежний разрешённый зритель один раз для снятия доступа. Ранее разрешённые watch refs подтверждаются существующими `apps.directory.resolve` / `world.directory.resolve`; собственных копий ACL нет. Public transitions могут будить все активные sessions. После отзыва следующий private edit прежнего зрителя не будит. Временный busy resolver сохраняет внутренний ref до следующей свежей проверки, а не объявляет ложный отказ.

Connect contact accept/remove/block и другие успешные contact actions после COMMIT дают content-free notification только двум участвующим accounts. Host сразу перепроверяет их действующие app streams существующим `apps.invalidateAccess`, затем будит живое полотно. На authority transition сервер очищает ранее подтверждённые watch refs, к которым доступ уже потерян; последующие private изменения не становятся дополнительным сигналом бывшему другу. Одного frontend refresh для этого недостаточно.

Hooks вызываются после успешного durable return source writer: регистрация app, app update/revoke/domains/publication/source, принятая cover commit/restore/remove, World mutations и avatar. Черновик cover не публикуется. COMMIT failure не публикуется. Observer не может отменить уже подтверждённую запись. Реальная отправка hints отложена microtask, чтобы не войти в Connect/World authority другого зрителя из ещё открытого outer Connect action.

Ожидания проверяют активность устройства при завершении; Connect revocation будит их отказом. HTTP disconnect передаёт trusted AbortSignal вторым host argument к Connect.handle и освобождает waiter. Этот signal не берётся из RPC JSON. Shutdown отменяет все ожидания. Пределы: 2048 tracked account/device sessions, 4 concurrent waits на session, до 512 confirmed watch refs и 512 deferred refs; idle sessions вытесняются с последующим reset. Переполнение deferred authority work завершает session явным `live_busy`, без silent drop. Это механизм текущего single-writer Root process; межпроцессный fanout и измеренная нагрузка не проверены.

## Проверки и доказательства

| Требование | Наблюдаемый результат | Проверка |
|---|---|---|
| Настоящее live app change | Открытый wait получает changed после регистрации | `server/test/platform-live-http.test.mjs`, первый сценарий |
| Нет блокировки исходящей очереди | Тот же реальный client регистрирует app при открытом wait | тот же сценарий, регистрация завершает wait до timeout |
| Приватность и отзыв | Чужой не будится; reader получает removal и теряет resolve; последующий private edit не будит | тот же сценарий, три подписанных HTTP clients |
| Durable avatar | Commit будит viewer, свежий avatar read возвращает новую revision | второй HTTP сценарий |
| Нет false success | Неудачный CAS avatar не меняет live cursor | второй HTTP сценарий |
| App cover | Draft не будит; commit будит; readback возвращает custom image | третий HTTP сценарий |
| Публичное обнаружение | Explicit listed publication будит внешнего зрителя | третий HTTP сценарий |
| Отмена | Реальный fetch abort освобождает server waiter | четвёртый HTTP сценарий |
| Cursor/restart/isolation | Чужой account/device и изменённый MAC отвергаются; restart делает reset | helper tests + первый HTTP сценарий |
| Способность ловить дефект | Контрольная глобальная private broadcast не проходит isolation oracle | `server/test/platform-live.test.mjs` |
| Реальный прежний дефект | При disconnected writer hook тот же SQLite COMMIT проходит, но live oracle проваливается; с hook проходит | последний helper test |
| Friendship authority | После contact accept app/profile доступны; после remove/block wait будится, оба resolve отказывают, outsider молчит; поздние private edits тоже молчат | пятый и шестой HTTP сценарии |

Команды: `node --test server/test/platform-live.test.mjs server/test/platform-live-http.test.mjs`; смежные suites — `modules/world/test/avatars.test.mjs`, `modules/apps/test/apps.test.mjs`, `modules/connect/test/extension-identity.test.mjs`, `server/test/field-directory-http.test.mjs`. Фактический браузерный путь, сохранение камеры при этих обновлениях, физические устройства и выпуск проверяются владельцем общей интеграции отдельно.
