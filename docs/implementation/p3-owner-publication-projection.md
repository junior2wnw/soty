# D4 — точное состояние публикации в карточке владельца

Дата: 2026-09-30. Изменение ограничено чтением и представлением состояния. Авторский source freeze; независимое ревью и повтор настоящего браузерного сценария выполняются отдельно.

## Подтверждённая причина

Root воспроизвёл: после `launchPolicy=anyone` и включения двух именных адресов карточка владельца показывала «Только вам». `apps.list` отдавал личные `grants`, но не публикацию; `app.ts` считал пустые grants признаком личного приложения. Локальный callback настроек временно заменял подпись и терял её после следующего list. Он также считал любой `anyone` публичным даже без активных aliases. Замок на карточке выбирался по grants, а размещение в сообществе вообще скрывало публикацию.

## Контракт

В owner-ветку существующего AppProjection добавлено ровно:

```ts
publication: {
  launchPolicy: 'restricted' | 'anyone';
  activeNamedAddressCount: number;
}
```

Это настройка допуска по конкретным именным адресам, а не работоспособность источника, DNS/TLS или разрешение на любой адрес приложения. Один SQL statement читает текущие app, target, policy и count. В count входят только enabled app и `app_publication_domains` → same-app/same-owner `app_domains` со state `bound`, role `alias`, zone kind `named`. Невключённые, закрытые, canonical и несовпадающие app/owner не учитываются. Действующие retained aliases продолжают учитываться при выключении **нового резервирования** имён.

При построении foreign projection повторно проверяется `canUse` для прочитанного актуального app. Это не позволяет устаревшей строке кандидата раскрыть новое имя после удаления личного допуска вторым writer. Новый publication summary, grants, порты, пути и имена устройств foreign caller не получает. Состав list, canonical admission, public alias admission и default launch не изменены; migrations и DDL не менялись.

## Представление

`src/world/app-audience.mjs` — общий formatter для list mapping, settings callback, карточки, окна связей и сохранённого summary/conflict copy в настройках. Последние используют только серверный snapshot, не несохранённые controls.

- `anyone` и count > 0: «Именные ссылки: всем», существующий значок внешней ссылки.
- `anyone` и count = 0: публичный доступ не заявляется; детали явно говорят «Именные ссылки: выключены».
- restricted: личный/выбранный доступ отдельно от включённых именных адресов.
- Неизвестный owner summary: «Ваше приложение», без догадки о приватности. Foreign: «Вам доступно».
- Revoked имеет приоритет: «Доступ закрыт».
- Offline/starting/stopped не отменяют настроенную публикацию и продолжают показываться отдельным состоянием источника.

В подробностях две явные границы: «Именные ссылки…» и «Личные допуски…». У карточки сообщества сохранены настоящий community control и чат сообщества; публичная подпись добавляется в существующий текстовый блок. CSS не менялся; используется существующее переносимое оформление identity. Фактическую геометрию 320px должен повторно проверить root, авторским VM-тестом она не доказывается.

## Выполненные проверки

1. `node --test --test-concurrency=1 modules/apps/test/app-owner-publication.test.mjs src/world/app-audience.test.mjs src/world/app-account-lifecycle.test.mjs` — **23/23 PASS, 0 skip**.
   - 5 новых реальных service/SQLite cases: owner/foreign projection; private canonical против public alias через policy; активный набор и retired aliases; retained zone; второй writer между candidates и projection; закрытый app; потеря foreign grant; явно внедрённые недопустимые active relations. Последний — проверка фильтра, не обещание восстановления произвольно повреждённой БД.
   - 4 новых formatter/actual compiled controller/card cases: settings → list одинаковая подпись, anyone+0, revoked/unknown, разделение личного допуска и публикации, link вместо lock, сохранение community/chat controls. DOM ports не являются настоящим браузером.
   - 14 прежних controller/lifecycle cases с новой реальной dependency в VM port: account transitions, late requests, same iframe и optional metadata.
2. `node --test --test-concurrency=1 modules/apps/test/apps.test.mjs` — **9/9 PASS, 0 skip**. Прежний реальный HTTP/WS, частные допуски, revoke, isolation и registry reopen.
3. `npm run typecheck` — PASS. `git diff --check` для изменённых tracked файлов — PASS (только уведомления Git о нормализации LF/CRLF).

Итого авторских целевых запусков после исправления: **32/32 PASS**, без тяжёлого полного suite. Настоящий browser list → settings → list, 320px и независимый review на момент этого receipt ещё не заявлены пройденными. Production, DNS и browser не изменялись автором.

## SHA-256 source freeze

| Файл | SHA-256 |
| --- | --- |
| `modules/apps/server/index.mjs` | `f3919d1d0708fa9a952761fb22b1a9c557424633ef29c8dba3fb033e65baeab6` |
| `src/world/app.ts` | `f8e2b400e364c0b14e5af9e7d1bb8838d0295fb2962e4f967e0b3c1d69f45125` |
| `src/world/types.ts` | `29f8a376e4d3cd22a2205d54161ed0911c3c4870ef7cb2ced6a68f5bb3bcf62c` |
| `src/world/application-card.ts` | `9eac4ee6b59b5e753a4dd20abf4f09d643d9029a2ea994acfe6bfbffa4292384` |
| `src/world/app-settings.ts` | `567641205a033e6d7c2992e403df2e9753e00c508ba3b39ffba67448ac7d1dd9` |
| `src/world/app-audience.mjs` | `fae0d1fc30feddf8ed094b8dd966869a20415317026bd8d8d31e3ab230b768c9` |
| `src/world/app-audience.d.mts` | `64450d2b1b8e5b47592f6cb94d0e578fd3e3022bf7e5c43e932aa7173fcc3853` |
| `modules/apps/test/app-owner-publication.test.mjs` | `32410e91ca096f5f54c56b5050e5b4ed21d8367f0c73f2420852e475a92e9b43` |
| `src/world/app-audience.test.mjs` | `a6c9885524e66954f33a62f8d9df786a06ad2784ea9f596febdef7bc53ffa5a4` |
| `src/world/app-account-lifecycle.test.mjs` | `08d58657a1151bc5c7c324faa049a02ec0d9269a563d1488f49cad3936f2bad6` |
