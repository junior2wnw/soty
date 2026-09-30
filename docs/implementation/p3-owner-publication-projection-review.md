# D4 — независимое ревью owner publication projection

30.09.2026. Reviewer: `publishing_architecture`. Проверен explicit freeze автора `agent_ecosystem`; production/tests не редактировались. Это read-only code/contract review, а не повтор браузерной приёмки.

## Результат

**Незакрытых блокирующих замечаний в заявленной области не найдено.** Исходное противоречие «Только вам» при `anyone` и двух активных именных адресах исправлено общим owner-only summary. Выявленный во время ревью оставшийся текст настроек также исправлен автором до freeze.

### Закрытый finding: настройки продолжали обещать публичный вход без адреса

Первоначальный срез применял formatter в `app.ts` при settings callback, но реальный `app-settings.ts` оставлял две старые ветки: summary и conflictDetail выбирали «всем по активным ссылкам» по одному `launchPolicy === 'anyone'`. Законный случай — закрытие последнего активного alias: политика остаётся `anyone`, count становится нулём. Карточка уже показывала личный доступ, а настройки продолжали прежнее описание.

Это source finding, не отдельно выполненный reviewer browser RED. Root подтвердил узкое исправление. В frozen `app-settings.ts:608` оба текста теперь используют `describeAppAudience`/`publicationFromInspection` из сохранённого snapshot. Несохранённый draft, команды, CAS, consent и pending не изменены. При нулевом count детали явно говорят «Именные ссылки: выключены».

## Проверенные границы

- `index.mjs:161` строит один current SQL statement: app state/name/grants, выбранный immutable target, owner device, publication policy и count. Это устраняет смешение старой строки candidates с новой политикой. Согласованность относится к одной проекции; не заявляется единый глобальный snapshot всего списка/World/канального health.
- Count содержит только `enabled` app и действующие связи `app_publication_domains` с точными app/owner, `app_domains.role='alias'`, `state='bound'`, `app_domain_zones.kind='named'`. Join сверяет domain ID вместе с app/owner. Canonical, другой app/owner, tombstone, лишь зарезервированный и отключённый alias исключаются. Выключение новых claims не выключает уже действующий сохранённый named zone.
- Новый `publication:{launchPolicy,activeNamedAddressCount}` возвращается только владельцу. Foreign projection не получает summary, origins/domain IDs, grants, port/path, connector ID/device name. Дополнительный `canUse(actor,current)` перед foreign projection отклоняет устаревшего кандидата после потери допуска; новых оснований для допуска не добавлено.
- `canUse`, canonical/public alias admission, registry mutation, launch URL, target pins и default entry не изменены. Публичная публикация по alias не добавляет незнакомца в личный `apps.list`. DDL/migration/reader gate не затронуты.
- Formatter отделяет публикацию именных адресов от личных допусков. `anyone+0` не public; `revoked` имеет приоритет; missing/null/invalid summary нейтрален, без догадки «только вам». Offline/stopped остаются отдельной информацией и не трактуются как снятие настроенной публикации.
- `loadApps`, settings callback, settings summary/conflict, card и details читают один formatter. `publicationFromInspection` учитывает только selected active bound aliases и ноль для закрытого app. Сохранённый summary не строится из пользовательского черновика.
- Карточка без community context использует фактический `audience.icon`: external для публичных именных ссылок, без private lock. Карточка в сообществе сохраняет настоящую кнопку community/chat; отдельная публичная подпись не выдаёт чат сообщества за публичное обсуждение приложения. CSS/новые SVG не добавлены.

## Доказательства и ограничения

Прочитаны все новые 5 service/SQLite тестов, 4 formatter/compiled-controller/card теста и единственный dependency-port diff старого lifecycle fixture. Проверены 10 SHA source/tests против [авторского receipt](p3-owner-publication-projection.md); все совпали. Особо прочитаны второй SQLite writer между candidates и projection, grant-loss перед foreign projection и test-only недопустимые domain relations.

По авторскому receipt: **23/23** целевых tests, **9/9** прежних actual HTTP/WS tests, typecheck/diff-check PASS, без skips. Reviewer эти прогоны не дублировал и не приписывает их себе. Повтор actual 320 px/card → settings → list и геометрия дополнительной подписи остаются root browser gate. VM DOM ports подтверждают выбранные элементы/иконки/тексты, но не их размеры или работу assistive technology.

Summary — настройка доступа на момент чтения, не разрешение на запуск, не health/DNS/TLS attestation и не гарантия доступности будущего запроса. Fresh launch по-прежнему проходит отдельную существующую авторизацию.

## Проверенный freeze

| Файл | SHA256 |
| --- | --- |
| `modules/apps/server/index.mjs` | `f3919d1d0708fa9a952761fb22b1a9c557424633ef29c8dba3fb033e65baeab6` |
| `src/world/app-audience.mjs` | `fae0d1fc30feddf8ed094b8dd966869a20415317026bd8d8d31e3ab230b768c9` |
| `src/world/app-settings.ts` | `567641205a033e6d7c2992e403df2e9753e00c508ba3b39ffba67448ac7d1dd9` |
| `src/world/app.ts` | `f8e2b400e364c0b14e5af9e7d1bb8838d0295fb2962e4f967e0b3c1d69f45125` |
| `src/world/application-card.ts` | `9eac4ee6b59b5e753a4dd20abf4f09d643d9029a2ea994acfe6bfbffa4292384` |

Полный список остальных совпавших SHA приведён в авторском receipt. После freeze production source не менялся reviewer; дальнейшая правка требует повторного просмотра затронутой границы.
