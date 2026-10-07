# Actual PostgreSQL Native managed core

Полный изолированный Linux gate298a47c07bc411d1e32108815c63092c прошёл. Это проверка реальных Prisma/PostgreSQL/Native HTTP операций, additive0036 managed store и холодного fallback. Human verifier в этом gate **синтетический**; приватный Source RP и production SSO этим результатом не подтверждены.

Root самостоятельно сверил все160 code pins и reviewed delta, выполнил exact-root archive guard и передачу собственных каталожных прав, офлайн подготовку Prisma и свежий DockerIPAM/host-route overlap check. Source children работают как UID1000 на отдельной internal сети10.203.199.16/28, без host ports, рабочих баз и настоящих provider calls. Собственные секреты и Native доказательства остались в закрытом каталоге; Native history защищена AES-GCM.

- Archive SHA256: `c874b12898920532f801d56c01fbf83808945b1a43f9b2bdec56a8d7ece117db`,2114048 bytes.
- Complete manifest SHA256: `8b48d2cebff48e4a53b198aaf078e44a4f5900de7040fe3a54761fb67a7ca479`.
- Unchanged literal0036 DDL SHA256: `b869842646fe5f42e707a420f60c1752b89bebdda8287ac3d24483b18ac23e73`.
- Unchanged Prisma schema SHA256: `9ab87c41701051f4714a4b2237a0d22e9347512ad0d513d644e4a3ca368e5a4f`.
- Prepared Source fixture image: `sha256:c973d2df6ad409d8909935be3ad0c55b7cc429a1b643a280ae4401d2f9b8f71a`; derived from exact old17 image, CLI/client6.19.3 and11 managed models. Это fixture overlay, не новая опубликованная Source версия.

Native phase проверила managed grants/effects/receipts, повтор после потерянного ответа и падения после COMMIT, конкурирующие процессы, окончательный срок/доступ после awaited Native projection, principal admission rollback, Native password/session linking и текущие site-admin полномочия, чужого пользователя/отозванный доступ/истёкший proof и permanent key tombstone. Обычный вход сам Native grant не выдаёт.

Fallback phase реально перезапустила PostgreSQL, открыла текущий совместимый Native reader с новой функцией выключенной и проверила приватность, Native admin/membership/site/object и links/counts/binding evidence. Literal reader2 признал точный normal layout; reader1 отклонён внешним gate **до START**. Не заявляется, что старый image17 самостоятельно распознаёт новые строки и отказывается.

Reader negatives проверяют типы/NULL/колонки/группировку CHECK и отсутствие ограничений. Каждый hostile layout устанавливается только внутри принудительно откатываемой собственной транзакции; reader обязан его отвергнуть, после rollback нормальный layout и сохранённые Native данные снова сверяются. Для regrouped CHECK удаляется лишь ограниченный fixture набор links внутри этой транзакции, чтобы PostgreSQL допустил VALID CHECK и проверка действительно дошла до reader. `NOT VALID` не применяется; normalizer/pins не расширены.

Независимый Root receipt после controller подтвердил identity/image/labels каждого из семи собственных контейнеров, ExitCode0 и отсутствие работающего PostgreSQL, exact internal IPAM и result `native=true,fallback=true,reader=true,productionTouched=false`. Public receipt сохранена в рабочем output каталоге: `povedai-pg-298a47c07bc411d1e32108815c63092c-public-receipt.json`.

Ранее сохранённые отказавшие fixtures не переписывались и не удалялись:2655 остановился на подготовке Prisma; b50 прошёл Native/fallback, но hostile CHECK не был установлен против существующего admin link;85c9 был отклонён archive-path guard до создания базы. Новый298a отличается от b50 ровно двумя code файлами: корректным негативным fixture/evidence и заранее проверенным fixed IPAM;158 остальных files/product/reader/DDL/schema bytes прежние.

Следующая обязательная граница: maintained Source RP с host-only original verified capture, текущими Principal/Session generation и fresh Native grant проверками внутри одной SQL-транзакции до и после awaited projection. Logout/refresh CAS должны использовать тот же порядок блокировок. Затем нужен actual Root OIDC +Source PostgreSQL +Root managed transport +browser gate на одном совместимом комплекте.
