# Generic selected resource v2 / Apps8

`soty.selected-human-embed.v2` добавляет typed `resource.selection`:
`{kind,nativeId,incarnationId}`. Native ID — точная непрозрачная строка,
1..4096 UTF8 bytes, wellformed/no controls. Unicode/case/spaces/slash не
нормализуются. Locator и Root app/account grants не создают Native Source ACL.

В Apps8 хранится тот же immutable admission pin/history. Explicit startup
`allowSelectedResourceMigration:true` / `SOTY_SELECTED_RESOURCE_MIGRATION=1`
разрешает точное7→8 изменение CHECK; неизвестный layout не ремонтируется.
Apps7/v1 route/consent wire сохраняется. Следующие Source kinds/compiled adapter
handlers используют этот формат8, без отдельного Root DB format на каждое app.
Unknown kind/handler/pin не выполняется. Reader1..8 нужен даже compatible
feature-off образу; настоящий old7 image должен быть отклонён до START после8.
Независимый67-object Apps8 DDL PIN:
`e3f5cdde4005ad32d7e28c3ef90e3736096265c97e84eb3c531221e3fc003267`.

Approved `HIVE_SELECTED_SOURCE` — static compiled handler. Автор не задаёт
endpoint/command/import/secret/routes. Fixed paths не выбирают другой Native ID.
Project read/response4MiB, mutation1MiB, feedback submit1.5M/get2MiB/list64KiB;
legacy v1 1MiB не расширяется. Capacity error предшествует отдаче тела;
4MiB не являются гарантией поддержки всех Native canvases.

Source profile1 (`HIVE_SELECTED_KERNEL_SOURCE`) остаётся прежним kernel pin и
не получает новые пути. Profile2 (`HIVE_SELECTED_SOURCE`) отдельно допускает
Native Vinext public JS `/_next/static/chunks/`, CSS `/_next/static/css/` и
fonts/images `/_next/static/media/`: только safe leaf и reviewed extensions,
без query, nested paths, percent encoding, redirects и Source cookie/MAC/subject.
Каждый файл ограничен4MiB; текущая исходная Root authority проверяется до/после
чтения. Это новый compiled Source approval, не новый Apps DB format. Старый
profile1 не превращается автоматически в profile2. Full editor browser evidence
и actual Source+Root session renewal остаются отдельными gates.

Public profile2 assets используют отдельную FIFO:4 active, не более32 ожидающих,
ожидание не более8s. Private/auth/write capacity остаётся4. Cancel/close освобождают
ожидания; после ожидания до Source fetch проверяются fresh Root/target/profile.
Переполнение/таймаут возвращает503 и fixed `Retry-After:1` без Source fetch;
очередь не повторяет запросы и не является durable execution/retry ledger.

Native handoff — exact approved origin `/soty/connect?intent=<opaque43>`,
не bearer-token и не permission. HIVE использует прежний confidential Native RP
client и exact `/account/soty/callback`; Root issuer остаётся независимым.
Source независимо проверяет maintained OIDC/current userinfo + Native session,
link/device/current project incarnation/ACL и отдельно явное Source consent.
REPORT consent отличается от редактирования полотна и Native Support.
Root current original account/device, app/target/Source/resource/issuer/client
привязаны MAC/request/body/cookie/nonce и fresh private Root read. Header Host
лишь virtual routing input; caller JSON не создаёт host/actor/context.

Readiness measurement использует только digest approved host pins, не Native
ID/owner/session/token. Optional trusted `resourceMigrationConfigured` отдельно
от v7 migration flag; старые DTO bytes без новой опции сохраняются. Readiness
конфигурации не доказывает текущую Source liveness/Native grant.

Пилот закрепляет один выбранный проект. Масштабирование библиотеки требует
Source-owned selection/navigation/current resource receipts и Native ACL;
operator config на каждый проект не является конечным onboarding. Current
64-host-profiles/100-apps/256-RAMslots — явные pilot bounds. Root RAM5min
continuation/renewal compose с отдельным frozen renewal packet, не выводится из
Source Native24h head или UI-ready.

`tests/selected-root-integration.test.mjs` Source HIVE запускает actual signed
Connect/Apps8 admission/installed connector/Native OIDC/SQLite-D1 save/replay,
feedback REPORT viewer/privacy, separate Native Support, feature-off restart
and revocation. Это HTTP/kernel evidence; full Native FlowEditor/Stage/browser,
compatible images и cold restore — отдельные release gates. Production не
переключался из этих tests.
