# Human renewal schema2 — reviewable source, 07.10.2026

Базовая версия Human v1 сохранена в независимом commit
`f5931914f2e4bcaab988c3887ef17e2d71bcf02a`. Этот документ описывает изменение
исходников и локальные проверки; не разрешает первый production write, migration
или публикацию. Приложения HIVE/Planner подключают собственное приватное durable
RP-хранилище отдельно. Test BFF не является их готовым runtime SDK.

## Явное согласие и прежний вход

Trusted host profile получает optional closed
`renewal:{admissionEnabled:boolean,clientIds:[approvedClientId]}`. Секреты не
входят в публичный pin. Eligible client получает новый смысл profileDigest и
`refresh_token` в static maintained engine metadata; для существующего client
нужен новый version. Другие clients, protocolDigest, issuer и exact-case subject
остаются прежними. Изменение одного operational admission boolean не меняет
client pins/version/generation.

Подписанная операция `identity.human.approve` принимает optional
`stayInAppSeconds:0|86400`. Отсутствие поля эквивалентно0; никакого implicit long
consent. Только актуальный device/account, eligible RP и pending interaction
могут принять86400 при admissionOn. Значение входит в immutable decision intent
и interaction. Повтор/новый request ID не увеличивает согласие. После отключения
новых approvals точный ACK прежнего решения сохраняется.

Обычный context/decision wire остаётся v1. Только actual schema2 eligible
admissionOn context добавляет `renewal:{maximumSessionSeconds:86400}`. Без choice
нет RT и изменений AT300/ID60/Grant600/Session600. Long Grant/family имеет
абсолютный конец `initialApprovedAt+86400`, без sliding expiry. AT по-прежнему
не более300s, ID60s; OP cookie/Session остаются600s. Это не Capabilities/agent
grant, hardware/passkey assurance или автоматическое объединение старых accounts.

После approved decision eligible context также показывает immutable
`renewal.approvedSessionSeconds:0|86400`, включая admissionOff. Это presentation
фактически принятого согласия для reload, а не новая authority. UI сначала
повторно подписывает тот же выбор актуальным actor, затем вызывает complete;
backend completion проверяет исходное signed approval и current device fence.
Новый request ID не меняет approvedAt/deadline.

`admissionEnabled:false` закрывает новые long approvals, сохраняя finite refresh
ранее принятых семей с каждой свежей проверкой. Для fallback необходимо оставить
eligible clientIds и reviewed versions/pins. Удалять renewal-смысл с прежним
version нельзя: такой rewrite откажет `human_identity_client_version_conflict`.
Отзыв семьи/клиента/устройства закрывает последующее fresh use. Смена/удаление
одного RP не затрагивает pins и семьи другого RP.

## Хранение и совместимость

`createHumanIdentityService({...,allowRenewalMigration:true})` — единственный
trusted migration switch. Он не берётся из HTTP body, signed actor или
запрошенных app pins. Defaultfalse на пустом/старом store создаёт/читает exact
schema1; admissionOn без explicit migration отказывает. Уже существующая exact
schema2 читается при false. Структура2 добавляет RefreshToken model,
`retain_until`/index, поля выбранного срока в interaction/family и immutable
абсолютный family deadline/index. Migration признаёт exact v1 layout/metadata/FK
до DROP/ALTER и выполняет изменения одной transaction. Issuer, scope, base
identity profile/AAD, токен IDs и payload_cipher сохраняются. Неверный key/keyId
не подменяется автоматически.

Reader pins: seven-store `soty.storage-format.v5` остаётся прежним форматом
manifest; humanIdentity readers должны стать `[1,2]`. Schema2 lineage:
`soty.human-identity.sqlite.v2`; metadata profile:
`oidc-provider-9.12.2-human-v1`. Независимый literal vector содержит33 объекта.
DDL SHA256 `36d71d26ec13f3129923f0742730174df67637a2219d036a9ca79b98e0265a19`,
normalized layout SHA256
`b096f63d2b27fa301d9fd336a57205278495ea80355fd27477f46115d4f8a2c9`.
`deploy/connector/storage-human-identity-v2-probe.mjs` содержит эти pins без
импортов writer, чтения token bodies или decryption. Filesystem/symlink/WAL
custody остаётся у вызывающего storage-probe.

**После schema2 writes v1 image не является rollback.** Перед migration нужны
actual v2-compatible image и fallback, читающие1 и2, cold boot с admissionOff,
coherent encrypted backup/restore и независимый purge/revocation checkpoint вне
rollback domain. Перенос старой копии SQLite без этого checkpoint может оживить
consumed/revoked family; source-level replay CAS не доказывает безопасность такого
restore. Linux/image/start gates и Root trusted loader выполняются отдельно.

## Maintained engine и capacity

Runtime импортирует `modules/human-identity/provider.mjs`, не experimental
namespace. OAuth/OIDC остаётся `oidc-provider@9.12.2`. Request client берётся из
аутентифицированного `Provider.ctx.oidc.client`, не bodyclient. Engine всегда
rotate RT; store хранит consumed tombstones до первоначального family end.
Каждый read/consume/upsert и fresh userinfo проверяет device, account,
client/profile/generation и absolute expiry под Connect→Human fence.
Reuse сохраняет семейный revoke внутри COMMIT; adapter выбрасывает InvalidGrant
после committed outcome. Offline ID-token verification может принять уже
подписанный JWT до его60s expiry; current userinfo/refresh закрываются сразу.

Pilot caps:16 active long families/global,8/account,512 RT rows/family, общий
artifact cap8192, SQLite64MiB максимум; GC не более128 rows/table/call.
Consumed/revoked RT и immutable client history не evict. Неактивная семья может
занимать admission quota до finite expiry; quota не считается подтверждением
актуальных permissions. Перед consume store проверяет два свободных artifact
slots, family cap и128KiB page headroom. Известное exhaustion отказывает до
consume, сохраняя токен; допустимая повторная попытка определяется фактом этого
отказа, а не предположением о потерянном ACK.

Это preflight, **не durable reservation всего SDK pipeline**. Между adapter
transactions другая запись или disk failure может исчерпать место. Если engine
отказывает после consume, private dispatch закрывает именно эту семью, не выдаёт
успешный token response и сохраняет tombstone. RP должен считать неизвестный ACK
неопределённым и закрывать свою durable session без blind old-RT retry; новый
signed login нужен только в этой редкой ситуации/после конечного expiry.
Неисправное хранилище не может гарантировать дополнительный revoke write; уже
committed consume и отсутствие успешного response остаются fail-closed границей.

## Локальное доказательство

Focused suite использует настоящий Root attachHumanIdentity, Connect signatures,
maintained Authorization Code/PKCE/refresh, JOSE и два независимых HTTP BFF.
Проверены expiry после AT и OP Session/short Grant, reuse/race, wrong client,
source device revoke, только свой RP revoke, admissionOff, issuer/RP SQLite
reopen, unknown ACK, immutable signed choice, bounded GC и private RP CAS.
Capacity vectors используют valid synthetic encrypted SDK artifacts, actual
SQLite page pressure и injected поздний SDK write failure. Independent literal
storage tests доказывают preservation, refusal старого v1 reader и отсутствие
repair/decryption у schema2 probe. Контролируемое время — общий fixture clock,
не параметр входящего запроса. Process-worker failover, реальные app data/ACL,
Linux images и production long-session admission пока не подтверждены.
