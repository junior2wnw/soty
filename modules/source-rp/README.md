# Source RP: общий приватный порт

Пакет переиспользует maintained `openid-client@6.8.4` и HIVE renewal contract
`d63b067575df43d9f0a3161d2771e34602638f35`. Это server-only библиотека Source.
Она не создаёт account, tenant, project grant, agent key или доверие к Root header.
Native Session/immutable issuer-sub association/current role и selected grant
принадлежат исходному приложению.

`createSourceRpProtocol` проверяет exact approved issuer/client/callback, PKCE,
nonce, signatures/audience, token rotation и fresh userinfo через maintained SDK.
Basic300 остаётся default. Optional finite24h требует reviewed Root client/profile,
настоящий signed Root user choice и Source-owned installed storage format.
Наличие manifest или renewalProfile не означает согласия человека.

`createSourceRpSessionService` не предполагает SQLite: storagePort может работать
на Source SQLite/D1 или PostgreSQL. Закрытые типы — `server/index.d.mts`.
Шифрование AAD связывает `SourceRpRenewal` и exact JSON-array
`["soty.source-rp-session.v1",sessionIdHash,profileDigest,bindingDigest,revision]`;
keyId/profile/binding точны. Используйте `sourceRpCipherBinding`, не свою строку.
Срок строго от loginStartedAt и не сдвигается refresh/replay/rebind.

Состояния `idle → refreshing → idle/revoked/unknown`, durable CAS и единственный
claimant ограничивают RT. Unknown/expired claim не получают takeover и не повторяют
старый RT. Точный local COMMIT ACK можно восстановить по revision/attempt/cipher.
Старый AT при unknown допускается лишь до старого срока с fresh userinfo.
Source authority проверяется до/после awaits и включается в native CAS conditions;
network находится вне DB transaction. Remote issuer и Source DB не имеют общей
atomic transaction.

`currentProof` — opaque host result `{issuer,sub,expiresAt,sessionGeneration,
assertCurrent}` после fresh userinfo. AT/RT не возвращаются в DTO/HTTP/browser.
Source затем отдельно решает свои ACL. Никаких TTL authorization caches.

Проверка пакета: `node --test --test-concurrency=1 modules/source-rp/test/*.test.mjs`.
В этом пакете 15 проверок: real Root OIDC/подписанный выбор/отзыв устройства,
controlled-clock после 300 секунд/restart, lost COMMIT ACK и unknown RT,
два настоящих OS процесса с одной зашифрованной synthetic SQLite базой и одним
refresh send. Synthetic storage — test contract, не production adapter.

Consumers Planner SQLite и Поведай PostgreSQL устанавливают собственный формат
хранения, native sessions, migrations и guards. Эти интеграции, настоящий браузер
после 300 секунд wall time, Source restart/resume и Root slot rebind — отдельные
gates; успешный пакет сам по себе их не подтверждает.

Новый native CAS не может создавать identity association или grant. Claim/finish
сравнивают exact head/revision/cipher/profile/binding и текущую native authority
в самой Source transaction. Stale refreshing → unknown, не takeover. При
provider outage ready() до claim сохраняет idle. local COMMIT ACK recovery
требует exact next revision/attempt/cipher плюс вновь действующую Source authority.
