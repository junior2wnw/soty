# v2: попытка продолжить доступ после истечения Root slot

V1/Planner требует прежний live context и сохраняет принятый wire. V2 может
хранить bounded private RAM witness исходного подписанного запуска. В witness
нет cookie/AT/RT, Source права или готовности. Истёкший reference остаётся
закрытым для read/effects; новый `apps.scoped.renew` требует заново подписанного
current actor, текущей publication/target/profile/human authority и отдельного
Source ACK. Runtime profile возвращается из `decision.profile`.

Исходные Root account/device, human issuer/sub/client generation/profile,
Source/resource совпадают. UI-only target/policy может обновиться только через
текущий reviewed Root target; Source отдельно сравнивает семантику своего
Native consent. Нет наследования cookie jar между references. Истечение старого
reference не делает raw cookie/old actor authority. Basic/unknown/revoked
Source не могут получить `ready:true` из самого witness или Root app grant.

`POST /api/embed/session-continue`/`soty.source-session-continuation.v1` остаются
закрытыми. Gateway публикует Source readiness только после exact verified broker
ACK. Отказ или потеря ответа не доказывает отсутствие Source commit; повтор
использует тот же requestId и получает Source durable receipt. Failed attempt
закрывается прежним cleanup capability; Source effects остаются Source-owned.

Witnesses: максимум256 на process, срок максимум old-slot-end+24h, без eviction
активного witness. Explicit close, app invalidation и dispose удаляют witness;
expiry удаляет **live authority**, оставляя только ограниченную возможность
попытки. Это RAM pilot boundary, не production persisted history/scaling proof.
После cold Root witness отсутствует: свежий `apps.launch` и Source continuation
могут найти действующее durable Native согласие без новой регистрации/consent.

Новый Source011 admission требует собственного literal reader[1,2], explicit
миграции и prepared admission-off fallback. Apps database format8 не меняется,
нового Root product DB нет. Native image/browser/voice/wall gates отдельно;
synthetic unit или installed HTTP clock injection не считаются browser PASS.
