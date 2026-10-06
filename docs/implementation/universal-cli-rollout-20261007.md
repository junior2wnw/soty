# Closed Universal rollout/CLI — 07.10.2026

Source slice подключён к existing connector rollout, сохраняет отдельный Kvartal
policy и original container env/mounts/networks/limits. Это локально проверенный
operator workflow; production transition, config/keys или serving mutations не
выполнялись. Перед использованием нужны exact Linux images/cold-boot/backup и
совместимый fallback. Human core остаётся замороженным независимо от этого slice.

## CLI и приватная custody

Existing prepare/promote flags сохраняются. Optional Universal добавляет:

- `--universal-plan /exact/local/plan.json` — closed approved plan;
- `--universal-custody-file /private/external-custody.json` — отдельно supplied
  `{keyId,key}`; key ровно32bytes canonical base64url, никаких autokeys;
- `--universal-witness-file /private/new-witness.json` — encrypted packet;
- `--universal-shell-origins https://approved-one,https://approved-two` — explicit
  host HTTPS origins, из того же approved host configuration;
- `--reviewed-receipt /private/review.json` на promote содержит existing approval
  fields, плюс exact `universalWitnessId` и `universalPolicyDigest` из prepare.

prepare сначала делает read-only guard и одноразово связывает opaque handle с
originalId/image, candidateImage, revision, transaction и private full-config
preservation fingerprint. Затем сохраняет AES-GCM witness exclusive atomically:
temporary0600 file, file fsync, exclusive hardlink к final target, remove temporary,
directory fsync на Unix. Существующий packet никогда не перезаписывается. После
этого повторный guard и CREATE stopped candidate. Приватные файлы проверяются
owner/readable/no-symlink/nlink/parent/mode/size и не выводятся.

promote под exclusive journal lock читает reviewed receipt, exact encrypted packet
и supplied custody key. Restored binding нельзя перепривязать. Изменение секретов
Human file при прежних public pins, либо одинаковое изменение inherited secrets
original/candidate, отказывает до STOP. Неверный witness/public digest/tuple/image
также не разрешает mutation. Public configurationSha256 вычисляется из закрытых
nonsecret IDs/image/revision/transaction/policyDigest; private fingerprint находится
только в WeakMap и ciphertext. Existing no-Universal legacy fingerprint wire
оставлен отдельной границей совместимости.

`--docker-socket` выбирает локальный Docker socket, без URL/shell. Только fixture
tests используют явные `--fixture-mode synthetic-local --fixture-root <task tmp>`
и собственный `engine.sock`/Windows `soty-fixture-*` pipe. Receipt fixtureOnly не
является production acceptance. Default production constructor запрещает fixture
admission. Windows подтверждает file fsync/atomic rename, но не Unix directory
durability; остальные ошибки не подавляются.

## Runtime API и переходы

`Rollout` получает `args.universalPolicy` как opaque/restored handle,
`args.universalWitnessId` и constructor `universalMeasurement`. `createConfig`
после existing Kvartal добавляет exact Universal delta. Inherited
`io.soty.universal.legacy` и storage reader container labels удаляются: настоящий
mode/readers проверяются по immutable image и runtime. Обе фазы явно включают
только approved private operator flag; baseline не читает Human/reviews configs.

Проверки file bytes/private binding/config/image повторяются до CREATE, STOP,
candidate START и leave. Feature prepare требует actual original legacy-baseline
operator DTO до CREATE; initial baseline не требует нового порта у старого image.
После candidate START поддерживаемый private exec получает actual closed DTO и
проверяет planned mode/objects/public Human pins/renewal admission/reviews digest.
Callback/file drift до leave закрывает transition. Новый compatible fallback
сохраняет existing data; несовместимый original не запускается и не получает
rollback helper. Changed original protected configuration также не auto-started.

`DockerApi.universalPreparedness(exactContainerId)` исполняет единственную fixed
Node command, импортирующую `readUniversalOperator()` из `/app`. Никаких supplied
command arguments, environment, bearer или credential body. AttachStdout=true,
stderr/stdin=false, Privileged/Tty=false. Before/after exact container/image/start
identity и exec identity/process/exit проверены; stdout framing≤64KiB, exact JSON
shape. Ambiguous CREATE/START/stream loss никогда не повторяются и не считаются
ready. Ошибки дают только безопасный code. Схема Engine API pinned1.45:
[официальный API](https://docs.docker.com/reference/api/engine/version/v1.45/).

## Доказательство и предел

Meaningful focused tests выполняют fake engine state transitions и настоящие CLI
subprocesses через локальный fake Docker HTTP transport. Проверены encrypted
cross-process binding/custody, wrong receipt/key/digest/config/image/mode/DTO,
exclusive witness publication/noautokeys, baseline flag only, file drift между
переходами, immutable preserved configuration, fixed exec/stream loss/size/stderr/
wrong container/restart, и Human2 reader1 fallback refusal. Default legacy suite
остаётся отдельной проверкой. Fake daemon подтверждает код/transport boundaries;
он не доказывает готовность actual Docker/Linux images или runtime secret mount.
Настоящий Docker exec/native socket/image gate должен завершиться до serving
switch; SSH mutations здесь не выполнялись.
