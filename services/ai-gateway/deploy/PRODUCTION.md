# Gateway account AI deployment

The deployed Gateway is immutable image
`sha256:d2b7c2468c95e6112663ad8c772a868ea295101ee4d05df5186890b6f291466d`,
source revision `eeb2cb75660af60247df82ba19ae3f9fd83fbfed`.
The image passed all 75 Gateway tests and the actual published MiniMax 32768
output profile against an isolated database and fake provider.

The 2026-10-25 release cloned the existing Docker configuration in memory.
Existing environment values, private pepper/database/brokers, legacy-key bind,
ports, resources, restart policy and the default egress network were preserved.
Only `RELEASE_ID`, `SOTY_BRIDGE_CONFIG_FILE` and the read-only bridge JSON mount
were added. The account network was already attached before the replacement.
The private JSON sets the finite account allowance (50 RUB once, 5 RUB/day,
5 RUB/request, 32768 output tokens). Global service and probe caps stay unchanged.

Docker `unless-stopped` restarts the current image after host reboot. The old
container is deliberately retained stopped. Its image cannot enforce Soty
account policy, so **never start the old Gateway after Soty accounts or traffic
have been admitted**. Root rollback must retain this Gateway. Migration 006 is
additive and is not rolled back.

The historical Compose label points at
`/home/ai2/inference-commerce/releases/f3c92cbed00c0d0f6182fc28447c911aedc9247d/infra/compose.yaml`
plus an ephemeral stdin overlay (`-`). Reusing that command without a new
overlay can select the previous shared image and omit the signed bridge.
Use `production-account-ai.override.yaml` as the final overlay. It pins the
qualified image, removes the inherited build, preserves the private env file,
and adds the existing external account network and read-only configuration.
Never rebuild or replace PostgreSQL as part of a Gateway update.

Read-only configuration qualification (the private file is read locally by
Compose; do not print or save its expanded output):

```sh
RUNTIME_DIR=/home/ai2/.config/inference-commerce \
RELEASE_ID=eeb2cb75660af60247df82ba19ae3f9fd83fbfed \
docker compose --project-name inference-commerce \
  --env-file /home/ai2/.config/inference-commerce/runtime.env \
  -f /home/ai2/inference-commerce/releases/f3c92cbed00c0d0f6182fc28447c911aedc9247d/infra/compose.yaml \
  -f /home/ai2/.config/soty/account-ai-20261025/gateway-compose.override.yaml \
  config --quiet
```

An operator replacement must first check exact image/config pins, zero active
inference and service holds, immutable history hashes, and the encrypted backup.
Create a stopped candidate and compare the normalized Docker creation spec
against the intended configuration in memory before stopping the serving
container. Switch only Gateway, then check readiness, canonical model catalog,
bridge policy, financial projections and the preserved original rows. Keep the
old container stopped. Continuing the existing automatic probes is expected;
record probe spending separately from explicitly authorized manual canaries.

Release evidence is stored outside source in
`D:/глава/РАБОТА/2026-10-25-soty-account-ai/implementation/gateway-release/`.
Two initial attempts rolled back because the release catalog assertion used
short model names and then a management endpoint without its session. Both
candidates had become ready; no Soty accounts were created, original financial
rows remained unchanged, and migration 006 applied exactly once. The final
guard used read-only persisted catalog snapshots plus the serving readiness
count and passed before the successful replacement.
