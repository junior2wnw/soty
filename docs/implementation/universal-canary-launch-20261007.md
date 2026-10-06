# Исполненный reviewed Linux canary v6

Root исполнил приведённую команду один раз: controller exit0, complete.ok:true,21children exit0, два собственных тома сохранены. Этот namespace не перезапускается: one-shot marker и данные остаются для review. Будущий запуск требует нового nonce, review и packet.

Application revision8e7a10468a144ed1842f9448adebc7099831cea3; source archive неизменён. Harness хранится отдельно:

- Local: D:/соты/output/soty-universal-platform-implementation-20261006/universal-canary-harness-v6-20f5e9cac0e34dde8bced550ff460d2e.
- Linux RO: /home/ai2/codex-soty-universal-20261007-9f8dcd71/harness-20f5e9cac0e34dde8bced550ff460d2e.
- Linux RW: /home/ai2/codex-soty-universal-20261007-9f8dcd71/canary-20f5e9cac0e34dde8bced550ff460d2e.
- Closed manifest digest:4f5c8dd92d3d970253f3cdd082235fcce98fe7c5bd8972d1ee8a3d17a5d7eaf8.

| Harness file | Exact SHA256 |
| --- | --- |
| universal-canary-bff.mjs | e3002f51641b0a3e1d070b6aa34e014b37be9b0e4f1ebfd7247c0737d6eca197 |
| universal-canary-runtime.mjs | 5fc66b39777a5404fe951b65c0bcd44d6250c4f69b024d769f7b4199d3f0c55f |
| universal-image-canary.mjs | 0e98d698dac33e25bfcdf5d53dd09febad6b3dedd101b4a62fb0e3bd703f58b7 |

Три files+manifest.json owner0:0/mode0600; nonce leaf dirs owner0:0/mode0700. Fixed no-network CAP_CHOWN helper менял только эти exact paths. Public source leaf700→755, выбранные parent dirs775→755: metadata-only, source ownerUID1000/bytes и task parent0700 сохранены. Это нужно User0:0 без DAC_OVERRIDE; recursive chmod/chown не выполнялся.

| Host source | Exact exported archive SHA256 |
| --- | --- |
| deploy/connector/docker-api.mjs | ede9d311989b1e6dad15e2110b7ce7b55a16f14f3b2fec4fd91d471f8d61f413 |
| deploy/connector/storage-guard.mjs | 7882ff84dcfa534f29582774d6f8e6a66feb116a2a775defcd9ac82976ac6f2d |
| deploy/connector/storage-probe.mjs | 37f514d5303832aa262e7ccb2f965cb7b23c2055f549ed469eb3c89f5cf43866 |
| deploy/connector/storage-snapshot.mjs | 1db67a36458a3e525b4a292e4baedde68ceedb9e219c004a3b13696acefc2834 |
| scripts/agent-modules/local-apps.mjs | a6373d81239593b3860546573be55f2959a15d02f6a853377bfb010acea0006b |

Archive содержит CRLF вследствие Windows Git export. Root бинарно сверил export и remote files, отдельно соответствие нормализованных bytes исходным Git blobs. Runner pins export, не преобразует bytes на лету, проверяет пять files перед мутациями. Здесь только public code hashes; config/key hashes не публикуются.

Точная исполненная команда из canary-run-reviewed-v6.sh:

```bash
set -eu
docker run \
  --name soty-universal-canary-20f5e9cac0e34dde8bced550ff460d2e-controller \
  --label io.soty.synthetic-universal-canary=20f5e9cac0e34dde8bced550ff460d2e \
  --label io.soty.synthetic-universal-canary.role=controller \
  --label io.soty.synthetic-universal-canary.revision=8e7a10468a144ed1842f9448adebc7099831cea3 \
  --network none --read-only --user 0:0 \
  --cap-drop ALL --security-opt no-new-privileges \
  --memory 805306368 --cpus 2 --pids-limit 96 \
  --env NODE_ENV=production --env SOTY_SYNTHETIC_CANARY=1 \
  --tmpfs /tmp:rw,noexec,nosuid,mode=1777,size=67108864 \
  --tmpfs /data:rw,noexec,nosuid,mode=700,size=1048576 \
  --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
  --mount type=bind,src=/home/ai2/codex-soty-universal-20261007-9f8dcd71/canary-20f5e9cac0e34dde8bced550ff460d2e,dst=/home/ai2/codex-soty-universal-20261007-9f8dcd71/canary-20f5e9cac0e34dde8bced550ff460d2e \
  --mount type=bind,src=/home/ai2/codex-soty-universal-20261007-9f8dcd71/harness-20f5e9cac0e34dde8bced550ff460d2e,dst=/home/ai2/codex-soty-universal-20261007-9f8dcd71/harness-20f5e9cac0e34dde8bced550ff460d2e,readonly \
  --mount type=bind,src=/home/ai2/codex-soty-universal-20261007-9f8dcd71/source-8e7a104,dst=/home/ai2/codex-soty-universal-20261007-9f8dcd71/source-8e7a104,readonly \
  --entrypoint node \
  sha256:21018c46a27f97ad419d37feb2ec1221ebcf10805ed8292fe67a6534f6f63558 \
  /home/ai2/codex-soty-universal-20261007-9f8dcd71/harness-20f5e9cac0e34dde8bced550ff460d2e/universal-image-canary.mjs \
  --owned-root /home/ai2/codex-soty-universal-20261007-9f8dcd71/canary-20f5e9cac0e34dde8bced550ff460d2e \
  --fixture-harness /home/ai2/codex-soty-universal-20261007-9f8dcd71/harness-20f5e9cac0e34dde8bced550ff460d2e \
  --harness-sha256 4f5c8dd92d3d970253f3cdd082235fcce98fe7c5bd8972d1ee8a3d17a5d7eaf8 \
  --fixture-source /home/ai2/codex-soty-universal-20261007-9f8dcd71/source-8e7a104 \
  --baseline-image sha256:c8754ba67f1883cb33417a970ef9947b0c1a499934d35f32f58f58a59c23c2e4 \
  --feature-image sha256:21018c46a27f97ad419d37feb2ec1221ebcf10805ed8292fe67a6534f6f63558 \
  --old-image sha256:1c79c2a71aa3438472da8b197f03494908ee242e67fd76bb6a72d3da6340c7da \
  --expected-revision 8e7a10468a144ed1842f9448adebc7099831cea3

```

Controller /data tmpfs подавляет Dockerfile anonymous VOLUME. Каждый child монтирует только один из двух новых named volumes; production data/config/Env не читаются. Только controller имеет socket. Engine operations ограничены exact owned create/start/stop/inspect, fixed image/new-volume/logs/private operator exec. Helpers не используют shell Cmd; bounded error/TAP parsing скрывает raw logs и секреты.

Safe log сохранён в локальном output; private config/evidence/backup и отдельный custody key остаются в закрытом собственном namespace. Deletion/публикация не выполнялись. [Результаты и пределы сценария](./universal-image-canary-20261007.md).
