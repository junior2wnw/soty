# Apps7: actual Linux images and cold restore

Root verified the exact application commit `4a9a7d7f88a93402ac955169527896b1e665bb53` in new isolated Dev containers. Production remained on its original image; the cold canary controller completed with exit0. This receipt covers Apps7 and Human2, not the later Apps8 or unfinished production Source integrations.

| Artifact | Exact identity |
| --- | --- |
| Source Git archive | `4187efcd58d8867ffcc6e2cc01587db69637bfdb6306fbac48c3425b3681462f` |
| Compatible baseline, compiled legacy1 | `sha256:4cacf01e2e05897f67c6c12567a4af91dd91d8c4e0aafed605e3af19a30462d0` |
| Features, compiled legacy0 | `sha256:9283aea6485b5f4c08bad9416c7d9470db38bb64c19ea1a9e256295cc9960d74` |
| Reviewed separate harness v7 | `1accc0c294ac283ce2f2584322a1c1d318057550c33a0ad1341553db79b317a8` |
| Owned canary nonce | `242780a6eeb14d24a6371914b807c2c1` |

The baseline build passed TSC/Vite and the complete Linux image gate: app release16/14pass/2explicit opt-in skips; Connect84/83pass/1platform skip; platform233/233pass; world1263/1248pass/15explicit skips; dev3/3pass; infrastructure402/400pass/2explicit skips. Identity, inference, durable connector and persistence selftests also passed. These sets total2001 tests,1981pass,20skips,0fail; the selftests are separate. The feature build reused the same validated code/layers and changed the reviewed compiled mode. Opt-in skips remain visible; the earlier separate installed scoped-channel/130-second WebSocket gate is a different receipt.

The actual harness checked eleven steps and twenty-one owned child containers, all completed with exit0, and retained two owned data volumes. It exercised signed Connect, application registration, Notes and effect receipts, mandatory feedback submit/exact replay/support reply/ready-to-check/reporter acceptance, and two real OAuth RP sessions. The controlled clock passed the original300-second access-token expiry; this is not a wall-time claim.

After stopping writers, the independent cold snapshot/probe recognized Rooms2/Apps7/Notes2/Capabilities3/Registration1/Feedback1/Human2 plus retained Connect. Source byte hashes came from the exact Git archive, including its existing CRLF bytes; runtime input was never normalized. The original production reader and the previous Apps6/Human2 baseline were both refused before CREATE/START. The new baseline reader7 reopened the same data with new features disabled and preserved private evidence.

The encrypted backup included data, private RP state and exact synthetic configuration. The original configuration was invalidated; restoration into a different owned volume recovered it from authenticated ciphertext. Both RP userinfo calls returned the original identities, retained receipts/tombstones matched, and the private native Unix operator passed9tests/8pass/1Windows-only skip (seven native cases). No secrets or private evidence fingerprints were printed.

The canary uses an empty approved selected-profile registry with an explicit Apps7 startup migration. It proves storage/reader/fallback behavior, not a real selected HIVE project, Planner renewal, managed PostgreSQL reviews, an external-author SSO wizard, local feedback-job triage, or production rollout. Those require their own Source and browser receipts. New Source integrations must preserve the same independent authority boundaries rather than borrowing this canary's synthetic identities.

Safe logs and public receipt are retained under `D:/соты/output/soty-universal-platform-implementation-20261006/`; private encrypted evidence stays in the owned Dev namespace. The older [Apps6 receipt](universal-image-canary-20261007.md) remains historical evidence for its exact8e image, and is not a fallback authorization for Apps7 data.
