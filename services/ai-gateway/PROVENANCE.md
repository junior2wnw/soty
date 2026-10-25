# Soty AI gateway

This module is maintained and released with Soty. Its separate process and PostgreSQL database retain the existing financial and provider isolation boundary.

The initial import on 2026-10-25 uses the source-only release `f3c92cbed00c0d0f6182fc28447c911aedc9247d` on the current operator server, with gateway/package code overlaid from the running image `sha256:2d0a6e5769f27f9dc97845dede080b36702ecf90f1274785c6750d494997d0d9`. This preserves runtime fixes absent from the release directory. Original migration bytes are retained; new schema changes must be forward migrations.

No installed configuration, database, credential, built asset or dependency directory is part of this import. The operator's safe file/hash inventory is `РАБОТА/2026-10-25-soty-account-ai/implementation/CANONICAL-SOURCE.safe.json` in the workspace above the repository.

Soty Root is the account identity authority. The gateway owns permanent account mapping, keys, policy, reservations and settlement. Service signing keys and provider credentials belong only in private runtime configuration.
