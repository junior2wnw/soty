# Scoped application model policy: supervisor review proposal

Status: prepared code and synthetic evidence only. No live policy, model request, credential, website setting, upstream account or endpoint was changed.

The existing `/api/inference/v1/chat/completions` authentication returns an application ID. The proposal adds an optional, separate, nonsecret policy file selected by `SOTY_GONKA_APPLICATION_MODEL_POLICY_FILE`. It is read once at process startup. The credential file remains byte-for-byte unchanged and retains its strict legacy `{applications:[{id,token}]}` schema. No automatic fallback is implemented: the caller chooses one exact native model, and the proxy forwards it unchanged only after authentication and explicit policy validation.

With no policy configured, every application and connector retains `deepseek-ai/DeepSeek-V4-Flash-0731`. In a valid policy, applications without a rule keep that default. The connector endpoint never consults application rules. The proxy retains its upstream origin/key, request size, token/output-count limits, timeout, redirect rejection, streaming behavior and two concurrent requests per authenticated identity; different models for the same application share that concurrency limit.

## Exact proposed nonsecret configuration change

Before: no `SOTY_GONKA_APPLICATION_MODEL_POLICY_FILE` setting or policy mount.

After supervisor acceptance, proposed mount only:

- Host file: `/home/ai2/.config/soty/application-model-policy.json`.
- Container file: `/run/config/soty-application-model-policy.json`, read-only.
- Added environment setting: `SOTY_GONKA_APPLICATION_MODEL_POLICY_FILE=/run/config/soty-application-model-policy.json`.

Proposed file:

```json
{
  "schema": "soty.application-model-policy.v1",
  "applications": [
    {
      "id": "kvartalufa",
      "allowedModels": [
        "deepseek-ai/DeepSeek-V4-Flash-0731",
        "MiniMaxAI/MiniMax-M2.7"
      ]
    }
  ]
}
```

`kvartalufa` is the documented application ID in the existing README and c0ca5b8 selftest. The operator must confirm that it is the ID of the already-existing production credential using safe selected metadata before approval. This proposal does not authorize creating a credential or assigning this rule to another application. Confirm the proposed host path against the configuration-preserving transport rollout plan; it is not a claim that this new file exists.

Both models remain allowed during candidate validation so the currently published website's DeepSeek requests keep working. A later website candidate still uses one configured model. This file does not enable global model discovery or arbitrary model names: supported native values are the existing default and the explicit MiniMax candidate only.

## Validation and failure behavior

Schema keys and application rule keys are exact. Invalid JSON, unknown fields/models, repeated application IDs/models, empty model lists, invalid IDs, files larger than 64 KiB, missing configured files and read failures fail closed. An invalid configured file makes application readiness false and authenticated application requests return 503; connector DeepSeek remains available. Therefore validate the mounted file and both existing-app and candidate-app behavior in the isolated candidate before any replacement. Unauthenticated requests remain 401. A valid but disallowed model returns 400 without an upstream request.

Run:

```text
node scripts/application-model-policy-selftest.mjs
node scripts/gonka-proxy-selftest.mjs
```

The new HTTP tests use only loopback synthetic upstream responses. They cover default and scoped behavior, wrong app/token/route/model, invalid/read-failed configuration, SSE tool/model/terminal preservation, unchanged limits and per-app concurrency across models. The existing proxy suite covers default authentication, upstream failure/timeout and strict legacy credential-file validation. These results do not prove live provider readiness, tariff billing, served model or tool quality.

After review and scoped enablement: retain the at-most-two small native readiness calls from decisions01/02, on the existing allowed proxy origin and account. Only positive served-model/tool/terminal evidence permits the full exact website-candidate real suite. The normal release, rollback drill and public acceptance gates remain required. A failed test blocks that release; no fallback or weakened gate is authorized.

## Rollback

Preserve the previous transport-compatible image/configuration fingerprints. Restore its existing environment/mount set without the optional policy setting and restart through the reviewed configuration-preserving rollback procedure. Keep the application token file and upstream credentials untouched. This restores the DeepSeek-only application policy; its legacy token parser still accepts the identical token file.

If a website has subsequently switched to MiniMax, first restore its known-good DeepSeek configuration/release so rollback does not strand it. A rollback of this optional policy does not authorize using an old JSON-only image against migrated SQLite storage: the independent transport migration/rollback contract remains mandatory. Do not run the old traffic rollout script or change Roy, Astra, OpenCode, traffic, Caddy or the Docker daemon.
