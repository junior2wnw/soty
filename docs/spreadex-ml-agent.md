# SpreadEx ML integration

Soty Connector exposes a loopback-only API at `/integrations/spreadex/v1` and keeps exchange credentials out of the agent. The integration only stores a scoped SpreadEx device token.

Enrollment is accepted only when the returned scopes are exactly `heartbeat`, `predictions`, and `settings:read`. A token containing trading, shell, admin, or any additional scope is rejected and is not persisted.

## Local API

- `GET /status` and `GET /health` return pairing, connection, settings, worker, and component state. They never return the device token.
- `POST /pair` accepts `{ "code": "<one-time-code>" }` and exchanges it at `POST https://miniapp.spreadex.me/api/ml/agent/enroll`.
- `GET|POST /settings` reads or updates the small user-facing settings contract.
- `POST /unpair` removes the scoped token.
- `POST /predict` is a loopback scoring endpoint. It fails with `503` unless a signed component is active.

Browser requests are accepted only from the exact configured SpreadEx origin, the exact Soty/update origin, or loopback development origins. Integration responses never emit wildcard CORS.

`SOTY_SPREADEX_BASE_URL` may override the production base URL, but only HTTPS is accepted outside loopback development. The default is `https://miniapp.spreadex.me`.

## Installation deep-link

SpreadEx opens `https://соты.online/install/spreadex?pair=<one-time-code>`. The Soty page validates the code, moves it immediately from the address bar into tab-scoped `sessionStorage`, and calls the loopback `/pair` endpoint. If a compatible agent is unavailable, the page offers the existing OS-aware installer and retries every two seconds for up to ten minutes. A successful pairing clears the stored code and shows a single return action to `https://miniapp.spreadex.me`.

## Remote protocol

After pairing, the agent sends an authenticated heartbeat every two seconds to `/api/ml/agent/heartbeat`. A response may contain at most 32 `prediction_tasks`. The agent validates the target device, snapshot age, expiry, and identifiers; deduplicates by `request_id:snapshot_id`; and posts a batch to `/api/ml/agent/predictions`.

Settings use the canonical contract:

```json
{
  "mode": "off|assistant|gate",
  "profile": "careful|balanced|opportunity",
  "min_probability": 0.78,
  "effective_min_probability": 0.78,
  "max_prediction_age_ms": 2500,
  "device_id": "spreadex-device-id",
  "revision": 1
}
```

The agent may also connect to an exact-host `wss:` URL returned by enrollment. Authentication is the first WebSocket message; the token is never placed in the URL.

## Worker and release contract

The worker is a separate versioned process. Its archive is not part of the connector source. Put a release descriptor at `release/spreadex-ml.json` before building. Without that file the public manifest explicitly advertises `available: false`.

For a Soty-hosted artifact, place the worker/model archive under `public/agent/components/` and use a URL such as `/agent/components/spreadex-ml-1.0.0-windows-x64.zip` in the platform entry. The archive contains the self-contained runtime, LightGBM native library, pinned model, and executable named by that entry.

Every published descriptor must contain an Ed25519 signature over canonical JSON with the `signature` fields removed and all object keys sorted recursively. The connector verifies it with `SOTY_SPREADEX_ML_RELEASE_PUBLIC_KEY` (PEM, escaped PEM, or base64 DER SPKI), then verifies the archive SHA-256. An unsigned component is never installed.

Create `release/spreadex-ml.unsigned.json`, provide the private PKCS#8 key only through `SOTY_SPREADEX_ML_RELEASE_PRIVATE_KEY`, then publish the local artifacts:

```powershell
pnpm spreadex:release:sign
pnpm agent:release
pnpm build
```

The private key must stay in the release secret store and must never be added to the repository or archive. The matching public key is provisioned to the managed connector as `SOTY_SPREADEX_ML_RELEASE_PUBLIC_KEY`.

Installation extracts to a staging directory, runs the component's `--self-test`, and switches an active receipt only after the returned `featureSchema` and `modelVersion` match the signed release. The previous receipt is retained for startup rollback.

At runtime the executable is started with `--jsonl` by default. Requests and responses are one JSON object per line, correlated by `id`:

```json
{"id":"ml-...","method":"observe","params":{"request":{"request_id":"..."},"route":{"direct_spread":0.01}}}
{"id":"ml-...","method":"predict_batch","params":{"requests":[...]}}
{"id":"ml-...","ok":true,"result":{"predictions":[...]}}
```

A malformed response, process exit, oversized message, or timeout kills the worker. The current batch is acknowledged as `ok: false`; the next batch starts a clean worker. No fallback probability is fabricated.
