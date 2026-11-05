# Referrals module state

## Root update 04.11.2026 11:20UTC

The handoff below is historical. Current integration exists in `server/referrals-host.js`, `server/referral-capabilities.js`, `server/invitation-referrals.js` and `server/http-app.js`. Node24.22.1:25 module+real signed HTTP/MCP tests PASS, including external reference discovery,3×10%/fourth0, exact cumulative refunds, future configuration and restart. Agent documentation is served from `public/agents/app-platform.md`. Both local dev runtimes now support an explicit `--agent-api`;18 dev/flags/transport tests PASS, live browser401→200→403 verified. Human referral settings UI, actual payment producer and production rollout remain unaccepted. Full current scope/evidence: `D:\глава\РАБОТА\2026-11-04-soty-live-platform\research\referrals-and-agent-entry-state.md`.

## Original subagent handoff

Date:04.11.2026, Asia/Yekaterinburg. Parent base:450204c317e56d1c092046ca9864a05fc985149b. Scope:modules/referrals/** only; no host/package/UI mutations.

- Created:real SQLite ledger, immutable app-scoped attribution, owner-authorized CAS config (default3×1000bps), trusted-host event port, idempotency, original config snapshots, exact cumulative refund reversal, exact cached balances and validated app-contract descriptor.
- Tested:20 module tests pass; exact handoff run is recorded in test-results.log. Includes a real COMMIT failure and matching broken-control oracles.
- Runtime observed:Nodev24.13.1 with experimental node:sqlite warning. Repository package declaresNode>=24.15.0; full repository qualification under its declared runtime is the parent's responsibility.
- Not integrated:host Connect extension, owner/grant authority, trusted invitation/payment adapter, public discovery publication, realtime frontend. No providers or real payouts.
- Next:parent integrates service.execute/operations and descriptor through existing host contracts; module internal.applyEvent stays private to trusted event producers. Independent reviewer should verify owner-adapter scope and refund/idempotency paths before enabling a producer.
