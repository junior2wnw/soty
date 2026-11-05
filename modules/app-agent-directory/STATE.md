# App agent directory state — 2026-11-04

Goal: R07/R15/R16 dynamic current Apps/module discovery with explicit owner opt-in and independent readonly grant transport.

Scope: new `modules/app-agent-directory/**`, `server/app-agent-directory*.js`, `public/agents/**`, and `server/test/app-agent-directory-root.test.mjs`. No edit to Root HTTP composition, AppsService, Root MCP/OAuth internals or canvas UI. Parent integrated Root wiring; its original-actor fix is retained in the directory authority helper. AppsService public authority port was implemented by the architecture agent and consumed unchanged.

Decisions: existing Apps registry only; existing `soty.app-agent.v1` closed SDK; SQLite opt-in metadata/CAS/immutable receipts; public-list/current-target fence; existing credential-to-owner host authority for agent read scopes; no JSON execution; existing Root `apps_query` transport only. Source pins attest runtime tuples, not code images. App-owned arbitrary MCP remains unadmitted. Module catalog is a live host registry, supports `canvas.widget` provider contracts without inventing backend/runtime readiness.

Versions: Node24.22.1 explicit bundled runtime. Baseline worktree HEAD450204c; shared Root integration files continue to change independently. Source SDK discovery points to the existing release manifest; no new package/dependency installed.

Evidence: `root-integration-results.log`,19/19 pass,0skip (16 module +3 real composing Root cases). Actual signed HTTP bootstrap/connector claim/two registrations/owner opt-in/publication without rebuild, public/private/unlisted metadata, current app grant/Source, HTTP vs MCP audience, independent synthetic HTTP Sources, exact replay after Root restart, awaited Root-grant revoke, then a separate Apps ACL denial after restoring the Source. Original15-case module log remains historical. Deferred-FK COMMIT failure and clean-download SDK/examples remain covered. Test source-change case deliberately emulates an already committed immutable target in test SQLite; it does not claim end-to-end Source promotion UI.

Corrections verified against failures: pack operation names now `apps.agentDescriptor.get/put` (old hyphen names prevented all Root startup; `root-before-ops-fix.log`). Service Source-contract lookup no longer nests its own Connect transaction, and is rechecked after the downstream fence. Service actors can read public metadata without receiving private owner rights; `canManage:false`. Page budget512KiB preserves Root2MiB text+structuredContent envelope; the original page demonstrably fails `root-before-pagination-fix.log`, and all12 large descriptors remain discoverable across pages. No global Connect regex or authorization requirement relaxed.

Open: Named Planner/Perimetrica/Hive Source credentials/grants, app-owned MCP protocol/health admission, owner-facing opt-in UI, full Source promotion UI and physical Android remain unverified. No production/publication/spend.

Next: parent reviews combined Root evidence and performs remaining application/UI acceptance. USER-only historical report is already delivered at `РАБОТА/2026-11-04-soty-live-platform/research/user-concept-evidence.md`. Pack and Root test frozen again; do not change without coordination.
